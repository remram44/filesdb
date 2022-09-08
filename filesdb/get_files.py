"""Get versions of projects from the API.
"""

import aiohttp
import asyncio
import contextlib
import hashlib
import itertools
import logging
import os
from pkg_resources import parse_version
import sqlalchemy
from sqlalchemy.sql import functions
import sys
import tarfile
import tempfile
import zipfile

from . import database
from .utils import retry, secure_filename


PROJECT_CHUNK_SIZE = 500

CONCURRENT_REQUESTS = 5

IGNORED_FILES = ('PKG-INFO', 'MANIFEST.in', 'setup.cfg')


logger = logging.getLogger('filesdb.get_files')


def check_top_level(filename, project_name):
    project_name = project_name.lower().replace('-', '_')
    filename = filename.lower().replace('-', '_')
    return filename.startswith(project_name)


def process_file(db, download_name, filename, fp):
    # Compute hashes
    h_sha1 = hashlib.sha1()
    h_sha256 = hashlib.sha256()
    size = 0

    chunk = fp.read(4096)
    while chunk:
        h_sha1.update(chunk)
        h_sha256.update(chunk)
        size += len(chunk)
        if len(chunk) != 4096:
            break
        chunk = fp.read(4096)

    # Sanitize filename a little bit
    filename = filename.encode('utf-8', 'replace').decode('utf-8')

    # Insert into database
    db.execute(
        database.files.insert()
        .values(
            download_name=download_name,
            name=filename,
            size_bytes=size,
            hash_sha1=h_sha1.hexdigest(),
            hash_sha256=h_sha256.hexdigest(),
        )
    )


def process_archive(db, project_name, download, filename):
    inserted = 0

    if filename.endswith(('.whl', '.egg')):
        with zipfile.ZipFile(filename) as arch:
            for member in set(arch.namelist()):
                if (
                    '.dist-info/' in member
                    or member.startswith('EGG-INFO')
                    or member.endswith('.dist-info')
                    or member in IGNORED_FILES
                ):
                    continue
                with arch.open(member) as fp:
                    process_file(db, download['name'], member, fp)
                    inserted += 1
    elif filename.endswith('.zip'):
        with zipfile.ZipFile(filename) as arch:
            for member in set(arch.namelist()):
                if member.endswith('/'):  # Directory
                    continue
                if not check_top_level(member, project_name):
                    logger.warning(
                        "File %s from download %s doesn't have the expected top-level directory",
                        member,
                        download['name'],
                    )
                    return 'wrong structure'
                if (
                    '.egg-info/' in member
                    or member.endswith('.egg-info')
                    or member in IGNORED_FILES
                ):
                    continue
                try:
                    idx = member.index('/')
                except ValueError:
                    logger.warning(
                        "File %s from download %s doesn't have the expected top-level directory",
                        member,
                        download['name'],
                    )
                    return 'wrong structure'
                name = member[idx + 1:]
                if name in IGNORED_FILES:
                    continue
                with arch.open(member) as fp:
                    process_file(db, download['name'], name, fp)
                    inserted += 1
    else:
        with tarfile.open(filename, 'r:*') as arch:
            members = {m.name: m for m in arch.getmembers()}.values()
            for member in members:
                if not member.isfile():
                    continue
                if not check_top_level(member.name, project_name):
                    logger.warning(
                        "File %s from download %s doesn't have the expected top-level directory",
                        member.name,
                        download['name'],
                    )
                    return 'wrong structure'
                if (
                    '.egg-info/' in member.name
                    or member.name.endswith('.egg-info')
                    or member.name == 'PKG-INFO'
                    or member.name in IGNORED_FILES
                ):
                    continue
                try:
                    idx = member.name.index('/')
                except ValueError:
                    logger.warning(
                        "File %s from download %s doesn't have the expected top-level directory",
                        member.name,
                        download['name'],
                    )
                    return 'wrong structure'
                name = member.name[idx + 1:]
                if name in IGNORED_FILES:
                    continue
                with arch.extractfile(member) as fp:
                    process_file(db, download['name'], name, fp)
                    inserted += 1

    if inserted == 0:
        return 'no files'
    logger.info("Got %d files", inserted)
    return 'yes'


@retry(3, logger)
async def process_versions(http_session, project_name, versions):
    latest_version = max(versions, key=parse_version)

    with database.connect() as db:
        # See if we have files for any downloads of the latest version
        is_indexed, = db.execute(
            '''\
                SELECT
                    EXISTS (
                        SELECT name
                        FROM downloads
                        WHERE project_name = :project
                            AND project_version = :version
                            AND indexed NOT NULL
                    ) AS is_indexed;
            ''',
            {'project': project_name, 'version': latest_version},
        ).one()
        if is_indexed:
            logger.info("%r %s is indexed, skipping", project_name, latest_version)
            return

        # List downloads
        downloads = db.execute(
            sqlalchemy.select([
                database.downloads.c.name,
                database.downloads.c.url,
                database.downloads.c.type,
            ])
            .where(database.downloads.c.project_name == project_name)
            .where(database.downloads.c.project_version == latest_version)
        ).fetchall()
        downloads = list(dict(row) for row in downloads)
        if not downloads:
            return

    # Pick a wheel
    for download in downloads:
        if download['type'] == 'bdist_wheel':
            if 'python_version' not in download:
                download['_filesdb_priority'] = 5
            elif 'py2' in download['python_version']:
                download['_filesdb_priority'] = 6
            elif 'py3' in download['python_version']:
                download['_filesdb_priority'] = 7
            elif 'cp' in download['python_version']:
                download['_filesdb_priority'] = 1
            else:
                download['_filesdb_priority'] = 4
        elif download['type'] == 'bdist_egg':
            download['_filesdb_priority'] = 3
        elif download['type'] == 'sdist':
            download['_filesdb_priority'] = 2
        else:
            download['_filesdb_priority'] = 0
    download = max(downloads, key=lambda d: d['_filesdb_priority'])

    with tempfile.TemporaryDirectory(prefix='filesdb_') as tmpdir:
        # Download file
        logger.info("Getting %s", download['url'])
        filename = os.path.join(tmpdir, secure_filename(download['name']))
        async with http_session.get(download['url']) as response:
            if response.status != 200:
                logger.warning("Download error %s: %s", response.status, download['name'])

            with open(filename, 'wb') as fp:
                async for data, _ in response.content.iter_chunks():
                    fp.write(data)

        with database.connect() as db:
            with contextlib.ExitStack() as stack:
                transaction = stack.enter_context(db.begin())

                try:
                    result = process_archive(db, project_name, download, filename)
                except (
                    tarfile.TarError, zipfile.BadZipFile,
                    EOFError,  # Can be raised by gzip
                ):
                    result = 'bad archive'
                    logger.warning("Error reading %s as an archive", download['name'])

                if result != 'yes':
                    logger.warning("Error: %s", result)

                    # Rollback transaction, start a new one
                    with stack.pop_all():
                        transaction.rollback()
                    transaction = stack.enter_context(db.begin())

                # Mark download as indexed
                db.execute(
                    database.downloads.update()
                    .where(database.downloads.c.project_name == project_name)
                    .where(database.downloads.c.name == download['name'])
                    .values(indexed=result)
                )


def iter_project_versions(db, start_from=None):
    query = '''\
        SELECT project_name, version
        FROM project_versions
        WHERE project_name > :project
            OR (project_name = :project AND version > :version)
    '''

    current_project_name = None
    versions = []

    projects = db.execute(query, {'project': start_from or '', 'version': ''}).fetchall()
    while projects:
        logger.info("Got %d versions (%s - %s)", len(projects), projects[0][0], projects[-1][0])
        for project_name, version in projects:
            if current_project_name is None:
                current_project_name = project_name
                versions.append(version)
            elif project_name == current_project_name:
                versions.append(version)
            else:
                yield current_project_name, versions
                current_project_name = project_name
                versions = [version]

        projects = db.execute(
            query,
            {'project': current_project_name, 'version': versions[-1]},
        ).fetchall()

    if versions:
        yield current_project_name, versions


async def amain(start_from):
    with database.connect() as db:
        async with aiohttp.ClientSession(
            headers={'User-Agent': 'filesdb (https://github.com/VIDA-NYU/filesdb)'},
            timeout=aiohttp.ClientTimeout(
                total=900,
                sock_connect=15,
                sock_read=900,
            ),
        ) as http_session:
            # Count projects
            total_projects, = db.execute(
                sqlalchemy.select([functions.count()])
                .select_from(database.projects)
            ).one()

            if start_from is None:
                done_projects = 0
            else:
                # Count projects we're not processing
                done_projects, = db.execute(
                    sqlalchemy.select([functions.count()])
                    .select_from(database.projects)
                    .where(database.projects.c.name < start_from)
                ).one()

            # List versions
            projects = iter_project_versions(db, start_from)

            # Start N tasks
            tasks = {
                asyncio.ensure_future(process_versions(http_session, project_name, versions))
                for project_name, versions in itertools.islice(projects, CONCURRENT_REQUESTS)
            }

            while tasks:
                # Wait for any task to complete
                done, pending = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # Poll them
                for task in done:
                    tasks.discard(task)
                    task.result()
                    done_projects += 1
                    if done_projects % 100 == 0:
                        logger.info("%d / %d", done_projects, total_projects)

                # Schedule new tasks
                for project_name, versions in itertools.islice(projects, CONCURRENT_REQUESTS - len(tasks)):
                    tasks.add(asyncio.ensure_future(process_versions(http_session, project_name, versions)))


def main(start_from):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(amain(start_from))


if __name__ == '__main__':
    if len(sys.argv) == 1:
        start_from = None
    elif len(sys.argv) == 2:
        start_from = sys.argv[1]
    else:
        raise AssertionError("Too many arguments")
    main(start_from)
