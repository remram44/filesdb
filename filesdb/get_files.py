"""Get versions of projects from the API.
"""

import aiohttp
import asyncio
import hashlib
import logging
import os
from pkg_resources import parse_version
import sqlalchemy
import tarfile
import tempfile
import zipfile

from . import database
from .utils import secure_filename


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

    # Insert into database
    query = (
        database.files.insert()
        .values(
            download_name=download_name,
            name=filename,
            size_bytes=size,
            hash_sha1=h_sha1.hexdigest(),
            hash_sha256=h_sha256.hexdigest(),
        )
    )
    db.execute(query)


async def process_versions(db, http_session, project_name, versions):
    latest_version = max(versions, key=parse_version)

    # See if we have files for any downloads of the latest version
    query = '''\
        SELECT
            EXISTS (
                SELECT files.name
                FROM downloads
                INNER JOIN files ON downloads.name = files.download_name
                WHERE downloads.project_name = :project
                    AND downloads.project_version = :version
            ) AS has_files;
    '''
    has_files, = db.execute(
        query,
        {'project': project_name, 'version': latest_version},
    ).fetchone()
    if has_files:
        logger.info("%r %s has files, skipping", project_name, latest_version)
        return

    # List downloads
    query = (
        sqlalchemy.select([
            database.downloads.c.name,
            database.downloads.c.url,
            database.downloads.c.type,
        ])
        .where(database.downloads.c.project_name == project_name)
        .where(database.downloads.c.project_version == latest_version)
    )
    downloads = list(dict(row) for row in db.execute(query).fetchall())
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

    with db.begin():
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

            try:
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
                                return
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
                                return
                            name = member[idx + 1:]
                            if name in IGNORED_FILES:
                                continue
                            with arch.open(member) as fp:
                                process_file(db, download['name'], name, fp)
                else:
                    with tarfile.open(filename, 'r:*') as arch:
                        for member in set(arch.getmembers()):
                            if not member.isfile():
                                continue
                            if not check_top_level(member.name, project_name):
                                logger.warning(
                                    "File %s from download %s doesn't have the expected top-level directory",
                                    member.name,
                                    download['name'],
                                )
                                return
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
                                return
                            name = member.name[idx + 1:]
                            if name in IGNORED_FILES:
                                continue
                            with arch.extractfile(member) as fp:
                                process_file(db, download['name'], name, fp)
            except (tarfile.TarError, zipfile.BadZipFile):
                logger.warning("Error reading %s as an archive", download['name'])
            else:
                # Mark download as indexed
                query = (
                    database.downloads.update()
                    .where(database.downloads.c.project_name == project_name)
                    .where(database.downloads.c.name == download['name'])
                    .values(indexed=True)
                )
                db.execute(query)


async def amain():
    with database.connect() as db:
        async with aiohttp.ClientSession() as http_session:
            # List versions
            query = '''\
                SELECT project_name, version
                FROM project_versions
                WHERE project_name IN (
                    SELECT name FROM projects WHERE name > ? ORDER BY name LIMIT 20
                );
            '''
            projects = db.execute(query, ['']).fetchall()
            while projects:
                logger.info("Got %d versions (%s - %s)", len(projects), projects[0][0], projects[-1][0])

                current_project_name = projects[0][0]
                versions = []
                for project_name, version in projects:
                    if project_name == current_project_name:
                        versions.append(version)
                    else:
                        await process_versions(db, http_session, current_project_name, versions)
                        current_project_name = project_name
                        versions = [version]
                if versions:
                    await process_versions(db, http_session, current_project_name, versions)

                # Get next batch
                projects = db.execute(query, [projects[-1][0]]).fetchall()


def main():
    logging.basicConfig(level=logging.INFO)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(amain())


if __name__ == '__main__':
    main()
