from concurrent.futures import ThreadPoolExecutor
from hashlib import sha1
import itertools
import logging
import os
from pkg_resources import parse_version
import re
import requests
import shutil
import sqlite3
import tarfile
import tempfile
import time
import threading
import zipfile


logger = logging.getLogger(__name__)


def url_get(url, stream=False, retry=True, ok404=False):
    wait = 2
    attempts = 5 if retry else 1
    for i in itertools.count(1):
        try:
            r = requests.get(url, stream=stream)
            if ok404 and r.status_code == 404:
                return r
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if i == attempts:
                raise
            logger.error("Request error (%d/%d): %r", i, attempts, e)
            time.sleep(wait)
            wait *= 2


re_project = re.compile(r'<a href="/simple/([^"]+)/">([^<]+)</a>')


schema = [
    '''
    CREATE TABLE projects(
        project VARCHAR(100) PRIMARY KEY,
        version VARCHAR(50),
        archive VARCHAR(255)
    );
    ''',
    '''
    CREATE TABLE files(
        project VARCHAR(100),
        filename VARCHAR(255),
        sha1 VARCHAR(41)
    );
    ''',
    '''CREATE INDEX files_idx_project ON files(project);''',
    '''CREATE INDEX files_idx_filename ON files(filename);''',
    '''CREATE INDEX files_idx_sha1 ON files(sha1);''',
    '''
    CREATE TABLE python_imports(
        project VARCHAR(100) PRIMARY KEY,
        import_name VARCHAR(50)
    );
    ''',
    '''
    CREATE INDEX python_imports_idx_import_name
    ON python_imports(import_name);
    ''',
]


def main():
    db_exists = os.path.exists('projects.sqlite3')
    db = sqlite3.connect('projects.sqlite3', check_same_thread=False)
    db_mutex = threading.Lock()
    if not db_exists:
        for statement in schema:
            db.execute(statement)

    threads = ThreadPoolExecutor(8)

    page = url_get('https://pypi.org/simple/').text
    for _ in threads.map(process_project,
                         ((db, db_mutex, m)
                          for m in re_project.finditer(page))):
        pass

    db.close()


def process_project(args):
    db, db_mutex, m = args

    name = m.group(2)
    link = m.group(1)

    logger.info("Processing %s", name)

    json_info = url_get('https://pypi.org/pypi/{}/json'.format(link),
                        ok404=True)
    if json_info.status_code == 404:
        logger.warning("JSON 404")
        return
    json_info.raise_for_status()
    json_info = json_info.json()

    releases = sorted(json_info['releases'].items(),
                      key=lambda p: parse_version(p[0]),
                      reverse=True)
    if not releases or not releases[0][1]:
        logger.warning("Project %s has no releases", name)
        return

    version, release_files = releases[0]

    # Select one of the archives
    for release_file in release_files:
        if release_file['packagetype'] == 'bdist_wheel':
            if 'python_version' not in release_file:
                release_file['_filesdb_priority'] = 5
            elif 'py2' in release_file['python_version']:
                release_file['_filesdb_priority'] = 6
            elif 'py3' in release_file['python_version']:
                release_file['_filesdb_priority'] = 7
            elif 'cp' in release_file['python_version']:
                release_file['_filesdb_priority'] = 1
            else:
                release_file['_filesdb_priority'] = 4
        elif release_file['packagetype'] == 'bdist_egg':
            release_file['_filesdb_priority'] = 3
        elif release_file['packagetype'] == 'sdist':
            release_file['_filesdb_priority'] = 2
        else:
            logger.error("Unknown package type %r",
                         release_file['packagetype'])
    release_file = sorted(release_files,
                          key=lambda r: r.get('_filesdb_priority', 0),
                          reverse=True)[0]

    # Check if project is up to date
    with db_mutex:
        cur = db.execute(
            '''
            SELECT archive FROM projects
            WHERE project=?;
            ''',
            [name],
        )
        try:
            archive_in_db = next(cur)[0]
            cur.close()
        except StopIteration:
            cur.close()
        else:
            if archive_in_db == release_file['filename']:
                logger.info("Project %s is up to date", name)
                return

        # Remove old versions from database
        db.execute(
            '''
            DELETE FROM files
            WHERE project=?;
            ''',
            [name],
        )
        db.execute(
            '''
            DELETE FROM python_imports
            WHERE project=?;
            ''',
            [name],
        )

    logger.info("Getting %s", release_file['url'])
    tmpdir = tempfile.mkdtemp()
    try:
        with url_get(release_file['url'], stream=True) as download:
            tmpfile = os.path.join(
                tmpdir,
                release_file['filename'].replace('/', '-'),
            )
            with open(tmpfile, 'wb') as fp:
                for chunk in download.iter_content(4096):
                    if chunk:
                        fp.write(chunk)
            process_archive(db, db_mutex, name, tmpfile)
    finally:
        shutil.rmtree(tmpdir)

    # Update database
    with db_mutex:
        db.execute(
            '''
            DELETE FROM projects
            WHERE project=?;
            ''',
            [name],
        )
        db.execute(
            '''
            INSERT INTO projects(project, version, archive)
            VALUES(?, ?, ?);
            ''',
            [name, version, release_file['filename']],
        )
        db.commit()


IGNORED_FILES = ('PKG-INFO', 'MANIFEST.in', 'setup.cfg')


def process_archive(db, db_mutex, project, filename):
    try:
        if filename.endswith('.whl') or filename.endswith('.egg'):
            with zipfile.ZipFile(filename) as zip:
                for member in zip.namelist():
                    if ('.dist-info/' in member or
                            member.startswith('EGG-INFO') or
                            member.endswith('.dist-info') or
                            member in IGNORED_FILES):
                        continue
                    with zip.open(member) as fp:
                        process_file(db, db_mutex, project, member, fp)
        elif filename.endswith('.zip'):
            with zipfile.ZipFile(filename) as zip:
                for member in zip.infolist():
                    if member.filename.endswith('/'):  # Directory
                        continue
                    if not member.filename.startswith(project):
                        logger.error("File %s from project %s doesn't have "
                                     "the expected top-level directory",
                                     member.filename, project)
                        return
                    if ('.egg-info/' in member.filename or
                            member.filename.endswith('.egg-info') or
                            member.filename in IGNORED_FILES):
                        continue
                    try:
                        idx = member.filename.index('/')
                    except ValueError:
                        logger.error("File %s from project %s doesn't have "
                                     "the expected top-level directory",
                                     member.filename, project)
                        return
                    member_name = member.filename[idx + 1:]
                    if member_name in IGNORED_FILES:
                        continue
                    with zip.open(member) as fp:
                        process_file(db, db_mutex, project, member_name, fp)
        else:
            with tarfile.open(filename, 'r:*') as tar:
                for member in tar.getmembers():
                    if not member.isfile():
                        continue
                    if not member.name.startswith(project):
                        logger.error("File %s from project %s doesn't have "
                                     "the expected top-level directory",
                                     member.name, project)
                        return
                    if ('.egg-info/' in member.name or
                            member.name.endswith('.egg-info') or
                            member.name == 'PKG-INFO' or
                            member.name in IGNORED_FILES):
                        continue
                    try:
                        idx = member.name.index('/')
                    except ValueError:
                        logger.error("File %s from project %s doesn't have "
                                     "the expected top-level directory",
                                     member.name, project)
                        return
                    member_name = member.name[idx + 1:]
                    if member_name in IGNORED_FILES:
                        continue
                    with tar.extractfile(member) as fp:
                        process_file(db, db_mutex, project, member_name, fp)
    except (tarfile.TarError, zipfile.BadZipFile):
        logger.error("Error reading %s as an archive", filename)


def process_file(db, db_mutex, project, filename, fp):
    # Compute hash
    h = sha1()
    chunk = fp.read(4096)
    while len(chunk) == 4096:
        h.update(chunk)
        chunk = fp.read(4096)
    if chunk:
        h.update(chunk)

    with db_mutex:
        # Insert file into database
        db.execute(
            '''
            INSERT INTO files(project, filename, sha1)
            VALUES(?, ?, ?);
            ''',
            [project, filename, h.hexdigest()],
        )

        # Guess Python package name
        if (filename.endswith('.py') and
                filename not in ('test.py', 'tests.py', 'setup.py')):
            if '/' in filename:
                package_name = filename[:filename.index('/')]
            else:
                package_name = filename[:-3]
            db.execute(
                '''
                INSERT OR IGNORE INTO python_imports(project, import_name)
                VALUES(?, ?);
                ''',
                [project, package_name],
            )

        db.commit()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
