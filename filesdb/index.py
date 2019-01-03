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


logger = logging.getLogger('index')


def url_get(url, stream=False, retry=True, ok404=False):
    wait = 2
    attempts = 5 if retry else 1
    for i in itertools.count(1):
        try:
            r = requests.get(url,
                             headers={'User-Agent': 'filesdb'},
                             stream=stream)
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
        archive VARCHAR(255),
        updated DATETIME,
        deleted DATETIME
    );
    ''',
    '''CREATE INDEX projects_idx_updated ON projects(updated);''',
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

    # Get list of packages
    page = url_get('https://pypi.org/simple/').text
    matches = re_project.finditer(page)

    # Attach a temporary database, store list of projects to process
    db.execute('''ATTACH DATABASE '' AS tmp;''')
    db.execute(
        '''
        CREATE TABLE tmp.todo(
            project VARCHAR(100) PRIMARY KEY,
            link VARCHAR(100)
        );
        '''
    )
    cursor = db.executemany(
        '''
        INSERT INTO tmp.todo(project, link)
        VALUES(?, ?);
        ''',
        ([m.group(2), m.group(1)] for m in matches)
    )
    logger.info("Found %d projects", cursor.rowcount)

    # Mark absent projects as deleted
    cursor = db.execute(
        '''
        UPDATE projects SET deleted=datetime()
        WHERE deleted IS NULL
            AND projects.project NOT IN (SELECT project FROM tmp.todo);
        '''
    )
    logger.info("%d projects were deleted", cursor.rowcount)

    # Get list of projects to process: those we haven't indexed in at least a
    # day, oldest first
    cursor = db.execute(
        '''
        SELECT tmp.todo.project, tmp.todo.link, projects.archive
        FROM tmp.todo
            LEFT OUTER JOIN projects ON tmp.todo.project = projects.project
        WHERE julianday() - IFNULL(julianday(projects.updated), 0) > 1
        ORDER BY projects.updated;
        '''
    )

    # Process projects in parallel
    threads = ThreadPoolExecutor(8)
    for _ in threads.map(lambda a: process_project(*a),
                         ((db, db_mutex, name, link, archive)
                          for name, link, archive in cursor)):
        pass

    cursor.close()

    db.close()


def process_project(db, db_mutex, name, link, archive_in_db):
    logger.info("Processing %s", name)

    json_info = url_get('https://pypi.org/pypi/{}/json'.format(link),
                        ok404=True)
    if json_info.status_code == 404:
        logger.warning("JSON 404: %s", name)
        releases = []
    else:
        json_info.raise_for_status()
        json_info = json_info.json()

        releases = sorted(json_info['releases'].items(),
                          key=lambda p: parse_version(p[0]),
                          reverse=True)
        releases = [(k, v) for k, v in releases if v]
        if not releases:
            logger.warning("Project %s has no releases", name)

    if not releases:
        with db_mutex:
            # Insert the project if it's not already there
            db.execute(
                '''
                INSERT OR IGNORE INTO projects(project, version,
                                               archive, updated)
                VALUES(?, NULL, NULL, datetime());
                ''',
                [name],
            )
            # Update the date if it is
            db.execute(
                '''
                UPDATE projects SET updated=datetime()
                WHERE project=?;
                ''',
                [name],
            )
            db.commit()
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
    if archive_in_db == release_file['filename']:
        logger.info("Project %s is up to date", name)
        return

    # Remove old versions from database
    with db_mutex:
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
        db.execute(
            '''
            DELETE FROM projects
            WHERE project=?;
            ''',
            [name],
        )

    logger.info("Getting %s", release_file['url'])
    tmpdir = tempfile.mkdtemp(prefix='filesdb_')
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
            INSERT INTO projects(project, version, archive, updated)
            VALUES(?, ?, ?, datetime());
            ''',
            [name, version, release_file['filename']],
        )
        db.commit()


IGNORED_FILES = ('PKG-INFO', 'MANIFEST.in', 'setup.cfg')


def check_top_level(filename, project):
    project = project.lower().replace('-', '_')
    filename = filename.lower().replace('-', '_')
    return filename.startswith(project)


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
                    if not check_top_level(member.filename, project):
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
                    if not check_top_level(member.name, project):
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
        try:
            db.execute(
                '''
                INSERT INTO files(project, filename, sha1)
                VALUES(?, ?, ?);
                ''',
                [project, filename, h.hexdigest()],
            )
        except UnicodeEncodeError:
            logger.warning("Error encoding project %r file %r",
                           project, filename)
            return

        # Guess Python package name
        if (filename.endswith('.py') and
                filename not in ('test.py', 'tests.py')):
            if '/' in filename:
                package_name = filename[:filename.index('/')]
            else:
                package_name = filename[:-3]
            if package_name not in ('setup', 'test', 'tests', 'examples',
                                    'samples'):
                db.execute(
                    '''
                    INSERT OR IGNORE INTO python_imports(project, import_name)
                    VALUES(?, ?);
                    ''',
                    [project, package_name],
                )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
