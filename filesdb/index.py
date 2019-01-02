from hashlib import sha1
import logging
import os
import re
import requests
import shutil
import sqlite3
import tarfile
import tempfile
import zipfile


logger = logging.getLogger(__name__)


re_project = re.compile(r'<a href="\/simple\/([^"]+)\/">([^<]+)</a>')


schema = [
    '''
    CREATE TABLE files(
        project VARCHAR(100),
        version VARCHAR(50),
        filename VARCHAR(255),
        sha1 VARCHAR(41)
    );
    ''',
    '''CREATE INDEX files-idx-project ON files(project);''',
    '''CREATE INDEX files-idx-filename ON files(filename);''',
    '''CREATE INDEX files-idx-sha1 ON files(sha1);''',
    '''
    CREATE TABLE python_imports(
        project VARCHAR(100) PRIMARY KEY,
        version VARCHAR(50),
        import_name VARCHAR(50)
    );
    ''',
    '''
    CREATE INDEX python_imports-idx-import_name
    ON python_imports(import_name);
    ''',
]


def main():
    db_exists = os.path.exists('projects.sqlite3')
    db = sqlite3.connect('projects.sqlite3')
    db.isolation_level = 'EXCLUSIVE'
    if not db_exists:
        for statement in schema:
            db.execute(statement)

    page = requests.get('https://pypi.org/simple/')
    page.raise_for_status()
    page = page.text
    for m in re_project.finditer(page):
        name = m.group(2)
        link = m.group(1)

        logger.info("Processing %s", name)

        json_info = requests.get('https://pypi.org/pypi/{}/json'.format(link))
        if json_info.status_code == 404:
            logger.warning("JSON 404")
            continue
        json_info.raise_for_status()
        json_info = json_info.json()

        releases = sorted(json_info['releases'].items(),
                          key=lambda p: p[0],
                          reverse=True)
        if not releases or not releases[0][1]:
            logger.warning("Project %s has no releases", name)
            continue

        version, release_files = releases[0]

        # Check if project is up to date
        cur = db.execute(
            '''
            SELECT version FROM files
            WHERE project=?;
            ''',
            [name],
        )
        try:
            version_in_db = next(cur)[0]
            cur.close()
        except StopIteration:
            cur.close()
        else:
            if version_in_db == version:
                logger.info("Project %s is up to date", name)
                continue

        # Prefer a wheel
        for release_file in release_files:
            if release_file['packagetype'] == 'bdist_wheel':
                break
        else:
            # Else, sdist
            for release_file in release_files:
                if release_file['packagetype'] == 'sdist':
                    break
            else:
                logger.error("Couldn't find a suitable file!")
                continue

        logger.info("Getting %s", release_file['url'])
        tmpdir = tempfile.mkdtemp()
        try:
            with requests.get(release_file['url'], stream=True) as download:
                download.raise_for_status()
                tmpfile = os.path.join(
                    tmpdir,
                    release_file['filename'].replace('/', '-'),
                )
                with open(tmpfile, 'wb') as fp:
                    for chunk in download.iter_content(4096):
                        if chunk:
                            fp.write(chunk)
                process_archive(db, name, version, tmpfile)
        finally:
            shutil.rmtree(tmpdir)

    db.commit()
    db.close()


def process_archive(db, project, version, filename):
    try:
        if filename.endswith('.whl'):
            with zipfile.ZipFile(filename) as zip:
                for member in zip.namelist():
                    if ('.dist-info/' in member or
                            member.endswith('.dist-info')):
                        continue
                    with zip.open(member) as fp:
                        record(db, project, version, member, fp)
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
                            member.filename == 'PKG-INFO' or
                            member.filename == 'MANIFEST.in' or
                            member.filename == 'setup.cfg'):
                        continue
                    try:
                        idx = member.filename.index('/')
                    except ValueError:
                        logger.error("File %s from project %s doesn't have "
                                     "the expected top-level directory",
                                     member.filename, project)
                        return
                    member_name = member.filename[idx + 1:]
                    with zip.open(member) as fp:
                        record(db, project, version, member_name, fp)
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
                            member.name == 'MANIFEST.in' or
                            member.name == 'setup.cfg'):
                        continue
                    try:
                        idx = member.name.index('/')
                    except ValueError:
                        logger.error("File %s from project %s doesn't have "
                                     "the expected top-level directory",
                                     member.name, project)
                        return
                    member_name = member.name[idx + 1:]
                    with tar.extractfile(member) as fp:
                        record(db, project, version, member_name, fp)
    except (tarfile.TarError, zipfile.BadZipFile):
        logger.error("Error reading %s as an archive", filename)


def record(db, project, version, filename, fp):
    # Compute hash
    h = sha1()
    chunk = fp.read(4096)
    while len(chunk) == 4096:
        h.update(chunk)
        chunk = fp.read(4096)
    if chunk:
        h.update(chunk)

    # Remove old versions from database
    db.execute(
        '''
        DELETE FROM files
        WHERE project=?;
        ''',
        [project],
    )
    db.execute(
        '''
        DELETE FROM python_imports
        WHERE project=?;
        ''',
        [project],
    )

    # Insert file into database
    db.execute(
        '''
        INSERT INTO files(project, version, filename, sha1)
        VALUES(?, ?, ?, ?);
        ''',
        [project, version, filename, h.hexdigest()],
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
            INSERT OR IGNORE INTO python_imports(project, version, import_name)
            VALUES(?, ?, ?);
            ''',
            [project, version, package_name],
        )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
