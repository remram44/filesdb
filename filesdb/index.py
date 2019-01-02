from hashlib import sha1
import logging
import os
import re
import requests
import shutil
import tarfile
import tempfile
import zipfile


logger = logging.getLogger(__name__)


re_project = re.compile(r'<a href="\/simple\/([^"]+)\/">([^<]+)</a>')


def main():
    page = requests.get('https://pypi.org/simple/')
    page.raise_for_status()
    page = page.text
    for m in re_project.finditer(page):
        name = m.group(2)
        link = m.group(1)

        if link != 'usagestats':
            continue

        logger.info("Processing %s", name)

        json_info = requests.get('https://pypi.org/pypi/{}/json'.format(link))
        json_info.raise_for_status()
        json_info = json_info.json()

        releases = sorted(json_info['releases'].items(),
                          key=lambda p: p[0],
                          reverse=True)
        if not releases:
            logger.warning("Package %s has no releases", name)
            continue

        version, release_files = releases[0]

        # Prefer a wheel
        for release_file in release_files:
            if release_file['packagetype'] == 'bdist_wheel':
                break
        else:
            release_file = release_files[0]
            assert release_file['packagetype'] == 'sdist'

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
                process_archive(name, version, tmpfile)
        finally:
            shutil.rmtree(tmpdir)


def process_archive(project, version, filename):
    if filename.endswith('.whl'):
        with zipfile.ZipFile(filename) as zip:
            for member in zip.namelist():
                if '.dist-info/' in member or member.endswith('.dist-info'):
                    continue
                with zip.open(member) as fp:
                    record(project, version, member, fp)
    else:
        with tarfile.open(filename, 'r:*') as tar:
            for member in tar.getmembers():
                if not member.isfile():
                    continue
                assert member.name.startswith(project)
                if ('.egg-info/' in member.name or
                        member.name.endswith('.egg-info') or
                        member.name == 'PKG-INFO' or
                        member.name == 'MANIFEST.in' or
                        member.name == 'setup.cfg'):
                    continue
                try:
                    idx = member.name.index('/')
                except ValueError:
                    logger.error("File %s from project %s doesn't have the "
                                 "expected top-level directory",
                                 member.name, project)
                    return
                member_name = member.name[idx + 1:]
                with tar.extractfile(member) as fp:
                    record(project, version, member_name, fp)


def record(project, version, filename, fp):
    h = sha1()
    chunk = fp.read(4096)
    while len(chunk) == 4096:
        h.update(chunk)
        chunk = fp.read(4096)
    if chunk:
        h.update(chunk)
    print(project, version, filename, h.hexdigest())


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
