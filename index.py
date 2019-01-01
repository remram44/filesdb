import re
import requests


re_project = re.compile(r'<a href="\/simple\/([^"]+)\/">([^<]+)</a>')
re_file = re.compile(r'<a href="(http[^"]+/'
                     r'[^/"]+'  # Distribution name
                     r'-([0-9][^/"-]*)'  # Version
                     r'(-[0-9][^/"-]*])?'  # Build tag
                     r'(?:-[^/"-]+)+'  # Compatibility tags
                     r'\.(?:whl|tar\.gz|tar\.bz2|tar\.xz)'
                     r'(#sha256=[0-9a-f]+)?'
                     r')">')


def main():
    page = requests.get('https://pypi.org/simple/')
    page.raise_for_status()
    page = page.text
    for m in re_project.finditer(page):
        name = m.group(2)
        link = m.group(1)

        if link != 'usagestats':
            continue

        download = requests.get('https://pypi.org/simple/{}/'.format(link))
        download.raise_for_status()
        download = download.text
        for m in re_file.finditer(download):
            file_link, version, build, sha = m.groups()


if __name__ == '__main__':
    main()
