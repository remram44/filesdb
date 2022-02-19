import functools
import os
import re
import time
import traceback


def normalize_project_name(name):
    return name.replace('_', '-').lower()


_windows_device_files = ('CON', 'AUX', 'COM1', 'COM2', 'COM3', 'COM4', 'LPT1',
                         'LPT2', 'LPT3', 'PRN', 'NUL')
_not_ascii_re = re.compile(r'[^A-Za-z0-9_.-]')


def retry(retries, logger):
    def wrap(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for _ in range(retries - 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    logger.warning(
                        "Error running %s: %s",
                        func,
                        ''.join(
                            traceback.format_exception_only(type(e), e),
                        ).rstrip(),
                    )
                time.sleep(5)

            return await func(*args, **kwargs)

        return wrapper

    return wrap


def secure_filename(name):
    """Sanitize a filename.

    This takes a filename, for example provided by a browser with a file
    upload, and turn it into something that is safe for opening.

    Adapted from werkzeug's secure_filename(), copyright 2007 the Pallets team.
    https://palletsprojects.com/p/werkzeug/
    """
    is_windows = os.name == 'nt'

    if '/' in name:
        name = name[name.rindex('/') + 1:]
    if is_windows and '\\' in name:
        # It seems that IE gets that wrong, at least when the file is from
        # a network share
        name = name[name.rindex('\\') + 1:]
    name, ext = os.path.splitext(name)
    name = name[:20]
    name = _not_ascii_re.sub('', name).strip('._')
    if not name:
        name = '_'
    ext = _not_ascii_re.sub('', ext)
    if (
        is_windows
        and name.split('.')[0].upper() in _windows_device_files
    ):
        name = '_' + name
    name = name + ext
    return name
