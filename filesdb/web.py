from flask import Flask, Response, jsonify, redirect, render_template, url_for
import logging
from pkg_resources import parse_version
import sqlalchemy
from sqlalchemy.sql import functions
import threading

from . import database
from .utils import normalize_project_name


logger = logging.getLogger('filesdb.web')

app = Flask('filesdb')


@app.route('/pypi/<project_name>')
def pypi_project(project_name):
    project_name = normalize_project_name(project_name)
    with database.connect() as db:
        # Get versions
        versions = db.execute(
            sqlalchemy.select([
                database.project_versions.c.version,
            ])
            .where(database.project_versions.c.project_name == project_name)
        ).fetchall()

        if not versions:
            return jsonify({'error': "No such project"}), 404

        return jsonify({
            'project': project_name,
            'versions': [row[0] for row in versions],
        })


@app.route('/pypi/<project_name>/latest')
def pypi_version_latest(project_name):
    project_name = normalize_project_name(project_name)
    with database.connect() as db:
        # Get versions
        versions = db.execute(
            sqlalchemy.select([
                database.project_versions.c.version,
            ])
            .where(database.project_versions.c.project_name == project_name)
        ).fetchall()

        if not versions:
            return jsonify({'error': "No such project"}), 404

        latest_version = max((v[0] for v in versions), key=parse_version)
        return redirect(
            url_for('.pypi_version', project_name=project_name, version=latest_version),
            302,
        )


@app.route('/pypi/<project_name>/files')
def pypi_version_files(project_name):
    project_name = normalize_project_name(project_name)
    with database.connect() as db:
        # Get versions
        versions = db.execute(
            sqlalchemy.select([
                database.project_versions.c.version,
            ])
            .where(database.project_versions.c.project_name == project_name)
        ).fetchall()

        if not versions:
            return jsonify({'error': "No such project"}), 404

        latest_version = max((v[0] for v in versions), key=parse_version)

        # Find an indexed download
        download = db.execute(
            sqlalchemy.select([database.downloads.c.name])
            .where(database.downloads.c.project_name == project_name)
            .where(database.downloads.c.project_version == latest_version)
            .where(database.downloads.c.indexed == 'yes')
        ).fetchone()
        if download is None:
            return jsonify({'error': "No indexed download"}), 503

        return redirect(
            url_for(
                '.pypi_download',
                project_name=project_name,
                version=latest_version,
                filename=download[0],
            ),
            302,
        )


@app.route('/pypi/<project_name>/<version>')
def pypi_version(project_name, version):
    project_name = normalize_project_name(project_name)
    with database.connect() as db:
        # Get downloads
        downloads = db.execute(
            sqlalchemy.select([
                database.downloads.c.name,
                database.downloads.c.size_bytes,
                database.downloads.c.upload_time,
                database.downloads.c.url,
                database.downloads.c.type,
                database.downloads.c.python_version,
                database.downloads.c.hash_md5,
                database.downloads.c.hash_sha256,
                database.downloads.c.indexed,
            ])
            .where(database.downloads.c.project_name == project_name)
            .where(database.downloads.c.project_version == version)
        ).fetchall()

        if not downloads:
            project = db.execute(
                sqlalchemy.select([database.projects.c.name])
                .where(database.projects.c.name == project_name)
            ).fetchone()
            if project is None:
                return jsonify({'error': "No such project"}), 404
            else:
                return jsonify({'error': "No such version"}), 404

        return jsonify({
            'downloads': [
                {
                    'name': row['name'],
                    'size_bytes': row['size_bytes'],
                    'upload_time': row['upload_time'].isoformat(),
                    'url': row['url'],
                    'type': row['type'],
                    'python_version': row['python_version'],
                    'hash_md5': row['hash_md5'],
                    'hash_sha256': row['hash_sha256'],
                    'indexed': (
                        False if row['indexed'] is None else
                        True if row['indexed'] == 'yes' else
                        {'error': row['indexed']}
                    ),
                }
                for row in downloads
            ],
        })


class GetDownloadError(KeyError):
    pass


def get_download(db, project_name, version, filename, columns=[]):
    project_name = normalize_project_name(project_name)

    # Get download
    download = db.execute(
        sqlalchemy.select([database.downloads.c.indexed] + columns)
        .where(database.downloads.c.name == filename)
        .where(database.downloads.c.project_name == project_name)
        .where(database.downloads.c.project_version == version)
    ).fetchone()
    if download is None:
        project = db.execute(
            sqlalchemy.select([database.projects.c.name])
            .where(database.projects.c.name == project_name)
        ).fetchone()
        if project is None:
            raise GetDownloadError("No such project")
        else:
            version = db.execute(
                sqlalchemy.select([database.project_versions.c.version])
                .where(database.project_versions.c.project_name == project_name)
                .where(database.project_versions.c.version == version)
            ).fetchone()
            if version is None:
                raise GetDownloadError("No such version")
            else:
                raise GetDownloadError("No such download")

    if download[0] is None:
        return jsonify({'error': "This download is not yet indexed"}), 404
    elif download[0] != 'yes':
        return jsonify({'error': download[0]})

    return download[1:]


@app.route('/pypi/<project_name>/<version>/<filename>')
def pypi_download(project_name, version, filename):
    with database.connect() as db:
        try:
            get_download(db, project_name, version, filename)
        except GetDownloadError as e:
            return jsonify({'error': e.args[0]}), 404

        # Get files
        files = db.execute(
            sqlalchemy.select([
                database.files.c.name,
                database.files.c.size_bytes,
                database.files.c.hash_sha1,
                database.files.c.hash_sha256,
            ])
            .where(database.files.c.download_name == filename)
        ).fetchall()

        return jsonify({
            'files': [
                {
                    'name': row['name'],
                    'size_bytes': row['size_bytes'],
                    'hash_sha1': row['hash_sha1'],
                    'hash_sha256': row['hash_sha256'],
                }
                for row in files
            ],
        })


@app.route('/pypi/<project_name>/<version>/<filename>/wheel_metadata')
def wheel_metadata(project_name, version, filename):
    with database.connect() as db:
        try:
            wheel_metadata, = get_download(
                db, project_name, version, filename,
                columns=[database.downloads.c.wheel_metadata],
            )
        except GetDownloadError as e:
            return jsonify({'error': e.args[0]}), 404

    return Response(wheel_metadata, content_type='text/plain')


@app.route('/files/<hash_function>/<digest>')
def file_hash(hash_function, digest):
    try:
        hash_column = {
            'sha1': database.files.c.hash_sha1,
            'sha256': database.files.c.hash_sha256,
        }[hash_function]
    except KeyError:
        return jsonify({'error': "No such hash function"}), 404

    with database.connect() as db:
        files = db.execute(
            sqlalchemy.select([
                database.files.c.download_name,
                database.files.c.name,
                database.files.c.size_bytes,
                database.files.c.hash_sha1,
                database.files.c.hash_sha256,
                database.downloads.c.project_name,
                database.downloads.c.project_version,
            ])
            .select_from(database.files.join(
                database.downloads,
                database.files.c.download_name == database.downloads.c.name,
            ))
            .where(hash_column == digest)
            .limit(100)
        ).fetchall()

        if not files:
            return jsonify({'files': []}), 404

        return jsonify({
            'files': [
                {
                    'download_name': row['download_name'],
                    'name': row['name'],
                    'size_bytes': row['size_bytes'],
                    'hash_sha1': row['hash_sha1'],
                    'hash_sha256': row['hash_sha256'],
                    'project_name': row['project_name'],
                    'project_version': row['project_version'],
                    'repository': 'pypi',
                }
                for row in files
            ],
        })


@app.route('/files/prefix/<path:file_prefix>')
def file(file_prefix):
    if len(file_prefix) <= 2:
        return jsonify({'error': "File prefix too short"}), 400

    with database.connect() as db:
        file_prefix_next = file_prefix[:-1] + chr(ord(file_prefix[-1]) + 1)

        files = db.execute(
            sqlalchemy.select([
                database.files.c.download_name,
                database.files.c.name,
                database.files.c.size_bytes,
                database.files.c.hash_sha1,
                database.files.c.hash_sha256,
                database.downloads.c.project_name,
                database.downloads.c.project_version,
            ])
            .select_from(database.files.join(
                database.downloads,
                database.files.c.download_name == database.downloads.c.name,
            ))
            .where(database.files.c.name >= file_prefix)
            .where(database.files.c.name < file_prefix_next)
            .limit(100)
        ).fetchall()

        return jsonify({
            'files': [
                {
                    'download_name': row['download_name'],
                    'name': row['name'],
                    'size_bytes': row['size_bytes'],
                    'hash_sha1': row['hash_sha1'],
                    'hash_sha256': row['hash_sha256'],
                    'project_name': row['project_name'],
                    'project_version': row['project_version'],
                    'repository': 'pypi',
                }
                for row in files
            ],
        })


@app.route('/python/import/<name>')
def python_import(name):
    with database.connect() as db:
        projects = db.execute(
            sqlalchemy.select([
                database.python_imports.c.project_name,
            ])
            .where(database.python_imports.c.import_path == name)
        ).fetchall()

    return jsonify({
        'projects': [
            {
                'name': project['project_name']
            }
            for project in projects
        ]
    })


_statistics = None


def _compute_statistics():
    global _statistics
    with database.connect() as db:
        projects, = db.execute(
            sqlalchemy.select(functions.count())
            .select_from(database.projects)
        ).one()
        downloads, = db.execute(
            sqlalchemy.select(functions.count())
            .select_from(database.downloads)
        ).one()
        downloads_indexed, = db.execute(
            sqlalchemy.select(functions.count())
            .select_from(database.downloads)
            .where(database.downloads.c.indexed == 'yes')
        ).one()
        files, = db.execute(
            sqlalchemy.select(functions.count())
            .select_from(database.files)
        ).one()

    _statistics = dict(
        projects=projects,
        downloads=downloads,
        downloads_indexed=downloads_indexed,
        files=files,
    )
    logger.info(
        "Statistics ready: %s",
        (
            "The database has {projects:,} projects, "
            + "{downloads:,} downloads, "
            + "{downloads_indexed:,} downloads contents, "
            + "{files:,} files."
        ).format(**_statistics)
    )


_statistics_thread = threading.Thread(target=_compute_statistics, daemon=True)
_statistics_thread.start()


@app.route('/')
def index():
    return render_template(
        'index.html',
        statistics=_statistics,
    )
