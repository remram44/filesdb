from flask import Flask, jsonify
import sqlite3

from werkzeug.utils import redirect

app = Flask('filesdb')


db = sqlite3.connect('projects.sqlite3', check_same_thread=False)


@app.route('/project/<project_name>')
def project(project_name):
    # Get project
    cursor = db.execute(
        '''
        SELECT project, version, archive FROM projects
        WHERE project=?;
        ''',
        [project_name],
    )
    try:
        project = next(cursor)
        cursor.close()
    except StopIteration:
        cursor.close()
        return jsonify({'error': "No such project"}), 404

    # Get files
    cursor = db.execute(
        '''
        SELECT filename, sha1 FROM files
        WHERE project=?;
        ''',
        [project_name],
    )
    files = list(cursor)
    cursor.close()

    return jsonify({
        'project': project[0],
        'version': project[1],
        'archive': project[2],
        'files': [
            {'filename': file[0], 'sha1': file[1]}
            for file in files
        ]
    })


def organize_files(cursor):
    projects = {}
    for project, version, archive, filename, sha1 in cursor:
        files = projects.setdefault(project, {
            'project': project,
            'version': version,
            'archive': archive,
            'files': [],
        })['files']
        files.append({'filename': filename, 'sha1': sha1})
    cursor.close()

    return jsonify(list(projects.values()))


@app.route('/file/<file_prefix>')
def file(file_prefix):
    if len(file_prefix) <= 2:
        return jsonify({'error': "File prefix too short"}), 400

    file_prefix = file_prefix.replace('[', '[[')
    file_prefix = file_prefix.replace('%', '[%]')
    file_prefix = file_prefix.replace("'", "''")

    # Find files
    cursor = db.execute(
        '''
        SELECT projects.project, projects.version, projects.archive,
            filename, sha1
        FROM files
        INNER JOIN projects ON files.project = projects.project
        WHERE filename LIKE '{}%';
        '''.format(file_prefix)
    )
    try:
        return organize_files(cursor)
    finally:
        cursor.close()


@app.route('/sha1/<sha1_hash>')
def sha1(sha1_hash):
    if sha1_hash != sha1_hash.lower():
        return redirect('/sha1/' + sha1_hash.lower(), 301)

    cursor = db.execute(
        '''
        SELECT projects.project, projects.version, projects.archive,
            filename, sha1
        FROM files
        INNER JOIN projects ON files.project = projects.project
        WHERE sha1 = ?;
        ''',
        [sha1_hash],
    )
    try:
        return organize_files(cursor)
    finally:
        cursor.close()
