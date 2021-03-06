from flask import Flask, jsonify
import sqlite3

from werkzeug.utils import redirect

app = Flask('filesdb')


db = sqlite3.connect('projects.sqlite3', check_same_thread=False)

nb_projects = db.execute(
    '''
    SELECT count(project)
    FROM projects;
    '''
)
nb_projects = next(nb_projects)[0]


nb_files = db.execute(
    '''
    SELECT count(*)
    FROM files;
    '''
)
nb_files = next(nb_files)[0]


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
        'repository': 'pypi',
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
            'repository': 'pypi',
            'files': [],
        })['files']
        files.append({'filename': filename, 'sha1': sha1})
    cursor.close()

    return jsonify(list(projects.values()))


@app.route('/file/<path:file_prefix>')
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


@app.route('/')
def index():
    return '''\
<!DOCTYPE html>
<html>
  <head>
    <title>FilesDB</title>
  </head>
  <body>
    <pre>File database for package managers

Available endpoints:

* /project/<project_name>: List files in the given project
* /file/<file_prefix>: List files matching a given prefix, and the projects
  they come from
* /sha1/<sha1_hash>: List files that have the given SHA1 hash, and the projects
  they come from

There are {projects} projects and {files} files in the database.</pre>

    <a href="https://github.com/ViDA-NYU/filesdb">
      <pre>https://github.com/ViDA-NYU/filesdb</pre>
    </a>
  </body>
</html>
'''.format(projects=nb_projects, files=nb_files)
