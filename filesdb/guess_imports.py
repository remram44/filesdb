from datetime import datetime
import logging
from pkg_resources import parse_version
import sqlalchemy
from sqlalchemy.sql import functions
import sys

from . import database
from .get_files import combine_versions


logger = logging.getLogger('filesdb.guess_imports')


def process_versions(project_name, versions):
    latest_version = max(versions, key=parse_version)

    with database.connect() as db:
        # See if it has been processed
        is_guessed, = db.execute(
            '''\
                SELECT
                    EXISTS (
                        SELECT project_name
                        FROM python_imports
                        WHERE project_name = :project
                            AND deduced_from_project_version = :version
                    ) AS is_guessed
            ''',
            {'project': project_name, 'version': latest_version},
        ).one()
        if is_guessed:
            logger.info("%r %s is guessed, skipping", project_name, latest_version)
            return

        # Find download
        download = db.execute(
            sqlalchemy.select([database.downloads.c.name])
            .where(database.downloads.c.project_name == project_name)
            .where(database.downloads.c.project_version == latest_version)
            .where(database.downloads.c.indexed == 'yes')
        ).fetchone()
        if download is None:
            logger.info("%r %s can't guess, no files", project_name, latest_version)
            return

        # List files
        files = db.execute(
            sqlalchemy.select([database.files.c.name])
            .where(database.files.c.download_name == download[0])
        ).fetchall()

        # Guess Python package name
        import_names = set()
        for filename, in files:
            if (
                filename.endswith('.py')
                and filename not in ('test.py', 'tests.py', 'setup.py')
            ):
                if '/' in filename:
                    import_name = filename.split('/', 1)[0]
                else:
                    import_name = filename[:-3]
                import_names.add(import_name)
        with db.begin():
            db.execute(
                database.python_imports.delete()
                .where(database.python_imports.c.project_name == project_name)
            )
            if import_names:
                logger.info(
                    "guess %r %s is import %r",
                    project_name, latest_version,
                    import_names,
                )
                db.execute(
                    database.python_imports.insert().values([
                        dict(
                            project_name=project_name,
                            deduced_from_project_version=latest_version,
                            deduced_from_download_name=download[0],
                            import_path=name,
                        )
                        for name in import_names
                    ])
                )
            else:
                logger.info(
                    "guess %r %s yielded nothing",
                    project_name, latest_version,
                )
                db.execute(
                    database.python_imports.insert().values(
                        project_name=project_name,
                        deduced_from_project_version=latest_version,
                        deduced_from_download_name=download[0],
                        import_path='',
                    )
                )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        if len(sys.argv) == 2:
            from_time = datetime.fromisoformat(sys.argv[2])
        elif len(sys.argv) == 1:
            from_time = None
        else:
            raise ValueError
    except ValueError:
        print(
            "Usage:\n  guess_imports.py\n"
            + "  guess_imports.py <since-isodate>",
            file=sys.stderr,
        )
        sys.exit(2)

    with database.connect() as db:
        # Count projects
        total_projects, = db.execute(
            sqlalchemy.select([functions.count()])
            .select_from(database.projects)
        ).one()

        query = '''\
            SELECT project_name, version
            FROM project_versions
            WHERE project_name IN (
                SELECT name FROM projects WHERE name > ? ORDER BY name LIMIT 20
            )
        '''
        done_projects = 0
        projects = db.execute(query, ['']).fetchall()
        while projects:
            logger.info("Got %d versions (%s - %s)", len(projects), projects[0][0], projects[-1][0])

            for project_name, versions in combine_versions(projects):
                process_versions(project_name, versions)
                done_projects += 1
                if done_projects % 100 == 0:
                    logger.info("%d / %d", done_projects, total_projects)

            # Get next batch
            projects = db.execute(query, [projects[-1][0]]).fetchall()


if __name__ == '__main__':
    main()
