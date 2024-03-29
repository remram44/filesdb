"""Guess the import names for Python packages, based on their files.
"""

from datetime import datetime
import logging
from pkg_resources import parse_version
import sqlalchemy
from sqlalchemy.sql import functions
import sys

from . import database
from .get_files import iter_project_versions


logger = logging.getLogger('filesdb.guess_imports')


def import_matches_project(project_name, import_name):
    # Start from the project name, possibly with a 'python-' prefix removed
    project_names = {project_name}
    if project_name.startswith('python-'):
        project_names.add(project_name[7:])

    # Replace - with _
    project_names = {name.replace('-', '_') for name in project_names}

    # TODO: Do something about namespace packages?

    return any(name == import_name for name in project_names)


def process_versions(project_name, versions, stats):
    latest_version = max(versions, key=parse_version)

    with database.connect() as db:
        # See if it has been processed
        prev_import_names = list(db.execute(
            '''\
                SELECT import_path
                FROM python_imports
                WHERE project_name = :project
                    AND deduced_from_project_version = :version
            ''',
            {'project': project_name, 'version': latest_version},
        ))
        if prev_import_names:
            logger.debug("%r %s is guessed, skipping", project_name, latest_version)
            if not any(
                import_matches_project(project_name, i[0])
                for i in prev_import_names
            ):
                stats[False] += 1
            elif len(prev_import_names) == 1:
                stats[True] += 1
            # If there's a match but there's other imports, don't count it
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

        if not any(
            import_matches_project(project_name, i)
            for i in import_names
        ):
            stats[False] += 1
        elif len(import_names) == 1:
            stats[True] += 1
        # If there's a match but there's other imports, don't count it

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

    # TODO: Only go over projects that have a download more recent than given date

    with database.connect() as db:
        # Count projects
        total_projects, = db.execute(
            sqlalchemy.select([functions.count()])
            .select_from(database.projects)
        ).one()

        # List versions
        done_projects = 0
        stats = {True: 0, False: 0}
        for project_name, versions in iter_project_versions(db):
            process_versions(project_name, versions, stats)
            done_projects += 1
            if done_projects % 100 == 0:
                logger.info("%d / %d", done_projects, total_projects)

        logger.info(
            "total: %d, name matching import: %d, not matching: %d",
            total_projects, stats[True], stats[False],
        )


if __name__ == '__main__':
    main()
