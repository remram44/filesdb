"""Get versions of projects from the API.
"""

import aiohttp
import asyncio
from datetime import datetime
import itertools
import logging
from pkg_resources import parse_version
import sqlalchemy

from . import database


PROJECT_CHUNK_SIZE = 500

CONCURRENT_REQUESTS = 5


logger = logging.getLogger('filesdb.get_versions')


class Stats(object):
    successful = 0
    total = 0

    @classmethod
    def success(cls):
        cls.successful += 1
        cls.total += 1
        cls._maybe_print()

    @classmethod
    def failure(cls):
        cls.total += 1
        cls._maybe_print()

    @classmethod
    def _maybe_print(cls):
        if cls.total % 50 == 0:
            cls.print()

    @classmethod
    def print(cls):
        logger.info("successful = %d / total = %d", cls.successful, cls.total)


async def get_versions(db, http_session, projects):
    projects = iter(projects)

    # Start N tasks
    tasks = {
        asyncio.ensure_future(get_version(db, http_session, name))
        for name, in itertools.islice(projects, CONCURRENT_REQUESTS)
    }

    while tasks:
        # Wait for any task to complete
        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Poll them
        for task in done:
            tasks.discard(task)
            success = task.result()
            if success:
                Stats.success()
            else:
                Stats.failure()

        # Schedule new tasks
        for name, in itertools.islice(projects, CONCURRENT_REQUESTS - len(tasks)):
            tasks.add(asyncio.ensure_future(get_version(db, http_session, name)))


async def get_version(db, http_session, project_name):
    async with http_session.get('https://pypi.org/pypi/%s/json' % project_name) as response:
        if response.status == 404:
            logger.warning("Removing project on 404: %r", project_name)
            query = (
                database.projects.delete()
                .where(database.projects.c.name == project_name)
            )
            db.execute(query)
            return False
        elif response.status != 200:
            logger.warning("Can't list versions: (%s): %r", response.status, project_name)
            return False

        obj = await response.json()

    versions = obj['releases'].keys()

    with db.begin():
        # List of versions
        query = (
            database.project_versions.insert()
            # FIXME on SQLAlchemy 1.4 update (this is SQLite3 only)
            .prefix_with('OR IGNORE')
            .values([
                {'project_name': project_name, 'version': number}
                for number in versions
            ])
        )
        db.execute(query)

        # Note that this project has up-to-date versions
        query = (
            database.projects.update()
            .where(database.projects.c.name == project_name)
            .values(versions_retrieved_date=datetime.utcnow())
        )
        db.execute(query)

    # Get downloads for latest version
    latest_version = max(versions, key=parse_version)

    with db.begin():
        # List of downloads
        assert all(
            isinstance(download['filename'], str) and download['filename']
            and isinstance(download['size'], int)
            and isinstance(download['url'], str) and download['url']
            and isinstance(download['packagetype'], str) and download['packagetype']
            and isinstance(download['digests']['md5'], str) and download['digests']['md5']
            and isinstance(download['digests']['sha256'], str) and download['digests']['sha256']
            for download in obj['releases'][latest_version]
        )
        query = (
            database.downloads.insert()
            # FIXME on SQLAlchemy 1.4 update (this is SQLite3 only)
            .prefix_with('OR IGNORE')
            .values([
                {
                    'project_name': project_name,
                    'project_version': latest_version,
                    'name': download['filename'],
                    'size_bytes': download['size'],
                    'url': download['url'],
                    'type': download['packagetype'],
                    'python_version': download['python_version'],
                    'hash_md5': download['digests']['md5'],
                    'hash_sha256': download['digests']['sha256'],
                }
                for download in obj['releases'][latest_version]
            ])
        )
        db.execute(query)

        # Note that this version has up-to-date downloads
        query = (
            database.project_versions.update()
            .where(database.project_versions.c.project_name == project_name)
            .where(database.project_versions.c.version == latest_version)
            .values(downloads_retrieved_date=datetime.utcnow())
        )
        db.execute(query)

    return True


async def amain():
    with database.connect() as db:
        async with aiohttp.ClientSession() as http_session:
            # List projects without versions
            query = (
                sqlalchemy.select([database.projects.c.name])
                .where(database.projects.c.versions_retrieved_date == None)
                .order_by(database.projects.c.name)
                .limit(PROJECT_CHUNK_SIZE)
            )
            projects = db.execute(query).fetchall()
            while True:
                logger.info("Got %d projects (%s - %s)", len(projects), projects[0][0], projects[-1][0])

                await get_versions(db, http_session, projects)

                if len(projects) < PROJECT_CHUNK_SIZE:
                    break

                # Get next batch
                projects = db.execute(
                    query.where(database.projects.c.name > projects[-1][0])
                ).fetchall()


def main():
    logging.basicConfig(level=logging.INFO)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(amain())


if __name__ == '__main__':
    main()
