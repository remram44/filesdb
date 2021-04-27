"""Get all projects from the API.
"""

import aiohttp
import asyncio
from datetime import datetime
import logging
import re

from . import database
from .utils import iwindows


INSERT_CHUNK_SIZE = 500


logger = logging.getLogger('filesdb.get_projects')


re_project = re.compile(r'<a href="/simple/([^"]+)/">([^<]+)</a>')


async def amain():
    with database.connect() as db:
        async with aiohttp.ClientSession() as session:
            async with session.get('https://pypi.org/simple/') as response:
                if response.status != 200:
                    raise ValueError("Server returned %s" % response.status)
                html = await response.text()
                matches = re_project.finditer(html)

                now = datetime.utcnow()

                total = 0

                # Insert projects
                for chunk in iwindows(matches, INSERT_CHUNK_SIZE):
                    total += len(chunk)

                    query = (
                        database.projects.insert()
                        # FIXME on SQLAlchemy 1.4 update (this is SQLite3 only)
                        .prefix_with('OR IGNORE')
                        .values([
                            {'name': m.group(1), 'seen': now}
                            for m in chunk
                        ])
                    )
                    db.execute(query)

                logger.info("Inserted %d projects", total)


def main():
    logging.basicConfig(level=logging.INFO)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(amain())


if __name__ == '__main__':
    main()
