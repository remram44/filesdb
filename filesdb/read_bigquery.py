"""Get projects, versions, and downloads from BigQuery data.

The most reliable source of information for PyPI is now on Google BigQuery. It
is the only way to get recent updates, bulk data, and some fields like download
counts.

This script allows you to import a CSV exported from BigQuery into the
database, or read from BigQuery directly over the API.

Use this SQL query on BigQuery:

    SELECT
        name, version,
        upload_time, filename, size,
        path,
        python_version, packagetype,
        md5_digest, sha256_digest
    FROM `bigquery-public-data.pypi.distribution_metadata`
    WHERE upload_time > :last_upload_time
    ORDER BY upload_time ASC
"""

import csv
from datetime import datetime
import logging
import os
import re
import sys

from . import database
from .utils import normalize_project_name


logger = logging.getLogger('filesdb.read_bigquery')


class BatchInserter(object):
    BATCH_SIZE = 500

    def __init__(self, db, query, dependencies=()):
        self.db = db
        self.query = query
        self.values = []
        self.dependencies = dependencies

    def insert(self, **kwargs):
        self.values.append(kwargs)
        if len(self.values) > self.BATCH_SIZE:
            values = self.values[:self.BATCH_SIZE]
            self.values = self.values[self.BATCH_SIZE:]

            for dep in self.dependencies:
                dep.flush()
            self.db.execute(self.query.values(values))

    def flush(self):
        if self.values:
            for dep in self.dependencies:
                dep.flush()

            self.db.execute(self.query.values(self.values))
            self.values = []


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if len(sys.argv) == 3 and sys.argv[1] == 'csv':
        filename = sys.argv[2]
        if not os.path.isfile(sys.argv[2]):
            print("Usage: read_bigquery.py <exported-table.csv>",
                  file=sys.stderr)
            sys.exit(2)

        with open(filename, 'r') as fp:
            total_rows = sum(1 for _ in fp) - 1

        with open(filename, 'r') as fp:
            reader = csv.DictReader(fp)

            header = reader.fieldnames
            assert header == [
                'name', 'version',
                'upload_time', 'filename', 'size',
                'path',
                'python_version', 'packagetype',
                'md5_digest', 'sha256_digest',
            ]

            read_data(reader, total_rows)
    elif (
        len(sys.argv) == 3
        and sys.argv[1] == 'query'
        and os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    ):
        from_time = datetime.fromisoformat(sys.argv[2])

        from google.cloud import bigquery

        client = bigquery.Client()
        query = '''\
            SELECT
                name, version,
                upload_time, filename, size,
                path,
                python_version, packagetype,
                md5_digest, sha256_digest
            FROM `bigquery-public-data.pypi.distribution_metadata`
            WHERE upload_time > "{time}"
            ORDER BY upload_time ASC
        '''.format(
            time=from_time.strftime('%Y-%m-%d %H:%M:%S')
        )
        job = client.query(query)
        total_rows = sum(1 for _ in job.result())
        iterator = job.result()
        read_data(iterator, total_rows)
    else:
        print(
            "Usage:\n  read_bigquery.py csv <exported-table.csv>\n"
            + "  GOOGLE_APPLICATION_CREDENTIALS=account.json "
            + "read_bigquery.py query <isodate>",
            file=sys.stderr,
        )
        sys.exit(2)


def read_data(iterator, total_rows):
    with database.connect() as db:
        projects = BatchInserter(
            db,
            database.insert_or_ignore(database.projects),
        )
        versions = BatchInserter(
            db,
            database.insert_or_ignore(database.project_versions),
            [projects],
        )
        downloads = BatchInserter(
            db,
            database.insert_or_ignore(database.downloads),
            [projects],
        )
        for i, row in enumerate(iterator):
            if i % 10000 == 0:
                logger.info("%d / %d", i, total_rows)

            if row['path']:
                url = 'https://files.pythonhosted.org/packages/' + row['path']
            else:
                url = None

            timestamp = row['upload_time']
            # datetime if coming from BigQuery, str if coming from CSV
            if not isinstance(timestamp, datetime):
                timestamp = re.sub(
                    r'^(20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]) ([0-9][0-9]:[0-9][0-9]:[0-9][0-9])(?:\.[0-9]*)? UTC$',
                    r'\1T\2',
                    timestamp,
                )
                timestamp = datetime.fromisoformat(timestamp)

            name = normalize_project_name(row['name'])

            projects.insert(
                name=name,
            )
            versions.insert(
                project_name=name,
                version=row['version'],
            )
            downloads.insert(
                project_name=name,
                project_version=row['version'],
                name=row['filename'],
                size_bytes=int(row['size']),
                upload_time=timestamp,
                url=url,
                type=row['packagetype'],
                python_version=row['python_version'],
                hash_md5=row['md5_digest'],
                hash_sha256=row['sha256_digest'],
            )

        projects.flush()
        versions.flush()
        downloads.flush()


if __name__ == '__main__':
    main()
