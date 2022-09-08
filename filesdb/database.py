import contextlib
import logging
import os
import sqlalchemy
from sqlalchemy import Column, ForeignKey, MetaData, Table
import sqlalchemy.dialects.postgresql
import sqlalchemy.dialects.sqlite
import sqlalchemy.event
from sqlalchemy.types import BLOB, DateTime, Integer, String


logger = logging.getLogger(__name__)


metadata = MetaData(naming_convention={
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
})


projects = Table(
    'projects',
    metadata,
    Column('name', String, primary_key=True),
)

project_versions = Table(
    'project_versions',
    metadata,
    Column('project_name', String, ForeignKey('projects.name'), primary_key=True),
    Column('version', String, primary_key=True),
)

downloads = Table(
    'downloads',
    metadata,
    Column('project_name', String, ForeignKey('projects.name'), index=True),
    Column('project_version', String, nullable=False, index=True),
    # Full name e.g. `reprozip-1.0.16-cp27-cp27m-manylinux2010_x86_64.whl`
    Column('name', String, primary_key=True),
    Column('size_bytes', Integer, nullable=False),
    Column('upload_time', DateTime, nullable=False),
    # Download URL on files.pythonhosted.org
    Column('url', String, nullable=True),
    # Type as reported by API, 'bdist_wheel', 'sdist'
    Column('type', String, nullable=False, index=True),
    Column('python_version', String, nullable=True),
    Column('hash_md5', String, nullable=False),
    Column('hash_sha256', String, nullable=False),
    # NULL: not indexed
    # 'yes': indexed
    # otherwise: error code
    Column('indexed', String, nullable=True, index=True),
    Column('wheel_metadata', BLOB, nullable=True)
)

files = Table(
    'files',
    metadata,
    Column('download_name', String, ForeignKey('downloads.name'), primary_key=True),
    Column('name', String, primary_key=True, index=True),
    Column('size_bytes', Integer, nullable=False),
    Column('hash_sha1', String, nullable=False, index=True),
    Column('hash_sha256', String, nullable=False, index=True),
)

wheel_metadata_fields = Table(
    'wheel_metadata_fields',
    metadata,
    Column('download_name', String, ForeignKey('downloads.name')),
    Column('key', String, nullable=False, index=True),
    Column('value', String, nullable=False),
)

python_imports = Table(
    'python_imports',
    metadata,
    Column('project_name', String, ForeignKey('projects.name'), nullable=False, primary_key=True),
    Column('deduced_from_project_version', String, nullable=False),
    Column('deduced_from_download_name', String, ForeignKey('downloads.name'), nullable=False),
    # This should be NULL when we couldn't guess, but many DBMS don't allow
    # nullable columns in primary key, so we use empty string
    Column('import_path', String, nullable=False, primary_key=True),
)


def insert_or_ignore(table):
    if os.environ['DATABASE_URL'].startswith('sqlite:'):
        return sqlalchemy.dialects.sqlite.insert(table).on_conflict_do_nothing()
    elif os.environ['DATABASE_URL'].startswith('postgresql:'):
        return sqlalchemy.dialects.postgresql.insert(table).on_conflict_do_nothing()
    else:
        raise ValueError("Don't know how to do INSERT OR IGNORE on this database")


def make_engine():
    engine = sqlalchemy.create_engine(os.environ['DATABASE_URL'])

    if os.environ['DATABASE_URL'].startswith('sqlite:'):
        @sqlalchemy.event.listens_for(engine, 'connect')
        def sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute('PRAGMA foreign_keys=ON')
            cursor.close()

    with engine.connect() as conn:
        if not engine.dialect.has_table(conn, projects.name):
            logger.warning("The tables don't seem to exist; creating")
            metadata.create_all(bind=engine)

    return engine


_engine = None


@contextlib.contextmanager
def connect():
    global _engine

    if _engine is None:
        _engine = make_engine()

    with _engine.connect() as conn:
        yield conn
