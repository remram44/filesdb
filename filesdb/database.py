import contextlib
import logging
import os
import sqlalchemy
from sqlalchemy import Column, ForeignKey, MetaData, Table
from sqlalchemy.types import DateTime, Integer, String


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
    Column('seen', DateTime, nullable=True),
    # If set, the 'projects_versions' table was populated for this project
    Column('versions_retrieved_date', DateTime, nullable=True, index=True),
)


project_versions = Table(
    'project_versions',
    metadata,
    Column('project_name', String, ForeignKey('projects.name'), primary_key=True),
    Column('version', String, primary_key=True),
    # If set, the 'downloads' table was populated for this version
    Column('downloads_retrieved_date', DateTime, nullable=True, index=True),
)

downloads = Table(
    'downloads',
    metadata,
    Column('project_name', String, ForeignKey('projects.name'), index=True),
    Column('project_version', String, nullable=False, index=True),
    # Full name e.g. `reprozip-1.0.16-cp27-cp27m-manylinux2010_x86_64.whl`
    Column('name', String, primary_key=True),
    Column('size_bytes', Integer, nullable=False),
    # Download URL on files.pythonhosted.org
    Column('url', String, nullable=True),
    # Type as reported by API, 'bdist_wheel', 'sdist'
    Column('type', String, nullable=False, index=True),
    Column('hash_md5', String, nullable=False),
    Column('hash_sha256', String, nullable=False),
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

python_imports = Table(
    'python_imports',
    metadata,
    Column('project_name', String, ForeignKey('projects.name'), nullable=False, primary_key=True),
    Column('deduced_from_project_version', String, nullable=False),
    Column('deduced_from_download_name', String, ForeignKey('downloads.name'), nullable=False),
    Column('import', String, nullable=False, index=True),
)


@contextlib.contextmanager
def connect():
    engine = sqlalchemy.create_engine(os.environ['DATABASE_URL'])
    with engine.connect() as conn:
        if not engine.dialect.has_table(conn, projects.name):
            logger.warning("The tables don't seem to exist; creating")
            metadata.create_all(bind=engine)

        yield conn
