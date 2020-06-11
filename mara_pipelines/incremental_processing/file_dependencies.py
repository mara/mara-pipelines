"""Functions for keeping track of whether a list of files changed since the last pipeline run"""
import datetime
import hashlib
import pathlib

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base

import mara_db.postgresql
from .. import config

Base = declarative_base()


class FileDependency(Base):
    """A combined hash of the content of a list of files"""
    __tablename__ = 'data_integration_file_dependency'

    node_path = sqlalchemy.Column(sqlalchemy.ARRAY(sqlalchemy.TEXT), primary_key=True)
    dependency_type = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
    hash = sqlalchemy.Column(sqlalchemy.String)
    timestamp = sqlalchemy.Column(sqlalchemy.TIMESTAMP(timezone=True))


def update(node_path: [str], dependency_type: str, pipeline_base_path: str, file_dependencies: [str]):
    """
    Stores the combined hash of a list of files

    Args:
        node_path: The path of the node that depends on the files
        dependency_type: An arbitrary string that allows to distinguish between multiple dependencies of a node
        pipeline_base_path: The base directory of the pipeline
        file_dependencies: A list of file names relative to pipeline_base_path
    """
    with mara_db.postgresql.postgres_cursor_context('mara') as cursor:
        cursor.execute(f"""
INSERT INTO data_integration_file_dependency (node_path, dependency_type, hash, timestamp)
VALUES ({'%s,%s,%s,%s'})
ON CONFLICT (node_path, dependency_type)
DO UPDATE SET timestamp = EXCLUDED.timestamp, hash = EXCLUDED.hash
    """, (node_path, dependency_type, hash(pipeline_base_path, file_dependencies), datetime.datetime.utcnow()))

def delete(node_path: [str], dependency_type: str):
    """
    Delets the combined hash of a list of files for that node and dependency type

    Args:
        node_path: The path of the node that depends on the files
        dependency_type: An arbitrary string that allows to distinguish between multiple dependencies of a node
    """
    with mara_db.postgresql.postgres_cursor_context('mara') as cursor:
        cursor.execute(f"""
DELETE FROM data_integration_file_dependency
WHERE node_path = {'%s'} AND dependency_type = {'%s'}
    """, (node_path, dependency_type))


def is_modified(node_path: [str], dependency_type: str, pipeline_base_path: str, file_dependencies: [str]):
    """
    Checks whether a list of files have been modified since the last pipeline run

    Args:
        node_path: The path of the node that depends on the files
        dependency_type: An arbitrary string that allows to distinguish between multiple dependencies of a node
        pipeline_base_path: The base directory of the pipeline
        file_dependencies: A list of file names relative to pipeline_base_path

    Returns: True when at least one of the files was modified

    """
    with mara_db.postgresql.postgres_cursor_context('mara') as cursor:
        cursor.execute("""
SELECT TRUE
FROM data_integration_file_dependency
WHERE node_path=%s AND dependency_type=%s AND hash=%s """,
                       (node_path, dependency_type, hash(pipeline_base_path, file_dependencies)))
        return False if cursor.fetchone() else True


def hash(pipeline_base_path: pathlib.Path, file_dependencies: [str]) -> str:
    """
    Creates a combined hash of the content of a list of files

    Args:
        pipeline_base_path: The base directory of the pipeline
        file_dependencies: A list of file names relative to pipeline_base_path

    Returns: a combined content hash
    """
    hash = str(config.first_date()) + ' ' + str(config.last_date())
    for file_dependency in file_dependencies:
        hash += ' ' + hashlib.md5((pipeline_base_path / pathlib.Path(file_dependency)).read_text().encode()).hexdigest()
    return hash
