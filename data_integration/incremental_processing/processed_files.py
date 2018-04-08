"""Functions for keeping track whether an input file has already been 'processed' """

import datetime

import mara_db.config
import mara_db.dbs
import mara_db.postgresql
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class ProcessedFile(Base):
    """A local file that has been 'processed' (e.g. has been read)"""
    __tablename__ = 'data_integration_processed_file'

    node_path = sqlalchemy.Column(sqlalchemy.ARRAY(sqlalchemy.Text), primary_key=True)
    file_name = sqlalchemy.Column(sqlalchemy.Text, primary_key=True)
    timestamp = sqlalchemy.Column(sqlalchemy.TIMESTAMP)


def track_processed_file(node_path: str, file_name: str):
    """
    Records that a file has been 'processed' by a node

    Args:
        node_path: The path of the node that processed the file
        file_name: The name of the file that has been processed

    Returns: True
    """
    with mara_db.postgresql.postgres_cursor_context('mara') as cursor:
        cursor.execute(f'''
INSERT INTO data_integration_processed_file (node_path, file_name, timestamp) 
VALUES ({'%s,%s,%s'})
ON CONFLICT (node_path, file_name) 
DO UPDATE SET timestamp = EXCLUDED.timestamp
''', (node_path, file_name, datetime.datetime.utcnow()))
    return True


def already_processed_files(node_path: str) -> [str]:
    """
    Returns all files that already have been processed by a node
    Args:
        node_path: The path of the node that processed the file

    Returns:

    """
    with mara_db.postgresql.postgres_cursor_context('mara') as cursor:
        cursor.execute(f"SELECT file_name FROM data_integration_processed_file WHERE node_path = {'%s'}", (node_path,))
        return [row[0] for row in cursor.fetchall()]
