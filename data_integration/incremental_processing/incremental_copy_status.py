"""Tracks the last comparison value of an incremental copy"""

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base

import mara_db.config
import mara_db.dbs
import mara_db.postgresql

Base = declarative_base()


class IncrementalCopyStatus(Base):
    """The last `modification_value` for a table that is incrementally loaded"""
    __tablename__ = 'data_integration_incremental_copy_status'

    node_path = sqlalchemy.Column(sqlalchemy.ARRAY(sqlalchemy.Text), primary_key=True, index=True)
    source_table = sqlalchemy.Column(sqlalchemy.Text, primary_key=True)
    last_comparison_value = sqlalchemy.Column(sqlalchemy.Text)


def update(node_path: [str], source_db_alias: str, source_table: str, last_comparison_value):
    """
    Updates the last_comparison_value for a pipeline node and table
    Args:
        node_path: The path of the parent pipeline node
        source_db_alias: The alias of the the db from which data is copied
        source_table: The table from which is copied
        max_modification_value: The last retrieved modification value

    Returns:

    """
    with mara_db.postgresql.postgres_cursor_context('mara') as cursor:
        cursor.execute(f'''
INSERT INTO data_integration_incremental_copy_status (node_path, source_table, last_comparison_value) 
VALUES ({'%s,%s,%s'})
ON CONFLICT (node_path, source_table) 
DO UPDATE SET last_comparison_value = EXCLUDED.last_comparison_value
''', (node_path, f'{source_db_alias}.{source_table}', last_comparison_value))


def get_last_comparison_value(node_path: [str], source_db_alias: str, source_table: str):
    """
    Returns the last comparison value for a pipeline node and table
    Args:
        node_path: The path of the parent pipeline node
        source_db_alias: The alias of the the db from which data is copied
        source_table: The table from which is copied

    Returns:
        The value or None
    """
    with mara_db.postgresql.postgres_cursor_context('mara') as cursor:
        cursor.execute(f"""
SELECT last_comparison_value 
FROM data_integration_incremental_copy_status 
WHERE node_path = {'%s'} AND source_table = {'%s'}""", (node_path, f'{source_db_alias}.{source_table}'))
        result = cursor.fetchone()
        return result[0] if result else None
