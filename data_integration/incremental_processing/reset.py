"""Resetting incremental copy status"""

import mara_db.config
import mara_db.postgresql


def reset_incremental_processing(node_path: [str]):
    """
    Recursively resets all incremental processing status information that is stored in the mara db
    Args:
        node_path: The path of the node to reset

    """
    with mara_db.postgresql.postgres_cursor_context('mara') as cursor:
        cursor.execute(f'''
SELECT *
FROM
  (SELECT node_path, 'processed files', count(*)
   FROM data_integration_processed_file
   GROUP BY node_path

   UNION

   SELECT node_path, 'file dependencies', count(*)
   FROM data_integration_file_dependency
   GROUP BY node_path

   UNION

   SELECT node_path, 'incremental copy statuses', count(*)
   FROM data_integration_incremental_copy_status
   GROUP BY node_path

  ) t
WHERE node_path [1:{'%s'}] = {'%s'}

ORDER BY 1, 2''', (len(node_path), node_path))

        for path, type, n in cursor.fetchall():
            print(f'{"/".join(path)}: {n} {type}')

        for table in ['data_integration_processed_file', 'data_integration_file_dependency',
                      'data_integration_incremental_copy_status']:
            cursor.execute(f'''DELETE FROM {table} WHERE node_path [1:{'%s'}] = {'%s'}''', (len(node_path), node_path))
