"""Logging pipeline runs, node output and status information in mara database"""

import psycopg2.extensions
import sqlalchemy.orm
from sqlalchemy.ext.declarative import declarative_base

import mara_db.postgresql
from .. import config
from ..logging import events, system_statistics

Base = declarative_base()


class Run(Base):
    """Runtime and status of a data integration run"""
    __tablename__ = 'data_integration_run'

    run_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    node_path = sqlalchemy.Column(sqlalchemy.ARRAY(sqlalchemy.TEXT), nullable=False, index=True)
    pid = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    start_time = sqlalchemy.Column(sqlalchemy.TIMESTAMP(timezone=True), nullable=False)
    end_time = sqlalchemy.Column(sqlalchemy.TIMESTAMP(timezone=True))
    succeeded = sqlalchemy.Column(sqlalchemy.BOOLEAN)


class NodeRun(Base):
    """Runtime, status information and output of a pipeline node run"""
    __tablename__ = 'data_integration_node_run'

    node_run_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    run_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('data_integration_run.run_id'), index=True)

    node_path = sqlalchemy.Column(sqlalchemy.ARRAY(sqlalchemy.TEXT), index=True)
    start_time = sqlalchemy.Column(sqlalchemy.TIMESTAMP(timezone=True), nullable=False)
    end_time = sqlalchemy.Column(sqlalchemy.TIMESTAMP(timezone=True))
    succeeded = sqlalchemy.Column(sqlalchemy.BOOLEAN)
    is_pipeline = sqlalchemy.Column(sqlalchemy.BOOLEAN)


class NodeOutput(Base):
    """Runtime, status information and output of a pipeline node run"""
    __tablename__ = 'data_integration_node_output'

    node_output_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    node_run_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('data_integration_node_run.node_run_id'),
                                    index=True)
    timestamp = sqlalchemy.Column(sqlalchemy.TIMESTAMP(timezone=True), nullable=False)
    message = sqlalchemy.Column(sqlalchemy.TEXT, nullable=False)
    format = sqlalchemy.Column(sqlalchemy.TEXT, nullable=False)
    is_error = sqlalchemy.Column(sqlalchemy.BOOLEAN, nullable=False)


class SystemStatistics(Base):
    """System stats measurements"""
    __tablename__ = 'data_integration_system_statistics'

    timestamp = sqlalchemy.Column(sqlalchemy.TIMESTAMP(timezone=True), primary_key=True, index=True)
    disc_read = sqlalchemy.Column(sqlalchemy.FLOAT)
    disc_write = sqlalchemy.Column(sqlalchemy.FLOAT)
    net_recv = sqlalchemy.Column(sqlalchemy.FLOAT)
    net_sent = sqlalchemy.Column(sqlalchemy.FLOAT)
    cpu_usage = sqlalchemy.Column(sqlalchemy.FLOAT)
    mem_usage = sqlalchemy.Column(sqlalchemy.FLOAT)
    swap_usage = sqlalchemy.Column(sqlalchemy.FLOAT)
    iowait = sqlalchemy.Column(sqlalchemy.FLOAT)


class RunLogger(events.EventHandler):
    run_id: int = None
    node_output: {tuple: [events.Output]} = None

    def handle_event(self, event: events.Event):

        if isinstance(event, events.RunStarted):
            with mara_db.postgresql.postgres_cursor_context(
                    'mara') as cursor:  # type: psycopg2.extensions.cursor
                cursor.execute(f'''
INSERT INTO data_integration_run (node_path, pid, start_time)
VALUES ({"%s, %s, %s"})
RETURNING run_id;''', (event.node_path, event.pid, event.start_time))
                self.run_id = cursor.fetchone()[0]

        elif isinstance(event, events.Output):
            key = tuple(event.node_path)

            if not self.node_output:
                self.node_output = {}

            if key in self.node_output:
                self.node_output[key].append(event)
            else:
                self.node_output[key] = [event]

        elif isinstance(event, events.NodeStarted):
            with mara_db.postgresql.postgres_cursor_context(
                    'mara') as cursor:  # type: psycopg2.extensions.cursor
                cursor.execute(f'''
INSERT INTO data_integration_node_run (run_id, node_path, start_time, is_pipeline)
VALUES  ({"%s, %s, %s, %s"})
RETURNING node_run_id''', (self.run_id, event.node_path, event.start_time, event.is_pipeline))

        elif isinstance(event, system_statistics.SystemStatistics):
            with mara_db.postgresql.postgres_cursor_context(
                    'mara') as cursor:  # type: psycopg2.extensions.cursor
                cursor.execute(f'''
INSERT INTO data_integration_system_statistics (timestamp, disc_read, disc_write, net_recv, net_sent, 
                                  cpu_usage, mem_usage, swap_usage, iowait)
VALUES ({"%s, %s, %s, %s, %s, %s, %s, %s, %s"})''',
                               (event.timestamp, event.disc_read, event.disc_write, event.net_recv,
                                event.net_sent, event.cpu_usage, event.mem_usage, event.swap_usage, event.iowait))

        elif isinstance(event, events.NodeFinished):
            with mara_db.postgresql.postgres_cursor_context(
                    'mara') as cursor:  # type: psycopg2.extensions.cursor
                cursor.execute(f'''
UPDATE data_integration_node_run
SET end_time={"%s"}, succeeded={"%s"}
WHERE run_id={"%s"} AND node_path={"%s"}
RETURNING node_run_id''', (event.end_time, event.succeeded, self.run_id, event.node_path))
                node_run_id = cursor.fetchone()[0]

                cursor.execute('''
INSERT INTO data_integration_node_output (node_run_id, timestamp, message, format, is_error) 
VALUES ''' + ','.join([cursor.mogrify('(%s,%s,%s,%s,%s)', (node_run_id, output_event.timestamp, output_event.message,
                                                           output_event.format, output_event.is_error))
                      .decode('utf-8')
                       for output_event in self.node_output.get(tuple(event.node_path))]))

        elif isinstance(event, events.RunFinished):
            with mara_db.postgresql.postgres_cursor_context(
                    'mara') as cursor:  # type: psycopg2.extensions.cursor
                cursor.execute(f'''
UPDATE data_integration_run 
SET end_time={"%s"}, succeeded={"%s"}
WHERE run_id={"%s"}''', (event.end_time, event.succeeded, self.run_id))

                cursor.execute(f'''
DELETE FROM data_integration_node_output WHERE node_run_id IN (
    SELECT node_run_id FROM data_integration_node_run WHERE run_id IN (
        SELECT run_id FROM data_integration_run
        WHERE start_time + INTERVAL '{config.run_log_retention_in_days()} days' < current_timestamp));''')

                cursor.execute(f'''
DELETE FROM data_integration_node_run WHERE run_id IN (
    SELECT run_id FROM data_integration_run 
    WHERE start_time + INTERVAL '{config.run_log_retention_in_days()} days' < current_timestamp);''')

                cursor.execute(f'''
DELETE FROM data_integration_run 
WHERE start_time + INTERVAL '{config.run_log_retention_in_days()} days' < current_timestamp;''')

                cursor.execute(f'''
DELETE FROM data_integration_system_statistics
WHERE timestamp + INTERVAL '{config.run_log_retention_in_days()} days' < current_timestamp;''')
