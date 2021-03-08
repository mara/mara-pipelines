"""Commands for working with sql databases"""

import functools
import json
import pathlib
import shlex
from typing import Callable, Union

import mara_db.dbs
import mara_db.shell
import mara_db.postgresql
from mara_page import _, html
from .. import config, shell, pipelines
from ..incremental_processing import file_dependencies
from ..incremental_processing import incremental_copy_status
from ..logging import logger


class _SQLCommand(pipelines.Command):
    def __init__(self, sql_statement: Union[Callable, str] = None, sql_file_name: str = None,
                 replace: {str: str} = None) -> None:
        """
        Something that runs a sql query (either a query string or a query file).

        Args:
            sql_statement: The statement to run as a string
            sql_file_name: The name of the file to run (relative to the directory of the parent pipeline)
            replace: A set of replacements to perform against the sql query `{'replace`: 'with', ..}`
        """
        if (not (sql_statement or sql_file_name)) or (sql_statement and sql_file_name):
            raise ValueError('Please provide either sql_statement or sql_file_name (but not both)')

        super().__init__()
        self.sql_file_name = sql_file_name
        self._sql_statement = sql_statement
        self.replace = replace or {}

    @property
    def sql_statement(self):
        return self._sql_statement() if callable(self._sql_statement) else self._sql_statement

    def sql_file_path(self) -> pathlib.Path:
        # Get the first pipeline in the tree (don't reach root)
        pipeline_candidate = self
        while not isinstance(pipeline_candidate, pipelines.Pipeline):
            pipeline_candidate = pipeline_candidate.parent
        assert isinstance(pipeline_candidate, pipelines.Pipeline)
        return pipeline_candidate.base_path() / self.sql_file_name

    def shell_command(self):
        if self.sql_file_name:
            command = 'cat ' + shlex.quote(str(self.sql_file_path().absolute())) + ' \\\n'
        else:
            command = f'echo {shlex.quote(self.sql_statement)} \\\n'

        if self.replace:
            command += '  | ' + shell.sed_command(_expand_pattern_substitution(self.replace)) + ' \\\n'
        return command

    def html_doc_items(self, db_alias: str):
        sql = self.sql_statement or \
              (self.sql_file_path().read_text().strip('\n') if self.sql_file_path().exists() else '-- file not found')
        doc = []
        if self.sql_statement:
            doc.append(
                ('sql statement', html.highlight_syntax(self.sql_statement, _sql_syntax_higlighting_lexter(db_alias))))
        else:
            doc.append(('sql file name', _.i[self.sql_file_name]))
            doc.append((_.i['sql file content'], html.highlight_syntax(sql, _sql_syntax_higlighting_lexter(db_alias))))

        doc.append(('replace', html.highlight_syntax(
            json.dumps(_expand_pattern_substitution(self.replace), indent=2), 'json')))

        if self.replace:
            for k, v in _expand_pattern_substitution(self.replace).items():
                sql = sql.replace(k, v)
            doc.append((_.i['substituted sql'], html.highlight_syntax(sql, _sql_syntax_higlighting_lexter(db_alias))))

        return doc


class ExecuteSQL(_SQLCommand):
    def __init__(self, sql_statement: str = None, sql_file_name: Union[str, Callable] = None,
                 replace: {str: str} = None, file_dependencies=None, db_alias: str = None,
                 echo_queries: bool = None, timezone: str = None) -> None:
        """
        Runs an sql file or statement in a database

        Args:
            sql_statement: The statement to run as a string
            sql_file_name: The name of the file to run (relative to the directory of the parent pipeline)
            replace: A set of replacements to perform against the sql query `{'replace`: 'with', ..}`
        """
        _SQLCommand.__init__(self, sql_statement, sql_file_name, replace)

        self._db_alias = db_alias
        self.timezone = timezone
        self.file_dependencies = file_dependencies or []
        self.echo_queries = echo_queries

    @property
    def db_alias(self):
        return self._db_alias or config.default_db_alias()

    def run(self) -> bool:
        if self.sql_file_name:
            logger.log(self.sql_file_name, logger.Format.ITALICS)

        dependency_type = 'ExecuteSQL ' + (self.sql_file_name or self.sql_statement)

        if self.file_dependencies:
            assert (self.parent)
            pipeline_base_path = self.parent.parent.base_path()
            if not file_dependencies.is_modified(self.node_path(), dependency_type,
                                                 pipeline_base_path,
                                                 self.file_dependencies):
                logger.log('no changes')
                return True
            else:
                # delete any old hash to trigger a run in case the next run is switched back to the old hash
                # This prevents inconsistent state in case you do a deplyoment with bad SQL which fails and
                # then revert.
                # The hash would still be correct for the old state but the results of this file would
                # probably not be there (usually the first step is a DROP).
                file_dependencies.delete(self.node_path(), dependency_type)

        if not super().run():
            return False

        if self.file_dependencies:
            file_dependencies.update(self.node_path(), dependency_type, pipeline_base_path, self.file_dependencies)
        return True

    def shell_command(self):
        return _SQLCommand.shell_command(self) \
               + '  | ' + mara_db.shell.query_command(self.db_alias, self.timezone, self.echo_queries)

    def html_doc_items(self):
        return [('db', _.tt[self.db_alias]),
                ('file dependencies', [_.i[dependency, _.br] for dependency in self.file_dependencies])] \
               + _SQLCommand.html_doc_items(self, self.db_alias) \
               + [('echo queries', _.tt[str(self.echo_queries)]),
                  ('timezone', _.tt[self.timezone or '']),
                  (_.i['shell command'], html.highlight_syntax(self.shell_command(), 'bash'))]


class Copy(_SQLCommand):
    """Loads data from an external database"""

    def __init__(self, source_db_alias: str, target_table: str, target_db_alias: str = None,
                 sql_statement: str = None, sql_file_name: Union[Callable, str] = None, replace: {str: str} = None,
                 timezone: str = None, csv_format: bool = None, delimiter_char: str = None,
                 file_dependencies=None) -> None:
        _SQLCommand.__init__(self, sql_statement, sql_file_name, replace)
        self.source_db_alias = source_db_alias
        self.target_table = target_table
        self._target_db_alias = target_db_alias
        self.timezone = timezone
        self.csv_format = csv_format
        self.delimiter_char = delimiter_char
        self.file_dependencies = file_dependencies or []

    @property
    def target_db_alias(self):
        return self._target_db_alias or config.default_db_alias()

    def file_path(self) -> pathlib.Path:
        return self.parent.parent.base_path() / self.sql_file_name

    def run(self) -> bool:
        if self.sql_file_name:
            logger.log(self.sql_file_name, logger.Format.ITALICS)

        dependency_type = 'Copy ' + (self.sql_file_name or self.sql_statement)

        if self.file_dependencies:
            assert (self.parent)
            pipeline_base_path = self.parent.parent.base_path()
            if not file_dependencies.is_modified(self.node_path(), dependency_type,
                                                 pipeline_base_path,
                                                 self.file_dependencies):
                logger.log('no changes')
                return True
            else:
                # delete any old hash to trigger a run in case the next run is switched back to the old hash
                # which in most cases would result in an newly created empty table but no load
                # (see also above in ExecuteSQL)
                file_dependencies.delete(self.node_path(), dependency_type)

        if not super().run():
            return False

        if self.file_dependencies:
            file_dependencies.update(self.node_path(), dependency_type, pipeline_base_path, self.file_dependencies)
        return True

    def shell_command(self):
        return _SQLCommand.shell_command(self) \
               + '  | ' + mara_db.shell.copy_command(self.source_db_alias, self.target_db_alias, self.target_table,
                                                     self.timezone, self.csv_format, self.delimiter_char)

    def html_doc_items(self) -> [(str, str)]:
        return [('source db', _.tt[self.source_db_alias])] \
               + _SQLCommand.html_doc_items(self, self.source_db_alias) \
               + [('target db', _.tt[self.target_db_alias]),
                  ('target table', _.tt[self.target_table]),
                  ('timezone', _.tt[self.timezone or '']),
                  ('csv format', _.tt[self.csv_format or '']),
                  ('delimiter char', _.tt[self.delimiter_char or '']),
                  (_.i['shell command'], html.highlight_syntax(self.shell_command(), 'bash'))]


class CopyIncrementally(_SQLCommand):
    def __init__(self, source_db_alias: str, source_table: str,
                 modification_comparison: str, comparison_value_placeholder: str,
                 target_table: str, primary_keys: [str],
                 sql_file_name: Union[str, Callable] = None, sql_statement: Union[str, Callable] = None,
                 target_db_alias: str = None, timezone: str = None, replace: {str: str} = None,
                 use_explicit_upsert: bool = False,
                 csv_format: bool = None, delimiter_char: str = None,
                 modification_comparison_type: str = None) -> None:
        """
        Incrementally loads data from one database into another.

        Requires the source table to have an monotonously increasing column or combination of columns that
        allow to identify "newer" columns (the modification comparison).

        After an initial full load, only those rows with a with a a higher modification comparison than the last
        comparison value are read.

        Args:
            source_db_alias: The database to load from
            source_table: The table to read from
            sql_statement: A query that is run to query the source database
            sql_file_name: The path of a file name that is run to query the source database
            replace: A set of replacements to perform against the sql query
            modification_comparison: SQL expression that evaluates to a comparable value
            modification_comparison_type: type of the saved (as string) modification_comparison value
            comparison_value_placeholder: A placeholder in the sql code that gets replaced with the
                                          actual incremental load comparison or `1=1`.
            target_db_alias: The database to write to
            target_table: The table for loading data into
            primary_keys: A combination of primary key columns that are used for upserting into the target table
            timezone: How to interpret timestamps in the target db
            use_explicit_upsert: When True, uses an Update + Insert query combination. Otherwise ON CONFLICT DO UPDATE.
        """
        _SQLCommand.__init__(self, sql_statement, sql_file_name, replace)
        self.source_db_alias = source_db_alias
        self.source_table = source_table
        self.modification_comparison = modification_comparison
        self.modification_comparison_type = modification_comparison_type
        self.comparison_value_placeholder = comparison_value_placeholder

        self._target_db_alias = target_db_alias
        self.target_table = target_table
        self.primary_keys = primary_keys
        self.timezone = timezone
        self.use_explicit_upsert = use_explicit_upsert
        self.csv_format = csv_format
        self.delimiter_char = delimiter_char

    @property
    def target_db_alias(self):
        return self._target_db_alias or config.default_db_alias()

    def run(self) -> bool:
        # retrieve the highest current value for the modification comparison (e.g.: the highest timestamp)
        # We intentionally use the command line here (rather than sqlalchemy) to avoid forcing people python drivers,
        # which can be hard for example in the case of SQL Server
        logger.log(f'Get new max modification comparison value...', format=logger.Format.ITALICS)
        max_value_query = f'SELECT max({self.modification_comparison}) AS maxval FROM {self.source_table}'
        logger.log(max_value_query, format=logger.Format.VERBATIM)
        result = shell.run_shell_command(f'echo {shlex.quote(max_value_query)} \\\n  | '
                                         + mara_db.shell.copy_to_stdout_command(self.source_db_alias))

        if not result:
            return False

        if isinstance(result, bool):
            # This happens if the query above ran, but returned no data and therefore the load
            # query below would also return no data
            # We assume that this happens e.g. when there is no data *yet* and let the load succeed
            # without actually doing anything
            logger.log("Found no data, not starting Copy.", format=logger.Format.VERBATIM)
            return True
        # be flexible with different output formats: remove the column header & remove whitespace & quotes
        max_modification_value = ''.join(result).replace('maxval', '').strip().strip('"')
        logger.log(f"New max modification comparison value: {max_modification_value!r}", format=logger.Format.VERBATIM)

        # check whether target table is empty
        target_table_is_empty = True

        target_table_empty_query = f'SELECT TRUE FROM {self.target_table} LIMIT 1'
        logger.log(f'Check if target table is empty', format=logger.Format.ITALICS)
        logger.log(target_table_empty_query, format=logger.Format.VERBATIM)
        with mara_db.postgresql.postgres_cursor_context(self.target_db_alias) as cursor:
            cursor.execute(f'SELECT TRUE FROM {self.target_table} LIMIT 1')
            target_table_is_empty = not cursor.fetchone()
            logger.log(f"target table{'' if target_table_is_empty else ' not'} empty", format=logger.Format.ITALICS)

        # get last comparison value
        logger.log('Get last comparison value...', format=logger.Format.ITALICS)
        last_comparison_value = incremental_copy_status.get_last_comparison_value(
            self.node_path(), self.source_db_alias, self.source_table)
        logger.log(f"Last max modification comparison value: {last_comparison_value!r}", format=logger.Format.VERBATIM)

        if target_table_is_empty or not last_comparison_value:
            # full load
            logger.log('Using full (non incremental) Copy', logger.Format.ITALICS)
            if not target_table_is_empty:
                truncate_query = f'TRUNCATE TABLE {self.target_table}'
                logger.log(truncate_query, format=logger.Format.VERBATIM)
                with mara_db.postgresql.postgres_cursor_context(self.target_db_alias) as cursor:
                    cursor.execute(truncate_query)
            elif last_comparison_value:
                # table is empty but we have a last comparison value from earlier runs
                # If we would crash during load (with some data already in the table), the next run would
                # not trigger a full load and we would miss data. To prevent that, delete the old
                # comparison value (we will then set it only on success)
                logger.log('Deleting old comparison value', logger.Format.ITALICS)
                incremental_copy_status.delete(self.node_path(), self.source_db_alias, self.source_table)

            # overwrite the comparison criteria to get everything
            replace = {self.comparison_value_placeholder: '(1=1)'}
            complete_copy_command = self._copy_command(self.target_table, replace)
            if not shell.run_shell_command(complete_copy_command):
                return False

        else:
            # incremental load. First create the table which will contain the delta
            logger.log('Using incremental Copy, create upsert table', logger.Format.ITALICS)
            create_upsert_table_query = (f'DROP TABLE IF EXISTS {self.target_table}_upsert;\n'
                                         + f'CREATE TABLE {self.target_table}_upsert AS SELECT * from {self.target_table} WHERE FALSE')

            if not shell.run_shell_command(f'echo {shlex.quote(create_upsert_table_query)} \\\n  | '
                                           + mara_db.shell.query_command(self.target_db_alias)):
                return False

            # perform the actual copy replacing the placeholder
            # with the comparison value from the latest successful execution
            modification_comparison_type = self.modification_comparison_type or ''
            replace = {self.comparison_value_placeholder:
                           f'({self.modification_comparison} >= {modification_comparison_type} \'{last_comparison_value}\')'}
            if not shell.run_shell_command(self._copy_command(self.target_table + '_upsert', replace)):
                return False

            # now the upsert table has to be merged with the target one

            # retrieve the target table columns to build the SET clause of the upsert query
            with mara_db.postgresql.postgres_cursor_context(self.target_db_alias) as cursor:
                retrieve_column_query = f"SELECT attname FROM pg_attribute WHERE attrelid = '{self.target_table}'::REGCLASS AND attnum > 0;"
                logger.log(retrieve_column_query, format=logger.Format.VERBATIM)
                cursor.execute(retrieve_column_query)
                if self.use_explicit_upsert:
                    set_clause = ', '.join([f'"{col[0]}" = src."{col[0]}"' for col in cursor.fetchall()])
                    key_definition = ' AND '.join([f'dst."{k}" = src."{k}"' for k in self.primary_keys])
                else:
                    set_clause = ', '.join([f'"{col[0]}" = EXCLUDED."{col[0]}"' for col in cursor.fetchall()])
                    key_definition = ', '.join(['"' + primary_key + '"' for primary_key in self.primary_keys])

            if self.use_explicit_upsert:
                update_query = f"""
UPDATE {self.target_table} dst
SET {set_clause}
FROM {self.target_table}_upsert src
WHERE {key_definition}"""

                insert_query = f"""
INSERT INTO {self.target_table}
SELECT src.*
FROM {self.target_table}_upsert src
WHERE NOT EXISTS (SELECT 1 FROM {self.target_table} dst WHERE {key_definition})"""
                if not shell.run_shell_command(f'echo {shlex.quote(update_query)} \\\n  | '
                                               + mara_db.shell.query_command(self.target_db_alias)):
                    return False
                elif not shell.run_shell_command(f'echo {shlex.quote(insert_query)} \\\n  | '
                                                 + mara_db.shell.query_command(self.target_db_alias)):
                    return False
            else:
                upsery_query = f"""
INSERT INTO {self.target_table}
SELECT {self.target_table}_upsert.*
FROM {self.target_table}_upsert
ON CONFLICT ({key_definition})
DO UPDATE SET {set_clause}"""
                if not shell.run_shell_command(f'echo {shlex.quote(upsery_query)} \\\n  | '
                                               + mara_db.shell.query_command(self.target_db_alias)):
                    return False

        # update data_integration_incremental_copy_status
        incremental_copy_status.update(self.node_path(), self.source_db_alias,
                                       self.source_table, max_modification_value)
        return True

    def _copy_command(self, target_table, replace):
        """Helper function for creating the actual copy command"""
        return (_SQLCommand.shell_command(self)
                + '  | ' + shell.sed_command(replace)
                + '  | ' + mara_db.shell.copy_command(self.source_db_alias, self.target_db_alias,
                                                      target_table, timezone=self.timezone,
                                                      csv_format=self.csv_format, delimiter_char=self.delimiter_char))

    def html_doc_items(self) -> [(str, str)]:
        return [('source db', _.tt[self.source_db_alias]),
                ('source table', _.tt[self.source_table]),
                ('modification comparison', _.tt[self.modification_comparison])] \
               + _SQLCommand.html_doc_items(self, self.source_db_alias) \
               + [('comparison value placeholder', _.tt[self.comparison_value_placeholder]),
                  ('modification comparison type', _.tt[self.modification_comparison_type if self.modification_comparison_type else '(no cast)']),
                  ('target db', _.tt[self.target_db_alias]),
                  ('target table', _.tt[self.target_table]),
                  ('primary_keys', _.tt[repr(self.primary_keys)]),
                  ('timezone', _.tt[self.timezone or '']),
                  ('csv format', _.tt[self.csv_format or '']),
                  ('delimiter char', _.tt[self.delimiter_char or '']),
                  ('use explicit upsert', _.tt[repr(self.use_explicit_upsert)])]


def _expand_pattern_substitution(replace: {str: str}) -> {str: str}:
    """Helper function for replacing callables with their value in a dictionary"""
    return {k: (str(v()) if callable(v) else str(v)) for k, v in replace.items()}


@functools.singledispatch
def _sql_syntax_higlighting_lexter(db):
    """Returns the best lexer from http://pygments.org/docs/lexers/ for a database dialect"""
    return 'sql'


_sql_syntax_higlighting_lexter.register(str, lambda alias: _sql_syntax_higlighting_lexter(mara_db.dbs.db(alias)))
_sql_syntax_higlighting_lexter.register(mara_db.dbs.PostgreSQLDB, lambda _: 'postgresql')
_sql_syntax_higlighting_lexter.register(mara_db.dbs.MysqlDB, lambda _: 'mysql')
