import inspect
import re
import typing

from mara_page import _, html
from .. import config, pipelines
from ..commands import sql


class ParallelExecuteSQL(pipelines.ParallelTask, sql._SQLCommand):
    def __init__(self, id: str, description: str, parameter_function: typing.Callable, parameter_placeholders: [str],
                 max_number_of_parallel_tasks: int = None, sql_statement: str = None, file_name: str = None,
                 commands_before: [pipelines.Command] = None, commands_after: [pipelines.Command] = None,
                 db_alias: str = None, echo_queries: bool = True, timezone: str = None,
                 replace: {str: str} = None) -> None:

        if (not (sql_statement or file_name)) or (sql_statement and file_name):
            raise ValueError('Please provide either sql_statement or file_name (but not both)')

        pipelines.ParallelTask.__init__(self, id=id, description=description,
                                        max_number_of_parallel_tasks=max_number_of_parallel_tasks,
                                        commands_before=commands_before, commands_after=commands_after)
        sql._SQLCommand.__init__(self, sql_statement, file_name, replace)

        self.parameter_function = parameter_function
        self.parameter_placeholders = parameter_placeholders
        self._db_alias = db_alias
        self.timezone = timezone
        self.echo_queries = echo_queries

    @property
    def db_alias(self):
        return self._db_alias or config.default_db_alias()

    def add_parallel_tasks(self, sub_pipeline: 'pipelines.Pipeline') -> None:
        parameters = self.parameter_function()

        if not isinstance(parameters, list) or not all(isinstance(item, tuple) for item in parameters):
            raise ValueError(f'parameter function should return a list of tuples, got "{repr(parameters)}"')

        for parameter_tuple in parameters:
            id = '-'.join([re.sub('[^0-9a-z\-_]+', '', str(x).lower().replace('-', '_')) for x in parameter_tuple])
            replace = self.replace.copy()
            for placeholder, param in zip(self.parameter_placeholders, parameter_tuple):
                replace[placeholder] = param

            sub_pipeline.add(pipelines.Task(
                id=id, description=f'Execute SQL for parameters {repr(parameter_tuple)}',
                commands=[
                    sql.ExecuteSQL(sql_file_name=self.sql_file_name, db_alias=self.db_alias,
                                   echo_queries=self.echo_queries, timezone=self.timezone, replace=replace)
                    if self.sql_file_name else
                    sql.ExecuteSQL(sql_statement=self.sql_statement, db_alias=self.db_alias,
                                   echo_queries=self.echo_queries, timezone=self.timezone, replace=replace)]))

    def html_doc_items(self) -> [(str, str)]:
        return [('db', _.tt[self.db_alias])] \
               + sql._SQLCommand.html_doc_items(self, self.db_alias) \
               + [('parameter function', html.highlight_syntax(inspect.getsource(self.parameter_function), 'python')),
                  ('parameter placeholders', _.tt[repr(self.parameter_placeholders)]),
                  ('echo queries', _.tt[str(self.echo_queries)]),
                  ('timezone', _.tt[self.timezone or ''])]
