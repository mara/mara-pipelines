import datetime
import glob
import json
import math
import os.path
import pathlib
import re
from html import escape

import enum

import mara_db.config
import mara_db.dbs
import mara_db.postgresql
from mara_page import _, html
from .. import config, pipelines
from ..commands import python, sql, files
from ..incremental_processing import file_dependencies as _file_dependencies
from ..incremental_processing import processed_files as _processed_files
from ..logging import logger


class ReadMode(enum.EnumMeta):
    """A mode for specifying which files from a list of files to load"""
    ALL = 'all'  # load all files that match the pattern
    ONLY_LATEST = 'only_latest'  # load only the latest matching file (requires date-regex)
    ONLY_NEW = 'only_new'  # load only files that have not been loaded yet
    ONLY_CHANGED = 'only_changed'  # load all files that were modified since the last run (or that are new)
    ONLY_NEW_EXCEPT_LATEST = 'only_new_except_latest'  # load only files that have not been loaded yet and not the last one


class _ParallelRead(pipelines.ParallelTask):
    def __init__(self, id: str, description: str, file_pattern: str, read_mode: ReadMode, target_table: str,
                 max_number_of_parallel_tasks: int = None, file_dependencies: [str] = None, date_regex: str = None,
                 partition_target_table_by_day_id: bool = False, truncate_partitions: bool = False,
                 commands_before: [pipelines.Command] = None, commands_after: [pipelines.Command] = None,
                 db_alias: str = None, timezone: str = None) -> None:
        pipelines.ParallelTask.__init__(self, id=id, description=description,
                                        max_number_of_parallel_tasks=max_number_of_parallel_tasks,
                                        commands_before=commands_before, commands_after=commands_after)
        self.file_pattern = file_pattern
        self.read_mode = read_mode
        self.date_regex = date_regex
        self.file_dependencies = file_dependencies or []
        self.partition_target_table_by_day_id = partition_target_table_by_day_id
        if self.partition_target_table_by_day_id:
            assert date_regex

        self.truncate_partitions = truncate_partitions
        if self.truncate_partitions:
            assert partition_target_table_by_day_id

        self.target_table = target_table
        self._db_alias = db_alias
        self.timezone = timezone

    @property
    def db_alias(self):
        return self._db_alias or config.default_db_alias()

    def add_parallel_tasks(self, sub_pipeline: 'pipelines.Pipeline') -> None:
        import more_itertools

        files = []  # A list of (file_name, date_or_file_name) tuples
        data_dir = config.data_dir()
        first_date = config.first_date()

        for file in glob.iglob(str(pathlib.Path(data_dir, self.file_pattern))):
            file = str(pathlib.Path(file).relative_to(pathlib.Path(data_dir)))
            if self.date_regex:
                match = re.match(self.date_regex, file)
                if not match:
                    raise Exception(f'file name "{file}" \ndoes not match date regex "{self.date_regex}"')
                date = datetime.date(*[int(group) for group in match.groups()])
                if date >= first_date:
                    files.append((file, date))
            else:
                files.append((file, file))

        # sort by date when regex provided or by filename otherwise
        files.sort(key=lambda x: x[1], reverse=True)

        # remove latest file when requested
        if self.read_mode == ReadMode.ONLY_NEW_EXCEPT_LATEST:
            files = files[1:]

        # take only latest file when requested
        if files and len(files) > 0 and self.read_mode == ReadMode.ONLY_LATEST:
            files = files[:1]

        # for incremental loading, determine which files already have been processed
        # reprocess all when file dependencies changed
        if (self.read_mode not in (ReadMode.ALL, ReadMode.ONLY_LATEST)
                and (not self.file_dependencies
                     or not _file_dependencies.is_modified(self.path(), 'ParallelReadFile', self.parent.base_path(),
                                                           self.file_dependencies))):
            processed_files = _processed_files.already_processed_files(self.path())

            files = [x for x in files
                     if x[0] not in processed_files  # everything not yet read
                     or (self.read_mode == ReadMode.ONLY_CHANGED  # everything modified
                         and self._last_modification_timestamp(x[0]) > processed_files[x[0]])]

        if not files:
            logger.log('No newer files', format=logger.Format.ITALICS)
            return

        if self.read_mode != ReadMode.ALL and self.file_dependencies:
            def update_file_dependencies():
                _file_dependencies.update(self.path(), 'ParallelReadFile', self.parent.base_path(),
                                          self.file_dependencies)
                return True

            sub_pipeline.final_node.commands.append(python.RunFunction(update_file_dependencies))

        chunk_size = math.ceil(len(files) / (2 * config.max_number_of_parallel_tasks()))

        if self.partition_target_table_by_day_id:
            if not isinstance(mara_db.dbs.db(self.db_alias), mara_db.dbs.PostgreSQLDB):
                raise NotImplementedError(
                    f'Partitioning by day_id has only been implemented for postgresql so far, \n'
                    f'not for {mara_db.postgresql.engine(self.db_alias).name}')
            files_per_day = {}
            for (file, date) in files:
                if date in files_per_day:
                    files_per_day[date].append(file)
                else:
                    files_per_day[date] = [file]

            sql_statements = []
            for date in files_per_day.keys():
                sql_statements.append(f'CREATE TABLE IF NOT EXISTS {self.target_table}_{date.strftime("%Y%m%d")}'
                 + f' PARTITION OF {self.target_table} FOR VALUES IN ({date.strftime("%Y%m%d")});')

                if self.truncate_partitions:
                    sql_statements.append(f'TRUNCATE {self.target_table}_{date.strftime("%Y%m%d")};')

            create_partitions_task = pipelines.Task(
                id='create_partitions',
                description='Creates required target table partitions',
                commands=[sql.ExecuteSQL(sql_statement='\n'.join(slice), echo_queries=False, db_alias=self.db_alias)
                          for slice in more_itertools.sliced(sql_statements, 50)])

            sub_pipeline.add(create_partitions_task)

            for n, chunk in enumerate(more_itertools.chunked(files_per_day.items(), chunk_size)):
                task = pipelines.Task(id=str(n), description='Reads a portion of the files')
                for (day, files) in chunk:
                    target_table = self.target_table + '_' + day.strftime("%Y%m%d")
                    for file in files:
                        task.add_commands(self.parallel_commands(file))
                    task.add_command(sql.ExecuteSQL(sql_statement=f'ANALYZE {target_table}'))
                sub_pipeline.add(task, ['create_partitions'])
        else:
            for n, chunk in enumerate(more_itertools.chunked(files, chunk_size)):
                sub_pipeline.add(
                    pipelines.Task(id=str(n), description=f'Reads {len(chunk)} files',
                                   commands=sum([self.parallel_commands(x[0]) for x in chunk], [])))

    def parallel_commands(self, file_name: str) -> [pipelines.Command]:
        return [self.read_command(file_name)] + (
            [python.RunFunction(function=lambda: _processed_files.track_processed_file(
                self.path(), file_name, self._last_modification_timestamp(file_name)))]
            if self.read_mode != ReadMode.ALL else [])

    def read_command(self) -> pipelines.Command:
        raise NotImplementedError

    def _last_modification_timestamp(self, file_name):
        return datetime.datetime.fromtimestamp(os.path.getmtime(pathlib.Path(config.data_dir()) / file_name))


class ParallelReadFile(_ParallelRead):
    def __init__(self, id: str, description: str, file_pattern: str, read_mode: ReadMode,
                 compression: files.Compression, target_table: str, file_dependencies: [str] = None,
                 date_regex: str = None, partition_target_table_by_day_id: bool = False,
                 truncate_partitions: bool = False,
                 commands_before: [pipelines.Command] = None, commands_after: [pipelines.Command] = None,
                 mapper_script_file_name: str = None, make_unique: bool = False, db_alias: str = None,
                 delimiter_char: str = None, quote_char: str = None, null_value_string: str = None,
                 skip_header: bool = None, csv_format: bool = False,
                 timezone: str = None, max_number_of_parallel_tasks: int = None) -> None:
        _ParallelRead.__init__(self, id=id, description=description, file_pattern=file_pattern,
                               read_mode=read_mode, target_table=target_table, file_dependencies=file_dependencies,
                               date_regex=date_regex, partition_target_table_by_day_id=partition_target_table_by_day_id,
                               truncate_partitions=truncate_partitions,
                               commands_before=commands_before, commands_after=commands_after,
                               db_alias=db_alias, timezone=timezone,
                               max_number_of_parallel_tasks=max_number_of_parallel_tasks)
        self.compression = compression
        self.mapper_script_file_name = mapper_script_file_name or ''
        self.make_unique = make_unique
        self.delimiter_char = delimiter_char
        self.quote_char = quote_char
        self.skip_header = skip_header
        self.csv_format = csv_format
        self.null_value_string = null_value_string

    def read_command(self, file_name: str) -> pipelines.Command:
        return files.ReadFile(file_name=file_name, compression=self.compression, target_table=self.target_table,
                              mapper_script_file_name=self.mapper_script_file_name, make_unique=self.make_unique,
                              db_alias=self.db_alias, delimiter_char=self.delimiter_char, skip_header=self.skip_header,
                              quote_char=self.quote_char, null_value_string=self.null_value_string,
                              csv_format=self.csv_format, timezone=self.timezone)

    def html_doc_items(self) -> [(str, str)]:
        path = self.parent.base_path() / self.mapper_script_file_name if self.mapper_script_file_name else ''
        return [('file pattern', _.i[self.file_pattern]),
                ('compression', _.tt[self.compression]),
                ('read mode', _.tt[self.read_mode]),
                ('date regex', _.tt[escape(self.date_regex)] if self.date_regex else None),
                ('file dependencies', [_.i[dependency, _.br] for dependency in self.file_dependencies]),
                ('mapper script file name', _.i[self.mapper_script_file_name]),
                (_.i['mapper script'], html.highlight_syntax(path.read_text().strip('\n')
                                                             if self.mapper_script_file_name and path.exists()
                                                             else '', 'python')),
                ('make unique', _.tt[repr(self.make_unique)]),
                ('skip header', _.tt[self.skip_header]),
                ('target_table', _.tt[self.target_table]),
                ('db alias', _.tt[self.db_alias]),
                ('partion target table by day_id', _.tt[self.partition_target_table_by_day_id]),
                ('truncate partitions', _.tt[self.truncate_partitions]),
                ('sql delimiter char',
                 _.tt[json.dumps(self.delimiter_char) if self.delimiter_char != None else None]),
                ('quote char', _.tt[json.dumps(self.quote_char) if self.quote_char != None else None]),
                ('null value string',
                 _.tt[json.dumps(self.null_value_string) if self.null_value_string != None else None]),
                ('time zone', _.tt[self.timezone])]


class ParallelReadSqlite(_ParallelRead):
    def __init__(self, id: str, description: str, file_pattern: str, read_mode: ReadMode, sql_file_name: str,
                 target_table: str, file_dependencies: [str] = None, date_regex: str = None,
                 partition_target_table_by_day_id: bool = False, truncate_partitions: bool = False,
                 commands_before: [pipelines.Command] = None, commands_after: [pipelines.Command] = None,
                 db_alias: str = None, timezone=None, max_number_of_parallel_tasks: int = None) -> None:
        _ParallelRead.__init__(self, id=id, description=description, file_pattern=file_pattern,
                               read_mode=read_mode, target_table=target_table, file_dependencies=file_dependencies,
                               date_regex=date_regex, partition_target_table_by_day_id=partition_target_table_by_day_id,
                               truncate_partitions=truncate_partitions,
                               commands_before=commands_before, commands_after=commands_after, db_alias=db_alias,
                               timezone=timezone, max_number_of_parallel_tasks=max_number_of_parallel_tasks)
        self.sql_file_name = sql_file_name

    def read_command(self, file_name: str) -> [pipelines.Command]:
        return files.ReadSQLite(sqlite_file_name=file_name, sql_file_name=self.sql_file_name,
                                target_table=self.target_table, db_alias=self.db_alias, timezone=self.timezone)

    def sql_file_path(self):
        return self.parent.base_path() / self.sql_file_name

    def html_doc_items(self) -> [(str, str)]:
        path = self.sql_file_path()
        return [('file pattern', _.i[self.file_pattern]),
                ('read mode', _.tt[self.read_mode]),
                ('date regex', _.tt[escape(self.date_regex)] if self.date_regex else None),
                ('file dependencies', [_.i[dependency, _.br] for dependency in self.file_dependencies]),
                ('query file name', _.i[self.sql_file_name]),
                (_.i['query'], html.highlight_syntax(path.read_text().strip('\n')
                                                     if self.sql_file_name and path.exists()
                                                     else '', 'sql')),
                ('target_table', _.tt[self.target_table]),
                ('db alias', _.tt[self.db_alias]),
                ('partion target table by day_id', _.tt[self.partition_target_table_by_day_id]),
                ('truncate partitions', _.tt[self.truncate_partitions]),
                ('time zone', _.tt[self.timezone])]
