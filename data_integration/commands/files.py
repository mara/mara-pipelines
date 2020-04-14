"""Commands for reading files"""

import json
import pathlib
import shlex
import sys

import enum

import mara_db.dbs
import mara_db.shell
from data_integration.commands import sql
from mara_page import _, html
from .. import config, pipelines


class Compression(enum.EnumMeta):
    """Different compression formats that are understood by file readers"""
    NONE = 'none'
    GZIP = 'gzip'
    TAR_GZIP = 'tar.gzip'
    ZIP = 'zip'


def uncompressor(compression: Compression) -> str:
    """Maps compression methods to command line programs that can unpack the respective files"""
    return {Compression.NONE: 'cat',
            Compression.ZIP: 'unzip -p',
            Compression.GZIP: 'gunzip -d -c',
            Compression.TAR_GZIP: 'tar -xOzf'}[compression]


class _ReadFile(pipelines.Command):
    """Base class to read in a file"""

    def __init__(self, compression: Compression, target_table: str,
                 mapper_script_file_name: str = None, make_unique: bool = False,
                 db_alias: str = None, csv_format: bool = False, skip_header: bool = False,
                 delimiter_char: str = None, quote_char: str = None,
                 null_value_string: str = None, timezone: str = None) -> None:
        super().__init__()
        self.compression = compression
        self.mapper_script_file_name = mapper_script_file_name
        self.make_unique = make_unique

        self.target_table = target_table
        self.csv_format = csv_format
        self.skip_header = skip_header
        self._db_alias = db_alias
        self.delimiter_char = delimiter_char
        self.quote_char = quote_char
        self.null_value_string = null_value_string
        self.timezone = timezone

    def db_alias(self):
        return self._db_alias or config.default_db_alias()

    def shell_command(self):
        return \
            f'{self.read_file_command()} \\\n' \
            + (f'  | {shlex.quote(sys.executable)} "{self.mapper_file_path()}" \\\n'
               if self.mapper_script_file_name else '') \
            + ('  | sort -u \\\n' if self.make_unique else '') \
            + '  | ' + mara_db.shell.copy_from_stdin_command(
                self.db_alias(), csv_format=self.csv_format, target_table=self.target_table,
                skip_header=self.skip_header,
                delimiter_char=self.delimiter_char, quote_char=self.quote_char,
                null_value_string=self.null_value_string, timezone=self.timezone)

    def read_file_command(self):
        raise NotImplementedError('')

    def mapper_file_path(self):
        return self.parent.parent.base_path() / self.mapper_script_file_name

    def html_doc_items(self) -> [(str, str)]:
        return [('compression', _.tt[self.compression]),
                ('mapper script file name', _.i[self.mapper_script_file_name]),
                (_.i['content'], html.highlight_syntax(self.mapper_file_path().read_text().strip('\n')
                                                       if self.mapper_script_file_name and self.mapper_file_path().exists()
                                                       else '', 'python')),
                ('make unique', _.tt[self.make_unique]),
                ('target_table', _.tt[self.target_table]),
                ('db alias', _.tt[self.db_alias()]),
                ('csv format', _.tt[self.csv_format]),
                ('skip header', _.tt[self.skip_header]),
                ('delimiter char',
                 _.tt[json.dumps(self.delimiter_char) if self.delimiter_char != None else None]),
                ('quote char', _.tt[json.dumps(self.quote_char) if self.quote_char != None else None]),
                ('null value string',
                 _.tt[json.dumps(self.null_value_string) if self.null_value_string != None else None]),
                ('time zone', _.tt[self.timezone]),
                (_.i['shell command'], html.highlight_syntax(self.shell_command(), 'bash'))]


class ReadFile(_ReadFile):
    """Reads data from a local file"""

    def __init__(self, file_name: str, compression: Compression, target_table: str,
                 mapper_script_file_name: str = None, make_unique: bool = False,
                 db_alias: str = None, csv_format: bool = False, skip_header: bool = False,
                 delimiter_char: str = None, quote_char: str = None,
                 null_value_string: str = None, timezone: str = None) -> None:
        super().__init__(compression=compression,
                         target_table=target_table,
                         mapper_script_file_name=mapper_script_file_name,
                         make_unique=make_unique,
                         db_alias=db_alias,
                         csv_format=csv_format,
                         skip_header=skip_header,
                         delimiter_char=delimiter_char,
                         quote_char=quote_char,
                         null_value_string=null_value_string,
                         timezone=timezone)
        self.file_name = file_name

    def read_file_command(self):
        return f'{uncompressor(self.compression)} "{pathlib.Path(config.data_dir()) / self.file_name}"'

    def html_doc_items(self) -> [(str, str)]:
        return [('file name', _.i[self.file_name])] + super().html_doc_items()


class ReadS3File(_ReadFile):
    """Reads data from a S3 file"""

    def __init__(self, s3_url: str, compression: Compression, target_table: str,
                 mapper_script_file_name: str = None, make_unique: bool = False,
                 db_alias: str = None, csv_format: bool = False, skip_header: bool = False,
                 delimiter_char: str = None, quote_char: str = None,
                 null_value_string: str = None, timezone: str = None,
                 aws_access_key_id: str = None, aws_secret_access_key: str = None, aws_profile: str = None
                 ) -> None:
        super().__init__(compression=compression,
                         target_table=target_table,
                         mapper_script_file_name=mapper_script_file_name,
                         make_unique=make_unique,
                         db_alias=db_alias,
                         csv_format=csv_format,
                         skip_header=skip_header,
                         delimiter_char=delimiter_char,
                         quote_char=quote_char,
                         null_value_string=null_value_string,
                         timezone=timezone)
        self.s3_url = s3_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_profile = aws_profile

    def read_file_command(self):
        env_vars = ''
        if self.aws_access_key_id:
            env_vars = f'  AWS_ACCESS_KEY_ID={self.aws_access_key_id} AWS_SECRET_ACCESS_KEY={self.aws_secret_access_key}'
        profile = ''
        if self.aws_profile:
            profile = f'--profile {self.aws_profile}'
        return (f" {env_vars} aws {profile} s3 cp '{self.s3_url}' - \\\n"
                + f'  | {uncompressor(self.compression)} - ')

    def html_doc_items(self) -> [(str, str)]:
        return [('AWS s3 url', _.i[self.s3_url]),
                ('AWS credentials', _.i[self.aws_access_key_id]) if self.aws_access_key_id else '',
                ('AWS profile', _.i[self.aws_profile]) if self.aws_profile else '',
                ] + super().html_doc_items()


class ReadSQLite(sql._SQLCommand):
    def __init__(self, sqlite_file_name: str, target_table: str,
                 sql_statement: str = None, sql_file_name: str = None, replace: {str: str} = None,
                 db_alias: str = None, timezone: str = None) -> None:
        sql._SQLCommand.__init__(self, sql_statement, sql_file_name, replace)
        self.sqlite_file_name = sqlite_file_name

        self.target_table = target_table
        self._db_alias = db_alias
        self.timezone = timezone

    @property
    def db_alias(self):
        return self._db_alias or config.default_db_alias()

    def shell_command(self):
        return (sql._SQLCommand.shell_command(self)
                + '  | ' + mara_db.shell.copy_command(
                mara_db.dbs.SQLiteDB(file_name=pathlib.Path(config.data_dir()).absolute() / self.sqlite_file_name),
                self.db_alias, self.target_table, timezone=self.timezone))

    def html_doc_items(self) -> [(str, str)]:
        return [('sqlite file name', _.i[self.sqlite_file_name])] \
               + sql._SQLCommand.html_doc_items(self, None) \
               + [('target_table', _.tt[self.target_table]),
                  ('db alias', _.tt[self.db_alias]),
                  ('time zone', _.tt[self.timezone]),
                  (_.i['shell command'], html.highlight_syntax(self.shell_command(), 'bash'))]


class ReadScriptOutput(pipelines.Command):
    """Reads the output from a python script into a database table"""

    def __init__(self, file_name: str, target_table: str, make_unique: bool = False,
                 db_alias: str = None, csv_format: bool = False, skip_header: bool = False,
                 delimiter_char: str = None, quote_char: str = None,
                 null_value_string: str = None, timezone: str = None) -> None:
        super().__init__()
        self.file_name = file_name
        self.make_unique = make_unique

        self.target_table = target_table
        self.csv_format = csv_format
        self.skip_header = skip_header
        self._db_alias = db_alias
        self.delimiter_char = delimiter_char
        self.quote_char = quote_char
        self.null_value_string = null_value_string
        self.timezone = timezone

    def db_alias(self):
        return self._db_alias or config.default_db_alias()

    def shell_command(self):
        return f'{shlex.quote(sys.executable)} "{self.file_path()}" \\\n' \
               + ('  | sort -u \\\n' if self.make_unique else '') \
               + '  | ' + mara_db.shell.copy_from_stdin_command(
            self.db_alias(), csv_format=self.csv_format, target_table=self.target_table, skip_header=self.skip_header,
            delimiter_char=self.delimiter_char, quote_char=self.quote_char,
            null_value_string=self.null_value_string, timezone=self.timezone)

    def file_path(self):
        return self.parent.parent.base_path() / self.file_name

    def html_doc_items(self) -> [(str, str)]:
        return [('file name', _.i[self.file_name]),
                (_.i['content'], html.highlight_syntax(self.file_path().read_text().strip('\n')
                                                       if self.file_name and self.file_path().exists()
                                                       else '', 'python')),
                ('make unique', _.tt[self.make_unique]),
                ('target_table', _.tt[self.target_table]),
                ('db alias', _.tt[self.db_alias()]),
                ('delimiter char',
                 _.tt[json.dumps(self.delimiter_char) if self.delimiter_char != None else None]),
                ('quote char', _.tt[json.dumps(self.quote_char) if self.quote_char != None else None]),
                ('null value string',
                 _.tt[json.dumps(self.null_value_string) if self.null_value_string != None else None]),
                ('time zone', _.tt[self.timezone]),
                (_.i['shell command'], html.highlight_syntax(self.shell_command(), 'bash'))]
