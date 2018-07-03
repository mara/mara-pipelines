"""Commands for running python functions and scripts"""

import inspect
import shlex
import sys
from html import escape
from typing import Union, Callable, List

from data_integration import pipelines
from mara_page import html, _


class RunFunction(pipelines.Command):
    def __init__(self, function: Callable = None) -> None:
        """
        Runs an arbitrary python function

        Args:
            function: The parameterless function to run
        """
        self.run = function

    def html_doc_items(self) -> [(str, str)]:
        return [('function', _.pre[escape(str(self.run))]),
                (_.i['implementation'], html.highlight_syntax(inspect.getsource(self.run), 'python'))]


class ExecutePython(pipelines.Command):
    def __init__(self, file_name: Union[Callable, str],
                 args: Union[Callable, List[str]] = None) -> None:
        """
        Runs a python script in a separate interpreter process

        Args:
            file_name: the path of the file to run, relative to the pipeline directory
            args: A list of arguments to be passed to the script
        """
        self._file_name = file_name
        self._args = args or []

    @property
    def file_name(self):
        return self._file_name() if callable(self._file_name) else self._file_name

    @property
    def args(self):
        return self._args() if callable(self._args) else self._args

    def shell_command(self):
        return f'{shlex.quote(sys.executable)} -u "{self.parent.parent.base_path() / self.file_name}" {" ".join(map(str,self.args))}'

    def html_doc_items(self):
        path = self.parent.parent.base_path() / self.file_name
        return [
            ('file name', _.i[self.file_name]),
            (_.i['content'], html.highlight_syntax(path.read_text().strip('\n') if path.exists() else '',
                                                   'python'))
        ]
