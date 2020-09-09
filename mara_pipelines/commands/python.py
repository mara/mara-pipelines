"""Commands for running python functions and scripts"""

import inspect
import shlex
import sys
import json
from html import escape
from typing import Union, Callable, List
from ..incremental_processing import file_dependencies
from ..logging import logger

from mara_page import html, _
from .. import pipelines


class RunFunction(pipelines.Command):
    def __init__(self, function: Callable = None, args: [str] = None, file_dependencies: [str] = None) -> None:
        """
        Runs an arbitrary python function

        Args:
            function: The parameterless function to run
            args: A list of arguments to be passed to the script
            file_dependencies: Run triggered based on whether a list of files changed since the last pipeline run
        Note:
            if you want to pass arguments, then use a lambda function
        """
        self.function = function
        self.args = args or []
        self.file_dependencies = file_dependencies or []

    def run(self) -> bool:
        dependency_type = 'RunFunction ' + self.function.__name__
        if self.file_dependencies:
            assert (self.parent)
            pipeline_base_path = self.parent.parent.base_path()
            if not file_dependencies.is_modified(self.node_path(), dependency_type,
                                                 pipeline_base_path,
                                                 self.file_dependencies):
                logger.log('no changes')
                return True

        if not self.function(*self.args):
            return False

        if self.file_dependencies:
            file_dependencies.update(self.node_path(), dependency_type, pipeline_base_path, self.file_dependencies)

        return True

    def html_doc_items(self) -> [(str, str)]:
        return [('function', _.pre[escape(str(self.function))]),
                ('args', _.tt[repr(self.args)]),
                (_.i['implementation'], html.highlight_syntax(inspect.getsource(self.function), 'python')),
                ('file dependencies', [_.i[dependency, _.br] for dependency in self.file_dependencies])]


class ExecutePython(pipelines.Command):
    def __init__(self, file_name: Union[Callable, str],
                 args: Union[Callable, List[str]] = None, file_dependencies: [str] = None) -> None:
        """
        Runs a python script in a separate interpreter process

        Args:
            file_name: the path of the file to run, relative to the pipeline directory
            args: A list of arguments to be passed to the script
            file_dependencies: Run triggered based on whether a list of files changed since the last pipeline run
        """
        self._file_name = file_name
        self._args = args or []
        self.file_dependencies = file_dependencies or []

    @property
    def file_name(self):
        return self._file_name() if callable(self._file_name) else self._file_name

    @property
    def args(self):
        return self._args() if callable(self._args) else self._args

    def run(self) -> bool:
        dependency_type = 'ExecutePython ' + self.file_name
        if self.file_dependencies:
            assert (self.parent)
            pipeline_base_path = self.parent.parent.base_path()
            if not file_dependencies.is_modified(self.node_path(), dependency_type,
                                                 pipeline_base_path,
                                                 self.file_dependencies):
                logger.log('no changes')
                return True

        if not super().run():
            return False

        if self.file_dependencies:
            file_dependencies.update(self.node_path(), dependency_type, pipeline_base_path, self.file_dependencies)
        return True

    def shell_command(self):
        return f'{shlex.quote(sys.executable)} -u "{self.parent.parent.base_path() / self.file_name}" {" ".join(map(str, self.args))}'

    def html_doc_items(self):
        path = self.parent.parent.base_path() / self.file_name
        return [
            ('file name', _.i[self.file_name]),
            ('args', _.tt[json.dumps(self.args)]),
            (_.i['content'], html.highlight_syntax(path.read_text().strip('\n') if path.exists() else '',
                                                   'python')),
            (_.i['shell command'], html.highlight_syntax(self.shell_command(), 'bash')),
            ('file dependencies', [_.i[dependency, _.br] for dependency in self.file_dependencies])
        ]
