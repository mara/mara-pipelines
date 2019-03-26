import inspect
import re
import typing
from html import escape

from mara_page import _, html
from .. import pipelines
from ..commands import python


class ParallelExecutePython(pipelines.ParallelTask):
    def __init__(self, id: str, description: str, file_name: str, parameter_function: typing.Callable,
                 max_number_of_parallel_tasks: int = None, commands_before: [pipelines.Command] = None,
                 commands_after: [pipelines.Command] = None) -> None:
        super().__init__(id=id, description=description, max_number_of_parallel_tasks=max_number_of_parallel_tasks,
                         commands_before=commands_before, commands_after=commands_after)
        self.file_name = file_name
        self.parameter_function = parameter_function

    def add_parallel_tasks(self, sub_pipeline: 'pipelines.Pipeline') -> None:
        parameters = self.parameter_function()

        if not isinstance(parameters, list) or not all(isinstance(item, tuple) for item in parameters):
            raise ValueError(f'parameter function should return a list of tuples, got "{repr(parameters)}"')

        for parameter_tuple in parameters:
            sub_pipeline.add(pipelines.Task(
                id='_'.join([re.sub('[^0-9a-z\-_]+', '', str(x).lower().replace('-', '_')) for x in parameter_tuple]),
                description=f'Runs the script with parameters {repr(parameter_tuple)}',
                commands=[python.ExecutePython(file_name=self.file_name, args=list(parameter_tuple))]))

    def html_doc_items(self) -> [(str, str)]:
        path = self.parent.base_path() / self.file_name
        return [('parameter function',
                 html.highlight_syntax(inspect.getsource(self.parameter_function), 'python')),
                ('file name', _.i[self.file_name]),
                (_.i['file content'], html.highlight_syntax(path.read_text().strip('\n')
                                                            if self.file_name and path.exists()
                                                            else '', 'python'))]


class ParallelRunFunction(pipelines.ParallelTask):
    def __init__(self, id: str, description: str, function: typing.Callable, parameter_function: typing.Callable,
                 max_number_of_parallel_tasks: int = None, commands_before: [pipelines.Command] = None,
                 commands_after: [pipelines.Command] = None) -> None:
        super().__init__(id=id, description=description, max_number_of_parallel_tasks=max_number_of_parallel_tasks,
                         commands_before=commands_before, commands_after=commands_after)
        self.function = function
        self.parameter_function = parameter_function

    def add_parallel_tasks(self, sub_pipeline: 'pipelines.Pipeline') -> None:
        parameters = self.parameter_function()

        if not isinstance(parameters, list):
            raise ValueError(f'parameter function should return a list, got "{repr(parameters)}"')

        for parameter in parameters:
            sub_pipeline.add(pipelines.Task(
                id=str(parameter).lower().replace(' ', '_').replace('-', '_'),
                description=f'Runs the function with parameters {repr(parameter)}',
                commands=[python.RunFunction(lambda args=parameter: self.function(args))]))

    def html_doc_items(self) -> [(str, str)]:
        return [('function', _.pre[escape(str(self.function))]),
                ('parameter function',
                 html.highlight_syntax(inspect.getsource(self.parameter_function), 'python')),
                (_.i['implementation'], html.highlight_syntax(inspect.getsource(self.function), 'python'))]
