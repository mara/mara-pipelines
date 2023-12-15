import pytest
from typing import List

from mara_pipelines.pipelines import Task
from mara_pipelines.commands.python import RunFunction

class _PythonFuncTestResult:
    has_run = False


def test_run_task():
    """
    A simple test executing a task.
    """
    test_result = _PythonFuncTestResult()

    def python_test_function(result: _PythonFuncTestResult):
        result.has_run = True  # noqa: F841

    assert not test_result.has_run

    task = Task(
        id='run_task',
        description="Unit test test_run_task",
        commands=[RunFunction(function=python_test_function, args=[test_result])])

    assert not test_result.has_run

    task.run()

    assert test_result.has_run


def test_run_task_dynamic_commands():
    """
    A simple test executing a task with callable commands
    """
    import mara_pipelines.ui.node_page

    test_result = _PythonFuncTestResult()

    def python_test_function(result: _PythonFuncTestResult):
        result.has_run = True  # noqa: F841

    def generate_command_list() -> List:
        yield RunFunction(function=lambda t: python_test_function(t), args=[test_result])

    assert not test_result.has_run

    task = Task(
        id='run_task_dynamic_commands',
        description="Unit test test_run_task_dynamic_commands",
        commands=generate_command_list)

    assert not test_result.has_run

    content = mara_pipelines.ui.node_page.node_content(task)
    assert content

    assert not test_result.has_run

    task.run()

    assert test_result.has_run
