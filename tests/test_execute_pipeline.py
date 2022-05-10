import pytest

from mara_app.monkey_patch import patch

import mara_db.config
patch(mara_db.config.databases)(lambda: {})


def test_execute_without_db():
    from mara_pipelines.commands.python import RunFunction
    from mara_pipelines.pipelines import Pipeline, Task
    from mara_pipelines.ui.cli import run_pipeline

    pipeline = Pipeline(
        id='test_execute_without_db',
        description="Tests if a pipeline can be executed without database")
    
    def command_function() -> bool:
        return True

    pipeline.add(
        Task(id='run_python_function',
             description="Runs a sample python function",
             commands=[RunFunction(function=command_function)]))

    assert run_pipeline(pipeline)