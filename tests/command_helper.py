import inspect

from mara_pipelines.pipelines import Pipeline, Task, Command


def run_command(command: Command, base_path) -> bool:
    """
    Runs a single command
    """
    test_name = inspect.stack()[0][3]

    # we put the command inside a pipeline so that we can define the base path for the
    # command via Pipeline(base_path=...)
    pipeline = Pipeline(
        id=test_name,
        description="Internal pipeline for command execution",
        base_path=base_path)

    pipeline.add(
        Task(id='command',
             description="Task holding the command to execute",
             commands=[command]))

    # execute the command in the current thread
    return command.run()
