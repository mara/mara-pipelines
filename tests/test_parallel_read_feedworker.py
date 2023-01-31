"""Various tests in connection with the ParallelTask where 'use_workers' is True. This activates the FeedWorkerProcess."""

import pytest
import inspect


from mara_pipelines.commands.python import RunFunction
from mara_app.monkey_patch import patch
from mara_pipelines.pipelines import ParallelTask, Command
import mara_pipelines.config


# the tests are executed without database
import mara_db.config
patch(mara_db.config.databases)(lambda: {})



def method_name():
    return inspect.stack()[1][3]


class SimpleParallelProcess(ParallelTask):
    def __init__(self, id: str, description: str,
        function: callable, number_of_tasks: int = 10, max_number_of_parallel_tasks: int = None, commands_before: [Command] = None, commands_after: [Command] = None) -> None:
        super().__init__(id, description, max_number_of_parallel_tasks, commands_before, commands_after)
        self.use_workers = True
        self.function = function
        self.number_of_tasks = number_of_tasks

    def feed_workers(self):
        for n in range(1, self.number_of_tasks):
            yield RunFunction(function=self.function, args=[n])


def simple_print_call(n: str):
    print(f"Task {n}")
    return True


def test_simple_parallel_process_succeeded():
    from mara_pipelines.pipelines import Pipeline
    from mara_pipelines.ui.cli import run_pipeline

    pipeline = Pipeline(id=method_name(), description="")

    pipeline.add(
        SimpleParallelProcess('simple_parallel_task', description="", function=simple_print_call))

    assert run_pipeline(pipeline)


def simple_print_call_fail(n: str):
    print(f"Task {n}")
    return False

def test_simple_parallel_process_fail():
    from mara_pipelines.pipelines import Pipeline
    from mara_pipelines.ui.cli import run_pipeline

    pipeline = Pipeline(id=method_name(), description="")

    pipeline.add(
        SimpleParallelProcess('simple_parallel_task', description="", function=simple_print_call_fail))

    assert not run_pipeline(pipeline)


def test_queue_stress_fail():
    """
    Sometimes the queue for tasks is full and the FeedWorkerProcess tries and tries
    again to fill in the recived commands into the internal queue, but it cannot
    succeed because all worker nodes failed.

    This is a test for this case: The main execution loop in this case must
    react on the worker process failed message and kill the FeedWorkerProcess.
    """

    from mara_pipelines.pipelines import Pipeline
    from mara_pipelines.ui.cli import run_pipeline

    pipeline = Pipeline(id=method_name(), description="")

    # we stress the number of tasks to be above the maximum of
    # the defined queue. We want to stress to come in a endless loop inside
    # the feed worker since 1. all worker will fail and 2. the queue will
    # be overloaded. The feed worker will retry and retry again to fill in
    # the tasks into the queue.
    number_of_tasks = 110 * mara_pipelines.config.max_number_of_parallel_tasks()

    pipeline.add(
        SimpleParallelProcess('simple_parallel_task', description="", function=simple_print_call_fail,
            number_of_tasks=number_of_tasks))

    assert not run_pipeline(pipeline)
