"""
Execution of data pipelines.
Uses forking (multiprocessing processes) for parallelism and message queues for inter-process communication.
"""

import datetime
import functools
import multiprocessing
import os
import signal
import time
import traceback
from multiprocessing import queues

from . import pipelines, config
from .logging import logger, events, system_statistics, run_log, node_cost, slack


def run_pipeline(pipeline: pipelines.Pipeline, nodes: {pipelines.Node} = None,
                 with_upstreams: bool = False) -> [events.Event]:
    """
    Runs a pipeline in a forked sub process. Acts as a generator that yields events from the sub process.

    Using forking has two advantages:
    1. The pipeline is also forked and thus can be modified without affecting the original pipeline.
    2. It's possible to hand over control to the parent process while the pipeline is running, for example
       for sending output to a browser.

    Args:
        pipeline: The pipeline to run
        nodes: A list of pipeline children that should run
        with_upstreams: When true and `nodes` are provided, then all upstreams of `nodes` in `pipeline` are also run
    Yields:
        Events emitted during pipeline execution
    """
    # A queue for receiving events from forked sub processes
    event_queue = multiprocessing.Queue()

    # The function that is run in a sub process
    def run():

        # collect system stats in a separate Process
        statistics_process = multiprocessing.Process(
            target=lambda: system_statistics.generate_system_statistics(event_queue), name='system_statistics')
        statistics_process.start()

        try:
            # capture output of print statements and other unplanned output
            logger.redirect_output(event_queue, pipeline.path())

            # all nodes that have not run yet, ordered by priority
            node_queue: [pipelines.Node] = []

            # data needed for computing cost
            node_durations_and_run_times = node_cost.node_durations_and_run_times(pipeline)

            # Putting nodes into the node queue
            def queue(nodes: [pipelines.Node]):
                for node in nodes:
                    node_cost.compute_cost(node, node_durations_and_run_times)
                    node_queue.append(node)
                node_queue.sort(key=lambda node: node.cost, reverse=True)

            if nodes:  # only run a set of child nodes
                def with_all_upstreams(nodes: {pipelines.Node}):
                    """recursively find all upstreams of a list of nodes"""
                    return functools.reduce(set.union, [with_all_upstreams(node.upstreams) for node in nodes], nodes)

                # when requested, include all upstreams of nodes, otherwise just use provided nodes
                nodes_to_run = with_all_upstreams(set(nodes)) if with_upstreams else set(nodes)

                # remove everything from pipeline that should not be run
                # (that's makes updating dependencies between nodes easier)
                for node in set(pipeline.nodes.values()) - nodes_to_run:
                    pipeline.remove(node)

                # queue remaining nodes
                queue(list((pipeline.nodes).values()))

            else:
                # remove dependencies to siblings
                pipeline.upstreams = set()
                pipeline.downstreams = set()
                # queue whole pipeline
                queue([pipeline])

            # book keeping
            run_start_time = datetime.datetime.now()
            # all nodes that already ran or that won't be run anymore
            processed_nodes: {pipelines.Node} = set()
            # running pipelines with start times and number of running children
            running_pipelines: {pipelines.Pipeline: [datetime.datetime, int]} = {}
            failed_pipelines: {pipelines.Pipeline} = set()  # pipelines with failed tasks
            running_task_processes: {pipelines.Task: TaskProcess} = {}

            def dequeue() -> pipelines.Node:
                """
                Finds the next task in the queue
                - without upstreams or where all upstreams have been run already
                - where the pipeline specific maximum number of parallel tasks per pipeline is not reached
                """
                for node in node_queue:  # type: pipelines.Node
                    if ((not node.upstreams or len(node.upstreams & processed_nodes) == len(node.upstreams))
                            and (not isinstance(node.parent, pipelines.Pipeline)
                                 or (not node.parent.max_number_of_parallel_tasks)
                                 or (not node.parent in running_pipelines)
                                 or (running_pipelines[node.parent][1] < node.parent.max_number_of_parallel_tasks))):
                        node_queue.remove(node)
                        if node.parent in failed_pipelines and not node.parent.force_run_all_children:
                            # if the parent pipeline failed (and no overwrite), don't launch new nodes
                            processed_nodes.add(node)
                        else:
                            return node

            def track_finished_pipelines():
                """when all nodes of a pipeline have been processed, then emit events"""
                for running_pipeline, (start_time, running_children) \
                        in dict(running_pipelines).items():  # type: pipelines.Pipeline
                    if len(set(running_pipeline.nodes.values()) & processed_nodes) == len(running_pipeline.nodes):
                        succeeded = running_pipeline not in failed_pipelines
                        event_queue.put(events.Output(
                            node_path=running_pipeline.path(), format=logger.Format.ITALICS, is_error=not succeeded,
                            message=f'{"succeeded" if succeeded else "failed"}, {logger.format_time_difference(run_start_time, datetime.datetime.now())}'))
                        event_queue.put(events.NodeFinished(
                            node_path=running_pipeline.path(), start_time=start_time,
                            end_time=datetime.datetime.now(), is_pipeline=True, succeeded=succeeded))
                        del running_pipelines[running_pipeline]
                        processed_nodes.add(running_pipeline)

            # announce run start
            event_queue.put(events.RunStarted(node_path=pipeline.path(), start_time=run_start_time, pid=os.getpid()))

            # run as long
            # - as task processes are still running
            # - as there is still stuff in the node queue
            while running_task_processes or node_queue:
                # don't do anything if the maximum number of parallel tasks is currently running
                if len(running_task_processes) < config.max_number_of_parallel_tasks():

                    next_node = dequeue()  # get the next runnable node from the queue

                    if next_node:
                        if isinstance(next_node, pipelines.Pipeline):
                            # connect pipeline nodes without upstreams to upstreams of pipeline
                            for upstream in next_node.upstreams:
                                for pipeline_node in next_node.nodes.values():
                                    if not pipeline_node.upstreams:
                                        next_node.add_dependency(upstream, pipeline_node)

                            # connect pipeline nodes without downstreams to downstream of pipeline
                            for downstream in next_node.downstreams:
                                for pipeline_node in next_node.nodes.values():
                                    if not pipeline_node.downstreams:
                                        next_node.add_dependency(pipeline_node, downstream)

                            # get cost information for children
                            node_durations_and_run_times.update(node_cost.node_durations_and_run_times(next_node))

                            # queue all child nodes
                            queue(list(next_node.nodes.values()))

                            # book keeping and event emission
                            pipeline_start_time = datetime.datetime.now()
                            running_pipelines[next_node] = [pipeline_start_time, 0]
                            event_queue.put(events.NodeStarted(next_node.path(), pipeline_start_time, True))
                            event_queue.put(events.Output(
                                node_path=next_node.path(), format=logger.Format.ITALICS,
                                message='★ ' + node_cost.format_duration(
                                    node_durations_and_run_times.get(tuple(next_node.path()), [0, 0])[0])))

                        elif isinstance(next_node, pipelines.ParallelTask):
                            # create sub tasks and queue them
                            task_start_time = datetime.datetime.now()
                            try:
                                logger.redirect_output(event_queue, next_node.path())
                                logger.log('☆ Launching tasks', format=logger.Format.ITALICS)
                                sub_pipeline = next_node.launch()
                                next_node.parent.replace(next_node, sub_pipeline)
                                queue([sub_pipeline])

                            except Exception as e:
                                event_queue.put(events.NodeStarted(
                                    node_path=next_node.path(), start_time=task_start_time, is_pipeline=True))
                                logger.log(message=f'Could not launch parallel tasks', format=logger.Format.ITALICS,
                                           is_error=True)
                                logger.log(message=traceback.format_exc(),
                                           format=events.Output.Format.VERBATIM, is_error=True)
                                event_queue.put(events.NodeFinished(
                                    node_path=next_node.path(), start_time=task_start_time,
                                    end_time=datetime.datetime.now(), is_pipeline=True, succeeded=False))

                                failed_pipelines.add(next_node.parent)
                                processed_nodes.add(next_node)
                            finally:
                                logger.redirect_output(event_queue, pipeline.path())

                        else:
                            # run a task in a subprocess
                            if next_node.parent in running_pipelines:
                                running_pipelines[next_node.parent][1] += 1
                            event_queue.put(events.NodeStarted(next_node.path(), datetime.datetime.now(), False))
                            event_queue.put(events.Output(
                                node_path=next_node.path(), format=logger.Format.ITALICS,
                                message='★ ' + node_cost.format_duration(
                                    node_durations_and_run_times.get(tuple(next_node.path()), [0, 0])[0])))

                            status_queue = multiprocessing.Queue()
                            process = TaskProcess(next_node, event_queue, status_queue)
                            process.start()
                            running_task_processes[next_node] = process

                # check whether some of the running processes finished
                for task_process in list(running_task_processes.values()):  # type: TaskProcess
                    if task_process.is_alive():
                        pass
                    else:
                        del running_task_processes[task_process.task]
                        if task_process.task.parent in running_pipelines:
                            running_pipelines[task_process.task.parent][1] -= 1

                        processed_nodes.add(task_process.task)

                        succeeded = not (task_process.status_queue.get() == False or task_process.exitcode != 0)
                        if not succeeded and not task_process.task.parent.ignore_errors:
                            for parent in task_process.task.parents()[:-1]:
                                failed_pipelines.add(parent)

                        end_time = datetime.datetime.now()
                        event_queue.put(
                            events.Output(task_process.task.path(),
                                          ('succeeded' if succeeded else 'failed') + ',  '
                                          + logger.format_time_difference(task_process.start_time, end_time),
                                          format=logger.Format.ITALICS, is_error=not succeeded))
                        event_queue.put(events.NodeFinished(task_process.task.path(), task_process.start_time,
                                                            end_time, False, succeeded))

                # check if some pipelines finished
                track_finished_pipelines()

                # don't busy-wait
                time.sleep(0.001)

        except:
            event_queue.put(events.Output(node_path=pipeline.path(), message=traceback.format_exc(),
                                          format=logger.Format.ITALICS, is_error=True))

        # run again because `dequeue` might have moved more nodes to `finished_nodes`
        track_finished_pipelines()

        # kill the stats process (joining or terminating does not work in gunicorn)
        os.kill(statistics_process.pid, signal.SIGKILL)
        statistics_process.join()

        # run finished
        event_queue.put(events.RunFinished(node_path=pipeline.path(), end_time=datetime.datetime.now(),
                                           succeeded=not failed_pipelines))

    # fork the process and run `run`
    run_process = multiprocessing.Process(target=run, name='pipeline-' + '-'.join(pipeline.path()))
    run_process.start()

    # todo: make event handlers configurable (e.g. for slack)
    event_handlers = [run_log.RunLogger()]

    if config.slack_token():
        event_handlers.append(slack.Slack())

    # process messages from forked child processes
    while True:
        try:
            while not event_queue.empty():
                event = event_queue.get(False)
                for event_handler in event_handlers:
                    event_handler.handle_event(event)
                yield event
        except queues.Empty:
            pass
        except:
            yield events.Output(node_path=pipeline.path(), message=traceback.format_exc(),
                                format=logger.Format.ITALICS, is_error=True)
            return
        if not run_process.is_alive():
            break
        time.sleep(0.001)


class TaskProcess(multiprocessing.Process):
    def __init__(self, task: pipelines.Task, event_queue: multiprocessing.Queue, status_queue: multiprocessing.Queue):
        """
        Runs a task in a separate sub process.

        Args:
            task: The task to run
            event_queue: The query for writing events to
            status_queue: A queue for reporting whether the task succeeded
        """
        super().__init__(name='task-' + '-'.join(task.path()))
        self.task = task
        self.event_queue = event_queue
        self.status_queue = status_queue
        self.start_time = datetime.datetime.now()

    def run(self):
        # redirect stdout and stderr to queue
        logger.redirect_output(self.event_queue, self.task.path())

        succeeded = True
        attempt = 0
        try:
            while True:
                if not self.task.run():
                    if attempt < self.task.max_retries:
                        attempt += 1
                        delay = pow(2, attempt + 2)
                        logger.log(message=f'Retry {attempt}/{self.task.max_retries} in {delay} seconds',
                                   is_error=True, format=logger.Format.ITALICS)
                        time.sleep(delay)
                    else:
                        succeeded = False
                        break
                else:
                    break
        except Exception as e:
            logger.log(message=traceback.format_exc(), format=logger.Format.VERBATIM, is_error=True)
            succeeded = False

        self.status_queue.put(succeeded)
