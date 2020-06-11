"""
Execution of data pipelines.
Uses forking (multiprocessing processes) for parallelism and message queues for inter-process communication.
"""

import datetime
from datetime import timezone as tz
import functools
import multiprocessing
import os
import sys
import signal
import atexit
import time
import traceback
from multiprocessing import queues

from . import pipelines, config
from .logging import logger, pipeline_events, system_statistics, run_log, node_cost
from . import events


def run_pipeline(pipeline: pipelines.Pipeline, nodes: {pipelines.Node} = None,
                 with_upstreams: bool = False,
                 interactively_started: bool = False
                 ) -> [events.Event]:
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

    # use forking for starting child processes to avoid cleanup functions and leakage and pickle problems
    #
    # On newer macs you need to set
    #   OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
    # env variable *before* starting python/flask otherwise you will get core dumps when any forked process calls
    # into certain native code (e.g. requests)! Note that this is done automatically if you create your virtual env
    # via the scripts from mara-app >= 2.1.1
    #
    multiprocessing_context = multiprocessing.get_context('fork')

    # A queue for receiving events from forked sub processes
    event_queue = multiprocessing_context.Queue()

    # The function that is run in a sub process
    def run():

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
            run_start_time = datetime.datetime.now(tz.utc)
            # all nodes that already ran or that won't be run anymore
            processed_nodes: {pipelines.Node} = set()
            # running pipelines with start times and number of running children
            running_pipelines: {pipelines.Pipeline: [datetime.datetime, int]} = {}
            failed_pipelines: {pipelines.Pipeline} = set()  # pipelines with failed tasks
            running_task_processes: {pipelines.Task: TaskProcess} = {}

            # make sure any running tasks are killed when this executor process is shutdown
            executor_pid = os.getpid()

            def ensure_task_processes_killed():
                # as we fork, the TaskProcess also runs this function -> ignore it there
                if os.getpid() != executor_pid: return
                try:
                    for tp in list(running_task_processes.values()):  # type: TaskProcess
                        if tp.is_alive():
                            # give it a chance to gracefully shutdown
                            tp.terminate()
                    statistics_process.kill()
                except BaseException as e:
                    print(f"Exception during TaskProcess cleanup: {repr(e)}", file=sys.stderr, flush=True)
                return

            atexit.register(ensure_task_processes_killed)

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
                        processed_as_parent_failed = False
                        parent = node.parent
                        while parent:
                            # if the parent pipeline failed (and no overwrite), don't launch new nodes
                            # this needs to go down to the ultimate parent as we can have cases where we already
                            # queued a subpipeline and now the parent pipeline failed but the tasks parent pipeline
                            # (the sub pipeline) is not failed.
                            # If a task from a parent pipeline fails, even with force_run_all_children on the
                            # sub pipeline, the sub pipeline would stop. Only if the failed parent pipeline also has
                            # force_run_all_children, the task would get scheduled
                            if parent in failed_pipelines and not parent.force_run_all_children:
                                processed_nodes.add(node)
                                processed_as_parent_failed = True
                                break
                            else: parent = parent.parent
                        if not processed_as_parent_failed:
                            return node

            def track_finished_pipelines():
                """when all nodes of a pipeline have been processed, then emit events"""
                for running_pipeline, (start_time, running_children) \
                    in dict(running_pipelines).items():  # type: pipelines.Pipeline
                    if len(set(running_pipeline.nodes.values()) & processed_nodes) == len(running_pipeline.nodes):
                        succeeded = running_pipeline not in failed_pipelines
                        event_queue.put(pipeline_events.Output(
                            node_path=running_pipeline.path(), format=logger.Format.ITALICS, is_error=not succeeded,
                            message=f'{"succeeded" if succeeded else "failed"}, {logger.format_time_difference(run_start_time, datetime.datetime.now(tz.utc))}'))
                        event_queue.put(pipeline_events.NodeFinished(
                            node_path=running_pipeline.path(), start_time=start_time,
                            end_time=datetime.datetime.now(tz.utc), is_pipeline=True, succeeded=succeeded))
                        del running_pipelines[running_pipeline]
                        processed_nodes.add(running_pipeline)

            # announce run start
            event_queue.put(pipeline_events.RunStarted(node_path=pipeline.path(),
                                                       start_time=run_start_time,
                                                       pid=os.getpid(),
                                                       interactively_started=interactively_started,
                                                       node_ids=[node.id for node in (nodes or [])],
                                                       is_root_pipeline=(pipeline.parent is None))
                            )

            # collect system stats in a separate Process
            statistics_process = multiprocessing.Process(
                target=lambda: system_statistics.generate_system_statistics(event_queue), name='system_statistics')
            statistics_process.start()

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
                            pipeline_start_time = datetime.datetime.now(tz.utc)
                            running_pipelines[next_node] = [pipeline_start_time, 0]
                            event_queue.put(pipeline_events.NodeStarted(next_node.path(), pipeline_start_time, True))
                            event_queue.put(pipeline_events.Output(
                                node_path=next_node.path(), format=logger.Format.ITALICS,
                                message='★ ' + node_cost.format_duration(
                                    node_durations_and_run_times.get(tuple(next_node.path()), [0, 0])[0])))

                        elif isinstance(next_node, pipelines.ParallelTask):
                            # create sub tasks and queue them
                            task_start_time = datetime.datetime.now(tz.utc)
                            try:
                                logger.redirect_output(event_queue, next_node.path())
                                logger.log('☆ Launching tasks', format=logger.Format.ITALICS)
                                sub_pipeline = next_node.launch()
                                next_node.parent.replace(next_node, sub_pipeline)
                                queue([sub_pipeline])

                            except Exception as e:
                                event_queue.put(pipeline_events.NodeStarted(
                                    node_path=next_node.path(), start_time=task_start_time, is_pipeline=True))
                                logger.log(message=f'Could not launch parallel tasks', format=logger.Format.ITALICS,
                                           is_error=True)
                                logger.log(message=traceback.format_exc(),
                                           format=pipeline_events.Output.Format.VERBATIM, is_error=True)
                                event_queue.put(pipeline_events.NodeFinished(
                                    node_path=next_node.path(), start_time=task_start_time,
                                    end_time=datetime.datetime.now(tz.utc), is_pipeline=True, succeeded=False))

                                failed_pipelines.add(next_node.parent)
                                processed_nodes.add(next_node)
                            finally:
                                logger.redirect_output(event_queue, pipeline.path())

                        else:
                            # run a task in a subprocess
                            if next_node.parent in running_pipelines:
                                running_pipelines[next_node.parent][1] += 1
                            event_queue.put(
                                pipeline_events.NodeStarted(next_node.path(), datetime.datetime.now(tz.utc), False))
                            event_queue.put(pipeline_events.Output(
                                node_path=next_node.path(), format=logger.Format.ITALICS,
                                message='★ ' + node_cost.format_duration(
                                    node_durations_and_run_times.get(tuple(next_node.path()), [0, 0])[0])))

                            status_queue = multiprocessing_context.Queue()
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

                        end_time = datetime.datetime.now(tz.utc)
                        event_queue.put(
                            pipeline_events.Output(task_process.task.path(),
                                                   ('succeeded' if succeeded else 'failed') + ',  '
                                                   + logger.format_time_difference(task_process.start_time, end_time),
                                                   format=logger.Format.ITALICS, is_error=not succeeded))
                        event_queue.put(pipeline_events.NodeFinished(task_process.task.path(), task_process.start_time,
                                                                     end_time, False, succeeded))

                # check if some pipelines finished
                track_finished_pipelines()

                # don't busy-wait
                time.sleep(0.001)

        except:
            event_queue.put(pipeline_events.Output(node_path=pipeline.path(), message=traceback.format_exc(),
                                                   format=logger.Format.ITALICS, is_error=True))

        # run again because `dequeue` might have moved more nodes to `finished_nodes`
        track_finished_pipelines()

        # kill the stats process (joining or terminating does not work in gunicorn)
        os.kill(statistics_process.pid, signal.SIGKILL)
        statistics_process.join()

        # run finished
        event_queue.put(pipeline_events.RunFinished(node_path=pipeline.path(), end_time=datetime.datetime.now(tz.utc),
                                                    succeeded=not failed_pipelines,
                                                    interactively_started=interactively_started))

    # fork the process and run `run`
    run_process = multiprocessing_context.Process(target=run, name='pipeline-' + '-'.join(pipeline.path()))
    run_process.start()

    runlogger = run_log.RunLogger()

    # make sure that we close this run (if still open) as failed when we close this python process
    # On SIGKILL we will still leave behind open runs...
    # this needs to run after we forked off the run_process as that one should not inherit the atexit function
    def ensure_closed_run_on_abort():
        try:
            run_log.close_open_run_after_error(runlogger.run_id)
        except BaseException as e:
            print(f"Exception during 'close_open_run_after_error()': {repr(e)}", file=sys.stderr, flush=True)
        return

    atexit.register(ensure_closed_run_on_abort)

    def _notify_all(event):
        try:
            runlogger.handle_event(event)
        except BaseException as e:
            # This includes the case when the mara DB is not reachable when writing the event.
            # Not sure if we should just ignore that, but at least get other notifications
            # out in case of an error
            events.notify_configured_event_handlers(event)
            # this will notify the UI in case of a problem later on
            raise e
        events.notify_configured_event_handlers(event)

    # process messages from forked child processes
    while True:
        try:
            while not event_queue.empty():
                event = event_queue.get(False)
                _notify_all(event)
                yield event
        except queues.Empty:
            pass
        except GeneratorExit:
            # This happens e.g. if the browser window is closed or we reload the page in the middle of a run
            # As we still have open runs, we need to close them as failed.
            run_log.close_open_run_after_error(runlogger.run_id)
            # Catching GeneratorExit needs to end in a return!
            return
        except:
            def _create_exception_output_event(msg: str = None):
                return pipeline_events.Output(node_path=pipeline.path(),
                                              message=(msg + '\n' if msg else '') + traceback.format_exc(),
                                              format=logger.Format.ITALICS, is_error=True)

            output_event = _create_exception_output_event()
            exception_events = []
            try:
                _notify_all(output_event)
            except BaseException as e:
                # we are already in the generic exception handler, so we cannot do anything
                # if we still fail, as we have to get to the final close_open_run_after_error()
                # and 'return'...
                exception_events.append(_create_exception_output_event("Could not notify about final output event"))
            yield output_event
            try:
                run_log.close_open_run_after_error(runlogger.run_id)
            except BaseException as e:
                exception_events.append(_create_exception_output_event("Exception during 'close_open_run_after_error()'"))

            # At least try to notify the UI
            for e in exception_events:
                print(f"{repr(e)}", file=sys.stderr)
                yield e
                events.notify_configured_event_handlers(e)
            # try to terminate the run_process which itself will also cleanup in an atexit handler
            try:
                run_process.terminate()
            except:
                pass
            return
        if not run_process.is_alive():
            # If we are here it might be that the executor dies without sending the necessary run finished events
            ensure_closed_run_on_abort()
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
        self.start_time = datetime.datetime.now(tz.utc)

    def run(self):
        # redirect stdout and stderr to queue
        logger.redirect_output(self.event_queue, self.task.path())

        succeeded = True
        attempt = 0
        try:
            while True:
                if not self.task.run():
                    max_retries = self.task.max_retries or config.default_task_max_retries()
                    if attempt < max_retries:
                        attempt += 1
                        delay = pow(2, attempt + 2)
                        logger.log(message=f'Retry {attempt}/{max_retries} in {delay} seconds',
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
