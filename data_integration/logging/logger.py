"""Text output logging with redirection to queues"""

import multiprocessing
import sys
from datetime import datetime

from ..logging import events
import data_integration.config

Format = events.Output.Format


def log(message: str, format: events.Output.Format = Format.STANDARD,
        is_error: bool = False) -> None:
    """
    Logs text messages.

    When run inside a pipeline, this will send a log message to the parent process.
    Otherwise, messages will be printed to `sys.stdout` and `sys.stderr`.

    Any string in `data_integration.config.password_masks()` will be replaced by '***'.

    Args:
        message: The message to display
        format: How to format the message
        is_error: Whether the message is considered an error message
    """
    message = message.rstrip()
    masks = data_integration.config.password_masks()
    if masks:
        for mask in masks:
            message = message.replace(mask, '***')
    if message:
        if _event_queue:
            _event_queue.put(events.Output(_node_path, message, format, is_error))
        elif is_error:
            sys.stderr.write(message + '\n')
        else:
            sys.stdout.write(message + '\n')


_event_queue: multiprocessing.Queue = None
"""When running in a forked process, this will be bound to a queue for sending events to the parent process."""

_current_node_path = None
"""When running in a forked task process, this will be bound to the path of the currently running pipeline node"""


def redirect_output(event_queue: multiprocessing.Queue, node_path: [str]):
    """
    Redirects the output of the `log` function as well as `sys.stdout` and `sys.stderr` to `event_queue`
    Args:
        queue: The queue where to redirect messages to
        node_path: The id and parent ids of the currently running tasks
    """
    global _event_queue
    _event_queue = event_queue

    global _node_path
    _node_path = node_path

    class OutputRedirector():
        def __init__(self, is_error: bool) -> None:
            self.is_error = is_error

        def write(self, message):
            log(message=message, format=Format.VERBATIM, is_error=self.is_error)

        def flush(self):
            pass

    sys.stdout = OutputRedirector(is_error=False)
    sys.stderr = OutputRedirector(is_error=True)


def format_time_difference(t1: datetime, t2: datetime):
    """
    Displays the time difference from t1 to t2 in a human - readable form.
    Inspired by https://stackoverflow.com/a/11157649/243519
    """
    import dateutil.relativedelta

    difference = dateutil.relativedelta.relativedelta(t2, t1)
    return ', '.join([str(getattr(difference, attr)) + ' ' + attr for attr in
                      ['years', 'months', 'days', 'hours', 'minutes', 'seconds']
                      if getattr(difference, attr) or attr == 'seconds'])


if __name__ == "__main__":
    # normal case when not run from a pipeline execution
    log(message='hello world')  # -> stdout
    log(message='oops', is_error=True)  # -> stderr
