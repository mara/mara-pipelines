"""Events that are emitted during pipeline execution"""

import datetime
import json

import enum
import typing as t

from ..events import Event


class PipelineEvent(Event):
    def __init__(self, node_path: [str]) -> None:
        """
        Base class for events that are emitted during a pipeline run.

        Args:
            node_path: The path of the current node in the data pipeline that is run

        """
        super().__init__()
        self.node_path = node_path


class RunStarted(PipelineEvent):
    def __init__(self, node_path: [str],
                 start_time: datetime.datetime,
                 pid: int,
                 is_root_pipeline: bool = False,
                 node_ids: t.Optional[t.List[str]] = None,
                 interactively_started: bool = False) -> None:
        """
        A pipeline run started
        Args:
            node_path: The path of the pipeline that was run
            start_time: The time when the run started
            pid: The process id of the process that runs the pipeline
            node_ids: list of node.ids which should be run
            is_root_pipeline: whether this pipeline run runs the root pipeline
            interactively_started: whether or not the run was started interactively
        """
        super().__init__([])
        self.node_path = node_path
        self.start_time = start_time
        self.pid = pid
        self.interactively_started = interactively_started
        self.is_root_pipeline = is_root_pipeline
        self.node_ids = node_ids or []

        self.user: str = get_user_display_name(interactively_started)


class RunFinished(PipelineEvent):
    def __init__(self, node_path: [str],
                 end_time: datetime.datetime,
                 succeeded: bool,
                 interactively_started: bool = False) -> None:
        """
        A pipeline run finished
        Args:
            node_path: The path of the pipeline that was run
            end_time: The time when the run finished
            succeeded: Whether the run succeeded
            interactively_started: whether or not the run was started interactively
        """
        super().__init__([])
        self.node_path = node_path
        self.end_time = end_time
        self.succeeded = succeeded
        self.interactively_started = interactively_started


class NodeStarted(PipelineEvent):
    def __init__(self, node_path: [str], start_time: datetime.datetime, is_pipeline: bool) -> None:
        """
        A task run started.
        Args:
            node_path: The path of the current node in the data pipeline that is run
            start_time: The time when the task started
            is_pipeline: Whether the node is a pipeline
        """
        super().__init__(node_path)
        self.start_time = start_time
        self.is_pipeline = is_pipeline


class NodeFinished(PipelineEvent):
    def __init__(self, node_path: [str], start_time: datetime.datetime, end_time: datetime.datetime,
                 is_pipeline: bool, succeeded: bool) -> None:
        """
        A run of a task or pipeline finished.
        Args:
            node_path: The path of the current node in the data pipeline that is run
            start_time: The time when the task started
            end_time: The time when the task finished
            is_pipeline: Whether the node is a pipeline
            succeeded: Whether the task succeeded
        """
        super().__init__(node_path)
        self.start_time = start_time
        self.end_time = end_time
        self.is_pipeline = is_pipeline
        self.succeeded = succeeded


class Output(PipelineEvent):
    class Format(enum.EnumMeta):
        """Formats for displaying log messages"""
        STANDARD = 'standard'
        VERBATIM = 'verbatim'
        ITALICS = 'italics'

    def __init__(self, node_path: [str], message: str,
                 format: Format = Format.STANDARD, is_error: bool = False) -> None:
        """
        Some text output occurred.
        Args:
            node_path: The path of the current node in the data pipeline that is run
            message: The message to display
            format: How to format the message
            is_error: Whether the message is considered an error message
        """
        super().__init__(node_path)
        self.message = message
        self.format = format
        self.is_error = is_error
        self.timestamp = datetime.datetime.now()


def get_user_display_name(interactively_started: bool) -> t.Optional[str]:
    """Gets the display name for the user which started a run

    Defaults to MARA_RUN_USER_DISPLAY_NAME and falls back to the current OS-level name
    if the run was started interactively, else None.

    :param interactively_started: True if the run was triggered interactively

    Patch if you have more sophisticated needs.
    """
    import os
    if 'MARA_RUN_USER_DISPLAY_NAME' in os.environ:
        return os.environ.get('MARA_RUN_USER_DISPLAY_NAME')
    if not interactively_started:
        return None
    return os.environ.get('SUDO_USER') or os.environ.get('USER') or os.getlogin()
