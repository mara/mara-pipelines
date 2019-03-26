"""Events that are emitted during pipeline execution"""

import abc
import datetime
import json

import enum


class Event():
    def __init__(self) -> None:
        """
        Base class for events that are emitted from mara.
        """

    def to_json(self):
        return json.dumps({field: value.isoformat() if isinstance(value, datetime.datetime) else value
                           for field, value in self.__dict__.items()})


class EventHandler(abc.ABC):
    @abc.abstractmethod
    def handle_event(self, event: Event):
        pass


class PipelineEvent():
    def __init__(self, node_path: [str]) -> None:
        """
        Base class for events that are emitted during a pipeline run.

        Args:
            node_path: The path of the current node in the data pipeline that is run

        """
        self.node_path = node_path

    def to_json(self):
        return json.dumps({field: value.isoformat() if isinstance(value, datetime.datetime) else value
                           for field, value in self.__dict__.items()})


class RunStarted(PipelineEvent):
    def __init__(self, node_path: [str], start_time: datetime.datetime, pid: int) -> None:
        """
        A pipeline run started
        Args:
            node_path: The path of the pipeline that was run
            start_time: The time when the run started
            pid: The process id of the process that runs the pipeline
        """
        super().__init__([])
        self.node_path = node_path
        self.start_time = start_time
        self.pid = pid


class RunFinished(PipelineEvent):
    def __init__(self, node_path: [str], end_time: datetime.datetime, succeeded: bool) -> None:
        """
        A pipeline run finished
        Args:
            node_path: The path of the pipeline that was run
            end_time: The time when the run finished
            succeeded: Whether the run succeeded
        """
        super().__init__([])
        self.node_path = node_path
        self.end_time = end_time
        self.succeeded = succeeded


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
