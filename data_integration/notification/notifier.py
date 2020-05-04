import abc

from data_integration import events
from data_integration.logging import pipeline_events


class ChatNotifier(events.EventHandler, abc.ABC):

    def __init__(self):
        """ Abstract class for sending notifications to chat bots when pipeline errors occur"""

        # keep a list of log messages and error log messages for each node
        self.node_output: {tuple: {bool: [events.Event]}} = None


    def handle_event(self, event: events.Event):
        """
        Send notifications for failed tasks and interactively started pipelines

        Args:
            event: The current event of interest
        """

        if isinstance(event, pipeline_events.Output):
            # collect the output and error output of each node so that it can be shown if something fails
            key = tuple(event.node_path)

            if not self.node_output:
                self.node_output = {}

            if not key in self.node_output:
                self.node_output[key] = {True: [], False: []}

            self.node_output[key][event.is_error].append(event)

        elif isinstance(event, pipeline_events.NodeFinished):
            if not event.succeeded and event.is_pipeline is False:
                self.send_task_failed_message(event)


        elif isinstance(event, pipeline_events.RunStarted):
            if event.interactively_started:
                self.send_run_started_interactively_message(event)
            # reset the saved outputs, just to be sure...
            self.node_output = None

        elif isinstance(event, pipeline_events.RunFinished):
            if event.interactively_started:
                self.send_run_finished_interactively_message(event)
            # reset the saved outputs
            self.node_output = None

    @abc.abstractmethod
    def send_run_started_interactively_message(self, event: pipeline_events.RunStarted):
        """Send a notification that somebody manually triggered the run of a pipeline"""
        pass

    @abc.abstractmethod
    def send_run_finished_interactively_message(self, event: pipeline_events.RunFinished):
        """Send a notification that a manually triggered pipeline run finished"""
        pass

    @abc.abstractmethod
    def send_task_failed_message(self, event: pipeline_events.NodeFinished):
        """Send a notification that a task failed"""
        pass
