"""Notifies the ChatRoom to send message to channels"""

import abc
from data_integration.logging import pipeline_events
from data_integration import events


class ChatNotifier(events.EventHandler, abc.ABC):

    def __init__(self, code_markup_start: str, code_markup_end: str, line_start: str,
                 line_end: str = '', replace_with: str = '_'):

        self.node_output: {tuple: {bool: [events.Event]}} = None
        self.code_markup_start = code_markup_start
        self.code_markup_end = code_markup_end
        self.line_start = line_start
        self.line_end = line_end
        self.replace_with = replace_with
        self.line_end = line_end

    def handle_event(self, event: events.Event):
        """
        Send the output of a node when event occurs.
        Args:
            event: The current event of interest
        """

        if isinstance(event, pipeline_events.Output):
            key = tuple(event.node_path)

            if not self.node_output:
                self.node_output = {}

            if not key in self.node_output:
                self.node_output[key] = {True: [], False: []}

            self.node_output[key][event.is_error].append(event)

        elif isinstance(event, pipeline_events.NodeFinished):
            key = tuple(event.node_path)
            if not event.succeeded and event.is_pipeline is False:

                text = self.create_error_text(node_path=event.node_path)

                log = None
                error_log = None
                if self.node_output[key][False]:
                    log = self.format_output(self.node_output[key][False])
                if self.node_output[key][True]:
                    error_log = self.format_output(self.node_output[key][True])

                message = self.create_error_msg(text, log, error_log)
                response = self.send_msg(message=message)

                if response.status_code != 200:
                    chat_type = str(type(self)).split('.')[-2]
                    raise ValueError(
                        'Request to %s returned an error %s. The response is:\n%s' % (
                            chat_type, response.status_code, response.text)
                    )

        elif isinstance(event, pipeline_events.RunStarted):
            # default handler only handles interactively started runs
            if event.interactively_started:
                text = self.create_run_msg(node_path=event.node_path, is_root_pipeline=event.is_root_pipeline)
                if event.node_ids:
                    text += ', nodes ' + ', '.join([f'`{node}`' for node in event.node_ids])
                self.send_msg(message={'text': text})

        elif isinstance(event, pipeline_events.RunFinished):
            # default handler only handles interactively started runs
            if event.interactively_started:
                if event.succeeded:
                    text = self.create_success_msg()
                else:
                    text = self.create_failure_msg()
                self.send_msg(message={'text': text})

    @abc.abstractmethod
    def create_error_text(self, node_path: []):
        pass

    @abc.abstractmethod
    def create_error_msg(self, text, log, error_log):
        pass

    @abc.abstractmethod
    def create_run_msg(self, node_path: [], is_root_pipeline: bool):
        pass

    @abc.abstractmethod
    def create_failure_msg(self):
        pass

    @abc.abstractmethod
    def create_success_msg(self):
        pass

    @abc.abstractmethod
    def send_msg(self, message):
        pass

    def format_output(self, output_events: [pipeline_events.Output]):

        output, last_format = '', ''
        for event in output_events:
            if event.format == pipeline_events.Output.Format.VERBATIM:
                if last_format == event.format:
                    # append new verbatim line to the already initialized verbatim output
                    output = output[0:-len(self.code_markup_end)] + '\n' + event.message + self.code_markup_end
                else:
                    output += '\n' + self.code_markup_start + event.message + self.code_markup_end
            elif event.format == pipeline_events.Output.Format.ITALICS:
                for line in event.message.splitlines():
                    output += self.line_start + str(line.replace('_', self.replace_with)) + self.line_end
            else:
                output = '\n' + event.message

            last_format = event.format
        return output


