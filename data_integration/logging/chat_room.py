import abc
from data_integration.logging import events


class ChatRoom(abc.ABC):

    def __init__(self, chat_type, code_markup_start: str, code_markup_end: str, line_start: str,
                 line_end: str = '', replace_with: str = '_'):
        self.chat_type = chat_type
        self.code_markup_start = code_markup_start
        self.code_markup_end = code_markup_end
        self.line_start = line_start
        self.line_end = line_end
        self.replace_with = replace_with
        self.line_end = line_end

    @abc.abstractmethod
    def create_error_text(self, node_path: []):
        pass

    @abc.abstractmethod
    def create_error_msg(self, text, error_log1, error_log2):
        pass

    @abc.abstractmethod
    def create_run_msg(self, pipeline):
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

    def format_output(self, output_events: [events.Output]):

        output, last_format = '', ''
        for event in output_events:
            if event.format == events.Output.Format.VERBATIM:
                if last_format == event.format:
                    # append new verbatim line to the already initialized verbatim output
                    output = output[0:-len(self.code_markup_end)] + '\n' + event.message + self.code_markup_end
                else:
                    output += '\n' + self.code_markup_start + event.message + self.code_markup_end
            elif event.format == events.Output.Format.ITALICS:
                for line in event.message.splitlines():
                    output += self.line_start + str(line.replace('_', self.replace_with)) + self.line_end
            else:
                output = '\n' + event.message

            last_format = event.format
        return output
