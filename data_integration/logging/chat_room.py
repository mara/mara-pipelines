import abc
import enum
from data_integration.logging import events

class ChatRoom(abc.ABC):

    class Type(enum.EnumMeta):
        SLACK = 'Slack'
        TEAMS = 'Teams'

    class MessageType(enum.EnumMeta):
        ERROR = "Error"

    @abc.abstractmethod
    def create_msg(self, message_type, node_path: []):
        pass

    @abc.abstractmethod
    def send_msg(self, message):
        pass

    def format_output(self, output_events: [events.Output], chat_type):

        code_markup_start = '```'
        code_markup_end = '```'
        if chat_type == self.Type.TEAMS:
            code_markup_start = '<pre>'
            code_markup_end = '</pre>'

        output, last_format = '', ''
        for event in output_events:
            if event.format == events.Output.Format.VERBATIM:
                if last_format == event.format:
                    # append new verbatim line to the already initialized verbatim output
                    output = output[0:-len(code_markup_end)] + '\n' + event.message + code_markup_end
                else:
                    output += '\n' + code_markup_start + event.message + code_markup_end
            elif event.format == events.Output.Format.ITALICS:
                for line in event.message.splitlines():
                    if chat_type == self.Type.TEAMS:
                        output += '\n\n' + str(line.replace('_', '\\_'))
                    elif chat_type == self.Type.SLACK:
                        output += '\n _ ' + str(line) + ' _ '
            else:
                output = '\n' + event.message

            last_format = event.format
        return output