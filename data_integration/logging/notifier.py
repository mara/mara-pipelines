"""Notifies the ChatRoom to send message to channels"""

from data_integration.logging import events
from ..logging.chat_room import ChatRoom

class Notifier(events.EventHandler):

    def __init__(self, chat_rooms: [ChatRoom]):
        self.chat_rooms = chat_rooms
        self.node_output: {tuple: {bool: [events.Event]}} = None

    def handle_event(self, event: events.Event):
        """
        Send the output of a node when the node failed.
        Args:
            event: The current event of interest
        """

        if isinstance(event, events.Output):
            key = tuple(event.node_path)

            if not self.node_output:
                self.node_output = {}

            if not key in self.node_output:
                self.node_output[key] = {True: [], False: []}

            self.node_output[key][event.is_error].append(event)

        elif isinstance(event, events.NodeFinished):
            key = tuple(event.node_path)
            if not event.succeeded and event.is_pipeline is False:
                for chat_room in self.chat_rooms:

                    text = chat_room.create_error_text(node_path=event.node_path)

                    error_log1 = ''
                    error_log2 = ''
                    if self.node_output[key][False]:
                        error_log1 = chat_room.format_output(self.node_output[key][False])
                    if self.node_output[key][True]:
                        error_log2 = chat_room.format_output(self.node_output[key][True])

                    message = chat_room.create_error_msg(text, error_log1, error_log2)
                    response = chat_room.send_msg(message=message)

                    if response.status_code != 200:
                        raise ValueError(
                            'Request to %s returned an error %s. The response is:\n%s' % (
                                chat_room.chat_type, response.status_code, response.text)
                        )
