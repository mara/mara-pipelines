"""Teams notifications"""

from .. import config
from ..logging.chat_room import ChatRoom
import requests

class Teams(ChatRoom):
    def create_msg(self, message_type: str, node_path: []):
        text = ""
        if message_type == self.MessageType.ERROR:
            path = '/'.join(node_path)
            path = path.replace("_", "\\_")
            text = '<font size="4">&#x1F424;</font> Ooops, a hiccup in [_' + path + '_](' + config.base_url() + '/' + \
                   '/'.join(node_path) + ')'
        return text

    def send_msg(self, message):
        return requests.post(url='https://outlook.office.com/webhook/' + config.teams_token(), json=message)
