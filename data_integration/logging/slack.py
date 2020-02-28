"""Slack notifications"""

from .. import config
from ..logging.chat_room import ChatRoom
import requests

class Slack(ChatRoom):

    def create_msg(self, message_type: str, node_path: []):
        text = ''
        if message_type == self.MessageType.ERROR:
            path = '/'.join(node_path)
            text = '\n:baby_chick: Ooops, a hiccup in ' + '_ <' + config.base_url() + '/' + path \
                   + ' | ' + path + ' > _'
        return text

    def send_msg(self, message):
        return requests.post(url='https://hooks.slack.com/services/' + config.slack_token(), json=message)
