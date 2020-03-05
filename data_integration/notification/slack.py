"""Slack notifications"""

from data_integration import config
from data_integration.notification.chat_room import ChatRoom
import requests
import os


class Slack(ChatRoom):

    def __init__(self):
        super().__init__(chat_type="Slack", code_markup_start="```", code_markup_end="```",
                         line_start='\n _', line_end=' _ ')

    def create_error_text(self, node_path: []):
        path = '/'.join(node_path)
        text = '\n:baby_chick: Ooops, a hiccup in ' + '_ <' + config.base_url() + '/' + path \
               + ' | ' + path + ' > _'
        return text

    def create_error_msg(self, text, error_log1, error_log2):
        message = {'text': text}
        attachments = [{'text': error_log1, 'mrkdwn_in': ['text']},
                       {'text': error_log2, 'color': '#eb4d5c', 'mrkdwn_in': ['text']}]
        message['attachments'] = attachments
        return message

    def create_run_msg(self, node_path: [], is_root_pipeline: bool):
        msg = (':hatching_chick: *' + (os.environ.get('SUDO_USER') or os.environ.get('USER') or os.getlogin())
               + '* manually triggered run of ' +
               ('pipeline <' + config.base_url() + '/' + '/'.join(node_path) + '|'
                + '/'.join(node_path) + ' >' if not is_root_pipeline else 'root pipeline'))
        return msg

    def create_failure_msg(self):
        return ':baby_chick: failed'

    def create_success_msg(self):
        return ':hatched_chick: succeeded'

    def send_msg(self, message):
        return requests.post(url='https://hooks.slack.com/services/' + config.slack_token(), json=message)
