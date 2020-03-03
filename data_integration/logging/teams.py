"""Teams notifications"""

from .. import config
from ..logging.chat_room import ChatRoom
import requests
import os


class Teams(ChatRoom):

    def __init__(self):
        super().__init__(chat_type=ChatRoom.ChatType.TEAMS, code_markup_start="<pre>", code_markup_end="</pre>",
                         line_start='\n\n', replace_with='\\_')

    def create_error_text(self, node_path: []):
        path = '/'.join(node_path)
        path = path.replace("_", "\\_")
        text = '<font size="4">&#x1F424;</font> Ooops, a hiccup in [_' + path + '_](' + config.base_url() + '/' + \
               '/'.join(node_path) + ')'
        return text

    def create_error_msg(self, text, error_log1, error_log2):
        return {'text': text + error_log1 + error_log2}

    def create_run_msg(self, pipeline):
        msg = ('<font size="4">&#x1F423;</font> ' + (
                os.environ.get('SUDO_USER') or os.environ.get('USER') or os.getlogin())
               + ' manually triggered run of ' +
               ('pipeline [' + '/'.join(pipeline.path()) + ']' +
                '(' + (config.base_url() + '/' + '/'.join(pipeline.path()) + ')'
                       if pipeline.parent else 'root pipeline')))
        return msg

    def create_failure_msg(self):
        return '<font size="4">&#x1F424;</font> <font color="red">failed</font>'

    def create_success_msg(self):
        return '<font size="4">&#x1F425;</font> <font color="green">succeeded</font>'

    def send_msg(self, message):
        return requests.post(url='https://outlook.office.com/webhook/' + config.teams_token(), json=message)
