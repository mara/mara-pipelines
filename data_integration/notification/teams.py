"""Teams notifications"""

from data_integration import config
from data_integration.notification.notifier import ChatNotifier
import requests
import os


class Teams(ChatNotifier):

    def __init__(self, token):
        super().__init__(code_markup_start="<pre>", code_markup_end="</pre>",
                         line_start='\n\n', replace_with='\\_')
        self.token = token

    def create_error_text(self, node_path: []):
        path = '/'.join(node_path)
        path = path.replace("_", "\\_")
        text = '<font size="4">&#x1F424;</font> Ooops, a hiccup in [_' + path + '_](' + config.base_url() + '/' + \
               '/'.join(node_path) + ')'
        return text

    def create_error_msg(self, text, log, error_log):

        whole_text = text + log + error_log
        # Shortening message to 2000 because Teams does not display message greater than 28 KB.
        # https://docs.microsoft.com/en-us/microsoftteams/limits-specifications-teams#chat
        if len(whole_text) > 2000:
            whole_text = whole_text[:2000] + '</pre>'
        return {'text': whole_text}

    def create_run_msg(self, node_path: [], is_root_pipeline: bool):
        path = '/'.join(node_path)
        path = path.replace("_", "\\_")
        msg = ('<font size="4">&#x1F423;</font> ' + (
                os.environ.get('SUDO_USER') or os.environ.get('USER') or os.getlogin())
               + ' manually triggered run of ' +
               ('pipeline [_' + path + '_]' +
                '(' + (config.base_url() + '/' + '/'.join(node_path) + ')'
                       if not is_root_pipeline else 'root pipeline')))
        return msg

    def create_failure_msg(self):
        return '<font size="4">&#x1F424;</font> <font color="red">failed</font>'

    def create_success_msg(self):
        return '<font size="4">&#x1F425;</font> <font color="green">succeeded</font>'

    def send_msg(self, message):
        return requests.post(url='https://outlook.office.com/webhook/' + self.token, json=message)
