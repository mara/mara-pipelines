import requests
from data_integration import config
from data_integration.logging import pipeline_events
from data_integration.notification.notifier import ChatNotifier


class Teams(ChatNotifier):
    def __init__(self, token):
        """
        Pipeline notifications via Microsoft Teams

        Args:
            token: The id of the notification web hook, e.g. `'1234abcd-1234-abcd-1234-12345abcdef@1234abcd-1234-abcd-1234-abcde12345/IncomingWebhook/12345678abcdefg/123abc-1235-abcd-1234-12345abcdef`
        """
        super().__init__()
        self.token = token

    def send_run_started_interactively_message(self, event: pipeline_events.RunStarted):
        text = ('<font size="4">&#x1F423;</font> ' + event.user
                + ' manually triggered run of ' +
                ('pipeline [_' + '/'.join(event.node_path).replace("_", "\\_") + '_]' +
                 '(' + (config.base_url() + '/' + '/'.join(event.node_path) + ')'
                        if not event.is_root_pipeline else 'root pipeline')))
        if event.node_ids:
            text += ', nodes ' + ', '.join([f'`{node}`' for node in event.node_ids])
        self._send_message({'text': text})

    def send_run_finished_interactively_message(self, event: pipeline_events.RunFinished):
        if event.succeeded:
            self._send_message({'text': '<font size="4">&#x1F425;</font> <font color="green">succeeded</font>'})
        else:
            self._send_message({'text': '<font size="4">&#x1F424;</font> <font color="red">failed</font>'})

    def send_task_failed_message(self, event: pipeline_events.NodeFinished):
        text = '<font size="4">&#x1F424;</font> Ooops, a hiccup in [_' + '/'.join(event.node_path).replace("_", "\\_") \
               + '_](' + config.base_url() + '/' + '/'.join(event.node_path) + ')'

        key = tuple(event.node_path)

        if self.node_output[key][False]:
            text += self._format_output(self.node_output[key][False])

        if self.node_output[key][True]:
            text += self._format_output(self.node_output[key][True])

        # Shortening message to 2000 because Teams does not display message greater than 28 KB.
        # https://docs.microsoft.com/en-us/microsoftteams/limits-specifications-teams#chat
        if len(text) > 2000:
            text = text[:2000] + '</pre>'

        self._send_message({'text': text})

    def _send_message(self, message):
        response = requests.post(url='https://outlook.office.com/webhook/' + self.token, json=message)

        if response.status_code != 200:
            raise ValueError(f'Could not send message. Status {response.status_code}, response "{response.text}"')

    def _format_output(self, output_events: [pipeline_events.Output]):
        output, last_format = '', ''
        for event in output_events:
            if event.format == pipeline_events.Output.Format.VERBATIM:
                if last_format == event.format:
                    # append new verbatim line to the already initialized verbatim output
                    output = output[0:-len('</pre>')] + f'\n{event.message}</pre>'
                else:
                    output += f'\n<pre>{event.message}</pre>'
            elif event.format == pipeline_events.Output.Format.ITALICS:
                for line in event.message.splitlines():
                    output += '\n\n' + line.replace('_', '\\_')
            else:
                output = f'\n{event.message}'

            last_format = event.format
        return output
