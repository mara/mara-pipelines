import requests
from data_integration import config
from data_integration.logging import pipeline_events
from data_integration.notification.notifier import ChatNotifier


class Slack(ChatNotifier):

    def __init__(self, token):
        """
        Pipeline notifications via Slack

        Args:
            token: The incoming webhook id to send data to, e.g. '123ABC/123ABC/123abc123abc123abc'
        """
        super().__init__()
        self.token = token

    def send_run_started_interactively_message(self, event: pipeline_events.RunStarted):
        text = (':hatching_chick: *' + event.user
                + '* manually triggered run of ' +
                ('pipeline <' + config.base_url() + '/' + '/'.join(event.node_path) + '|'
                 + '/'.join(event.node_path) + ' >' if not event.is_root_pipeline else 'root pipeline'))

        if event.node_ids:
            text += ', nodes ' + ', '.join([f'`{node}`' for node in event.node_ids])
        self._send_message({'text': text})

    def send_run_finished_interactively_message(self, event: pipeline_events.RunFinished):
        if event.succeeded:
            self._send_message({'text': ':hatched_chick: succeeded'})
        else:
            self._send_message({'text': ':baby_chick: failed'})

    def send_task_failed_message(self, event: pipeline_events.NodeFinished):
        path = '/'.join(event.node_path)
        text = '\n:baby_chick: Ooops, a hiccup in ' + '_ <' + config.base_url() + '/' + path \
               + ' | ' + path + ' > _'

        attachments = []
        key = tuple(event.node_path)

        if self.node_output[key][False]:
            attachments.append({
                'text': self._format_output(self.node_output[key][False]),
                'mrkdwn_in': ['text']
            })

        if self.node_output[key][True]:
            attachments.append({
                'text': self._format_output(self.node_output[key][True]),
                'color': '#eb4d5c',
                'mrkdwn_in': ['text']
            })

        self._send_message({'text': text, 'attachments': attachments})

    def _send_message(self, message):
        response = requests.post(url='https://hooks.slack.com/services/' + self.token, json=message)
        if response.status_code != 200:
            raise ValueError(f'Could not send message. Status {response.status_code}, response "{response.text}"')

    def _format_output(self, output_events: [pipeline_events.Output]):
        output, last_format = '', ''
        for event in output_events:
            if event.format == pipeline_events.Output.Format.VERBATIM:
                if last_format == event.format:
                    # append new verbatim line to the already initialized verbatim output
                    output = output[0:-3] + f'\n{event.message}```'
                else:
                    output += f'```{event.message}```'
            elif event.format == pipeline_events.Output.Format.ITALICS:
                for line in event.message.splitlines():
                    output += f'\n _{line} _ '
            else:
                output = f'\n{event.message}'

            last_format = event.format
        return output
