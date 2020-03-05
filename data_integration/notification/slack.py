"""Slack notifications for failed node runs"""

from data_integration import config
from data_integration.logging import pipeline_events
from data_integration import events


class Slack(events.EventHandler):
    node_output: {tuple: {bool: [events.Event]}} = None

    def handle_event(self, event: events.Event):
        """
        Send the output of a node to Slack when the node failed.
        Args:
            event: The current event of interest
        """
        import requests

        if isinstance(event, pipeline_events.Output):
            key = tuple(event.node_path)

            if not self.node_output:
                self.node_output = {}

            if not key in self.node_output:
                self.node_output[key] = {True: [], False: []}

            self.node_output[key][event.is_error].append(event)

        elif isinstance(event, pipeline_events.NodeFinished):
            key = tuple(event.node_path)
            if not event.succeeded and event.is_pipeline is False:

                message = {'text': '\n:baby_chick: Ooops, a hiccup in '
                                   + '_ <' + config.base_url() + '/' + '/'.join(event.node_path)
                                   + ' | ' + '/'.join(event.node_path) + ' > _',
                           'attachments': []}

                if (self.node_output[key][False]):
                    message['attachments'].append({'text': self.format_output(self.node_output[key][False]),
                                                   'mrkdwn_in': ['text']})

                if (self.node_output[key][True]):
                    message['attachments'].append({'text': self.format_output(self.node_output[key][True]),
                                                   'color': '#eb4d5c', 'mrkdwn_in': ['text']})

                response = requests.post('https://hooks.slack.com/services/' + config.slack_token(), json=message)

                if response.status_code != 200:
                    raise ValueError(
                        'Request to slack returned an error %s. The response is:\n%s' % (
                            response.status_code, response.text)
                    )
        elif isinstance(event, pipeline_events.RunStarted):
            # default handler only handles interactively started runs
            if event.interactively_started:
                message = f':hatching_chick: *{event.user}* manually triggered run of '
                message += ('pipeline <' + config.base_url() + '/' + '/'.join(event.node_path) + '|'
                            + '/'.join(event.node_path) + ' >' if not event.is_root_pipeline else 'root pipeline')

                if event.node_ids:
                    message += ', nodes ' + ', '.join([f'`{id_}`' for id_ in event.node_ids])

                requests.post('https://hooks.slack.com/services/' + config.slack_token(), json={'text': message})
        elif isinstance(event, pipeline_events.RunFinished):
            # default handler only handles interactively started runs
            if event.interactively_started:
                if event.succeeded:
                    msg = ':hatched_chick: succeeded'
                else:
                    msg = ':baby_chick: failed'
                requests.post('https://hooks.slack.com/services/' + config.slack_token(),
                              json={'text': msg})

    def format_output(self, output_events: [pipeline_events.Output]):
        output, last_format = '', ''
        for event in output_events:
            if event.format == pipeline_events.Output.Format.VERBATIM:
                if last_format == event.format:
                    # append new verbatim line to the already initialized verbatim output
                    output = output[0:-3] + '\n' + event.message + '```'
                else:
                    output += '\n' + '```' + event.message + '```'
            elif event.format == pipeline_events.Output.Format.ITALICS:
                for line in event.message.splitlines():
                    output += '\n _ ' + str(line) + ' _ '
            else:
                output = '\n' + event.message

            last_format = event.format
        return output
