"""Slack notifications for failed node runs"""

from .. import config
from ..logging import events
from .. import event_base
from ..ui import cli


class Slack(event_base.EventHandler):
    node_output: {tuple: {bool: [event_base.Event]}} = None

    def handle_event(self, event: event_base.Event):
        """
        Send the output of a node to Slack when the node failed.
        Args:
            event: The current event of interest
        """
        import requests

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
        elif isinstance(event, cli.PipelineStartEvent):
            # default handler only handles manually started runs
            if event.manually_started:
                message = f':hatching_chick: *{event.user}* manually triggered run of '
                message +=  ('pipeline <' + config.base_url() + '/' + '/'.join(event.pipeline.path()) + '|'
                            + '/'.join(event.pipeline.path()) + ' >' if event.pipeline.parent else 'root pipeline')

                if event.nodes:
                    message += ', nodes ' + ', '.join([f'`{node.id}`' for node in event.nodes])

                requests.post('https://hooks.slack.com/services/' + config.slack_token(), json={'text': message})
        elif isinstance(event, cli.PipelineEndEvent):
            # default handler only handles manually started runs
            if event.manually_started:
                if event.success:
                    msg = ':hatched_chick: succeeded'
                else:
                    msg = ':baby_chick: failed'
                requests.post('https://hooks.slack.com/services/' + config.slack_token(),
                              json={'text': msg})

    def format_output(self, output_events: [events.Output]):
        output, last_format = '', ''
        for event in output_events:
            if event.format == events.Output.Format.VERBATIM:
                if last_format == event.format:
                    # append new verbatim line to the already initialized verbatim output
                    output = output[0:-3] + '\n' + event.message + '```'
                else:
                    output += '\n' + '```' + event.message + '```'
            elif event.format == events.Output.Format.ITALICS:
                for line in event.message.splitlines():
                    output += '\n _ ' + str(line) + ' _ '
            else:
                output = '\n' + event.message

            last_format = event.format
        return output
