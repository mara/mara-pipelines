"""Teams notifications for failed node runs"""

from .. import config
from ..logging import events


class Teams(events.EventHandler):
    node_output: {tuple: {bool: [events.Event]}} = None

    def handle_event(self, event: events.Event):
        """
        Send the output of a node to Teams when the node failed.
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
                node_path = '/'.join(event.node_path)
                node_path = node_path.replace("_", "\\_")
                text = '<font size="4">&#x1F424;</font> Ooops, a hiccup in [_' + node_path + '_](' + config.base_url() + '/' + \
                       '/'.join(event.node_path) + ')'

                if self.node_output[key][False]:
                    text += self.format_output(self.node_output[key][False])

                if self.node_output[key][True]:
                    text += self.format_output(self.node_output[key][True])

                message = {'text': text}
                response = requests.post('https://outlook.office.com/webhook/' + config.teams_token(), json=message)

                if response.status_code != 200:
                    raise ValueError(
                        'Request to slack returned an error %s. The response is:\n%s' % (
                            response.status_code, response.text)
                    )

    def format_output(self, output_events: [events.Output]):
        output, last_format = '', ''
        for event in output_events:
            if event.format == events.Output.Format.VERBATIM:
                if last_format == event.format:
                    # append new verbatim line to the already initialized verbatim output
                    '''<pre>Code</pre> is used to display code in text as there is bug
                        in display code using ```'''
                    output = output[0:-6] + '\n' + event.message + '</pre>'
                else:
                    output += '\n' + '<pre>' + event.message + '</pre>'
            elif event.format == events.Output.Format.ITALICS:
                for line in event.message.splitlines():
                    output += '\n\n' + str(line.replace('_', '\\_'))
            else:
                output = '\n' + event.message

            last_format = event.format
        return output
