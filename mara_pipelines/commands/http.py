"""Commands for interacting with HTTP"""

from typing import List, Tuple, Dict

from mara_page import html, _
from .. import pipelines
from ..shell import http_request_command


class HttpRequest(pipelines.Command):
    def __init__(self, url: str, headers: Dict[str, str] = None, method: str = None, body: str = None) -> None:
        """
        Executes a HTTP request

        Args:
            url: The url
            headers: The HTTP headers as dict
            method: The HTTP method to be used
            body: The body string to be used in the HTTP request
        """
        super().__init__()
        self.url = url
        self.headers = headers
        self.method = method
        self.body = body

    def shell_command(self):
        return http_request_command(self.url, self.headers, self.method, self.body)

    def html_doc_items(self) -> List[Tuple[str, str]]:
        return [
            ('method', _.tt[self.method or 'GET']),
            ('url', _.tt[self.url]),
            ('headers', _.pre[
                '\n'.join([f'{header}: {content}' for header, content in self.headers.items()]) if self.headers else ''
            ]),
            ('body', _.pre[self.body or '']),
            ('command', html.highlight_syntax(self.shell_command(), 'bash'))
        ]
