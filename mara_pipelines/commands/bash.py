"""Commands for running bash scripts"""

from typing import Union, Callable, List, Tuple

from mara_page import html
from .. import pipelines


class RunBash(pipelines.Command):
    """
    Runs a command in a bash shell

    Args:
        command: The command to run
    """
    def __init__(self, command: Union[str, Callable]) -> None:
        super().__init__()
        self._command = command

    @property
    def command(self) -> str:
        return (self._command() if callable(self._command) else self._command).strip()

    def shell_command(self) -> str:
        return self.command

    def html_doc_items(self) -> List[Tuple[str, str]]:
        return [
            ('command', html.highlight_syntax(self.shell_command(), 'bash'))
        ]
