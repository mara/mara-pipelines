from . import _LocalShellExecutionContext
from .. import config


class BashExecutionContext(_LocalShellExecutionContext):
    """Runs the shell commands in the local bash shell"""
    def __init__(self, bash_command_string: str = None):
        self.bash_command_string = bash_command_string or config.bash_command_string() or '/usr/bin/env bash -o pipefail'
