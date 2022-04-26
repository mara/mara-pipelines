import shlex
import typing as t

from . import _LocalShellExecutionContext
from .. import config


class BashExecutionContext(_LocalShellExecutionContext):
    """Runs the shell commands in the local bash shell"""
    def __init__(self, bash_command_string: str = None):
        self.bash_command_string = bash_command_string or config.bash_command_string() or '/usr/bin/env bash -o pipefail'


class SshBashExecutionContext(_LocalShellExecutionContext):
    """Runs the shell commands in a bash shell on a remote host using ssh"""
    def __init__(self, host: str, port: int = None, user: str = None, password: str = None, identity_file: str = None,
                 configfile: str = None, options: t.List[str] = None):
        """
        Args:
            host: the remote ssh post
            port: the ssh port. By default 22
            user: the remote user
            password: the remote user password.
            identity_file: the identity file for the user login
            configfile: a custom config file for ssh
            options: a list of SSH options to be used
        """
        self.bash_command_string = ('/usr/bin/env '
                                    + (f'sshpass -p {shlex.quote(password)} ' if password and not identity_file else '')
                                    + 'ssh '
                                    + (f'-F {str(configfile)} ' if configfile else '')
                                    + (f'-i {identity_file} ' if identity_file else '')
                                    + (' '.join(f"-o '{o}'" for o in options)+' ' if options else '')
                                    + (f'{user}@' if user else '')
                                    + host
                                    + (f':{port}' if port else '')
                                    + f' bash -o pipefail')
