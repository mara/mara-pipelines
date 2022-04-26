import shlex

from . import _LocalShellExecutionContext


class SshExecutionContext(_LocalShellExecutionContext):
    """Runs the shell commands on a remote host via ssh"""
    def __init__(self, host: str, port: int = None, user: str = None, password: str = None, identity_file: str = None, configfile: str = None):
        """
        Args:
            host: the remote ssh post
            port: the ssh port. By default 22
            user: the remote user
            password: the remote user password.
            identity_file: the identity file for the user login
            configfile: a custom config file for ssh
        """
        self.bash_command_string = ('/usr/bin/env '
                                    + (f'sshpass -p {shlex.quote(password)} ' if password and not identity_file else '')
                                    + 'ssh '
                                    + (f'-c {configfile} ' if configfile else '')
                                    + (f'-i {identity_file} ' if identity_file else '')
                                    + (f'{user}@' if user else '')
                                    + host
                                    + (f':{port}' if port else '')
                                    + f' {shlex.quote("set -o pipefail ; ")}')
