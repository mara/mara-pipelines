from . import _LocalShellExecutionContext


class DockerExecutionContext(_LocalShellExecutionContext):
    """Runs the shell commands in a bash shell on a remote host using ssh"""
    def __init__(self, container: str, context: str = None):
        """
        Args:
            container: the docker container name
            context: the docker context
        """
        self.bash_command_string = ('/usr/bin/env docker '
                                    + (f'--context={context} ' if context else '')
                                    + f'exec -i {container}'
                                    + f' bash -o pipefail -c')
