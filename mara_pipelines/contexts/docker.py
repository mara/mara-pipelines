from . import _LocalShellExecutionContext


class DockerBashExecutionContext(_LocalShellExecutionContext):
    """Runs the shell commands in a bash shell on a docker container"""
    def __init__(self, container: str, context: str = None):
        """
        Args:
            container: the docker container name
            context: the docker context
        """
        self.bash_command_string = ('/usr/bin/env docker'
                                    + (f' --context={context}' if context else '')
                                    + f' exec -i {container}'
                                    + f' bash -o pipefail')


class DockerComposeBashExecutionContext(_LocalShellExecutionContext):
    """Runs the shell commands in a bash shell on a docker container isinde a docker compose project"""
    def __init__(self, container: str, context: str = None, compose_project_name: str = None):
        """
        Args:
            container: the docker container name
            context: the docker context
            compose_project_name: the docker compose project name
        """
        self.bash_command_string = ('/usr/bin/env '
                                     + (f'COMPOSE_PROJECT_NAME={compose_project_name} ' if compose_project_name else '')
                                     + 'docker'
                                     + (f' --context={context}' if context else '')
                                     + ' compose'
                                     + f' exec -i {container}'
                                     + f' bash -o pipefail')
