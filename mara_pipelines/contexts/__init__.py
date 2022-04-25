from .. import shell


class ExecutionContext:
    """The execution context for a shell command"""
    def __init__(self) -> None:
        self._active = False

    @property
    def is_active(self) -> bool:
        return self._active

    def __enter__(self): # -> ExecutionContext:
        """
        Enters the execution context.
        
        This place can be used to spin up cloud resource.        
        """
        self._active = True
        return self

    def __exit__(self, type, value, traceback) -> bool:
        """Exits the execution context freeing up used resources."""
        self._active = False
        return True

    def _test_active(self):
        """Thest if the current context is active"""
        if not self.is_active:
            raise Exception('The current context is not activated. Call mycontext.__enter__ before using this method')

    def run_shell_command(self, shell_command: str) -> bool:
        """Executes a shell command in the context"""
        raise NotImplementedError()


class _LocalShellExecutionContext(ExecutionContext):
    """Runs the shell commands in a context through the local shell"""
    def __init__(self):
        self.bash_command_string: str = None

    def run_shell_command(self, shell_command: str) -> bool:
        self._test_active()

        # logger.log(f'{self.bash_command_string} -c {shlex.quote(shell_command)}', format=logger.Format.ITALICS)
        return shell.run_shell_command(shell_command, bash_command_string=self.bash_command_string)


def context(alias: str) -> ExecutionContext:
    """Returns a execution config by alias"""
    from .. import config
    execution_contexts = config.execution_contexts()
    if alias not in execution_contexts:
        raise KeyError(f'execution context alias "{alias}" not configured')
    return execution_contexts[alias]
