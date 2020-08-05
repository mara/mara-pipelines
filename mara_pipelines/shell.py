"""Command execution in bash shells"""

import time

from . import config
from .logging import logger


def run_shell_command(command: str, log_command: bool = True):
    """
    Runs a command in a bash shell and logs the output of the command in (near)real-time.

    Args:
        command: The command to run
        log_command: When true, then the command itself is logged before execution

    Returns:
        Either (in order)
        - False when the exit code of the command was not 0
        - True when there was no output to stdout
        - The output to stdout, as an array of lines
    """
    import shlex, subprocess, threading

    if log_command:
        logger.log(command, format=logger.Format.ITALICS)

    process = subprocess.Popen(shlex.split(config.bash_command_string()) + ['-c', command],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               universal_newlines=True)

    # keep stdout output
    output_lines = []

    # unfortunately, only file descriptors and the system stream can be passed to
    # subprocess.Popen(..) (and not custom streams without a file handle).
    # So in order to see be able to log the output in real-time, we have to
    # query the output steams of the process from to separate threads
    def read_process_stdout():
        for line in process.stdout:
            output_lines.append(line)
            logger.log(line, format=logger.Format.VERBATIM)

    def read_process_stderr():
        for line in process.stderr:
            logger.log(line, format=logger.Format.VERBATIM, is_error=True)

    read_stdout_thread = threading.Thread(target=read_process_stdout)
    read_stdout_thread.start()
    read_stderr_thread = threading.Thread(target=read_process_stderr)
    read_stderr_thread.start()

    # wait until the process finishes
    while process.poll() is None:
        time.sleep(0.005)

    read_stdout_thread.join()
    read_stderr_thread.join()

    exitcode = process.returncode
    if exitcode != 0:
        logger.log(f'exit code {exitcode}', is_error=True, format=logger.Format.ITALICS)
        return False

    return output_lines or True


def sed_command(replace: {str: str}) -> str:
    """
    Creates a sed command string from a dictionary of replacements

    Examples:
    >>> print(sed_command({'foo':'a','bar':'b'}))
    sed "s/foo/a/g; s/bar/b/g"
    """

    def quote(s):
        return str(s).replace('/', '\/').replace('"', '\\\"').replace('\n', '\\\\\n')

    return 'sed "' + \
           ';'.join(
               ['s/' + quote(search) + '/' + quote(_replace) + '/g' for search, _replace in
                replace.items()]) \
           + '"'


if __name__ == "__main__":
    run_shell_command('ping -c 3 google.com; ping null')
