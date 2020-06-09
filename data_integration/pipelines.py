import copy
import pathlib
import re
import typing

from . import config


class Node():
    """Base class for pipeline elements"""

    def __init__(self, id: str, description: str, labels: {str: str} = None) -> None:
        if not re.match('^[a-z0-9_]+$', id):
            raise ValueError(f'Invalid id "{id}". Should only contain lowercase letters, numbers and "_".')
        self.id: str = id
        self.description: str = description
        self.labels: {str: str} = labels or {}

        self.upstreams: {'Node'} = set()
        self.downstreams: {'Node'} = set()

        self.parent: typing.Optional['Pipeline'] = None
        self.cost: typing.Optional[float] = None


    def parents(self):
        """Returns all parents of a node from top to bottom"""
        if self.parent:
            return self.parent.parents() + [self]
        else:
            return [self]

    def path(self) -> [str]:
        """Returns a list of ids that identify the node across all pipelines, from top to bottom"""
        return [node.id for node in self.parents()[1:]]

    def url_path(self) -> str:
        """Returns a uri fragment for referring to nodes"""
        return '/'.join(self.path()) or None

    def __repr__(self):
        return f'<{self.__class__.__name__} "{self.id}">'


class Command():
    """
    Base class for operations that can run inside a pipeline task

    Args:
        parent: The pipeline node that contains this command (needed for debug output)
    """
    parent: Node = None

    def run(self) -> bool:
        """
        Runs the command

        Returns:
            False on failure
        """
        from . import shell
        shell_command = self.shell_command()

        # logger.log(f'{config.bash_command_string()} -c {shlex.quote(shell_command)}', format=logger.Format.ITALICS)
        return shell.run_shell_command(shell_command)

    def shell_command(self):
        """A bash command string that that runs the command"""
        raise NotImplementedError()

    def node_path(self):
        """The path of the parent node"""
        return self.parent.path() if self.parent else ''

    def html_doc_items(self) -> [(str, str)]:
        """
        Things to display in the documentation of a command. Can contain html
        Example: `[('filename','/tmp/foo.txt'), ('max-retries', 15)]`
        """
        raise NotImplementedError()


class Task(Node):
    def __init__(self, id: str, description: str, commands: [Command] = None, max_retries: int = None) -> None:
        super().__init__(id, description)
        self.commands = []
        self.max_retries = max_retries

        for command in commands or []:
            self.add_command(command)

    def add_command(self, command: Command, prepend=False):
        if prepend:
            self.commands.insert(0, command)
        else:
            self.commands.append(command)
        command.parent = self

    def add_commands(self, commands: [Command]):
        for command in commands:
            self.add_command(command)

    def run(self):
        for command in self.commands:
            if not command.run():
                return False
        return True


class ParallelTask(Node):
    def __init__(self, id: str, description: str, max_number_of_parallel_tasks: int = None,
                 commands_before: [Command] = None, commands_after: [Command] = None) -> None:
        super().__init__(id, description)
        self.commands_before = []
        for command in commands_before or []:
            self.add_command_before(command)
        self.commands_after = []
        for command in commands_after or []:
            self.add_command_after(command)
        self.max_number_of_parallel_tasks = max_number_of_parallel_tasks

    def add_command_before(self, command: Command):
        self.commands_before.append(command)
        command.parent = self

    def add_command_after(self, command: Command):
        self.commands_after.append(command)
        command.parent = self

    def add_parallel_tasks(self, sub_pipeline: 'Pipeline') -> None:
        pass

    def launch(self) -> 'Pipeline':
        sub_pipeline = Pipeline(self.id, description=f'Runs f{self.id} in parallel',
                                max_number_of_parallel_tasks=self.max_number_of_parallel_tasks)
        sub_pipeline.add_initial(Task(id='before', description='Runs commands-before', commands=self.commands_before))
        sub_pipeline.add_final(Task(id='after', description='Runs commands-after', commands=self.commands_after))

        self.add_parallel_tasks(sub_pipeline)

        return sub_pipeline

    def html_doc_items(self) -> [(str, str)]:
        """
        Things to display in the documentation of the parallel task. Can contain html.
        Example: `[('filename','/tmp/foo.txt'), ('max-retries', 15)]`
        """
        raise NotImplementedError


class Pipeline(Node):
    nodes: {str: Node} = None
    initial_node: Node = None
    final_node: Node = None

    def __init__(self, id: str,
                 description: str,
                 max_number_of_parallel_tasks: int = None,
                 base_path: pathlib.Path = None,
                 labels: {str: str} = None,
                 ignore_errors: bool = False,
                 force_run_all_children: bool = False) -> None:
        """
        A directed acyclic graph (DAG) of nodes with dependencies between them.

        Args:
            id: The id of the pipeline
            description: A short summary of what the pipeline is doing
            max_number_of_parallel_tasks: Only that many nodes of the pipeline will run in parallel
            base_path: The absolute path of the pipeline root, file names are relative to that
            labels: An arbitrary dictionary application specific tags, schemas and so on.
            ignore_errors: When true, then the pipeline execution will not fail when a child node fails
            force_run_all_children: When true, child nodes will run even when their upstreams failed
        """
        super().__init__(id, description, labels)
        self.nodes = {}
        self._base_path = base_path
        self.max_number_of_parallel_tasks = max_number_of_parallel_tasks
        self.force_run_all_children = force_run_all_children
        self.ignore_errors = ignore_errors

    def add(self, node: Node, upstreams: [typing.Union[str, Node]] = None) -> 'Pipeline':
        if node.id in self.nodes:
            raise ValueError(f'A node with id "{node.id}" already exists in pipeline "{self.id}"')

        self.nodes[node.id] = node
        node.parent = self

        for upstream in upstreams or []:
            self.add_dependency(upstream, node)

        if self.initial_node and not node.upstreams and self.initial_node != node:
            self.add_dependency(self.initial_node, node)

        if self.final_node and self.final_node != node:
            self.add_dependency(node, self.final_node)

        return self

    def remove(self, node: Node) -> None:
        """
        Removes a `node` from the pipeline and its dependencies.
        All `upstreams` of `node` will be connected to all `downstreams` of `node`.

        Args:
            node: The node to remove
        """

        for upstream in node.upstreams:
            for downstream in node.downstreams:
                self.add_dependency(upstream, downstream)

        for upstream in copy.copy(node.upstreams):
            self.remove_dependency(upstream, node)

        for downstream in copy.copy(node.downstreams):
            self.remove_dependency(node, downstream)

        if self.initial_node == node:
            self.initial_node = None

        if self.final_node == node:
            self.final_node = None

        del (self.nodes[node.id])

    def replace(self, node: Node, new_node) -> Node:
        """
        Replaces `node` with `new_node` while keeping dependencies intact
        Args:
            node: The node to replace
            new_node: The new node
        """
        for upstream in copy.copy(node.upstreams):
            self.add_dependency(upstream, new_node)

        for downstream in copy.copy(node.downstreams):
            self.add_dependency(new_node, downstream)

        self.remove(node)
        self.add(new_node)

    def add_dependency(self, upstream: typing.Union[Node, str], downstream: typing.Union[Node, str]):
        if isinstance(upstream, str):
            if not upstream in self.nodes:
                raise KeyError(f'Node "{upstream}" not found in pipeline "{self.id}"')
            upstream = self.nodes[upstream]

        if isinstance(downstream, str):
            if not downstream in self.nodes:
                raise KeyError(f'Node "{downstream}" not found in pipeline "{self.id}"')
            downstream = self.nodes[downstream]

        upstream.downstreams.add(downstream)
        downstream.upstreams.add(upstream)

        if self.final_node and self.final_node != downstream:
            self.remove_dependency(upstream, self.final_node)

        if self.initial_node and self.initial_node != upstream:
            self.remove_dependency(self.initial_node, downstream)

    def remove_dependency(self, upstream: Node, downstream: Node):
        upstream.downstreams.discard(downstream)
        downstream.upstreams.discard(upstream)

    def add_initial(self, node: Node) -> 'Pipeline':
        self.initial_node = node
        for downstream in self.nodes.values():
            if not downstream.upstreams and downstream != self.final_node:
                self.add_dependency(node, downstream)
        self.add(node)

    def add_final(self, node: Node) -> 'Pipeline':
        self.final_node = node
        for upstream in self.nodes.values():
            if not upstream.downstreams and upstream != self.initial_node:
                self.add_dependency(upstream, node)
        self.add(node)

    def base_path(self):
        return self._base_path or (self.parent.base_path() if self.parent else pathlib.Path('.'))


def find_node(path: [str]) -> (Node, bool):
    """
    Retrieves a node by the the path from its parents
    Args:
        path: The ids of the node and of all its parent, from top to bottom

    Returns:
        A tuple of the node and True if the node was found, or a the closest known parent node if and False otherwise
    """
    if not path or path == ['']:
        return config.root_pipeline(), True

    def _find_node(node: Node, path) -> Node:
        if len(path) == 0:
            return node, True
        else:
            if isinstance(node, Pipeline) and path[0] in node.nodes:
                return _find_node(node.nodes[path[0]], path[1::])
            else:
                return node, False

    return _find_node(config.root_pipeline(), path)


def demo_pipeline():
    """Returns a demo pipeline"""
    from .commands import bash, python
    pipeline = Pipeline(id='demo',
                        description='A small pipeline that demonstrates the interplay between pipelines, tasks and commands')

    pipeline.add(Task(id='ping_localhost', description='Pings localhost',
                      commands=[bash.RunBash('ping -c 3 localhost')]))

    sub_pipeline = Pipeline(id='sub_pipeline', description='Pings a number of hosts')

    for host in ['google', 'amazon', 'facebook']:
        sub_pipeline.add(Task(id=f'ping_{host}', description=f'Pings {host}',
                              commands=[bash.RunBash(f'ping -c 3 {host}.com'),
                                        python.RunFunction(lambda: 1)]))
    sub_pipeline.add_dependency('ping_amazon', 'ping_facebook')
    sub_pipeline.add(Task(id='ping_foo', description='Pings foo',
                          commands=[bash.RunBash('ping foo')]), ['ping_amazon'])

    pipeline.add(sub_pipeline, ['ping_localhost'])

    pipeline.add(Task(id='sleep', description='Sleeps for 2 seconds',
                      commands=[bash.RunBash('sleep 2')]), ['sub_pipeline'])

    return pipeline
