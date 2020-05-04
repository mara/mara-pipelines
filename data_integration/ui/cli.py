"""Command line interface for running data pipelines"""

import sys

import click

from .. import config, pipelines


def run_pipeline(pipeline: pipelines.Pipeline, nodes: {pipelines.Node} = None,
                 with_upstreams: bool = False,
                 interactively_started: bool = False,
                 disable_colors: bool = False) -> bool:
    """
    Runs a pipeline or parts of it with output printed to stdout
    Args:
        pipeline: The pipeline to run
        nodes: A list of pipeline children that should run
        with_upstreams: When true and `nodes` are provided, then all upstreams of `nodes` in `pipeline` are also run
        interactively_started: Whether or not this run was started interactively, passed on in RunStarted and
                               RunFinished events.
        disable_colors: If true, don't use escape sequences to make the log colorful (default: colorful logging)
    Return:
        True when the pipeline run succeeded
    """
    from ..logging import logger, pipeline_events
    from .. import execution

    RESET_ALL = 'reset_all'
    PATH_COLOR = 'path_color'
    ERROR_COLOR = 'error_color'

    # https://godoc.org/github.com/whitedevops/colors
    colorful = {logger.Format.STANDARD: '\033[01m',  # bold
                logger.Format.ITALICS: '\033[02m',  # dim
                logger.Format.VERBATIM: '',
                PATH_COLOR: '\033[36m',  # cyan
                ERROR_COLOR: '\033[91m',  # light red
                RESET_ALL: '\033[0m',  # reset all
                }
    plain = {key: '' for key in colorful.keys()}

    theme = plain if disable_colors else colorful

    succeeded = False
    for event in execution.run_pipeline(pipeline, nodes, with_upstreams, interactively_started=interactively_started):
        if isinstance(event, pipeline_events.Output):
            print(f'{theme[PATH_COLOR]}{" / ".join(event.node_path)}{":" if event.node_path else ""}{theme[RESET_ALL]} '
                  + theme[event.format] + (theme[ERROR_COLOR] if event.is_error else '')
                  + event.message + theme[RESET_ALL])
        elif isinstance(event, pipeline_events.RunFinished):
            if event.succeeded:
                succeeded = True

    return succeeded


@click.command()
@click.option('--path', default='',
              help='The id of of the pipeline to run. Example: "pipeline-id"; "" (default) is the root pipeline.')
@click.option('--nodes',
              help='IDs of sub-nodes of the pipeline to run, separated by comma. When provided, then only these nodes are run. Example: "do-this,do-that".')
@click.option('--with_upstreams', default=False, is_flag=True,
              help='Also run all upstreams of --nodes within the pipeline.')
@click.option('--disable-colors', default=False, is_flag=True,
              help='Output logs without coloring them.')
def run(path, nodes, with_upstreams, disable_colors: bool = False):
    """Runs a pipeline or a sub-set of its nodes"""

    # the pipeline to run
    path = path.split(',')
    pipeline, found = pipelines.find_node(path)
    if not found:
        print(f'Pipeline {path} not found', file=sys.stderr)
        sys.exit(-1)
    if not isinstance(pipeline, pipelines.Pipeline):
        print(f'Node {path} is not a pipeline, but a {pipeline.__class__.__name__}', file=sys.stderr)
        sys.exit(-1)

    # a list of nodes to run selectively in the pipeline
    _nodes = set()
    for id in (nodes.split(',') if nodes else []):
        node = pipeline.nodes.get(id)
        if not node:
            print(f'Node "{id}" not found in pipeline {path}', file=sys.stderr)
            sys.exit(-1)
        else:
            _nodes.add(node)

    if not run_pipeline(pipeline, _nodes, with_upstreams, interactively_started=False, disable_colors=disable_colors):
        sys.exit(-1)


@click.command()
def run_interactively():
    """Select and run data pipelines"""
    from dialog import Dialog

    d = Dialog(dialog="dialog", autowidgetsize=True)  # see http://pythondialog.sourceforge.net/doc/widgets.html

    def run_pipeline_and_notify(pipeline: pipelines.Pipeline, nodes: {pipelines.Node} = None):
        if not run_pipeline(pipeline, nodes, interactively_started=True):
            sys.exit(-1)

    def menu(node: pipelines.Node):
        if isinstance(node, pipelines.Pipeline):

            code, choice = d.menu(
                text='Pipeline ' + '.'.join(node.path()) if node.parent else 'Root pipeline',
                choices=[('▶ ', 'Run'), ('>> ', 'Run selected')]
                        + [(child.id, '→' if isinstance(child, pipelines.Pipeline) else 'Run')
                           for child in node.nodes.values()])
            if code == d.CANCEL:
                return

            if choice == '▶ ':
                run_pipeline_and_notify(node)
            elif choice == '>> ':
                code, node_ids = d.checklist('Select sub-nodes to run. If you want to run all, then select none.',
                                             choices=[(node_id, '', False) for node_id in node.nodes.keys()])
                if code == d.OK:
                    run_pipeline_and_notify(node, {node.nodes[id] for id in node_ids})
            else:
                menu(node.nodes[choice])
            return
        else:
            run_pipeline_and_notify(pipeline=node.parent, nodes=[node])

    menu(config.root_pipeline())


@click.command()
@click.option('--path', default='',
              help='The parent ids of of the node to reset. Example: "pipeline-id,sub-pipeline-id".')
def reset_incremental_processing(path):
    """Reset status of incremental processing for a node"""
    from ..incremental_processing import reset

    path = path.split(',') if path else []
    node, found = pipelines.find_node(path)
    if not found:
        print(f'Node {path} not found', file=sys.stderr)
        sys.exit(-1)
    reset.reset_incremental_processing(path)
