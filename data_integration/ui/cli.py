"""Command line interface for running data pipelines"""

import sys

import click

from .. import config, pipelines


def run_pipeline(pipeline: pipelines.Pipeline, nodes: {pipelines.Node} = None,
                 with_upstreams: bool = False) -> bool:
    """
    Runs a pipeline or parts of it with output printed to stdout
    Args:
        pipeline: The pipeline to run
        nodes: A list of pipeline children that should run
        with_upstreams: When true and `nodes` are provided, then all upstreams of `nodes` in `pipeline` are also run
    Return:
        True when the pipeline run succeeded
    """
    from ..logging import logger, events
    from .. import execution

    succeeded = True
    for event in execution.run_pipeline(pipeline, nodes, with_upstreams):
        if isinstance(event, events.Output):
            print(f'\033[36m{" / ".join(event.node_path)}{":" if event.node_path else ""}\033[0m '
                  + {logger.Format.STANDARD: '\033[01m',
                     logger.Format.ITALICS: '\033[02m',
                     logger.Format.VERBATIM: ''}[event.format]
                  + ('\033[91m' if event.is_error else '') + event.message + '\033[0m')
        elif isinstance(event, events.RunFinished):
            if not event.succeeded:
                succeeded = False

    return succeeded


@click.command()
@click.option('--path', default='',
              help='The parent ids of of the pipeline to run, separated by comma. Example: "pipeline-id,sub-pipeline-id".')
@click.option('--nodes',
              help='IDs of sub-nodes of the pipeline to run, separated by comma. When provided, then only these nodes are run. Example: "do-this, do-that".')
@click.option('--with_upstreams', default=False, is_flag=True,
              help='Also run all upstreams of --nodes within the pipeline.')
def run(path, nodes, with_upstreams):
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

    if not run_pipeline(pipeline, _nodes, with_upstreams):
        sys.exit(-1)


@click.command()
def run_interactively():
    """Select and run data pipelines"""
    from dialog import Dialog

    d = Dialog(dialog="dialog", autowidgetsize=True)  # see http://pythondialog.sourceforge.net/doc/widgets.html

    def run_pipeline_and_notify(pipeline: pipelines.Pipeline, nodes: {pipelines.Node} = None):
        import requests, os

        if config.slack_token():
            message = (':hatching_chick: *' + (os.environ.get('SUDO_USER') or os.environ.get('USER') or os.getlogin())
                       + '* manually triggered run of ' +
                       ('pipeline <' + config.base_url() + '/' + '/'.join(pipeline.path()) + '|'
                        + '/'.join(pipeline.path()) + ' >' if pipeline.parent else 'root pipeline'))

            if nodes:
                message += ', nodes ' + ', '.join([f'`{node.id}`' for node in nodes])

            requests.post('https://hooks.slack.com/services/' + config.slack_token(), json={'text': message})

        if not run_pipeline(pipeline, nodes):
            if config.slack_token():
                requests.post('https://hooks.slack.com/services/' + config.slack_token(),
                              json={'text': ':baby_chick: failed'})
            sys.exit(-1)
        if config.slack_token():
            requests.post('https://hooks.slack.com/services/' + config.slack_token(),
                          json={'text': ':hatched_chick: succeeded'})

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
