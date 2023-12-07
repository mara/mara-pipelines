"""(Deprecated) Command line interface for running data pipelines"""

import click
from warnings import warn
from typing import Set

from .. import pipelines
from .. import cli


def run_pipeline(pipeline: pipelines.Pipeline, nodes: Set[pipelines.Node] = None,
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
    warn("This method is deprecated. Please use `mara_pipelines.cli.run_pipeline` instead.")
    return cli.run_pipeline(pipeline, nodes, with_upstreams, interactively_started, disable_colors)


@click.command("run")
@click.option('--path', default='',
              help='The id of of the pipeline to run. Example: "pipeline-id"; "" (default) is the root pipeline.')
@click.option('--nodes',
              help='IDs of sub-nodes of the pipeline to run, separated by comma. When provided, then only these nodes are run. Example: "do-this,do-that".')
@click.option('--with_upstreams', default=False, is_flag=True,
              help='Also run all upstreams of --nodes within the pipeline.')
@click.option('--disable-colors', default=False, is_flag=True,
              help='Output logs without coloring them.')
def _run(path, nodes, with_upstreams, disable_colors: bool = False):
    """Runs a pipeline or a sub-set of its nodes"""
    warn("CLI command `<app> mara_pipelines.ui.run` will be dropped in 4.0. Please use `<app> mara-pipelines run` instead.")
    cli.run(path, nodes, with_upstreams, disable_colors)


@click.command("run_interactively")
def _run_interactively():
    """Select and run data pipelines"""
    warn("CLI command `<pp> mara_pipelines.ui.run_interactively` will be dropped in 4.0. Please use `<app> mara-pipelines run_interactively` instead.")
    cli.run_interactively()


@click.command("reset_incremental_processing")
@click.option('--path', default='',
              help='The parent ids of of the node to reset. Example: "pipeline-id,sub-pipeline-id".')
def _reset_incremental_processing(path):
    """Reset status of incremental processing for a node"""
    warn("CLI command `<pp> mara_pipelines.ui.reset_incremental_processing` will be dropped in 4.0. Please use `<app> mara-pipelines reset_incremental_processing` instead.")
    cli.reset_incremental_processing(path)
