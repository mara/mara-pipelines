"""Data integration web UI"""

import functools

import flask

from mara_page import acl, navigation, _
from .. import config, pipelines

blueprint = flask.Blueprint('mara_pipelines', __name__, url_prefix='/pipelines', static_folder='static')

acl_resource = acl.AclResource(name='Pipelines')


@functools.singledispatch
def navigation_icon(node: pipelines.Node):
    """Returns the navigation entry icon for a pipeline node"""
    return 'question'


navigation_icon.register(pipelines.Task)(lambda _: 'play')
navigation_icon.register(pipelines.ParallelTask)(lambda _: 'forward')
navigation_icon.register(pipelines.Pipeline)(lambda _: 'random')


def navigation_entry():
    """Creates a navigation entry that contains links to all data pipelines and their nodes"""

    def node_entry(node: pipelines.Node) -> navigation.NavigationEntry:
        return navigation.NavigationEntry(
            label=node.id, description=node.description, icon=navigation_icon(node),
            uri_fn=functools.partial(lambda n: node_url(n), node),
            children=([navigation.NavigationEntry(label='Overview', icon='list',
                                                  uri_fn=functools.partial(lambda: node_url(node)))]
                      + [node_entry(node) for node in node.nodes.values()]
                      if isinstance(node, pipelines.Pipeline) else None))

    entry = node_entry(config.root_pipeline())
    entry.label = 'Pipelines'
    entry.icon = 'wrench'
    entry.description = 'Pipelines for loading and transforming data'
    return entry


def node_url(node: pipelines.Node) -> str:
    """The url of the page that documents a node"""
    return flask.url_for('mara_pipelines.node_page', path=node.url_path())


def format_labels(node: pipelines.Node):
    """Html markup that comma-separates labels of a node"""
    return ', '.join([str(_.span[label, ': ', _.tt(style='white-space:nowrap')[repr(value)]])
                      for label, value in node.labels.items()]) or ''
