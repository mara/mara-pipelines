"""Visualization of pipeline nodes"""

import functools
import json

import flask

from mara_page import _, bootstrap, html, response, acl
from . import views, last_runs, dependency_graph, run_time_chart
from .. import pipelines, config


@views.blueprint.route('/<path:path>')
@views.blueprint.route('', defaults={'path': ''})
def node_page(path: str):
    """Creates a node visualization page including title, action buttons, etc."""
    node, found = pipelines.find_node(path.split('/'))
    if not found and node:
        return flask.redirect(views.node_url(node), 302)
    elif not node:
        flask.abort(404, f'Node "{path}" not found')

    title = [node.__class__.__name__, ' ',
             [[_.a(href=views.node_url(parent))[parent.id], ' / '] for parent in node.parents()[1:-1]],
             node.id] if node.parent else 'Root pipeline'
    return response.Response(
        title=title,
        action_buttons=action_buttons(node) if config.allow_run_from_web_ui() else [],
        html=[_.script['''
var nodePage = null;
document.addEventListener('DOMContentLoaded', function() {
     nodePage = NodePage("''' + flask.url_for('mara_pipelines.node_page', path='') + '''", '''
                       + json.dumps(node.path()) + ''');
});'''],
              dependency_graph.card(node),
              run_time_chart.card(node),
              node_content(node),
              last_runs.card(node)],
        js_files=['https://www.gstatic.com/charts/loader.js',
                  flask.url_for('mara_pipelines.static', filename='node-page.js'),
                  flask.url_for('mara_pipelines.static', filename='utils.js'),
                  flask.url_for('mara_pipelines.static', filename='run-time-chart.js'),
                  flask.url_for('mara_pipelines.static', filename='system-stats-chart.js'),
                  flask.url_for('mara_pipelines.static', filename='timeline-chart.js'),
                  flask.url_for('mara_pipelines.static', filename='kolorwheel.js')],
        css_files=[flask.url_for('mara_pipelines.static', filename='common.css'),
                   flask.url_for('mara_pipelines.static', filename='node-page.css'),
                   flask.url_for('mara_pipelines.static', filename='timeline-chart.css')])


@functools.singledispatch
def node_content(_: object) -> str:
    """Renders the node class specific part of a node page"""
    raise NotImplementedError()


@node_content.register(pipelines.Pipeline)
def __(pipeline: pipelines.Pipeline):
    return bootstrap.card(
        header_left='Nodes',
        header_right=[
            bootstrap.button(
                id='run-with-upstreams-button', label='Run with upstreams', icon='play',
                url=flask.url_for('mara_pipelines.run_page', path=pipeline.url_path(), with_upstreams=True),
                title=f'Run selected nodes with all their upstreams in pipeline "{pipeline.id}"'),
            '&nbsp;&nbsp;&nbsp;&nbsp;',
            bootstrap.button(
                id='run-button', label='Run ', icon='play',
                url=flask.url_for('mara_pipelines.run_page', path=pipeline.url_path(),
                                  with_upstreams=False),

                title='Run selected nodes')

        ] if config.allow_run_from_web_ui() else [],
        body=html.asynchronous_content(
            url=flask.url_for('mara_pipelines.pipeline_children_table', path=pipeline.url_path())))


@node_content.register(pipelines.Task)
def __(task: pipelines.Task):
    if not acl.current_user_has_permission(views.acl_resource):
        return bootstrap.card(header_left='Commands', body=acl.inline_permission_denied_message())
    else:
        commands_card = bootstrap.card(
            header_left='Commands',
            fixed_header_height=True,
            sections=[_render_command(command) for command in task.commands])
        if task.max_retries:
            return [bootstrap.card(header_left=f'Max retries: {task.max_retries}'), commands_card]
        else:
            return commands_card


@node_content.register(pipelines.ParallelTask)
def __(task: pipelines.ParallelTask):
    if not acl.current_user_has_permission(views.acl_resource):
        return bootstrap.card(header_left=acl.inline_permission_denied_message())
    else:
        return [
            bootstrap.card(
                header_left='Commands before',
                fixed_header_height=True,
                sections=[_render_command(command) for command in task.commands_before]
            ) if task.commands_before else '',

            bootstrap.card(header_left='Sub task creation',
                           body=bootstrap.table([], [_.tr[_.td[_.div[section]], _.td(style='width:90%')[content]]
                                                     for section, content in task.html_doc_items()])),

            bootstrap.card(
                header_left='Commands after',
                fixed_header_height=True,
                sections=[_render_command(command) for command in task.commands_after]
            ) if task.commands_after else '']


def _render_command(command: pipelines.Command):
    """Creates a html documentation for a command"""
    from mara_page.xml import render

    def __mask_passwords(content):
        masks = config.password_masks()
        if masks:
            content = ''.join(render(content))
            for mask in masks:
                content = content.replace(mask, "***")
        return content

    try:
        doc = bootstrap.table([], [_.tr[_.td[_.div[section]], _.td(style='width:90%')[__mask_passwords(content)]]
                                   for section, content in command.html_doc_items()])
    except Exception as e:
        import traceback
        doc = [_.p(style='color:red')[_.i['Error in rendering command documentation']],
               _.pre(style='color:red')[traceback.format_exc()]]

    return [_.p[_.b[command.__class__.__name__]], doc]


@functools.singledispatch
def action_buttons(node: pipelines.Node):
    """The action buttons to be displayed on a node page"""
    path = node.path()
    return [
        response.ActionButton(
            action=flask.url_for('mara_pipelines.run_page', path='/'.join(path[:-1]),
                                 with_upstreams=True, ids=path[-1]),
            label='Run with upstreams', icon='play',
            title=f'Run the task and all its upstreams in the pipeline "{node.parent.id}"'),
        response.ActionButton(
            action=flask.url_for('mara_pipelines.run_page', path='/'.join(path[:-1]),
                                 with_upstreams=False, ids=path[-1]),
            label='Run', icon='play',
            title=f'Run only this task, without upstreams')]


@action_buttons.register(pipelines.Pipeline)
def __(pipeline: pipelines.Pipeline):
    return [response.ActionButton(action=flask.url_for('mara_pipelines.run_page', path=pipeline.url_path(),
                                                       with_upstreams=False),
                                  label='Run', icon='play',
                                  title='Run the pipeline')]


@views.blueprint.route('/<path:path>/pipeline-children-table')
@views.blueprint.route('/pipeline-children-table', defaults={'path': ''})
@acl.require_permission(views.acl_resource, do_abort=False)
def pipeline_children_table(path: str):
    """Creates a table that documents all child nodes of a table"""
    from ..logging import node_cost

    pipeline, __ = pipelines.find_node(path.split('/'))
    assert (isinstance(pipeline, pipelines.Pipeline))

    node_durations_and_run_times = node_cost.node_durations_and_run_times(pipeline)

    rows = []
    for node in pipeline.nodes.values():
        [avg_duration, avg_run_time] = node_durations_and_run_times.get(tuple(node.path()), ['', ''])

        rows.append(
            _.tr[_.td[_.a(href=views.node_url(node))[node.id.replace('_', '_<wbr>')]],
                 _.td[node.description],
                 _.td[views.format_labels(node)],
                 _.td[node_cost.format_duration(avg_duration)],
                 _.td(style='color:#bbb' if avg_duration == avg_run_time else '')[
                     node_cost.format_duration(avg_run_time)],
                 _.td[node_cost.format_duration(
                     node_cost.compute_cost(node, node_durations_and_run_times))],
                 _.td[(_.input(class_='pipeline-node-checkbox', type='checkbox',
                               value=node.id, name='ids[]', onchange='runButtons.update()')
                       if config.allow_run_from_web_ui() else '')]])

    return \
        str(_.script['var runButtons = new PipelineRunButtons();']) \
        + str(bootstrap.table(['ID', 'Description', '', 'Avg duration', 'Avg run time', 'Cost', ''], rows)) \
        + str(_.script['floatMaraTableHeaders();'])
