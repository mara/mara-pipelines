"""UI for running pipelines from the web browser"""

import json

import flask

from mara_page import _, bootstrap, response, acl
from . import views
from .. import pipelines, config


@views.blueprint.route('/run-', defaults={'path': '', 'with_upstreams': False, 'ids': None})
@views.blueprint.route('/run-/<path:ids>', defaults={'path': '', 'with_upstreams': False})
@views.blueprint.route('/run-with-upstreams', defaults={'path': '', 'with_upstreams': True, 'ids': None})
@views.blueprint.route('/run-with-upstreams/<path:ids>', defaults={'path': '', 'with_upstreams': True})
@views.blueprint.route('/<path:path>/run-', defaults={'with_upstreams': False, 'ids': None})
@views.blueprint.route('/<path:path>/run-/<path:ids>', defaults={'with_upstreams': False})
@views.blueprint.route('/<path:path>/run-with-upstreams', defaults={'with_upstreams': True, 'ids': None})
@views.blueprint.route('/<path:path>/run-with-upstreams/<path:ids>', defaults={'with_upstreams': True})
@acl.require_permission(views.acl_resource)
def run_page(path: str, with_upstreams: bool, ids: str):
    if not config.allow_run_from_web_ui():
        flask.abort(403, 'Running piplelines from web ui is disabled for this instance')

    # the pipeline to run
    pipeline, found = pipelines.find_node(path.split('/'))
    if not found:
        flask.abort(404, f'Pipeline "{path}" not found')
    assert (isinstance(pipeline, pipelines.Pipeline))

    # a list of nodes to run selectively in the pipeline
    nodes = []
    for id in (ids.split('/') if ids else []):
        node = pipeline.nodes.get(id)
        if not node:
            flask.abort(404, f'Node "{id}" not found in pipeline "{path}"')
        else:
            nodes.append(node)

    stream_url = flask.url_for('mara_pipelines.do_run', path=path, with_upstreams=with_upstreams, ids=ids)

    title = ['Run ', 'with upstreams ' if with_upstreams else '',
             ' / '.join([str(_.a(href=views.node_url(parent))[parent.id]) for parent in pipeline.parents()[1:]])]
    if nodes:
        title += [' / [', ', '.join([str(_.a(href=views.node_url(node))[node.id]) for node in nodes]), ']']

    return response.Response(
        html=[
            _.script['''
document.addEventListener('DOMContentLoaded', function() {
     processRunEvents(''' + json.dumps(flask.url_for('mara_pipelines.node_page', path='')) + ', '
                     + json.dumps(stream_url) + ', ' + json.dumps(pipeline.path()) + ''');
});'''],

            _.style['span.action-buttons > * {display:none}'],  # hide reload button until run finishes
            _.div(class_='row')[
                _.div(class_='col-lg-7')[
                    bootstrap.card(body=_.div(id='main-output-area', class_='run-output')[''])],
                _.div(class_='col-lg-5 scroll-container')[
                    bootstrap.card(header_left='Timeline',
                                   body=[_.div(id='system-stats-chart', class_='google-chart')[' '],
                                         _.div(id='timeline-chart')[' ']]),
                    _.div(id='failed-tasks-container')[''],
                    _.div(id='running-tasks-container')[''],
                    _.div(id='succeeded-tasks-container')[''],

                    bootstrap.card(id='card-template', header_left=' ', header_right=' ',
                                   body=[_.div(class_='run-output')['']])
                ]
            ]
        ],
        js_files=['https://www.gstatic.com/charts/loader.js',
                  flask.url_for('mara_pipelines.static', filename='timeline-chart.js'),
                  flask.url_for('mara_pipelines.static', filename='system-stats-chart.js'),
                  flask.url_for('mara_pipelines.static', filename='utils.js'),
                  flask.url_for('mara_pipelines.static', filename='run-page.js')],
        css_files=[flask.url_for('mara_pipelines.static', filename='timeline-chart.css'),
                   flask.url_for('mara_pipelines.static', filename='run-page.css'),
                   flask.url_for('mara_pipelines.static', filename='common.css')],
        action_buttons=[response.ActionButton(action='javascript:location.reload()', label='Run again', icon='play',
                                              title='Run pipeline again with same parameters as before')],
        title=title,

    )


@views.blueprint.route('/do-run', defaults={'path': '', 'with_upstreams': False, 'ids': None})
@views.blueprint.route('/do-run/<path:ids>', defaults={'path': '', 'with_upstreams': False})
@views.blueprint.route('/do-run-with-upstreams', defaults={'path': '', 'with_upstreams': True, 'ids': None})
@views.blueprint.route('/do-run-with-upstreams/<path:ids>', defaults={'path': '', 'with_upstreams': True})
@views.blueprint.route('/<path:path>/do-run', defaults={'with_upstreams': False, 'ids': None})
@views.blueprint.route('/<path:path>/do-run/<path:ids>', defaults={'with_upstreams': False})
@views.blueprint.route('/<path:path>/do-run-with-upstreams', defaults={'with_upstreams': True, 'ids': None})
@views.blueprint.route('/<path:path>/do-run-with-upstreams/<path:ids>', defaults={'with_upstreams': True})
@acl.require_permission(views.acl_resource)
def do_run(path: str, with_upstreams: bool, ids: str):
    from .. import execution

    if not config.allow_run_from_web_ui():
        flask.abort(403, 'Running piplelines from web ui is disabled for this instance')
    pipeline, found = pipelines.find_node(path.split('/'))
    if not found:
        flask.abort(404, f'Pipeline "{path}" not found')

    nodes = {pipeline.nodes[id] for id in (ids.split('/') if ids else [])}

    def process_events():
        for event in execution.run_pipeline(pipeline, nodes, with_upstreams):
            yield f'event: {event.__class__.__name__}\ndata: ' + event.to_json() + '\n\n'

    return flask.Response(process_events(), mimetype="text/event-stream")
