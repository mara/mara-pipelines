import json
import pathlib

import flask
import psycopg2.extensions

import mara_db.config
import mara_db.postgresql
from mara_page import acl, bootstrap, html, _
from . import views
from .. import pipelines


def card(node: pipelines.Node):
    """A card that shows the duration of the node and its top children over time"""
    return bootstrap.card(
        header_left='Run times',
        body=html.asynchronous_content(
            flask.url_for('mara_pipelines.run_time_chart', path=node.url_path())))


@views.blueprint.route('/<path:path>/run-time-chart')
@views.blueprint.route('/run-time-chart', defaults={'path': ''})
@acl.require_permission(views.acl_resource, do_abort=False)
def run_time_chart(path: str):
    node, found = pipelines.find_node(path.split('/'))
    if not found:
        flask.abort(404, f'Node "{path}" not found')

    query = (pathlib.Path(__file__).parent / 'run_time_chart.sql').read_text()

    with mara_db.postgresql.postgres_cursor_context('mara') as cursor:  # type: psycopg2.extensions.cursor
        cursor.execute(query)
        cursor.execute(f'SELECT row_to_json(t) FROM pg_temp.node_run_times({"%s"}) t', (node.path(),))
        rows = [row[0] for row in cursor.fetchall()]

        if rows and len(rows) > 1:
            number_of_child_runs = len(rows[0]['child_runs']) if rows[0]['child_runs'] else 0

            return str(_.div[_.div(id='run-time-chart', class_='google-chart',
                                   style=f'height:{100 + 15 * number_of_child_runs}px')[' '],
                             _.script[f'''
drawRunTimeChart('run-time-chart', '{path}', {json.dumps(rows)});
    ''']])
        else:
            return str(_.i(style='color:#888')['Not enough data'])
