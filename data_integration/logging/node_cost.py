"""Computation of node duration, run time and cost"""

import functools
import math

import mara_db.config
import mara_db.postgresql
from .. import pipelines


def node_durations_and_run_times(node: pipelines.Node) -> {tuple: [float, float]}:
    """
    Returns for children of `node` the average duration and run time (sum of average duration of all leaf nodes)

    Args:
        node: The parent node

    Returns:
        A dictionary of {(node_path,): [avg_duration, avg_run_time]} entries for all children of `node`

    """
    with mara_db.postgresql.postgres_cursor_context('mara') as cursor:
        cursor.execute(f"""
WITH child_nodes AS
       (SELECT node.node_path [ 0 : {'%(level)s'} + 1]                     AS node_path,
               avg(CASE -- direct children
                     WHEN array_length(node.node_path, 1) = {'%(level)s'} + 1
                       THEN extract(EPOCH FROM end_time - start_time) END) AS avg_duration,
               avg(CASE -- all children except pipelines
                     WHEN NOT is_pipeline
                       THEN extract(EPOCH FROM end_time - start_time) END) AS avg_run_time
        FROM data_integration_node_run node
        WHERE node.node_path [ 0 : {'%(level)s'}] = %(path)s
          AND array_length(node.node_path, 1) > {'%(level)s'}
        GROUP BY node.node_path)
SELECT node_path,
       round(avg(avg_duration)::NUMERIC, 1) AS avg_duration,
       round(sum(avg_run_time)::NUMERIC, 1) AS avg_run_time
FROM child_nodes
GROUP BY node_path;""", {'path': node.path(), 'level': len(node.path())})

        return {tuple(row[0]): row[1:] for row in cursor.fetchall()}


def compute_cost(node: pipelines.Node, node_durations_and_run_times: {tuple: [float, float]}) -> float:
    """
    Computes the cost of a node as maximum cumulative run time of a node and all its downstreams.
    Stores the result in `node` and also returns it

    Args:
        node: The node for which to compute the cost
        node_durations_and_run_times: Duration and run time information as computed by
                                      the `node_durations_and_run_times` function
    """
    path = tuple(node.path())
    if node.cost is None:
        node.cost = (
                max([compute_cost(downstream, node_durations_and_run_times)
                     for downstream in node.downstreams] or [0])
                + (node_durations_and_run_times.get(path, [0, 0])[1] or 0))

    return node.cost


@functools.lru_cache(maxsize=None)
def format_duration(duration: float) -> str:
    """
    Formats a duration in human readable form

    Args:
        duration: A duration in seconds

    Returns:
        The duration as a string

    Examples:
        >>>> print(format_duration(0.12))
        0.12s

        >>>> print(format_duration(5.6))
        5.6s

        >>>> print(format_duration(70.2))
        1:10m

        >>>> print(format_duration(4000))
        1:06h
    """
    if duration in [None, '']:
        return ''

    hours = math.floor(duration / 3600)
    duration -= 3600 * hours

    minutes = math.floor(duration / 60)
    duration -= 60 * minutes

    seconds = math.floor(duration)

    if hours:
        return f"{hours}:{str(minutes).rjust(2, '0')[:2]}h"
    elif minutes:
        return f"{minutes}:{str(seconds).rjust(2, '0')[:2]}m"
    else:
        return f"{round(duration, 1)}s"
