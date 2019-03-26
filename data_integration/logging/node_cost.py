"""Computation of node duration, run time and cost"""

import functools
import math

import mara_db.config
import mara_db.postgresql
from .. import pipelines


def node_durations_and_run_times(path: [str]) -> {tuple: [float, float]}:
    """
    Returns for all nodes below `path` the average duration and run time (sum of average duration of all leaf nodes)

    Args:
        path: The path of the parent node

    Returns:
        A dictionary of {(node_path,): [avg_duration, avg_run_time]} entries

    """
    with mara_db.postgresql.postgres_cursor_context('mara') as cursor:
        cursor.execute(f"""
WITH all_durations AS (
  -- average run time for all nodes below the path
    SELECT
      node_path,
      round(avg(extract(EPOCH FROM end_time - start_time)) :: NUMERIC, 1)::DOUBLE PRECISION AS avg_duration,
      bool_or(is_pipeline) as is_pipeline
    FROM data_integration_node_run
    WHERE node_path [0:{'%(level)s'}] = %(path)s
    GROUP BY node_path)

SELECT 
    node.node_path, 
    min(node.avg_duration) AS avg_duration,
    sum(leaf.avg_duration) AS avg_run_time
FROM all_durations node
  LEFT JOIN all_durations leaf
    ON leaf.node_path[0:array_length(node.node_path,1)] = node.node_path
       AND NOT leaf.is_pipeline
GROUP BY node.node_path;""", {'path': path, 'level': len(path)})

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
