"""Interfaces for viewing and running pipelines"""

# make sure flask routes are loaded
from data_integration.ui import node_page, run_page, run_time_chart, dependency_graph, last_runs