CLI
===

.. module:: mara_pipelines.cli

This part of the documentation covers all the available cli commands of Mara DB.


``run``
-----------

.. tabs::

    .. group-tab:: Mara CLI

        .. code-block:: shell

            mara pipelines run [--path <path>] [--nodes <nodes>] [--with_upstreams] [--disable-colors]

    .. group-tab:: Mara Flask App

        .. code-block:: python

            flask mara-pipelines run [--path <path>] [--nodes <nodes>] [--with_upstreams] [--disable-colors]


Runs a pipeline or a sub-set of its nodes

Available arguments:

* `--path <path>` - The id of of the pipeline to run. Example: "pipeline-id"; "" (default) is the root pipeline.
* `--nodes <nodes>` - IDs of sub-nodes of the pipeline to run, separated by comma. When provided, then only these nodes are run. Example: "do-this,do-that".
* `--with_upstreams` - Also run all upstreams of --nodes within the pipeline.
* `--disable-colors` - Output logs without coloring them.


``run_interactively``
-----------

.. tabs::

    .. group-tab:: Mara CLI

        .. code-block:: shell

            mara pipelines run-interactively

    .. group-tab:: Mara Flask App

        .. code-block:: python

            flask mara-pipelines run-interactively


Select and run data pipelines



``reset_incremental_processing``
-----------

.. tabs::

    .. group-tab:: Mara CLI

        .. code-block:: shell

            mara pipelines reset-incremental-processing [--path <path>]

    .. group-tab:: Mara Flask App

        .. code-block:: python

            flask mara-pipelines reset-incremental-processing [--path <path>]


Reset status of incremental processing for a node

Available arguments:

* `--path <path>` - The parent ids of of the node to reset. Example: "pipeline-id,sub-pipeline-id".
