Parallel Tasks
==============

Parallel tasks allows you to run commands in parallel.

Sample implementation:

* see `github activity <https://github.com/mara/mara-example-project-2/blob/master/app/pipelines/github/__init__.py#L28-L49>`_ pipeline


.. module:: mara_pipelines.parallel_tasks

File tasks
----------

.. module:: mara_pipelines.parallel_tasks.files

.. autoclass:: ParallelReadFile

.. autoclass:: ParallelReadSqlite


Python tasks
------------

.. module:: mara_pipelines.parallel_tasks.python

.. autoclass:: ParallelExecutePython

.. autoclass:: ParallelRunFunction


SQL tasks
---------

.. module:: mara_pipelines.parallel_tasks.sql

.. autoclass:: ParallelExecuteSQL
