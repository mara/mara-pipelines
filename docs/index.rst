.. rst-class:: hide-header

Mara Pipelines documentation
============================

Welcome to Mara Pipelines’s documentation. This is one of the core modules of the `Mara Framework <https://github.com/mara>`_
contains a lightweight data transformation framework with a focus on transparency and complexity reduction. It has a number of baked-in assumptions/ principles:

* Data integration pipelines as code: pipelines, tasks and commands are created using declarative Python code.

* PostgreSQL as a data processing engine.

* Extensive web ui. The web browser as the main tool for inspecting, running and debugging pipelines.

* GNU make semantics. Nodes depend on the completion of upstream nodes. No data dependencies or data flows.

* No in-app data processing: command line tools as the main tool for interacting with databases and data.

* Single machine pipeline execution based on Python's multiprocessing. No need for distributed task queues. Easy debugging and output logging.

* Cost based priority queues: nodes with higher cost (based on recorded run times) are run first.


User's Guide
------------

This part of the documentation focuses on step-by-step instructions how to use this module.

.. toctree::
   :maxdepth: 2

   installation
   getting-started
   example
   config


API Reference
-------------

If you are looking for information on a specific function, class or
method, this part of the documentation is for you.

.. toctree::
   :maxdepth: 2

   commands
   parallel_tasks
   api


Additional Notes
----------------

Legal information and changelog are here for the interested.

.. toctree::
   :maxdepth: 2

   license
   changes
