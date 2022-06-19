Configuration
=============


Mara Configuration Values
-------------------------

The following configuration values are used by this module. They are defined as python functions in ``mara_db.config``
and can be changed with the `monkey patch`_ from `Mara App`_. An example can be found `here <https://github.com/mara/mara-example-project-1/blob/master/app/local_setup.py.example>`_.

.. _monkey patch: https://github.com/mara/mara-app/blob/master/mara_app/monkey_patch.py
.. _Mara App: https://github.com/mara/mara-app


.. module:: mara_pipelines.config

.. autofunction:: root_pipeline

|

.. autofunction:: data_dir

|

.. autofunction:: default_db_alias

|

.. autofunction:: default_task_max_retries

|

.. autofunction:: first_date

|

.. autofunction:: last_date

|

.. autofunction:: max_number_of_parallel_tasks

|

.. autofunction:: bash_command_string

|

.. autofunction:: system_statistics_collection_period

|

.. autofunction:: run_log_retention_in_days

|

.. autofunction:: allow_run_from_web_ui

|

.. autofunction:: base_url

|

.. autofunction:: slack_token

|

.. autofunction:: event_handlers

|

.. autofunction:: password_masks
