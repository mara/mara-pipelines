API
===

.. module:: mara_pipelines

This part of the documentation covers all the interfaces of Mara Page. For
parts where the package depends on external libraries, we document the most
important right here and provide links to the canonical documentation.


Incremental processing
----------------------

.. automodule:: mara_pipelines.incremental_processing
    :noindex:

.. module:: mara_pipelines.incremental_processing.file_dependencies

.. autofunction:: update

.. autofunction:: delete

.. autofunction:: is_modified

.. autofunction:: hash

|

.. module:: mara_pipelines.incremental_processing.incremental_copy_status

.. autofunction:: update

.. autofunction:: delete

.. autofunction:: get_last_comparison_value

|

.. module:: mara_pipelines.incremental_processing.processed_files

.. autofunction:: track_processed_file

.. autofunction:: already_processed_files

|

.. module:: mara_pipelines.incremental_processing.reset

.. autofunction:: reset_incremental_processing

Events
------

.. module:: mara_pipelines.events

.. autoclass:: Event
    :members:

.. autoclass:: EventHandler
    :members:

.. autofunction:: notify_configured_event_handlers


Pipelines
--------------

.. module:: mara_pipelines.pipelines

.. autoclass:: Pipeline

.. autoclass:: Task

.. autoclass:: Command

.. autofunction:: find_node


Shell
-----

.. module:: mara_pipelines.shell

.. autofunction:: run_shell_command

.. autofunction:: sed_command
