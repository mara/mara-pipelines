Installation
============

Python Version
--------------

We recommend using the latest version of Python. Mara supports Python
3.6 and newer.

Dependencies
------------

These packages will be installed automatically when installing Mara DB.

* `Mara DB`_ core module offering a minimal API for defining pages of Flask-based backends
* `Mara Page`_ core module for defining pages of Flask-based backends
* `Graphviz <https://graphviz.readthedocs.io/>`_ facilitates the creation and rendering of graph descriptions in the `DOT <https://www.graphviz.org/doc/info/lang.html>`_ language of the `Graphviz <https://www.graphviz.org/>`_ graph drawing software from Python.
* `python-dateutil`_ provides powerful extensions to the standard *datetime* module
* `pythondialog`_ easy writing of graphical interfaces for terminal-based applications
* `more-itertools`_ python library is a gem - you can compose elegant solutions for a variety of problems with the functions it provides
* `psutil`_ a cross-platform library for retrieving information on running processes and system utilization (CPU, memory, disks, network, sensors)
* `requests`_ is an elegant and simple HTTP library for Python, built for human beings.

.. _Mara DB: https://mara-db.readthedocs.io/
.. _Mara Page: https://mara-page.readthedocs.io/
.. _python-dateutil: https://pythondialog.sourceforge.io/
.. _pythondialog: https://github.com/dateutil/dateutil
.. _more-itertools: https://github.com/more-itertools/more-itertools
.. _psutil: https://github.com/giampaolo/psutil
.. _requests: https://requests.readthedocs.io/en/latest/


Install Mara Pipelines
----------------------

To use the library directly, use pip:

.. tabs::

    .. group-tab:: pip

        .. code-block:: bash

            $ pip install mara-pipelines

    .. group-tab:: pip + git repo

        .. code-block:: bash

            $ pip install git+https://github.com/mara/mara-pipelines.git
