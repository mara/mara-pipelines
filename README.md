# Mara Pipelines

[![Build & Test](https://github.com/mara/mara-pipelines/actions/workflows/build.yaml/badge.svg)](https://github.com/mara/mara-pipelines/actions/workflows/build.yaml)
[![PyPI - License](https://img.shields.io/pypi/l/mara-pipelines.svg)](https://github.com/mara/mara-pipelines/blob/master/LICENSE)
[![PyPI version](https://badge.fury.io/py/mara-pipelines.svg)](https://badge.fury.io/py/mara-pipelines)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://communityinviter.com/apps/mara-users/public-invite)



This package contains a lightweight data transformation framework with a focus on transparency and complexity reduction. It has a number of baked-in assumptions/ principles:

- Data integration pipelines as code: pipelines, tasks and commands are created using declarative Python code.

- PostgreSQL as a data processing engine.

- Extensive web ui. The web browser as the main tool for inspecting, running and debugging pipelines.

- GNU make semantics. Nodes depend on the completion of upstream nodes. No data dependencies or data flows.

- No in-app data processing: command line tools as the main tool for interacting with databases and data.

- Single machine pipeline execution based on Python's [multiprocessing](https://docs.python.org/3.6/library/multiprocessing.html). No need for distributed task queues. Easy debugging and output logging.

- Cost based priority queues: nodes with higher cost (based on recorded run times) are run first.

&nbsp;

## Installation

To use the library directly, use pip:

```
pip install mara-pipelines
```

or
 
```
pip install git+https://github.com/mara/mara-pipelines.git
```

For an example of an integration into a flask application, have a look at the [mara example project 1](https://github.com/mara/mara-example-project-1) and [mara example project 2](https://github.com/mara/mara-example-project-2).

Due to the heavy use of forking, Mara Pipelines does not run natively on Windows. If you want to run it on Windows, then please use Docker or the [Windows Subsystem for Linux](https://en.wikipedia.org/wiki/Windows_Subsystem_for_Linux). 

&nbsp;

## Example

Here is a pipeline "demo" consisting of three nodes that depend on each other: the task `ping_localhost`, the pipeline `sub_pipeline` and the task `sleep`:

```python
from mara_pipelines.commands.bash import RunBash
from mara_pipelines.pipelines import Pipeline, Task
from mara_pipelines.ui.cli import run_pipeline, run_interactively

pipeline = Pipeline(
    id='demo',
    description='A small pipeline that demonstrates the interplay between pipelines, tasks and commands')

pipeline.add(Task(id='ping_localhost', description='Pings localhost',
                  commands=[RunBash('ping -c 3 localhost')]))

sub_pipeline = Pipeline(id='sub_pipeline', description='Pings a number of hosts')

for host in ['google', 'amazon', 'facebook']:
    sub_pipeline.add(Task(id=f'ping_{host}', description=f'Pings {host}',
                          commands=[RunBash(f'ping -c 3 {host}.com')]))

sub_pipeline.add_dependency('ping_amazon', 'ping_facebook')
sub_pipeline.add(Task(id='ping_foo', description='Pings foo',
                      commands=[RunBash('ping foo')]), ['ping_amazon'])

pipeline.add(sub_pipeline, ['ping_localhost'])

pipeline.add(Task(id='sleep', description='Sleeps for 2 seconds',
                  commands=[RunBash('sleep 2')]), ['sub_pipeline'])
```

Tasks contain lists of commands, which do the actual work (in this case running bash commands that ping various hosts). 

&nbsp;

In order to run the pipeline, a PostgreSQL database needs to be configured for storing run-time information, run output and status of incremental processing: 

```python
import mara_db.auto_migration
import mara_db.config
import mara_db.dbs

mara_db.config.databases \
    = lambda: {'mara': mara_db.dbs.PostgreSQLDB(host='localhost', user='root', database='example_etl_mara')}

mara_db.auto_migration.auto_discover_models_and_migrate()
```

Given that PostgresSQL is running and the credentials work, the output looks like this (a database with a number of tables is created):

```
Created database "postgresql+psycopg2://root@localhost/example_etl_mara"

CREATE TABLE data_integration_file_dependency (
    node_path TEXT[] NOT NULL, 
    dependency_type VARCHAR NOT NULL, 
    hash VARCHAR, 
    timestamp TIMESTAMP WITHOUT TIME ZONE, 
    PRIMARY KEY (node_path, dependency_type)
);

.. more tables
```

### CLI UI

This runs a pipeline with output to stdout:

```python
from mara_pipelines.ui.cli import run_pipeline

run_pipeline(pipeline)
```

![Example run cli 1](https://github.com/mara/mara-pipelines/raw/3.2.x/docs/example-run-cli-1.gif)

&nbsp;

And this runs a single node of pipeline `sub_pipeline` together with all the nodes that it depends on:

```python
run_pipeline(sub_pipeline, nodes=[sub_pipeline.nodes['ping_amazon']], with_upstreams=True)
```

![Example run cli 2](https://github.com/mara/mara-pipelines/raw/3.2.x/docs/example-run-cli-2.gif)

&nbsp;


And finally, there is some sort of menu based on [pythondialog](http://pythondialog.sourceforge.net/) that allows to navigate and run pipelines like this:

```python
from mara_pipelines.ui.cli import run_interactively

run_interactively()
```

![Example run cli 3](https://github.com/mara/mara-pipelines/raw/3.2.x/docs/example-run-cli-3.gif)



### Web UI

More importantly, this package provides an extensive web interface. It can be easily integrated into any [Flask](https://flask.palletsprojects.com/) based app and the [mara example project](https://github.com/mara/mara-example-project) demonstrates how to do this using [mara-app](https://github.com/mara/mara-app).

For each pipeline, there is a page that shows

- a graph of all child nodes and the dependencies between them
- a chart of the overal run time of the pipeline and it's most expensive nodes over the last 30 days (configurable)
- a table of all the pipeline's nodes with their average run times and the resulting queuing priority
- output and timeline for the last runs of the pipeline


![Mara pipelines web ui 1](https://github.com/mara/mara-pipelines/raw/3.2.x/docs/mara-pipelines-web-ui-1.png)

For each task, there is a page showing 

- the upstreams and downstreams of the task in the pipeline
- the run times of the task in the last 30 days
- all commands of the task
- output of the last runs of the task

![Mara pipelines web ui 2](https://github.com/mara/mara-pipelines/raw/3.2.x/docs/mara-pipelines-web-ui-2.png)


Pipelines and tasks can be run from the web ui directly, which is probably one of the main features of this package: 

![Example run web ui](https://github.com/mara/mara-pipelines/raw/3.2.x/docs/example-run-web-ui.gif)

&nbsp;

# Getting started

Documentation is currently work in progress. Please use the [mara example project 1](https://github.com/mara/mara-example-project-1) and [mara example project 2](https://github.com/mara/mara-example-project-2) as a reference for getting started. 


