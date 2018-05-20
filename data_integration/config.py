"""Configuration of data integration pipelines and how to run them"""

import datetime
import multiprocessing
import pathlib

from mara_config import declare_config

from data_integration import pipelines


@declare_config()
def root_pipeline() -> 'pipelines.Pipeline':
    """A pipeline that contains all other pipelines of the project"""
    return pipelines.demo_pipeline()


@declare_config()
def data_dir() -> str:
    """Where to find local data files"""
    return str(pathlib.Path('data').absolute())


@declare_config()
def default_db_alias() -> str:
    """The alias of the database that should be used when not specified otherwise"""
    return 'dwh-etl'


@declare_config()
def first_date() -> datetime.date:
    """Ignore data before this date"""
    return datetime.date(2000, 1, 1)


@declare_config()
def last_date() -> datetime.date:
    """Ignore data after this date"""
    return datetime.date(3000, 1, 1)


@declare_config()
def max_number_of_parallel_tasks():
    """How many tasks can run in parallel at maximum"""
    return multiprocessing.cpu_count()


@declare_config()
def bash_command_string() -> str:
    """The command used for running a bash, should somehow include the `pipefail` option"""
    return '/usr/bin/env bash -o pipefail'


@declare_config()
def system_statistics_collection_period() -> int:
    """How often should system statistics be collected in seconds"""
    return 1


@declare_config()
def run_log_retention_in_days() -> int:
    """How many days to keep node run times, output logs and system statistics"""
    return 30


@declare_config()
def allow_run_from_web_ui() -> bool:
    """When false, then it is not possible to run an ETL from the web UI"""
    return True


@declare_config()
def base_url() -> str:
    """External url of flask app, for linking nodes in slack messages"""
    return 'http://127.0.0.1:5000/data-integration'


@declare_config()
def slack_token() -> str:
    """
    When not None, then this slack webhook is notified of failed nodes.
    Slack channel's token (i.e. THISIS/ASLACK/TOCKEN) can be retrieved from the
    channel's app "Incoming WebHooks" configuration as part part of the Webhook URL
    """
    return None
