"""Make the functionalities of this package auto-discoverable by mara-app"""


def MARA_CONFIG_MODULES():
    from . import config
    return [config]


def MARA_FLASK_BLUEPRINTS():
    from .ui import views
    return [views.blueprint]


def MARA_AUTOMIGRATE_SQLALCHEMY_MODELS():
    from .incremental_processing import file_dependencies, processed_files, incremental_copy_status
    from .logging import run_log

    return [
        processed_files.ProcessedFile,
        file_dependencies.FileDependency,
        incremental_copy_status.IncrementalCopyStatus,
        run_log.Run, run_log.NodeRun, run_log.NodeOutput, run_log.SystemStatistics
    ]


def MARA_ACL_RESOURCES():
    from .ui import views
    return {'Pipelines': views.acl_resource}


def MARA_CLICK_COMMANDS():
    from .ui import cli
    return [cli.run, cli.run_interactively, cli.reset_incremental_processing]


def MARA_NAVIGATION_ENTRIES():
    from .ui import views
    return {'Pipelines': views.navigation_entry()}
