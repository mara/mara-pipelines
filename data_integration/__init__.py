def MARA_CONFIG_MODULES():
    from data_integration import config
    return [config]


def MARA_FLASK_BLUEPRINTS():
    from data_integration.ui import views
    return [views.blueprint]


def MARA_AUTOMIGRATE_SQLALCHEMY_MODELS():
    from data_integration.incremental_processing import file_dependencies, processed_files, incremental_copy_status
    from data_integration.logging import run_log

    return [processed_files.ProcessedFile, file_dependencies.FileDependency,
            incremental_copy_status.IncrementalCopyStatus,
            run_log.Run, run_log.NodeRun, run_log.NodeOutput, run_log.SystemStatistics]


def MARA_ACL_RESOURCES():
    from data_integration.ui import views
    return [views.acl_resource]


def MARA_CLICK_COMMANDS():
    from data_integration.ui import views, cli
    return [cli.run, cli.run_interactively, cli.reset_incremental_processing]


def MARA_NAVIGATION_ENTRY_FNS():
    from data_integration.ui import views
    return [views.navigation_entry]
