from data_integration import config
from data_integration.incremental_processing import file_dependencies, processed_files, incremental_copy_status
from data_integration.logging import run_log
from data_integration.ui import views, cli

MARA_CONFIG_MODULES = [config]

MARA_FLASK_BLUEPRINTS = [views.blueprint]

MARA_AUTOMIGRATE_SQLALCHEMY_MODELS = [processed_files.ProcessedFile, file_dependencies.FileDependency,
                                      incremental_copy_status.IncrementalCopyStatus,
                                      run_log.Run, run_log.NodeRun, run_log.NodeOutput, run_log.SystemStatistics]

MARA_ACL_RESOURCES = [views.acl_resource]

MARA_CLICK_COMMANDS = [cli.run, cli.run_interactively, cli.reset_incremental_processing]

MARA_NAVIGATION_ENTRY_FNS = [views.navigation_entry]
