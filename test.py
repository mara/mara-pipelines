import mara_db.auto_migration
import mara_db.config
import mara_db.dbs
from data_integration.pipelines import demo_pipeline
from data_integration.ui.cli import run_pipeline

mara_db.config.databases \
    = lambda: {'mara': mara_db.dbs.PostgreSQLDB(host='127.0.0.1', user='root', database='example_etl_mara')}

mara_db.auto_migration.auto_discover_models_and_migrate()

run_pipeline(demo_pipeline())
