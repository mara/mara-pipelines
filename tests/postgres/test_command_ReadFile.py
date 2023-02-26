import pathlib
import pytest
import sqlalchemy
from typing import Tuple

from mara_app.monkey_patch import patch

from mara_db import dbs, formats
import mara_pipelines.config
patch(mara_pipelines.config.data_dir)(lambda: pathlib.Path(__file__).parent)

from mara_pipelines.commands.sql import ExecuteSQL
from mara_pipelines.commands.files import ReadFile, Compression
from mara_pipelines.pipelines import Pipeline, Task
from tests.command_helper import run_command


FILE_PATH = pathlib.Path(__file__).parent


def db_is_responsive(db: dbs.DB) -> bool:
    """Returns True when the DB is available on the given port, otherwise False"""
    engine = sqlalchemy.create_engine(db.sqlalchemy_url, pool_pre_ping=True)

    try:
        with engine.connect() as conn:
            return True
    except:
        return False


@pytest.fixture(scope="session")
def postgres_db(docker_ip, docker_services) -> Tuple[str, int]:
    """Ensures that PostgreSQL server is running on docker."""

    docker_port = docker_services.port_for("postgres", 5432)
    mara_db = dbs.PostgreSQLDB(host=docker_ip,
                               port=docker_port,
                               user="mara",
                               password="mara",
                               database="mara")

    # here we need to wait until the PostgreSQL port is available.
    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: db_is_responsive(mara_db)
    )

    # create the dwh database
    conn = dbs.connect(mara_db)  # dbt.cursor_context cannot be used here because
                                 # CREATE DATABASE cannot run inside a
                                 # transaction block
    cur = conn.cursor()
    conn.autocommit = True
    cur.execute('''
CREATE DATABASE "dwh"
  WITH OWNER "mara"
  ENCODING 'UTF8'
  TEMPLATE template0
  LC_COLLATE = 'en_US.UTF-8'
  LC_CTYPE = 'en_US.UTF-8'
''')
    cur.close()
    conn.close()

    dwh_db = dbs.PostgreSQLDB(host=docker_ip,
                              port=docker_port,
                              user="mara",
                              password="mara",
                              database="dwh")

    import mara_db.config
    patch(mara_db.config.databases)(lambda: {
        'mara': mara_db,
        'dwh': dwh_db
    })
    patch(mara_pipelines.config.default_db_alias)(lambda: 'dwh')

    return dwh_db


@pytest.mark.dependency()
def test_postgres_initial_ddl(postgres_db):
    """Creates DDL scripts required for other tests"""
    ddl_file_path = str((pathlib.Path(__file__).parent / 'postgres_initial_ddl.sql').absolute())
    assert run_command(
        ExecuteSQL(sql_file_name=ddl_file_path),

        base_path=FILE_PATH
    )


@pytest.mark.dependency(depends=["test_postgres_initial_ddl"])
def test_read_file(postgres_db):
    """Tests command ReadFile"""
    assert run_command(
        ReadFile(file_name='names.csv',
                 compression=Compression.NONE,
                 target_table='names',
                 file_format=formats.CsvFormat()),

        base_path=FILE_PATH
    )

    with dbs.cursor_context('dwh') as conn:
        result = conn.execute("SELECT COUNT(*) FROM names;")
        assert 10, result.fetchone()[0]


@pytest.mark.dependency(depends=["test_postgres_initial_ddl"])
def test_read_file_old_parameters(postgres_db):
    """Tests command ReadFile"""
    assert run_command(
        ReadFile(file_name='names.csv',
                 compression=Compression.NONE,
                 target_table='names',
                 csv_format=True),

        base_path=FILE_PATH
    )

    with dbs.cursor_context('dwh') as conn:
        result = conn.execute("SELECT COUNT(*) FROM names;")
        assert 10, result.fetchone()[0]
