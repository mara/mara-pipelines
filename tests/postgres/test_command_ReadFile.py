import pathlib
import pytest
from typing import Tuple, Iterator

from mara_app.monkey_patch import patch
from mara_db import dbs, formats
from mara_pipelines.commands.sql import ExecuteSQL
from mara_pipelines.commands.files import ReadFile, Compression

from tests.command_helper import run_command
from tests.db_test_helper import db_is_responsive, db_replace_placeholders
from tests.local_config import POSTGRES_DB

import mara_pipelines.config
patch(mara_pipelines.config.data_dir)(lambda: pathlib.Path(__file__).parent)

FILE_PATH = pathlib.Path(__file__).parent


if not POSTGRES_DB:
    pytest.skip("skipping PostgreSQL tests: variable POSTGRES_DB not set", allow_module_level=True)


@pytest.fixture(scope="session")
def postgres_db(docker_ip, docker_services) -> Tuple[str, int]:
    """Ensures that PostgreSQL server is running on docker."""

    docker_port = docker_services.port_for("postgres", 5432)
    _mara_db = db_replace_placeholders(POSTGRES_DB, docker_ip, docker_port)

    # here we need to wait until the PostgreSQL port is available.
    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: db_is_responsive(_mara_db)
    )

    # create the dwh database
    conn: dbs.DB = None
    try:
        conn = dbs.connect(_mara_db)  # dbt.cursor_context cannot be used here because
                                      # CREATE DATABASE cannot run inside a
                                      # transaction block
        try:
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
        finally:
            if cur:
                cur.close()
    finally:
        if conn:
            conn.close()

    dwh_db = db_replace_placeholders(POSTGRES_DB, docker_ip, docker_port, database='dwh')

    import mara_db.config
    patch(mara_db.config.databases)(lambda: {
        'mara': _mara_db,
        'dwh': dwh_db
    })
    patch(mara_pipelines.config.default_db_alias)(lambda: 'dwh')

    return dwh_db


@pytest.mark.dependency()
@pytest.fixture
def names_table(postgres_db) -> Iterator[str]:
    """
    Provides a 'names' table for tests.
    """
    ddl_file_path = str((pathlib.Path(__file__).parent / 'names_dll_create.sql').absolute())
    assert run_command(
        ExecuteSQL(sql_file_name=ddl_file_path),

        base_path=FILE_PATH
    )

    yield "names"

    ddl_file_path = str((pathlib.Path(__file__).parent / 'names_dll_drop.sql').absolute())
    assert run_command(
        ExecuteSQL(sql_file_name=ddl_file_path),

        base_path=FILE_PATH
    )


@pytest.mark.postgres_db
def test_read_file(names_table):
    """Tests command ReadFile"""
    assert run_command(
        ReadFile(file_name='names.csv',
                 compression=Compression.NONE,
                 target_table=names_table,
                 file_format=formats.CsvFormat()),

        base_path=FILE_PATH
    )


    with dbs.cursor_context('dwh') as cur:
        try:
            result = cur.execute(f'SELECT COUNT(*) FROM "{names_table}";')
            assert 10, result.fetchone()[0]

        finally:
            cur.execute(f'DELETE FROM "{names_table}";')


@pytest.mark.postgres_db
def test_read_file_old_parameters(names_table):
    """Tests command ReadFile"""
    assert run_command(
        ReadFile(file_name='names.csv',
                 compression=Compression.NONE,
                 target_table=names_table,
                 csv_format=True),

        base_path=FILE_PATH
    )

    with dbs.cursor_context('dwh') as cur:
        try:
            result = cur.execute(f'SELECT COUNT(*) FROM "{names_table}";')
            assert 10, result.fetchone()[0]

        finally:
            cur.execute(f'DELETE FROM "{names_table}";')
