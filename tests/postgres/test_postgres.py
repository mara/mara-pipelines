import os
import pathlib
import pytest
import typing as t

from mara_app.monkey_patch import patch
from mara_db import formats
import mara_pipelines.config
from mara_pipelines.commands.bash import RunBash
from mara_pipelines.commands.files import WriteFile
from mara_pipelines.commands.sql import ExecuteSQL
from mara_pipelines.pipelines import Pipeline, Task
from mara_pipelines.cli import run_pipeline

from tests.db_test_helper import db_is_responsive, db_replace_placeholders
from tests.local_config import POSTGRES_DB


if not POSTGRES_DB:
    pytest.skip("skipping PostgreSQL tests: variable POSTGRES_DB not set", allow_module_level=True)


@pytest.fixture(scope="session")
def postgres_db(docker_ip, docker_services) -> t.Tuple[str, int]:
    """Ensures that PostgreSQL server is running on docker."""

    docker_port = docker_services.port_for("postgres", 5432)
    db = db_replace_placeholders(POSTGRES_DB, docker_ip, docker_port)

    # here we need to wait until the PostgreSQL port is available.
    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: db_is_responsive(db)
    )

    import mara_db.config
    patch(mara_db.config.databases)(lambda: {'dwh': db})
    patch(mara_pipelines.config.default_db_alias)(lambda: 'dwh')

    return db


@pytest.mark.dependency()
@pytest.mark.postgres_db
def test_postgres_command_WriteFile(postgres_db):

    # set local temp path
    patch(mara_pipelines.config.data_dir)(lambda: str((pathlib.Path(__file__).parent / '.tmp').absolute()))

    pipeline = Pipeline(
        id='test_postgres_command_write_file',
        description="")

    pipeline.add_initial(
        Task(id='initial_ddl',
             description="",
             commands=[
                 ExecuteSQL("""
DROP TABLE IF EXISTS "test_postgres_command_WriteFile";

CREATE TABLE "test_postgres_command_WriteFile"
(
    Id INT GENERATED ALWAYS AS IDENTITY,
    LongText1 TEXT,
    LongText2 TEXT
);

INSERT INTO "test_postgres_command_WriteFile" (
    LongText1, LongText2
) VALUES
('Hello', 'World!'),
('He lo', ' orld! '),
('Hello\t', ', World! ');
"""),
                 RunBash(f'mkdir -p {mara_pipelines.config.data_dir()}')
             ]))

    pipeline.add(
        Task(id='write_file_csv',
             description="Wirte content of table to file",
             commands=[WriteFile(dest_file_name='write-file.csv',
                                 sql_statement="""SELECT * FROM "test_postgres_command_WriteFile";""",
                                 format=formats.CsvFormat(delimiter_char='\t', header=False))]))

    pipeline.add(
        Task(id='write_file_tsv',
             description="Wirte content of table to file",
             commands=[WriteFile(dest_file_name='write-file.tsv',
                                 sql_statement="""SELECT * FROM "test_postgres_command_WriteFile";""",
                                 format=formats.CsvFormat(delimiter_char='\t', header=False))]))

    assert run_pipeline(pipeline)

    files = [
        str((pathlib.Path(mara_pipelines.config.data_dir()) / 'write-file.csv').absolute()),
        str((pathlib.Path(mara_pipelines.config.data_dir()) / 'write-file.tsv').absolute())
    ]

    file_not_found = []
    for file in files:
        if not os.path.exists(file):
            file_not_found.append(file)
        else:
            os.remove(file)

    assert not file_not_found
