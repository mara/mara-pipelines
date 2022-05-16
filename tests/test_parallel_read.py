import pathlib
import pytest
import os

from mara_pipelines.commands.bash import RunBash
from mara_pipelines.parallel_tasks.files import _ParallelRead, ReadMode
from mara_pipelines import pipelines
from mara_pipelines.ui.cli import run_pipeline


EMPTY_FILES_BASE_PATH = pathlib.Path('tests/.tmp/empty_files')


from mara_app.monkey_patch import patch

import mara_pipelines.config
patch(mara_pipelines.config.max_number_of_parallel_tasks)(lambda: 4)

import mara_db.config
patch(mara_db.config.databases)(lambda: {})


@pytest.fixture
def empty_files():
    root_path = EMPTY_FILES_BASE_PATH
    root_path.mkdir(parents=True, exist_ok=True)

    file_list = [str((root_path / str(file)).absolute()) for file in range(25)]

    # create empty files
    for file in file_list:
        open(file, mode='a').close()

    # pass value for testing
    yield file_list

    # cleanup
    for file in file_list:
        os.remove(file)
    root_path.rmdir()


class ParallelTestFileExists(_ParallelRead):
    """Runs a simple test if the file exists"""
    def __init__(self, id: str, description: str, file_pattern: str, read_mode: ReadMode, target_table: str, max_number_of_parallel_tasks: int = None, file_dependencies: [str] = None, date_regex: str = None, partition_target_table_by_day_id: bool = False, truncate_partitions: bool = False, commands_before: [pipelines.Command] = None, commands_after: [pipelines.Command] = None, db_alias: str = None, timezone: str = None) -> None:
        super().__init__(id, description, file_pattern, read_mode, target_table, max_number_of_parallel_tasks, file_dependencies, date_regex, partition_target_table_by_day_id, truncate_partitions, commands_before, commands_after, db_alias, timezone)

    def read_command(self, file_name: str) -> pipelines.Command:
        return RunBash(f'test -f {file_name}')


def test_read_mode_all(empty_files):
    """Tests if the ReadMode.ALL works"""
    pipeline = pipelines.Pipeline(
        id='test_parallel_file_processing',
        description="Test")

    import mara_pipelines.config
    patch(mara_pipelines.config.data_dir)(lambda: EMPTY_FILES_BASE_PATH)

    pipeline.add(
        ParallelTestFileExists(
            id='parallel_test_file_exists',
            description="Runs a test pipeline which checks if a file exist",
            file_pattern='*',
            read_mode=ReadMode.ALL,
            target_table=None,
            max_number_of_parallel_tasks=4))

    run_pipeline(pipeline)