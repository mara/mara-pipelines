# Changelog

## 3.2.0 (2021-03-08)

- Fix CopyIncrementally with no data (#54)
- Add ability to specify modification value type in CopyIncrementally (#53)	66e7dc1	Jan Katins <jan.katins@zenjob.com>	4. Mar 2021 at 22:06
- Fix read stderr during command execution (#47)
- Use echo_queries from mara_db.config.default_echo_queries (#58)
- Include all versioned package files in wheel


## 3.1.1 (2020-07-31)

- Fix for visible passwords in the logs despite `mara_pipelines.config.password_masks()`
  set. Bug was introduced in 3.0.0.

## 3.1.0 (2020-07-21)

- Modify shell command to support the Google BigQuery integration
- Add file_dependencies argument to Python commands

## 3.0.0 (2020-06-11)

Rename package from `data_integration` to `mara_pipelines`.

**required changes**

- In requirements.txt, change `-e git+https://github.com/mara/data-integration.git@2.8.3#egg=data-integration` to `-e git+https://github.com/mara/mara-pipelines.git@3.0.0#egg=mara-pipelines`
- If you use the `mara-etl-tools` package, update to version `4.0.0`
- In your project code, replace all imports from `data_integration` to `mara_pipelines`
- Adapt navigation and ACL entries, if you have any (their names changed from "Data integration" to "Pipelines")

Here's an example of how that looks at the mara example project 2: https://github.com/mara/mara-example-project-2/commit/fa2fba148e65533f821a70c18bb0c05c37706a83


## 2.8.3 (2020-06-10)

- Fix duplicated system stats if you run multiple ETLs in parallel (#38)
- Add config default_task_max_retries (#39)
- Cleaner shutdown (#41)


## 2.8.2 (2020-05-04)

- Ignore not succeeded executions in cost calculation (#36)
- Ensure we log errors via events in case of error/shutdown (#33)
- Fix a bug where we reported the wrong error to chat channels when running in
  the browser and did not restart between failed runs (#33)

## 2.8.1 (2020-04-27)

- Fix Problems when frontend and database are in a different timezone (#34)

## 2.8.0 (2020-03-25)

- Implement pipeline notifications via Microsoft Teams #28
- Make it possible to disable output coloring in command line etl runs (#31)

## 2.7.0 (2020-03-05)

- Make event handlers configurable: this allows for e.g. adding your own notifier for specific events
- Switch slack to use events for notifications of interactive pipeline runs
- Fix an edge case bug where reverting a commit after an error in the table creation for an incremental load
  job would not recreate the original tables leading to a failed load
- Fix an edge case bug where crashing during a triggered (code change, TRUNCATE) full load of an
  incremental load job after the table was already loaded would not rerun the full load
  leading to missing data
- Optimize how we set the spawning method in multiprocessing


## 2.6.1 (2020-02-20)

- Fix for Python 3.7 ("RuntimeError: context has already been set")


## 2.6.0 (2020-02-12)

- Python 3.8 compatibility (explicitly set process spawning method to 'fork')
- Fix open runs after browser reload
- Add workaround for system statistics on wsl1
- Speedup incremental insert into partitioned tables
- Show warning when graphviz is not installed

## 2.5.1 (2019-08-01)

- Include file_dependencies as variable for Copy Commands: This could handle cases in ETL pipeline, where the copy command shall be skipped if the sql_files stay the same.


## 2.5.0 (2019-07-07)

- Bug fix: make last modification timestamp of parallel file reading time zone aware (fixes `TypeError: can't compare offset-naive and offset-aware datetimes` error)


## 2.4.0 - 2.4.2 (2019-07-04)

- Add travis integration and PyPi upload


## 2.3.0 (2019-07-04)

- Add parameter `csv_format` and `delimiter_char` to `Copy` and `CopyIncrementally` commands.


## 2.2.0 (2019-07-02)

- Changed all `TIMSTAMP` to `TIMSTAMPTZ` in the mara tables. You have to manually run the
  below migration commands as `make migrate-mara-db` won't pick up this change.

**required changes**
You need to manually convert the mara tables to `TIMESTAMPTZ`:

```SQL
-- Change the timezone to whatever your ETL process is running in
ALTER TABLE data_integration_run ALTER start_time TYPE timestamptz
  USING start_time AT TIME ZONE 'Europe/Berlin';
ALTER TABLE data_integration_run ALTER end_time TYPE timestamptz
  USING end_time AT TIME ZONE 'Europe/Berlin';
ALTER TABLE data_integration_processed_file ALTER last_modified_timestamp TYPE timestamptz
  USING last_modified_timestamp AT TIME ZONE 'Europe/Berlin';
ALTER TABLE data_integration_node_run ALTER start_time TYPE timestamptz
  USING start_time AT TIME ZONE 'Europe/Berlin';
ALTER TABLE data_integration_node_run ALTER end_time TYPE timestamptz
  USING end_time AT TIME ZONE 'Europe/Berlin';
ALTER TABLE data_integration_node_output ALTER timestamp TYPE timestamptz
  USING timestamp AT TIME ZONE 'Europe/Berlin';
ALTER TABLE data_integration_file_dependency ALTER timestamp TYPE timestamptz
  USING timestamp AT TIME ZONE 'Europe/Berlin';
ALTER TABLE data_integration_system_statistics ALTER timestamp TYPE timestamptz
  USING timestamp AT TIME ZONE 'Europe/Berlin';
```

## 2.1.0 (2019-05-15)

- Track and visualize also unfinished pipeline runs
- Speed up computation of node durations and node cost
- Improve error handling in launching of parallel tasks
- Improve run times visualization (better axis labels, independent tooltips)
- Smaller ui improvements


## 2.0.0 - 2.0.1 (2019-04-12)

- Remove dependency_links from setup.py to regain compatibility with recent pip versions
- Change MARA_XXX variables to functions to delay importing of imports
- move some imports into the functions that use them in order to improve loading speed
- Add ability to mask passwords in `Command`s, so they cannot show up in the UI anymore
  or are not written to the database in saved Events (config
  `data_integration.config.password_masks()`). See the example in the original function
  how to not let passwords show up in the settings UI.
  ([gh #14](https://github.com/mara/data-integration/pull/14))

**required changes**

- Update `mara-app` to `>=2.0.0`


## 1.4.0 - 1.4.7 (2018-09-15)

- Use postgresql 10 native partitioning for creating day_id partitions in ParallelReadFile
- Catch and display exceptions when creating html command documentation
- Add python ParallelRunFunction
- Add option to use explicit upsert on incremental load (explicit UPDATE + INSERT)
- Emit a proper NodeFinished event when the launching of a parallel task failed
- Add option truncate_partition to parallel tasks
- Fix bug in run_interactively cli command
- Make it possible to run the ExecuteSQL command outside of a pipeline via .run()
- Add args parameter to RunFunction command
- Show redundant node upstreams as dashed line in pipeline graphs
- Fix problems with too long bash commands by using multiple commands for partition generation in ParallelReadXXX tasks

**required changes**

- When using `ParallelReadFile` with parameter `partition_target_table_by_day_id=True`, then make sure the target table is natively partitioned by adding `PARTITION BY LIST (day_id)`.



## 1.3.0 (2018-07-17)

- Add possibility to continue running child nodes on error (new `Pipeline` parameters `continue_on_error` and `force_run_all_children`)
- Make dependency on requests explicit


## 1.2.0 (2018-06-01)

- Implement ReadMode ONLY_CHANGED that reads all new or modified files
- Show node links in run output only relative to current node (to save space)


## 1.1.0 (2018-05-23)

- Add slack notifications to "run_interactively" cli command
- Add parameter max_retries to class Task
- Fix typos in Readme
- Optimize imports


## 1.0.0 - 1.0.4 (2018-05-02)

- Move to Github
- Improve documentation
- Add ReadMode 'ONLY_LATEST'
- Add new command `ReadScriptOutput`
- Add slack bot configuration
- Fix url in slack event handler
