# Changelog

## 2.0.0 (2019-04-12)

- Remove dependency_links from setup.py to regain compatibility with recent pip versions
- Change MARA_XXX variables to functions to delay importing of imports
- move some imports into the functions that use them in order to improve loading speed

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
