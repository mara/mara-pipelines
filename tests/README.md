Test notes
==========

There are several types of tests:
* tests run without docker
* tests run with docker

The tests running in docker are marked with their execution setup. E.g. mark `postgres_db` is used for a setup where PostgreSQL is used as data warehouse database, `mssql_db` is used for a setup where SQL Server is used as data warehouse database / and so on. Docker tests are executed sequential, because otherwise they would override their mara configuration.
