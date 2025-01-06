## Test Env
Using the image `hub.pingcap.net/lilinghai/sysbench:master`, you only need to copy the Lua files in that directory to `/usr/local/share/sysbench/`. There is no need to recompile or build the image.

## workload command
Supported commands: prepareuser, rotateuser, preparedb, preparetable, preparedata, analyze, run, ddl, admincheck, rename, cleanup, help.   
preparedb, create database  
preparetable, create table    
preparedata, insert data(You can config ```./pd-ctl scheduler remove evict-slow-store-scheduler``` to avoid raising region unavailable error)  
run, dml workload  
analyze, collect table statistics   
cleanup, drop database  
prepareuser, create 2 users for each database  
rotateuser, alter user credentials  
ddl, Execute ADD COLUMN, DROP COLUMN, ADD INDEX, DROP INDEX, ALTER TABLE ADD/MODIFY COLUMN DDL  
admincheck, admin check table  
rename, rename table name  
help displays usage information for the test specified with the testname argument.  

## workload parameters
sysbench testname help command is used to describe available options provided by a particular test.  
sysbench oltp_read_write help
```lua
    db_prefix = {"Database name prefix", "sbtest"},
    dbs = {"Number of databases", 1},
    tables = {"Number of tables per db", 1},
    partition_table_ratio = {"Ratio of partition table", 0},
    partition_type = {"Type of partition. The value can be one of [range,list,hash]", "hash"},
    partitions_per_table = {"Number of partitions per db", 10},
    extra_columns = {"Number of extra string columns", 0},
    extra_indexs = {"Number of extra normal indexs", 0},
    extra_column_width = {"Width of extra string column", 10},
    create_global_index = {"Create a global index", false},
    db_begin_id = {"Begin ID of db operation(preparedb,preparetable,analyze,cleanup,ddl)", 1},
    db_end_id = {"End ID of db operation(preparedb,preparetable,analyze,cleanup,ddl), 0 means dbs", 0},
    dml_percentage = {"DML on percentage of all tables [0~1]", 0.1},
    table_random_type = {"Random type of DML table [uniform,iter]", "uniform"},
    txn_interval = {"Transaction interval(ms)", 0},
    read_staleness = {"Read staleness in seconds, for example you can set -5", 0},
    extra_selects = {"Enable/disable extra SELECT queries when extra_indexs > 0", false},
    user_batch = {"Number of Alter user", 1},
    ddl_type = {"Type of ddl [drop_column,add_column,drop_index,add_index,change_column_type,all], all means all ddls",
                "all"},
    ddl_name_prefix = {"Name of ddl name prefix(dnp_c for new column, dnp_i for new index)", "dnp"},
    rename_db_prefix = {"Database rename prefix. You shoud create databases before rename", "rnsbtest"},
    table_size = {"Number of rows per table", 10000},
    range_size = {"Range size for range SELECT queries", 100},
    point_selects = {"Number of point SELECT queries per transaction", 10},
    simple_ranges = {"Number of simple range SELECT queries per transaction", 1},
    sum_ranges = {"Number of SELECT SUM() queries per transaction", 1},
    order_ranges = {"Number of SELECT ORDER BY queries per transaction", 1},
    distinct_ranges = {"Number of SELECT DISTINCT queries per transaction", 1},
    index_updates = {"Number of UPDATE index queries per transaction", 1},
    non_index_updates = {"Number of UPDATE non-index queries per transaction", 1},
    delete_inserts = {"Number of DELETE/INSERT combinations per transaction", 1},
    point_get = {"Enable/disable point get query", true},
    range_selects = {"Enable/disable all range SELECT queries", true},
    index_equal_selects = {"Enable/disable index equal (k column) SELECT queries", false},
    index_range_selects = {"Enable/disable index range (k column) SELECT queries", false},
    join1_selects = {"Enable/disable join1 SELECT queries", false},
    auto_inc = {"Use AUTO_INCREMENT column as Primary Key (for MySQL), " ..
        "or its alternatives in other DBMS. When disabled, use " .. "client-generated IDs", true},
    create_table_options = {"Extra CREATE TABLE options", ""},
    skip_trx = {"Don't start explicit transactions and execute all queries " .. "in the AUTOCOMMIT mode", false},
    secondary = {"Use a secondary index in place of the PRIMARY KEY", false},
    reconnect = {"Reconnect after every N events. The default (0) is to not reconnect", 0},
    mysql_storage_engine = {"Storage engine, if MySQL is used", "innodb"},
    pgsql_variant = {"Use this PostgreSQL variant when running with the " ..
        "PostgreSQL driver. The only currently supported " .. "variant is 'redshift'. When enabled, " ..
        "create_secondary is automatically disabled, and " .. "delete_inserts is set to 0"}
```

You can use sysbench --help to display the general command line syntax and options.

## Usage example  
### Prepare data
1. Create 100000 databases
```bash
sysbench oltp_read_write preparedb --mysql-db=test --mysql-user=root --mysql-password="" --mysql-host=10.104.104.44 --mysql-port=4000 --db_prefix=sbtest --dbs=100000 --tables=2 --table_size=10000 --threads=64
```
2. Create 200000 tables  
```bash
sysbench oltp_read_write preparetable --mysql-db=test --mysql-user=root --mysql-password="" --mysql-host=10.104.104.44 --mysql-port=4000 --db_prefix=sbtest --dbs=100000 --tables=2 --table_size=10000 --threads=64
```
3. Insert 10000 rows to each table
```bash
sysbench oltp_read_write preparedata --mysql-db=test --mysql-user=root --mysql-password="" --mysql-host=10.104.104.44 --mysql-port=4000 --db_prefix=sbtest --dbs=100000 --tables=2 --table_size=10000 --threads=64
```

### dml scenario
1. Execute dml stmts on the 1/10 tables
```bash
sysbench oltp_read_write run --mysql-db=test --mysql-user=root --mysql-password="" --mysql-host=10.104.104.44 --mysql-port=4000 --db_prefix=sbtest --dbs=100000 --tables=2 --table_size=10000 --threads=64 --dml_percentage=0.1
```

### ddl scenario
1. Add column ddl
```bash
sysbench oltp_read_write ddl --mysql-db=test --mysql-user=root --mysql-password="" --mysql-host=10.104.104.44 --mysql-port=4000 --db_prefix=sbtest --dbs=100000 --tables=2 --table_size=10000 --threads=64 --ddl_type=add_column
```
2. Add index ddl(add_column needs to be executed first)  
```bash
sysbench oltp_read_write ddl --mysql-db=test --mysql-user=root --mysql-password="" --mysql-host=10.104.104.44 --mysql-port=4000 --db_prefix=sbtest --dbs=100000 --tables=2 --table_size=10000 --threads=64 --ddl_type=add_index
```

## image build
Due to the memory limitations of Lua, it has been observed that a single client can encounter a PANIC: unprotected error in call to Lua API (not enough memory) when executing read_write loads on 100,000 tables (https://github.com/akopytov/sysbench/issues/120). On 64-bit systems (including x86_64), the LuaJIT garbage collector can only manage up to 2GB of memory, which has been a longstanding complaint in the community. To resolve this issue, sysbench needs to be upgraded to LuaJIT-2.1 and recompiled. An internally compiled image is available at hub.pingcap.net/lilinghai/sysbench:master.

1. download LuaJIT-2.1 from https://github.com/LuaJIT/LuaJIT
2. replace in Sysbench source tree ./third_party/luajit/luajit by the tree from LuaJIT-2.1
3. https://github.com/akopytov/sysbench/blob/master/Dockerfile?open_in_browser=true compile master sysbench and build image
