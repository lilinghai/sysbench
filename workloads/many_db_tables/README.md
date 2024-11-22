替换 sysbench 默认的 oltp_common.lua，一般在 /usr/local/share/sysbench/oltp_common.lua 路径下。  

Supported commands: preparedb, preparetable, preparedata, analyze, run, cleanup, prepareuser, rotateuser, help.  
preparedb 创建库  
preparetable 创建表
preparedata insert 数据  
run 运行负载  
analyze 收集所有表统计信息  
cleanup 删除所有的库  
prepareuser 创建用户，需要先创建库，每个库分配两个 user  
rotateuser 修改用户权限  

workload parameters:
```
    db_prefix = {"Database name prefix", "sbtest"},
    dbs = {"Number of databases", 1},
    tables = {"Number of tables per db", 1},
    db_begin_id = {"Begin ID of db operation(preparedb,preparetable,analyze,cleanup)", 1},
    db_end_id = {"End ID of db operation(preparedb,preparetable,analyze,cleanup), 0 means dbs", 0},
    dml_percentage = {"DML on percentage of all tables [0~1]", 0.1},
    user_batch = {"Number of Alter user", 1},
    partition_table_ratio = {"Ratio of partition table", 0},
    partition_type = {"Type of partition. The value can be one of [range,list,hash]", "hash"},
    partitions_per_table = {"Number of partitions per db", 10},
    extra_columns = {"Number of extra string columns", 0},
    extra_indexs = {"Number of extra normal indexs", 0},
    extra_column_width = {"Width of extra string column", 10},
    create_global_index = {"Create a global index", false},
    read_staleness = {"Read staleness in seconds, for example you can set -5", 0},
```

example1  
1. 创建 100000 个 database  
sysbench oltp_read_write preparedb --mysql-db=test --mysql-user=root --mysql-password="" --mysql-host=10.104.104.44 --mysql-port=4000 --db_prefix=sbtest --dbs=100000 --tables=2 --table_size=10000 --threads=64
2. 创建 200000 个 tables  
sysbench oltp_read_write preparetable --mysql-db=test --mysql-user=root --mysql-password="" --mysql-host=10.104.104.44 --mysql-port=4000 --db_prefix=sbtest --dbs=100000 --tables=2 --table_size=10000 --threads=64
3. 每个表插入 10000 行数据  
sysbench oltp_read_write preparedata --mysql-db=test --mysql-user=root --mysql-password="" --mysql-host=10.104.104.44 --mysql-port=4000 --db_prefix=sbtest --dbs=100000 --tables=2 --table_size=10000 --threads=64
4. 在 1/10 表上执行 dml 语句  
sysbench oltp_read_write run --mysql-db=test --mysql-user=root --mysql-password="" --mysql-host=10.104.104.44 --mysql-port=4000 --db_prefix=sbtest --dbs=100000 --tables=2 --table_size=10000 --threads=64 --dml_percentage=0.1

由于 lua 内存的限制，实测单客户端可以对 10w 个表执行 read_write 负载
PANIC: unprotected error in call to Lua API (not enough memory)
https://github.com/akopytov/sysbench/issues/120 在64 位系统（包括 x86_64 ）上，LuaJIT 垃圾回收器能管理的内存最大只有2GB 一直为社区所诟病
需要升级 sysbench 到 LuaJIT-2.1，重新编译 sysbench 解决该问题，可以使用内部已经变编译好的 image hub.pingcap.net/lilinghai/sysbench:master 。

https://github.com/akopytov/sysbench/blob/master/Dockerfile?open_in_browser=true 编译 master sysbench
download LuaJIT-2.1 from https://github.com/LuaJIT/LuaJIT
replace in Sysbench source tree ./third_party/luajit/luajit by the tree from LuaJIT-2.1
