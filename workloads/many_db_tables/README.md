Supported commands: preparedb, preparetable, analyze, run, cleanup, help.
preparedb 创建库
preparetable 创建表，写入数据
run 运行负载
analyze 收集所有表统计信息
cleanup 删除所有的库

新增下列参数:
```
    db_prefix = {"Database name prefix", "sbtest"},
    dbs = {"Number of databases", 1},
    dml_percentage = {"DML on percentage of all tables", 0.1},
    user_batch = {"Number of Alter user", 1},
    partition_table_ratio = {"Ratio of partition table", 0},
    partition_type = {"Type of partition. The value can be one of [range,list,hash]", "hash"},
    partitions_per_table = {"Number of partitions per db", 10},
    extra_columns = {"Number of extra string columns", 0},
    extra_indexs = {"Number of extra normal indexs", 0},
    extra_column_width = {"Width of extra string column", 10},
    create_global_index = {"Create a global index", false},
```

prepare 时候先并发 create database，然后并发创建 table
由于 lua 内存的限制，实测单客户端可以对 10w 个表执行 read_write 负载
PANIC: unprotected error in call to Lua API (not enough memory)
https://github.com/akopytov/sysbench/issues/120 在64 位系统（包括 x86_64 ）上，LuaJIT 垃圾回收器能管理的内存最大只有2GB 一直为社区所诟病
需要升级 sysbench 到 LuaJIT-2.1，重新编译 sysbench 解决该问题

https://github.com/akopytov/sysbench/blob/master/Dockerfile?open_in_browser=true 编译 master sysbench
download LuaJIT-2.1 from https://github.com/LuaJIT/LuaJIT
replace in Sysbench source tree ./third_party/luajit/luajit by the tree from LuaJIT-2.1

hub.pingcap.net/lilinghai/sysbench:master