支持下列命令:
preparedb 创建库
preparetable 创建表，写入数据
run 运行负载
cleanup 删除所有的库

新增下列参数:
  --db_prefix=STRING            Database name prefix [sbtest]
  --dbs=N                       Number of databases [1]
  --dml_percentage=N            DML on percentage of all tables [0.1]  

prepare 时候先并发 create database，然后并发创建 table
由于 lua 内存的限制，实测单客户端可以对 10w 个表执行 read_write 负载