# 执行负载的时候可以把 /usr/share/sysbench/ 目录下的负载对应的 lua 文件拷贝到该目录下执行

# prepare
nohup ./oltp_read_write.lua --mysql-host=10.100.127.189 --mysql-port=4000 --mysql-user=root --db-driver=mysql --mysql-db=test --threads=50 --db_prefix=cemo --dbs=100000 --tables=10 --table-size=2000 --auto_inc=off --report-interval=10 --dml_percentage=1 --time=1200 preparedb > res.log 2>&1

nohup ./oltp_read_write.lua --mysql-host=10.100.127.189 --mysql-port=4000 --mysql-user=root --db-driver=mysql --mysql-db=test --threads=50 --db_prefix=cemo --dbs=100000 --tables=10 --table-size=2000 --auto_inc=off --report-interval=10 --dml_percentage=1 --time=1200 preparetable > res.log 2>&1

# run
./oltp_read_write.lua --mysql-host=10.100.127.189 --mysql-port=4000 --mysql-user=root --db-driver=mysql --mysql-db=test --threads=100 --db_prefix=cemo --dbs=100000 --tables=10 --table-size=2000 --auto_inc=off --report-interval=10 --dml_percentage=0.1 --time=1200 --db-ps-mode=disable  run
