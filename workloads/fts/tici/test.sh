# tiup playground(minio)
tiup playground:v1.16.2-feature.fts  --mode tidb-fts --s3.endpoint "http://minio.pingcap.net:9000" --s3.bucket "fts-demo" --without-monitor

# Start pd instance: v9.0.0-beta.2.pre-nightly
# Start tikv instance: v9.0.0-beta.2.pre-nightly
# Start tidb instance: v9.0.0-feature.fts
# Start cdc instance: v8.5.4-nextgen.202510.3-nightly
# Creating changefeed...
# Changefeed created
# Start tici-meta instance: v0.1.0-alpha-nightly
# Start tici-worker instance: v0.1.0-alpha-nightly
# Waiting for tidb instances ready
# - TiDB: 127.0.0.1:4000 ... Done
# The component `tiflash` version v9.0.0-feature.fts is not installed; downloading from repository.
# download http://tiup.pingcap.net:8988/tiflash-v9.0.0-feature.fts-darwin-arm64.tar.gz 118.57 MiB / 118.57 MiB 100.00% 1.40 MiB/s
# Start tiflash instance: v9.0.0-feature.fts
# Waiting for tiflash instances ready
# - TiFlash: 127.0.0.1:3930 ... Done

# 🎉 TiDB Playground Cluster is started with TiCI, enjoy!

# Connect TiDB:    mysql --host 127.0.0.1 --port 4000 -u root
# TiDB Dashboard:  http://127.0.0.1:2379/dashboard

# tiup playground(ks3)
tiup playground:v1.16.2-feature.fts  --mode tidb-fts --s3.endpoint "https://ks3-cn-beijing-internal.ksyuncs.com&force-path-style=false&region=Beijing&provider=ks" --s3.bucket "fts" --s3.access_key "AKLT0DZAljgMQuC5jsGzCTLZ" --s3.secret_key "ONLJF7NNoh1nNOGhccEErxz9x82gm0DOtHD3CkMq" --without-monitor

# tiup playground 和 cluster 创建的集群都会自动创建 changefeed
# prefix 参数用来指定使用 s3 中 bucket 中的哪个目录存储数据，tiup playground 默认使用集群名称
# cluster 默认使用 tici_default_prefix

# [tici]
# [tici.s3]
# access_key = "minioadmin"
# bucket = "fts-demo"
# endpoint = "http://minio.pingcap.net:9000"
# prefix = "V5QnqqW"
# secret_key = "minioadmin"

# idc 创建环境 https://tcms.pingcap.net/dashboard/executions/plan/8019465
# tiup cluster
# tiup cluster 有 bug ，需要给 tiflash 节点添加配置
# [tici.reader_node]
# addr = "0.0.0.0:8520"
# advertise_addr = "tiflash-1-peer:8520"
tiup cluster:v1.16.2-feature.fts deploy ctici nightly deploy.yaml


use test;
create table t3(id varchar(100), a varchar(100), b int, primary key(id));
insert into t3 values ("va", "bonjour", 10);

alter table t3 set tiflash replica 1;
alter table t3 add fulltext index ft_index(a);
select count(*) from t3;

select count(*) from t3 where fts_match_word("bonjour", a);