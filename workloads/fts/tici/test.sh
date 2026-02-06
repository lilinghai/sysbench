# tiup mirror set http://tiup.pingcap.net:8988
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
tiup playground:v1.16.2-feature.fts  --mode tidb-fts --s3.endpoint "https://ks3-cn-beijing-internal.ksyuncs.com&force-path-style=false&region=Beijing&provider=ks" --s3.bucket "fts" --s3.access_key "xxx" --s3.secret_key "yyy" --without-monitor

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

alter table t3 set tiflash replica 1;

use test;
create table t3(id varchar(100), a varchar(100), b int, primary key(id));
insert into t3 values ("va", "bonjour", 10);

alter table t3 add fulltext index ft_index(a);
select count(*) from t3;

select count(*) from t3 where fts_match_word("bonjour", a);






# tici k8s 部署
# operator pd/tikv/tidb

# ticdc, tici meta, tici worker, tiflash

# tici meta start cmd and conf
bin/tici-server meta --port 8500 --status-port=8501 --host=0.0.0.0 --advertise-host=tici-meta-peer --pd-addr=pd-peer:2379 --config=conf/tici-meta.toml --log-file=/tiup/deploy/tici-meta-8500/log/tici-meta.log --tidb-addr=mysql://root@tidb-1-peer:4000,mysql://root@tidb-2-peer:4000

# conf/tici-meta.toml
[s3]
access_key = "xxx"
bucket = "fts"
endpoint = "https://ks3-cn-beijing-internal.ksyuncs.com"
secret_key = "yyy"
use_path_style = false


# tiflash start cmd and conf
bin/tiflash/tiflash server --config-file conf/tiflash.toml

# conf/tiflash.toml
[tici]
[tici.reader-node]
addr = "0.0.0.0:8520"
advertise-addr = "tiflash-1-peer:8520"
[tici.s3]
access_key = "xxx"
bucket = "fts"
endpoint = "https://ks3-cn-beijing-internal.ksyuncs.com"
region = "Beijing"
secret_key = "yyy"
use_path_style = false

# tici worker start cmd and conf
bin/tici-server worker --port 8510 --status-port=8511 --host=0.0.0.0 --advertise-host=tici-worker-1-peer --pd-addr=pd-peer:2379 --config=conf/tici-worker.toml --data-dir=/tiup/data/tici-worker-8510 --log-file=/tiup/deploy/tici-worker-8510/log/tici-worker.log

# conf/tici-worker.toml
[s3]
access_key = "xxx"
bucket = "fts"
endpoint = "https://ks3-cn-beijing-internal.ksyuncs.com"
region = "Beijing"
secret_key = "yyy"
use_path_style = false

tiup ctl:nightly cdc changefeed --pd http://pd-peer:2379 create "--sink-uri=s3://fts/tici_default_prefix/cdc?protocol=canal-json&enable-tidb-extension=true&output-row-key=true&flush-interval=0.5s&access-key=xxx&secret-access-key=yyy&force-path-style=false&endpoint=https://ks3-cn-beijing-internal.ksyuncs.com"


# 地址格式
# tc-tiflash-0.tc-tiflash-peer.endless-htap-mpp-kill-tps-8029054-1-93.svc.cluster.local
# tc-pd.endless-htap-mpp-kill-tps-8029054-1-93.svc:2379