-- TiDB connection panic
-- ERROR 1105 (HY000): interface conversion: expression.Expression is *expression.ScalarFunction, not *expression.Column
CREATE TABLE `t3` (
  `id` varchar(100) NOT NULL,
  `a` varchar(100) DEFAULT NULL,
  `b` int DEFAULT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  FULLTEXT INDEX `ft_index`(`a`) WITH PARSER STANDARD
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
explain select count(*) from test.t3 where fts_match_word("2025", b);

-- 不支持多列查询
mysql> select count(*) from t8 where fts_match_word("2025", b, a);
ERROR 1582 (42000): Incorrect parameter count in the call to native function 'fts_match_word'

-- 创建表时指定了全文索引
mysql> create table t6(a int,b varchar(20),fulltext index(b),primary key(a));
Query OK, 0 rows affected (0.07 sec)
mysql> select count(*) from test.t6 where fts_match_word("2025", b);
ERROR 1105 (HY000): GetShardLocalCacheInfo failed: 1

-- 部署，配置错误要提示，如 s3 配置问题

-- 组合查询还不支持
mysql> SELECT * FROM wiki_abstract WHERE fts_match_word('Bosonic', abstract) or fts_match_word('Bosonic', title);
ERROR 1105 (HY000): Full text search can only be used with a matching fulltext index or you write it in a wrong way

-- dashboard 执行计划显示，cop[tiflash] 应该是 cop[tici]

| id                        | estRows | estCost    | actRows | task         | access object                                 | execution info                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | operator info                                                                                          | memory  | disk  |
| IndexLookUp_8             | 1000.00 | 1955204.29 | 16      | root         |                                               | time:195.3ms, open:6.56µs, close:4.75µs, loops:2, index_task: {total_time: 2.5ms, fetch_handle: 2.5ms, build: 1.51µs, wait: 1.8µs}, table_task: {total_time: 2.7ms, num: 1, concurrency: 5}, next: {wait_index: 192.6ms, wait_table_lookup_build: 7.77µs, wait_table_lookup_resp: 2.64ms}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |                                                                                                        | 11.0 KB | N/A   |
| ├─IndexRangeScan_6(Build) | 1000.00 | 162800.00  | 16      | cop[tiflash] | table:wiki_abstract, index:abstract(abstract) | time:2.5ms, open:0s, close:0s, loops:3, cop_task: {num: 4, max: 0s, min: 0s, avg: 0s, p95: 0s, copr_cache_hit_ratio: 0.00, max_distsql_concurrency: 15}, fetch_resp_duration: 2.41ms, tiflash_task:{proc max:149µs, min:140µs, avg: 144.5µs, p80:149µs, p95:149µs, iters:4, tasks:2, threads:4}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | range:[-inf,+inf], search func:fts_match_word("Bosonic", fts.wiki_abstract.abstract), keep order:false | N/A     | N/A   |
| └─TableRowIDScan_7(Probe) | 1000.00 | 334628.43  | 16      | cop[tikv]    | table:wiki_abstract   
                        | time:2.63ms, open:0s, close:1.77µs, loops:2, cop_task: {num: 5, max: 1.47ms, min: 0s, avg: 509.3µs, p95: 1.47ms, max_proc_keys: 8, p95_proc_keys: 8, tot_proc: 1.37ms, tot_wait: 1.6ms, copr_cache_hit_ratio: 0.00, build_task_duration: 27.1µs, max_distsql_concurrency: 1, max_extra_concurrency: 1, store_batch_num: 3}, fetch_resp_duration: 2.59ms, rpc_info:{Cop:{num_rpc:2, total_time:2.52ms}}, tikv_task:{proc max:0s, min:0s, avg: 0s, p80:0s, p95:0s, iters:5, tasks:5}, scan_detail: {total_process_keys: 16, total_process_keys_size: 6024, total_keys: 16, get_snapshot_time: 1.53ms, rocksdb: {block: {cache_hit_count: 60, read_count: 2, read_byte: 17.7 KB, read_time: 26.8µs}}}, time_detail: {total_process_time: 1.37ms, total_wait_time: 1.6ms, tikv_grpc_process_time: 85.4µs, tikv_grpc_wait_time: 19.8µs, tikv_wall_time: 1.68ms} | keep order:false                                                                                       | N/A     | N/A   |


-- meta 错误日志
[2026-01-07T02:42:34Z INFO  tici::meta::writer::gc_worker] gc error, service error

    Caused by:
        0: unhandled error (InvalidRequest)
        1: Error { code: "InvalidRequest", message: "Missing required header for this request: Content-MD5.", aws_request_id: "fksa0teg3otoa47vfp8qpmtclmdc0ahu" }


-- worker 错误日志
[2025-12-20T17:37:28Z ERROR tici_shard::writer::cdc_file_poller] Failed to list objects=ServiceError(ServiceError { source: Unhandled(Unhandled { source: ErrorMetadata { code: Some("TooManyListRequests"), message: Some("Too Many List Requests"), extras: Some({"aws_request_id": "fbdk6020kcmobjfvc9ib5mv4lkjg1b4j"}) }, meta: ErrorMetadata { code: Some("TooManyListRequests"), message: Some("Too Many List Requests"), extras: Some({"aws_request_id": "fbdk6020kcmobjfvc9ib5mv4lkjg1b4j"}) } }), raw: Response { status: StatusCode(429), headers: Headers { headers: {"date": HeaderValue { _private: H1("Sat, 20 Dec 2025 17:37:28 GMT") }, "content-type": HeaderValue { _private: H1("application/xml") }, "transfer-encoding": HeaderValue { _private: H1("chunked") }, "connection": HeaderValue { _private: H1("keep-alive") }, "x-kss-request-id": HeaderValue { _private: H1("fbdk6020kcmobjfvc9ib5mv4lkjg1b4j") }, "x-amz-request-id": HeaderValue { _private: H1("fbdk6020kcmobjfvc9ib5mv4lkjg1b4j") }, "server": HeaderValue { _private: H1("KS3") }} }, body: SdkBody { inner: Once(Some(b"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><Error><Code>TooManyListRequests</Code><Message>Too Many List Requests</Message><Resource>/fts/?list-type=2&amp;prefix=tici_default_prefix/cdc/fts/wiki_abstract&amp;start-after=tici_default_prefix/cdc/fts/wiki_abstract/462934195599835137/2025-12-17/CDC00000000000000000065.json</Resource><RequestId>fbdk6020kcmobjfvc9ib5mv4lkjg1b4j</RequestId></Error>")), retryable: true }, extensions: Extensions { extensions_02x: Extensions, extensions_1x: Extensions } } }), bucket=fts, prefix=tici_default_prefix/cdc/fts/wiki_abstract, start_key=tici_default_prefix/cdc/fts/wiki_abstract/462934195599835137/2025-12-17/CDC00000000000000000065.json


-- tici_searchlib.log 在 tiflash/log 日志目录外层，这和日志收集工具是否兼容，日志管理问题（rotate ？）

-- prepare 有问题
mysql> prepare stmt from 'select * from wiki_abstract WHERE fts_match_word(?, abstract)';
ERROR 1235 (42000): This version of TiDB doesn't yet support 'match against a non-constant string'

