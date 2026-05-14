# FTS Many Indexes Workload

This workload is used for FTS 1000-index scalability testing.

It supports:

- `prepare`: create tables and load base rows.
- `add_index`: create generated FTS indexes with `IF NOT EXISTS`.
- `drop_index`: drop generated FTS indexes with `IF EXISTS`.
- `run`: run mixed INSERT / UPDATE / SELECT / DELETE workload through sysbench `event()`.
- `verify`: compare FTS query counts and `id, ts` row results with LIKE-equivalent query results.
- `cleanup`: drop generated tables.

`prepare`, `add_index`, and `drop_index` are parallel sysbench commands. `run` uses standard sysbench `--events` and `--time` controls.

## Naming Rules

Table name:

```text
<table_name_prefix><zero-padded table id>
```

Example:

```text
fts_many_t001
fts_many_t002
```

Single-column FTS index name:

```text
<index_name_prefix>_s_<table id>_<index seq>
```

Multi-column FTS index name:

```text
<index_name_prefix>_m_<table id>_<index seq>
```

Text columns are generated as:

```text
text_001, text_002, ...
```

## Common Options

- `--tables`: number of tables.
- `--table_rows`: rows per table during `prepare`.
- `--total_rows`: total rows across all tables; overrides `--table_rows` when greater than 0.
- `--text_cols`: number of text columns per table.
- `--single_fts_indexes`: number of single-column FTS indexes per table.
- `--multi_fts_indexes`: number of multi-column FTS indexes per table.
- `--multi_fts_index_cols`: number of columns in each multi-column FTS index.
- `--parser`: `standard` or `ngram`.
- `--select_mode`: `count`, `latest`, `columns`, or `mixed`.
- `--query_mode`: `word`, `prefix`, `phrase`, `boolean`, or `mixed`.
- `--verify_mode`: `word`, `prefix`, `phrase`, `boolean`, or `mixed`.
- `--verify_queries`: number of verification queries for every table/index column group.
- `--verify_print_results`: print every verify query result, `on` or `off`.

## FTS Column Selection

Single-column index `N` uses:

```text
text_((N - 1) % text_cols + 1)
```

Multi-column index `N` uses the `N`th lexicographic combination of `multi_fts_index_cols` columns from all `text_cols` columns. For example, `--text_cols=10 --multi_fts_index_cols=2 --multi_fts_indexes=20` is supported and generates 20 different 2-column combinations out of 45 possible combinations.

If the requested index count exceeds the available unique column groups, the mapping wraps and duplicate column groups are generated. For unique multi-column indexes, keep:

```text
multi_fts_indexes <= C(text_cols, multi_fts_index_cols)
```

`run` SELECT chooses one generated FTS index column group randomly. The probability follows the configured index counts, so single-column and multi-column indexes are selected in proportion to `single_fts_indexes` and `multi_fts_indexes`.

`verify` does not use random table/index selection. It traverses every table and every generated FTS index column group. With multiple sysbench threads, table/index groups are split across threads.

## SELECT Modes

`run` supports three FTS SELECT shapes through `--select_mode`:

```sql
SELECT count(*) FROM `<table>` WHERE MATCH(<fts_columns>) AGAINST ('<query>' IN BOOLEAN MODE)
```

```sql
SELECT id, ts FROM `<table>` WHERE MATCH(<fts_columns>) AGAINST ('<query>' IN BOOLEAN MODE) ORDER BY ts DESC LIMIT <select_limit>
```

```sql
SELECT <fts_columns> FROM `<table>` WHERE MATCH(<fts_columns>) AGAINST ('<query>' IN BOOLEAN MODE) LIMIT <select_limit>
```

`--select_mode=mixed` randomly chooses one of the three shapes for every SELECT operation.

## Single Table 500-Index Model

```sh
sysbench --threads=8 \
  --mysql-host=127.0.0.1 --mysql-port=4000 \
  --mysql-user=root --mysql-db=test \
  --tables=1 \
  --table_name_prefix=fts_many_single \
  --text_cols=250 \
  --single_fts_indexes=250 \
  --multi_fts_indexes=250 \
  --multi_fts_index_cols=2 \
  many_indexes.lua prepare

sysbench --threads=8 \
  --mysql-host=127.0.0.1 --mysql-port=4000 \
  --mysql-user=root --mysql-db=test \
  --tables=1 \
  --table_name_prefix=fts_many_single \
  --text_cols=250 \
  --single_fts_indexes=250 \
  --multi_fts_indexes=250 \
  --multi_fts_index_cols=2 \
  many_indexes.lua add_index
```

## Multi-Table 500-Index Model

```sh
sysbench --threads=16 \
  --mysql-host=127.0.0.1 --mysql-port=4000 \
  --mysql-user=root --mysql-db=test \
  --tables=100 \
  --table_name_prefix=fts_many_t \
  --text_cols=8 \
  --single_fts_indexes=3 \
  --multi_fts_indexes=2 \
  --multi_fts_index_cols=2 \
  many_indexes.lua prepare

sysbench --threads=16 \
  --mysql-host=127.0.0.1 --mysql-port=4000 \
  --mysql-user=root --mysql-db=test \
  --tables=100 \
  --table_name_prefix=fts_many_t \
  --text_cols=8 \
  --single_fts_indexes=3 \
  --multi_fts_indexes=2 \
  --multi_fts_index_cols=2 \
  many_indexes.lua add_index
```

The two models together provide 1000 FTS indexes.

## Run Workload

```sh
sysbench --threads=64 \
  --mysql-host=127.0.0.1 --mysql-port=4000 \
  --mysql-user=root --mysql-db=test \
  --tables=100 \
  --table_name_prefix=fts_many_t \
  --text_cols=8 \
  --single_fts_indexes=3 \
  --multi_fts_indexes=2 \
  --multi_fts_index_cols=2 \
  --selects_per_event=5 \
  --select_mode=mixed \
  --inserts_per_event=1 \
  --updates_per_event=1 \
  --deletes_per_event=1 \
  --time=600 \
  many_indexes.lua run
```

## Verification

`verify` traverses every table and every generated FTS index column group. For each table/index group, it runs `--verify_queries` verification queries.

Each verification first compares FTS `COUNT(*)` with LIKE-equivalent `COUNT(*)`, then compares `SELECT id, ts FROM <table> WHERE ...` row results to cover lookup/back-table query behavior. It prints the table, index name, columns, count results, and returned row counts when `--verify_print_results=on`.

Generated words use fixed-length alphabetic tokens to avoid substring false positives in `LIKE '%word%'` checks.

```sh
sysbench --threads=8 \
  --mysql-host=127.0.0.1 --mysql-port=4000 \
  --mysql-user=root --mysql-db=test \
  --tables=100 \
  --table_name_prefix=fts_many_t \
  --text_cols=8 \
  --single_fts_indexes=3 \
  --multi_fts_indexes=2 \
  --multi_fts_index_cols=2 \
  --verify_queries=3 \
  --verify_mode=word \
  --verify_print_results=off \
  many_indexes.lua verify
```

`word` and `phrase` are recommended for strict FTS-vs-LIKE consistency checks. `prefix` and `boolean` are also supported, but they should be used with generated data semantics in mind.
