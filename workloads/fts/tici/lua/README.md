# Full-Text Search (FTS) workload

Lua workloads for exercising TiDB/MySQL full-text search functions (`fts_match_word`, `fts_match_prefix`, `fts_match_phrase`). Workloads are read-only (`oltp_read_only.lua`) and write-heavy (`oltp_write_only.lua`).

## Prerequisites
- sysbench built with MySQL/TiDB driver in PATH.
- Target database already has tables and FTS indexes for one of the supported datasets: `wiki_abstract`, `wiki_page`, or `amazon_review`.

## Common options
- `--workload`              : workload template to use: `wiki_abstract` (default) | `wiki_page` | `amazon_review`.
- `--table_name`            : actual SQL table name to replace in generated statements. Suggested values are `wiki_abstract` (default), `wiki_page`, and `amazon_review`, but other strings are also allowed.
- `--table_name` should point to a table whose schema matches the selected `--workload`.
- `--ret_little_rows`       : choose rarer words/phrases (true) or common ones (false).
- `--proj_type`             : `all` (default, uses `SELECT *`) or `count` (uses `SELECT count(*)`) to switch projection.
- FTS phrase/word mixes (each count runs both AND + OR variants per iteration):
  - `--phrase_matchs`, `--two_phrases_conj_matchs`
  - `--phrase_word_conj_matchs`, `--phrase_prefix_conj_matchs`
  - `--two_words_conj_matchs`, `--two_fields_word_conj_matchs`
  - `--one_word_prefix_matchs`, `--two_words_prefix_conj_matchs`, `--two_fields_word_prefix_conj_matchs`
  - `--word_prefix_conj_matchs`
- Data source controls (writes): `--source_files`, `--source_file_dir`, `--insert_ratio`, `--update_ratio`, `--delete_ratio`, `--delete_limit`, `--update_random_ids`, `--auto_inc`.
- Update ID sampling: sampled IDs are divided into per-thread ranges to reduce collisions; `--update_rand_ids` toggles `ORDER BY RAND()` when loading IDs (off by default).
- Transaction controls: `--skip_trx`, `--reconnect`.
- Supply standard MySQL connection flags (e.g., `--mysql-host`, `--mysql-port`, `--mysql-user`, `--mysql-password`, `--mysql-db`).

## Run read-only FTS
Example: wiki abstract workload with phrases, two-phrase and word-prefix mixes:
```sh
sysbench \
  --workload=wiki_abstract \
  --table_name=wiki_abstract \
  --phrase_matchs=2 \
  --two_phrases_conj_matchs=1 \
  --word_prefix_conj_matchs=1 \
  --mysql-host=127.0.0.1 --mysql-port=4000 \
  --mysql-user=root --mysql-password= \
  oltp_read_only.lua run
```

## Run write-only (load/update/delete)
Provide CSV inputs under `--source_file_dir` named like `fts.<workload>.<n>.csv` where `n` ranges to `--source_files`.
```sh
sysbench \
  --workload=wiki_abstract \
  --table_name=wiki_abstract \
  --source_file_dir=/path/to/csvs \
  --source_files=4 \
  --insert_ratio=5 --update_ratio=4 --delete_ratio=1 \
  --mysql-host=127.0.0.1 --mysql-port=4000 \
  --mysql-user=root --mysql-password= \
  oltp_write_only.lua run
```
On init, `oltp_write_only.lua` samples IDs into `<table_name>.ids.txt` for updates; keep this file alongside the script or regenerate by re-running init.
