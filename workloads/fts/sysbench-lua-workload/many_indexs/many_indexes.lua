#!/usr/bin/env sysbench

if sysbench.cmdline.command == nil then
  error("Command is required. Supported commands: prepare, add_index, drop_index, run, verify, cleanup, help")
end

local TABLE_ID_WIDTH = 3

sysbench.cmdline.options = {
  tables = {"Number of tables", 100},
  table_rows = {"Number of rows per table during prepare", 10000},
  total_rows = {"Optional total rows across all tables; when > 0 it overrides table_rows", 0},
  table_name_prefix = {"Table name prefix", "fts_many_t"},

  text_cols = {"Number of TEXT columns per table", 8},
  single_fts_indexes = {"Number of single-column FTS indexes per table", 3},
  multi_fts_indexes = {"Number of multi-column FTS indexes per table", 2},
  multi_fts_index_cols = {"Number of columns in each multi-column FTS index", 2},
  index_name_prefix = {"FTS index name prefix", "idx_fts"},
  parser = {"FTS parser: standard or ngram", "standard"},

  dict_size = {"Dictionary size", "10000"},
  min_words = {"Minimum number of words in each text column", 3},
  max_words = {"Maximum number of words in each text column", 20},

  selects_per_event = {"Number of SELECT operations per sysbench event", 1},
  inserts_per_event = {"Number of INSERT operations per sysbench event", 1},
  updates_per_event = {"Number of UPDATE operations per sysbench event", 1},
  deletes_per_event = {"Number of DELETE operations per sysbench event", 1},
  delete_limit = {"Rows to delete per DELETE operation", 1},

  select_mode = {"SELECT SQL shape: count, latest, columns, mixed", "mixed"},
  query_mode = {"FTS query mode for run SELECT: word, prefix, phrase, boolean, mixed", "mixed"},
  select_limit = {"LIMIT for run SELECT queries", 100},
  verify_queries = {"Verification queries per table/index column group", 3},
  verify_mode = {"Verification query mode: word, prefix, phrase, boolean, mixed", "mixed"},
  verify_print_results = {"Print every verify query count result: on/off", "on"}
}

local dictionary = nil
local runtime_ts_seq = 0
local con = nil

local function validate_options()
  assert(sysbench.opt.tables > 0, "--tables must be > 0")
  assert(sysbench.opt.table_rows >= 0, "--table_rows must be >= 0")
  assert(sysbench.opt.total_rows >= 0, "--total_rows must be >= 0")
  assert(sysbench.opt.text_cols >= 1, "--text_cols must be >= 1")
  assert(sysbench.opt.single_fts_indexes >= 0, "--single_fts_indexes must be >= 0")
  assert(sysbench.opt.multi_fts_indexes >= 0, "--multi_fts_indexes must be >= 0")
  assert(sysbench.opt.multi_fts_index_cols >= 2, "--multi_fts_index_cols must be >= 2")
  assert(sysbench.opt.multi_fts_index_cols <= sysbench.opt.text_cols,
    "--multi_fts_index_cols must be <= --text_cols")
  assert(sysbench.opt.single_fts_indexes + sysbench.opt.multi_fts_indexes > 0,
    "At least one FTS index per table is required")
  sysbench.opt.dict_size = tonumber(sysbench.opt.dict_size)
  assert(sysbench.opt.dict_size > 0, "--dict_size must be > 0")
  assert(sysbench.opt.min_words > 0, "--min_words must be > 0")
  assert(sysbench.opt.max_words >= sysbench.opt.min_words, "--max_words must be >= --min_words")
  assert(sysbench.opt.selects_per_event >= 0, "--selects_per_event must be >= 0")
  assert(sysbench.opt.inserts_per_event >= 0, "--inserts_per_event must be >= 0")
  assert(sysbench.opt.updates_per_event >= 0, "--updates_per_event must be >= 0")
  assert(sysbench.opt.deletes_per_event >= 0, "--deletes_per_event must be >= 0")
  assert(sysbench.opt.delete_limit >= 0, "--delete_limit must be >= 0")
  assert(sysbench.opt.select_limit >= 0, "--select_limit must be >= 0")
  assert(sysbench.opt.verify_queries >= 0, "--verify_queries must be >= 0")

  local parser = string.lower(tostring(sysbench.opt.parser or "standard"))
  assert(parser == "standard" or parser == "ngram", "--parser must be standard or ngram")
  sysbench.opt.parser = parser
end

local function option_enabled(value)
  value = string.lower(tostring(value or ""))
  return value == "on" or value == "true" or value == "1" or value == "yes"
end

local function dictionary_word_width()
  local width = 1
  local capacity = 26

  while capacity < sysbench.opt.dict_size do
    width = width + 1
    capacity = capacity * 26
  end

  if width < 4 then
    return 4
  end

  return width
end

local function build_word(i)
  local value = i - 1
  local chars = {}
  local width = dictionary_word_width()

  for pos = width, 1, -1 do
    chars[pos] = string.char(97 + (value % 26))
    value = math.floor(value / 26)
  end

  return "ftstok" .. table.concat(chars) .. "end"
end

local function ensure_dictionary()
  if dictionary ~= nil then
    return
  end

  dictionary = {}
  for i = 1, sysbench.opt.dict_size do
    dictionary[i] = build_word(i)
  end
end

local function quote_ident(name)
  local escaped = tostring(name):gsub("`", "``")
  return "`" .. escaped .. "`"
end

local function quote_literal(value)
  local s = tostring(value)
  s = s:gsub("\\", "\\\\")
  s = s:gsub("'", "''")
  return "'" .. s .. "'"
end

local function format_ts(epoch, micros)
  return os.date("%Y-%m-%d %H:%M:%S", epoch) .. string.format(".%06d", micros % 1000000)
end

local function prepare_timestamp(table_num, row_num)
  local epoch = 1704067200 + table_num * 100000 + row_num
  local micros = table_num * 1000 + row_num
  return format_ts(epoch, micros)
end

local function runtime_timestamp()
  runtime_ts_seq = runtime_ts_seq + 1
  local epoch = os.time() + (sysbench.tid or 0) * 1000 + runtime_ts_seq
  local micros = ((sysbench.tid or 0) * 10000 + runtime_ts_seq) % 1000000
  return format_ts(epoch, micros)
end

local function rows_for_table(table_num)
  if sysbench.opt.total_rows == 0 then
    return sysbench.opt.table_rows
  end

  local base = math.floor(sysbench.opt.total_rows / sysbench.opt.tables)
  local extra = sysbench.opt.total_rows % sysbench.opt.tables
  if table_num <= extra then
    return base + 1
  end

  return base
end

local function id_width()
  local width = TABLE_ID_WIDTH
  local table_digits = #tostring(sysbench.opt.tables)
  if table_digits > width then
    return table_digits
  end
  return width
end

local function seq_width()
  local max_seq = math.max(sysbench.opt.single_fts_indexes, sysbench.opt.multi_fts_indexes, sysbench.opt.text_cols)
  local width = #tostring(max_seq)
  if width < 3 then
    return 3
  end
  return width
end

local function padded_id(n)
  return string.format("%0" .. id_width() .. "d", n)
end

local function padded_seq(n)
  return string.format("%0" .. seq_width() .. "d", n)
end

local function table_name(table_num)
  return string.format("%s%s", sysbench.opt.table_name_prefix, padded_id(table_num))
end

local function text_col_name(col_num)
  return string.format("text_%s", padded_seq(col_num))
end

local function single_index_name(table_num, index_num)
  return string.format("%s_s_%s_%s", sysbench.opt.index_name_prefix, padded_id(table_num), padded_seq(index_num))
end

local function multi_index_name(table_num, index_num)
  return string.format("%s_m_%s_%s", sysbench.opt.index_name_prefix, padded_id(table_num), padded_seq(index_num))
end

local function single_index_cols(index_num)
  local col_num = ((index_num - 1) % sysbench.opt.text_cols) + 1
  return {text_col_name(col_num)}
end

local function combination_count(n, k)
  if k < 0 or k > n then
    return 0
  end
  if k > n - k then
    k = n - k
  end

  local result = 1
  for i = 1, k do
    result = result * (n - k + i) / i
    if result > 1000000000 then
      return result
    end
  end

  return math.floor(result + 0.5)
end

local function combination_by_rank(n, k, rank)
  local cols = {}
  local start = 1

  for pos = 1, k do
    for candidate = start, n - (k - pos) do
      local count = combination_count(n - candidate, k - pos)
      if rank < count then
        cols[#cols + 1] = candidate
        start = candidate + 1
        break
      end
      rank = rank - count
    end
  end

  return cols
end

local function multi_index_cols(index_num)
  local cols = {}
  local total = combination_count(sysbench.opt.text_cols, sysbench.opt.multi_fts_index_cols)
  local rank = (index_num - 1) % total
  local col_nums = combination_by_rank(sysbench.opt.text_cols, sysbench.opt.multi_fts_index_cols, rank)

  for i, col_num in ipairs(col_nums) do
    cols[i] = text_col_name(col_num)
  end

  return cols
end

local function quote_columns(cols)
  local quoted = {}
  for i, col in ipairs(cols) do
    quoted[i] = quote_ident(col)
  end
  return table.concat(quoted, ", ")
end

local function parser_clause()
  return " WITH PARSER " .. string.upper(sysbench.opt.parser)
end

local function random_table_num()
  return sysbench.rand.uniform(1, sysbench.opt.tables)
end

local function random_dict_word()
  return dictionary[sysbench.rand.uniform(1, sysbench.opt.dict_size)]
end

local function build_body()
  local word_count = sysbench.rand.uniform(sysbench.opt.min_words, sysbench.opt.max_words)
  local words = {}

  for i = 1, word_count do
    words[i] = random_dict_word()
  end

  return table.concat(words, " ")
end

local function build_text_values()
  local values = {}
  for i = 1, sysbench.opt.text_cols do
    values[i] = build_body()
  end
  return values
end

local function create_table(conn, table_num)
  local cols = {
    "id BIGINT NOT NULL AUTO_INCREMENT",
    "ts TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)"
  }

  for i = 1, sysbench.opt.text_cols do
    cols[#cols + 1] = string.format("%s TEXT NOT NULL", quote_ident(text_col_name(i)))
  end
  cols[#cols + 1] = "PRIMARY KEY (id)"

  local sql = string.format("CREATE TABLE IF NOT EXISTS %s (%s)",
    quote_ident(table_name(table_num)),
    table.concat(cols, ", "))
  conn:query(sql)
end

local function fill_table(conn, table_num)
  local rows = rows_for_table(table_num)
  if rows <= 0 then
    return
  end

  local cols = {quote_ident("ts")}
  for i = 1, sysbench.opt.text_cols do
    cols[#cols + 1] = quote_ident(text_col_name(i))
  end

  conn:bulk_insert_init(string.format("INSERT INTO %s (%s) VALUES",
    quote_ident(table_name(table_num)),
    table.concat(cols, ", ")))

  for row_num = 1, rows do
    local values = build_text_values()
    local quoted = {quote_literal(prepare_timestamp(table_num, row_num))}
    for i, value in ipairs(values) do
      quoted[#quoted + 1] = quote_literal(value)
    end
    conn:bulk_insert_next("(" .. table.concat(quoted, ", ") .. ")")
  end

  conn:bulk_insert_done()
end

local function create_index_sql(table_num, index_name, cols)
  return string.format("CREATE FULLTEXT INDEX IF NOT EXISTS %s ON %s (%s)%s",
    quote_ident(index_name),
    quote_ident(table_name(table_num)),
    quote_columns(cols),
    parser_clause())
end

local function drop_index_sql(table_num, index_name)
  return string.format("DROP INDEX IF EXISTS %s ON %s",
    quote_ident(index_name),
    quote_ident(table_name(table_num)))
end

local function for_each_assigned_table(fn)
  for table_num = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables, sysbench.opt.threads do
    fn(table_num)
  end
end

local function for_each_table_index(table_num, fn)
  for i = 1, sysbench.opt.single_fts_indexes do
    fn(single_index_name(table_num, i), single_index_cols(i), "single", i)
  end
  for i = 1, sysbench.opt.multi_fts_indexes do
    fn(multi_index_name(table_num, i), multi_index_cols(i), "multi", i)
  end
end

local function for_each_assigned_table_index(fn)
  local work_idx = 0
  local tid = sysbench.tid or 0
  local threads = sysbench.opt.threads or 1

  for table_num = 1, sysbench.opt.tables do
    for_each_table_index(table_num, function(index_name, cols, index_type, index_seq)
      if work_idx % threads == tid then
        fn(table_num, index_name, cols, index_type, index_seq)
      end
      work_idx = work_idx + 1
    end)
  end
end

local function random_index_cols()
  local single_count = sysbench.opt.single_fts_indexes
  local multi_count = sysbench.opt.multi_fts_indexes
  if multi_count == 0 or (single_count > 0 and sysbench.rand.uniform(1, single_count + multi_count) <= single_count) then
    return single_index_cols(sysbench.rand.uniform(1, single_count))
  end
  return multi_index_cols(sysbench.rand.uniform(1, multi_count))
end

local function normalize_mode(mode)
  mode = string.lower(tostring(mode or "mixed"))
  if mode == "mixed" then
    local modes = {"word", "prefix", "phrase", "boolean"}
    return modes[sysbench.rand.uniform(1, #modes)]
  end
  if mode ~= "word" and mode ~= "prefix" and mode ~= "phrase" and mode ~= "boolean" then
    error("Unsupported query mode: " .. tostring(mode))
  end
  return mode
end

local function normalize_select_mode(mode)
  mode = string.lower(tostring(mode or "mixed"))
  if mode == "mixed" then
    local modes = {"count", "latest", "columns"}
    return modes[sysbench.rand.uniform(1, #modes)]
  end
  if mode ~= "count" and mode ~= "latest" and mode ~= "columns" then
    error("Unsupported select mode: " .. tostring(mode))
  end
  return mode
end

local function build_query_terms(mode)
  if mode == "word" then
    local word = random_dict_word()
    return mode, word, word, nil
  end

  if mode == "prefix" then
    local word = random_dict_word()
    local prefix_len = math.max(3, #word - 1)
    local prefix = string.sub(word, 1, prefix_len)
    return mode, prefix .. "*", prefix, nil
  end

  if mode == "phrase" then
    local phrase = random_dict_word() .. " " .. random_dict_word()
    return mode, "\"" .. phrase .. "\"", phrase, nil
  end

  local positive = random_dict_word()
  local negative = random_dict_word()
  while negative == positive do
    negative = random_dict_word()
  end
  return mode, "+" .. positive .. " -" .. negative, positive, negative
end

local function like_any_condition(cols, pattern)
  local parts = {}
  for _, col in ipairs(cols) do
    parts[#parts + 1] = string.format("%s LIKE %s", quote_ident(col), quote_literal("%" .. pattern .. "%"))
  end
  return "(" .. table.concat(parts, " OR ") .. ")"
end

local function like_none_condition(cols, pattern)
  local parts = {}
  for _, col in ipairs(cols) do
    parts[#parts + 1] = string.format("%s NOT LIKE %s", quote_ident(col), quote_literal("%" .. pattern .. "%"))
  end
  return "(" .. table.concat(parts, " AND ") .. ")"
end

local function fts_condition(cols, fts_query)
  return string.format("MATCH (%s) AGAINST (%s IN BOOLEAN MODE)",
    quote_columns(cols),
    quote_literal(fts_query))
end

local function like_condition_for_mode(mode, cols, positive, negative)
  if mode == "boolean" then
    return like_any_condition(cols, positive) .. " AND " .. like_none_condition(cols, negative)
  end
  return like_any_condition(cols, positive)
end

local function execute_select(conn)
  local table_num = random_table_num()
  local cols = random_index_cols()
  local mode, fts_query = build_query_terms(normalize_mode(sysbench.opt.query_mode))
  local table_ident = quote_ident(table_name(table_num))
  local condition = fts_condition(cols, fts_query)
  local select_mode = normalize_select_mode(sysbench.opt.select_mode)
  local sql

  if select_mode == "count" then
    sql = string.format("SELECT count(*) FROM %s WHERE %s",
      table_ident,
      condition)
  elseif select_mode == "latest" then
    sql = string.format("SELECT id, ts FROM %s WHERE %s ORDER BY ts DESC LIMIT %d",
      table_ident,
      condition,
      sysbench.opt.select_limit)
  else
    sql = string.format("SELECT %s FROM %s WHERE %s LIMIT %d",
      quote_columns(cols),
      table_ident,
      condition,
      sysbench.opt.select_limit)
  end

  conn:query(sql)
end

local function execute_insert(conn)
  local table_num = random_table_num()
  local cols = {quote_ident("ts")}
  for i = 1, sysbench.opt.text_cols do
    cols[#cols + 1] = quote_ident(text_col_name(i))
  end

  local values = build_text_values()
  local quoted = {quote_literal(runtime_timestamp())}
  for i, value in ipairs(values) do
    quoted[#quoted + 1] = quote_literal(value)
  end

  conn:query(string.format("INSERT INTO %s (%s) VALUES (%s)",
    quote_ident(table_name(table_num)),
    table.concat(cols, ", "),
    table.concat(quoted, ", ")))
end

local function random_existing_id(table_num)
  local upper = rows_for_table(table_num)
  if upper < 1 then
    upper = 1
  end
  return sysbench.rand.uniform(1, upper)
end

local function execute_update(conn)
  local table_num = random_table_num()
  local col_num = sysbench.rand.uniform(1, sysbench.opt.text_cols)
  local sql = string.format("UPDATE %s SET ts=%s, %s=%s WHERE id=%d",
    quote_ident(table_name(table_num)),
    quote_literal(runtime_timestamp()),
    quote_ident(text_col_name(col_num)),
    quote_literal(build_body()),
    random_existing_id(table_num))
  conn:query(sql)
end

local function execute_delete(conn)
  if sysbench.opt.delete_limit <= 0 then
    return
  end
  local table_num = random_table_num()
  local sql = string.format("DELETE FROM %s WHERE id >= %d LIMIT %d",
    quote_ident(table_name(table_num)),
    random_existing_id(table_num),
    sysbench.opt.delete_limit)
  conn:query(sql)
end

local function query_count(conn, sql)
  local rs = conn:query(sql)
  if rs == nil then
    error("Query returned nil result: " .. sql)
  end
  local row = rs:fetch_row()
  if row == nil then
    error("Query returned no rows: " .. sql)
  end
  return tonumber(row[1])
end

local function query_id_ts_rows(conn, sql)
  local rs = conn:query(sql)
  if rs == nil then
    error("Query returned nil result: " .. sql)
  end

  local rows = {}
  local row_count = 0

  while true do
    local row = rs:fetch_row()
    if row == nil then
      break
    end

    row_count = row_count + 1
    rows[tostring(row[1]) .. "\t" .. tostring(row[2])] = true
  end

  return row_count, rows
end

local function compare_id_ts_rows(left_rows, right_rows)
  for key in pairs(left_rows) do
    if not right_rows[key] then
      return false, key
    end
  end
  for key in pairs(right_rows) do
    if not left_rows[key] then
      return false, key
    end
  end
  return true, nil
end

local function verify_one(conn, table_num, index_name, cols)
  local mode, fts_query, positive, negative = build_query_terms(normalize_mode(sysbench.opt.verify_mode))

  local table_ident = quote_ident(table_name(table_num))
  local fts_where = fts_condition(cols, fts_query)
  local like_where = like_condition_for_mode(mode, cols, positive, negative)
  local fts_count_sql = string.format("SELECT COUNT(*) FROM %s WHERE %s",
    table_ident,
    fts_where)
  local like_count_sql = string.format("SELECT COUNT(*) FROM %s WHERE %s",
    table_ident,
    like_where)

  local fts_count = query_count(conn, fts_count_sql)
  local like_count = query_count(conn, like_count_sql)

  if fts_count ~= like_count then
    error(string.format(
      "FTS/LIKE count mismatch: table=%s index=%s mode=%s cols=%s fts_count=%s like_count=%s fts_sql=%s like_sql=%s",
      table_name(table_num),
      index_name,
      mode,
      table.concat(cols, ","),
      tostring(fts_count),
      tostring(like_count),
      fts_count_sql,
      like_count_sql))
  end

  if fts_count == 0 then
    if option_enabled(sysbench.opt.verify_print_results) then
      print(string.format(
        "verify table=%s index=%s mode=%s cols=%s fts_count=%s like_count=%s fts_rows=0 like_rows=0",
        table_name(table_num),
        index_name,
        mode,
        table.concat(cols, ","),
        tostring(fts_count),
        tostring(like_count)))
    end
    return
  end

  local fts_rows_sql = string.format("SELECT id, ts FROM %s WHERE %s",
    table_ident,
    fts_where)
  local like_rows_sql = string.format("SELECT id, ts FROM %s WHERE %s",
    table_ident,
    like_where)
  local fts_rows_count, fts_rows = query_id_ts_rows(conn, fts_rows_sql)
  local like_rows_count, like_rows = query_id_ts_rows(conn, like_rows_sql)

  if option_enabled(sysbench.opt.verify_print_results) then
    print(string.format(
      "verify table=%s index=%s mode=%s cols=%s fts_count=%s like_count=%s fts_rows=%s like_rows=%s",
      table_name(table_num),
      index_name,
      mode,
      table.concat(cols, ","),
      tostring(fts_count),
      tostring(like_count),
      tostring(fts_rows_count),
      tostring(like_rows_count)))
  end

  if fts_rows_count ~= like_rows_count then
    error(string.format(
      "FTS/LIKE row count mismatch: table=%s index=%s mode=%s cols=%s fts_rows=%s like_rows=%s fts_sql=%s like_sql=%s",
      table_name(table_num),
      index_name,
      mode,
      table.concat(cols, ","),
      tostring(fts_rows_count),
      tostring(like_rows_count),
      fts_rows_sql,
      like_rows_sql))
  end

  local rows_match, diff_key = compare_id_ts_rows(fts_rows, like_rows)
  if not rows_match then
    error(string.format(
      "FTS/LIKE row set mismatch: table=%s index=%s mode=%s cols=%s diff_key=%s fts_sql=%s like_sql=%s",
      table_name(table_num),
      index_name,
      mode,
      table.concat(cols, ","),
      tostring(diff_key),
      fts_rows_sql,
      like_rows_sql))
  end
end

function cmd_prepare()
  validate_options()
  ensure_dictionary()

  local drv = sysbench.sql.driver()
  local conn = drv:connect()

  for_each_assigned_table(function(table_num)
    print(string.format("Preparing table %s", table_name(table_num)))
    conn:query(string.format("DROP TABLE IF EXISTS %s", quote_ident(table_name(table_num))))
    create_table(conn, table_num)
    fill_table(conn, table_num)
  end)

  conn:disconnect()
end

function cmd_add_index()
  validate_options()

  local drv = sysbench.sql.driver()
  local conn = drv:connect()

  for_each_assigned_table(function(table_num)
    for_each_table_index(table_num, function(index_name, cols)
      print(string.format("Adding index %s on %s(%s)", index_name, table_name(table_num), table.concat(cols, ",")))
      conn:query(create_index_sql(table_num, index_name, cols))
    end)
  end)

  conn:disconnect()
end

function cmd_drop_index()
  validate_options()

  local drv = sysbench.sql.driver()
  local conn = drv:connect()

  for_each_assigned_table(function(table_num)
    for_each_table_index(table_num, function(index_name)
      print(string.format("Dropping index %s on %s", index_name, table_name(table_num)))
      conn:query(drop_index_sql(table_num, index_name))
    end)
  end)

  conn:disconnect()
end

function thread_init()
  validate_options()
  ensure_dictionary()

  local drv = sysbench.sql.driver()
  con = drv:connect()
end

function thread_done()
  if con ~= nil then
    con:disconnect()
    con = nil
  end
end

function event()
  for _ = 1, sysbench.opt.selects_per_event do
    execute_select(con)
  end
  for _ = 1, sysbench.opt.inserts_per_event do
    execute_insert(con)
  end
  for _ = 1, sysbench.opt.updates_per_event do
    execute_update(con)
  end
  for _ = 1, sysbench.opt.deletes_per_event do
    execute_delete(con)
  end
end

function cmd_verify()
  validate_options()
  ensure_dictionary()

  local drv = sysbench.sql.driver()
  local conn = drv:connect()
  local checked_queries = 0
  local checked_indexes = 0

  for_each_assigned_table_index(function(table_num, index_name, cols)
    checked_indexes = checked_indexes + 1
    for _ = 1, sysbench.opt.verify_queries do
      verify_one(conn, table_num, index_name, cols)
      checked_queries = checked_queries + 1
    end
  end)

  print(string.format("Thread %d verified %d table/index groups and %d queries",
    sysbench.tid,
    checked_indexes,
    checked_queries))
  conn:disconnect()
end

function cleanup()
  validate_options()

  local drv = sysbench.sql.driver()
  local conn = drv:connect()

  for_each_assigned_table(function(table_num)
    print(string.format("Dropping table %s", table_name(table_num)))
    conn:query(string.format("DROP TABLE IF EXISTS %s", quote_ident(table_name(table_num))))
  end)

  conn:disconnect()
end

sysbench.cmdline.commands = {
  prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND},
  add_index = {cmd_add_index, sysbench.cmdline.PARALLEL_COMMAND},
  drop_index = {cmd_drop_index, sysbench.cmdline.PARALLEL_COMMAND},
  verify = {cmd_verify, sysbench.cmdline.PARALLEL_COMMAND},
  cleanup = {cleanup, sysbench.cmdline.PARALLEL_COMMAND}
}
