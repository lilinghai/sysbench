#!/usr/bin/env sysbench

local csv = require("csv")
function init()
    assert(event ~= nil,
        "this script is meant to be included by other lua scripts and " .. "should not be called directly.")
end

if sysbench.cmdline.command == nil then
    error("Command is required. Supported commands: run, help")
end

-- Command line options
sysbench.cmdline.options = {
    table_name = {"Actual SQL table name. Suggested values: wiki_abstract, wiki_page, amazon_review", "wiki_abstract"},
    workload = {"Using one of the workloads [wiki_abstract,wiki_page,amazon_review]", "wiki_abstract"},
    ret_little_rows = {"return little rows(less than 1000) for fts query", true},
    proj_type = {"Projection for SELECT queries: 'all' (default, same as '*') or 'count'", "all"},
    syntax = {"Query syntax: 'old' (fts_match_word) or 'new' (MATCH ... AGAINST)", "old"},
    parser = {"Text parser: 'standard' (default) or 'ngram'", "standard"},
    ngram = {"N-gram length when parser is 'ngram'", 2},

    one_word_matchs = {"Number of one word match SELECT queries per transaction", 5},
    phrase_matchs = {"Number of phrase match SELECT queries per transaction", 5},
    one_word_prefix_matchs = {"Number of one word prefix match SELECT queries per transaction", 5},

    two_words_conj_matchs = {"Number of two words match SELECT queries per transaction", 1},
    two_phrases_conj_matchs = {"Number of two phrases match SELECT queries per transaction", 1},
    two_words_prefix_conj_matchs = {"Number of two words prefix match SELECT queries per transaction", 1},

    phrase_word_conj_matchs = {"Number of phrase and word match SELECT queries per transaction", 1},
    phrase_prefix_conj_matchs = {"Number of phrase and prefix match SELECT queries per transaction", 1},
    word_prefix_conj_matchs = {"Number of prefix+word mix match SELECT queries per transaction", 1},

    two_fields_word_conj_matchs = {"Number of two fields word match SELECT queries per transaction", 0},
    two_fields_word_prefix_conj_matchs = {"Number of two fields prefix match SELECT queries per transaction", 0},

    source_files = {"Source csv files for insert", 1},
    source_file_dir = {"Directory containing source csv files", "."},
    insert_ratio = {"How many INSERT operations to run per input row", 5},
    update_ratio = {"How many UPDATE operations to run per input row", 4},
    delete_ratio = {"How many DELETE operations to run per input row", 1},
    delete_limit = {"How many rows to delete per DELETE operation", 1},
    update_random_ids = {"Number of random IDs to load for update operations", 1000000},
    update_rand_ids = {"Use ORDER BY RAND() when sampling update IDs (default: off)", false},

    auto_inc = {"Use AUTO_INCREMENT column as Primary Key (for MySQL), " ..
        "or its alternatives in other DBMS. When disabled, use " .. "client-generated IDs", true},
    skip_trx = {"Don't start explicit transactions and execute all queries " .. "in the AUTOCOMMIT mode", false},
    reconnect = {"Reconnect after every N events. The default (0) is to not reconnect", 0}
}

local cached_projection
local cached_syntax
local cached_parser
local cached_ngram
local cached_workload
local cached_table_name
local ngram_cache = {}
local thread_update_ids

local supported_workloads = {
    wiki_abstract = true,
    wiki_page = true,
    amazon_review = true
}

local function parse_workload(value)
    if value == nil then
        return nil
    end

    local workload = tostring(value)
    if workload == "" then
        return nil
    end

    if not supported_workloads[workload] then
        error("Unsupported workload: " .. workload .. ". Use wiki_abstract, wiki_page, or amazon_review.")
    end

    return workload
end

local function parse_table_name(value)
    if value == nil then
        return nil
    end

    local table_name = tostring(value)
    if table_name == "" then
        return nil
    end

    return table_name
end

local function get_workload()
    if cached_workload then
        return cached_workload
    end

    local workload = parse_workload(sysbench.opt.workload)
    if not workload then
        workload = "wiki_abstract"
    end

    cached_workload = workload
    sysbench.opt.workload = workload
    return cached_workload
end

local function get_table_name()
    if cached_table_name then
        return cached_table_name
    end

    local table_name = parse_table_name(sysbench.opt.table_name)
    if not table_name then
        table_name = "wiki_abstract"
    end

    cached_table_name = table_name
    sysbench.opt.table_name = table_name
    return cached_table_name
end

-- the default table name is same to workload
local function apply_table_name(sql)
    local workload = get_workload()
    local table_name = get_table_name():gsub("%%", "%%%%")
    return sql:gsub(workload, table_name, 1)
end

local function get_projection()
    if cached_projection then
        return cached_projection
    end

    local proj = tostring(sysbench.opt.proj_type or "all")
    local lower_proj = string.lower(proj)
    if lower_proj == "all" or proj == "*" then
        cached_projection = "*"
    elseif lower_proj == "count" or lower_proj == "count(*)" then
        cached_projection = "count(*)"
    else
        error("Unsupported proj_type: " .. proj .. ". Use 'all' or 'count'.")
    end

    return cached_projection
end

local function get_syntax()
    if cached_syntax then
        return cached_syntax
    end

    local syntax_opt = sysbench.opt.syntax or "old"
    syntax_opt = string.lower(tostring(syntax_opt))
    if syntax_opt ~= "old" and syntax_opt ~= "new" then
        error("Unsupported syntax: " .. tostring(sysbench.opt.syntax) .. ". Use 'old' or 'new'.")
    end
    cached_syntax = syntax_opt
    return cached_syntax
end

local function get_parser()
    if cached_parser then
        return cached_parser
    end

    local parser_opt = sysbench.opt.parser or "standard"
    parser_opt = string.lower(tostring(parser_opt))
    if parser_opt ~= "standard" and parser_opt ~= "ngram" then
        error("Unsupported parser: " .. tostring(sysbench.opt.parser) .. ". Use 'standard' or 'ngram'.")
    end
    cached_parser = parser_opt
    return cached_parser
end

local function get_ngram_size()
    if cached_ngram then
        return cached_ngram
    end

    local n = tonumber(sysbench.opt.ngram) or 2
    n = math.floor(n)
    if n < 1 then
        n = 1
    end
    cached_ngram = n
    return cached_ngram
end

local function apply_projection(sql)
    return sql:gsub("^SELECT %*", "SELECT " .. get_projection(), 1)
end

local function apply_syntax(sql)
    if get_syntax() == "old" then
        return sql
    end

    sql = sql:gsub("fts_match_prefix%(%?,%s*([^)]+)%)", "MATCH(%1) AGAINST(? IN BOOLEAN MODE)")
    sql = sql:gsub("fts_match_phrase%(%?,%s*([^)]+)%)", "MATCH(%1) AGAINST(? IN BOOLEAN MODE)")
    sql = sql:gsub("fts_match_word%(%?,%s*([^)]+)%)", "MATCH(%1) AGAINST(? IN BOOLEAN MODE)")
    return sql
end

local function format_prefix_param(val)
    if get_syntax() == "new" then
        return tostring(val) .. "*"
    end
    return val
end

local function format_phrase_param(val)
    if get_syntax() == "new" then
        local escaped = tostring(val):gsub("\\", "\\\\"):gsub("\"", "\\\"")
        return "\"" .. escaped .. "\""
    end
    return val
end

local function build_ngrams_from_list(cache_key, source_list, n)
    ngram_cache[cache_key] = ngram_cache[cache_key] or {}
    if ngram_cache[cache_key][n] then
        return ngram_cache[cache_key][n]
    end

    local grams = {}
    for _, text in ipairs(source_list) do
        for token in tostring(text):gmatch("%w+") do
            local len = #token
            if len >= n then
                for i = 1, len - n + 1 do
                    grams[#grams + 1] = string.sub(token, i, i + n - 1)
                end
            end
        end
    end

    ngram_cache[cache_key][n] = grams
    return grams
end

local function pick_ngram(cache_key, source_list)
    local size = get_ngram_size()
    local alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    local buf = {}
    for i = 1, size do
        local idx = sysbench.rand.default(1, #alphabet)
        buf[i] = string.sub(alphabet, idx, idx)
    end
    return table.concat(buf)
end

local t = sysbench.sql.type
local stmt_defs = {
    wiki_abstract = {
        -- not conj
        one_word_match = {"SELECT * FROM wiki_abstract WHERE fts_match_word(?, abstract)", {t.CHAR, 50}},
        phrase_match = {"SELECT * FROM wiki_abstract WHERE fts_match_phrase(?, abstract)", {t.VARCHAR, 128}},
        one_word_prefix_match = {"SELECT * FROM wiki_abstract WHERE fts_match_prefix(?, abstract)", {t.CHAR, 50}},

        -- same exprs conj
        two_words_and_match = {"SELECT * FROM wiki_abstract WHERE fts_match_word(?, abstract) or fts_match_word(?, abstract)",
                               {t.CHAR, 50}, {t.CHAR, 50}},
        two_words_or_match = {"SELECT * FROM wiki_abstract WHERE fts_match_word(?, abstract) and fts_match_word(?, abstract)",
                              {t.CHAR, 50}, {t.CHAR, 50}},
        two_phrases_and_match = {"SELECT * FROM wiki_abstract WHERE fts_match_phrase(?, abstract) and fts_match_phrase(?, abstract)",
                                 {t.VARCHAR, 128}, {t.VARCHAR, 128}},
        two_phrases_or_match = {"SELECT * FROM wiki_abstract WHERE fts_match_phrase(?, abstract) or fts_match_phrase(?, abstract)",
                                {t.VARCHAR, 128}, {t.VARCHAR, 128}},
        two_words_and_prefix_match = {"SELECT * FROM wiki_abstract WHERE fts_match_prefix(?, abstract) or fts_match_prefix(?, abstract)",
                                      {t.CHAR, 50}, {t.CHAR, 50}},
        two_words_or_prefix_match = {"SELECT * FROM wiki_abstract WHERE fts_match_prefix(?, abstract) and fts_match_prefix(?, abstract)",
                                     {t.CHAR, 50}, {t.CHAR, 50}},

        -- diff exprs conj
        word_and_prefix_match = {"SELECT * FROM wiki_abstract WHERE fts_match_prefix(?, abstract) and fts_match_word(?, abstract)",
                                 {t.CHAR, 50}, {t.CHAR, 50}},
        word_or_prefix_match = {"SELECT * FROM wiki_abstract WHERE fts_match_prefix(?, abstract) or fts_match_word(?, abstract)",
                                {t.CHAR, 50}, {t.CHAR, 50}},
        phrase_and_word_match = {"SELECT * FROM wiki_abstract WHERE fts_match_phrase(?, abstract) and fts_match_word(?, abstract)",
                                 {t.VARCHAR, 128}, {t.CHAR, 50}},
        phrase_or_word_match = {"SELECT * FROM wiki_abstract WHERE fts_match_phrase(?, abstract) or fts_match_word(?, abstract)",
                                {t.VARCHAR, 128}, {t.CHAR, 50}},
        phrase_and_prefix_match = {"SELECT * FROM wiki_abstract WHERE fts_match_phrase(?, abstract) and fts_match_prefix(?, abstract)",
                                   {t.VARCHAR, 128}, {t.CHAR, 50}},
        phrase_or_prefix_match = {"SELECT * FROM wiki_abstract WHERE fts_match_phrase(?, abstract) or fts_match_prefix(?, abstract)",
                                  {t.VARCHAR, 128}, {t.CHAR, 50}},

        -- diff fields and exprs conj, title must be fts index
        two_fields_word_and_match = {"SELECT * FROM wiki_abstract WHERE fts_match_word(?, abstract) and fts_match_word(?, title)",
                                     {t.CHAR, 50}, {t.CHAR, 50}},
        two_fields_word_or_match = {"SELECT * FROM wiki_abstract WHERE fts_match_word(?, abstract) and fts_match_word(?, title)",
                                    {t.CHAR, 50}, {t.CHAR, 50}},

        two_fields_word_and_prefix_match = {"SELECT * FROM wiki_abstract WHERE fts_match_prefix(?, abstract) and fts_match_prefix(?, title)",
                                            {t.CHAR, 50}, {t.CHAR, 50}},
        two_fields_word_or_prefix_match = {"SELECT * FROM wiki_abstract WHERE fts_match_prefix(?, abstract) and fts_match_prefix(?, title)",
                                           {t.CHAR, 50}, {t.CHAR, 50}},

        -- TODO other select query types
        -- abstract,title,url
        update = {"UPDATE wiki_abstract SET abstract=?,title=?,url=? WHERE id=?", {t.CHAR, 2048}, {t.CHAR, 256},
                  {t.CHAR, 256}, t.INT},
        delete = {"DELETE FROM wiki_abstract LIMIT ?", t.INT},
        insert = {"INSERT INTO wiki_abstract (abstract,title,url) VALUES (?, ?, ?)", {t.CHAR, 2048}, {t.CHAR, 256},
                  {t.CHAR, 256}}
    },
    wiki_page = {
        one_word_match = {"SELECT * FROM wiki_page WHERE fts_match_word(?, `text`)", {t.CHAR, 50}},
        phrase_match = {"SELECT * FROM wiki_page WHERE fts_match_phrase(?, `text`)", {t.VARCHAR, 128}},
        two_phrases_and_match = {"SELECT * FROM wiki_page WHERE fts_match_phrase(?, `text`) and fts_match_phrase(?, `text`)",
                                 {t.VARCHAR, 128}, {t.VARCHAR, 128}},
        two_phrases_or_match = {"SELECT * FROM wiki_page WHERE fts_match_phrase(?, `text`) or fts_match_phrase(?, `text`)",
                                {t.VARCHAR, 128}, {t.VARCHAR, 128}},
        phrase_and_word_match = {"SELECT * FROM wiki_page WHERE fts_match_phrase(?, `text`) and fts_match_word(?, `text`)",
                                 {t.VARCHAR, 128}, {t.CHAR, 50}},
        phrase_or_word_match = {"SELECT * FROM wiki_page WHERE fts_match_phrase(?, `text`) or fts_match_word(?, `text`)",
                                {t.VARCHAR, 128}, {t.CHAR, 50}},
        phrase_and_prefix_match = {"SELECT * FROM wiki_page WHERE fts_match_phrase(?, `text`) and fts_match_prefix(?, `text`)",
                                   {t.VARCHAR, 128}, {t.CHAR, 50}},
        phrase_or_prefix_match = {"SELECT * FROM wiki_page WHERE fts_match_phrase(?, `text`) or fts_match_prefix(?, `text`)",
                                  {t.VARCHAR, 128}, {t.CHAR, 50}},
        two_words_and_match = {"SELECT * FROM wiki_page WHERE fts_match_word(?, `text`) or fts_match_word(?, `text`)",
                               {t.CHAR, 50}, {t.CHAR, 50}},
        two_words_or_match = {"SELECT * FROM wiki_page WHERE fts_match_word(?, `text`) and fts_match_word(?, `text`)",
                              {t.CHAR, 50}, {t.CHAR, 50}},
        -- comment must be fts index
        two_fields_word_and_match = {"SELECT * FROM wiki_page WHERE fts_match_word(?, `text`) and fts_match_word(?, `comment`)",
                                     {t.CHAR, 50}, {t.CHAR, 50}},
        two_fields_word_or_match = {"SELECT * FROM wiki_page WHERE fts_match_word(?, `text`) and fts_match_word(?, `comment`)",
                                    {t.CHAR, 50}, {t.CHAR, 50}},

        one_word_prefix_match = {"SELECT * FROM wiki_page WHERE fts_match_prefix(?, `text`)", {t.CHAR, 50}},
        two_words_and_prefix_match = {"SELECT * FROM wiki_page WHERE fts_match_prefix(?, `text`) or fts_match_prefix(?, `text`)",
                                      {t.CHAR, 50}, {t.CHAR, 50}},
        two_words_or_prefix_match = {"SELECT * FROM wiki_page WHERE fts_match_prefix(?, `text`) and fts_match_prefix(?, `text`)",
                                     {t.CHAR, 50}, {t.CHAR, 50}},
        -- comment must be fts index
        two_fields_word_and_prefix_match = {"SELECT * FROM wiki_page WHERE fts_match_prefix(?, `text`) and fts_match_prefix(?, `comment`)",
                                            {t.CHAR, 50}, {t.CHAR, 50}},
        two_fields_word_or_prefix_match = {"SELECT * FROM wiki_page WHERE fts_match_prefix(?, `text`) and fts_match_prefix(?, `comment`)",
                                           {t.CHAR, 50}, {t.CHAR, 50}},

        word_and_prefix_match = {"SELECT * FROM wiki_page WHERE fts_match_prefix(?, `text`) and fts_match_word(?, `text`)",
                                 {t.CHAR, 50}, {t.CHAR, 50}},
        word_or_prefix_match = {"SELECT * FROM wiki_page WHERE fts_match_prefix(?, `text`) or fts_match_word(?, `text`)",
                                {t.CHAR, 50}, {t.CHAR, 50}},

        -- TODO other select query types
        -- title,text,comment,username,timestamp
        update = {"UPDATE wiki_page SET title=?,`text`=?,`comment`=?,username=?,`timestamp`=? WHERE id=?",
                  {t.CHAR, 256}, {t.CHAR, 65532}, {t.CHAR, 256}, {t.CHAR, 256}, {t.CHAR, 128}, t.INT},
        delete = {"DELETE FROM wiki_page LIMIT ?", t.INT},
        insert = {"INSERT INTO wiki_page (title,`text`,`comment`,username,`timestamp`) (?, ?, ?, ?, ?)", {t.CHAR, 256},
                  {t.CHAR, 65532}, {t.CHAR, 256}, {t.CHAR, 256}, {t.CHAR, 128}}
    },
    amazon_review = {
        -- not conj
        one_word_match = {"SELECT * FROM amazon_review WHERE fts_match_word(?, review_body)", {t.CHAR, 50}},
        phrase_match = {"SELECT * FROM amazon_review WHERE fts_match_phrase(?, review_body)", {t.VARCHAR, 128}},
        one_word_prefix_match = {"SELECT * FROM amazon_review WHERE fts_match_prefix(?, review_body)", {t.CHAR, 50}},

        -- same exprs conj
        two_words_and_match = {"SELECT * FROM amazon_review WHERE fts_match_word(?, review_body) or fts_match_word(?, review_body)",
                               {t.CHAR, 50}, {t.CHAR, 50}},
        two_words_or_match = {"SELECT * FROM amazon_review WHERE fts_match_word(?, review_body) and fts_match_word(?, review_body)",
                              {t.CHAR, 50}, {t.CHAR, 50}},
        two_phrases_and_match = {"SELECT * FROM amazon_review WHERE fts_match_phrase(?, review_body) and fts_match_phrase(?, review_body)",
                                 {t.VARCHAR, 128}, {t.VARCHAR, 128}},
        two_phrases_or_match = {"SELECT * FROM amazon_review WHERE fts_match_phrase(?, review_body) or fts_match_phrase(?, review_body)",
                                {t.VARCHAR, 128}, {t.VARCHAR, 128}},
        two_words_and_prefix_match = {"SELECT * FROM amazon_review WHERE fts_match_prefix(?, review_body) or fts_match_prefix(?, review_body)",
                                      {t.CHAR, 50}, {t.CHAR, 50}},
        two_words_or_prefix_match = {"SELECT * FROM amazon_review WHERE fts_match_prefix(?, review_body) and fts_match_prefix(?, review_body)",
                                     {t.CHAR, 50}, {t.CHAR, 50}},

        --  diff exprs conj
        word_and_prefix_match = {"SELECT * FROM amazon_review WHERE fts_match_prefix(?, review_body) and fts_match_word(?, review_body)",
                                 {t.CHAR, 50}, {t.CHAR, 50}},
        word_or_prefix_match = {"SELECT * FROM amazon_review WHERE fts_match_prefix(?, review_body) or fts_match_word(?, review_body)",
                                {t.CHAR, 50}, {t.CHAR, 50}},
        phrase_and_word_match = {"SELECT * FROM amazon_review WHERE fts_match_phrase(?, review_body) and fts_match_word(?, review_body)",
                                 {t.VARCHAR, 128}, {t.CHAR, 50}},
        phrase_or_word_match = {"SELECT * FROM amazon_review WHERE fts_match_phrase(?, review_body) or fts_match_word(?, review_body)",
                                {t.VARCHAR, 128}, {t.CHAR, 50}},
        phrase_and_prefix_match = {"SELECT * FROM amazon_review WHERE fts_match_phrase(?, review_body) and fts_match_prefix(?, review_body)",
                                   {t.VARCHAR, 128}, {t.CHAR, 50}},
        phrase_or_prefix_match = {"SELECT * FROM amazon_review WHERE fts_match_phrase(?, review_body) or fts_match_prefix(?, review_body)",
                                  {t.VARCHAR, 128}, {t.CHAR, 50}},

        --   diff fields and exprs conj, review_headline must be fts index
        two_fields_word_and_match = {"SELECT * FROM amazon_review WHERE fts_match_word(?, review_body) and fts_match_word(?, review_headline)",
                                     {t.CHAR, 50}, {t.CHAR, 50}},
        two_fields_word_or_match = {"SELECT * FROM amazon_review WHERE fts_match_word(?, review_body) and fts_match_word(?, review_headline)",
                                    {t.CHAR, 50}, {t.CHAR, 50}},

        two_fields_word_and_prefix_match = {"SELECT * FROM amazon_review WHERE fts_match_prefix(?, review_body) and fts_match_prefix(?, review_headline)",
                                            {t.CHAR, 50}, {t.CHAR, 50}},
        two_fields_word_or_prefix_match = {"SELECT * FROM amazon_review WHERE fts_match_prefix(?, review_body) and fts_match_prefix(?, review_headline)",
                                           {t.CHAR, 50}, {t.CHAR, 50}},

        -- TODO other select query types
        -- review_date,marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body
        update = {"UPDATE amazon_review SET review_date=?,marketplace=?,customer_id=?,review_id=?,product_id=?,product_parent=?,product_title=?,product_category=?,star_rating=?,helpful_votes=?,total_votes=?,vine=?,verified_purchase=?,review_headline=?,review_body=? WHERE id=?",
                  t.INT, {t.CHAR, 20}, t.BIGINT, {t.CHAR, 40}, {t.CHAR, 20}, t.BIGINT, {t.CHAR, 500}, {t.CHAR, 50},
                  t.INT, t.INT, t.INT, t.INT, t.INT, {t.CHAR, 500}, {t.CHAR, 65532}, t.BIGINT},
        delete = {"DELETE FROM amazon_review LIMIT ?", t.INT},
        insert = {"INSERT INTO amazon_review (review_date,marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  t.INT, {t.CHAR, 20}, t.BIGINT, {t.CHAR, 40}, {t.CHAR, 20}, t.BIGINT, {t.CHAR, 500}, {t.CHAR, 50},
                  t.INT, t.INT, t.INT, t.INT, t.INT, {t.CHAR, 500}, {t.CHAR, 65532}}
    }
}

local function quote_sql_literal(val)
    local s = tostring(val)
    s = s:gsub("\\", "\\\\")
    s = s:gsub("'", "\\'")
    return "'" .. s .. "'"
end

local function build_direct_sql(key, values)
    local w = get_workload()
    local def = stmt_defs[w][key]
    if not def then
        error("Unknown statement key: " .. tostring(key))
    end
    local sql = apply_table_name(apply_projection(apply_syntax(def[1])))
    local idx = 0
    sql = sql:gsub("%?", function()
        idx = idx + 1
        local v = values[idx]
        if v == nil then
            error(string.format("Missing param %d for key %s", idx, tostring(key)))
        end
        return quote_sql_literal(v)
    end)
    if idx ~= #values then
        error(string.format("Parameter count mismatch for key %s: expected %d placeholders, got %d values", tostring(key), idx, #values))
    end
    -- print(sql)
    return sql
end

function prepare_begin()
    stmt.begin = con:prepare("BEGIN")
end

function prepare_commit()
    stmt.commit = con:prepare("COMMIT")
end

function prepare_for_stmts(key)
    local w = get_workload()
    local def = stmt_defs[w][key]
    stmt[w][key] = con:prepare(apply_table_name(apply_projection(def[1])))

    local nparam = #def - 1

    if nparam > 0 then
        param[w][key] = {}
    end

    for p = 1, nparam do
        local btype = def[p + 1]
        local len
        if type(btype) == "table" then
            len = btype[2]
            btype = btype[1]
        end
        if btype == sysbench.sql.type.VARCHAR or btype == sysbench.sql.type.CHAR then
            param[w][key][p] = stmt[w][key]:bind_create(btype, len)
        else
            param[w][key][p] = stmt[w][key]:bind_create(btype)
        end
    end

    if nparam > 0 then
        stmt[w][key]:bind_param(unpack(param[w][key]))
    end
end

function prepare_insert()
    prepare_for_stmts("insert")
end

function prepare_update()
    prepare_for_stmts("update")
end

function prepare_delete()
    prepare_for_stmts("delete")
end

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()
    local w = get_workload()

    stmt = {}
    param = {}

    stmt[w] = {}
    param[w] = {}

    prepare_statements()
    assign_thread_update_ids()
    build_operation_ratios()
end

-- Close prepared statements
function close_statements()
    local w = get_workload()
    if stmt and stmt[w] then
        for _, s in pairs(stmt[w]) do
            if s and s.close then
                s:close()
            end
        end
    end
end

function thread_done()
    close_statements()
    con:disconnect()
end

local function get_id()
    return sysbench.rand.default(1, sysbench.opt.table_size)
end

local function get_wiki_abstract_word()
    -- all rows is about 55 millions

    local words = {}
    if sysbench.opt.ret_little_rows then
        -- Chineses 3 rows
        -- chinana 8 rows
        -- usdollar 8 rows
        -- Australi 8 rows
        -- 19300 17 rows
        -- Philosoph 19 rows
        -- Bosonic 230 rows
        -- misunderstood 374 rows
        -- Panchatantra 472 rows
        -- Rostropovich 592 rows
        words = {"Chineses", "chinana", "usdollar", "Australi", "19300", "Philosoph", "Bosonic", "misunderstood",
                 "Panchatantra", "Rostropovich"}
    else
        -- Drill 13000 rows
        -- athlete 33000 rows
        -- location 46000 rows
        -- creation 75000 rows
        -- next 100000 rows
        -- long 450000 rows
        -- Louis 120000 rows
        -- founder 270000 rows
        -- subdivision 2000000 rows
        -- birth 7500000 rows
        words = {"Drill", "athlete", "location", "creation", "next", "long", "Louis", "founder", "subdivision", "birth"}
    end
    return words[sysbench.rand.default(1, #words)]
end

local function get_wiki_page_word()
    local words = {"history", "university", "development", "software", "system", "information", "government",
                   "research", "community", "including", "between", "based", "known", "local"}
    return words[sysbench.rand.default(1, #words)]
end

local function get_amazon_review_word()

    local words = {}
    if sysbench.opt.ret_little_rows then
        -- lightweigh 176
        -- lighte 49
        -- Constructio 28
        -- Constru 24
        -- accom 44
        -- stainles 177
        -- tita 158
        -- NCJ-30 16
        -- NCJ 16
        -- 30000 488
        -- 1000000 491
        -- 2025 1255
        -- Alleg 9
        -- Alle 1407
        words = {"lightweigh", "lighte", "Constructio", "Constru", "accom", "stainles", "tita", "NCJ30", "NCJ",
                 "30000", "1000000", "2025", "Alleg", "Alle"}
    else
        -- excellent 4503847
        -- quality 7969086
        -- product 11600853
        -- work 8836326
        -- perfect 6634541
        -- easy 9040191
        -- recommend 9158995
        -- price 8575235
        -- value 1309120
        -- comfortable 2990806
        -- use 12427544
        -- love 18479833
        -- fast 3425589
        -- great 29911391
        words = {"excellent", "quality", "product", "work", "perfect", "easy", "recommend", "price", "value",
                 "comfortable", "use", "love", "fast", "great"}
    end

    if get_parser() == "ngram" then
        return pick_ngram("amazon_review_word", words)
    end

    return words[sysbench.rand.default(1, #words)]
end

local function get_wiki_abstract_phrase()
    local phrases = {}
    if sysbench.opt.ret_little_rows then
        -- Danish sociologist 48 rows
        -- Japanese storyboard artist 40 rows
        -- biggest upset 8 rows
        -- Brennan Hesser 8 rows
        -- Television Limited 64 rows
        -- several feature films 130 rows
        -- quantum entanglement 234 rows
        -- gothic cathedral 71 rows
        -- ancient monastery 40 rows
        -- neutron star 208 rows
        -- marine biology 555 rows
        -- artificial satellite 227 rows
        -- mythical creature 417 rows
        -- historic harbor 8 rows
        phrases = {"Danish sociologist", "Japanese storyboard artist", "biggest upset", "Brennan Hesser",
                   "Television Limited", "several feature films", "quantum entanglement", "gothic cathedral",
                   "ancient monastery", "neutron star", "marine biology", "artificial satellite", "mythical creature",
                   "historic harbor"}
    else
        -- New York 577240 rows
        -- New York Times 21197 rows
        -- best known 170790 rows
        -- South African 46520 rows
        -- United States 1081683
        -- World War 220669 rows
        -- United Kingdom 249375 rows
        -- Los Angeles 129717 rows
        -- United Nations 50955 rows
        -- prime minister 38323 rows
        -- football club 96464 rows
        -- film director 35641 rows
        -- civil rights 11545 rows
        -- political party 45472 rows
        -- trade union 27619 rows
        -- human rights 33895 rows
        phrases = {"New York", "New York Times", "best known", "South African", "United States", "World War",
                   "United Kingdom", "Los Angeles", "United Nations", "prime minister", "football club",
                   "film director", "civil rights", "political party", "trade union", "human rights"}
    end
    if get_parser() == "ngram" then
        return pick_ngram("wiki_abstract_phrase", phrases)
    end
    return phrases[sysbench.rand.default(1, #phrases)]
end

local function get_wiki_page_phrase()
    local phrases = {}
    if sysbench.opt.ret_little_rows then
        phrases = {"distributed ledger", "artificial reef", "quantum cascade", "photonic crystal", "medieval charter",
                   "solar observatory", "lunar mission", "data locality", "compiler design", "dynamic graph"}
    else
        phrases = {"computer science", "operating system", "open source", "machine learning", "user interface",
                   "information system", "data structure", "research university", "prime minister", "city council"}
    end
    return phrases[sysbench.rand.default(1, #phrases)]
end

local function get_amazon_review_phrase()
    local phrases = {}
    if sysbench.opt.ret_little_rows then
        -- pressure sensor 935 rows
        -- leaking again 725 rows
        -- mortar chain 49 rows
        -- without attacking 311 rows
        -- room more effectively 6 rows
        -- wet washcloth 818 rows
        -- pretty and absorbent 364 rows
        -- real pinball machines 24 rows
        -- inventory unlocked 1 row
        -- entire feud developed 2 rows
        -- last encounter 582 rows
        -- Von Trapp family 204 rows
        -- Within the context 971 rows
        -- Allen Stagg 2 rows
        phrases = {"pressure sensor", "leaking again", "mortar chain", "without attacking", "room more effectively",
                   "wet washcloth", "pretty and absorbent", "real pinball machines", "inventory unlocked",
                   "entire feud developed", "last encounter", "Von Trapp family", "Within the context", "Allen Stagg"}
    else
        -- high quality 987705 rows
        -- well made 1459593
        -- great product 1876429 rows
        -- easy to use 11821 rows
        -- customer service 930555 rows
        -- fast shipping 494421 rows
        -- works great 2478859 rows
        -- love this 20245935 rows
        -- fit perfectly 430865 rows
        -- as described 1907844 rows
        -- battery compartment 42243 rows
        -- instruction manual 75296 rows
        -- control panel 49771 rows
        -- car seat 76303 rows
        -- coffee grinder 61296 rows
        -- power adapter 78862 rows
        phrases = {"high quality", "well made", "great product", "easy to use", "customer service", "fast shipping",
                   "works great", "love this", "fit perfectly", "as described", "battery compartment",
                   "instruction manual", "control panel", "car seat", "coffee grinder", "power adapter"}
    end
    return phrases[sysbench.rand.default(1, #phrases)]
end

local function get_fts_word()
    if sysbench.opt.workload == "wiki_abstract" then
        return get_wiki_abstract_word()
    elseif sysbench.opt.workload == "wiki_page" then
        return get_wiki_page_word()
    elseif sysbench.opt.workload == "amazon_review" then
        return get_amazon_review_word()
    else
        error("Unknown workload: " .. sysbench.opt.workload)
    end
end

local function get_fts_phrase()
    if sysbench.opt.workload == "wiki_abstract" then
        return get_wiki_abstract_phrase()
    elseif sysbench.opt.workload == "wiki_page" then
        return get_wiki_page_phrase()
    elseif sysbench.opt.workload == "amazon_review" then
        return get_amazon_review_phrase()
    else
        error("Unknown workload: " .. sysbench.opt.workload)
    end
end

function begin()
    con:query("BEGIN")
end

function commit()
    con:query("COMMIT")
end

function execute_one_word_match()
    for i = 1, sysbench.opt.one_word_matchs do
        local sql = build_direct_sql("one_word_match", {get_fts_word()})
        con:query(sql)
    end
end

function execute_phrase_match()
    for i = 1, sysbench.opt.phrase_matchs do
        local sql = build_direct_sql("phrase_match", {format_phrase_param(get_fts_phrase())})
        con:query(sql)
    end
end

function execute_two_phrases_conj_match()
    local w = sysbench.opt.workload
    for i = 1, sysbench.opt.two_phrases_conj_matchs do
        local sql_and = build_direct_sql("two_phrases_and_match",
                                         {format_phrase_param(get_fts_phrase()), format_phrase_param(get_fts_phrase())})
        con:query(sql_and)
        local sql_or = build_direct_sql("two_phrases_or_match",
                                        {format_phrase_param(get_fts_phrase()), format_phrase_param(get_fts_phrase())})
        con:query(sql_or)
    end
end

function execute_phrase_word_conj_match()
    local w = sysbench.opt.workload
    for i = 1, sysbench.opt.phrase_word_conj_matchs do
        local phrase = format_phrase_param(get_fts_phrase())
        local word = get_fts_word()
        local sql_and = build_direct_sql("phrase_and_word_match", {phrase, word})
        con:query(sql_and)
        local sql_or = build_direct_sql("phrase_or_word_match", {phrase, word})
        con:query(sql_or)
    end
end

function execute_phrase_prefix_conj_match()
    local w = sysbench.opt.workload
    for i = 1, sysbench.opt.phrase_prefix_conj_matchs do
        local phrase = format_phrase_param(get_fts_phrase())
        local prefix = format_prefix_param(get_fts_word())
        local sql_and = build_direct_sql("phrase_and_prefix_match", {phrase, prefix})
        con:query(sql_and)
        local sql_or = build_direct_sql("phrase_or_prefix_match", {phrase, prefix})
        con:query(sql_or)
    end
end

function execute_two_words_conj_match()
    local w = sysbench.opt.workload
    for i = 1, sysbench.opt.two_words_conj_matchs do
        local w1 = get_fts_word()
        local w2 = get_fts_word()
        local sql_and = build_direct_sql("two_words_and_match", {w1, w2})
        con:query(sql_and)
        local sql_or = build_direct_sql("two_words_or_match", {w1, w2})
        con:query(sql_or)
    end
end

function execute_two_fields_word_conj_match()
    local w = sysbench.opt.workload
    for i = 1, sysbench.opt.two_fields_word_conj_matchs do
        local w1 = get_fts_word()
        local w2 = get_fts_word()
        local sql_and = build_direct_sql("two_fields_word_and_match", {w1, w2})
        con:query(sql_and)
        local sql_or = build_direct_sql("two_fields_word_or_match", {w1, w2})
        con:query(sql_or)
    end
end

function execute_one_word_prefix_match()
    for i = 1, sysbench.opt.one_word_prefix_matchs do
        local sql = build_direct_sql("one_word_prefix_match", {format_prefix_param(get_fts_word())})
        con:query(sql)
    end
end

function execute_two_words_prefix_conj_match()
    local w = sysbench.opt.workload
    for i = 1, sysbench.opt.two_words_prefix_conj_matchs do
        local w1 = format_prefix_param(get_fts_word())
        local w2 = format_prefix_param(get_fts_word())
        local sql_and = build_direct_sql("two_words_and_prefix_match", {w1, w2})
        con:query(sql_and)
        local sql_or = build_direct_sql("two_words_or_prefix_match", {w1, w2})
        con:query(sql_or)
    end
end

function execute_two_fields_word_prefix_conj_match()
    local w = sysbench.opt.workload
    for i = 1, sysbench.opt.two_fields_word_prefix_conj_matchs do
        local w1 = format_prefix_param(get_fts_word())
        local w2 = format_prefix_param(get_fts_word())
        local sql_and = build_direct_sql("two_fields_word_and_prefix_match", {w1, w2})
        con:query(sql_and)
        local sql_or = build_direct_sql("two_fields_word_or_prefix_match", {w1, w2})
        con:query(sql_or)
    end
end

function execute_word_prefix_conj_match()
    local w = sysbench.opt.workload
    for i = 1, sysbench.opt.word_prefix_conj_matchs do
        local w1 = format_prefix_param(get_fts_word())
        local w2 = format_prefix_param(get_fts_word())
        local sql_and = build_direct_sql("word_and_prefix_match", {w1, w2})
        con:query(sql_and)
        local sql_or = build_direct_sql("word_or_prefix_match", {w1, w2})
        con:query(sql_or)
    end
end

function execute_insert(row)
    if sysbench.opt.workload == "wiki_abstract" then
        -- abstract,title,url
        param[sysbench.opt.workload].insert[1]:set(row["abstract"])
        local title = row["title"] or ""
        local url = row["url"] or ""
        if #title > 256 then
            title = string.sub(title, 1, 256)
        end
        if #url > 256 then
            url = string.sub(url, 1, 256)
        end
        param[sysbench.opt.workload].insert[2]:set(title)
        param[sysbench.opt.workload].insert[3]:set(url)
    elseif sysbench.opt.workload == "wiki_page" then
        -- title,text,comment,username,timestamp
        param[sysbench.opt.workload].insert[1]:set(row["title"])
        param[sysbench.opt.workload].insert[2]:set(row["text"])
        param[sysbench.opt.workload].insert[3]:set(row["comment"])
        param[sysbench.opt.workload].insert[4]:set(row["username"])
        param[sysbench.opt.workload].insert[5]:set(row["timestamp"])
    elseif sysbench.opt.workload == "amazon_review" then
        -- review_date,marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body
        param[sysbench.opt.workload].insert[1]:set(tonumber(row["review_date"]))
        param[sysbench.opt.workload].insert[2]:set(row["marketplace"])
        param[sysbench.opt.workload].insert[3]:set(tonumber(row["customer_id"]))
        param[sysbench.opt.workload].insert[4]:set(row["review_id"])
        param[sysbench.opt.workload].insert[5]:set(row["product_id"])
        param[sysbench.opt.workload].insert[6]:set(tonumber(row["product_parent"]))
        param[sysbench.opt.workload].insert[7]:set(row["product_title"])
        param[sysbench.opt.workload].insert[8]:set(row["product_category"])
        param[sysbench.opt.workload].insert[9]:set(tonumber(row["star_rating"]))
        param[sysbench.opt.workload].insert[10]:set(tonumber(row["helpful_votes"]))
        param[sysbench.opt.workload].insert[11]:set(tonumber(row["total_votes"]))
        param[sysbench.opt.workload].insert[12]:set(str2boolint(row["vine"]))
        param[sysbench.opt.workload].insert[13]:set(str2boolint(row["verified_purchase"]))
        param[sysbench.opt.workload].insert[14]:set(row["review_headline"])
        param[sysbench.opt.workload].insert[15]:set(row["review_body"])
    end
    stmt[sysbench.opt.workload].insert:execute()
end

function execute_update(row)
    -- TODO handle write conflict
    local ids = thread_update_ids or update_ids
    if not ids or #ids == 0 then
        return
    end
    local update_id = tonumber(ids[sysbench.rand.uniform(1, #ids)])
    if not update_id then
        return
    end
    if sysbench.opt.workload == "wiki_abstract" then
        -- abstract,title,url
        param[sysbench.opt.workload].update[1]:set(row["abstract"])
        local title = row["title"] or ""
        local url = row["url"] or ""
        if #title > 256 then
            title = string.sub(title, 1, 256)
        end
        if #url > 256 then
            url = string.sub(url, 1, 256)
        end
        param[sysbench.opt.workload].update[2]:set(title)
        param[sysbench.opt.workload].update[3]:set(url)
        param[sysbench.opt.workload].update[4]:set(update_id)
    elseif sysbench.opt.workload == "wiki_page" then
        -- title,text,comment,username,timestamp
        param[sysbench.opt.workload].update[1]:set(row["title"])
        param[sysbench.opt.workload].update[2]:set(row["text"])
        param[sysbench.opt.workload].update[3]:set(row["comment"])
        param[sysbench.opt.workload].update[4]:set(row["username"])
        param[sysbench.opt.workload].update[5]:set(row["timestamp"])
        param[sysbench.opt.workload].update[6]:set(update_id)
    elseif sysbench.opt.workload == "amazon_review" then
        -- review_date,marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body
        param[sysbench.opt.workload].update[1]:set(tonumber(row["review_date"]))
        param[sysbench.opt.workload].update[2]:set(row["marketplace"])
        param[sysbench.opt.workload].update[3]:set(tonumber(row["customer_id"]))
        param[sysbench.opt.workload].update[4]:set(row["review_id"])
        param[sysbench.opt.workload].update[5]:set(row["product_id"])
        param[sysbench.opt.workload].update[6]:set(tonumber(row["product_parent"]))
        param[sysbench.opt.workload].update[7]:set(row["product_title"])
        param[sysbench.opt.workload].update[8]:set(row["product_category"])
        param[sysbench.opt.workload].update[9]:set(tonumber(row["star_rating"]))
        param[sysbench.opt.workload].update[10]:set(tonumber(row["helpful_votes"]))
        param[sysbench.opt.workload].update[11]:set(tonumber(row["total_votes"]))
        param[sysbench.opt.workload].update[12]:set(str2boolint(row["vine"]))
        param[sysbench.opt.workload].update[13]:set(str2boolint(row["verified_purchase"]))
        param[sysbench.opt.workload].update[14]:set(row["review_headline"])
        param[sysbench.opt.workload].update[15]:set(row["review_body"])
        param[sysbench.opt.workload].update[16]:set(update_id)
    end
    stmt[sysbench.opt.workload].update:execute()
end

function execute_delete(row)
    local limit = tonumber(sysbench.opt.delete_limit) or 1
    limit = math.floor(limit)
    if limit < 1 then
        return
    end
    param[sysbench.opt.workload].delete[1]:set(limit)
    stmt[sysbench.opt.workload].delete:execute()
end

function write(...)
    local handles = {...}
    if #handles == 0 then
        return
    end
    local handle_count = #handles
    local ratio_weights = {}
    local ratio_total = 0
    for idx, weight in ipairs(operation_ratios.weights) do
        local count = math.max(math.floor(tonumber(weight) or 0), 0)
        ratio_weights[idx] = count
        if idx <= handle_count then
            ratio_total = ratio_total + count
        end
    end

    local file_name = "fts.wiki_abstract"
    if sysbench.opt.workload == "wiki_abstract" then
        file_name = "fts.wiki_abstract"
    elseif sysbench.opt.workload == "wiki_page" then
        file_name = "fts.wiki_page"
    elseif sysbench.opt.workload == "amazon_review" then
        file_name = "fts.amazon_review"
    else
        error("Unknown workload: " .. sysbench.opt.workload)
    end
    -- file name format: fts.wiki_abstract.3.csv
    file_name = file_name .. "." .. sysbench.rand.uniform(1, sysbench.opt.source_files) .. ".csv"
    local dir = sysbench.opt.source_file_dir or "."
    if dir ~= "/" then
        dir = dir:gsub("/+$", "")
        if dir == "" then
            dir = "."
        end
    end
    local file_path = file_name
    if dir ~= "." then
        if string.sub(dir, -1) == "/" then
            file_path = dir .. file_name
        else
            file_path = dir .. "/" .. file_name
        end
    end
    local f = csv.open(file_path, {
        header = true
    })
    print("Thread ", sysbench.tid, " open csv file name: ", file_path)
    --  iter read for large file
    for r in f:lines() do
        for k, v in pairs(r) do
            v = string.gsub(v, "\\", "")
            v = string.gsub(v, "'", "")
            v = string.gsub(v, "([\"'])", "\\%1")
            r[k] = v
        end

        local executed = false
        if not sysbench.opt.skip_trx and ratio_total > 0 then
            begin()
        end
        for idx, handle in ipairs(handles) do
            local times = ratio_weights[idx] or 0
            for _ = 1, times do
                handle(r)
                executed = true
            end
        end
        if not sysbench.opt.skip_trx and executed then
            commit()
        end
        check_reconnect()
    end
    f:close()
end

function str2boolint(s)
    local lower_s = string.lower(s)
    local map = {
        ["true"] = 1,
        ["false"] = 0
    }
    return map[lower_s] or 0
end

function assign_thread_update_ids()
    thread_update_ids = nil
    if not update_ids or #update_ids == 0 then
        return
    end

    local threads = tonumber(sysbench.opt.threads) or 1
    if threads < 1 then
        threads = 1
    end
    local total = #update_ids
    local chunk = math.floor((total + threads - 1) / threads)

    local start_idx = sysbench.tid * chunk + 1
    if start_idx > total then
        thread_update_ids = {}
        return
    end

    local end_idx = math.min(start_idx + chunk - 1, total)
    thread_update_ids = {}
    for i = start_idx, end_idx do
        thread_update_ids[#thread_update_ids + 1] = update_ids[i]
    end
end

function gen_random_update_ids()
    local table_name = get_table_name()
    local limit = tonumber(sysbench.opt.update_random_ids) or 1000000
    limit = math.floor(limit)
    if limit < 1 then
        return
    end

    local drv = sysbench.sql.driver()
    local con = drv:connect()
    local query
    if sysbench.opt.update_rand_ids then
        query = string.format("select id from %s order by rand() limit %d", table_name, limit)
    else
        query = string.format("select id from %s limit %d", table_name, limit)
    end
    local rs = con:query(query)
    if rs.nrows < 1 then
        return
    end
    local values = {}
    while true do
        local r = rs:fetch_row()
        if not r then
            break
        end
        table.insert(values, r[1])
    end

    local file = io.open(table_name .. ".ids.txt", "w")
    if file then
        file:write(table.concat(values, "\n"))
        file:close()
    else
        error("can't open file: " .. table_name .. ".ids.txt")
    end
end

function build_operation_ratios()
    operation_ratios = {
        weights = {math.max(math.floor(sysbench.opt.insert_ratio), 0),
                   math.max(math.floor(sysbench.opt.update_ratio), 0),
                   math.max(math.floor(sysbench.opt.delete_ratio), 0)}
    }
end

-- No prepared statements to rebuild after restart events.
function sysbench.hooks.before_restart_event(errdesc)
    if errdesc.sql_errno == 2013 or -- CR_SERVER_LOST
    errdesc.sql_errno == 2055 or -- CR_SERVER_LOST_EXTENDED
    errdesc.sql_errno == 2006 or -- CR_SERVER_GONE_ERROR
    errdesc.sql_errno == 2011 -- CR_TCP_CONNECTION
    then
        close_statements()
        prepare_statements()
        assign_thread_update_ids()
    end
end

function check_reconnect()
    if sysbench.opt.reconnect > 0 then
        transactions = (transactions or 0) + 1
        if transactions % sysbench.opt.reconnect == 0 then
            con:reconnect()
            close_statements()
            prepare_statements()
            assign_thread_update_ids()
        end
    end
end
