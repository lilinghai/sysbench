#!/usr/bin/env sysbench

function init()
    assert(event ~= nil,
        "this script is meant to be included by other lua scripts and " .. "should not be called directly.")
end

if sysbench.cmdline.command == nil then
    error("Command is required. Supported commands: run, help")
end

-- Command line options
sysbench.cmdline.options = {
    workload = {"Using one of the workloads [wiki_abstract,wiki_page,amazon_review]", "wiki_abstract"},
    one_word_matchs = {"Number of one word match SELECT queries per transaction", 1},
    two_words_or_matchs = {"Number of two words or match SELECT queries per transaction", 1},
    two_words_and_matchs = {"Number of two words and match SELECT queries per transaction", 1},
    two_fields_word_or_matchs = {"Number of two fields or match SELECT queries per transaction", 1},
    two_fields_word_and_matchs = {"Number of two fields and match SELECT queries per transaction", 1},
    one_word_prefix_matchs = {"Number of one word prefix match SELECT queries per transaction", 1},
    two_words_or_prefix_matchs = {"Number of two words or prefix match SELECT queries per transaction", 1},
    two_words_and_prefix_matchs = {"Number of two words and prefix match SELECT queries per transaction", 1},
    two_fields_word_or_prefix_matchs = {"Number of two fields or prefix match SELECT queries per transaction", 1},
    two_fields_word_and_prefix_matchs = {"Number of two fields and prefix match SELECT queries per transaction", 1},
    mix_prefix_and_word_matchs = {"Number of mix prefix and word match SELECT queries per transaction", 1},
    mix_prefix_or_word_matchs = {"Number of mix prefix or word match SELECT queries per transaction", 1},

    auto_inc = {"Use AUTO_INCREMENT column as Primary Key (for MySQL), " ..
        "or its alternatives in other DBMS. When disabled, use " .. "client-generated IDs", true},
    skip_trx = {"Don't start explicit transactions and execute all queries " .. "in the AUTOCOMMIT mode", false},
    reconnect = {"Reconnect after every N events. The default (0) is to not reconnect", 0},
    mysql_storage_engine = {"Storage engine, if MySQL is used", "innodb"},
    pgsql_variant = {"Use this PostgreSQL variant when running with the " ..
        "PostgreSQL driver. The only currently supported " .. "variant is 'redshift'. When enabled, " ..
        "create_secondary is automatically disabled, and " .. "delete_inserts is set to 0"}
}

local t = sysbench.sql.type
local stmt_defs = {
    wiki_abstract = {
        one_word_match = {"SELECT * FROM wiki_abstract WHERE fts_match_word(?, abstract)", {t.CHAR, 50}},
        two_words_and_match = {"SELECT * FROM wiki_abstract WHERE fts_match_word(?, abstract) or fts_match_word(?, abstract)",
                               {t.CHAR, 50}, {t.CHAR, 50}},
        two_words_or_match = {"SELECT * FROM wiki_abstract WHERE fts_match_word(?, abstract) and fts_match_word(?, abstract)",
                              {t.CHAR, 50}, {t.CHAR, 50}},
        -- title must be fts index
        two_fields_word_and_match = {"SELECT * FROM wiki_abstract WHERE fts_match_word(?, abstract) and fts_match_word(?, title)",
                                     {t.CHAR, 50}, {t.CHAR, 50}},
        two_fields_word_or_match = {"SELECT * FROM wiki_abstract WHERE fts_match_word(?, abstract) and fts_match_word(?, title)",
                                    {t.CHAR, 50}, {t.CHAR, 50}},

        one_word_prefix_match = {"SELECT * FROM wiki_abstract WHERE fts_match_prefix(?, abstract)", {t.CHAR, 50}},
        two_words_and_prefix_match = {"SELECT * FROM wiki_abstract WHERE fts_match_prefix(?, abstract) or fts_match_prefix(?, abstract)",
                                      {t.CHAR, 50}, {t.CHAR, 50}},
        two_words_or_prefix_match = {"SELECT * FROM wiki_abstract WHERE fts_match_prefix(?, abstract) and fts_match_prefix(?, abstract)",
                                     {t.CHAR, 50}, {t.CHAR, 50}},
        -- title must be fts index
        two_fields_word_and_prefix_match = {"SELECT * FROM wiki_abstract WHERE fts_match_prefix(?, abstract) and fts_match_prefix(?, title)",
                                            {t.CHAR, 50}, {t.CHAR, 50}},
        two_fields_word_or_prefix_match = {"SELECT * FROM wiki_abstract WHERE fts_match_prefix(?, abstract) and fts_match_prefix(?, title)",
                                           {t.CHAR, 50}, {t.CHAR, 50}},

        mix_prefix_and_word_match = {"SELECT * FROM wiki_abstract WHERE fts_match_prefix(?, abstract) and fts_match_word(?, abstract)",
                                     {t.CHAR, 50}, {t.CHAR, 50}},
        mix_prefix_and_word_match2 = {"SELECT * FROM wiki_abstract WHERE fts_match_prefix(?, abstract) and fts_match_word(?, title)",
                                      {t.CHAR, 50}, {t.CHAR, 50}},
        mix_prefix_or_word_match = {"SELECT * FROM wiki_abstract WHERE fts_match_prefix(?, abstract) or fts_match_word(?, abstract)",
                                    {t.CHAR, 50}, {t.CHAR, 50}},
        mix_prefix_or_word_match2 = {"SELECT * FROM wiki_abstract WHERE fts_match_prefix(?, abstract) or fts_match_word(?, title)",
                                     {t.CHAR, 50}, {t.CHAR, 50}}

        -- TODO other select query types
        -- TODO insert/update/delete types
    },
    wiki_page = {
        one_word_match = {"SELECT * FROM wiki_page WHERE fts_match_word(?, `text`)", {t.CHAR, 50}},
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

        mix_prefix_and_word_match = {"SELECT * FROM wiki_page WHERE fts_match_prefix(?, `text`) and fts_match_word(?, `text`)",
                                     {t.CHAR, 50}, {t.CHAR, 50}},
        mix_prefix_and_word_match2 = {"SELECT * FROM wiki_page WHERE fts_match_prefix(?, `text`) and fts_match_word(?, `comment`)",
                                      {t.CHAR, 50}, {t.CHAR, 50}},
        mix_prefix_or_word_match = {"SELECT * FROM wiki_page WHERE fts_match_prefix(?, `text`) or fts_match_word(?, `text`)",
                                    {t.CHAR, 50}, {t.CHAR, 50}},
        mix_prefix_or_word_match2 = {"SELECT * FROM wiki_page WHERE fts_match_prefix(?, `text`) or fts_match_word(?, `comment`)",
                                     {t.CHAR, 50}, {t.CHAR, 50}}
        -- TODO other select query types
        -- TODO insert/update/delete types
    },
    amazon_review = {
        one_word_match = {"SELECT * FROM amazon_review WHERE fts_match_word(?, review_body)", {t.CHAR, 50}},
        two_words_and_match = {"SELECT * FROM amazon_review WHERE fts_match_word(?, review_body) or fts_match_word(?, review_body)",
                               {t.CHAR, 50}, {t.CHAR, 50}},
        two_words_or_match = {"SELECT * FROM amazon_review WHERE fts_match_word(?, review_body) and fts_match_word(?, review_body)",
                              {t.CHAR, 50}, {t.CHAR, 50}},
        -- review_headline must be fts index
        two_fields_word_and_match = {"SELECT * FROM amazon_review WHERE fts_match_word(?, review_body) and fts_match_word(?, review_headline)",
                                     {t.CHAR, 50}, {t.CHAR, 50}},
        two_fields_word_or_match = {"SELECT * FROM amazon_review WHERE fts_match_word(?, review_body) and fts_match_word(?, review_headline)",
                                    {t.CHAR, 50}, {t.CHAR, 50}},

        one_word_prefix_match = {"SELECT * FROM amazon_review WHERE fts_match_prefix(?, review_body)", {t.CHAR, 50}},
        two_words_and_prefix_match = {"SELECT * FROM amazon_review WHERE fts_match_prefix(?, review_body) or fts_match_prefix(?, review_body)",
                                      {t.CHAR, 50}, {t.CHAR, 50}},
        two_words_or_prefix_match = {"SELECT * FROM amazon_review WHERE fts_match_prefix(?, review_body) and fts_match_prefix(?, review_body)",
                                     {t.CHAR, 50}, {t.CHAR, 50}},
        -- review_headline must be fts index
        two_fields_word_and_prefix_match = {"SELECT * FROM amazon_review WHERE fts_match_prefix(?, review_body) and fts_match_prefix(?, review_headline)",
                                            {t.CHAR, 50}, {t.CHAR, 50}},
        two_fields_word_or_prefix_match = {"SELECT * FROM amazon_review WHERE fts_match_prefix(?, review_body) and fts_match_prefix(?, review_headline)",
                                           {t.CHAR, 50}, {t.CHAR, 50}},

        mix_prefix_and_word_match = {"SELECT * FROM amazon_review WHERE fts_match_prefix(?, review_body) and fts_match_word(?, review_headline)",
                                     {t.CHAR, 50}, {t.CHAR, 50}},
        mix_prefix_and_word_match2 = {"SELECT * FROM amazon_review WHERE fts_match_prefix(?, review_body) and fts_match_word(?, title)",
                                      {t.CHAR, 50}, {t.CHAR, 50}},
        mix_prefix_or_word_match = {"SELECT * FROM amazon_review WHERE fts_match_prefix(?, review_body) or fts_match_word(?, review_headline)",
                                    {t.CHAR, 50}, {t.CHAR, 50}},
        mix_prefix_or_word_match2 = {"SELECT * FROM amazon_review WHERE fts_match_prefix(?, review_body) or fts_match_word(?, title)",
                                     {t.CHAR, 50}, {t.CHAR, 50}}
        -- TODO other select query types
        -- TODO insert/update/delete types
    }
}

function prepare_begin()
    stmt.begin = con:prepare("BEGIN")
end

function prepare_commit()
    stmt.commit = con:prepare("COMMIT")
end

function prepare_for_stmts(key)
    local w = sysbench.opt.workload
    stmt[w][key] = con:prepare(stmt_defs[w][key][1])

    local nparam = #stmt_defs[w][key] - 1

    if nparam > 0 then
        param[w][key] = {}
    end

    for p = 1, nparam do
        local btype = stmt_defs[w][key][p + 1]
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

function prepare_one_word_match()
    prepare_for_stmts("one_word_match")
end

function prepare_two_words_and_match()
    prepare_for_stmts("two_words_and_match")
end

function prepare_two_words_or_match()
    prepare_for_stmts("two_words_or_match")
end

function prepare_two_fields_word_and_match()
    prepare_for_stmts("two_fields_word_and_match")
end

function prepare_two_fields_word_or_match()
    prepare_for_stmts("two_fields_word_or_match")
end

function prepare_one_word_prefix_match()
    prepare_for_stmts("one_word_prefix_match")
end

function prepare_two_words_and_prefix_match()
    prepare_for_stmts("two_words_and_prefix_match")
end

function prepare_two_words_or_prefix_match()
    prepare_for_stmts("two_words_or_prefix_match")
end

function prepare_two_fields_word_and_prefix_match()
    prepare_for_stmts("two_fields_word_and_prefix_match")
end

function prepare_two_fields_word_or_prefix_match()
    prepare_for_stmts("two_fields_word_or_prefix_match")
end

function prepare_mix_prefix_and_word_match()
    prepare_for_stmts("mix_prefix_and_word_match")
end

function prepare_mix_prefix_and_word_match2()
    prepare_for_stmts("mix_prefix_and_word_match2")
end

function prepare_mix_prefix_or_word_match()
    prepare_for_stmts("mix_prefix_or_word_match")
end

function prepare_mix_prefix_or_word_match2()
    prepare_for_stmts("mix_prefix_or_word_match2")
end

function prepare_delete_inserts()
    prepare_for_stmts("deletes")
    prepare_for_stmts("inserts")
end

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()

    -- Create global nested tables for prepared statements and their
    -- parameters. We need a statement and a parameter set for each combination
    -- of connection/table/query
    stmt = {}
    param = {}

    stmt[sysbench.opt.workload] = {}
    param[sysbench.opt.workload] = {}

    -- This function is a 'callback' defined by individual benchmark scripts
    prepare_statements()
end

-- Close prepared statements
function close_statements()
    for k, s in pairs(stmt[sysbench.opt.workload][t]) do
        stmt[sysbench.opt.workload][t][k]:close()
    end
    if (stmt.begin ~= nil) then
        stmt.begin:close()
    end
    if (stmt.commit ~= nil) then
        stmt.commit:close()
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
    local words = {"Familial", "Louis", "creation", "subdivision", "location", "Drill", "Bosonic", "birth", "founder",
                   "There", "next", "long", "athlete"}
    return words[sysbench.rand.default(1, #words)]
end

local function get_wiki_page_word()
    local words = {"history", "university", "development", "software", "system", "information", "government",
                   "research", "community", "including", "between", "based", "known", "local"}
    return words[sysbench.rand.default(1, #words)]
end

local function get_amazon_review_word()
    local words = {"excellent", "quality", "product", "work", "perfect", "easy", "recommend", "price", "value",
                   "comfortable", "use", "love", "fast", "great"}
    return words[sysbench.rand.default(1, #words)]
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

function begin()
    stmt.begin:execute()
end

function commit()
    stmt.commit:execute()
end

function execute_one_word_match()
    for i = 1, sysbench.opt.one_word_matchs do
        param[sysbench.opt.workload].one_word_match[1]:set(get_fts_word())
        stmt[sysbench.opt.workload].one_word_match:execute()
    end
end

function execute_two_words_and_match()
    for i = 1, sysbench.opt.two_words_and_matchs do
        param[sysbench.opt.workload].two_words_and_match[1]:set(get_fts_word())
        param[sysbench.opt.workload].two_words_and_match[2]:set(get_fts_word())
        stmt[sysbench.opt.workload].two_words_and_match:execute()
    end
end

function execute_two_words_or_match()
    for i = 1, sysbench.opt.two_words_or_matchs do
        param[sysbench.opt.workload].two_words_or_match[1]:set(get_fts_word())
        param[sysbench.opt.workload].two_words_or_match[2]:set(get_fts_word())
        stmt[sysbench.opt.workload].two_words_or_match:execute()
    end
end

function execute_two_fields_word_and_match()
    for i = 1, sysbench.opt.two_fields_word_and_matchs do
        param[sysbench.opt.workload].two_fields_word_and_match[1]:set(get_fts_word())
        param[sysbench.opt.workload].two_fields_word_and_match[2]:set(get_fts_word())
        stmt[sysbench.opt.workload].two_fields_word_and_match:execute()
    end
end

function execute_two_fields_word_or_match()
    for i = 1, sysbench.opt.two_fields_word_or_matchs do
        param[sysbench.opt.workload].two_fields_word_or_match[1]:set(get_fts_word())
        param[sysbench.opt.workload].two_fields_word_or_match[2]:set(get_fts_word())
        stmt[sysbench.opt.workload].two_fields_word_or_match:execute()
    end
end

function execute_one_word_prefix_match()
    for i = 1, sysbench.opt.one_word_prefix_matchs do
        param[sysbench.opt.workload].one_word_prefix_match[1]:set(get_fts_word())
        stmt[sysbench.opt.workload].one_word_prefix_match:execute()
    end
end

function execute_two_words_and_prefix_match()
    for i = 1, sysbench.opt.two_words_and_prefix_matchs do
        param[sysbench.opt.workload].two_words_and_prefix_match[1]:set(get_fts_word())
        param[sysbench.opt.workload].two_words_and_prefix_match[2]:set(get_fts_word())
        stmt[sysbench.opt.workload].two_words_and_prefix_match:execute()
    end
end

function execute_two_words_or_prefix_match()
    for i = 1, sysbench.opt.two_words_or_prefix_matchs do
        param[sysbench.opt.workload].two_words_or_prefix_match[1]:set(get_fts_word())
        param[sysbench.opt.workload].two_words_or_prefix_match[2]:set(get_fts_word())
        stmt[sysbench.opt.workload].two_words_or_prefix_match:execute()
    end
end

function execute_two_fields_word_and_prefix_match()
    for i = 1, sysbench.opt.two_fields_word_and_prefix_matchs do
        param[sysbench.opt.workload].two_fields_word_and_prefix_match[1]:set(get_fts_word())
        param[sysbench.opt.workload].two_fields_word_and_prefix_match[2]:set(get_fts_word())
        stmt[sysbench.opt.workload].two_fields_word_and_prefix_match:execute()
    end
end

function execute_two_fields_word_or_prefix_match()
    for i = 1, sysbench.opt.two_fields_word_or_prefix_matchs do
        param[sysbench.opt.workload].two_fields_word_or_prefix_match[1]:set(get_fts_word())
        param[sysbench.opt.workload].two_fields_word_or_prefix_match[2]:set(get_fts_word())
        stmt[sysbench.opt.workload].two_fields_word_or_prefix_match:execute()
    end
end

function execute_mix_prefix_and_word_match()
    for i = 1, sysbench.opt.mix_prefix_and_word_matchs do
        param[sysbench.opt.workload].mix_prefix_and_word_match[1]:set(get_fts_word())
        param[sysbench.opt.workload].mix_prefix_and_word_match[2]:set(get_fts_word())
        param[sysbench.opt.workload].mix_prefix_and_word_match2[1]:set(get_fts_word())
        param[sysbench.opt.workload].mix_prefix_and_word_match2[2]:set(get_fts_word())
        stmt[sysbench.opt.workload].mix_prefix_and_word_match:execute()
        stmt[sysbench.opt.workload].mix_prefix_and_word_match2:execute()
    end
end

function execute_mix_prefix_or_word_match()
    for i = 1, sysbench.opt.mix_prefix_or_word_matchs do
        param[sysbench.opt.workload].mix_prefix_or_word_match[1]:set(get_fts_word())
        param[sysbench.opt.workload].mix_prefix_or_word_match[2]:set(get_fts_word())
        param[sysbench.opt.workload].mix_prefix_or_word_match2[1]:set(get_fts_word())
        param[sysbench.opt.workload].mix_prefix_or_word_match2[2]:set(get_fts_word())
        stmt[sysbench.opt.workload].mix_prefix_or_word_match:execute()
        stmt[sysbench.opt.workload].mix_prefix_or_word_match2:execute()
    end
end

-- Re-prepare statements if we have reconnected, which is possible when some of
-- the listed error codes are in the --mysql-ignore-errors list
function sysbench.hooks.before_restart_event(errdesc)
    if errdesc.sql_errno == 2013 or -- CR_SERVER_LOST
    errdesc.sql_errno == 2055 or -- CR_SERVER_LOST_EXTENDED
    errdesc.sql_errno == 2006 or -- CR_SERVER_GONE_ERROR
    errdesc.sql_errno == 2011 -- CR_TCP_CONNECTION
    then
        close_statements()
        prepare_statements()
    end
end

function check_reconnect()
    if sysbench.opt.reconnect > 0 then
        transactions = (transactions or 0) + 1
        if transactions % sysbench.opt.reconnect == 0 then
            close_statements()
            con:reconnect()
            prepare_statements()
        end
    end
end
