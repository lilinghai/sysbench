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
    workload = {"Using one of the workloads [wiki_abstract,wiki_page,amazon_review]", "wiki_abstract"},
    ret_little_rows = {"return little rows(less than 1000) for fts_match_word query", true},
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

    source_files = {"Source csv files for insert", 1},

    auto_inc = {"Use AUTO_INCREMENT column as Primary Key (for MySQL), " ..
        "or its alternatives in other DBMS. When disabled, use " .. "client-generated IDs", true},
    skip_trx = {"Don't start explicit transactions and execute all queries " .. "in the AUTOCOMMIT mode", false},
    reconnect = {"Reconnect after every N events. The default (0) is to not reconnect", 0}
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
                                     {t.CHAR, 50}, {t.CHAR, 50}},

        -- TODO other select query types
        -- abstract,title,url
        update = {"UPDATE wiki_abstract SET abstract=?,title=?,url=? WHERE id=?", {t.CHAR, 2048}, {t.CHAR, 256},
                  {t.CHAR, 256}, t.INT},
        insert = {"INSERT INTO wiki_abstract (abstract,title,url) VALUES (?, ?, ?)", {t.CHAR, 2048}, {t.CHAR, 256},
                  {t.CHAR, 256}}
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
                                     {t.CHAR, 50}, {t.CHAR, 50}},

        -- TODO other select query types
        -- title,text,comment,username,timestamp
        update = {"UPDATE wiki_page SET title=?,`text`=?,`comment`=?,username=?,`timestamp`=? WHERE id=?",
                  {t.CHAR, 256}, {t.CHAR, 65532}, {t.CHAR, 256}, {t.CHAR, 256}, {t.CHAR, 128}, t.INT},
        insert = {"INSERT INTO wiki_page (title,`text`,`comment`,username,`timestamp`) (?, ?, ?, ?, ?)", {t.CHAR, 256},
                  {t.CHAR, 65532}, {t.CHAR, 256}, {t.CHAR, 256}, {t.CHAR, 128}}
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
                                     {t.CHAR, 50}, {t.CHAR, 50}},

        -- TODO other select query types
        -- review_date,marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body
        update = {"UPDATE amazon_review SET review_date=?,marketplace=?,customer_id=?,review_id=?,product_id=?,product_parent=?,product_title=?,product_category=?,star_rating=?,helpful_votes=?,total_votes=?,vine=?,verified_purchase=?,review_headline=?,review_body=? WHERE id=?",
                  t.INT, {t.CHAR, 20}, t.BIGINT, {t.CHAR, 40}, {t.CHAR, 20}, t.BIGINT, {t.CHAR, 500}, {t.CHAR, 50},
                  t.INT, t.INT, t.INT, t.INT, t.INT, {t.CHAR, 500}, {t.CHAR, 65532}, t.BIGINT},
        insert = {"INSERT INTO amazon_review (review_date,marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  t.INT, {t.CHAR, 20}, t.BIGINT, {t.CHAR, 40}, {t.CHAR, 20}, t.BIGINT, {t.CHAR, 500}, {t.CHAR, 50},
                  t.INT, t.INT, t.INT, t.INT, t.INT, {t.CHAR, 500}, {t.CHAR, 65532}}
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

function prepare_insert()
    prepare_for_stmts("insert")
end

function prepare_update()
    prepare_for_stmts("update")
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

    stmt = {}
    param = {}

    stmt[sysbench.opt.workload] = {}
    param[sysbench.opt.workload] = {}

    prepare_statements()
end

-- Close prepared statements
function close_statements()
    for k, s in pairs(stmt[sysbench.opt.workload]) do
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
    -- all rows is about 55 millions

    -- return large mounts of rows
    -- Familial 2300 rows
    -- practicing 4600 rows
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

    -- return small mounts of rows
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
    local words = {}
    if sysbench.opt.ret_little_rows then
        words = {"Chineses", "chinana", "usdollar", "Australi", "19300", "Philosoph", "Bosonic", "misunderstood",
                 "Panchatantra", "Rostropovich"}
    else
        words = {"Familial", "practicing", "Drill", "athlete", "location", "creation", "next", "long", "Louis",
                 "founder", "subdivision", "birth"}
    end
    return words[sysbench.rand.default(1, #words)]
end

local function get_wiki_page_word()
    local words = {"history", "university", "development", "software", "system", "information", "government",
                   "research", "community", "including", "between", "based", "known", "local"}
    return words[sysbench.rand.default(1, #words)]
end

local function get_amazon_review_word()
    -- return large mounts of rows
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

    -- return small mounts of rows
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
    local words = {}
    if sysbench.opt.ret_little_rows then
        words = {"lightweigh", "lighte", "Constructio", "Constru", "accom", "stainles", "tita", "NCJ-30", "NCJ",
                 "30000", "1000000", "2025", "Alleg", "Alle"}
    else
        words = {"excellent", "quality", "product", "work", "perfect", "easy", "recommend", "price", "value",
                 "comfortable", "use", "love", "fast", "great"}
    end

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

function execute_insert(row)
    if sysbench.opt.workload == "wiki_abstract" then
        -- abstract,title,url
        param[sysbench.opt.workload].insert[1]:set(row["abstract"])
        param[sysbench.opt.workload].insert[2]:set(row["title"])
        param[sysbench.opt.workload].insert[3]:set(row["url"])
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
    local update_id = update_ids[math.random(#update_ids)]
    update_id = tonumber(update_id)
    if sysbench.opt.workload == "wiki_abstract" then
        -- abstract,title,url
        param[sysbench.opt.workload].update[1]:set(row["abstract"])
        param[sysbench.opt.workload].update[2]:set(row["title"])
        param[sysbench.opt.workload].update[3]:set(row["url"])
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

function write(...)
    local handles = {...}
    if #handles == 0 then
        return
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
    local f = csv.open(file_name, {
        header = true
    })
    print("Thread ", sysbench.tid, " open csv file name: ", file_name)
    --  iter read for large file
    for r in f:lines() do
        for k, v in pairs(r) do
            v = string.gsub(v, "\\", "")
            v = string.gsub(v, "'", "")
            v = string.gsub(v, "([\"'])", "\\%1")
            r[k] = v
        end
        if not sysbench.opt.skip_trx then
            begin()
        end
        for i, handle in pairs(handles) do
            handle(r)
        end
        if not sysbench.opt.skip_trx then
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

function gen_random_update_ids()
    local drv = sysbench.sql.driver()
    local con = drv:connect()
    local rs = con:query(string.format("select id from %s order by rand() limit 1000000", sysbench.opt.workload))
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

    local file = io.open(sysbench.opt.workload .. ".ids.txt", "w")
    if file then
        file:write(table.concat(values, "\n"))
        file:close()
    else
        error("can't open file: " .. sysbench.opt.workload .. ".ids.txt")
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
