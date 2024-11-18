-- Copyright (C) 2006-2018 Alexey Kopytov <akopytov@gmail.com>
-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2 of the License, or
-- (at your option) any later version.
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
-- -----------------------------------------------------------------------------
-- Common code for OLTP benchmarks.
-- -----------------------------------------------------------------------------
function init()
    assert(event ~= nil,
        "this script is meant to be included by other OLTP scripts and " .. "should not be called directly.")
    dml_tables = math.floor(sysbench.opt.dbs * sysbench.opt.tables * sysbench.opt.dml_percentage)
    print("dml tables", dml_tables)
end

if sysbench.cmdline.command == nil then
    error("Command is required. Supported commands: prepareuser, rotateuser, preparedb, preparetable, analyze, run, " ..
              "cleanup, help")
end

-- Command line options
sysbench.cmdline.options = {
    db_prefix = {"Database name prefix", "sbtest"},
    dbs = {"Number of databases", 1},
    dml_percentage = {"DML on percentage of all tables", 0.1},
    user_batch = {"Number of Alter user", 1},
    partition_table_ratio = {"Ratio of partition table", 0},
    partition_type = {"Type of partition. The value can be one of [range,list,hash]", 1},
    partitions_per_table = {"Number of partitions per db", 10},
    table_size = {"Number of rows per table", 10000},
    range_size = {"Range size for range SELECT queries", 100},
    tables = {"Number of tables per db", 1},
    point_selects = {"Number of point SELECT queries per transaction", 10},
    simple_ranges = {"Number of simple range SELECT queries per transaction", 1},
    sum_ranges = {"Number of SELECT SUM() queries per transaction", 1},
    order_ranges = {"Number of SELECT ORDER BY queries per transaction", 1},
    distinct_ranges = {"Number of SELECT DISTINCT queries per transaction", 1},
    index_updates = {"Number of UPDATE index queries per transaction", 1},
    non_index_updates = {"Number of UPDATE non-index queries per transaction", 1},
    delete_inserts = {"Number of DELETE/INSERT combinations per transaction", 1},
    range_selects = {"Enable/disable all range SELECT queries", true},
    auto_inc = {"Use AUTO_INCREMENT column as Primary Key (for MySQL), " ..
        "or its alternatives in other DBMS. When disabled, use " .. "client-generated IDs", true},
    create_table_options = {"Extra CREATE TABLE options", ""},
    skip_trx = {"Don't start explicit transactions and execute all queries " .. "in the AUTOCOMMIT mode", false},
    secondary = {"Use a secondary index in place of the PRIMARY KEY", false},
    create_secondary = {"Create a secondary index in addition to the PRIMARY KEY", true},
    reconnect = {"Reconnect after every N events. The default (0) is to not reconnect", 0},
    mysql_storage_engine = {"Storage engine, if MySQL is used", "innodb"},
    pgsql_variant = {"Use this PostgreSQL variant when running with the " ..
        "PostgreSQL driver. The only currently supported " .. "variant is 'redshift'. When enabled, " ..
        "create_secondary is automatically disabled, and " .. "delete_inserts is set to 0"}
}

function cmd_prepare_db()
    local drv = sysbench.sql.driver()
    local con = drv:connect()

    for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.dbs, sysbench.opt.threads do
        create_database(con, i)
    end
end

-- Prepare the dataset. This command supports parallel execution, i.e. will
-- benefit from executing with --threads > 1 as long as --tables > 1
function cmd_prepare_table()
    local drv = sysbench.sql.driver()
    local con = drv:connect()

    local tables = sysbench.opt.dbs * sysbench.opt.tables

    for i = sysbench.tid % sysbench.opt.threads + 1, tables, sysbench.opt.threads do
        create_table(drv, con, i)
    end
end

function cmd_analyze()
    local drv = sysbench.sql.driver()
    local con = drv:connect()

    local tables = sysbench.opt.dbs * sysbench.opt.tables

    for i = sysbench.tid % sysbench.opt.threads + 1, tables, sysbench.opt.threads do
        local db_num, table_num_in_db = get_db_table_num(i)
        local table_name = string.format("%s%d.sbtest%d", sysbench.opt.db_prefix, db_num, table_num_in_db)
        print(string.format("Analyzing table %s ...", table_name))
        con:query("ANALYZE TABLE " .. table_name)
    end
end

function cmd_cleanup()
    local drv = sysbench.sql.driver()
    local con = drv:connect()

    for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.dbs, sysbench.opt.threads do
        print(string.format("Droping database '%s%d'...", sysbench.opt.db_prefix, i))
        con:query("DROP database IF EXISTS " .. sysbench.opt.db_prefix .. i)
    end
end

function cmd_prepare_user()
    local drv = sysbench.sql.driver()
    local con = drv:connect()

    for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.dbs, sysbench.opt.threads do
        create_user(con, i)
    end

end

function cmd_rotate_user()
    local drv = sysbench.sql.driver()
    local con = drv:connect()
    local step = 0
    local user_nums = {}

    for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.dbs, sysbench.opt.threads do
        step = step + 1
        user_nums[step] = i
        if step % sysbench.opt.user_batch == 0 then
            rotate_user(con, user_nums)
            user_nums = {}
            step = 0
        end
    end

    rotate_user(con, user_nums)
end

-- Implement parallel commands
sysbench.cmdline.commands = {
    prepareuser = {cmd_prepare_user, sysbench.cmdline.PARALLEL_COMMAND},
    rotateuser = {cmd_rotate_user, sysbench.cmdline.PARALLEL_COMMAND},
    preparedb = {cmd_prepare_db, sysbench.cmdline.PARALLEL_COMMAND},
    preparetable = {cmd_prepare_table, sysbench.cmdline.PARALLEL_COMMAND},
    analyzetable = {cmd_analyze, sysbench.cmdline.PARALLEL_COMMAND},
    cleanup = {cmd_cleanup, sysbench.cmdline.PARALLEL_COMMAND}
}

-- Template strings of random digits with 11-digit groups separated by dashes

-- 10 groups, 119 characters
local c_value_template = "###########-###########-###########-" .. "###########-###########-###########-" ..
                             "###########-###########-###########-" .. "###########"

-- 5 groups, 59 characters
local pad_value_template = "###########-###########-###########-" .. "###########-###########"

function get_c_value()
    return sysbench.rand.string(c_value_template)
end

function get_pad_value()
    return sysbench.rand.string(pad_value_template)
end

function rotate_user(con, user_nums)
    if #user_nums == 0 then
        return
    end
    local query1 = "alter user "
    local query2 = "alter user "
    for i = 1, #user_nums do
        local user_num = user_nums[i]
        if i == #user_nums then
            query1 = query1 ..
                         string.format("%s%d IDENTIFIED BY '%s%dp3'", sysbench.opt.db_prefix, user_num,
                    sysbench.opt.db_prefix, user_num)
            query2 = query2 ..
                         string.format("%s%du2 IDENTIFIED BY '%s%du2p3'", sysbench.opt.db_prefix, user_num,
                    sysbench.opt.db_prefix, user_num)
        else
            query1 = query1 ..
                         string.format("%s%d IDENTIFIED BY '%s%dp3',", sysbench.opt.db_prefix, user_num,
                    sysbench.opt.db_prefix, user_num)
            query2 = query2 ..
                         string.format("%s%du2 IDENTIFIED BY '%s%du2p3',", sysbench.opt.db_prefix, user_num,
                    sysbench.opt.db_prefix, user_num)
        end
    end

    print(string.format("Rotating user count %d ...", #user_nums))
    con:query(query1)
    print(string.format("Rotating user count %d ...", #user_nums))
    con:query(query2)
end

function create_user(con, user_num)
    print(string.format("Creating user '%s%d'...", sysbench.opt.db_prefix, user_num))
    local query = string.format([[
        create user IF NOT EXISTS %s%d IDENTIFIED BY '%s%d'
    ]], sysbench.opt.db_prefix, user_num, sysbench.opt.db_prefix, user_num)
    con:query(query)

    local query = string.format([[
        grant all privileges on %s%d.* to '%s%d'
    ]], sysbench.opt.db_prefix, user_num, sysbench.opt.db_prefix, user_num)
    con:query(query)

    print(string.format("Creating user '%s%du2'...", sysbench.opt.db_prefix, user_num))
    local query = string.format([[
        create user IF NOT EXISTS %s%du2 IDENTIFIED BY '%s%du2'
    ]], sysbench.opt.db_prefix, user_num, sysbench.opt.db_prefix, user_num)
    con:query(query)

    local query = string.format([[
        grant all privileges on %s%d.* to '%s%du2'
    ]], sysbench.opt.db_prefix, user_num, sysbench.opt.db_prefix, user_num)
    con:query(query)
end

function create_database(con, db_num)
    print(string.format("Creating database '%s%d'...", sysbench.opt.db_prefix, db_num))
    local query = string.format([[
        create database %s%d
    ]], sysbench.opt.db_prefix, db_num)
    con:query(query)
end

function create_table(drv, con, table_num)
    local id_index_def, id_def
    local engine_def = ""
    local extra_table_options = ""
    local query

    local db_num, table_num_in_db = get_db_table_num(table_num)
    -- print("debug number", table_num, db_num, table_num_in_db)
    local table_name = string.format("%s%d.sbtest%d", sysbench.opt.db_prefix, db_num, table_num_in_db)

    if sysbench.opt.secondary then
        id_index_def = "KEY xid"
    else
        id_index_def = "PRIMARY KEY"
    end

    if drv:name() == "mysql" then
        if sysbench.opt.auto_inc then
            id_def = "INTEGER NOT NULL AUTO_INCREMENT"
        else
            id_def = "INTEGER NOT NULL"
        end
        engine_def = "/*! ENGINE = " .. sysbench.opt.mysql_storage_engine .. " */"
    elseif drv:name() == "pgsql" then
        if not sysbench.opt.auto_inc then
            id_def = "INTEGER NOT NULL"
        elseif pgsql_variant == 'redshift' then
            id_def = "INTEGER IDENTITY(1,1)"
        else
            id_def = "SERIAL"
        end
    else
        error("Unsupported database driver:" .. drv:name())
    end

    print(string.format("Creating table %s ...", table_name))

    local partition_column = ""
    local partition_desc = ""
    local partition_column_name = ""

    if is_partition_table(table_num) then
        local p_id_max_value = sysbench.opt.partitions_per_table * 10
        partition_column = "p_id INTEGER"
        partition_column_name = ",p_id"
        if sysbench.opt.partition_type == "range" then
            partition_desc = "PARTITION BY RANGE (p_id) ( "
            for i = 1, sysbench.opt.partitions_per_table - 1 do
                partition_desc = partition_desc ..
                                     string.format("PARTITION p%d VALUES LESS THAN (%d),", i,
                        p_id_max_value % sysbench.opt.partitions_per_table * i)
            end
            partition_desc = partition_desc ..
                                 string.format("PARTITION p%d VALUES LESS THAN (%d) )", i, p_id_max_value + 1)

        elseif sysbench.opt.partition_type == "list" then
            partition_desc = "PARTITION BY LIST (p_id) ("
            for i = 1, sysbench.opt.partitions_per_table - 1 do
                partition_desc = partition_desc ..
                                     string.format("PARTITION p%d VALUES IN %s ,", i,
                        get_in_list_condition(1 + (i - 1) * 10, i * 10))
            end

            partition_desc = partition_desc ..
                                 string.format("PARTITION p%d VALUES IN %s )", i,
                    get_in_list_condition(1 + (sysbench.opt.partitions_per_table - 1) * 10,
                        sysbench.opt.partitions_per_table * 10))
        elseif sysbench.opt.partition_type == "hash" then
            partition_desc = "PARTITION BY HASH(p_id) PARTITIONS " .. sysbench.opt.partitions_per_table
        end
    end

    query = string.format([[
CREATE TABLE %s(
  id %s,
  k INTEGER DEFAULT '0' NOT NULL,
  c CHAR(120) DEFAULT '' NOT NULL,
  pad CHAR(60) DEFAULT '' NOT NULL,
  %s,
  INDEX k_%d(k),
  %s (id)
) %s %s %s]], table_name, id_def, partition_column, table_num_in_db, id_index_def, partition_desc, engine_def,
        sysbench.opt.create_table_options)

    con:query(query)

    if (sysbench.opt.table_size > 0) then
        print(string.format("Inserting %d records into '%s%d.sbtest%d'", sysbench.opt.table_size,
            sysbench.opt.db_prefix, db_num, table_num_in_db))
    end

    if sysbench.opt.auto_inc then
        query = "INSERT INTO " .. table_name .. string.format("(k, c, pad %s) VALUES", partition_column_name)
    else
        query = "INSERT INTO " .. table_name .. string.format("(id, k, c, pad %s) VALUES", partition_column_name)
    end

    con:bulk_insert_init(query)

    local c_val
    local pad_val

    for i = 1, sysbench.opt.table_size do

        c_val = get_c_value()
        pad_val = get_pad_value()

        if (sysbench.opt.auto_inc) then
            query = string.format("(%d, '%s', '%s')", sysbench.rand.default(1, sysbench.opt.table_size), c_val, pad_val)
        else
            query = string.format("(%d, %d, '%s', '%s')", i, sysbench.rand.default(1, sysbench.opt.table_size), c_val,
                pad_val)
        end

        con:bulk_insert_next(query)
    end

    con:bulk_insert_done()
end

local t = sysbench.sql.type
local stmt_defs = {
    point_selects = {"SELECT c FROM %s%d.sbtest%u WHERE id=?", t.INT},
    simple_ranges = {"SELECT c FROM %s%d.sbtest%u WHERE id BETWEEN ? AND ?", t.INT, t.INT},
    sum_ranges = {"SELECT SUM(k) FROM %s%d.sbtest%u WHERE id BETWEEN ? AND ?", t.INT, t.INT},
    order_ranges = {"SELECT c FROM %s%d.sbtest%u WHERE id BETWEEN ? AND ? ORDER BY c", t.INT, t.INT},
    distinct_ranges = {"SELECT DISTINCT c FROM %s%d.sbtest%u WHERE id BETWEEN ? AND ? ORDER BY c", t.INT, t.INT},
    index_updates = {"UPDATE %s%d.sbtest%u SET k=k+1 WHERE id=?", t.INT},
    non_index_updates = {"UPDATE %s%d.sbtest%u SET c=? WHERE id=?", {t.CHAR, 120}, t.INT},
    deletes = {"DELETE FROM %s%d.sbtest%u WHERE id=?", t.INT},
    inserts = {"INSERT INTO %s%d.sbtest%u (id, k, c, pad) VALUES (?, ?, ?, ?)", t.INT, t.INT, {t.CHAR, 120},
               {t.CHAR, 60}}
}

function prepare_begin()
    stmt.begin = con:prepare("BEGIN")
end

function prepare_commit()
    stmt.commit = con:prepare("COMMIT")
end

-- 每个线程只 prepare 自己相关的 tables，避免 prepare 过多占用大量的内存
-- todo 可以使用的时候 prepare, 避免 init 时候 prepare 耗时过长
function prepare_for_each_table(key)
    -- print("stmt len", #stmt)
    for t = 1, #stmt do
        tn = get_table_num(t)
        local db_num, table_num_in_db = get_db_table_num(tn)
        stmt[t][key] = con:prepare(string.format(stmt_defs[key][1], sysbench.opt.db_prefix, db_num, table_num_in_db))

        local nparam = #stmt_defs[key] - 1

        if nparam > 0 then
            param[t][key] = {}
        end

        for p = 1, nparam do
            local btype = stmt_defs[key][p + 1]
            local len

            if type(btype) == "table" then
                len = btype[2]
                btype = btype[1]
            end
            if btype == sysbench.sql.type.VARCHAR or btype == sysbench.sql.type.CHAR then
                param[t][key][p] = stmt[t][key]:bind_create(btype, len)
            else
                param[t][key][p] = stmt[t][key]:bind_create(btype)
            end
        end

        if nparam > 0 then
            stmt[t][key]:bind_param(unpack(param[t][key]))
        end
    end
end

function prepare_point_selects()
    prepare_for_each_table("point_selects")
end

function prepare_simple_ranges()
    prepare_for_each_table("simple_ranges")
end

function prepare_sum_ranges()
    prepare_for_each_table("sum_ranges")
end

function prepare_order_ranges()
    prepare_for_each_table("order_ranges")
end

function prepare_distinct_ranges()
    prepare_for_each_table("distinct_ranges")
end

function prepare_index_updates()
    prepare_for_each_table("index_updates")
end

function prepare_non_index_updates()
    prepare_for_each_table("non_index_updates")
end

function prepare_delete_inserts()
    prepare_for_each_table("deletes")
    prepare_for_each_table("inserts")
end

-- 1,5 -> (1,2,3,4,5)
function get_in_list_condition(si, ei)
    local res = "("
    for i = si, ei - 1 do
        res = res .. i .. ","
    end
    res = res .. ei .. ")"
end

function is_partition_table(table_num)
    if table_num <= math.floor(sysbench.opt.partition_table_ratio * sysbench.opt.dbs * sysbench.opt.tables) then
        return true
    end
    return false
end

function get_table_num(j)
    -- (j-1) * sysbench.opt.threads + sysbench.tid % sysbench.opt.threads + 1 => i
    -- such as threads=100
    -- 1,101,201,301 ... => 1,2,3,4 ... , sysbench.tid=0
    -- 2,102,202,302 ... => 1,2,3,4 ... , sysbench.tid=1
    return (j - 1) * sysbench.opt.threads + sysbench.tid % sysbench.opt.threads + 1
end

-- 同一个 db_num 只包含一段连续的 table_num
-- prepare 和 run 保持一致，方便 workload prepare 中断后重新开始
function get_db_table_num(table_num)
    local db_num = math.floor((table_num - 1) / sysbench.opt.tables) + 1
    local table_num_in_db = (table_num - 1) % sysbench.opt.tables + 1
    return db_num, table_num_in_db
end

function thread_init()
    -- print("thread_id", sysbench.tid)
    drv = sysbench.sql.driver()
    con = drv:connect()

    -- Create global nested tables for prepared statements and their
    -- parameters. We need a statement and a parameter set for each combination
    -- of connection/table/query
    stmt = {}
    param = {}
    dml_tables = math.floor(sysbench.opt.dbs * sysbench.opt.tables * sysbench.opt.dml_percentage)

    local j = 1
    for i = sysbench.tid % sysbench.opt.threads + 1, dml_tables, sysbench.opt.threads do
        stmt[j] = {}
        param[j] = {}
        j = j + 1
    end

    -- This function is a 'callback' defined by individual benchmark scripts
    prepare_statements()
end

-- Close prepared statements
function close_statements()
    for t = 1, #stmt do
        for k, s in pairs(stmt[t]) do
            stmt[t][k]:close()
        end
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

local function get_stmt_num()
    return sysbench.rand.uniform(1, #stmt)
end

local function get_id()
    return sysbench.rand.default(1, sysbench.opt.table_size)
end

function begin()
    stmt.begin:execute()
end

function commit()
    stmt.commit:execute()
end

function execute_point_selects()
    local tnum = get_stmt_num()
    local i

    for i = 1, sysbench.opt.point_selects do
        param[tnum].point_selects[1]:set(get_id())

        stmt[tnum].point_selects:execute()
    end
end

local function execute_range(key)
    local tnum = get_stmt_num()

    for i = 1, sysbench.opt[key] do
        local id = get_id()

        param[tnum][key][1]:set(id)
        param[tnum][key][2]:set(id + sysbench.opt.range_size - 1)

        stmt[tnum][key]:execute()
    end
end

function execute_simple_ranges()
    execute_range("simple_ranges")
end

function execute_sum_ranges()
    execute_range("sum_ranges")
end

function execute_order_ranges()
    execute_range("order_ranges")
end

function execute_distinct_ranges()
    execute_range("distinct_ranges")
end

function execute_index_updates()
    local tnum = get_stmt_num()

    for i = 1, sysbench.opt.index_updates do
        param[tnum].index_updates[1]:set(get_id())

        stmt[tnum].index_updates:execute()
    end
end

function execute_non_index_updates()
    local tnum = get_stmt_num()

    for i = 1, sysbench.opt.non_index_updates do
        param[tnum].non_index_updates[1]:set_rand_str(c_value_template)
        param[tnum].non_index_updates[2]:set(get_id())

        stmt[tnum].non_index_updates:execute()
    end
end

function execute_delete_inserts()
    local tnum = get_stmt_num()

    for i = 1, sysbench.opt.delete_inserts do
        local id = get_id()
        local k = get_id()

        param[tnum].deletes[1]:set(id)

        param[tnum].inserts[1]:set(id)
        param[tnum].inserts[2]:set(k)
        param[tnum].inserts[3]:set_rand_str(c_value_template)
        param[tnum].inserts[4]:set_rand_str(pad_value_template)

        stmt[tnum].deletes:execute()
        stmt[tnum].inserts:execute()
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
