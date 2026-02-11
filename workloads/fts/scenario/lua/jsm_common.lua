#!/usr/bin/env sysbench

function init()
    assert(event ~= nil,
        "this script is meant to be included by other lua scripts and should not be called directly.")
end

if sysbench.cmdline.command == nil then
    error("Command is required. Supported commands: prepare, run, help")
end

sysbench.cmdline.options = {
    obj_table = {"Object table name", "obj_new"},
    rel_table = {"Relationship table name", "obj_relationship_new"},
    obj_rows = {"Number of rows for obj_new in prepare", 100000},
    text_value_cols = {"Number of text_value columns to populate (1-15)", 15},
    text_value_len = {"Length of each text_value column", 32},
    label_prefix = {"Label prefix for generated rows", "asset"},
    workspace_ids = {"Comma-separated workspace_id list", "aa01e3d3-0423-4614-8004-206989601265"},
    workspace_count = {"Number of generated workspace_ids when workspace_ids is empty", 1},
    obj_type_ids = {
        "Comma-separated obj_type_id list",
        "21bcdd9b-5bed-4ead-9a2c-778a6cf60d0b,3307bd5f-0564-4aea-807f-10b71c936cb8,770e0734-c440-47b0-90de-6abd76ec9fe2,9639c0b6-eb74-4d4d-96d3-ee562099d1f0,b95ee1c2-f117-4f9c-8dd7-0473a70d3237"
    },
    select_weight = {"Weight for select operations", 8},
    insert_weight = {"Weight for insert operations", 1},
    update_weight = {"Weight for update operations", 1},
    rel_update_weight = {"Weight for relationship update operations", 1},
    ops_per_txn = {"Operations per transaction", 10},
    update_text_cols = {"Number of text_value columns to update", 3},
    select_limit_min = {"Minimum LIMIT for SELECT", 0},
    select_limit_max = {"Maximum LIMIT for SELECT", 100000},
    select_offset_max = {"Maximum OFFSET for SELECT", 100000},
    match_len = {"Maximum length for MATCH query string", 128},
    
    skip_trx = {"Don't start explicit transactions and execute all queries in AUTOCOMMIT mode", false},
    reconnect = {"Reconnect after every N events. The default (0) is to not reconnect", 0}
}

local t = sysbench.sql.type
local stmt_defs = nil
local stmt = nil
local param = nil
local drv = nil
local con = nil

local workspace_ids = {}
local obj_type_ids = {}
local match_builders = {}
local seq_counters = {}

local data_ready = false
local text_value_cols = 15
local update_text_cols = 3
local rel_per_obj_min = 10
local rel_per_obj_max = 20
local prepared_rel_rows = 0
local label_len = 32

local next_obj_id = 0
local next_rel_id = 0

local select_weight = 0
local insert_weight = 0
local update_weight = 0
local rel_update_weight = 0
local total_weight = 0

local default_obj_type_ids = {
    "21bcdd9b-5bed-4ead-9a2c-778a6cf60d0b",
    "3307bd5f-0564-4aea-807f-10b71c936cb8",
    "770e0734-c440-47b0-90de-6abd76ec9fe2",
    "9639c0b6-eb74-4d4d-96d3-ee562099d1f0",
    "b95ee1c2-f117-4f9c-8dd7-0473a70d3237"
}

local default_fts_terms = {
    "Siemens",
    "China",
    "US",
    "apple",
    "Chine",
    "Jira",
    "Assets",
    "Service",
    "Management",
    "Server",
    "Laptop",
    "Firewall",
    "Router",
    "Switch",
    "Datacenter",
    "Cloud",
    "AWS",
    "Azure",
    "GCP",
    "Ticket",
    "Incident",
    "Change",
    "Problem",
    "Hardware",
    "Software",
    "License",
    "Warranty",
    "Vendor",
    "Siemens AG",
    "Siemens US",
    "Siemens CN",
    "Apple",
    "Dell",
    "HP",
    "Lenovo",
    "Cisco",
    "Juniper"
}

local fts_terms = default_fts_terms

local function trim(value)
    return (value:gsub("^%s+", ""):gsub("%s+$", ""))
end

local function split_csv(value)
    local items = {}
    if value == nil then
        return items
    end
    for item in string.gmatch(value, "([^,]+)") do
        local trimmed = trim(item)
        if trimmed ~= "" then
            table.insert(items, trimmed)
        end
    end
    return items
end

local function uuid_from_int(n)
    if n < 0 then
        n = -n
    end
    local high = math.floor(n / 0x1000000000000)
    local low = n % 0x1000000000000
    return string.format("%08x-0000-0000-0000-%012x", high, low)
end

local function quote(value)
    if value == nil then
        return "NULL"
    end
    value = string.gsub(value, "'", "''")
    return "'" .. value .. "'"
end

local function random_ascii(len)
    if len <= 0 then
        return ""
    end
    return sysbench.rand.string(string.rep("@", len))
end

local function random_term()
    return fts_terms[sysbench.rand.default(1, #fts_terms)]
end

local function build_text_value(term, len)
    if len <= 0 then
        return ""
    end
    if #term >= len then
        return string.sub(term, 1, len)
    end
    local filler_len = len - #term - 1
    if filler_len <= 0 then
        return term
    end
    return term .. " " .. random_ascii(filler_len)
end

local function build_text_values(row_num, use_random)
    local values = {}
    for i = 1, text_value_cols do
        local term
        if use_random then
            term = random_term()
        else
            term = fts_terms[((row_num + i - 1) % #fts_terms) + 1]
        end
        values[i] = build_text_value(term, sysbench.opt.text_value_len)
    end
    return values
end

local function build_label(row_num)
    local label = sysbench.opt.label_prefix .. "_" .. tostring(row_num)
    if #label > label_len then
        return string.sub(label, 1, label_len)
    end
    return label
end

local function build_schema_key(schema_id)
    local key = "schema_" .. string.gsub(schema_id, "-", "")
    if #key > 32 then
        return string.sub(key, 1, 32)
    end
    return key
end

local function workspace_index_for_row(row_num)
    return ((row_num - 1) % #workspace_ids) + 1
end

local function obj_type_index_for_row(row_num)
    return ((row_num - 1) % #obj_type_ids) + 1
end

local function workspace_row_count(ws_index)
    local total = sysbench.opt.obj_rows
    if total <= 0 then
        return 0
    end
    local count = math.floor(total / #workspace_ids)
    local remainder = total % #workspace_ids
    if ws_index <= remainder then
        count = count + 1
    end
    return count
end

local function random_obj_row_for_workspace(ws_index)
    local count = workspace_row_count(ws_index)
    if count <= 0 then
        return ws_index
    end
    local offset = sysbench.rand.default(0, count - 1)
    return offset * #workspace_ids + ws_index
end

local function workspace_id_for_row(row_num)
    return workspace_ids[workspace_index_for_row(row_num)]
end

local function obj_type_id_for_row(row_num)
    return obj_type_ids[obj_type_index_for_row(row_num)]
end

local function rand_between(min_val, max_val)
    if max_val < min_val then
        return min_val
    end
    if max_val == min_val then
        return min_val
    end
    return sysbench.rand.default(min_val, max_val)
end

local function rel_count_for_obj(obj_id)
    local span = rel_per_obj_max - rel_per_obj_min + 1
    return (obj_id % span) + rel_per_obj_min
end

local function rel_total_for_range(start_obj, end_obj)
    if end_obj < start_obj then
        return 0
    end
    local total = 0
    for i = start_obj, end_obj do
        total = total + rel_count_for_obj(i)
    end
    return total
end

local function relationships_before_object(obj_index)
    if obj_index <= 1 then
        return 0
    end
    return rel_total_for_range(1, obj_index - 1)
end

local function init_data_lists()
    if data_ready then
        return
    end

    workspace_ids = split_csv(sysbench.opt.workspace_ids)
    if #workspace_ids == 0 then
        local count = tonumber(sysbench.opt.workspace_count) or 1
        if sysbench.opt.obj_rows > 0 and count > sysbench.opt.obj_rows then
            count = sysbench.opt.obj_rows
        end
        if count < 1 then
            count = 1
        end
        workspace_ids = {}
        local base = 1000000
        for i = 1, count do
            workspace_ids[i] = uuid_from_int(base + i)
        end
    end

    obj_type_ids = split_csv(sysbench.opt.obj_type_ids)
    if #obj_type_ids == 0 then
        obj_type_ids = default_obj_type_ids
    end

    text_value_cols = math.min(15, math.max(1, sysbench.opt.text_value_cols))
    update_text_cols = math.min(text_value_cols, math.max(1, sysbench.opt.update_text_cols))
    prepared_rel_rows = rel_total_for_range(1, sysbench.opt.obj_rows)

    match_builders = {
        function()
            return random_term()
        end,
        function()
            local t1 = random_term()
            local t2 = random_term()
            return "+" .. t1 .. " -" .. t2
        end,
        function()
            local t1 = random_term()
            local t2 = random_term()
            return "\"" .. t1 .. " " .. t2 .. "\""
        end,
        function()
            local term = random_term()
            local prefix = term
            if #term > 3 then
                prefix = string.sub(term, 1, #term - 1)
            end
            return "+" .. prefix .. "* +" .. random_term()
        end,
        function(workspace_id)
            return "+\"" .. workspace_id .. "\" +" .. random_term()
        end,
        function()
            return "-" .. random_term()
        end
    }

    data_ready = true
end

local function build_stmt_defs()
    local obj_table = sysbench.opt.obj_table
    local rel_table = sysbench.opt.rel_table

    local in_placeholders = {}
    for _ = 1, 5 do
        table.insert(in_placeholders, "UUID_TO_BIN(?)")
    end

    local match_columns = {"o1.workspace_id"}
    for i = 1, 15 do
        table.insert(match_columns, "o1.text_value_" .. i)
    end

    local select_sql = string.format([[
SELECT o.sequential_id, o.label
FROM %s o
INNER JOIN %s r ON r.referenced_object_id = o.id
INNER JOIN %s o1 ON o1.id = r.object_id
WHERE o.workspace_id = ?
  AND o1.workspace_id = ?
  AND o.obj_type_id IN (%s)
  AND o1.obj_type_id IN (%s)
  AND MATCH (%s) AGAINST (? IN BOOLEAN MODE)
ORDER BY o.label ASC
LIMIT ? OFFSET ?]],
        obj_table,
        rel_table,
        obj_table,
        table.concat(in_placeholders, ", "),
        table.concat(in_placeholders, ", "),
        table.concat(match_columns, ", "))

    local select_def = {select_sql}
    table.insert(select_def, {t.CHAR, 36})
    table.insert(select_def, {t.CHAR, 36})
    for _ = 1, 10 do
        table.insert(select_def, {t.CHAR, 36})
    end
    table.insert(select_def, {t.CHAR, sysbench.opt.match_len})
    table.insert(select_def, t.INT)
    table.insert(select_def, t.INT)

    local obj_cols = {"id", "workspace_id", "sequential_id", "label", "obj_type_id", "schema_id", "schema_key"}
    local obj_vals = {"UUID_TO_BIN(?)", "?", "?", "?", "UUID_TO_BIN(?)", "UUID_TO_BIN(?)", "?"}
    for i = 1, text_value_cols do
        table.insert(obj_cols, "text_value_" .. i)
        table.insert(obj_vals, "?")
    end
    local insert_obj_sql = string.format("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE id=id",
        obj_table, table.concat(obj_cols, ", "), table.concat(obj_vals, ", "))

    local insert_obj_def = {insert_obj_sql}
    table.insert(insert_obj_def, {t.CHAR, 36})
    table.insert(insert_obj_def, {t.CHAR, 36})
    table.insert(insert_obj_def, t.BIGINT)
    table.insert(insert_obj_def, {t.CHAR, label_len})
    table.insert(insert_obj_def, {t.CHAR, 36})
    table.insert(insert_obj_def, {t.CHAR, 36})
    table.insert(insert_obj_def, {t.CHAR, 32})
    for _ = 1, text_value_cols do
        table.insert(insert_obj_def, {t.CHAR, sysbench.opt.text_value_len})
    end

    local insert_rel_sql = string.format(
        "INSERT INTO %s (id, workspace_id, object_id, referenced_object_id, object_type_attribute_id, object_type_id, referenced_object_type_id) VALUES (UUID_TO_BIN(?), ?, UUID_TO_BIN(?), UUID_TO_BIN(?), UUID_TO_BIN(?), UUID_TO_BIN(?), UUID_TO_BIN(?)) ON DUPLICATE KEY UPDATE id=id",
        rel_table)
    local insert_rel_def = {insert_rel_sql}
    for _ = 1, 7 do
        table.insert(insert_rel_def, {t.CHAR, 36})
    end

    local update_cols = {"label"}
    for i = 1, update_text_cols do
        table.insert(update_cols, "text_value_" .. i)
    end
    local update_sets = {}
    for _, col in ipairs(update_cols) do
        table.insert(update_sets, col .. "=?")
    end
    local update_sql = string.format("UPDATE %s SET %s WHERE id=UUID_TO_BIN(?)",
        obj_table, table.concat(update_sets, ", "))
    local update_def = {update_sql}
    table.insert(update_def, {t.CHAR, label_len})
    for _ = 1, update_text_cols do
        table.insert(update_def, {t.CHAR, sysbench.opt.text_value_len})
    end
    table.insert(update_def, {t.CHAR, 36})

    local update_rel_sql = string.format(
        "UPDATE %s SET object_type_attribute_id=UUID_TO_BIN(?), referenced_object_type_id=UUID_TO_BIN(?) WHERE id=UUID_TO_BIN(?)",
        rel_table)
    local update_rel_def = {update_rel_sql, {t.CHAR, 36}, {t.CHAR, 36}, {t.CHAR, 36}}

    stmt_defs = {
        select = select_def,
        insert_obj = insert_obj_def,
        insert_rel = insert_rel_def,
        update_obj = update_def,
        update_rel = update_rel_def
    }
end

local function prepare_for_stmt(key)
    stmt[key] = con:prepare(stmt_defs[key][1])
    local nparam = #stmt_defs[key] - 1

    if nparam > 0 then
        param[key] = {}
    end

    for p = 1, nparam do
        local btype = stmt_defs[key][p + 1]
        local len
        if type(btype) == "table" then
            len = btype[2]
            btype = btype[1]
        end
        if btype == sysbench.sql.type.VARCHAR or btype == sysbench.sql.type.CHAR then
            param[key][p] = stmt[key]:bind_create(btype, len)
        else
            param[key][p] = stmt[key]:bind_create(btype)
        end
    end

    if nparam > 0 then
        stmt[key]:bind_param(unpack(param[key]))
    end
end

function prepare_begin()
    stmt.begin = con:prepare("BEGIN")
end

function prepare_commit()
    stmt.commit = con:prepare("COMMIT")
end

function prepare_select()
    prepare_for_stmt("select")
end

function prepare_insert_obj()
    prepare_for_stmt("insert_obj")
end

function prepare_insert_rel()
    prepare_for_stmt("insert_rel")
end

function prepare_update_obj()
    prepare_for_stmt("update_obj")
end

function prepare_update_rel()
    prepare_for_stmt("update_rel")
end

function begin()
    stmt.begin:execute()
end

function commit()
    stmt.commit:execute()
end

function execute_select()
    local ws_id = workspace_ids[sysbench.rand.default(1, #workspace_ids)]
    param.select[1]:set(ws_id)
    param.select[2]:set(ws_id)
    local idx = 3
    for _ = 1, 5 do
        param.select[idx]:set(obj_type_ids[sysbench.rand.default(1, #obj_type_ids)])
        idx = idx + 1
    end
    for _ = 1, 5 do
        param.select[idx]:set(obj_type_ids[sysbench.rand.default(1, #obj_type_ids)])
        idx = idx + 1
    end
    local builder = match_builders[sysbench.rand.default(1, #match_builders)]
    local match_query = builder(ws_id)
    if #match_query > sysbench.opt.match_len then
        match_query = string.sub(match_query, 1, sysbench.opt.match_len)
    end
    param.select[idx]:set(match_query)
    idx = idx + 1
    param.select[idx]:set(rand_between(sysbench.opt.select_limit_min, sysbench.opt.select_limit_max))
    idx = idx + 1
    param.select[idx]:set(rand_between(0, sysbench.opt.select_offset_max))
    stmt.select:execute()
end

local function next_object_id()
    local id = next_obj_id
    next_obj_id = next_obj_id + sysbench.opt.threads
    return id
end

local function next_relationship_id()
    local id = next_rel_id
    next_rel_id = next_rel_id + sysbench.opt.threads
    return id
end

local function next_sequential_id(ws_index)
    if seq_counters[ws_index] == nil then
        local base = os.time() * 1000000 + sysbench.rand.default(1, 1000000) + sysbench.tid * 10000000 + ws_index * 1000
        seq_counters[ws_index] = base
    end
    local v = seq_counters[ws_index]
    seq_counters[ws_index] = v + 1
    return v
end

function execute_insert()
    local obj_id = next_object_id()
    local ws_index = workspace_index_for_row(obj_id)
    local ws_id = workspace_id_for_row(obj_id)
    local sequential_id = next_sequential_id(ws_index)
    local obj_type_id = obj_type_id_for_row(obj_id)
    local schema_id = obj_type_id
    local schema_key = build_schema_key(schema_id)
    local label = build_label(obj_id)
    local text_values = build_text_values(obj_id, true)

    param.insert_obj[1]:set(uuid_from_int(obj_id))
    param.insert_obj[2]:set(ws_id)
    param.insert_obj[3]:set(sequential_id)
    param.insert_obj[4]:set(label)
    param.insert_obj[5]:set(obj_type_id)
    param.insert_obj[6]:set(schema_id)
    param.insert_obj[7]:set(schema_key)
    local idx = 8
    for i = 1, text_value_cols do
        param.insert_obj[idx]:set(text_values[i])
        idx = idx + 1
    end
    stmt.insert_obj:execute()

    local rel_count = rel_count_for_obj(obj_id)
    for _ = 1, rel_count do
        local rel_id = next_relationship_id()
        local ref_obj = random_obj_row_for_workspace(ws_index)
        if ref_obj == obj_id then
            ref_obj = random_obj_row_for_workspace(ws_index)
        end
        local ref_type_id = obj_type_id_for_row(ref_obj)
        local attr_id = uuid_from_int(rel_id + 5000000000)

        param.insert_rel[1]:set(uuid_from_int(rel_id))
        param.insert_rel[2]:set(ws_id)
        param.insert_rel[3]:set(uuid_from_int(obj_id))
        param.insert_rel[4]:set(uuid_from_int(ref_obj))
        param.insert_rel[5]:set(attr_id)
        param.insert_rel[6]:set(obj_type_id)
        param.insert_rel[7]:set(ref_type_id)
        stmt.insert_rel:execute()
    end
end

local function random_existing_obj_id()
    if sysbench.opt.obj_rows <= 0 then
        return 1
    end
    return sysbench.rand.default(1, sysbench.opt.obj_rows)
end

function execute_update()
    local obj_id = random_existing_obj_id()
    local ws_id = workspace_id_for_row(obj_id)
    local label = build_label(obj_id + sysbench.rand.default(1, 1000000))
    local text_values = build_text_values(obj_id + sysbench.rand.default(1, 1000000), true)

    param.update_obj[1]:set(label)
    local idx = 2
    for i = 1, update_text_cols do
        param.update_obj[idx]:set(text_values[i])
        idx = idx + 1
    end
    param.update_obj[idx]:set(uuid_from_int(obj_id))
    stmt.update_obj:execute()
end

local function random_existing_rel_id()
    if prepared_rel_rows <= 0 then
        return 1
    end
    return sysbench.rand.default(1, prepared_rel_rows)
end

function execute_update_rel()
    local rel_id = random_existing_rel_id()
    local new_attr = uuid_from_int(rel_id + sysbench.rand.default(100000, 200000))
    local new_ref_type = obj_type_ids[sysbench.rand.default(1, #obj_type_ids)]
    param.update_rel[1]:set(new_attr)
    param.update_rel[2]:set(new_ref_type)
    param.update_rel[3]:set(uuid_from_int(rel_id))
    stmt.update_rel:execute()
end

function execute_operation()
    if total_weight <= 0 then
        return
    end
    local choice = sysbench.rand.default(1, total_weight)
    if choice <= select_weight then
        execute_select()
        return
    end
    choice = choice - select_weight
    if choice <= insert_weight then
        execute_insert()
        return
    end
    choice = choice - insert_weight
    if choice <= update_weight then
        execute_update()
        return
    end
    execute_update_rel()
end

local function get_thread_range(total_rows)
    if total_rows <= 0 then
        return 1, 0
    end
    local threads = sysbench.opt.threads
    local base = math.floor(total_rows / threads)
    local extra = total_rows % threads
    local start = sysbench.tid * base + math.min(sysbench.tid, extra) + 1
    local count = base
    if sysbench.tid < extra then
        count = count + 1
    end
    local finish = start + count - 1
    return start, finish
end

local function build_obj_insert_row(row_num)
    local ws_id = workspace_id_for_row(row_num)
    local obj_type_id = obj_type_id_for_row(row_num)
    local schema_id = obj_type_id_for_row(row_num)
    local schema_key = build_schema_key(schema_id)
    local label = build_label(row_num)
    local text_values = build_text_values(row_num, false)

    local values = {
        string.format("UUID_TO_BIN(%s)", quote(uuid_from_int(row_num))),
        quote(ws_id),
        tostring(row_num),
        quote(label),
        string.format("UUID_TO_BIN(%s)", quote(obj_type_id)),
        string.format("UUID_TO_BIN(%s)", quote(schema_id)),
        quote(schema_key)
    }
    for i = 1, text_value_cols do
        table.insert(values, quote(text_values[i]))
    end
    return "(" .. table.concat(values, ",") .. ")"
end

local function build_rel_insert_row(rel_id, obj_num, ref_num, ws_id, obj_type_id, ref_type_id, attr_id)
    local values = {
        string.format("UUID_TO_BIN(%s)", quote(uuid_from_int(rel_id))),
        quote(ws_id),
        string.format("UUID_TO_BIN(%s)", quote(uuid_from_int(obj_num))),
        string.format("UUID_TO_BIN(%s)", quote(uuid_from_int(ref_num))),
        string.format("UUID_TO_BIN(%s)", quote(attr_id)),
        string.format("UUID_TO_BIN(%s)", quote(obj_type_id)),
        string.format("UUID_TO_BIN(%s)", quote(ref_type_id))
    }

    return "(" .. table.concat(values, ",") .. ")"
end

local function bulk_insert_obj(con_handle, start_id, end_id)
    if end_id < start_id then
        return
    end
    local obj_table = sysbench.opt.obj_table
    local obj_cols = {"id", "workspace_id", "sequential_id", "label", "obj_type_id", "schema_id", "schema_key"}
    for i = 1, text_value_cols do
        table.insert(obj_cols, "text_value_" .. i)
    end
    local query = string.format("INSERT INTO %s (%s) VALUES", obj_table, table.concat(obj_cols, ", "))
    con_handle:bulk_insert_init(query)
    for i = start_id, end_id do
        con_handle:bulk_insert_next(build_obj_insert_row(i))
    end
    con_handle:bulk_insert_done()
end

local function bulk_insert_rel(con_handle, obj_start, obj_end, rel_start_id)
    if obj_end < obj_start then
        return
    end
    local rel_table = sysbench.opt.rel_table
    local query = string.format(
        "INSERT INTO %s (id, workspace_id, object_id, referenced_object_id, object_type_attribute_id, object_type_id, referenced_object_type_id) VALUES",
        rel_table)
    con_handle:bulk_insert_init(query)
    local rel_id = rel_start_id
    for obj_num = obj_start, obj_end do
        local ws_index = workspace_index_for_row(obj_num)
        local ws_id = workspace_id_for_row(obj_num)
        local rel_count = rel_count_for_obj(obj_num)
        local obj_type_id = obj_type_id_for_row(obj_num)
        for _ = 1, rel_count do
            local ref_num = random_obj_row_for_workspace(ws_index)
            if ref_num == obj_num then
                ref_num = random_obj_row_for_workspace(ws_index)
            end
            local ref_type_id = obj_type_id_for_row(ref_num)
            local attr_id = uuid_from_int(rel_id + 5000000000)

            con_handle:bulk_insert_next(
                build_rel_insert_row(
                    rel_id,
                    obj_num,
                    ref_num,
                    ws_id,
                    obj_type_id,
                    ref_type_id,
                    attr_id))
            rel_id = rel_id + 1
        end
    end
    con_handle:bulk_insert_done()
end

function cmd_prepare()
    init_data_lists()
    local drv_local = sysbench.sql.driver()
    local con_local = drv_local:connect()

    local obj_start, obj_end = get_thread_range(sysbench.opt.obj_rows)

    if obj_end >= obj_start then
        print(string.format("Thread %d inserting obj_new rows %d-%d", sysbench.tid, obj_start, obj_end))
        bulk_insert_obj(con_local, obj_start, obj_end)

        local rel_start_id = relationships_before_object(obj_start) + 1
        local rel_count = rel_total_for_range(obj_start, obj_end)
        local rel_end_id = rel_start_id + rel_count - 1
        print(string.format("Thread %d inserting obj_relationship_new rows %d-%d", sysbench.tid, rel_start_id, rel_end_id))
        bulk_insert_rel(con_local, obj_start, obj_end, rel_start_id)
    end
    con_local:disconnect()
end

sysbench.cmdline.commands = {
    prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND}
}

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()

    stmt = {}
    param = {}

    init_data_lists()
    build_stmt_defs()

    select_weight = tonumber(sysbench.opt.select_weight) or 0
    insert_weight = tonumber(sysbench.opt.insert_weight) or 0
    update_weight = tonumber(sysbench.opt.update_weight) or 0
    rel_update_weight = tonumber(sysbench.opt.rel_update_weight) or 0
    total_weight = select_weight + insert_weight + update_weight + rel_update_weight

    local run_offset = (os.time() * 1000000) + sysbench.rand.default(1, 1000000)
    next_obj_id = run_offset + sysbench.tid
    next_rel_id = run_offset + sysbench.tid

    prepare_statements()
end

function close_statements()
    for k, s in pairs(stmt) do
        if s ~= nil and k ~= "begin" and k ~= "commit" then
            s:close()
        end
    end
    if stmt.begin ~= nil then
        stmt.begin:close()
    end
    if stmt.commit ~= nil then
        stmt.commit:close()
    end
end

function thread_done()
    close_statements()
    con:disconnect()
end

function sysbench.hooks.before_restart_event(errdesc)
    if errdesc.sql_errno == 2013 or
        errdesc.sql_errno == 2055 or
        errdesc.sql_errno == 2006 or
        errdesc.sql_errno == 2011
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
