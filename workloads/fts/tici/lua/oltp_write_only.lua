#!/usr/bin/env sysbench

require("oltp_common")

function prepare_statements()
    update_ids = {}
    local table_name = get_table_name()
    local ids_file = io.open(table_name .. ".ids.txt", "r")
    if ids_file then
        for line in ids_file:lines() do
            table.insert(update_ids, line)
        end
        ids_file:close()
    end

    prepare_insert()
    prepare_update()
    prepare_delete()
end

function event()
    write(execute_insert, execute_update, execute_delete)
end

function init()
    gen_random_update_ids()
end
