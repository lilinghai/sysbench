#!/usr/bin/env sysbench

require("oltp_common")

local operation_ratios

local function build_operation_ratios()
    return {
        __ratios = true,
        weights = {
            math.max(math.floor(sysbench.opt.insert_ratio), 0),
            math.max(math.floor(sysbench.opt.update_ratio), 0),
            math.max(math.floor(sysbench.opt.delete_ratio), 0)
        }
    }
end

function prepare_statements()
    if not sysbench.opt.skip_trx then
        prepare_begin()
        prepare_commit()
    end

    update_ids = {}
    -- init update ids param
    for line in io.lines(sysbench.opt.workload .. ".ids.txt") do
        table.insert(update_ids, line)
    end

    prepare_insert()
    prepare_update()
    prepare_delete()
end

function event()
    write(execute_insert, execute_update, execute_delete, operation_ratios)
end

function init()
    gen_random_update_ids()
    operation_ratios = build_operation_ratios()
    print("Init update id params")
end
