#!/usr/bin/env sysbench

require("oltp_common")

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
end

function event()
    if not sysbench.opt.skip_trx then
        begin()
    end
    write(execute_insert, execute_update)

    if not sysbench.opt.skip_trx then
        commit()
    end

    check_reconnect()
end

function init()
    gen_random_update_ids()
    print("Init update id params")
end
