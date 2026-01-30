#!/usr/bin/env sysbench

require("jsm_common")

function prepare_statements()
    if not sysbench.opt.skip_trx then
        prepare_begin()
        prepare_commit()
    end

    if sysbench.opt.select_weight > 0 then
        prepare_select()
    end

    if sysbench.opt.insert_weight > 0 then
        prepare_insert_obj()
        prepare_insert_rel()
    end

    if sysbench.opt.update_weight > 0 then
        prepare_update_obj()
    end

    if sysbench.opt.rel_update_weight > 0 then
        prepare_update_rel()
    end
end

function event()
    if not sysbench.opt.skip_trx then
        begin()
    end

    for _ = 1, sysbench.opt.ops_per_txn do
        execute_operation()
    end

    if not sysbench.opt.skip_trx then
        commit()
    end

    check_reconnect()
end
