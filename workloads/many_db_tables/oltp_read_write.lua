#!/usr/bin/env sysbench

-- Copyright (C) 2006-2017 Alexey Kopytov <akopytov@gmail.com>
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
-- ----------------------------------------------------------------------
-- Read/Write OLTP benchmark
-- ----------------------------------------------------------------------
require("oltp_common")

function prepare_statements()
    if not sysbench.opt.skip_trx then
        prepare_begin()
        prepare_commit()
    end

    if sysbench.opt.point_get then
        prepare_point_selects()
    end

    if sysbench.opt.range_selects then
        prepare_simple_ranges()
        prepare_sum_ranges()
        prepare_order_ranges()
        prepare_distinct_ranges()
    end

    if sysbench.opt.extra_selects then
        prepare_extra_selects()
    end

    if sysbench.opt.index_equal_selects then
        prepare_index_equal_select()
    end

    if sysbench.opt.index_range_selects then
        prepare_simple_index_range()
    end

    if sysbench.opt.join1_selects then
        prepare_join1()
    end

    prepare_index_updates()
    prepare_non_index_updates()
    prepare_delete_inserts()
end

function event()
    if not sysbench.opt.skip_trx then
        begin()
    end

    if sysbench.opt.point_get then
        execute_point_selects()
    end

    if sysbench.opt.range_selects then
        execute_simple_ranges()
        execute_sum_ranges()
        execute_order_ranges()
        execute_distinct_ranges()
    end

    if sysbench.opt.extra_selects then
        execute_extra_selects()
    end

    if sysbench.opt.index_equal_selects then
        execute_index_equal_select()
    end

    if sysbench.opt.index_range_selects then
        execute_simple_index_range()
    end

    if sysbench.opt.join1_selects then
        execute_join1()
    end

    execute_index_updates()
    execute_non_index_updates()
    execute_delete_inserts()

    if not sysbench.opt.skip_trx then
        commit()
    end

    check_reconnect()
    if sysbench.opt.txn_interval > 0 then
        ffi.C.usleep(sysbench.opt.txn_interval * 1000)
    end
end
