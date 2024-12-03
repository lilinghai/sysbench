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
-- Insert-Only OLTP benchmark
-- ----------------------------------------------------------------------
require("oltp_common")

sysbench.cmdline.commands.prepare = {function()
    assert(sysbench.opt.auto_inc == true)
    cmd_prepare()
end, sysbench.cmdline.PARALLEL_COMMAND}

function prepare_statements()
    -- We do not use prepared statements here, but oltp_common.sh expects this
    -- function to be defined
end

function event()
    local tnum = get_stmt_num()
    local tn = get_table_num(tnum)
    local db_num, table_num_in_db = get_db_table_num(tn)

    local table_name = string.format("%s%d.sbtest%d", sysbench.opt.db_prefix, db_num, table_num_in_db)
    local k_val = sysbench.rand.default(1, sysbench.opt.table_size)
    local c_val = get_c_value()
    local pad_val = get_pad_value()

    assert(sysbench.opt.auto_inc == true)
    con:query(string.format("INSERT INTO %s (k, c, pad) VALUES " .. "(%d, '%s', '%s')", table_name, k_val, c_val,
        pad_val))

    check_reconnect()
    if sysbench.opt.txn_interval > 0 then
        ffi.C.usleep(sysbench.opt.txn_interval * 1000)
    end
end
