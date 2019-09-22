/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.griffin.engine.functions.StatelessFunction;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.str.CharSink;

class Long256BindVariable extends Long256Function implements StatelessFunction {
    final Long256Impl value = new Long256Impl();

    public Long256BindVariable(Long256 value) {
        super(0);
        this.value.copyFrom(value);
    }

    public Long256BindVariable(long l0, long l1, long l2, long l3) {
        super(0);
        this.value.setAll(l0, l1, l2, l3);
    }

    @Override
    public Long256 getLong256A(Record rec) {
        return value;
    }

    @Override
    public Long256 getLong256B(Record rec) {
        return value;
    }

    @Override
    public void getLong256(Record rec, CharSink sink) {
        final long a = value.getLong0();
        final long b = value.getLong1();
        final long c = value.getLong2();
        final long d = value.getLong3();

        Numbers.appendLong256(a, b, c, d, sink);
    }
}
