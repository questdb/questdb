/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.ops.conv;

import com.questdb.ql.ops.AbstractUnaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;

public class DtoA4Function extends AbstractUnaryOperator {
    public final static VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new DtoA4Function(position);
    private final StringSink sinkA = new StringSink();
    private final StringSink sinkB = new StringSink();

    private DtoA4Function(int position) {
        super(ColumnType.STRING, position);
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        sinkA.clear();
        DateFormatUtils.formatMMMDYYYY(sinkA, value.getDate(rec));
        return sinkA;
    }

    @Override
    public CharSequence getFlyweightStrB(Record rec) {
        sinkB.clear();
        DateFormatUtils.formatMMMDYYYY(sinkB, value.getDate(rec));
        return sinkB;
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        DateFormatUtils.formatMMMDYYYY(sink, value.getDate(rec));
    }
}
