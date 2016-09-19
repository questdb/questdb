/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

import com.questdb.io.sink.StringSink;
import com.questdb.misc.Dates;
import com.questdb.ql.Record;
import com.questdb.ql.ops.AbstractUnaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.std.CharSink;
import com.questdb.std.ObjectFactory;
import com.questdb.store.ColumnType;

public class DtoA4Function extends AbstractUnaryOperator {
    public final static ObjectFactory<Function> FACTORY = new ObjectFactory<Function>() {
        @Override
        public Function newInstance() {
            return new DtoA4Function();
        }
    };
    private final StringSink sinkA = new StringSink();
    private final StringSink sinkB = new StringSink();

    private DtoA4Function() {
        super(ColumnType.STRING);
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        sinkA.clear();
        Dates.formatMMMDYYYY(sinkA, value.getDate(rec));
        return sinkA;
    }

    @Override
    public CharSequence getFlyweightStrB(Record rec) {
        sinkB.clear();
        Dates.formatMMMDYYYY(sinkB, value.getDate(rec));
        return sinkB;
    }

    @Override
    public CharSequence getStr(Record rec) {
        return getFlyweightStr(rec).toString();
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        Dates.formatMMMDYYYY(sink, value.getDate(rec));
    }

    @Override
    public int getStrLen(Record rec) {
        return getFlyweightStr(rec).length();
    }
}
