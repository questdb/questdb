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

import com.questdb.ex.ParserException;
import com.questdb.ql.ops.AbstractBinaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocale;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;

public class DateToCharFunction extends AbstractBinaryOperator {

    public final static VirtualColumnFactory<Function> FACTORY = (position, env) -> new DateToCharFunction(position, env.dateFormatFactory, env.dateLocaleFactory.getDefaultDateLocale());
    private final static String ZONE = "Z";
    private final DateFormatFactory dateFormatFactory;
    private final DateLocale dateLocale;
    private final StringSink sinkA = new StringSink();
    private final StringSink sinkB = new StringSink();
    private DateFormat fmt;

    private DateToCharFunction(int position, DateFormatFactory dateFormatFactory, DateLocale defaultLocale) {
        super(ColumnType.STRING, position);
        this.dateFormatFactory = dateFormatFactory;
        this.dateLocale = defaultLocale;
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        return getFlyweightStr0(rec, sinkA);
    }

    @Override
    public CharSequence getFlyweightStrB(Record rec) {
        return getFlyweightStr0(rec, sinkB);
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        long instant = lhs.getLong(rec);
        if (instant > Long.MIN_VALUE) {
            fmt.format(instant, dateLocale, ZONE, sink);
        }
    }

    @Override
    public void setRhs(VirtualColumn rhs) throws ParserException {
        super.setRhs(rhs);
        fmt = dateFormatFactory.get(rhs.getFlyweightStr(null));
    }

    private CharSequence getFlyweightStr0(Record rec, StringSink sink) {
        long instant = lhs.getLong(rec);
        if (instant == Long.MIN_VALUE) {
            return null;
        }

        sink.clear();
        fmt.format(instant, dateLocale, ZONE, sink);
        return sink;
    }
}
