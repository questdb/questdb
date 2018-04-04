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

import com.questdb.common.ColumnType;
import com.questdb.common.Record;
import com.questdb.common.StorageFacade;
import com.questdb.ex.ParserException;
import com.questdb.parser.sql.QueryError;
import com.questdb.ql.ops.AbstractVirtualColumn;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.std.Numbers;
import com.questdb.std.NumericException;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;
import com.questdb.std.time.*;

public class DateToCharTZLocaleFunction extends AbstractVirtualColumn implements Function {

    public final static VirtualColumnFactory<Function> FACTORY = (position, env) -> new DateToCharTZLocaleFunction(position, env.dateFormatFactory, env.dateLocaleFactory);
    private final DateFormatFactory dateFormatFactory;
    private final DateLocaleFactory dateLocaleFactory;
    private final StringSink sinkA = new StringSink();
    private final StringSink sinkB = new StringSink();
    private DateLocale dateLocale;
    private DateFormat fmt;
    private VirtualColumn column;
    private long offset;
    private CharSequence tz;
    private VirtualColumn tzArg;
    private TimeZoneRules rules;

    private DateToCharTZLocaleFunction(int position, DateFormatFactory dateFormatFactory, DateLocaleFactory dateLocaleFactory) {
        super(ColumnType.STRING, position);
        this.dateFormatFactory = dateFormatFactory;
        this.dateLocaleFactory = dateLocaleFactory;
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
        long instant = column.getLong(rec);
        if (instant > Long.MIN_VALUE) {
            fmtToSink(instant, sink);
        }
    }

    @Override
    public boolean isConstant() {
        return column.isConstant();
    }

    @Override
    public void prepare(StorageFacade facade) {
        column.prepare(facade);
    }

    @Override
    public void setArg(int pos, VirtualColumn arg) throws ParserException {
        switch (pos) {
            case 0:
                column = arg;
                break;
            case 1:
                fmt = dateFormatFactory.get(arg.getFlyweightStr(null));
                break;
            case 2:
                tz = arg.getFlyweightStr(null);
                tzArg = arg;
                break;
            case 3:
                dateLocale = dateLocaleFactory.getDateLocale(arg.getFlyweightStr(null));
                if (dateLocale == null) {
                    throw QueryError.$(arg.getPosition(), "Invalid locale");
                }

                long l = Dates.parseOffset(tz, 0, tz.length());
                try {
                    if (l == Long.MIN_VALUE) {
                        rules = dateLocale.getZoneRules(Numbers.decodeInt(dateLocale.matchZone(tz, 0, tz.length())));
                    } else {
                        offset = Numbers.decodeInt(l) * Dates.MINUTE_MILLIS;
                        rules = null;
                    }
                } catch (NumericException e) {
                    throw QueryError.$(tzArg.getPosition(), "Invalid timezone");
                }


            default:
                break;

        }
    }

    private void fmtToSink(long instant, CharSink sink) {
        if (rules != null) {
            fmt.format(instant + rules.getOffset(instant), dateLocale, tz, sink);
        } else {
            fmt.format(instant + offset, dateLocale, tz, sink);
        }
    }

    private CharSequence getFlyweightStr0(Record rec, StringSink sink) {
        long instant = column.getLong(rec);
        if (instant == Long.MIN_VALUE) {
            return null;
        }

        sink.clear();
        fmtToSink(instant, sink);
        return sink;
    }
}
