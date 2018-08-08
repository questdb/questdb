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
import com.questdb.std.NumericException;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocale;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;

public class ToDateTwoArgFunction extends AbstractBinaryOperator {

    public final static VirtualColumnFactory<Function> FACTORY = (position, env) -> new ToDateTwoArgFunction(position, env.dateFormatFactory, env.dateLocaleFactory.getDefaultDateLocale());

    private final DateFormatFactory dateFormatFactory;
    private final DateLocale dateLocale;
    private DateFormat fmt;

    private ToDateTwoArgFunction(int position, DateFormatFactory dateFormatFactory, DateLocale defaultLocale) {
        super(ColumnType.DATE, position);
        this.dateFormatFactory = dateFormatFactory;
        this.dateLocale = defaultLocale;
    }

    @Override
    public long getDate(Record rec) {
        return getLong(rec);
    }

    @Override
    public double getDouble(Record rec) {
        return getLong(rec);
    }

    @Override
    public long getLong(Record rec) {
        try {
            CharSequence s = lhs.getFlyweightStr(rec);
            return s == null ? Long.MIN_VALUE : fmt.parse(s, dateLocale);
        } catch (NumericException ignore) {
            return Long.MIN_VALUE;
        }
    }

    @Override
    public void setRhs(VirtualColumn rhs) throws ParserException {
        super.setRhs(rhs);
        fmt = dateFormatFactory.get(rhs.getFlyweightStr(null));
    }
}
