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
import com.questdb.std.NumericException;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocale;
import com.questdb.std.time.DateLocaleFactory;

public class ToDateThreeArgFunction extends AbstractVirtualColumn implements Function {

    public final static VirtualColumnFactory<Function> FACTORY = (position, env) -> new ToDateThreeArgFunction(position, env.dateFormatFactory, env.dateLocaleFactory);

    private final DateFormatFactory dateFormatFactory;
    private final DateLocaleFactory dateLocaleFactory;
    private DateFormat fmt;
    private VirtualColumn column;
    private DateLocale locale;

    private ToDateThreeArgFunction(int position, DateFormatFactory dateFormatFactory, DateLocaleFactory dateLocaleFactory) {
        super(ColumnType.DATE, position);
        this.dateFormatFactory = dateFormatFactory;
        this.dateLocaleFactory = dateLocaleFactory;
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
            CharSequence s = column.getFlyweightStr(rec);
            return s == null ? Long.MIN_VALUE : fmt.parse(s, locale);
        } catch (NumericException ignore) {
            return Long.MIN_VALUE;
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
                locale = dateLocaleFactory.getDateLocale(arg.getFlyweightStr(null));
                if (locale == null) {
                    throw QueryError.$(arg.getPosition(), "Invalid locale");
                }
                break;
            default:
                break;
        }
    }
}
