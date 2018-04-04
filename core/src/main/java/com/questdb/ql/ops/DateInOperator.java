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

package com.questdb.ql.ops;

import com.questdb.common.ColumnType;
import com.questdb.common.Record;
import com.questdb.common.StorageFacade;
import com.questdb.ex.ParserException;
import com.questdb.parser.sql.QueryError;
import com.questdb.std.NumericException;
import com.questdb.std.time.DateFormatUtils;

public class DateInOperator extends AbstractVirtualColumn implements Function {

    public final static VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new DateInOperator(position);

    private VirtualColumn lhs;
    private long lo;
    private long hi;

    private DateInOperator(int position) {
        super(ColumnType.BOOLEAN, position);
    }

    @Override
    public boolean getBool(Record rec) {
        long date = lhs.getDate(rec);
        return date > lo && date < hi;
    }

    @Override
    public boolean isConstant() {
        return lhs.isConstant();
    }

    @Override
    public void prepare(StorageFacade facade) {
        lhs.prepare(facade);
    }

    @Override
    public void setArg(int pos, VirtualColumn arg) throws ParserException {
        if (pos == 0) {
            lhs = arg;
        } else {

            if (pos > 2) {
                throw QueryError.$(arg.getPosition(), "Too many args");
            }

            assertConstant(arg);

            switch (arg.getType()) {
                case ColumnType.STRING:
                    try {
                        CharSequence cs = arg.getFlyweightStr(null);

                        if (cs == null) {
                            throw QueryError.$(arg.getPosition(), "Cannot be null");
                        }

                        if (pos == 1) {
                            lo = DateFormatUtils.parseDateTime(cs) - 1;
                        } else {
                            hi = DateFormatUtils.parseDateTime(cs) + 1;
                        }
                    } catch (NumericException e) {
                        throw QueryError.$(arg.getPosition(), "Not a date");
                    }

                    break;
                default:
                    typeError(arg.getPosition(), ColumnType.STRING);
                    break;
            }
        }
    }
}
