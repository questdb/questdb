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

package com.questdb.ql.ops.gt;

import com.questdb.ex.NumericException;
import com.questdb.ex.ParserException;
import com.questdb.misc.Dates;
import com.questdb.net.http.ServerConfiguration;
import com.questdb.ql.Record;
import com.questdb.ql.ops.AbstractBinaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.ql.parser.QueryError;
import com.questdb.store.ColumnType;

public class DateGreaterThanStrOperator extends AbstractBinaryOperator {

    public final static VirtualColumnFactory<Function> FACTORY = new VirtualColumnFactory<Function>() {
        @Override
        public Function newInstance(int position, ServerConfiguration configuration) {
            return new DateGreaterThanStrOperator(position);
        }
    };

    private long date;
    private boolean alwaysFalse = false;

    private DateGreaterThanStrOperator(int position) {
        super(ColumnType.BOOLEAN, position);
    }

    @Override
    public boolean getBool(Record rec) {
        return !alwaysFalse && lhs.getDate(rec) > date;
    }

    @Override
    public boolean isConstant() {
        return super.isConstant() || alwaysFalse;
    }

    @Override
    public void setRhs(VirtualColumn rhs) throws ParserException {
        assertConstant(rhs);
        CharSequence cs = rhs.getFlyweightStr(null);
        if (cs == null) {
            alwaysFalse = true;
        } else {
            try {
                date = Dates.parseDateTime(cs);
                super.setRhs(rhs);
            } catch (NumericException e) {
                throw QueryError.$(rhs.getPosition(), "Not a date");
            }
        }
    }
}
