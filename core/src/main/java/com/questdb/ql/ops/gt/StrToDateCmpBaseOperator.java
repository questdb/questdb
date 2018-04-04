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

package com.questdb.ql.ops.gt;

import com.questdb.common.ColumnType;
import com.questdb.ex.ParserException;
import com.questdb.parser.sql.QueryError;
import com.questdb.ql.ops.AbstractBinaryOperator;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.NumericException;
import com.questdb.std.time.DateFormatUtils;

public class StrToDateCmpBaseOperator extends AbstractBinaryOperator {

    protected long date;
    protected boolean alwaysFalse = false;

    protected StrToDateCmpBaseOperator(int position) {
        super(ColumnType.BOOLEAN, position);
    }

    @Override
    public boolean isConstant() {
        return super.isConstant() || alwaysFalse;
    }

    @Override
    public void setLhs(VirtualColumn lhs) throws ParserException {
        assertConstant(lhs);
        super.setLhs(lhs);
        CharSequence cs = lhs.getFlyweightStr(null);
        if (cs == null) {
            alwaysFalse = true;
        } else {
            try {
                date = DateFormatUtils.parseDateTime(cs);
            } catch (NumericException e) {
                throw QueryError.$(lhs.getPosition(), "Not a date");
            }
        }
    }
}
