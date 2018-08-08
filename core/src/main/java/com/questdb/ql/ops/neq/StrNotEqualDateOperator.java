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

package com.questdb.ql.ops.neq;

import com.questdb.ex.ParserException;
import com.questdb.parser.sql.IntervalCompiler;
import com.questdb.ql.ops.AbstractBinaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.std.LongList;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;

public class StrNotEqualDateOperator extends AbstractBinaryOperator {

    public final static VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new StrNotEqualDateOperator(position);

    private final LongList intervals = new LongList();
    private int intervalCount;

    private StrNotEqualDateOperator(int position) {
        super(ColumnType.BOOLEAN, position);
    }

    @Override
    public boolean getBool(Record rec) {
        long date = rhs.getDate(rec);
        for (int i = 0; i < intervalCount; i++) {
            if (date < IntervalCompiler.getIntervalLo(intervals, i)) {
                return true;
            }

            if (date <= IntervalCompiler.getIntervalHi(intervals, i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void setLhs(VirtualColumn lhs) throws ParserException {
        super.setLhs(lhs);
        // null is handled by another operator
        CharSequence intervalStr = lhs.getFlyweightStr(null);
        if (intervalStr != null) {
            IntervalCompiler.parseIntervalEx(intervalStr, 0, intervalStr.length(), lhs.getPosition(), intervals);
        }
        intervalCount = intervals.size() / 2;
    }
}
