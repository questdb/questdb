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

package com.questdb.ql.ops.eq;

import com.questdb.ex.ParserException;
import com.questdb.net.http.ServerConfiguration;
import com.questdb.ql.Record;
import com.questdb.ql.ops.AbstractBinaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.ql.parser.IntervalCompiler;
import com.questdb.std.LongList;
import com.questdb.store.ColumnType;

public class DateEqualsStrConstOperator extends AbstractBinaryOperator {

    public final static VirtualColumnFactory<Function> FACTORY = new VirtualColumnFactory<Function>() {
        @Override
        public Function newInstance(int position, ServerConfiguration configuration) {
            return new DateEqualsStrConstOperator(position);
        }
    };

    private final LongList intervals = new LongList();
    private int intervalCount;

    private DateEqualsStrConstOperator(int position) {
        super(ColumnType.BOOLEAN, position);
    }

    @Override
    public boolean getBool(Record rec) {
        long date = lhs.getDate(rec);
        for (int i = 0; i < intervalCount; i++) {
            if (date < IntervalCompiler.getIntervalLo(intervals, i)) {
                return false;
            }

            if (date <= IntervalCompiler.getIntervalHi(intervals, i)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void setRhs(VirtualColumn rhs) throws ParserException {
        super.setRhs(rhs);
        CharSequence intervalStr = rhs.getFlyweightStr(null);
        IntervalCompiler.parseIntervalEx(intervalStr, 0, intervalStr.length(), rhs.getPosition(), intervals);
        intervalCount = intervals.size() / 2;
    }
}
