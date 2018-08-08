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

package com.questdb.ql.ops.lte;

import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.ql.ops.gt.DateToStrCmpBaseOperator;
import com.questdb.std.Numbers;
import com.questdb.store.Record;

public class DateLessOrEqualStrOperator extends DateToStrCmpBaseOperator {

    public final static VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new DateLessOrEqualStrOperator(position);

    private DateLessOrEqualStrOperator(int position) {
        super(position);
    }

    @Override
    public boolean getBool(Record rec) {
        if (alwaysFalse) {
            return false;
        }

        long d = lhs.getDate(rec);

        return d <= date && d > Numbers.LONG_NaN;
    }
}
