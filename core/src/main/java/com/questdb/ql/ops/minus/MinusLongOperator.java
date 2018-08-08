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

package com.questdb.ql.ops.minus;

import com.questdb.ql.ops.AbstractBinaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;

public class MinusLongOperator extends AbstractBinaryOperator {

    public final static VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new MinusLongOperator(position);

    private MinusLongOperator(int position) {
        super(ColumnType.LONG, position);
    }

    @Override
    public double getDouble(Record rec) {
        long l = lhs.getLong(rec);
        long r = rhs.getLong(rec);
        return l > Long.MIN_VALUE && r > Long.MIN_VALUE ? l - r : Double.NaN;
    }

    @Override
    public long getLong(Record rec) {
        long l = lhs.getLong(rec);
        long r = rhs.getLong(rec);
        return l > Long.MIN_VALUE && r > Long.MIN_VALUE ? l - r : Long.MIN_VALUE;
    }
}
