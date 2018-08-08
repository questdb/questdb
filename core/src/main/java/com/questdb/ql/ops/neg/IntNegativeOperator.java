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

package com.questdb.ql.ops.neg;

import com.questdb.ql.ops.AbstractUnaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;

public class IntNegativeOperator extends AbstractUnaryOperator {

    public final static VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new IntNegativeOperator(position);

    private IntNegativeOperator(int position) {
        super(ColumnType.INT, position);
    }

    @Override
    public double getDouble(Record rec) {
        int v = value.getInt(rec);
        return v == Integer.MIN_VALUE ? Double.NaN : -v;
    }

    @Override
    public int getInt(Record rec) {
        int v = value.getInt(rec);
        return v == Integer.MIN_VALUE ? Integer.MIN_VALUE : -v;
    }

    @Override
    public long getLong(Record rec) {
        int v = value.getInt(rec);
        return v == Integer.MIN_VALUE ? Long.MIN_VALUE : -v;
    }
}
