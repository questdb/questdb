/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.ops.max;

import com.nfsdb.ql.Record;
import com.nfsdb.ql.impl.map.MapValues;
import com.nfsdb.ql.ops.AbstractUnaryAggregator;
import com.nfsdb.ql.ops.Function;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.ColumnType;

public final class MaxLongAggregator extends AbstractUnaryAggregator {
    public static final MaxLongAggregator FACTORY = new MaxLongAggregator();

    private MaxLongAggregator() {
        super(ColumnType.LONG);
    }

    @Override
    public void calculate(Record rec, MapValues values) {
        long v = value.getLong(rec);
        if (values.isNew() || v > values.getLong(valueIndex)) {
            values.putLong(valueIndex, v);
        }
    }

    @Override
    public Function newInstance(ObjList<VirtualColumn> args) {
        return new MaxLongAggregator();
    }
}
