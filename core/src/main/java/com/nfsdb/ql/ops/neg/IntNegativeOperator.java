/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.ql.ops.neg;

import com.nfsdb.ql.Record;
import com.nfsdb.ql.ops.AbstractUnaryOperator;
import com.nfsdb.ql.ops.Function;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.ColumnType;

public class IntNegativeOperator extends AbstractUnaryOperator {

    public final static IntNegativeOperator FACTORY = new IntNegativeOperator();

    private IntNegativeOperator() {
        super(ColumnType.INT);
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

    @Override
    public Function newInstance(ObjList<VirtualColumn> args) {
        return new IntNegativeOperator();
    }

}
