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

package com.nfsdb.ql.ops.count;

import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.AggregatorFunction;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.impl.map.MapValues;
import com.nfsdb.ql.ops.AbstractVirtualColumn;
import com.nfsdb.ql.ops.Function;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.std.ObjList;
import com.nfsdb.std.ObjectFactory;
import com.nfsdb.store.ColumnType;

public final class CountAggregator extends AbstractVirtualColumn implements AggregatorFunction, Function {

    public static final ObjectFactory<Function> FACTORY = new ObjectFactory<Function>() {
        @Override
        public Function newInstance() {
            return new CountAggregator();
        }
    };

    private int index;

    private CountAggregator() {
        super(ColumnType.LONG);
    }

    @Override
    public void calculate(Record rec, MapValues values) {
        if (values.isNew()) {
            values.putLong(index, 1);
        } else {
            values.putLong(index, values.getLong(index) + 1);
        }
    }

    @Override
    public void prepare(RecordMetadata metadata, ObjList<RecordColumnMetadata> columns, int offset) {
        columns.add(this);
        index = offset;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public void prepare(StorageFacade facade) {
    }

    @Override
    public void setArg(int pos, VirtualColumn arg) {
    }
}
