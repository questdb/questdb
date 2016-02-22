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

package com.nfsdb.ql.ops;

import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.AggregatorFunction;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.impl.map.MapRecordValueInterceptor;
import com.nfsdb.ql.impl.map.MapValues;
import com.nfsdb.std.ObjList;
import com.nfsdb.std.ObjectFactory;
import com.nfsdb.store.ColumnType;

public final class AvgAggregator extends AbstractUnaryOperator implements AggregatorFunction, MapRecordValueInterceptor {

    public static final ObjectFactory<Function> FACTORY = new ObjectFactory<Function>() {
        @Override
        public Function newInstance() {
            return new AvgAggregator();
        }
    };

    private final static ColumnMetadata INTERNAL_COL_COUNT = new ColumnMetadata().setName("$count").setType(ColumnType.LONG);
    private final static ColumnMetadata INTERNAL_COL_SUM = new ColumnMetadata().setName("$sum").setType(ColumnType.DOUBLE);
    private int countIdx;
    private int sumIdx;
    private int avgIdx;

    private AvgAggregator() {
        super(ColumnType.DOUBLE);
    }

    @Override
    public void beforeRecord(MapValues values) {
        values.putDouble(avgIdx, values.getDouble(sumIdx) / values.getLong(countIdx));
    }

    @Override
    public void calculate(Record rec, MapValues values) {
        if (values.isNew()) {
            values.putLong(countIdx, 1);
            values.putDouble(sumIdx, value.getDouble(rec));
        } else {
            values.putLong(countIdx, values.getLong(countIdx) + 1);
            values.putDouble(sumIdx, values.getDouble(sumIdx) + value.getDouble(rec));
        }
    }

    @Override
    public void prepare(RecordMetadata rm, ObjList<RecordColumnMetadata> columns, int offset) {
        columns.add(INTERNAL_COL_COUNT);
        columns.add(INTERNAL_COL_SUM);
        columns.add(new ColumnMetadata().setName(getName()).setType(ColumnType.DOUBLE));
        countIdx = offset;
        sumIdx = offset + 1;
        avgIdx = offset + 2;
    }
}
