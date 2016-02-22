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

public final class VwapAggregator extends AbstractBinaryOperator implements AggregatorFunction, MapRecordValueInterceptor {

    public static final ObjectFactory<Function> FACTORY = new ObjectFactory<Function>() {
        @Override
        public Function newInstance() {
            return new VwapAggregator();
        }
    };

    private final static ColumnMetadata INTERNAL_COL_AMOUNT = new ColumnMetadata().setName("$sumAmt").setType(ColumnType.DOUBLE);
    private final static ColumnMetadata INTERNAL_COL_QUANTITY = new ColumnMetadata().setName("$sumQty").setType(ColumnType.DOUBLE);
    private int sumAmtIdx;
    private int sumQtyIdx;
    private int vwap;

    private VwapAggregator() {
        super(ColumnType.DOUBLE);
    }

    @Override
    public void beforeRecord(MapValues values) {
        values.putDouble(vwap, values.getDouble(sumAmtIdx) / values.getDouble(sumQtyIdx));
    }

    @Override
    public void calculate(Record rec, MapValues values) {
        double price = lhs.getDouble(rec);
        double quantity = rhs.getDouble(rec);
        if (values.isNew()) {
            values.putDouble(sumAmtIdx, price * quantity);
            values.putDouble(sumQtyIdx, quantity);
        } else {
            values.putDouble(sumAmtIdx, values.getDouble(sumAmtIdx) + price * quantity);
            values.putDouble(sumQtyIdx, values.getDouble(sumQtyIdx) + quantity);
        }
    }

    @Override
    public void prepare(RecordMetadata metadata, ObjList<RecordColumnMetadata> columns, int offset) {
        columns.add(INTERNAL_COL_AMOUNT);
        columns.add(INTERNAL_COL_QUANTITY);
        columns.add(new ColumnMetadata().setName(getName()).setType(ColumnType.DOUBLE));
        sumAmtIdx = offset;
        sumQtyIdx = offset + 1;
        vwap = offset + 2;
    }
}
