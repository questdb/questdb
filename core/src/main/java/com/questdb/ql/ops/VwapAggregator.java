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

package com.questdb.ql.ops;

import com.questdb.ql.AggregatorFunction;
import com.questdb.ql.RecordColumnMetadataImpl;
import com.questdb.ql.map.DirectMapValues;
import com.questdb.ql.map.MapRecordValueInterceptor;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;
import com.questdb.store.RecordColumnMetadata;
import com.questdb.store.factory.configuration.ColumnMetadata;

public final class VwapAggregator extends AbstractBinaryOperator implements AggregatorFunction, MapRecordValueInterceptor {

    public static final VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new VwapAggregator(position);

    private final static RecordColumnMetadata INTERNAL_COL_AMOUNT = new RecordColumnMetadataImpl("$sumAmt", ColumnType.DOUBLE);
    private final static RecordColumnMetadata INTERNAL_COL_QUANTITY = new RecordColumnMetadataImpl("$sumQty", ColumnType.DOUBLE);
    private int sumAmtIdx;
    private int sumQtyIdx;
    private int vwap;

    private VwapAggregator(int position) {
        super(ColumnType.DOUBLE, position);
    }

    @Override
    public void beforeRecord(DirectMapValues values) {
        values.putDouble(vwap, values.getDouble(sumAmtIdx) / values.getDouble(sumQtyIdx));
    }

    @Override
    public void calculate(Record rec, DirectMapValues values) {
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
    public void clear() {
    }

    @Override
    public void prepare(ObjList<RecordColumnMetadata> columns, int offset) {
        columns.add(INTERNAL_COL_AMOUNT);
        columns.add(INTERNAL_COL_QUANTITY);
        columns.add(new ColumnMetadata().setName(getName()).setType(ColumnType.DOUBLE));
        sumAmtIdx = offset;
        sumQtyIdx = offset + 1;
        vwap = offset + 2;
    }

    @Override
    public boolean isConstant() {
        return false;
    }
}
