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

package com.questdb.ql.ops;

import com.questdb.factory.configuration.ColumnMetadata;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.ql.AggregatorFunction;
import com.questdb.ql.Record;
import com.questdb.ql.impl.RecordColumnMetadataImpl;
import com.questdb.ql.impl.map.DirectMapValues;
import com.questdb.ql.impl.map.MapRecordValueInterceptor;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;

public final class AvgAggregator extends AbstractUnaryOperator implements AggregatorFunction, MapRecordValueInterceptor {

    public static final VirtualColumnFactory<Function> FACTORY = new VirtualColumnFactory<Function>() {
        @Override
        public Function newInstance(int position) {
            return new AvgAggregator(position);
        }
    };

    private final static RecordColumnMetadata INTERNAL_COL_COUNT = new RecordColumnMetadataImpl("$count", ColumnType.LONG);
    private final static RecordColumnMetadata INTERNAL_COL_SUM = new RecordColumnMetadataImpl("$sum", ColumnType.DOUBLE);
    private int countIdx;
    private int sumIdx;
    private int avgIdx;

    private AvgAggregator(int position) {
        super(ColumnType.DOUBLE, position);
    }

    @Override
    public void beforeRecord(DirectMapValues values) {
        values.putDouble(avgIdx, values.getDouble(sumIdx) / values.getLong(countIdx));
    }

    @Override
    public void calculate(Record rec, DirectMapValues values) {
        if (values.isNew()) {
            values.putLong(countIdx, 1);
            values.putDouble(sumIdx, value.getDouble(rec));
        } else {
            values.putLong(countIdx, values.getLong(countIdx) + 1);
            values.putDouble(sumIdx, values.getDouble(sumIdx) + value.getDouble(rec));
        }
    }

    @Override
    public void prepare(ObjList<RecordColumnMetadata> columns, int offset) {
        columns.add(INTERNAL_COL_COUNT);
        columns.add(INTERNAL_COL_SUM);
        columns.add(new ColumnMetadata().setName(getName()).setType(ColumnType.DOUBLE));
        countIdx = offset;
        sumIdx = offset + 1;
        avgIdx = offset + 2;
    }

    @Override
    public boolean isConstant() {
        return false;
    }
}
