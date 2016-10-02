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

package com.questdb.ql.ops.stat;

import com.questdb.factory.configuration.ColumnMetadata;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.ql.AggregatorFunction;
import com.questdb.ql.Record;
import com.questdb.ql.impl.RecordColumnMetadataImpl;
import com.questdb.ql.impl.map.DirectMapValues;
import com.questdb.ql.impl.map.MapRecordValueInterceptor;
import com.questdb.ql.ops.AbstractUnaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;

public final class StableAvgAggregator extends AbstractUnaryOperator implements AggregatorFunction, MapRecordValueInterceptor {

    public static final VirtualColumnFactory<Function> FACTORY = new VirtualColumnFactory<Function>() {
        @Override
        public Function newInstance(int position) {
            return new StableAvgAggregator(position);
        }
    };

    private final static RecordColumnMetadata INTERNAL_COL_TOTAL = new RecordColumnMetadataImpl("$total", ColumnType.LONG);
    private final static RecordColumnMetadata INTERNAL_COL_LOCAL_TOTAL = new RecordColumnMetadataImpl("$local_total", ColumnType.LONG);
    private final static RecordColumnMetadata INTERNAL_COL_SUM = new RecordColumnMetadataImpl("$sum", ColumnType.DOUBLE);

    private int oTotal;
    private int oLocalTotal;
    private int oSum;
    private int oAvg;
    private int pass;

    private StableAvgAggregator(int position) {
        super(ColumnType.DOUBLE, position);
    }

    @Override
    public void beforeRecord(DirectMapValues values) {
        calcLocalAvgAndUpdate(values.getLong(oLocalTotal), values.getDouble(oSum), values);
    }

    @Override
    public void calculate(Record rec, DirectMapValues values) {
        switch (pass) {
            case 1:
                count(values);
                break;
            default:
                avg(rec, values);
                break;
        }
    }

    @Override
    public void prepare(ObjList<RecordColumnMetadata> columns, int offset) {
        columns.add(INTERNAL_COL_TOTAL);
        columns.add(INTERNAL_COL_LOCAL_TOTAL);
        columns.add(INTERNAL_COL_SUM);
        columns.add(new ColumnMetadata().setName(getName()).setType(ColumnType.DOUBLE));
        oTotal = offset;
        oLocalTotal = offset + 1;
        oSum = offset + 2;
        oAvg = offset + 3;
    }

    public void onIterationBegin(int pass) {
        this.pass = pass;
    }

    @Override
    public int getPassCount() {
        return 2;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    private void avg(Record r, DirectMapValues values) {
        long localTotal = values.getLong(oLocalTotal);
        double sum = values.getDouble(oSum);
        double value = this.value.getDouble(r);

        double x = sum + value;
        if (x == Double.POSITIVE_INFINITY || x == Double.NEGATIVE_INFINITY) {
            // we would overflow if we keep adding sum.
            // calculate avg from what we have so far and restart local sum
            calcLocalAvgAndUpdate(localTotal, sum, values);
            values.putLong(oLocalTotal, 1);
            values.putDouble(oSum, value);
        } else {
            values.putLong(oLocalTotal, localTotal + 1);
            values.putDouble(oSum, x);
        }
    }

    private void calcLocalAvgAndUpdate(long localTotal, double sum, DirectMapValues values) {
        double alpha = localTotal / (double) values.getLong(oTotal);
        double localAvg = sum / localTotal;
        double avg = values.getDouble(oAvg);
        avg += alpha * localAvg;
        values.putDouble(oAvg, avg);
    }

    private void count(DirectMapValues values) {
        if (values.isNew()) {
            values.putLong(oTotal, 1);
            values.putDouble(oSum, 0);
            values.putDouble(oAvg, 0);
            values.putLong(oLocalTotal, 0);
        } else {
            values.putLong(oTotal, values.getLong(oTotal) + 1);
        }
    }
}
