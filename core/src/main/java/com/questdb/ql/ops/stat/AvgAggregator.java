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

package com.questdb.ql.ops.stat;

import com.questdb.ql.AggregatorFunction;
import com.questdb.ql.CollectionRecordMetadata;
import com.questdb.ql.RecordColumnMetadataImpl;
import com.questdb.ql.RecordList;
import com.questdb.ql.map.DirectMapValues;
import com.questdb.ql.map.MapRecordValueInterceptor;
import com.questdb.ql.ops.AbstractUnaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.std.Misc;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;
import com.questdb.store.RecordColumnMetadata;
import com.questdb.store.factory.configuration.ColumnMetadata;

import java.io.Closeable;

public final class AvgAggregator extends AbstractUnaryOperator implements AggregatorFunction, MapRecordValueInterceptor, Closeable {

    public static final VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new AvgAggregator(position);

    private final static RecordColumnMetadata INTERNAL_COL_TOTAL = new RecordColumnMetadataImpl("$total", ColumnType.LONG);
    private final static RecordColumnMetadata INTERNAL_COL_LOCAL_TOTAL = new RecordColumnMetadataImpl("$local_total", ColumnType.LONG);
    private final static RecordColumnMetadata INTERNAL_COL_SUM = new RecordColumnMetadataImpl("$sum", ColumnType.DOUBLE);
    private final static RecordColumnMetadata INTERNAL_COL_LIST_HEAD = new RecordColumnMetadataImpl("$listHead", ColumnType.LONG);
    private final static RecordColumnMetadata INTERNAL_COL_LIST_TAIL = new RecordColumnMetadataImpl("$listTail", ColumnType.LONG);
    private final static CollectionRecordMetadata listMetadata;
    private final RecordList records;
    private int oTotal;
    private int oLocalTotal;
    private int oSum;
    private int oListHead;
    private int oListTail;
    private int oAvg;

    private AvgAggregator(int position) {
        super(ColumnType.DOUBLE, position);
        this.records = new RecordList(listMetadata, 1024 * 1024);
    }

    @Override
    public void beforeRecord(DirectMapValues values) {
        double d = values.getDouble(oAvg);
        if (d != d) {
            computeAvg(values);
        }
    }

    @Override
    public void calculate(Record rec, DirectMapValues values) {
        long localTotal;
        double sum;

        if (values.isNew()) {
            // set initial values for new group
            values.putLong(oTotal, 1);
            values.putDouble(oAvg, Double.NaN);
            localTotal = 0;
            sum = 0;
            values.putLong(oListHead, -1);
            values.putLong(oListTail, -1);
        } else {
            // increment total record count for existing group and retrieve existing sum and partial count
            values.putLong(oTotal, values.getLong(oTotal) + 1);
            localTotal = values.getLong(oLocalTotal);
            sum = values.getDouble(oSum);
        }

        double value = this.value.getDouble(rec);
        double x = sum + value;

        // check if the new sum overflows double
        if (x == Double.POSITIVE_INFINITY || x == Double.NEGATIVE_INFINITY) {

            // save partial sum to record list
            long head = values.getLong(oListHead);
            long tail = values.getLong(oListTail);
            tail = records.beginRecord(tail);
            values.putLong(oListTail, tail);
            if (head == -1) {
                values.putLong(oListHead, tail);
            }
            records.appendDouble(sum);
            records.appendLong(localTotal);

            // reset partial sum with new value
            values.putLong(oLocalTotal, 1);
            values.putDouble(oSum, value);
        } else {

            // in case of no overflow carry on with sum and count
            values.putLong(oLocalTotal, localTotal + 1);
            values.putDouble(oSum, x);
        }
    }

    @Override
    public void clear() {
        records.clear();
    }

    @Override
    public void prepare(ObjList<RecordColumnMetadata> columns, int offset) {
        columns.add(INTERNAL_COL_TOTAL);
        columns.add(INTERNAL_COL_LOCAL_TOTAL);
        columns.add(INTERNAL_COL_SUM);
        columns.add(INTERNAL_COL_LIST_HEAD);
        columns.add(INTERNAL_COL_LIST_TAIL);
        columns.add(new ColumnMetadata().setName(getName()).setType(ColumnType.DOUBLE));
        oTotal = offset;
        oLocalTotal = offset + 1;
        oSum = offset + 2;
        oListHead = offset + 3;
        oListTail = offset + 4;
        oAvg = offset + 5;
    }

    @Override
    public void close() {
        Misc.free(records);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    private void computeAvg(DirectMapValues values) {
        long ref = values.getLong(oListHead);
        if (ref == -1) {
            // no partial sums found, compute average of what we have accumulated so far
            values.putDouble(oAvg, values.getDouble(oSum) / (double) values.getLong(oLocalTotal));
        } else {
            // there are partial sums
            // compute average of last partial sum (which hasn't been added to record list)
            double total = (double) values.getLong(oTotal);
            double count = (double) values.getLong(oLocalTotal);
            double avg = (count / total) * (values.getDouble(oSum) / count);

            // add partial sums together using weight of each sum
            // weight = partial count / total count
            records.of(ref);
            while (records.hasNext()) {
                Record r = records.next();
                count = r.getLong(1);
                avg += (count / total) * (r.getDouble(0) / count);
            }
            values.putDouble(oAvg, avg);
        }
    }

    static {
        listMetadata = new CollectionRecordMetadata();
        listMetadata.add(new RecordColumnMetadataImpl("sum", ColumnType.DOUBLE));
        listMetadata.add(new RecordColumnMetadataImpl("count", ColumnType.LONG));
    }
}
