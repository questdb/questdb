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
import com.questdb.misc.Misc;
import com.questdb.ql.AggregatorFunction;
import com.questdb.ql.Record;
import com.questdb.ql.impl.CollectionRecordMetadata;
import com.questdb.ql.impl.RecordColumnMetadataImpl;
import com.questdb.ql.impl.RecordList;
import com.questdb.ql.impl.map.DirectMapValues;
import com.questdb.ql.impl.map.MapRecordValueInterceptor;
import com.questdb.ql.ops.AbstractUnaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;
import com.sun.xml.internal.ws.Closeable;

public final class AvgAggregator extends AbstractUnaryOperator implements AggregatorFunction, MapRecordValueInterceptor, Closeable {

    public static final VirtualColumnFactory<Function> FACTORY = new VirtualColumnFactory<Function>() {
        @Override
        public Function newInstance(int position) {
            return new AvgAggregator(position);
        }
    };

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
        long ref = values.getLong(oListHead);
        if (ref == -1) {
            values.putDouble(oAvg, values.getDouble(oSum) / (double) values.getLong(oLocalTotal));
        } else {
            double total = (double) values.getLong(oTotal);
            double count = (double) values.getLong(oLocalTotal);
            double avg = (count / total) * (values.getDouble(oSum) / count);
            records.of(ref);
            while (records.hasNext()) {
                Record r = records.next();
                double sum = r.getDouble(0);
                count = r.getLong(1);
                avg += (count / total) * (sum / count);
            }
            values.putDouble(oAvg, avg);
        }
    }

    @Override
    public void calculate(Record rec, DirectMapValues values) {
        long localTotal;
        double sum;

        if (values.isNew()) {
            values.putLong(oTotal, 1);
            values.putDouble(oAvg, 0);
            localTotal = 0;
            sum = 0;
            values.putLong(oListHead, -1);
            values.putLong(oListTail, -1);
        } else {
            values.putLong(oTotal, values.getLong(oTotal) + 1);
            localTotal = values.getLong(oLocalTotal);
            sum = values.getDouble(oSum);
        }

        double value1 = this.value.getDouble(rec);
        double x = sum + value1;

        if (x == Double.POSITIVE_INFINITY || x == Double.NEGATIVE_INFINITY) {
            long head = values.getLong(oListHead);
            long tail = values.getLong(oListTail);
            tail = records.beginRecord(tail);
            values.putLong(oListTail, tail);
            if (head == -1) {
                values.putLong(oListHead, tail);
            }

            records.appendDouble(sum);
            records.appendLong(localTotal);
            values.putLong(oLocalTotal, 1);
            values.putDouble(oSum, value1);
        } else {
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

    static {
        listMetadata = new CollectionRecordMetadata();
        listMetadata.add(new RecordColumnMetadataImpl("sum", ColumnType.DOUBLE));
        listMetadata.add(new RecordColumnMetadataImpl("count", ColumnType.LONG));
    }
}
