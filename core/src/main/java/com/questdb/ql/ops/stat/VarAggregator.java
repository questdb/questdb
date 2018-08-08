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

import com.questdb.ServerConfiguration;
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

public class VarAggregator extends AbstractUnaryOperator implements AggregatorFunction, MapRecordValueInterceptor, Closeable {

    public static final VirtualColumnFactory<Function> FACTORY = (position, env) -> new VarAggregator(position, env.configuration);
    private final static RecordColumnMetadata INTERNAL_COL_TOTAL = new RecordColumnMetadataImpl("$total", ColumnType.LONG);
    private final static RecordColumnMetadata INTERNAL_COL_LOCAL_TOTAL = new RecordColumnMetadataImpl("$part_total", ColumnType.LONG);
    private final static RecordColumnMetadata INTERNAL_COL_SUM = new RecordColumnMetadataImpl("$part_sum", ColumnType.DOUBLE);
    private final static RecordColumnMetadata INTERNAL_COL_MEAN_HEAD = new RecordColumnMetadataImpl("$meanHead", ColumnType.LONG);
    private final static RecordColumnMetadata INTERNAL_COL_MEAN_TAIL = new RecordColumnMetadataImpl("$meanTail", ColumnType.LONG);
    private final static RecordColumnMetadata INTERNAL_COL_VALUES_HEAD = new RecordColumnMetadataImpl("$valuesHead", ColumnType.LONG);
    private final static RecordColumnMetadata INTERNAL_COL_VALUES_TAIL = new RecordColumnMetadataImpl("$valuesTail", ColumnType.LONG);
    private final static CollectionRecordMetadata meanPartialsMetadata;
    private final static CollectionRecordMetadata sourceMetadata;
    private final RecordList meanPartials;
    private final RecordList srcRecords;
    private int oTotal;
    private int oPartialTotal;
    private int oPartialSum;
    private int oPartialHead;
    private int oPartialTail;
    private int oValuesHead;
    private int oValuesTail;
    private int oVariance;

    protected VarAggregator(int position, ServerConfiguration configuration) {
        super(ColumnType.DOUBLE, position);
        this.meanPartials = new RecordList(meanPartialsMetadata, configuration.getDbFnVarianceMeans());
        this.srcRecords = new RecordList(sourceMetadata, configuration.getDbFnVarianceData());
    }

    @Override
    public void beforeRecord(DirectMapValues values) {
        double r = getResult(values);
        if (r != r) {
            r = computeVar(values);
            storeResult(values, r);
        }
    }

    @Override
    public void calculate(Record rec, DirectMapValues values) {
        long localTotal;
        double sum;

        // this code computes mean, total and stores values for further processing

        if (values.isNew()) {
            values.putLong(oTotal, 1);
            values.putDouble(oVariance, Double.NaN);
            localTotal = 0;
            sum = 0;
            values.putLong(oPartialHead, -1);
            values.putLong(oPartialTail, -1);
            values.putLong(oValuesHead, -1);
            values.putLong(oValuesTail, -1);
        } else {
            values.putLong(oTotal, values.getLong(oTotal) + 1);
            localTotal = values.getLong(oPartialTotal);
            sum = values.getDouble(oPartialSum);
        }

        double value = this.value.getDouble(rec);
        double x = sum + value;

        if (x == Double.POSITIVE_INFINITY || x == Double.NEGATIVE_INFINITY) {
            beginRecord(values, oPartialHead, oPartialTail, meanPartials);
            meanPartials.appendDouble(sum);
            meanPartials.appendLong(localTotal);

            values.putLong(oPartialTotal, 1);
            values.putDouble(oPartialSum, value);
        } else {
            values.putLong(oPartialTotal, localTotal + 1);
            values.putDouble(oPartialSum, x);
        }

        beginRecord(values, oValuesHead, oValuesTail, srcRecords);
        srcRecords.appendDouble(value);
    }

    @Override
    public void clear() {
        meanPartials.clear();
        srcRecords.clear();
    }

    @Override
    public void prepare(ObjList<RecordColumnMetadata> columns, int offset) {
        columns.add(INTERNAL_COL_TOTAL);
        columns.add(INTERNAL_COL_LOCAL_TOTAL);
        columns.add(INTERNAL_COL_SUM);
        columns.add(INTERNAL_COL_MEAN_HEAD);
        columns.add(INTERNAL_COL_MEAN_TAIL);
        columns.add(INTERNAL_COL_VALUES_HEAD);
        columns.add(INTERNAL_COL_VALUES_TAIL);
        columns.add(new ColumnMetadata().setName(getName()).setType(ColumnType.DOUBLE));
        oTotal = offset;
        oPartialTotal = offset + 1;
        oPartialSum = offset + 2;
        oPartialHead = offset + 3;
        oPartialTail = offset + 4;
        oValuesHead = offset + 5;
        oValuesTail = offset + 6;
        oVariance = offset + 7;
    }

    @Override
    public void close() {
        Misc.free(meanPartials);
        Misc.free(srcRecords);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    private static void beginRecord(DirectMapValues values, int oHead, int oTail, RecordList list) {
        long head = values.getLong(oHead);
        long tail = values.getLong(oTail);
        tail = list.beginRecord(tail);
        values.putLong(oTail, tail);
        if (head == -1) {
            values.putLong(oHead, tail);
        }
    }

    protected double computeVar(DirectMapValues values) {
        long ref = values.getLong(oPartialHead);
        double total;
        // calculate mean
        double mean;
        if (ref == -1) {
            total = (double) values.getLong(oPartialTotal);
            mean = values.getDouble(oPartialSum) / total;
        } else {
            total = (double) values.getLong(oTotal);
            double count = (double) values.getLong(oPartialTotal);
            mean = (count / total) * (values.getDouble(oPartialSum) / count);
            meanPartials.of(ref);
            while (meanPartials.hasNext()) {
                Record r = meanPartials.next();
                double sum = r.getDouble(0);
                count = r.getLong(1);
                mean += (count / total) * (sum / count);
            }
        }

        //

        double variance = 0;
        double partialSum = 0;
        long partialCount = 0;
        srcRecords.of(values.getLong(oValuesHead));
        while (srcRecords.hasNext()) {
            Record r = srcRecords.next();
            double d = r.getDouble(0);
            double x = (mean - d) * (mean - d);
            double y = x + partialSum;

            if (y == Double.POSITIVE_INFINITY || y == Double.NEGATIVE_INFINITY) {
                variance += (partialCount / total) * (partialSum / (double) partialCount);
                partialSum = x;
                partialCount = 1;
            } else {
                partialSum = y;
                partialCount++;
            }
        }
        variance += (partialCount / total) * (partialSum / (double) partialCount);
        return variance;
    }

    protected double getResult(DirectMapValues values) {
        return values.getDouble(oVariance);
    }

    protected void storeResult(DirectMapValues values, double value) {
        values.putDouble(oVariance, value);
    }

    static {
        meanPartialsMetadata = new CollectionRecordMetadata();
        meanPartialsMetadata.add(new RecordColumnMetadataImpl("sum", ColumnType.DOUBLE));
        meanPartialsMetadata.add(new RecordColumnMetadataImpl("count", ColumnType.LONG));

        sourceMetadata = new CollectionRecordMetadata();
        sourceMetadata.add(new RecordColumnMetadataImpl("value", ColumnType.DOUBLE));
    }
}
