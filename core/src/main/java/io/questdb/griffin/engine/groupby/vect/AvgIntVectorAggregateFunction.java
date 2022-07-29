/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.groupby.vect;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;

import java.io.Closeable;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

import static io.questdb.griffin.SqlCodeGenerator.GKK_HOUR_INT;

public class AvgIntVectorAggregateFunction extends DoubleFunction implements VectorAggregateFunction, Closeable {

    private final DoubleAdder sum = new DoubleAdder();
    private final LongAdder count = new LongAdder();
    private final int columnIndex;
    private final DistinctFunc distinctFunc;
    private final KeyValueFunc keyValueFunc;
    private final int workerCount;
    private int valueOffset;
    private long counts;

    public AvgIntVectorAggregateFunction(int keyKind, int columnIndex, int workerCount) {
        this.columnIndex = columnIndex;
        if (keyKind == GKK_HOUR_INT) {
            distinctFunc = Rosti::keyedHourDistinct;
            keyValueFunc = Rosti::keyedHourSumInt;
        } else {
            distinctFunc = Rosti::keyedIntDistinct;
            keyValueFunc = Rosti::keyedIntSumInt;
        }

        counts = Unsafe.malloc((long) workerCount * Misc.CACHE_LINE_SIZE, MemoryTag.NATIVE_DEFAULT);
        this.workerCount = workerCount;
    }

    @Override
    public void aggregate(long address, long addressSize, int columnSizeHint, int workerId) {
        if (address != 0) {
            final double value = Vect.avgIntAcc(address, addressSize / Integer.BYTES, counts + (long) workerId * Misc.CACHE_LINE_SIZE);
            if (value == value) {
                final long count = Unsafe.getUnsafe().getLong(counts + (long) workerId * Misc.CACHE_LINE_SIZE);
                // we have to include "weight" of this avg value in the formula,
                // which calculates final result
                sum.add(value * count);
                this.count.add(count);
            }
        }
    }

    @Override
    public boolean aggregate(long pRosti, long keyAddress, long valueAddress, long valueAddressSize, int columnSizeShr, int workerId) {
        if (valueAddress == 0) {
            return distinctFunc.run(pRosti, keyAddress, valueAddressSize / Integer.BYTES);
        } else {
            return keyValueFunc.run(pRosti, keyAddress, valueAddress, valueAddressSize / Integer.BYTES, valueOffset);
        }
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public int getValueOffset() {
        return valueOffset;
    }

    @Override
    public void initRosti(long pRosti) {
        // as for sums, except we space out values
        // we will have to replace them with doubles
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset), 0);
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset + 1), 0);
    }

    @Override
    public void merge(long pRostiA, long pRostiB) {
        Rosti.keyedIntSumIntMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.DOUBLE);
        types.add(ColumnType.LONG);
    }

    @Override
    public void wrapUp(long pRosti) {
        Rosti.keyedIntAvgLongWrapUp(pRosti, valueOffset, sum.sum(), count.sum());
    }

    @Override
    public void clear() {
        sum.reset();
        count.reset();
    }

    @Override
    public void close() {
        if (counts != 0) {
            Unsafe.free(counts, (long) workerCount * Misc.CACHE_LINE_SIZE, MemoryTag.NATIVE_DEFAULT);
            counts = 0;
        }
        super.close();
    }

    @Override
    public double getDouble(Record rec) {
        final long count = this.count.sum();
        if (count > 0) {
            return sum.sum() / count;
        }
        return Double.NaN;
    }

    @Override
    public boolean isReadThreadSafe() {
        return false;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put("AvgIntVector(").put(columnIndex).put(')');
    }
}
