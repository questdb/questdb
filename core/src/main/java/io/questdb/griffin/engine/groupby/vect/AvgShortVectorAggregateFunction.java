/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

import static io.questdb.griffin.SqlCodeGenerator.GKK_MICRO_HOUR_INT;
import static io.questdb.griffin.SqlCodeGenerator.GKK_NANO_HOUR_INT;

public class AvgShortVectorAggregateFunction extends DoubleFunction implements VectorAggregateFunction {

    private final int columnIndex;
    private final LongAdder count = new LongAdder();
    private final DistinctFunc distinctFunc;
    private final KeyValueFunc keyValueFunc;
    private final DoubleAdder sum = new DoubleAdder();
    private final int workerCount;
    private long countsAddr;
    private int valueOffset;

    public AvgShortVectorAggregateFunction(int keyKind, int columnIndex, int workerCount) {
        this.columnIndex = columnIndex;
        if (keyKind == GKK_MICRO_HOUR_INT) {
            distinctFunc = Rosti::keyedMicroHourDistinct;
            keyValueFunc = Rosti::keyedMicroHourSumShortLong;
        } else if (keyKind == GKK_NANO_HOUR_INT) {
            distinctFunc = Rosti::keyedNanoHourDistinct;
            keyValueFunc = Rosti::keyedNanoHourSumShortLong;
        } else {
            distinctFunc = Rosti::keyedIntDistinct;
            keyValueFunc = Rosti::keyedIntSumShortLong;
        }
        countsAddr = Unsafe.malloc((long) workerCount * Misc.CACHE_LINE_SIZE, MemoryTag.NATIVE_FUNC_RSS);
        this.workerCount = workerCount;
    }

    @Override
    public void aggregate(long address, long frameRowCount, int workerId) {
        if (address != 0) {
            final double value = Vect.avgShortAcc(address, frameRowCount, countsAddr + (long) workerId * Misc.CACHE_LINE_SIZE);
            if (Numbers.isFinite(value)) {
                final long count = Unsafe.getUnsafe().getLong(countsAddr + (long) workerId * Misc.CACHE_LINE_SIZE);
                // we have to include "weight" of this avg value in the formula,
                // which calculates final result
                sum.add(value * count);
                this.count.add(count);
            }
        }
    }

    @Override
    public boolean aggregate(long pRosti, long keyAddress, long valueAddress, long frameRowCount) {
        if (valueAddress == 0) {
            return distinctFunc.run(pRosti, keyAddress, frameRowCount);
        } else {
            return keyValueFunc.run(pRosti, keyAddress, valueAddress, frameRowCount, valueOffset);
        }
    }

    @Override
    public void clear() {
        sum.reset();
        count.reset();
    }

    @Override
    public void close() {
        if (countsAddr != 0) {
            countsAddr = Unsafe.free(countsAddr, (long) workerCount * Misc.CACHE_LINE_SIZE, MemoryTag.NATIVE_FUNC_RSS);
        }
        super.close();
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
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
    public String getName() {
        return "avg";
    }

    @Override
    public int getValueOffset() {
        return valueOffset;
    }

    @Override
    public void initRosti(long pRosti) {
        // although the final values are double, avg() calculates sum and count for longs
        // double is derived at the very end. The initial values need to be set
        // correctly with long sum and count in mind.
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset), 0);
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset + 1), 0);
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset + 2), 0);
    }

    @Override
    public boolean merge(long pRostiA, long pRostiB) {
        return Rosti.keyedIntSumLongLongMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.LONG); // accumulator low part
        types.add(ColumnType.LONG); // accumulator high part
        types.add(ColumnType.LONG); // count
    }

    @Override
    public boolean wrapUp(long pRosti) {
        return Rosti.keyedIntAvgLongLongWrapUp(pRosti, valueOffset, sum.sum(), count.sum());
    }
}
