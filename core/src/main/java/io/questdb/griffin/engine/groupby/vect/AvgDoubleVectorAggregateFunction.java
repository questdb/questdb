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
import io.questdb.std.MemoryTag;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

import static io.questdb.griffin.SqlCodeGenerator.GKK_HOUR_INT;

public class AvgDoubleVectorAggregateFunction extends DoubleFunction implements VectorAggregateFunction {

    private final DoubleAdder sum = new DoubleAdder();
    private final LongAdder count = new LongAdder();
    private final int columnIndex;
    private final DistinctFunc distinctFunc;
    private final KeyValueFunc keyValueFunc;
    private final int workerCount;
    private int valueOffset;
    private long counts;

    public AvgDoubleVectorAggregateFunction(int keyKind, int columnIndex, int workerCount) {
        this.columnIndex = columnIndex;
        if (keyKind == GKK_HOUR_INT) {
            distinctFunc = Rosti::keyedHourDistinct;
            keyValueFunc = Rosti::keyedHourSumDouble;
        } else {
            distinctFunc = Rosti::keyedIntDistinct;
            keyValueFunc = Rosti::keyedIntSumDouble;
        }
        counts = Unsafe.malloc(workerCount * 8L, MemoryTag.NATIVE_DEFAULT);
        this.workerCount = workerCount;
    }

    @Override
    public void aggregate(long address, long addressSize, int columnSizeHint, int workerId) {
        if (address != 0) {
            double value = Vect.avgDoubleAcc(address, addressSize / Double.BYTES, counts + workerId * 8L);
            if (value == value) {
                final long count = Unsafe.getUnsafe().getLong(counts + workerId * 8L);
                // we have to include "weight" of this avg value in the formula,
                // which calculates final result
                sum.add(value * count);
                this.count.add(count);
            }
        }
    }

    @Override
    public void aggregate(long pRosti, long keyAddress, long valueAddress, long valueAddressSize, int columnSizeShr, int workerId) {
        if (valueAddress == 0) {
            distinctFunc.run(pRosti, keyAddress, valueAddressSize / Double.BYTES);
        } else {
            keyValueFunc.run(pRosti, keyAddress, valueAddress, valueAddressSize / Double.BYTES, valueOffset);
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
        Unsafe.getUnsafe().putDouble(Rosti.getInitialValueSlot(pRosti, this.valueOffset), 0);
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, this.valueOffset + 1), 0);
    }

    @Override
    public void merge(long pRostiA, long pRostiB) {
        Rosti.keyedIntSumDoubleMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.DOUBLE);
        types.add(ColumnType.LONG);
    }

    @Override
    public void wrapUp(long pRosti) {
        Rosti.keyedIntAvgDoubleWrapUp(pRosti, valueOffset, this.sum.sum(), this.count.sum());
    }

    @Override
    public void clear() {
        sum.reset();
        count.reset();
    }

    @Override
    public void close() {
        if (counts != 0) {
            Unsafe.free(counts, workerCount * 8L, MemoryTag.NATIVE_DEFAULT);
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
    public boolean isStateless() {
        // group-by functions are not stateless when values are computed
        // however, once values are calculated, the read becomes stateless
        return true;
    }
}
