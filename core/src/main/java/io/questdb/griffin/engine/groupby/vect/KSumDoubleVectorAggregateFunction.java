/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.Misc;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.util.Arrays;

import static io.questdb.griffin.SqlCodeGenerator.GKK_HOUR_INT;

public class KSumDoubleVectorAggregateFunction extends DoubleFunction implements VectorAggregateFunction {

    private final int columnIndex;
    private final double[] sum;
    private final long[] count;
    private final int workerCount;
    private final DistinctFunc distinctFunc;
    private final KeyValueFunc keyValueFunc;
    private int valueOffset;

    public KSumDoubleVectorAggregateFunction(int keyKind, int columnIndex, int workerCount) {
        this.columnIndex = columnIndex;
        this.sum = new double[workerCount * Misc.CACHE_LINE_SIZE];
        this.count = new long[workerCount * Misc.CACHE_LINE_SIZE];
        this.workerCount = workerCount;
        if (keyKind == GKK_HOUR_INT) {
            this.distinctFunc = Rosti::keyedHourDistinct;
            this.keyValueFunc = Rosti::keyedHourKSumDouble;
        } else {
            this.keyValueFunc = Rosti::keyedIntKSumDouble;
            this.distinctFunc = Rosti::keyedIntDistinct;
        }
    }

    @Override
    public void aggregate(long address, long addressSize, int columnSizeHint, int workerId) {
        if (address != 0) {
            // Kahan compensated summation
            final double x = Vect.sumDoubleKahan(address, addressSize / Double.BYTES);
            if (x == x) {
                final int offset = workerId * Misc.CACHE_LINE_SIZE;
                final double sum = this.sum[offset];
                final double y = x - this.sum[offset + 1]; // y = x - c
                final double t = sum + y; // t = sum + y
                this.sum[offset + 1] = t - sum - y; // c = t - sum - y
                this.sum[offset] = t; // sum = t
                this.count[offset]++;
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
        Unsafe.getUnsafe().putDouble(Rosti.getInitialValueSlot(pRosti, valueOffset), 0.0);
        Unsafe.getUnsafe().putDouble(Rosti.getInitialValueSlot(pRosti, valueOffset + 1), 0.0);
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset + 2), 0);
    }

    @Override
    public void merge(long pRostiA, long pRostiB) {
        Rosti.keyedIntKSumDoubleMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.DOUBLE); // sum
        types.add(ColumnType.DOUBLE); // c
        types.add(ColumnType.LONG); // count
    }

    @Override
    public void wrapUp(long pRosti) {
        double sum = 0;
        long count = 0;
        double c = 0;
        for (int i = 0; i < workerCount; i++) {
            final int offset = i * Misc.CACHE_LINE_SIZE;
            double y = this.sum[offset] - c;
            double t = sum + y;
            c = t - sum - y;
            sum = t;
            count += this.count[offset];
        }
        Rosti.keyedIntKSumDoubleWrapUp(pRosti, valueOffset, sum, count);
    }

    @Override
    public void clear() {
        Arrays.fill(sum, 0);
        Arrays.fill(count, 0);
    }

    @Override
    public double getDouble(Record rec) {
        double sum = 0;
        long count = 0;
        double c = 0;
        for (int i = 0; i < workerCount; i++) {
            final int offset = i * Misc.CACHE_LINE_SIZE;
            double y = this.sum[offset] - c;
            double t = sum + y;
            c = t - sum - y;
            sum = t;
            count += this.count[offset];
        }
        return count > 0 ? sum : Double.NaN;
    }
}
