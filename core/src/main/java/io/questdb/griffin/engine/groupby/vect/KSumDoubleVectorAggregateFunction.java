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
import io.questdb.std.Misc;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.util.Arrays;

import static io.questdb.griffin.SqlCodeGenerator.GKK_MICRO_HOUR_INT;
import static io.questdb.griffin.SqlCodeGenerator.GKK_NANO_HOUR_INT;

public class KSumDoubleVectorAggregateFunction extends DoubleFunction implements VectorAggregateFunction {
    private static final int COUNT_PADDING = Misc.CACHE_LINE_SIZE / Long.BYTES;
    // We're using two double values per worker, hence +1 element in the padding.
    private static final int SUM_PADDING = (Misc.CACHE_LINE_SIZE / Double.BYTES) + 1;
    private final int columnIndex;
    private final long[] count;
    private final DistinctFunc distinctFunc;
    private final KeyValueFunc keyValueFunc;
    private final double[] sum;
    private final int workerCount;
    private int valueOffset;

    public KSumDoubleVectorAggregateFunction(int keyKind, int columnIndex, int workerCount) {
        this.columnIndex = columnIndex;
        this.sum = new double[workerCount * SUM_PADDING];
        this.count = new long[workerCount * COUNT_PADDING];
        this.workerCount = workerCount;
        if (keyKind == GKK_MICRO_HOUR_INT) {
            this.distinctFunc = Rosti::keyedMicroHourDistinct;
            this.keyValueFunc = Rosti::keyedMicroHourKSumDouble;
        } else if (keyKind == GKK_NANO_HOUR_INT) {
            this.distinctFunc = Rosti::keyedNanoHourDistinct;
            this.keyValueFunc = Rosti::keyedNanoHourKSumDouble;
        } else {
            this.keyValueFunc = Rosti::keyedIntKSumDouble;
            this.distinctFunc = Rosti::keyedIntDistinct;
        }
    }

    @Override
    public void aggregate(long address, long frameRowCount, int workerId) {
        if (address != 0) {
            // Kahan compensated summation
            final double x = Vect.sumDoubleKahan(address, frameRowCount);
            if (x == x) {
                final int sumOffset = workerId * SUM_PADDING;
                final double sum = this.sum[sumOffset];
                final double y = x - this.sum[sumOffset + 1]; // y = x - c
                final double t = sum + y; // t = sum + y
                this.sum[sumOffset + 1] = t - sum - y; // c = t - sum - y
                this.sum[sumOffset] = t; // sum = t
                this.count[workerId * COUNT_PADDING]++;
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
        Arrays.fill(sum, 0);
        Arrays.fill(count, 0);
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public double getDouble(Record rec) {
        double sum = 0;
        long count = 0;
        double c = 0;
        for (int i = 0; i < workerCount; i++) {
            double y = this.sum[i * SUM_PADDING] - c;
            double t = sum + y;
            c = t - sum - y;
            sum = t;
            count += this.count[i * COUNT_PADDING];
        }
        return count > 0 ? sum : Double.NaN;
    }

    @Override
    public String getName() {
        return "ksum";
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
    public boolean merge(long pRostiA, long pRostiB) {
        return Rosti.keyedIntKSumDoubleMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.DOUBLE); // sum
        types.add(ColumnType.DOUBLE); // c
        types.add(ColumnType.LONG); // count
    }

    @Override
    public boolean wrapUp(long pRosti) {
        double sum = 0;
        long count = 0;
        double c = 0;
        for (int i = 0; i < workerCount; i++) {
            double y = this.sum[i * SUM_PADDING] - c;
            double t = sum + y;
            c = t - sum - y;
            sum = t;
            count += this.count[i * COUNT_PADDING];
        }
        return Rosti.keyedIntKSumDoubleWrapUp(pRosti, valueOffset, sum, count);
    }
}
