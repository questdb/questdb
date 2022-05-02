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
import io.questdb.std.Misc;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.util.Arrays;

import static io.questdb.griffin.SqlCodeGenerator.GKK_HOUR_INT;

public class NSumDoubleVectorAggregateFunction extends DoubleFunction implements VectorAggregateFunction {
    private static final int SUM_PADDING = Misc.CACHE_LINE_SIZE / Double.BYTES;
    private static final int COUNT_PADDING = Misc.CACHE_LINE_SIZE / Long.BYTES;

    private final int columnIndex;
    private final double[] sum;
    private final long[] count;
    private final int workerCount;
    private final DistinctFunc distinctFunc;
    private final KeyValueFunc keyValueFunc;
    private int valueOffset;
    private double transientSum;
    private double transientC;
    private long transientCount;

    public NSumDoubleVectorAggregateFunction(int keyKind, int columnIndex, int workerCount) {
        this.columnIndex = columnIndex;
        this.sum = new double[workerCount * SUM_PADDING];
        this.count = new long[workerCount * COUNT_PADDING];
        this.workerCount = workerCount;
        if (keyKind == GKK_HOUR_INT) {
            this.distinctFunc = Rosti::keyedHourDistinct;
            this.keyValueFunc = Rosti::keyedHourNSumDouble;
        } else {
            this.distinctFunc = Rosti::keyedIntDistinct;
            this.keyValueFunc = Rosti::keyedIntNSumDouble;
        }
    }

    @Override
    public void aggregate(long address, long addressSize, int columnSizeHint, int workerId) {
        if (address != 0) {
            // Neumaier compensated summation
            final double x = Vect.sumDoubleNeumaier(address, addressSize / Double.BYTES);
            if (x == x) {
                final int sumOffset = workerId * SUM_PADDING;
                final double sum = this.sum[sumOffset];
                final double t = sum + x;
                double c = this.sum[sumOffset + 1];
                if (Math.abs(sum) >= x) {
                    c += (sum - t) + x;
                } else {
                    c += (x - t) + sum;
                }
                this.sum[sumOffset] = t; // sum = t
                this.sum[sumOffset + 1] = c;
                this.count[workerId * COUNT_PADDING]++;
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
        Rosti.keyedIntNSumDoubleMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.DOUBLE);
        types.add(ColumnType.DOUBLE);
        types.add(ColumnType.LONG);
    }

    @Override
    public void wrapUp(long pRosti) {
        computeSum();
        Rosti.keyedIntNSumDoubleWrapUp(pRosti, valueOffset, transientSum, transientCount, transientC);
    }

    @Override
    public void clear() {
        Arrays.fill(sum, 0);
        Arrays.fill(count, 0);
    }

    @Override
    public double getDouble(Record rec) {
        computeSum();
        return transientCount > 0 ? transientSum + transientC : Double.NaN;
    }

    private void computeSum() {
        double sum = 0;
        long count = 0;
        double c = 0;
        for (int i = 0; i < workerCount; i++) {
            double x = this.sum[i * SUM_PADDING] + this.sum[i * SUM_PADDING + 1];
            double t = sum + x;
            if (Math.abs(sum) >= x) {
                c += (sum - t) + x;
            } else {
                c += (x - t) + sum;
            }
            sum = t;
            count += this.count[i * COUNT_PADDING];
        }
        this.transientSum = sum;
        this.transientCount = count;
        this.transientC = c;
    }
}
