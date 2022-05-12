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
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static io.questdb.griffin.SqlCodeGenerator.GKK_HOUR_INT;

public class SumDoubleVectorAggregateFunction extends DoubleFunction implements VectorAggregateFunction {
    private static final int SUM_PADDING = Misc.CACHE_LINE_SIZE / Double.BYTES;
    private static final int COUNT_PADDING = Misc.CACHE_LINE_SIZE / Long.BYTES;

    private final int columnIndex;
    private final double[] sum;
    private final long[] count;
    private final int workerCount;
    private final DistinctFunc distinctFunc;
    private final KeyValueFunc keyValueFunc;
    private int valueOffset;

    public SumDoubleVectorAggregateFunction(int keyKind, int columnIndex, int workerCount) {
        this.columnIndex = columnIndex;
        this.sum = new double[workerCount * SUM_PADDING];
        this.count = new long[workerCount * COUNT_PADDING];
        this.workerCount = workerCount;

        if (keyKind == GKK_HOUR_INT) {
            distinctFunc = Rosti::keyedHourDistinct;
            keyValueFunc = Rosti::keyedHourSumDouble;
        } else {
            distinctFunc = Rosti::keyedIntDistinct;
            keyValueFunc = Rosti::keyedIntSumDouble;
        }
    }

    @Override
    public void aggregate(long address, long addressSize, int columnSizeHint, int workerId) {
        if (address != 0) {
            final double value = Vect.sumDouble(address, addressSize / Double.BYTES);
            if (value == value) {
                this.sum[workerId * SUM_PADDING] += value;
                this.count[workerId * COUNT_PADDING]++;
            }
        }
    }

    @Override
    public void aggregate(long pRosti, long keyAddress, long valueAddress, long valueAddressSize, int columnSizeShr, int workerId) {
        if (valueAddress == 0) {
            // no values? no problem :)
            // create list of distinct key values so that we can show NULL against them
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
        Unsafe.getUnsafe().putDouble(Rosti.getInitialValueSlot(pRosti, this.valueOffset + 1), 0);
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
        double sum = 0;
        long count = 0;
        for (int i = 0; i < workerCount; i++) {
            sum += this.sum[i * SUM_PADDING];
            count += this.count[i * COUNT_PADDING];
        }
        Rosti.keyedIntSumDoubleWrapUp(pRosti, valueOffset, sum, count);
    }

    @Override
    public void clear() {
        Arrays.fill(sum, 0);
        Arrays.fill(count, 0);
    }

    @Override
    public double getDouble(@Nullable Record rec) {
        double sum = 0;
        long count = 0;
        for (int i = 0; i < workerCount; i++) {
            sum += this.sum[i * SUM_PADDING];
            count += this.count[i * COUNT_PADDING];
        }
        return count > 0 ? sum : Double.NaN;
    }

    @Override
    public boolean isReadoutStateless() {
        return false;
    }
}
