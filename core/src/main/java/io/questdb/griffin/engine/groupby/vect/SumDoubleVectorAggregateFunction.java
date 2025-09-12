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
import io.questdb.std.Numbers;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static io.questdb.griffin.SqlCodeGenerator.GKK_MICRO_HOUR_INT;
import static io.questdb.griffin.SqlCodeGenerator.GKK_NANO_HOUR_INT;

public class SumDoubleVectorAggregateFunction extends DoubleFunction implements VectorAggregateFunction {
    private static final int COUNT_PADDING = Misc.CACHE_LINE_SIZE / Long.BYTES;
    private static final int SUM_PADDING = Misc.CACHE_LINE_SIZE / Double.BYTES;
    private final int columnIndex;
    private final long[] count;
    private final DistinctFunc distinctFunc;
    private final KeyValueFunc keyValueFunc;
    private final double[] sum;
    private final int workerCount;
    private int valueOffset;

    public SumDoubleVectorAggregateFunction(int keyKind, int columnIndex, int workerCount) {
        this.columnIndex = columnIndex;
        this.sum = new double[workerCount * SUM_PADDING];
        this.count = new long[workerCount * COUNT_PADDING];
        this.workerCount = workerCount;

        if (keyKind == GKK_MICRO_HOUR_INT) {
            distinctFunc = Rosti::keyedMicroHourDistinct;
            keyValueFunc = Rosti::keyedMicroHourSumDouble;
        } else if (keyKind == GKK_NANO_HOUR_INT) {
            distinctFunc = Rosti::keyedNanoHourDistinct;
            keyValueFunc = Rosti::keyedNanoHourSumDouble;
        } else {
            distinctFunc = Rosti::keyedIntDistinct;
            keyValueFunc = Rosti::keyedIntSumDouble;
        }
    }

    @Override
    public void aggregate(long address, long frameRowCount, int workerId) {
        if (address != 0) {
            final double value = Vect.sumDouble(address, frameRowCount);
            if (Numbers.isFinite(value)) {
                this.sum[workerId * SUM_PADDING] += value;
                this.count[workerId * COUNT_PADDING]++;
            }
        }
    }

    @Override
    public boolean aggregate(long pRosti, long keyAddress, long valueAddress, long frameRowCount) {
        if (valueAddress == 0) {
            // no values? no problem :)
            // create list of distinct key values so that we can show NULL against them
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
    public String getName() {
        return "sum";
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
    public boolean merge(long pRostiA, long pRostiB) {
        return Rosti.keyedIntSumDoubleMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.DOUBLE);
        types.add(ColumnType.LONG);
    }

    @Override
    public boolean wrapUp(long pRosti) {
        double sum = 0;
        long count = 0;
        for (int i = 0; i < workerCount; i++) {
            sum += this.sum[i * SUM_PADDING];
            count += this.count[i * COUNT_PADDING];
        }
        return Rosti.keyedIntSumDoubleWrapUp(pRosti, valueOffset, sum, count);
    }
}
