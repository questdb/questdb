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
import io.questdb.std.Vect;

import java.util.Arrays;

public class SumDoubleVectorAggregateFunction extends DoubleFunction implements VectorAggregateFunction {

    private final int columnIndex;
    private final double[] sum;
    private final long[] count;
    private final int workerCount;
    private int valueOffset;

    public SumDoubleVectorAggregateFunction(int position, int columnIndex, int workerCount) {
        super(position);
        this.columnIndex = columnIndex;
        this.sum = new double[workerCount * Misc.CACHE_LINE_SIZE];
        this.count = new long[workerCount * Misc.CACHE_LINE_SIZE];
        this.workerCount = workerCount;
    }

    @Override
    public void aggregate(long address, long count, int workerId) {
        if (address != 0) {
            final double value = Vect.sumDouble(address, count);
            if (value == value) {
                final int offset = workerId * Misc.CACHE_LINE_SIZE;
                this.sum[offset] += value;
                this.count[offset]++;
            }
        }
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public void clear() {
        Arrays.fill(sum, 0);
        Arrays.fill(count, 0);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.DOUBLE);
        types.add(ColumnType.LONG);
    }

    @Override
    public void aggregate(long pRosti, long keyAddress, long valueAddress, long count, int workerId) {
        // subtract 1 from valueOffset because 0 = value offset of 1-st value or key
        Rosti.keyedIntSumDouble(pRosti, keyAddress, valueAddress, count, valueOffset);
    }

    @Override
    public void merge(long pRostiA, long pRostiB) {
        Rosti.keyedIntSumDoubleMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public double getDouble(Record rec) {
        double sum = 0;
        long count = 0;
        for (int i = 0; i < workerCount; i++) {
            final int offset = i * Misc.CACHE_LINE_SIZE;
            sum += this.sum[offset];
            count += this.count[offset];
        }
        return count > 0 ? sum : Double.NaN;
    }
}
