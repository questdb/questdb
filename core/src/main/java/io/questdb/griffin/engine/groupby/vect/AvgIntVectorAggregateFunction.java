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
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

public class AvgIntVectorAggregateFunction extends DoubleFunction implements VectorAggregateFunction {

    private final DoubleAdder sum = new DoubleAdder();
    private final LongAdder count = new LongAdder();
    private final int columnIndex;
    private int valueOffset;

    public AvgIntVectorAggregateFunction(int position, int columnIndex) {
        super(position);
        this.columnIndex = columnIndex;
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.DOUBLE);
        types.add(ColumnType.LONG);
    }

    @Override
    public void initRosti(long pRosti) {
        // as for sums, except we space out values
        // we will have to replace them with doubles
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset), 0);
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset + 1), 0);
    }

    @Override
    public void aggregate(long pRosti, long keyAddress, long valueAddress, long count, int workerId) {
        Rosti.keyedIntSumInt(pRosti, keyAddress, valueAddress, count, valueOffset);
    }

    @Override
    public void merge(long pRostiA, long pRostiB) {
        Rosti.keyedIntSumIntMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void wrapUp(long pRosti) {
        Rosti.keyedIntAvgIntSetNull(pRosti, valueOffset);
    }

    @Override
    public int getValueOffset() {
        return valueOffset;
    }

    @Override
    public void aggregate(long address, long count, int workerId) {
        if (address != 0) {
            final double value = Vect.avgInt(address, count);
            if (value == value) {
                sum.add(value);
                this.count.increment();
            }
        }
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public void clear() {
        sum.reset();
        count.reset();
    }

    @Override
    public double getDouble(Record rec) {
        final long count = this.count.sum();
        if (count > 0) {
            return sum.sum() / count;
        }
        return Double.NaN;
    }
}
