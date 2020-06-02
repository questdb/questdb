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
import io.questdb.std.Vect;

import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.function.DoubleBinaryOperator;

public class MinDoubleVectorAggregateFunction extends DoubleFunction implements VectorAggregateFunction {

    public static final DoubleBinaryOperator MIN = Math::min;
    private int valueOffset;

    private final DoubleAccumulator min = new DoubleAccumulator(
            MIN, Double.POSITIVE_INFINITY
    );

    private final int columnIndex;

    public MinDoubleVectorAggregateFunction(int position, int columnIndex) {
        super(position);
        this.columnIndex = columnIndex;
    }

    @Override
    public void aggregate(long address, long count, int workerId) {
        if (address != 0) {
            final double value = Vect.minDouble(address, count);
            if (value == value) {
                min.accumulate(value);
            }
        }
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public void clear() {
        min.reset();
    }

    @Override
    public double getDouble(Record rec) {
        final double min = this.min.get();
        if (Double.isInfinite(min)) {
            return Double.NaN;
        }
        return min;
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.DOUBLE);
    }

    @Override
    public void aggregate(long pRosti, long keyAddress, long valueAddress, long count, int workerId) {
        Rosti.keyedIntMinDouble(pRosti, keyAddress, valueAddress, count, valueOffset);
    }

    @Override
    public void merge(long pRostiA, long pRostiB) {
        Rosti.keyedIntMinDoubleMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void setNull(long pRosti) {
        Rosti.keyedIntMinDoubleSetNull(pRosti, valueOffset);
    }
}
