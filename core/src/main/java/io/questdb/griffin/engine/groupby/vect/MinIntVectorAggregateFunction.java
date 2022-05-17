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
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.std.Numbers;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.LongBinaryOperator;

import static io.questdb.griffin.SqlCodeGenerator.GKK_HOUR_INT;

public class MinIntVectorAggregateFunction extends IntFunction implements VectorAggregateFunction {

    public static final LongBinaryOperator MIN = Math::min;
    private final LongAccumulator accumulator = new LongAccumulator(
            MIN, Integer.MAX_VALUE
    );
    private final int columnIndex;
    private final DistinctFunc distinctFunc;
    private final KeyValueFunc keyValueFunc;
    private int valueOffset;

    public MinIntVectorAggregateFunction(int keyKind, int columnIndex, int workerCount) {
        this.columnIndex = columnIndex;
        if (keyKind == GKK_HOUR_INT) {
            this.distinctFunc = Rosti::keyedHourDistinct;
            this.keyValueFunc = Rosti::keyedHourMinInt;
        } else {
            this.distinctFunc = Rosti::keyedIntDistinct;
            this.keyValueFunc = Rosti::keyedIntMinInt;
        }
    }

    @Override
    public void aggregate(long address, long addressSize, int columnSizeHint, int workerId) {
        if (address != 0) {
            final int value = Vect.minInt(address, addressSize / Integer.BYTES);
            if (value != Numbers.INT_NaN) {
                accumulator.accumulate(value);
            }
        }
    }

    @Override
    public void aggregate(long pRosti, long keyAddress, long valueAddress, long valueAddressSize, int columnSizeShr, int workerId) {
        if (valueAddress == 0) {
            distinctFunc.run(pRosti, keyAddress, valueAddressSize / Integer.BYTES);
        } else {
            keyValueFunc.run(pRosti, keyAddress, valueAddress, valueAddressSize / Integer.BYTES, valueOffset);
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
        Unsafe.getUnsafe().putInt(Rosti.getInitialValueSlot(pRosti, this.valueOffset), Integer.MAX_VALUE);
    }

    @Override
    public void merge(long pRostiA, long pRostiB) {
        Rosti.keyedIntMinIntMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        valueOffset = types.getColumnCount();
        types.add(ColumnType.INT);
    }

    @Override
    public void wrapUp(long pRosti) {
        Rosti.keyedIntMinIntWrapUp(pRosti, valueOffset, accumulator.intValue());
    }

    @Override
    public void clear() {
        accumulator.reset();
    }

    @Override
    public int getInt(Record rec) {
        final int value = accumulator.intValue();
        return value == Integer.MAX_VALUE ? Numbers.INT_NaN : value;
    }

    @Override
    public boolean isReadThreadSafe() {
        return false;
    }
}
