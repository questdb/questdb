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
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.CharSink;

import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.function.DoubleBinaryOperator;

import static io.questdb.griffin.SqlCodeGenerator.GKK_HOUR_INT;

public class MaxDoubleVectorAggregateFunction extends DoubleFunction implements VectorAggregateFunction {

    public static final DoubleBinaryOperator MAX = Math::max;
    private final DoubleAccumulator max = new DoubleAccumulator(
            MAX, Double.NEGATIVE_INFINITY
    );
    private final int columnIndex;
    private final DistinctFunc distinctFunc;
    private final KeyValueFunc keyValueFunc;
    private int valueOffset;

    public MaxDoubleVectorAggregateFunction(int keyKind, int columnIndex, int workerCount) {
        this.columnIndex = columnIndex;
        if (keyKind == GKK_HOUR_INT) {
            this.distinctFunc = Rosti::keyedHourDistinct;
            this.keyValueFunc = Rosti::keyedHourMaxDouble;
        } else {
            this.distinctFunc = Rosti::keyedIntDistinct;
            this.keyValueFunc = Rosti::keyedIntMaxDouble;
        }
    }

    @Override
    public void aggregate(long address, long addressSize, int columnSizeHint, int workerId) {
        if (address != 0) {
            final double value = Vect.maxDouble(address, addressSize / Double.BYTES);
            if (value == value) {
                max.accumulate(value);
            }
        }
    }

    @Override
    public boolean aggregate(long pRosti, long keyAddress, long valueAddress, long valueAddressSize, int columnSizeShr, int workerId) {
        if (valueAddress == 0) {
            return distinctFunc.run(pRosti, keyAddress, valueAddressSize / Double.BYTES);
        } else {
            return keyValueFunc.run(pRosti, keyAddress, valueAddress, valueAddressSize / Double.BYTES, valueOffset);
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
        Unsafe.getUnsafe().putDouble(Rosti.getInitialValueSlot(pRosti, this.valueOffset), Double.NEGATIVE_INFINITY);
    }

    @Override
    public boolean merge(long pRostiA, long pRostiB) {
        return Rosti.keyedIntMaxDoubleMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.DOUBLE);
    }

    @Override
    public void wrapUp(long pRosti) {
        Rosti.keyedIntMaxDoubleWrapUp(pRosti, valueOffset, max.get());
    }

    @Override
    public void clear() {
        max.reset();
    }

    @Override
    public double getDouble(Record rec) {
        final double value = max.get();
        return Double.isInfinite(value) ? Double.NaN : value;
    }

    @Override
    public boolean isReadThreadSafe() {
        return false;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put("MaxDoubleVector(").put(columnIndex).put(')');
    }
}
