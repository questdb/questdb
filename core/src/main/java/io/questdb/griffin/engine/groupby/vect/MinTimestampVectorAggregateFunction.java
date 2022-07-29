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
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.Numbers;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.CharSink;

import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.LongBinaryOperator;

import static io.questdb.griffin.SqlCodeGenerator.GKK_HOUR_INT;

public class MinTimestampVectorAggregateFunction extends TimestampFunction implements VectorAggregateFunction {

    public static final LongBinaryOperator MIN = Math::min;
    private final LongAccumulator accumulator = new LongAccumulator(
            MIN, Long.MAX_VALUE
    );
    private final int columnIndex;
    private final DistinctFunc distinctFunc;
    private final KeyValueFunc keyValueFunc;
    private int valueOffset;

    public MinTimestampVectorAggregateFunction(int keyKind, int columnIndex, int workerCount) {
        this.columnIndex = columnIndex;
        if (keyKind == GKK_HOUR_INT) {
            this.distinctFunc = Rosti::keyedHourDistinct;
            this.keyValueFunc = Rosti::keyedHourMinLong;
        } else {
            this.distinctFunc = Rosti::keyedIntDistinct;
            this.keyValueFunc = Rosti::keyedIntMinLong;
        }
    }

    @Override
    public void aggregate(long address, long addressSize, int columnSizeHint, int workerId) {
        if (address != 0) {
            final long value = Vect.minLong(address, addressSize / Long.BYTES);
            if (value != Numbers.LONG_NaN) {
                accumulator.accumulate(value);
            }
        }
    }

    @Override
    public boolean aggregate(long pRosti, long keyAddress, long valueAddress, long valueAddressSize, int columnSizeShr, int workerId) {
        if (valueAddress == 0) {
            return distinctFunc.run(pRosti, keyAddress, valueAddressSize / Long.BYTES);
        } else {
            return keyValueFunc.run(pRosti, keyAddress, valueAddress, valueAddressSize / Long.BYTES, valueOffset);
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
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset), Long.MAX_VALUE);
    }

    @Override
    public void merge(long pRostiA, long pRostiB) {
        Rosti.keyedIntMinLongMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.LONG);
    }

    @Override
    public void wrapUp(long pRosti) {
        Rosti.keyedIntMinLongWrapUp(pRosti, valueOffset, accumulator.longValue());
    }

    @Override
    public void clear() {
        accumulator.reset();
    }

    @Override
    public long getTimestamp(Record rec) {
        final long value = accumulator.longValue();
        return value == Long.MAX_VALUE ? Numbers.LONG_NaN : value;
    }

    @Override
    public boolean isReadThreadSafe() {
        return false;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put("MinTimestampVector(").put(columnIndex).put(')');
    }
}
