/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.griffin.engine.functions.UInt64Function;
import io.questdb.std.Numbers;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.LongBinaryOperator;

import static io.questdb.griffin.SqlCodeGenerator.GKK_MICRO_HOUR_INT;
import static io.questdb.griffin.SqlCodeGenerator.GKK_NANO_HOUR_INT;

public class MaxUInt64VectorAggregateFunction extends UInt64Function implements VectorAggregateFunction {

    public static final LongBinaryOperator MAX = (long l1, long l2) -> {
        if (l1 == Numbers.LONG_NULL) {
            return l2;
        }
        if (l2 == Numbers.LONG_NULL) {
            return l1;
        }
        return Math.max(l1, l2);
    };
    private final LongAccumulator accumulator = new LongAccumulator(
            MAX, Numbers.LONG_NULL
    );
    private final int columnIndex;
    private final DistinctFunc distinctFunc;
    private final KeyValueBitmapFunc keyValueBitmapFunc;
    private final KeyValueFunc keyValueFunc;
    private int valueOffset;

    @SuppressWarnings("unused")
    public MaxUInt64VectorAggregateFunction(int keyKind, int columnIndex, int timestampIndex, int workerCount) {
        this.columnIndex = columnIndex;
        if (keyKind == GKK_MICRO_HOUR_INT) {
            this.distinctFunc = Rosti::keyedMicroHourDistinct;
            this.keyValueFunc = Rosti::keyedMicroHourMaxLong;
            this.keyValueBitmapFunc = Rosti::keyedMicroHourMaxUInt64BitmapNull;
        } else if (keyKind == GKK_NANO_HOUR_INT) {
            this.distinctFunc = Rosti::keyedNanoHourDistinct;
            this.keyValueFunc = Rosti::keyedNanoHourMaxLong;
            this.keyValueBitmapFunc = Rosti::keyedNanoHourMaxUInt64BitmapNull;
        } else {
            this.distinctFunc = Rosti::keyedIntDistinct;
            this.keyValueFunc = Rosti::keyedIntMaxLong;
            this.keyValueBitmapFunc = Rosti::keyedIntMaxUInt64BitmapNull;
        }
    }

    @Override
    public void aggregate(long address, long frameRowCount, int workerId) {
        // Non-bitmap path: UINT64 always uses bitmap nulls, so this is a fallback.
        // Vect.maxLong() uses signed comparison which is incorrect for unsigned values,
        // so this method is effectively a no-op for UINT64.
    }

    @Override
    public void aggregate(long address, long bitmapAddr, long bitOffset, long frameRowCount, int workerId) {
        if (address != 0) {
            if (bitmapAddr != 0) {
                final long value = Vect.maxUInt64BitmapNull(address, bitmapAddr, bitOffset, frameRowCount);
                if (value != Numbers.LONG_NULL) {
                    accumulator.accumulate(value);
                }
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
    public boolean aggregate(long pRosti, long keyAddress, long valueAddress, long bitmapAddr, long bitOffset, long frameRowCount) {
        if (valueAddress == 0) {
            return distinctFunc.run(pRosti, keyAddress, frameRowCount);
        } else if (bitmapAddr != 0) {
            return keyValueBitmapFunc.run(pRosti, keyAddress, valueAddress, bitmapAddr, bitOffset, frameRowCount, valueOffset);
        } else {
            return keyValueFunc.run(pRosti, keyAddress, valueAddress, frameRowCount, valueOffset);
        }
    }

    @Override
    public void clear() {
        accumulator.reset();
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public long getLong(Record rec) {
        long v = accumulator.longValue();
        return v == Numbers.LONG_NULL ? Numbers.LONG_NULL : v;
    }

    @Override
    public boolean isNull(Record rec) {
        return accumulator.longValue() == Numbers.LONG_NULL;
    }

    @Override
    public String getName() {
        return "max";
    }

    @Override
    public int getValueOffset() {
        return valueOffset;
    }

    @Override
    public void initRosti(long pRosti) {
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset), Numbers.LONG_NULL);
    }

    @Override
    public boolean merge(long pRostiA, long pRostiB) {
        return Rosti.keyedIntMaxLongMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.LONG);
    }

    @Override
    public boolean wrapUp(long pRosti) {
        return Rosti.keyedIntMaxLongWrapUp(pRosti, valueOffset, accumulator.longValue());
    }
}
