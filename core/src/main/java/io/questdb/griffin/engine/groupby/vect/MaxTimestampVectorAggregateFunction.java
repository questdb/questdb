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
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.LongBinaryOperator;

import static io.questdb.griffin.SqlCodeGenerator.GKK_MICRO_HOUR_INT;
import static io.questdb.griffin.SqlCodeGenerator.GKK_NANO_HOUR_INT;

public class MaxTimestampVectorAggregateFunction extends TimestampFunction implements VectorAggregateFunction {

    public static final LongBinaryOperator MAX = Math::max;
    private final int columnIndex;
    private final DistinctFunc distinctFunc;
    private final KeyValueFunc keyValueFunc;
    private final LongAccumulator max = new LongAccumulator(
            MAX, Long.MIN_VALUE
    );
    private int valueOffset;

    public MaxTimestampVectorAggregateFunction(int keyKind, int columnIndex, int workerCount, int timestampType) {
        super(timestampType);
        this.columnIndex = columnIndex;
        if (keyKind == GKK_MICRO_HOUR_INT) {
            this.distinctFunc = Rosti::keyedMicroHourDistinct;
            this.keyValueFunc = Rosti::keyedMicroHourMaxLong;
        } else if (keyKind == GKK_NANO_HOUR_INT) {
            this.distinctFunc = Rosti::keyedNanoHourDistinct;
            this.keyValueFunc = Rosti::keyedNanoHourMaxLong;
        } else {
            this.distinctFunc = Rosti::keyedIntDistinct;
            this.keyValueFunc = Rosti::keyedIntMaxLong;
        }
    }

    @Override
    public void aggregate(long address, long frameRowCount, int workerId) {
        if (address != 0) {
            max.accumulate(Vect.maxLong(address, frameRowCount));
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
    public void clear() {
        max.reset();
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public String getName() {
        return "max";
    }

    @Override
    public long getTimestamp(Record rec) {
        return max.longValue();
    }

    @Override
    public int getValueOffset() {
        return valueOffset;
    }

    @Override
    public void initRosti(long pRosti) {
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset), Long.MIN_VALUE);
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
        return Rosti.keyedIntMaxLongWrapUp(pRosti, valueOffset, max.longValue());
    }
}
