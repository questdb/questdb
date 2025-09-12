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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;

import java.util.concurrent.atomic.LongAdder;

import static io.questdb.griffin.SqlCodeGenerator.GKK_MICRO_HOUR_INT;
import static io.questdb.griffin.SqlCodeGenerator.GKK_NANO_HOUR_INT;

public class CountVectorAggregateFunction extends LongFunction implements VectorAggregateFunction {
    private final LongAdder count = new LongAdder();
    private final CountFunc countFunc;
    private int valueOffset;

    public CountVectorAggregateFunction(int keyKind) {
        if (keyKind == GKK_MICRO_HOUR_INT) {
            countFunc = Rosti::keyedMicroHourCount;
        } else if (keyKind == GKK_NANO_HOUR_INT) {
            countFunc = Rosti::keyedNanoHourCount;
        } else {
            countFunc = Rosti::keyedIntCount;
        }
    }

    @Override
    public void aggregate(long address, long frameRowCount, int workerId) {
        this.count.add(frameRowCount);
    }

    @Override
    public boolean aggregate(long pRosti, long keyAddress, long valueAddress, long frameRowCount) {
        return countFunc.count(pRosti, keyAddress, frameRowCount, valueOffset);
    }

    @Override
    public void clear() {
        count.reset();
    }

    @Override
    public int getColumnIndex() {
        return -1;
    }

    @Override
    public long getLong(Record rec) {
        return count.sum();
    }

    @Override
    public int getValueOffset() {
        return valueOffset;
    }

    @Override
    public void initRosti(long pRosti) {
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset), 0);
    }

    @Override
    public boolean merge(long pRostiA, long pRostiB) {
        return Rosti.keyedIntCountMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.LONG);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("count(*)");
    }

    @Override
    public boolean wrapUp(long pRosti) {
        return Rosti.keyedIntCountWrapUp(pRosti, valueOffset, count.sum() > 0 ? count.sum() : -1);
    }

    @FunctionalInterface
    private interface CountFunc {
        boolean count(long pRosti, long pKeys, long count, int valueOffset);
    }
}
