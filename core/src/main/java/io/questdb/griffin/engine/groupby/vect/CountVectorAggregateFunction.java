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
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;

import java.util.concurrent.atomic.LongAdder;

public class CountVectorAggregateFunction extends LongFunction implements VectorAggregateFunction {
    private final LongAdder count = new LongAdder();
    private final CountFunc countFunc;
    private int valueOffset;

    public CountVectorAggregateFunction(int keyKind) {
        countFunc = keyKind == SqlCodeGenerator.GKK_HOUR_INT ? Rosti::keyedHourCount : Rosti::keyedIntCount;
    }

    @Override
    public void aggregate(long address, long addressSize, int columnSizeHint, int workerId) {
        this.count.add(addressSize >>> columnSizeHint);
    }

    @Override
    public void aggregate(long pRosti, long keyAddress, long valueAddress, long valueAddressSize, int columnSizeShr, int workerId) {
        countFunc.count(pRosti, keyAddress, valueAddressSize >>> columnSizeShr, valueOffset);
    }

    @Override
    public int getColumnIndex() {
        return -1;
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
    public void merge(long pRostiA, long pRostiB) {
        Rosti.keyedIntCountMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.LONG);
    }

    @Override
    public void wrapUp(long pRosti) {
    }

    @Override
    public void clear() {
        count.reset();
    }

    @Override
    public long getLong(Record rec) {
        return count.sum();
    }

    @FunctionalInterface
    private interface CountFunc {
        void count(long pRosti, long pKeys, long count, int valueOffset);
    }
}
