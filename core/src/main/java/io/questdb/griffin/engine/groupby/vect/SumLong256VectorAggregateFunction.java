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
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.mp.SimpleSpinLock;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Long256Util;
import io.questdb.std.Numbers;
import io.questdb.std.Rosti;
import io.questdb.std.ThreadLocal;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;

import java.util.concurrent.atomic.LongAdder;

import static io.questdb.griffin.SqlCodeGenerator.GKK_MICRO_HOUR_INT;
import static io.questdb.griffin.SqlCodeGenerator.GKK_NANO_HOUR_INT;

public class SumLong256VectorAggregateFunction extends Long256Function implements VectorAggregateFunction {
    private static final ThreadLocal<Long256Impl> partialSums = new ThreadLocal<>(Long256Impl::new);
    private final int columnIndex;
    private final LongAdder count = new LongAdder();
    private final DistinctFunc distinctFunc;
    private final KeyValueFunc keyValueFunc;
    private final SimpleSpinLock lock = new SimpleSpinLock();
    private final Long256Impl sumA = new Long256Impl();
    private final Long256Impl sumB = new Long256Impl();
    private int valueOffset;

    public SumLong256VectorAggregateFunction(int keyKind, int columnIndex, int workerCount) {
        this.columnIndex = columnIndex;
        if (keyKind == GKK_MICRO_HOUR_INT) {
            distinctFunc = Rosti::keyedMicroHourDistinct;
            keyValueFunc = Rosti::keyedMicroHourSumLong256;
        } else if (keyKind == GKK_NANO_HOUR_INT) {
            distinctFunc = Rosti::keyedNanoHourDistinct;
            keyValueFunc = Rosti::keyedNanoHourSumLong256;
        } else {
            distinctFunc = Rosti::keyedIntDistinct;
            keyValueFunc = Rosti::keyedIntSumLong256;
        }
    }

    @Override
    public void aggregate(long address, long frameRowCount, int workerId) {
        if (address != 0) {
            Long256Impl value = sumLong256(partialSums.get(), address, frameRowCount);
            if (value != Long256Impl.NULL_LONG256) {
                lock.lock();
                try {
                    Long256Util.add(sumA, value);
                    this.count.increment();
                } finally {
                    lock.unlock();
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
    public void clear() {
        sumA.setAll(0, 0, 0, 0);
        sumB.setAll(0, 0, 0, 0);
        count.reset();
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public void getLong256(Record rec, CharSink<?> sink) {
        Long256Impl v = (Long256Impl) getLong256A(rec);
        v.toSink(sink);
    }

    @Override
    public Long256 getLong256A(Record rec) {
        if (count.sum() > 0) {
            return sumA;
        }
        return Long256Impl.NULL_LONG256;
    }

    @Override
    public Long256 getLong256B(Record rec) {
        if (count.sum() > 0) {
            sumB.copyFrom(sumA);
            return sumB;
        }
        return Long256Impl.NULL_LONG256;
    }

    @Override
    public String getName() {
        return "sum";
    }

    @Override
    public int getValueOffset() {
        return valueOffset;
    }

    @Override
    public void initRosti(long pRosti) {
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset), 0);
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset) + Long.BYTES, 0);
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset) + 2 * Long.BYTES, 0);
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset) + 3 * Long.BYTES, 0);
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset + 1), 0);
    }

    @Override
    public boolean merge(long pRostiA, long pRostiB) {
        return Rosti.keyedIntSumLong256Merge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.LONG256);
        types.add(ColumnType.LONG);
    }

    @Override
    public boolean wrapUp(long pRosti) {
        return Rosti.keyedIntSumLong256WrapUp(pRosti, valueOffset, sumA.getLong0(), sumA.getLong1(), sumA.getLong2(), sumA.getLong3(), count.sum());
    }

    private Long256Impl sumLong256(Long256Impl sum, long address, long count) {
        boolean hasData = false;
        long offset = 0;
        sum.setAll(0, 0, 0, 0);
        for (long i = 0; i < count; i++) {
            final long l0 = Unsafe.getUnsafe().getLong(address + offset);
            final long l1 = Unsafe.getUnsafe().getLong(address + offset + Long.BYTES);
            final long l2 = Unsafe.getUnsafe().getLong(address + offset + Long.BYTES * 2);
            final long l3 = Unsafe.getUnsafe().getLong(address + offset + Long.BYTES * 3);

            boolean isNull = l0 == Numbers.LONG_NULL &&
                    l1 == Numbers.LONG_NULL &&
                    l2 == Numbers.LONG_NULL &&
                    l3 == Numbers.LONG_NULL;

            if (!isNull) {
                Long256Util.add(sum, l0, l1, l2, l3);
                hasData = true;
            }
            offset += 4 * Long.BYTES;
        }
        return hasData ? sum : Long256Impl.NULL_LONG256;
    }
}
