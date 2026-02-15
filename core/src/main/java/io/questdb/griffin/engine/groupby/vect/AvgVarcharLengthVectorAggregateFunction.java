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
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

/**
 * Vectorized AVG(length(varchar_column)) that reads varchar aux + data
 * pages directly via native SIMD code. ASCII strings are resolved from
 * the aux header alone (no data vector access), non-ASCII strings are
 * scanned with AVX-512/AVX2/SWAR and batched prefetching.
 */
public class AvgVarcharLengthVectorAggregateFunction extends DoubleFunction implements VectorAggregateFunction {

    private final int columnIndex;
    private final LongAdder count = new LongAdder();
    private final DoubleAdder sum = new DoubleAdder();
    private int valueOffset;

    public AvgVarcharLengthVectorAggregateFunction(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    @Override
    public void aggregate(PageFrameMemory memory, long frameRowCount, int workerId) {
        final long auxAddr = memory.getAuxPageAddress(columnIndex);
        if (auxAddr != 0) {
            final long dataAddr = memory.getPageAddress(columnIndex);
            // Use a thread-local buffer for sum/count to avoid contention.
            // Unsafe native memory padded to cache line size would be ideal,
            // but DoubleAdder/LongAdder handle contention well enough.
            final long sumBuf = Unsafe.malloc(Double.BYTES + Long.BYTES, MemoryTag.NATIVE_FUNC_RSS);
            try {
                Unsafe.getUnsafe().putDouble(sumBuf, 0.0);
                Unsafe.getUnsafe().putLong(sumBuf + Double.BYTES, 0);
                Vect.varcharUtf8LengthSum(auxAddr, dataAddr, frameRowCount, sumBuf, sumBuf + Double.BYTES);
                sum.add(Unsafe.getUnsafe().getDouble(sumBuf));
                count.add(Unsafe.getUnsafe().getLong(sumBuf + Double.BYTES));
            } finally {
                Unsafe.free(sumBuf, Double.BYTES + Long.BYTES, MemoryTag.NATIVE_FUNC_RSS);
            }
        }
    }

    @Override
    public void aggregate(long address, long frameRowCount, int workerId) {
        // Not used — the PageFrameMemory overload is called instead.
    }

    @Override
    public boolean aggregate(long pRosti, long keyAddress, long valueAddress, long frameRowCount) {
        // Keyed aggregation not yet supported for this composite function.
        return false;
    }

    @Override
    public void clear() {
        sum.reset();
        count.reset();
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public double getDouble(Record rec) {
        final long c = count.sum();
        if (c > 0) {
            return sum.sum() / c;
        }
        return Double.NaN;
    }

    @Override
    public String getName() {
        return "avg";
    }

    @Override
    public int getValueOffset() {
        return valueOffset;
    }

    @Override
    public void initRosti(long pRosti) {
        // Not used for keyed aggregation.
    }

    @Override
    public boolean merge(long pRostiA, long pRostiB) {
        return false;
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.DOUBLE);
        types.add(ColumnType.LONG);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("avg(length(").putColumnName(columnIndex).val("))");
    }

    @Override
    public boolean wrapUp(long pRosti) {
        return false;
    }
}
