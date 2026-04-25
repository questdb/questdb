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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

/**
 * Concatenates {@code DOUBLE[]} arrays into a single flat {@code DOUBLE[]} during
 * GROUP BY / SAMPLE BY. See {@link AbstractArrayAggDoubleGroupByFunction} for the
 * shared parallelism strategy and buffer layout.
 */
public class ArrayAggDoubleArrayGroupByFunction extends AbstractArrayAggDoubleGroupByFunction {

    public ArrayAggDoubleArrayGroupByFunction(@NotNull Function arg, int maxArrayElementCount) {
        super(arg, maxArrayElementCount);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        ArrayView arr = arg.getArray(record);
        if (arr.isNull()) {
            mapValue.putLong(valueIndex, 0);
            return;
        }
        int len = arr.getFlatViewLength();
        if (len == 0) {
            mapValue.putLong(valueIndex, 0);
            return;
        }
        checkCapacityLimit(len);
        int capacity = Math.max(INITIAL_CAPACITY, Numbers.ceilPow2(len));
        long ptr = allocator.malloc(HEADER_SIZE + (long) capacity * ENTRY_SIZE);
        Unsafe.getUnsafe().putInt(ptr, len);
        Unsafe.getUnsafe().putInt(ptr + CAPACITY_OFFSET, capacity);
        copyArrayElements(arr, len, ptr + HEADER_SIZE, rowId);
        mapValue.putLong(valueIndex, ptr);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        ArrayView arr = arg.getArray(record);
        if (arr.isNull()) {
            return;
        }
        int len = arr.getFlatViewLength();
        if (len == 0) {
            return;
        }
        long ptr = mapValue.getLong(valueIndex);
        if (ptr == 0) {
            // First non-null array in this group (previous rows were all null).
            computeFirst(mapValue, record, rowId);
            return;
        }
        int count = Unsafe.getUnsafe().getInt(ptr);
        int capacity = Unsafe.getUnsafe().getInt(ptr + CAPACITY_OFFSET);
        int newCount = count + len;
        if (newCount < 0) {
            throw CairoException.nonCritical().put("array_agg: array exceeds maximum capacity");
        }
        checkCapacityLimit(newCount);
        if (newCount > capacity) {
            int newCapacity = Numbers.ceilPow2(newCount);
            if (newCapacity < newCount) {
                throw CairoException.nonCritical().put("array_agg: array exceeds maximum capacity");
            }
            long oldSize = HEADER_SIZE + (long) capacity * ENTRY_SIZE;
            long newSize = HEADER_SIZE + (long) newCapacity * ENTRY_SIZE;
            ptr = allocator.realloc(ptr, oldSize, newSize);
            Unsafe.getUnsafe().putInt(ptr + CAPACITY_OFFSET, newCapacity);
            mapValue.putLong(valueIndex, ptr);
        }
        copyArrayElements(arr, len, ptr + HEADER_SIZE + (long) count * ENTRY_SIZE, rowId);
        Unsafe.getUnsafe().putInt(ptr, newCount);
    }

    private static void copyArrayElements(ArrayView arr, int len, long destAddr, long rowId) {
        if (arr.isVanilla()) {
            // Bulk-copy values into the low 8*len bytes of the pair buffer via a
            // SIMD-capable memcpy, then expand in place backwards into (rowId, value)
            // pairs. The write range at iteration i is [16*i, 16*i + 16), the pending
            // reads lie at [0, 8*i); 16*i >= 8*i for all i >= 0 so no pending read
            // gets clobbered.
            arr.flatView().appendPlainDoubleValue(destAddr, arr.getFlatViewOffset(), len);
            for (int i = len - 1; i >= 0; i--) {
                double v = Unsafe.getUnsafe().getDouble(destAddr + (long) i * Double.BYTES);
                Unsafe.getUnsafe().putLong(destAddr + (long) i * ENTRY_SIZE, rowId);
                Unsafe.getUnsafe().putDouble(destAddr + (long) i * ENTRY_SIZE + VALUE_OFFSET, v);
            }
        } else {
            // Non-vanilla 1D view (e.g., a column slice of a 2D array). Apply the
            // stride when reading from the flat backing store so elements come out
            // in logical order; arr.getDouble(flatIndex) reads physical memory and
            // would silently drop the stride.
            final FlatArrayView flatView = arr.flatView();
            final int flatOffset = arr.getFlatViewOffset();
            final int stride = arr.getStride(0);
            for (int i = 0; i < len; i++) {
                final double v = flatView.getDoubleAtAbsIndex(flatOffset + i * stride);
                Unsafe.getUnsafe().putLong(destAddr + (long) i * ENTRY_SIZE, rowId);
                Unsafe.getUnsafe().putDouble(destAddr + (long) i * ENTRY_SIZE + VALUE_OFFSET, v);
            }
        }
    }
}
