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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

/**
 * Concatenates {@code DOUBLE[]} arrays into a single flat {@code DOUBLE[]} during GROUP BY / SAMPLE BY.
 * <p>
 * <b>Parallelism strategy.</b> Same pattern as {@link TwapGroupByFunction} and
 * {@link SparklineGroupByFunction}. Each worker accumulates its observations into a
 * native buffer via its own {@link GroupByAllocator}. Within a single worker, page
 * frames arrive in rowId order, so per-worker buffers are sorted runs. During the
 * merge phase two sorted runs are combined with a merge-sort step into a fresh buffer
 * in the destination allocator, preserving insertion order deterministically.
 * All elements from the same source row share the same rowId, so intra-row element
 * order is preserved by the stable dest-first tie-breaking in the merge step.
 * <p>
 * Buffer layout in native memory (managed by {@link GroupByAllocator}):
 * <pre>
 * | count: INT (4 bytes) | capacity: INT (4 bytes) | (rowId: LONG, value: DOUBLE) * N |
 * </pre>
 * {@code capacity} is repurposed as a compaction sentinel: the first call to
 * {@link #getArray} compacts the pair buffer in-place (stripping rowIds) and writes
 * {@code -1} to the capacity slot so subsequent calls skip the step.
 * <p>
 * The count field at offset 0 doubles as the shape descriptor for
 * {@link io.questdb.cairo.arr.BorrowedArray}.
 * <p>
 * A single LONG slot in the map value stores the buffer pointer (0 = null/empty group).
 */
public class ArrayAggDoubleArrayGroupByFunction extends ArrayFunction implements GroupByFunction, UnaryFunction {
    // Element count must fit in int when multiplied by Double.BYTES because
    // BorrowedArray.of() takes valueSize as int. Clamp the configured max
    // to keep narrowing casts in getArray() safe under all configurations.
    private static final int BYTE_SAFE_ELEMENT_LIMIT = Integer.MAX_VALUE / Double.BYTES;
    private static final int CAPACITY_OFFSET = Integer.BYTES;
    private static final long ENTRY_SIZE = 16L;
    private static final int HEADER_SIZE = 2 * Integer.BYTES;
    private static final int INITIAL_CAPACITY = 16;
    private final Function arg;
    private final BorrowedArray borrowedArray = new BorrowedArray();
    private final int maxArrayElementCount;
    private GroupByAllocator allocator;
    private int valueIndex;

    public ArrayAggDoubleArrayGroupByFunction(@NotNull Function arg, int maxArrayElementCount) {
        this.arg = arg;
        this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
        this.maxArrayElementCount = Math.min(maxArrayElementCount, BYTE_SAFE_ELEMENT_LIMIT);
    }

    @Override
    public void close() {
        Misc.free(arg);
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

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public ArrayView getArray(Record rec) {
        long ptr = rec.getLong(valueIndex);
        if (ptr == 0) {
            return ArrayConstant.NULL;
        }
        int count = Unsafe.getUnsafe().getInt(ptr);
        if (count == 0) {
            return ArrayConstant.NULL;
        }
        if (Unsafe.getUnsafe().getInt(ptr + CAPACITY_OFFSET) != -1) {
            // First render: compact pairs in-place by moving each value over its rowId slot.
            // Write offset i*8 <= read offset i*16+8 for all i>=0, so the forward pass is safe.
            for (int i = 0; i < count; i++) {
                double v = Unsafe.getUnsafe().getDouble(ptr + HEADER_SIZE + (long) i * ENTRY_SIZE + 8);
                Unsafe.getUnsafe().putDouble(ptr + HEADER_SIZE + (long) i * Double.BYTES, v);
            }
            // -1 marks the buffer as compacted so subsequent calls skip this step.
            Unsafe.getUnsafe().putInt(ptr + CAPACITY_OFFSET, -1);
        }
        return borrowedArray.of(type, ptr, ptr + HEADER_SIZE, (int) ((long) count * Double.BYTES));
    }

    @Override
    public String getName() {
        return "array_agg";
    }

    @Override
    public int getSampleByFlags() {
        return SAMPLE_BY_FILL_NONE | SAMPLE_BY_FILL_NULL | SAMPLE_BY_FILL_PREVIOUS;
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG); // buffer pointer
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isScalar() {
        return false;
    }

    /**
     * Must return false because this function stores a per-worker
     * {@link GroupByAllocator} reference. Returning true would cause the
     * parallel GROUP BY engine to share a single function instance across
     * workers, leading to concurrent access to the non-thread-safe allocator.
     */
    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcPtr = srcValue.getLong(valueIndex);
        if (srcPtr == 0) {
            return;
        }
        int srcCount = Unsafe.getUnsafe().getInt(srcPtr);
        if (srcCount == 0) {
            return;
        }

        long destPtr = destValue.getLong(valueIndex);
        if (destPtr == 0 || Unsafe.getUnsafe().getInt(destPtr) == 0) {
            // Dest empty - deep copy src into dest's allocator. Cannot shallow-copy
            // srcPtr because src's allocator is reclaimed independently.
            checkCapacityLimit(srcCount);
            long newPtr = allocator.malloc(HEADER_SIZE + (long) srcCount * ENTRY_SIZE);
            Unsafe.getUnsafe().putInt(newPtr, srcCount);
            Unsafe.getUnsafe().putInt(newPtr + CAPACITY_OFFSET, srcCount);
            Vect.memcpy(newPtr + HEADER_SIZE, srcPtr + HEADER_SIZE, (long) srcCount * ENTRY_SIZE);
            destValue.putLong(valueIndex, newPtr);
            return;
        }

        int destCount = Unsafe.getUnsafe().getInt(destPtr);
        int mergedCount = destCount + srcCount;
        if (mergedCount < 0) {
            throw CairoException.nonCritical().put("array_agg: merged array exceeds maximum capacity");
        }
        checkCapacityLimit(mergedCount);
        long mergedPtr = allocator.malloc(HEADER_SIZE + (long) mergedCount * ENTRY_SIZE);
        // Two-pointer merge-sort by rowId: both runs are sorted within their respective
        // workers because page frames arrive in rowId order per worker. Elements from the
        // same row share the same rowId and dest-first tie-breaking keeps them contiguous.
        int di = 0, si = 0, mi = 0;
        while (di < destCount && si < srcCount) {
            long destAddr = destPtr + HEADER_SIZE + (long) di * ENTRY_SIZE;
            long srcAddr = srcPtr + HEADER_SIZE + (long) si * ENTRY_SIZE;
            long destRowId = Unsafe.getUnsafe().getLong(destAddr);
            long srcRowId = Unsafe.getUnsafe().getLong(srcAddr);
            long mergedAddr = mergedPtr + HEADER_SIZE + (long) mi * ENTRY_SIZE;
            if (destRowId <= srcRowId) {
                Unsafe.getUnsafe().putLong(mergedAddr, destRowId);
                Unsafe.getUnsafe().putLong(mergedAddr + 8, Unsafe.getUnsafe().getLong(destAddr + 8));
                di++;
            } else {
                Unsafe.getUnsafe().putLong(mergedAddr, srcRowId);
                Unsafe.getUnsafe().putLong(mergedAddr + 8, Unsafe.getUnsafe().getLong(srcAddr + 8));
                si++;
            }
            mi++;
        }
        if (di < destCount) {
            Vect.memcpy(mergedPtr + HEADER_SIZE + (long) mi * ENTRY_SIZE, destPtr + HEADER_SIZE + (long) di * ENTRY_SIZE, (long) (destCount - di) * ENTRY_SIZE);
        }
        if (si < srcCount) {
            Vect.memcpy(mergedPtr + HEADER_SIZE + (long) mi * ENTRY_SIZE, srcPtr + HEADER_SIZE + (long) si * ENTRY_SIZE, (long) (srcCount - si) * ENTRY_SIZE);
        }
        Unsafe.getUnsafe().putInt(mergedPtr, mergedCount);
        Unsafe.getUnsafe().putInt(mergedPtr + CAPACITY_OFFSET, mergedCount);
        destValue.putLong(valueIndex, mergedPtr);
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("array_agg(").val(arg).val(')');
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
                Unsafe.getUnsafe().putDouble(destAddr + (long) i * ENTRY_SIZE + 8, v);
            }
        } else {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putLong(destAddr + (long) i * ENTRY_SIZE, rowId);
                Unsafe.getUnsafe().putDouble(destAddr + (long) i * ENTRY_SIZE + 8, arr.getDouble(i));
            }
        }
    }

    private void checkCapacityLimit(int count) {
        if (count > maxArrayElementCount) {
            throw CairoException.nonCritical()
                    .put("array_agg: array size exceeds configured maximum [maxArrayElementCount=")
                    .put(maxArrayElementCount)
                    .put(']');
        }
    }
}
