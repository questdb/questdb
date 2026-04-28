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
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

/**
 * Shared base for {@code array_agg(D)} and {@code array_agg(D[])}: collects
 * double values (or array elements) into a 1D {@code DOUBLE[]} during
 * GROUP BY / SAMPLE BY.
 * <p>
 * <b>Parallelism strategy.</b> Same pattern as {@link TwapGroupByFunction} and
 * {@link SparklineGroupByFunction}. Each worker accumulates its observations
 * into a native buffer via its own {@link GroupByAllocator}. Within a single
 * worker, page frames arrive in rowId order, so per-worker buffers are sorted
 * runs. During the merge phase two sorted runs are combined with a merge-sort
 * step into a fresh buffer in the destination allocator, preserving insertion
 * order deterministically.
 * <p>
 * For the array variant, all elements from the same source row share the same
 * rowId, so intra-row element order is preserved by the stable dest-first
 * tie-breaking in the merge step.
 * <p>
 * Buffer layout in native memory (managed by {@link GroupByAllocator}):
 * <pre>
 * | count: INT (4 bytes) | capacity: INT (4 bytes) | (rowId: LONG, value: DOUBLE) * N |
 * </pre>
 * {@code capacity} is repurposed as a compaction sentinel: the first call to
 * {@link #getArray} compacts the pair buffer in-place (stripping rowIds) and
 * writes {@code -1} to the capacity slot so subsequent calls skip the step.
 * <p>
 * The count field at offset 0 doubles as the shape descriptor for
 * {@link io.questdb.cairo.arr.BorrowedArray}.
 * <p>
 * A single LONG slot in the map value stores the buffer pointer (0 = null/empty group).
 */
public abstract class AbstractArrayAggDoubleGroupByFunction extends ArrayFunction implements GroupByFunction, UnaryFunction {
    // Element count must fit in int when multiplied by Double.BYTES because
    // BorrowedArray.of() takes valueSize as int. Clamp the configured max
    // to keep narrowing casts in getArray() safe under all configurations.
    protected static final int BYTE_SAFE_ELEMENT_LIMIT = Integer.MAX_VALUE / Double.BYTES;
    protected static final int CAPACITY_OFFSET = Integer.BYTES;
    protected static final long ENTRY_SIZE = 16L;
    protected static final int HEADER_SIZE = 2 * Integer.BYTES;
    protected static final int INITIAL_CAPACITY = 16;
    protected static final int VALUE_OFFSET = Long.BYTES;
    protected final Function arg;
    protected final BorrowedArray borrowedArray = new BorrowedArray();
    protected final int maxArrayElementCount;
    protected GroupByAllocator allocator;
    protected int valueIndex;

    protected AbstractArrayAggDoubleGroupByFunction(@NotNull Function arg, int maxArrayElementCount) {
        this.arg = arg;
        this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
        this.maxArrayElementCount = Math.min(maxArrayElementCount, BYTE_SAFE_ELEMENT_LIMIT);
    }

    @Override
    public void close() {
        Misc.free(arg);
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
                double v = Unsafe.getUnsafe().getDouble(ptr + HEADER_SIZE + (long) i * ENTRY_SIZE + VALUE_OFFSET);
                Unsafe.getUnsafe().putDouble(ptr + HEADER_SIZE + (long) i * Double.BYTES, v);
            }
            // -1 marks the buffer as compacted. The sentinel is load-bearing for correctness,
            // not just a performance optimization: re-running compaction would read post-compaction
            // values from offset i*16+8 (now garbage) and write them over already-compacted slots.
            // toTop() rewinds the cursor without rebuilding the map, so subsequent renders MUST
            // hit this skip path. computeFirst/computeNext/merge after compaction would also
            // corrupt the buffer; the asserts in those methods guard against pipeline misuse.
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
        assert Unsafe.getUnsafe().getInt(srcPtr + CAPACITY_OFFSET) != -1
                : "array_agg: merge called on already-rendered src buffer";

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
        assert Unsafe.getUnsafe().getInt(destPtr + CAPACITY_OFFSET) != -1
                : "array_agg: merge called on already-rendered dest buffer";

        int destCount = Unsafe.getUnsafe().getInt(destPtr);
        int mergedCount = destCount + srcCount;
        if (mergedCount < 0) {
            throw CairoException.nonCritical().put("array_agg: array size exceeds maximum supported size");
        }
        checkCapacityLimit(mergedCount);
        long mergedPtr = allocator.malloc(HEADER_SIZE + (long) mergedCount * ENTRY_SIZE);
        // Two-pointer merge-sort by rowId: both runs are sorted within their respective
        // workers because page frames arrive in rowId order per worker. For the array
        // variant, elements from the same row share the same rowId; dest-first
        // tie-breaking keeps them contiguous.
        int di = 0, si = 0, mi = 0;
        while (di < destCount && si < srcCount) {
            long destAddr = destPtr + HEADER_SIZE + (long) di * ENTRY_SIZE;
            long srcAddr = srcPtr + HEADER_SIZE + (long) si * ENTRY_SIZE;
            long destRowId = Unsafe.getUnsafe().getLong(destAddr);
            long srcRowId = Unsafe.getUnsafe().getLong(srcAddr);
            long mergedAddr = mergedPtr + HEADER_SIZE + (long) mi * ENTRY_SIZE;
            if (destRowId <= srcRowId) {
                Unsafe.getUnsafe().putLong(mergedAddr, destRowId);
                Unsafe.getUnsafe().putLong(mergedAddr + VALUE_OFFSET, Unsafe.getUnsafe().getLong(destAddr + VALUE_OFFSET));
                di++;
            } else {
                Unsafe.getUnsafe().putLong(mergedAddr, srcRowId);
                Unsafe.getUnsafe().putLong(mergedAddr + VALUE_OFFSET, Unsafe.getUnsafe().getLong(srcAddr + VALUE_OFFSET));
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

    protected void checkCapacityLimit(int count) {
        if (count > maxArrayElementCount) {
            throw CairoException.nonCritical()
                    .put("array_agg: array size exceeds configured maximum [maxArrayElementCount=")
                    .put(maxArrayElementCount)
                    .put(']');
        }
    }
}
