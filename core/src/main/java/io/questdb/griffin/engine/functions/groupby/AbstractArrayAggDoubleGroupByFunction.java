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
 * Build buffer layout in native memory (managed by {@link GroupByAllocator}):
 * <pre>
 * | count: INT (4 bytes) | capacity: INT (4 bytes) | (rowId: LONG, value: DOUBLE) * N |
 * </pre>
 * {@link #getArray} produces a fresh allocator-backed flat {@code DOUBLE[]}
 * buffer the first time a group is rendered and caches the
 * {@code (srcPtr -> renderPtr)} mapping per instance. The build buffer is
 * never written to during render, so the read path is safe to call from a
 * shared cursor's GroupByFunction whose {@code initSharedFrom} hands back a
 * read-only flyweight over the same {@link MapValue}.
 * <p>
 * The count field at offset 0 of the build buffer doubles as the shape
 * descriptor for {@link io.questdb.cairo.arr.BorrowedArray}, and
 * {@link #getArray} passes it as the shape address while pointing the value
 * address at the rendered flat buffer.
 * <p>
 * A single LONG slot in the map value stores the build buffer pointer
 * (0 = null/empty group).
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
    // Per-instance render cache. The first getArray() call for a given build
    // buffer copies the values into a fresh flat buffer in the allocator and
    // remembers the mapping. The build buffer itself is never touched, so a
    // shared cursor's read-only flyweight (via initSharedFrom) is safe.
    protected long cachedRenderPtr;
    protected long cachedSrcPtr;
    // Primary instance for read-only shared instances. The shared instance does
    // not receive its own setAllocator() call, so it dereferences the primary's
    // allocator at render time. Null on the primary itself.
    protected AbstractArrayAggDoubleGroupByFunction primary;
    protected int valueIndex;

    protected AbstractArrayAggDoubleGroupByFunction(@NotNull Function arg, int maxArrayElementCount) {
        this.arg = arg;
        this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
        this.maxArrayElementCount = Math.min(maxArrayElementCount, BYTE_SAFE_ELEMENT_LIMIT);
    }

    @Override
    public void clear() {
        // Render cache holds native pointers into the GroupByAllocator. When
        // the enclosing factory is reused across cursor runs, the allocator is
        // reset and the same addresses may be handed out again - stale cache
        // entries would then point at freed memory.
        cachedRenderPtr = 0;
        cachedSrcPtr = 0;
    }

    @Override
    public void close() {
        Misc.free(arg);
    }

    @Override
    public void cursorClosed() {
        // The shared instance never has clear() invoked on it (it lives in
        // sharedRecordFunctions, not in the primary's groupByFunctions list).
        // cursorClosed() is the only lifecycle hook that runs on both primary
        // and shared instances when a cursor closes, so reset the render cache
        // here too. Without this, on the next factory.getCursor() the allocator
        // reopens and may hand back the same address; the stale cachedSrcPtr
        // would then short-circuit getArray() to a freed cachedRenderPtr.
        UnaryFunction.super.cursorClosed();
        cachedRenderPtr = 0;
        cachedSrcPtr = 0;
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
        if (ptr != cachedSrcPtr) {
            // Render: copy values from the (rowId, value) pair layout into a
            // fresh flat buffer in the allocator. The build buffer is never
            // written to, so the read path is safe to invoke concurrently
            // from a shared cursor's read-only flyweight. Shared instances do
            // not receive their own setAllocator() call - they dereference the
            // primary's allocator, which the engine wires up before the cursor
            // opens.
            GroupByAllocator alloc = (primary != null) ? primary.allocator : allocator;
            long renderPtr = alloc.malloc((long) count * Double.BYTES);
            for (int i = 0; i < count; i++) {
                double v = Unsafe.getUnsafe().getDouble(ptr + HEADER_SIZE + (long) i * ENTRY_SIZE + VALUE_OFFSET);
                Unsafe.getUnsafe().putDouble(renderPtr + (long) i * Double.BYTES, v);
            }
            cachedRenderPtr = renderPtr;
            cachedSrcPtr = ptr;
        }
        // Reuse the build buffer's count int (at offset 0) as the 1D shape
        // descriptor; the value bytes live in the rendered flat buffer.
        return borrowedArray.of(type, ptr, cachedRenderPtr, (int) ((long) count * Double.BYTES));
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
    public void initSharedFrom(GroupByFunction primary) {
        // The shared instance reads from the same MapValue slot as the
        // primary, so it can render groups without performing computeFirst /
        // computeNext / merge itself. The engine calls setAllocator() on the
        // primary AFTER assembleGroupByFunctions() wires this method, so we
        // hold a reference to the primary and dereference its allocator at
        // render time when it has been populated.
        this.valueIndex = primary.getValueIndex();
        this.primary = (AbstractArrayAggDoubleGroupByFunction) primary;
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
            throw CairoException.nonCritical().put("array_agg: array size exceeds maximum supported size");
        }
        checkCapacityLimit(mergedCount);
        long mergedPtr = allocator.malloc(HEADER_SIZE + (long) mergedCount * ENTRY_SIZE);
        // Append-only fast path: when worker N's last rowId is <= worker N+1's first
        // rowId (the common parallel-scan case), the entire dest run precedes the
        // entire src run. Two memcpys avoid the scalar two-pointer loop. Equality is
        // safe because the loop's tie-break is dest-first.
        long destLastRowId = Unsafe.getUnsafe().getLong(destPtr + HEADER_SIZE + (long) (destCount - 1) * ENTRY_SIZE);
        long srcFirstRowId = Unsafe.getUnsafe().getLong(srcPtr + HEADER_SIZE);
        if (destLastRowId <= srcFirstRowId) {
            Vect.memcpy(mergedPtr + HEADER_SIZE, destPtr + HEADER_SIZE, (long) destCount * ENTRY_SIZE);
            Vect.memcpy(mergedPtr + HEADER_SIZE + (long) destCount * ENTRY_SIZE, srcPtr + HEADER_SIZE, (long) srcCount * ENTRY_SIZE);
            Unsafe.getUnsafe().putInt(mergedPtr, mergedCount);
            Unsafe.getUnsafe().putInt(mergedPtr + CAPACITY_OFFSET, mergedCount);
            destValue.putLong(valueIndex, mergedPtr);
            return;
        }
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
