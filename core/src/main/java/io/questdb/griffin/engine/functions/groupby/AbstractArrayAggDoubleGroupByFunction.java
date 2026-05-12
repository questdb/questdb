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
    // Single-slot render cache, owned by the primary. The first getArray() call
    // for a given build buffer copies the values into a fresh flat buffer in
    // the allocator and remembers the mapping. Shared instances (created by
    // initSharedFrom for shared cursors like LATERAL joins) read and write
    // these fields through the primary, so the cache is reset by primary's
    // clear() on cursor close - matching the StringAgg / ApproxPercentile
    // shared-container convention.
    protected long cachedRenderPtr;
    protected long cachedSrcPtr;
    // Set on shared instances to redirect cache and allocator access through
    // the primary. Null on the primary itself.
    protected AbstractArrayAggDoubleGroupByFunction primary;
    protected int valueIndex;

    protected AbstractArrayAggDoubleGroupByFunction(@NotNull Function arg, int maxArrayElementCount) {
        this.arg = arg;
        this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
        this.maxArrayElementCount = Math.min(maxArrayElementCount, BYTE_SAFE_ELEMENT_LIMIT);
    }

    @Override
    public void clear() {
        // Skip on shared instances: cache and allocator live on the primary,
        // and clear() runs on the primary via Misc.clearObjList(groupByFunctions)
        // when the cursor closes. Same convention as StringAggGroupByFunction.
        if (primary != null) {
            return;
        }
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
        // Defence-in-depth: reset the render cache on every instance (primary
        // and shared) when the cursor closes. The primary's clear() already
        // resets the cache via Misc.clearObjList(groupByFunctions) on the
        // owning cursor close, but a shared cursor (LATERAL join) reuses the
        // same factory across executions and only sees cursorClosed() on its
        // lifecycle hook. The cache lives on the primary (see getArray()),
        // so route the reset through owner: writes to this.cached* on a
        // shared instance would be dead. Without this, allocator address
        // recycling between executions could short-circuit getArray() to a
        // freed cachedRenderPtr and silently return stale array bytes.
        AbstractArrayAggDoubleGroupByFunction owner = (primary != null) ? primary : this;
        owner.cachedRenderPtr = 0;
        owner.cachedSrcPtr = 0;
        UnaryFunction.super.cursorClosed();
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
        int count = Unsafe.getInt(ptr);
        if (count == 0) {
            return ArrayConstant.NULL;
        }
        // Cache and allocator live on the primary. On a shared instance we
        // read and write through the primary so primary.clear() on cursor
        // close resets the cache uniformly across shared and primary reads.
        AbstractArrayAggDoubleGroupByFunction owner = (primary != null) ? primary : this;
        if (ptr != owner.cachedSrcPtr) {
            // Render: copy values from the (rowId, value) pair layout into a
            // fresh flat buffer in the allocator. The build buffer is never
            // written to, so the read path is safe to invoke from a shared
            // cursor's read-only flyweight.
            long renderPtr = owner.allocator.malloc((long) count * Double.BYTES);
            for (int i = 0; i < count; i++) {
                double v = Unsafe.getUnsafe().getDouble(ptr + HEADER_SIZE + (long) i * ENTRY_SIZE + VALUE_OFFSET);
                Unsafe.getUnsafe().putDouble(renderPtr + (long) i * Double.BYTES, v);
            }
            owner.cachedRenderPtr = renderPtr;
            owner.cachedSrcPtr = ptr;
        }
        // Reuse the build buffer's count int (at offset 0) as the 1D shape
        // descriptor; the value bytes live in the rendered flat buffer.
        return borrowedArray.of(type, ptr, owner.cachedRenderPtr, (int) ((long) count * Double.BYTES));
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
        // primary and routes its render-cache reads/writes and allocator
        // access through the primary. setAllocator() is only called on the
        // primary; clear() is only called on the primary. Holding a primary
        // back-reference keeps shared and primary in sync without duplicate
        // state on the shared instance, matching the StringAgg / ApproxPercentile
        // shared-container pattern.
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
        int srcCount = Unsafe.getInt(srcPtr);
        if (srcCount == 0) {
            return;
        }

        long destPtr = destValue.getLong(valueIndex);
        if (destPtr == 0 || Unsafe.getInt(destPtr) == 0) {
            // Dest empty - deep copy src into dest's allocator. Cannot shallow-copy
            // srcPtr because src's allocator is reclaimed independently.
            checkCapacityLimit(srcCount);
            long newPtr = allocator.malloc(HEADER_SIZE + (long) srcCount * ENTRY_SIZE);
            Unsafe.putInt(newPtr, srcCount);
            Unsafe.putInt(newPtr + CAPACITY_OFFSET, srcCount);
            Vect.memcpy(newPtr + HEADER_SIZE, srcPtr + HEADER_SIZE, (long) srcCount * ENTRY_SIZE);
            destValue.putLong(valueIndex, newPtr);
            return;
        }

        int destCount = Unsafe.getInt(destPtr);
        int mergedCount = destCount + srcCount;
        if (mergedCount < 0) {
            throw CairoException.nonCritical().put("array_agg: array size exceeds maximum supported size");
        }
        checkCapacityLimit(mergedCount);
        long mergedPtr = allocator.malloc(HEADER_SIZE + (long) mergedCount * ENTRY_SIZE);
        if (tryMergeDisjointRuns(destValue, mergedPtr, destPtr, destCount, srcPtr, srcCount, mergedCount)
                || tryMergeDisjointRuns(destValue, mergedPtr, srcPtr, srcCount, destPtr, destCount, mergedCount)) {
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
            long destRowId = Unsafe.getLong(destAddr);
            long srcRowId = Unsafe.getLong(srcAddr);
            long mergedAddr = mergedPtr + HEADER_SIZE + (long) mi * ENTRY_SIZE;
            if (destRowId <= srcRowId) {
                Unsafe.putLong(mergedAddr, destRowId);
                Unsafe.putLong(mergedAddr + VALUE_OFFSET, Unsafe.getLong(destAddr + VALUE_OFFSET));
                di++;
            } else {
                Unsafe.putLong(mergedAddr, srcRowId);
                Unsafe.putLong(mergedAddr + VALUE_OFFSET, Unsafe.getLong(srcAddr + VALUE_OFFSET));
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
        Unsafe.putInt(mergedPtr, mergedCount);
        Unsafe.putInt(mergedPtr + CAPACITY_OFFSET, mergedCount);
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

    private boolean tryMergeDisjointRuns(
            MapValue destValue,
            long mergedPtr,
            long firstPtr,
            int firstCount,
            long secondPtr,
            int secondCount,
            int mergedCount
    ) {
        long firstLastRowId = Unsafe.getLong(firstPtr + HEADER_SIZE + (long) (firstCount - 1) * ENTRY_SIZE);
        long secondFirstRowId = Unsafe.getLong(secondPtr + HEADER_SIZE);
        if (firstLastRowId > secondFirstRowId) {
            return false;
        }
        Vect.memcpy(mergedPtr + HEADER_SIZE, firstPtr + HEADER_SIZE, (long) firstCount * ENTRY_SIZE);
        Vect.memcpy(mergedPtr + HEADER_SIZE + (long) firstCount * ENTRY_SIZE, secondPtr + HEADER_SIZE, (long) secondCount * ENTRY_SIZE);
        Unsafe.putInt(mergedPtr, mergedCount);
        Unsafe.putInt(mergedPtr + CAPACITY_OFFSET, mergedCount);
        destValue.putLong(valueIndex, mergedPtr);
        return true;
    }
}
