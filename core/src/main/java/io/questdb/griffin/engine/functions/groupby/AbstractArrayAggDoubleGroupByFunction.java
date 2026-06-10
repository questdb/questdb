/*+*****************************************************************************
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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.griffin.engine.groupby.SortedRunsMerge;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

/**
 * Shared base for {@code array_agg(D)} and {@code array_agg(D[])}: collects
 * double values (or array elements) into a 1D {@code DOUBLE[]} during
 * GROUP BY / SAMPLE BY. NULL handling differs between the two variants: the
 * scalar variant preserves NULL inputs as null elements; the array variant
 * skips NULL and empty input arrays (concat identity). See the respective
 * subclass javadoc.
 * <p>
 * <b>Parallelism strategy.</b> Same pattern as {@link TwapGroupByFunction} and
 * {@link SparklineGroupByFunction}. Each per-slot function instance accumulates
 * observations into a native buffer through its own {@link GroupByAllocator}.
 * A single {@code computeNext} loop processes one page frame in rowId order,
 * appending one key-sorted batch. The slot a frame lands on is chosen by lock
 * acquisition in {@link io.questdb.griffin.engine.PerWorkerLocks}, not by worker
 * identity, so under cross-query work-stealing a single slot's buffer can
 * accumulate batches in non-monotonic order. {@code computeNext} therefore
 * records per-frame batch boundaries (see {@link SortedRunsMerge}): a new batch
 * starts at a gap or an out-of-order frame, while consecutive in-order frames
 * extend the current batch (the page frame is read from the row id,
 * {@code rowId >>> 44}). The boundaries let merge and read time sort the buffer
 * by permuting whole batches, with no element-wise merge. A group whose frames
 * arrive as a single consecutive run keeps its descriptor buffer unallocated
 * ({@code descPtr == 0}).
 * <p>
 * For the array variant, all elements from the same source row share the same
 * rowId; intra-row element order is preserved because the compaction copies
 * each run as a whole {@code memcpy} block, never interleaving entries inside
 * a run.
 * <p>
 * Build buffer layout in native memory (managed by {@link GroupByAllocator}):
 * <pre>
 * | count: INT (4 bytes) | capacity: INT (4 bytes) | (rowId: LONG, value: DOUBLE) * N |
 * </pre>
 * {@link #getArray} renders into a per-function-instance {@link DirectArray}
 * the first time a group is read and caches the source build-buffer pointer.
 * The scratch buffer is grow-only (1.5x reallocation), and its native memory
 * is owned by the {@code DirectArray} instance and freed on {@link #close()},
 * not by the {@link GroupByAllocator}. The read path is not pure: on a
 * cache miss {@link SortedRunsMerge#compactInPlace} sorts the build buffer
 * in place and, when the buffer holds more than one run, {@code memcpy}s
 * the sorted result back over it. That mutation is safe because a single
 * consumer thread drives the cursor sequentially, and the
 * {@code cachedSrcPtr} guard short-circuits subsequent reads of the same
 * buffer without re-mutating it. Shared cursor instances
 * ({@code initSharedFrom}) hand back a flyweight over the same
 * {@link MapValue} and route render through the primary, so a single
 * render serves all readers of a given group on that consumer thread.
 * Concurrent reads of the same {@link MapValue} are not supported.
 * <p>
 * <b>MapValue layout</b> (5 slots):
 * <pre>
 *   +0  LONG  build buffer pointer (0 = null/empty group)
 *   +1  LONG  descriptor buffer pointer (0 = single batch)
 *   +2  LONG  descriptor count
 *   +3  LONG  descriptor buffer capacity in 8-byte entries
 *   +4  LONG  frame index of the most recently appended entry
 * </pre>
 * The entry {@code count} and {@code capacity} live in the build buffer header
 * (see above), not in MapValue slots; only the batch-descriptor state does.
 */
public abstract class AbstractArrayAggDoubleGroupByFunction extends ArrayFunction implements GroupByFunction, UnaryFunction {
    protected static final int BYTE_SAFE_ELEMENT_LIMIT = Integer.MAX_VALUE / Double.BYTES;
    protected static final int CAPACITY_OFFSET = Integer.BYTES;
    // MapValue slot offsets, relative to valueIndex, of the per-frame batch
    // descriptor state handed to SortedRunsMerge. The descriptor pointer, count
    // and capacity occupy three consecutive slots as appendBatchStart requires.
    // See the class javadoc for the full MapValue layout.
    protected static final int DESC_CAPACITY_SLOT = 3;
    protected static final int DESC_COUNT_SLOT = 2;
    protected static final int DESC_PTR_SLOT = 1;
    protected static final long ENTRY_SIZE = 16L;
    protected static final int HEADER_SIZE = 2 * Integer.BYTES;
    protected static final int INITIAL_CAPACITY = 16;
    protected static final int LAST_FRAME_SLOT = 4;
    protected static final int VALUE_OFFSET = Long.BYTES;
    protected GroupByAllocator allocator;
    protected final Function arg;

    // Build-buffer pointer that the scratch currently holds rendered values for.
    // Lives logically on the primary; shared instances read and write through
    // owner = primary so a single render is shared with all readers.
    protected long cachedSrcPtr;
    protected final int maxArrayElementCount;
    // Set on shared instances to redirect cache and allocator access through
    // the primary. Null on the primary itself.
    protected AbstractArrayAggDoubleGroupByFunction primary;
    // Scratch list for run descriptors used by SortedRunsMerge. Lives
    // logically on the primary; shared instances route via owner = primary.
    // Not thread-safe; the per-slot instance has serialized access by virtue
    // of PerWorkerLocks.
    protected final LongList runScratch = new LongList(16);
    protected final DirectArray scratch;
    protected int valueIndex;

    protected AbstractArrayAggDoubleGroupByFunction(@NotNull Function arg, @NotNull CairoConfiguration configuration) {
        this.arg = arg;
        this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
        this.maxArrayElementCount = Math.min(configuration.maxArrayElementCount(), BYTE_SAFE_ELEMENT_LIMIT);
        this.scratch = new DirectArray(configuration);
        this.scratch.setType(this.type);
    }

    @Override
    public void clear() {
        // Skip on shared instances: cache lives on the primary, and clear()
        // runs on the primary via Misc.clearObjList(groupByFunctions) on
        // cursor close. Same convention as StringAggGroupByFunction.
        if (primary != null) {
            return;
        }
        cachedSrcPtr = 0;
    }

    @Override
    public void close() {
        Misc.free(arg);
        Misc.free(scratch);
    }

    @Override
    public void cursorClosed() {
        // Defence-in-depth: reset the render cache key on every instance
        // (primary and shared) on cursor close. A shared cursor (LATERAL join)
        // reuses the same factory across executions and only sees
        // cursorClosed() on its lifecycle hook; route through owner so writes
        // hit the primary's field. Without this, allocator address recycling
        // between executions could short-circuit getArray() to a stale srcPtr
        // and silently return wrong array bytes.
        AbstractArrayAggDoubleGroupByFunction owner = (primary != null) ? primary : this;
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
        AbstractArrayAggDoubleGroupByFunction owner = (primary != null) ? primary : this;
        if (ptr != owner.cachedSrcPtr) {
            // Under parallel GROUP BY the entries buffer may be a
            // concatenation of disjoint per-frame batches in non-monotonic
            // order. Compact in place to a single sorted run before rendering
            // so the output array reflects rowId order, not slot-acquisition
            // order. The buffer pointer is preserved across compaction, so
            // owner.cachedSrcPtr stays a valid cache key. The batch boundaries
            // come from the descriptor buffer recorded during accumulation.
            SortedRunsMerge.compactInPlace(
                    owner.allocator,
                    owner.runScratch,
                    ptr + HEADER_SIZE,
                    count,
                    rec.getLong(valueIndex + DESC_PTR_SLOT),
                    rec.getLong(valueIndex + DESC_COUNT_SLOT),
                    ENTRY_SIZE
            );
            owner.scratch.setDimLen(0, count);
            owner.scratch.applyShape();
            long dst = owner.scratch.ptr();
            for (int i = 0; i < count; i++) {
                double v = Unsafe.getDouble(ptr + HEADER_SIZE + (long) i * ENTRY_SIZE + VALUE_OFFSET);
                Unsafe.putDouble(dst + (long) i * Double.BYTES, v);
            }
            owner.cachedSrcPtr = ptr;
        }
        return owner.scratch;
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
        // shared-container pattern. The shared instance's own scratch field is
        // never grown - getArray() always routes through owner = primary.
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
        columnTypes.add(ColumnType.LONG); // +0 build buffer pointer
        columnTypes.add(ColumnType.LONG); // +1 descriptor buffer pointer
        columnTypes.add(ColumnType.LONG); // +2 descriptor count
        columnTypes.add(ColumnType.LONG); // +3 descriptor buffer capacity
        columnTypes.add(ColumnType.LONG); // +4 last frame index
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

    /**
     * Combines the source slot's entries with the destination's into a
     * single sorted buffer allocated in the destination's allocator. Both
     * inputs are treated as concatenations of disjoint per-frame batches; the
     * combined batch set still satisfies the disjointness invariant because
     * different frames cover disjoint rowId ranges, including the array variant
     * where multiple entries from one row share its rowId but all live inside
     * that frame's batch. The merged buffer carries its own descriptor buffer
     * so a higher-level merge can interleave it with another partial result.
     * <p>
     * See {@link SortedRunsMerge} for the algorithm. Intra-batch order is
     * preserved by bulk {@code memcpy}, which keeps the array variant's
     * intra-row element order intact.
     */
    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        assert primary == null : "merge called on shared instance";
        long srcPtr = srcValue.getLong(valueIndex);
        if (srcPtr == 0) {
            return;
        }
        int srcCount = Unsafe.getInt(srcPtr);
        if (srcCount == 0) {
            return;
        }
        long srcDescPtr = srcValue.getLong(valueIndex + DESC_PTR_SLOT);
        long srcDescCount = srcValue.getLong(valueIndex + DESC_COUNT_SLOT);

        long destPtr = destValue.getLong(valueIndex);
        int destCount = (destPtr == 0) ? 0 : Unsafe.getInt(destPtr);
        long destDescPtr = destCount > 0 ? destValue.getLong(valueIndex + DESC_PTR_SLOT) : 0;
        long destDescCount = destCount > 0 ? destValue.getLong(valueIndex + DESC_COUNT_SLOT) : 0;

        int mergedCount = destCount + srcCount;
        checkCapacityLimit(mergedCount);

        // Count the batches each side contributes (descPtr == 0 is one implicit
        // batch) so the merged descriptor buffer is sized exactly.
        long destBatches = destCount <= 0 ? 0 : (destDescPtr == 0 ? 1 : destDescCount);
        long srcBatches = srcDescPtr == 0 ? 1 : srcDescCount;
        long mergedBatches = destBatches + srcBatches;

        // Fold the entry buffer (with its header) and the optional descriptor
        // buffer into a single allocator request, mirroring TwapGroupByFunction:
        // one larger block keeps the entry/descriptor pair contiguous and
        // halves the allocator's chunk-grow rate on the high-cardinality merge
        // path.
        long entryBytes = (long) mergedCount * ENTRY_SIZE;
        long descBytes = mergedBatches > 1 ? mergedBatches * Long.BYTES : 0;
        long mergedPtr = allocator.malloc(HEADER_SIZE + entryBytes + descBytes);
        long mergedDescPtr = descBytes > 0 ? mergedPtr + HEADER_SIZE + entryBytes : 0;
        SortedRunsMerge.compactInto(
                runScratch,
                mergedPtr + HEADER_SIZE, mergedDescPtr,
                (destCount > 0) ? destPtr + HEADER_SIZE : 0, destCount, destDescPtr, destDescCount,
                srcPtr + HEADER_SIZE, srcCount, srcDescPtr, srcDescCount,
                ENTRY_SIZE
        );
        Unsafe.putInt(mergedPtr, mergedCount);
        Unsafe.putInt(mergedPtr + CAPACITY_OFFSET, mergedCount);
        destValue.putLong(valueIndex, mergedPtr);
        destValue.putLong(valueIndex + DESC_PTR_SLOT, mergedDescPtr);
        destValue.putLong(valueIndex + DESC_COUNT_SLOT, mergedDescPtr != 0 ? mergedBatches : 0);
        destValue.putLong(valueIndex + DESC_CAPACITY_SLOT, mergedDescPtr != 0 ? mergedBatches : 0);
        // The descriptor buffer is sized exactly to the merge result, so a
        // post-merge appendBatchStart would silently realloc. No path appends
        // after merge; stamp a negative sentinel that appendFrameIfNeeded
        // asserts on.
        destValue.putLong(valueIndex + LAST_FRAME_SLOT, -1L);
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        assert primary == null : "setAllocator called on shared instance";
        this.allocator = allocator;
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
        mapValue.putLong(valueIndex + DESC_PTR_SLOT, 0);
        mapValue.putLong(valueIndex + DESC_COUNT_SLOT, 0);
        mapValue.putLong(valueIndex + DESC_CAPACITY_SLOT, 0);
        mapValue.putLong(valueIndex + LAST_FRAME_SLOT, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("array_agg(").val(arg).val(')');
    }

    /**
     * Records a per-frame batch boundary before {@code count} more entries are
     * appended for {@code rowId}. Frames map to slots by lock acquisition, so a
     * slot can observe frames out of rowId order. A consecutive frame
     * ({@code lastFrameId + 1}) continues the current batch: its keys follow on
     * contiguously, so the batch stays key-disjoint from every other batch. A
     * gap or an out-of-order frame starts a new batch at the current entry
     * offset. The array variant passes the same {@code rowId} for every element
     * of one row, so all of a row's elements land in a single batch.
     */
    protected void appendFrameIfNeeded(MapValue mapValue, long rowId, long count) {
        long frameId = rowId >>> 44;
        long lastFrameId = mapValue.getLong(valueIndex + LAST_FRAME_SLOT);
        assert lastFrameId >= 0 : "computeNext on a post-merge MapValue";
        if (frameId != lastFrameId) {
            if (frameId != lastFrameId + 1) {
                SortedRunsMerge.appendBatchStart(allocator, mapValue, valueIndex + DESC_PTR_SLOT, count);
            }
            mapValue.putLong(valueIndex + LAST_FRAME_SLOT, frameId);
        }
    }

    protected void checkCapacityLimit(int count) {
        if (count > maxArrayElementCount) {
            throw CairoException.nonCritical()
                    .put("array_agg: array size exceeds configured maximum [maxArrayElementCount=")
                    .put(maxArrayElementCount)
                    .put(']');
        }
    }

    /**
     * Initializes the batch-descriptor state for a freshly started group: a
     * single implicit batch ({@code descPtr == 0}), with the first out-of-order
     * frame allocating the descriptor buffer lazily via {@link #appendFrameIfNeeded}.
     */
    protected void initRunState(MapValue mapValue, long rowId) {
        mapValue.putLong(valueIndex + DESC_PTR_SLOT, 0);
        mapValue.putLong(valueIndex + DESC_COUNT_SLOT, 0);
        mapValue.putLong(valueIndex + DESC_CAPACITY_SLOT, 0);
        mapValue.putLong(valueIndex + LAST_FRAME_SLOT, rowId >>> 44);
    }
}
