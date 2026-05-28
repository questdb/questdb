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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.groupby.SortedRunsMerge;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

/**
 * Time-Weighted Average Price (TWAP) aggregate function.
 * <p>
 * Computes the area under a step function where each price is held constant
 * until the next observation:
 * <pre>
 *   TWAP = sum(price_i * (ts_{i+1} - ts_i)) / (ts_last - ts_first)
 * </pre>
 * When all observations share the same timestamp, falls back to a simple
 * arithmetic average.
 * <p>
 * <b>Parallelism strategy.</b> An incremental (running weighted sum) approach
 * cannot work with parallel GROUP BY because workers process non-adjacent page
 * frames via work-stealing. A worker that sees frames [0, 4, 8] would
 * incorrectly bridge the gaps between those frames, attributing duration to
 * observations that belong to other workers.
 * <p>
 * Instead, each per-slot {@link GroupByFunction} instance records all
 * (timestamp, price) observations into a native buffer through its own
 * {@link GroupByAllocator}. A single {@code computeNext} loop processes one
 * page frame's rows in timestamp order, appending one key-sorted batch.
 * However, the slot a frame lands on is determined by lock acquisition in
 * {@link io.questdb.griffin.engine.PerWorkerLocks}, not by the worker's
 * identity, so under cross-query work-stealing a single slot's buffer can
 * accumulate batches in non-monotonic order. {@code computeNext} therefore
 * records per-frame batch boundaries (see {@link SortedRunsMerge}): a new
 * batch starts at a gap or an out-of-order frame, while consecutive in-order
 * frames extend the current batch (the page frame is read from the row id,
 * {@code rowId >>> 44}). The boundaries let merge and read time sort the
 * buffer by permuting whole batches, with no element-wise merge. A group
 * whose frames arrive as a single consecutive run keeps its descriptor
 * buffer unallocated.
 * <p>
 * The final TWAP is computed from the compacted, fully sorted buffer in
 * {@link #getDouble(Record)}.
 * <p>
 * <b>MapValue layout</b> (7 slots):
 * <pre>
 *   +0  LONG    entry buffer pointer (0 = no observations)
 *   +1  LONG    observation count
 *   +2  LONG    entry buffer capacity in entries
 *   +3  LONG    descriptor buffer pointer (0 = single batch)
 *   +4  LONG    descriptor count
 *   +5  LONG    descriptor buffer capacity in entries
 *   +6  LONG    frame index of the most recently appended entry
 * </pre>
 * <b>Buffer entry layout</b> (16 bytes each):
 * <pre>
 *   [0..7]   timestamp (long, microseconds)
 *   [8..15]  price     (double)
 * </pre>
 */
public class TwapGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
    private static final long ENTRY_SIZE = 16;
    private static final long INITIAL_CAPACITY = 16;
    private final Function priceFunc;
    // Scratch list for batch records used by SortedRunsMerge. Not
    // thread-safe; each per-slot function instance owns its own.
    private final LongList scratch = new LongList(16);
    private final Function tsFunc;
    private GroupByAllocator allocator;
    private long cachedPtr;
    private double cachedValue;
    // Populated on the primary instance by each shared dependent's initSharedFrom
    // call. setAllocator iterates this list so shared instances reach getDouble
    // with a non-null allocator for the in-place sort's transient aux buffer.
    private ObjList<TwapGroupByFunction> sharedDependents;
    private int valueIndex;

    public TwapGroupByFunction(@NotNull Function priceFunc, @NotNull Function tsFunc) {
        this.priceFunc = priceFunc;
        this.tsFunc = tsFunc;
    }

    @Override
    public void clear() {
        // cachedPtr is a native address into the GroupByAllocator. When the
        // enclosing factory is reused across cursor runs the allocator is
        // reset and the same address may be handed out again - a stale cache
        // entry would then return the previous run's TWAP for unrelated data.
        // computeFirst also zeroes cachedPtr on every new group, but in the
        // parallel keyed path the owner's instance sees groups only via
        // merge() and getDouble() and would miss that reset.
        cachedPtr = 0;
        cachedValue = 0;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        cachedPtr = 0;
        double price = priceFunc.getDouble(record);
        long ts = tsFunc.getLong(record);
        if (Double.isNaN(price) || ts == Numbers.LONG_NULL) {
            // NULL observation - initialize empty state
            setNull(mapValue);
        } else {
            // Allocate buffer and store first observation as the first batch
            long ptr = allocator.malloc(INITIAL_CAPACITY * ENTRY_SIZE);
            Unsafe.putLong(ptr, ts);
            Unsafe.putDouble(ptr + 8, price);
            mapValue.putLong(valueIndex, ptr);
            mapValue.putLong(valueIndex + 1, 1);
            mapValue.putLong(valueIndex + 2, INITIAL_CAPACITY);
            mapValue.putLong(valueIndex + 3, 0);             // descPtr: single implicit batch
            mapValue.putLong(valueIndex + 4, 0);             // descCount
            mapValue.putLong(valueIndex + 5, 0);             // descCapacity
            mapValue.putLong(valueIndex + 6, rowId >>> 44);  // lastFrameId
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        double price = priceFunc.getDouble(record);
        long ts = tsFunc.getLong(record);
        if (Double.isNaN(price) || ts == Numbers.LONG_NULL) {
            return; // skip NULL observations
        }
        long count = mapValue.getLong(valueIndex + 1);
        if (count <= 0) {
            // First valid observation after initial NULLs - starts the first batch
            long ptr = allocator.malloc(INITIAL_CAPACITY * ENTRY_SIZE);
            Unsafe.putLong(ptr, ts);
            Unsafe.putDouble(ptr + 8, price);
            mapValue.putLong(valueIndex, ptr);
            mapValue.putLong(valueIndex + 1, 1);
            mapValue.putLong(valueIndex + 2, INITIAL_CAPACITY);
            mapValue.putLong(valueIndex + 3, 0);
            mapValue.putLong(valueIndex + 4, 0);
            mapValue.putLong(valueIndex + 5, 0);
            mapValue.putLong(valueIndex + 6, rowId >>> 44);
            return;
        }
        // Frames map to slots by lock acquisition, so a slot can observe
        // frames out of rowId order. A consecutive frame (lastFrameId + 1)
        // continues the current batch: its keys follow on contiguously and
        // the batch stays a contiguous frame interval, so it remains
        // key-disjoint from every other batch. A gap or an out-of-order
        // frame starts a new batch.
        long frameId = rowId >>> 44;
        long lastFrameId = mapValue.getLong(valueIndex + 6);
        assert lastFrameId >= 0 : "computeNext on a post-merge MapValue";
        if (frameId != lastFrameId) {
            if (frameId != lastFrameId + 1) {
                SortedRunsMerge.appendBatchStart(allocator, mapValue, valueIndex + 3, count);
            }
            mapValue.putLong(valueIndex + 6, frameId);
        }
        // Append observation, growing the buffer if needed
        long capacity = mapValue.getLong(valueIndex + 2);
        long ptr = mapValue.getLong(valueIndex);
        if (count >= capacity) {
            long newCapacity = capacity * 2;
            long newPtr = allocator.malloc(newCapacity * ENTRY_SIZE);
            Vect.memcpy(newPtr, ptr, count * ENTRY_SIZE);
            ptr = newPtr;
            mapValue.putLong(valueIndex, ptr);
            mapValue.putLong(valueIndex + 2, newCapacity);
        }
        long offset = count * ENTRY_SIZE;
        Unsafe.putLong(ptr + offset, ts);
        Unsafe.putDouble(ptr + offset + 8, price);
        mapValue.putLong(valueIndex + 1, count + 1);
    }

    /**
     * Computes TWAP from the observation buffer. Before integration, sorts the
     * buffer by timestamp via {@link SortedRunsMerge#compactInPlace}: under
     * parallel GROUP BY the buffer is a concatenation of per-frame batches that
     * a slot may have appended out of order, and the step-function integration
     * relies on monotonic timestamps.
     * <p>
     * The result is memoized per buffer pointer so that repeated calls within
     * the same SQL projection avoid re-scanning the buffer. The compaction
     * step preserves the buffer pointer, so the cache key stays valid across
     * the in-place sort.
     * <p>
     * The step-function integration walks consecutive pairs: each price is
     * weighted by the duration until the next observation. When all timestamps
     * are identical (totalDuration == 0), returns the simple arithmetic mean.
     */
    @Override
    public double getDouble(Record rec) {
        long count = rec.getLong(valueIndex + 1);
        if (count <= 0) {
            return Double.NaN;
        }
        long ptr = rec.getLong(valueIndex);
        if (ptr == cachedPtr) {
            return cachedValue;
        }
        SortedRunsMerge.compactInPlace(
                allocator, scratch, ptr, count,
                rec.getLong(valueIndex + 3), rec.getLong(valueIndex + 4),
                ENTRY_SIZE
        );
        double result;
        if (count == 1) {
            result = Unsafe.getDouble(ptr + 8);
        } else {
            double weightedSum = 0;
            double priceSum;
            long firstTs = Unsafe.getLong(ptr);
            long prevTs = firstTs;
            double prevPrice = Unsafe.getDouble(ptr + 8);
            priceSum = prevPrice;
            for (long i = 1; i < count; i++) {
                long offset = i * ENTRY_SIZE;
                long currTs = Unsafe.getLong(ptr + offset);
                double currPrice = Unsafe.getDouble(ptr + offset + 8);
                weightedSum += prevPrice * (currTs - prevTs);
                priceSum += currPrice;
                prevTs = currTs;
                prevPrice = currPrice;
            }
            long totalDuration = prevTs - firstTs;
            if (totalDuration > 0) {
                result = weightedSum / totalDuration;
            } else {
                // All observations at the same timestamp — simple average
                result = priceSum / count;
            }
        }
        cachedPtr = ptr;
        cachedValue = result;
        return result;
    }

    @Override
    public Function getLeft() {
        return priceFunc;
    }

    @Override
    public String getName() {
        return "twap";
    }

    @Override
    public Function getRight() {
        return tsFunc;
    }

    @Override
    public int getSampleByFlags() {
        return GroupByFunction.SAMPLE_BY_FILL_ALL;
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public void initSharedFrom(GroupByFunction primary) {
        initValueIndex(primary.getValueIndex());
        // Register with the primary so its setAllocator propagates the
        // owner's allocator here. getDouble's compactInPlace allocates a
        // transient aux buffer it frees before return, so sharing the
        // owner's allocator is safe.
        TwapGroupByFunction p = (TwapGroupByFunction) primary;
        if (p.sharedDependents == null) {
            p.sharedDependents = new ObjList<>();
        }
        p.sharedDependents.add(this);
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG);   // +0: entry buffer pointer
        columnTypes.add(ColumnType.LONG);   // +1: count
        columnTypes.add(ColumnType.LONG);   // +2: entry buffer capacity
        columnTypes.add(ColumnType.LONG);   // +3: descriptor buffer pointer
        columnTypes.add(ColumnType.LONG);   // +4: descriptor count
        columnTypes.add(ColumnType.LONG);   // +5: descriptor buffer capacity
        columnTypes.add(ColumnType.LONG);   // +6: last frame index
    }

    @Override
    public boolean isConstant() {
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
     * Combines the source slot's observation buffer with the destination's
     * into a single sorted buffer allocated in the destination's allocator
     * (the owner's allocator on the non-sharded merge path, a per-worker
     * allocator on the sharded one). Each input is a sequence of per-frame
     * batches; {@link SortedRunsMerge#compactInto} sorts the combined batch
     * set by timestamp and concatenates the batches, with no element-wise
     * merge. The merged buffer carries its own descriptor buffer so a
     * higher-level merge can interleave it with another partial result.
     * <p>
     * The old destination buffers are abandoned and reclaimed when the
     * allocator is closed.
     */
    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcCount = srcValue.getLong(valueIndex + 1);
        if (srcCount <= 0) {
            return;
        }
        long srcPtr = srcValue.getLong(valueIndex);
        long srcDescPtr = srcValue.getLong(valueIndex + 3);
        long srcDescCount = srcValue.getLong(valueIndex + 4);
        long destCount = destValue.getLong(valueIndex + 1);
        long destPtr = destCount > 0 ? destValue.getLong(valueIndex) : 0;
        long destDescPtr = destCount > 0 ? destValue.getLong(valueIndex + 3) : 0;
        long destDescCount = destCount > 0 ? destValue.getLong(valueIndex + 4) : 0;

        long mergedCount = destCount + srcCount;
        long destBatches = destCount <= 0 ? 0 : (destDescPtr == 0 ? 1 : destDescCount);
        long srcBatches = srcDescPtr == 0 ? 1 : srcDescCount;
        long mergedBatches = destBatches + srcBatches;

        // Fold the entry buffer and (optional) descriptor buffer into a
        // single allocator request. Two malloc calls per merge doubled the
        // chunk-grow rate on the high-cardinality path (JFR: 373 samples on
        // FastGroupByAllocator.malloc's chunk-alloc slow path under the 1M-
        // groups scenario); one larger block hits chunk-grow once per
        // merge and keeps the entry/descriptor pair contiguous for the
        // next merge's gather.
        long entryBytes = mergedCount * ENTRY_SIZE;
        long descBytes = mergedBatches > 1 ? mergedBatches * Long.BYTES : 0;
        long mergedPtr = allocator.malloc(entryBytes + descBytes);
        long mergedDescPtr = descBytes > 0 ? mergedPtr + entryBytes : 0;
        SortedRunsMerge.compactInto(
                scratch,
                mergedPtr, mergedDescPtr,
                destPtr, destCount, destDescPtr, destDescCount,
                srcPtr, srcCount, srcDescPtr, srcDescCount,
                ENTRY_SIZE
        );
        destValue.putLong(valueIndex, mergedPtr);
        destValue.putLong(valueIndex + 1, mergedCount);
        destValue.putLong(valueIndex + 2, mergedCount);
        destValue.putLong(valueIndex + 3, mergedDescPtr);
        destValue.putLong(valueIndex + 4, mergedDescPtr != 0 ? mergedBatches : 0);
        destValue.putLong(valueIndex + 5, mergedDescPtr != 0 ? mergedBatches : 0);
        // The descriptor buffer is sized exactly to fit the merge result, so
        // the first appendBatchStart on this value would silently trigger a
        // realloc. No current path adds rows after merge; assert that contract
        // by stamping a negative sentinel that computeNext picks up.
        destValue.putLong(valueIndex + 6, -1L);
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
        if (sharedDependents != null) {
            for (int i = 0, n = sharedDependents.size(); i < n; i++) {
                sharedDependents.getQuick(i).allocator = allocator;
            }
        }
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
        mapValue.putLong(valueIndex + 1, 0);
        mapValue.putLong(valueIndex + 2, 0);
        mapValue.putLong(valueIndex + 3, 0);
        mapValue.putLong(valueIndex + 4, 0);
        mapValue.putLong(valueIndex + 5, 0);
        mapValue.putLong(valueIndex + 6, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return BinaryFunction.super.supportsParallelism();
    }

    /**
     * Rejects the query at compile time unless the second argument is the
     * table's designated timestamp and the base query delivers rows in
     * ascending order of it. Both the parallel and the serial aggregation
     * paths rely on each page frame's observations already being sorted by
     * the timestamp argument; that holds only for the designated timestamp
     * of a forward scan.
     */
    public void validateTimestampArg(int designatedTimestampIndex, boolean isBaseTimestampAscending, int position) throws SqlException {
        ColumnFunction cf = ColumnFunction.unwrap(tsFunc);
        if (designatedTimestampIndex < 0 || cf == null || cf.getColumnIndex() != designatedTimestampIndex) {
            throw SqlException.$(position, "twap() requires the table's designated timestamp as the second argument");
        }
        if (!isBaseTimestampAscending) {
            throw SqlException.$(position, "twap() requires the base query to provide ascending designated timestamp order");
        }
    }
}
