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
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.SortedRunsMerge;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
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
 * page frame's rows in timestamp order, producing one sorted run per frame.
 * However, the slot a frame lands on is determined by lock acquisition in
 * {@link io.questdb.griffin.engine.PerWorkerLocks}, not by the worker's
 * identity, so under cross-query work-stealing a single slot's buffer can
 * accumulate frames in non-monotonic order. The buffer is therefore best
 * modelled as a concatenation of disjoint sorted runs whose key (timestamp)
 * ranges do not overlap, since each frame covers a contiguous rowId range
 * and frames are dispatched chronologically on TS-ordered data. See
 * {@link SortedRunsMerge} for the run-permutation compaction that
 * exploits this property at merge and read time.
 * <p>
 * The final TWAP is computed from the compacted, fully sorted buffer in
 * {@link #getDouble(Record)}.
 * <p>
 * <b>MapValue layout</b> (3 slots):
 * <pre>
 *   +0  LONG    native buffer pointer (0 = no observations)
 *   +1  LONG    observation count
 *   +2  LONG    buffer capacity in entries
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
    // Scratch list for run descriptors used by SortedRunsMerge. Not
    // thread-safe; each per-slot function instance owns its own.
    private final LongList runScratch = new LongList(16);
    private final Function tsFunc;
    private GroupByAllocator allocator;
    private long cachedPtr;
    private double cachedValue;
    private int valueIndex;

    public TwapGroupByFunction(@NotNull Function priceFunc, @NotNull Function tsFunc) {
        this.priceFunc = priceFunc;
        this.tsFunc = tsFunc;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        cachedPtr = 0;
        double price = priceFunc.getDouble(record);
        long ts = tsFunc.getLong(record);
        if (Double.isNaN(price) || ts == Numbers.LONG_NULL) {
            // NULL observation — initialize empty state
            mapValue.putLong(valueIndex, 0);
            mapValue.putLong(valueIndex + 1, 0);
            mapValue.putLong(valueIndex + 2, 0);
        } else {
            // Allocate buffer and store first observation
            long ptr = allocator.malloc(INITIAL_CAPACITY * ENTRY_SIZE);
            Unsafe.putLong(ptr, ts);
            Unsafe.putDouble(ptr + 8, price);
            mapValue.putLong(valueIndex, ptr);
            mapValue.putLong(valueIndex + 1, 1);
            mapValue.putLong(valueIndex + 2, INITIAL_CAPACITY);
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
            // First valid observation after initial NULLs
            long ptr = allocator.malloc(INITIAL_CAPACITY * ENTRY_SIZE);
            Unsafe.putLong(ptr, ts);
            Unsafe.putDouble(ptr + 8, price);
            mapValue.putLong(valueIndex, ptr);
            mapValue.putLong(valueIndex + 1, 1);
            mapValue.putLong(valueIndex + 2, INITIAL_CAPACITY);
        } else {
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
    }

    /**
     * Computes TWAP from the observation buffer. Before integration, ensures
     * the buffer is a single sorted run via
     * {@link SortedRunsMerge#compactInPlace}: under parallel GROUP BY the
     * buffer may be a concatenation of disjoint sorted runs (one per frame
     * landing on this slot), and the step-function integration relies on
     * monotonic timestamps.
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
        SortedRunsMerge.compactInPlace(allocator, runScratch, ptr, count, ENTRY_SIZE);
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
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG);   // +0: buffer pointer
        columnTypes.add(ColumnType.LONG);   // +1: count
        columnTypes.add(ColumnType.LONG);   // +2: buffer capacity
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
     * allocator on the sharded one). Both inputs are treated as
     * concatenations of disjoint sorted runs (one run per page frame); the
     * combined run set still satisfies the disjointness invariant because
     * different frames cover disjoint rowId ranges. See
     * {@link SortedRunsMerge} for the algorithm and the assertion that
     * fails loudly under {@code -ea} if disjointness ever breaks.
     * <p>
     * The old destination buffer is abandoned and reclaimed when the
     * allocator is closed.
     */
    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcCount = srcValue.getLong(valueIndex + 1);
        if (srcCount <= 0) {
            return;
        }
        long srcPtr = srcValue.getLong(valueIndex);
        long destCount = destValue.getLong(valueIndex + 1);
        long destPtr = destCount > 0 ? destValue.getLong(valueIndex) : 0;
        long mergedCount = destCount + srcCount;
        long mergedPtr = allocator.malloc(mergedCount * ENTRY_SIZE);
        SortedRunsMerge.compactInto(
                allocator,
                runScratch,
                mergedPtr,
                destPtr, destCount,
                srcPtr, srcCount,
                ENTRY_SIZE
        );
        destValue.putLong(valueIndex, mergedPtr);
        destValue.putLong(valueIndex + 1, mergedCount);
        destValue.putLong(valueIndex + 2, mergedCount);
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
        mapValue.putLong(valueIndex + 1, 0);
        mapValue.putLong(valueIndex + 2, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return true;
    }
}
