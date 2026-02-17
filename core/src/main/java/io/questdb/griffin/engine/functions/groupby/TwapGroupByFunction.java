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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
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
 * Instead, each worker records all (timestamp, price) observations in a native
 * buffer. Within a single worker, observations arrive in timestamp order
 * (page frames are dispatched chronologically). During the merge phase, the
 * two sorted per-worker buffers are combined with a merge-sort merge step.
 * The final TWAP is computed from the fully sorted buffer in
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
            Unsafe.getUnsafe().putLong(ptr, ts);
            Unsafe.getUnsafe().putDouble(ptr + 8, price);
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
            Unsafe.getUnsafe().putLong(ptr, ts);
            Unsafe.getUnsafe().putDouble(ptr + 8, price);
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
            Unsafe.getUnsafe().putLong(ptr + offset, ts);
            Unsafe.getUnsafe().putDouble(ptr + offset + 8, price);
            mapValue.putLong(valueIndex + 1, count + 1);
        }
    }

    /**
     * Computes TWAP from the sorted observation buffer. The result is memoized
     * per buffer pointer so that repeated calls within the same SQL projection
     * avoid re-scanning the buffer.
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
        double result;
        if (count == 1) {
            result = Unsafe.getUnsafe().getDouble(ptr + 8);
        } else {
            double weightedSum = 0;
            double priceSum;
            long firstTs = Unsafe.getUnsafe().getLong(ptr);
            long prevTs = firstTs;
            double prevPrice = Unsafe.getUnsafe().getDouble(ptr + 8);
            priceSum = prevPrice;
            for (long i = 1; i < count; i++) {
                long offset = i * ENTRY_SIZE;
                long currTs = Unsafe.getUnsafe().getLong(ptr + offset);
                double currPrice = Unsafe.getUnsafe().getDouble(ptr + offset + 8);
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
     * Merges a source worker's observation buffer into the destination (owner)
     * buffer. Both buffers are sorted by timestamp within their respective
     * workers. The merge produces a single sorted buffer using a classic
     * merge-sort merge step, allocated in the owner's allocator.
     * <p>
     * When the destination is empty, the source buffer is copied into a fresh
     * allocation in the owner's allocator rather than copying the raw pointer,
     * because the source buffer lives in a worker allocator that will be
     * reclaimed independently.
     */
    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcCount = srcValue.getLong(valueIndex + 1);
        if (srcCount <= 0) {
            return;
        }
        long srcPtr = srcValue.getLong(valueIndex);

        long destCount = destValue.getLong(valueIndex + 1);
        if (destCount <= 0) {
            // Destination is empty — copy source buffer into owner's allocator
            long newCapacity = Math.max(INITIAL_CAPACITY, srcCount);
            long newPtr = allocator.malloc(newCapacity * ENTRY_SIZE);
            Vect.memcpy(newPtr, srcPtr, srcCount * ENTRY_SIZE);
            destValue.putLong(valueIndex, newPtr);
            destValue.putLong(valueIndex + 1, srcCount);
            destValue.putLong(valueIndex + 2, newCapacity);
            return;
        }

        // Both non-empty: merge two sorted buffers
        long destPtr = destValue.getLong(valueIndex);
        long mergedCount = destCount + srcCount;
        long mergedPtr = allocator.malloc(mergedCount * ENTRY_SIZE);

        // Classic merge-sort merge of two sorted runs
        long di = 0, si = 0, mi = 0;
        while (di < destCount && si < srcCount) {
            long destTs = Unsafe.getUnsafe().getLong(destPtr + di * ENTRY_SIZE);
            long srcTs = Unsafe.getUnsafe().getLong(srcPtr + si * ENTRY_SIZE);
            if (destTs <= srcTs) {
                Vect.memcpy(mergedPtr + mi * ENTRY_SIZE, destPtr + di * ENTRY_SIZE, ENTRY_SIZE);
                di++;
            } else {
                Vect.memcpy(mergedPtr + mi * ENTRY_SIZE, srcPtr + si * ENTRY_SIZE, ENTRY_SIZE);
                si++;
            }
            mi++;
        }
        // Copy remaining tail from whichever run was not exhausted
        if (di < destCount) {
            Vect.memcpy(
                    mergedPtr + mi * ENTRY_SIZE,
                    destPtr + di * ENTRY_SIZE,
                    (destCount - di) * ENTRY_SIZE
            );
        }
        if (si < srcCount) {
            Vect.memcpy(
                    mergedPtr + mi * ENTRY_SIZE,
                    srcPtr + si * ENTRY_SIZE,
                    (srcCount - si) * ENTRY_SIZE
            );
        }

        // Old dest buffer is abandoned — its allocator will reclaim the memory
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
