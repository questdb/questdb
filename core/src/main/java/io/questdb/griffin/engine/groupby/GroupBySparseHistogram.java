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


package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.CairoException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Off-heap HdrHistogram for {@link io.questdb.griffin.engine.functions.GroupByFunction}s over LONG values,
 * with lowestDiscernibleValue 1 and a fixed number of significant digits (0..5). It records exactly the
 * per-index counts an auto-resizing {@link io.questdb.std.histogram.org.HdrHistogram.Histogram} or
 * {@link io.questdb.std.histogram.org.HdrHistogram.PackedHistogram} with the same precision records,
 * and {@link #getValueAtPercentile(double)} walks them with the same arithmetic, so the two return
 * the same value for the same multiset of recorded values. The counts are integers, so
 * {@link #merge(GroupBySparseHistogram)} is exact and order-independent: a histogram merged from
 * per-worker partials equals the one recorded serially.
 * <p>
 * Only the non-zero counts are stored, in an open-addressing hash table keyed by counts index. The
 * memory is proportional to the number of distinct counts indexes recorded (12 bytes per slot, at
 * most 4/3 slots per index after the first resize), never to the span of the recorded values: a group
 * that records 1 and 10^15 at 5 digits takes 64 bytes. The table lives in the {@link GroupByAllocator},
 * so it is charged to the query's memory tracker with the rest of the GROUP BY state. Recording a
 * value is an index computation, a hash probe and an increment.
 * <p>
 * Reading a percentile copies the (index, count) pairs into a scratch buffer owned by this flyweight,
 * sorts them by index and walks them; the table itself is never modified by a read. The scratch buffer
 * is freed by {@link #close()}.
 * <p>
 * Layout, one block from the {@link GroupByAllocator}:
 * <pre>
 * | totalCount (8) | size (4) | capacity (4) | keys: capacity * 4 | counts: capacity * 8 |
 * </pre>
 * A key is the counts index + 1; 0 marks an empty slot. The capacity is a power of two, at least 4.
 */
public class GroupBySparseHistogram implements Mutable, QuietCloseable {
    private static final long CAPACITY_OFFSET = 12;
    private static final long HEADER_SIZE = 16;
    private static final int INITIAL_CAPACITY = 4;
    private static final long SIZE_OFFSET = 8;
    private static final long TOTAL_COUNT_OFFSET = 0;
    private final int leadingZeroCountBase;
    private final int subBucketHalfCount;
    private final int subBucketHalfCountMagnitude;
    private final long subBucketMask;
    private GroupByAllocator allocator;
    private long ptr;
    // scratch for reads: sorted (index, count) pairs of the histogram at sortedPtr
    private long scratchCapacity;
    private long scratchPtr;
    private int sortedSize = -1;

    public GroupBySparseHistogram(int numberOfSignificantValueDigits) {
        if (numberOfSignificantValueDigits < 0 || numberOfSignificantValueDigits > 5) {
            throw new IllegalArgumentException("numberOfSignificantValueDigits must be between 0 and 5");
        }
        // The derived fields of AbstractHistogram with lowestDiscernibleValue = 1 (unitMagnitude = 0),
        // computed with the same floating-point expressions.
        final long largestValueWithSingleUnitResolution = 2 * (long) Math.pow(10, numberOfSignificantValueDigits);
        final int subBucketCountMagnitude = (int) Math.ceil(Math.log(largestValueWithSingleUnitResolution) / Math.log(2));
        this.subBucketHalfCountMagnitude = subBucketCountMagnitude - 1;
        final int subBucketCount = 1 << subBucketCountMagnitude;
        this.subBucketHalfCount = subBucketCount / 2;
        this.subBucketMask = (long) subBucketCount - 1;
        this.leadingZeroCountBase = 64 - subBucketCountMagnitude;
    }

    /**
     * Bytes the histogram at ptr occupies in the allocator.
     */
    public static long footprint(long ptr) {
        return ptr != 0 ? blockSize(Unsafe.getInt(ptr + CAPACITY_OFFSET)) : 0;
    }

    /**
     * Detaches the flyweight and frees its read scratch buffer. The histogram memory belongs to the
     * allocator and is not touched.
     */
    @Override
    public void clear() {
        ptr = 0;
        sortedSize = -1;
        if (scratchPtr != 0) {
            scratchPtr = Unsafe.free(scratchPtr, scratchCapacity, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
            scratchCapacity = 0;
        }
    }

    @Override
    public void close() {
        clear();
    }

    public long getTotalCount() {
        return ptr != 0 ? Unsafe.getLong(ptr + TOTAL_COUNT_OFFSET) : 0;
    }

    /**
     * The same walk as AbstractHistogram.getValueAtPercentile: the first counts index at which the
     * running count reaches ceil(percentile * totalCount / 100), at least 1, mapped to the highest
     * value equivalent to it (the lowest one for percentile 0). Indexes that were never recorded hold
     * zero counts, so walking only the recorded ones in index order does not change where the running
     * count crosses the target.
     *
     * @param percentile percentile in [0, 100]
     * @return the value at the percentile, or 0 when nothing was recorded
     */
    public long getValueAtPercentile(final double percentile) {
        final long totalCount = getTotalCount();
        if (totalCount == 0) {
            return 0;
        }
        sortCounts();
        final double requestedPercentile =
                Math.min(Math.max(Math.nextAfter(percentile, Double.NEGATIVE_INFINITY), 0.0D), 100.0D);
        final double fpCountAtPercentile = (requestedPercentile * totalCount) / 100.0D;
        long countAtPercentile = (long) (Math.ceil(fpCountAtPercentile)); // round up
        countAtPercentile = Math.max(countAtPercentile, 1); // make sure we at least reach the first recorded entry
        long totalToCurrentIndex = 0;
        for (long p = scratchPtr, lim = scratchPtr + ((long) sortedSize << 4); p < lim; p += 16) {
            totalToCurrentIndex += Unsafe.getLong(p + 8);
            if (totalToCurrentIndex >= countAtPercentile) {
                final long valueAtIndex = valueFromIndex((int) (Unsafe.getLong(p) - 1));
                return (percentile == 0.0) ? lowestEquivalentValue(valueAtIndex) : highestEquivalentValue(valueAtIndex);
            }
        }
        return 0;
    }

    /**
     * Adds the other histogram's counts to this one. Both must have the same precision. The other
     * histogram is not modified and none of its memory is adopted.
     */
    public void merge(GroupBySparseHistogram other) {
        final long otherPtr = other.ptr;
        if (otherPtr == 0) {
            return;
        }
        final long otherTotal = Unsafe.getLong(otherPtr + TOTAL_COUNT_OFFSET);
        if (otherTotal == 0) {
            return;
        }
        final int otherCapacity = Unsafe.getInt(otherPtr + CAPACITY_OFFSET);
        if (ptr == 0) {
            // adopt the other's layout: a copy, so the other's memory stays its own
            final long size = blockSize(otherCapacity);
            ptr = allocator.malloc(size);
            Vect.memcpy(ptr, otherPtr, size);
            sortedSize = -1;
            return;
        }
        final long otherKeys = otherPtr + HEADER_SIZE;
        final long otherCounts = otherKeys + ((long) otherCapacity << 2);
        for (int slot = 0; slot < otherCapacity; slot++) {
            final int key = Unsafe.getInt(otherKeys + ((long) slot << 2));
            if (key != 0) {
                add(key, Unsafe.getLong(otherCounts + ((long) slot << 3)));
            }
        }
        Unsafe.putLong(ptr + TOTAL_COUNT_OFFSET, Unsafe.getLong(ptr + TOTAL_COUNT_OFFSET) + otherTotal);
        sortedSize = -1;
    }

    public GroupBySparseHistogram of(long ptr) {
        this.ptr = ptr;
        sortedSize = -1;
        return this;
    }

    public long ptr() {
        return ptr;
    }

    public void recordValue(long value) {
        if (value < 0) {
            // the same error as AbstractHistogram.countsArrayIndex
            throw CairoException.nonCritical().put("Histogram recorded value cannot be negative.");
        }
        if (ptr == 0) {
            ptr = allocator.malloc(blockSize(INITIAL_CAPACITY));
            Vect.memset(ptr, blockSize(INITIAL_CAPACITY), 0);
            Unsafe.putInt(ptr + CAPACITY_OFFSET, INITIAL_CAPACITY);
        }
        add(countsArrayIndex(value) + 1, 1);
        Unsafe.putLong(ptr + TOTAL_COUNT_OFFSET, Unsafe.getLong(ptr + TOTAL_COUNT_OFFSET) + 1);
        sortedSize = -1;
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    private static long blockSize(int capacity) {
        return HEADER_SIZE + 12L * capacity;
    }

    private static int slotOf(int key, int capacity) {
        // Fibonacci hashing: the top bits of key * 2^64 / phi, spreads runs and strides of indexes
        return (int) ((key * 0x9E3779B97F4A7C15L) >>> (64 - Integer.numberOfTrailingZeros(capacity)));
    }

    // Adds count to the counts index key - 1, inserting it when absent. Does not touch totalCount.
    private void add(int key, long count) {
        int capacity = Unsafe.getInt(ptr + CAPACITY_OFFSET);
        long keys = ptr + HEADER_SIZE;
        int mask = capacity - 1;
        int slot = slotOf(key, capacity);
        while (true) {
            final long keyAddr = keys + ((long) slot << 2);
            final int k = Unsafe.getInt(keyAddr);
            if (k == key) {
                final long countAddr = keys + ((long) capacity << 2) + ((long) slot << 3);
                Unsafe.putLong(countAddr, Unsafe.getLong(countAddr) + count);
                return;
            }
            if (k == 0) {
                final int size = Unsafe.getInt(ptr + SIZE_OFFSET);
                if ((size + 1) * 4L > capacity * 3L) {
                    // over 3/4 full: double, then insert into the new table
                    grow(capacity << 1);
                    capacity = Unsafe.getInt(ptr + CAPACITY_OFFSET);
                    keys = ptr + HEADER_SIZE;
                    mask = capacity - 1;
                    slot = slotOf(key, capacity);
                    while (Unsafe.getInt(keys + ((long) slot << 2)) != 0) {
                        slot = (slot + 1) & mask;
                    }
                }
                Unsafe.putInt(keys + ((long) slot << 2), key);
                Unsafe.putLong(keys + ((long) capacity << 2) + ((long) slot << 3), count);
                Unsafe.putInt(ptr + SIZE_OFFSET, size + 1);
                return;
            }
            slot = (slot + 1) & mask;
        }
    }

    private int countsArrayIndex(long value) {
        final int bucketIndex = leadingZeroCountBase - Long.numberOfLeadingZeros(value | subBucketMask);
        final int subBucketIndex = (int) (value >>> bucketIndex);
        final int bucketBaseIndex = (bucketIndex + 1) << subBucketHalfCountMagnitude;
        return bucketBaseIndex + subBucketIndex - subBucketHalfCount;
    }

    private int getBucketIndex(long value) {
        return leadingZeroCountBase - Long.numberOfLeadingZeros(value | subBucketMask);
    }

    private void grow(int newCapacity) {
        final long oldPtr = ptr;
        final int oldCapacity = Unsafe.getInt(oldPtr + CAPACITY_OFFSET);
        final long newSize = blockSize(newCapacity);
        final long newPtr = allocator.malloc(newSize);
        Vect.memset(newPtr, newSize, 0);
        Unsafe.putLong(newPtr + TOTAL_COUNT_OFFSET, Unsafe.getLong(oldPtr + TOTAL_COUNT_OFFSET));
        Unsafe.putInt(newPtr + SIZE_OFFSET, Unsafe.getInt(oldPtr + SIZE_OFFSET));
        Unsafe.putInt(newPtr + CAPACITY_OFFSET, newCapacity);
        final long oldKeys = oldPtr + HEADER_SIZE;
        final long oldCounts = oldKeys + ((long) oldCapacity << 2);
        final long newKeys = newPtr + HEADER_SIZE;
        final long newCounts = newKeys + ((long) newCapacity << 2);
        final int mask = newCapacity - 1;
        for (int slot = 0; slot < oldCapacity; slot++) {
            final int key = Unsafe.getInt(oldKeys + ((long) slot << 2));
            if (key != 0) {
                int s = slotOf(key, newCapacity);
                while (Unsafe.getInt(newKeys + ((long) s << 2)) != 0) {
                    s = (s + 1) & mask;
                }
                Unsafe.putInt(newKeys + ((long) s << 2), key);
                Unsafe.putLong(newCounts + ((long) s << 3), Unsafe.getLong(oldCounts + ((long) slot << 3)));
            }
        }
        allocator.free(oldPtr, blockSize(oldCapacity));
        ptr = newPtr;
    }

    private long highestEquivalentValue(long value) {
        final int bucketIndex = getBucketIndex(value);
        return lowestEquivalentValue(value) + (1L << bucketIndex) - 1;
    }

    private long lowestEquivalentValue(long value) {
        final int bucketIndex = getBucketIndex(value);
        final int subBucketIndex = (int) (value >>> bucketIndex);
        return ((long) subBucketIndex) << bucketIndex;
    }

    // Copies the (key, count) pairs of the current histogram into the scratch buffer, sorted by key.
    private void sortCounts() {
        if (sortedSize >= 0) {
            return;
        }
        final int size = Unsafe.getInt(ptr + SIZE_OFFSET);
        final long bytes = (long) size << 4;
        if (bytes > scratchCapacity) {
            final long newCapacity = Math.max(bytes, scratchCapacity * 2);
            scratchPtr = scratchPtr == 0
                    ? Unsafe.malloc(newCapacity, MemoryTag.NATIVE_GROUP_BY_FUNCTION)
                    : Unsafe.realloc(scratchPtr, scratchCapacity, newCapacity, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
            scratchCapacity = newCapacity;
        }
        final int capacity = Unsafe.getInt(ptr + CAPACITY_OFFSET);
        final long keys = ptr + HEADER_SIZE;
        final long counts = keys + ((long) capacity << 2);
        long p = scratchPtr;
        for (int slot = 0; slot < capacity; slot++) {
            final int key = Unsafe.getInt(keys + ((long) slot << 2));
            if (key != 0) {
                Unsafe.putLong(p, key);
                Unsafe.putLong(p + 8, Unsafe.getLong(counts + ((long) slot << 3)));
                p += 16;
            }
        }
        if (size > 1) {
            Vect.sortLongIndexAscInPlace(scratchPtr, size);
        }
        sortedSize = size;
    }

    private long valueFromIndex(int index) {
        int bucketIndex = (index >> subBucketHalfCountMagnitude) - 1;
        int subBucketIndex = (index & (subBucketHalfCount - 1)) + subBucketHalfCount;
        if (bucketIndex < 0) {
            subBucketIndex -= subBucketHalfCount;
            bucketIndex = 0;
        }
        return ((long) subBucketIndex) << bucketIndex;
    }
}
