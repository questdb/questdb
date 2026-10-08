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
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Off-heap HdrHistogram for {@link io.questdb.griffin.engine.functions.GroupByFunction}s over LONG values,
 * with lowestDiscernibleValue 1 and a fixed number of significant digits (0..5). It records exactly the
 * per-index counts an auto-resizing {@link io.questdb.std.histogram.org.HdrHistogram.Histogram} or
 * {@link io.questdb.std.histogram.org.HdrHistogram.PackedHistogram} with the same precision records,
 * and {@link #getValueAtPercentile(double)} walks them with the same arithmetic, so the two return
 * the same value for the same multiset of recorded values. The counts are integers, so
 * {@link #merge(GroupByPagedHistogram)} is exact and order-independent: a histogram merged from
 * per-worker partials equals the one recorded serially.
 * <p>
 * The counts array is split into pages of {@link #PAGE_SIZE} counts, allocated when a value first
 * lands in them. A directory of page pointers covers the page range seen so far, so the memory is
 * proportional to the pages touched plus the span between the lowest and the highest one, not to
 * the full counts array of the precision (2^17 counts per bucket at 5 digits). Recording a value is
 * an index computation and an increment; the directory grows geometrically, so it is rarely resized.
 * <p>
 * Layout, all memory from the {@link GroupByAllocator}:
 * <pre>
 * header: | totalCount (8) | directory ptr (8) | first page index (4) | directory length (4) |
 * directory: directory length * 8 bytes, page pointers (0 = empty page)
 * page: PAGE_SIZE * 8 bytes, counts
 * </pre>
 */
public class GroupByPagedHistogram implements Mutable {
    public static final int PAGE_SHIFT = 7;
    public static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    private static final int PAGE_BYTES = PAGE_SIZE * Long.BYTES;
    private static final int PAGE_MASK = PAGE_SIZE - 1;
    private static final long DIR_BASE_OFFSET = 16;
    private static final long DIR_LEN_OFFSET = 20;
    private static final long DIR_PTR_OFFSET = 8;
    private static final long HEADER_SIZE = 24;
    private static final long TOTAL_COUNT_OFFSET = 0;
    private final int leadingZeroCountBase;
    private final int subBucketHalfCount;
    private final int subBucketHalfCountMagnitude;
    private final long subBucketMask;
    private GroupByAllocator allocator;
    private int dirBase;
    private int dirLen;
    private long dirPtr;
    private long ptr;

    public GroupByPagedHistogram(int numberOfSignificantValueDigits) {
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

    @Override
    public void clear() {
        ptr = 0;
        dirPtr = 0;
        dirBase = 0;
        dirLen = 0;
    }

    public long getTotalCount() {
        return ptr != 0 ? Unsafe.getLong(ptr + TOTAL_COUNT_OFFSET) : 0;
    }

    /**
     * The same walk as AbstractHistogram.getValueAtPercentile: the first counts index at which the
     * running count reaches ceil(percentile * totalCount / 100), at least 1, mapped to the highest
     * value equivalent to it (the lowest one for percentile 0). Pages that were never allocated hold
     * only zero counts, so skipping them does not change where the running count crosses the target.
     *
     * @param percentile percentile in [0, 100]
     * @return the value at the percentile, or 0 when nothing was recorded
     */
    public long getValueAtPercentile(final double percentile) {
        final long totalCount = getTotalCount();
        final double requestedPercentile =
                Math.min(Math.max(Math.nextAfter(percentile, Double.NEGATIVE_INFINITY), 0.0D), 100.0D);
        final double fpCountAtPercentile = (requestedPercentile * totalCount) / 100.0D;
        long countAtPercentile = (long) (Math.ceil(fpCountAtPercentile)); // round up
        countAtPercentile = Math.max(countAtPercentile, 1); // make sure we at least reach the first recorded entry
        long totalToCurrentIndex = 0;
        for (int slot = 0; slot < dirLen; slot++) {
            final long pagePtr = Unsafe.getLong(dirPtr + ((long) slot << 3));
            if (pagePtr == 0) {
                continue;
            }
            for (int j = 0; j < PAGE_SIZE; j++) {
                totalToCurrentIndex += Unsafe.getLong(pagePtr + ((long) j << 3));
                if (totalToCurrentIndex >= countAtPercentile) {
                    final long valueAtIndex = valueFromIndex(((dirBase + slot) << PAGE_SHIFT) + j);
                    return (percentile == 0.0) ? lowestEquivalentValue(valueAtIndex) : highestEquivalentValue(valueAtIndex);
                }
            }
        }
        return 0;
    }

    /**
     * Adds the other histogram's counts to this one. Both must have the same precision. The other
     * histogram is not modified and none of its memory is adopted.
     */
    public void merge(GroupByPagedHistogram other) {
        final long otherTotal = other.getTotalCount();
        if (otherTotal == 0) {
            return;
        }
        ensureHeader();
        for (int slot = 0, n = other.dirLen; slot < n; slot++) {
            final long srcPage = Unsafe.getLong(other.dirPtr + ((long) slot << 3));
            if (srcPage == 0) {
                continue;
            }
            final long dstPage = page(other.dirBase + slot);
            for (int j = 0; j < PAGE_SIZE; j++) {
                final long count = Unsafe.getLong(srcPage + ((long) j << 3));
                if (count != 0) {
                    final long addr = dstPage + ((long) j << 3);
                    Unsafe.putLong(addr, Unsafe.getLong(addr) + count);
                }
            }
        }
        Unsafe.putLong(ptr + TOTAL_COUNT_OFFSET, Unsafe.getLong(ptr + TOTAL_COUNT_OFFSET) + otherTotal);
    }

    public GroupByPagedHistogram of(long ptr) {
        this.ptr = ptr;
        if (ptr != 0) {
            dirPtr = Unsafe.getLong(ptr + DIR_PTR_OFFSET);
            dirBase = Unsafe.getInt(ptr + DIR_BASE_OFFSET);
            dirLen = Unsafe.getInt(ptr + DIR_LEN_OFFSET);
        } else {
            dirPtr = 0;
            dirBase = 0;
            dirLen = 0;
        }
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
        final int index = countsArrayIndex(value);
        ensureHeader();
        final long addr = page(index >>> PAGE_SHIFT) + ((long) (index & PAGE_MASK) << 3);
        Unsafe.putLong(addr, Unsafe.getLong(addr) + 1);
        Unsafe.putLong(ptr + TOTAL_COUNT_OFFSET, Unsafe.getLong(ptr + TOTAL_COUNT_OFFSET) + 1);
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    private int countsArrayIndex(long value) {
        final int bucketIndex = leadingZeroCountBase - Long.numberOfLeadingZeros(value | subBucketMask);
        final int subBucketIndex = (int) (value >>> bucketIndex);
        final int bucketBaseIndex = (bucketIndex + 1) << subBucketHalfCountMagnitude;
        return bucketBaseIndex + subBucketIndex - subBucketHalfCount;
    }

    private void ensureHeader() {
        if (ptr == 0) {
            ptr = allocator.malloc(HEADER_SIZE);
            Vect.memset(ptr, HEADER_SIZE, 0);
            dirPtr = 0;
            dirBase = 0;
            dirLen = 0;
        }
    }

    private int getBucketIndex(long value) {
        return leadingZeroCountBase - Long.numberOfLeadingZeros(value | subBucketMask);
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

    // Returns the address of the page, allocating it (and growing the directory) when absent.
    private long page(int pageIndex) {
        int slot = pageIndex - dirBase;
        if (slot < 0 || slot >= dirLen) {
            growDirectory(pageIndex);
            slot = pageIndex - dirBase;
        }
        final long slotAddr = dirPtr + ((long) slot << 3);
        long pagePtr = Unsafe.getLong(slotAddr);
        if (pagePtr == 0) {
            pagePtr = allocator.malloc(PAGE_BYTES);
            Vect.memset(pagePtr, PAGE_BYTES, 0);
            Unsafe.putLong(slotAddr, pagePtr);
        }
        return pagePtr;
    }

    private void growDirectory(int pageIndex) {
        final int newBase;
        final int newEnd;
        if (dirLen == 0) {
            newBase = pageIndex;
            newEnd = pageIndex + 1;
        } else if (pageIndex < dirBase) {
            // grow downwards by at least the current length, not below page 0
            newBase = Math.max(0, Math.min(pageIndex, dirBase - dirLen));
            newEnd = dirBase + dirLen;
        } else {
            newBase = dirBase;
            newEnd = (int) Math.max(pageIndex + 1L, Math.min(Integer.MAX_VALUE, (long) dirBase + 2L * dirLen));
        }
        final int newLen = newEnd - newBase;
        final long newDirBytes = (long) newLen << 3;
        final long newDirPtr = allocator.malloc(newDirBytes);
        Vect.memset(newDirPtr, newDirBytes, 0);
        if (dirLen > 0) {
            Vect.memcpy(newDirPtr + ((long) (dirBase - newBase) << 3), dirPtr, (long) dirLen << 3);
            allocator.free(dirPtr, (long) dirLen << 3);
        }
        dirPtr = newDirPtr;
        dirBase = newBase;
        dirLen = newLen;
        Unsafe.putLong(ptr + DIR_PTR_OFFSET, dirPtr);
        Unsafe.putInt(ptr + DIR_BASE_OFFSET, dirBase);
        Unsafe.putInt(ptr + DIR_LEN_OFFSET, dirLen);
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
