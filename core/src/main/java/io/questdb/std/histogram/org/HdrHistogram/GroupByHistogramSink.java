/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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

package io.questdb.std.histogram.org.HdrHistogram;

import io.questdb.cairo.CairoException;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Specialized off-heap histogram sink used in {@link io.questdb.griffin.engine.functions.GroupByFunction}s.
 * <p>
 * Uses provided {@link GroupByAllocator} to allocate the underlying buffer. Grows the buffer when needed.
 * <p>
 * Buffer layout is the following:
 * <pre>
 * | totalCount | normalizingIndexOffset | maxValue | minNonZeroValue | histogram counts array |
 * +------------+------------------------+----------+-----------------+------------------------+
 * |  8 bytes   |        4 bytes         | 8 bytes  |    8 bytes      | countsArrayLength * 8  |
 * +------------+------------------------+----------+-----------------+------------------------+
 * </pre>
 */
public class GroupByHistogramSink extends AbstractHistogram implements Mutable {

    private static final long headerSize = Long.BYTES + Integer.BYTES + Long.BYTES + Long.BYTES;
    private static final long normalizingIndexOffsetPosition = Long.BYTES;
    private static final long maxValuePosition = Long.BYTES + Integer.BYTES;
    private static final long minNonZeroValuePosition = Long.BYTES + Integer.BYTES + Long.BYTES;

    private GroupByAllocator allocator;
    private long ptr;
    private long allocatedSize;
    private int normalizingIndexOffset;
    private long totalCount;

    public GroupByHistogramSink(int numberOfSignificantValueDigits) {
        this(1, 2, numberOfSignificantValueDigits);
        setAutoResize(true);
    }

    public GroupByHistogramSink(long lowestDiscernibleValue, long highestTrackableValue, int numberOfSignificantValueDigits) {
        super(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
        wordSizeInBytes = 8;
    }

    GroupByHistogramSink(AbstractHistogram source) {
        super(source);
        wordSizeInBytes = 8;
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    public long ptr() {
        return ptr;
    }

    public GroupByHistogramSink of(long ptr) {
        this.ptr = ptr;
        if (ptr != 0) {
            this.allocatedSize = headerSize + (countsArrayLength * 8L);
            this.totalCount = Unsafe.getUnsafe().getLong(ptr);
            this.normalizingIndexOffset = Unsafe.getUnsafe().getInt(ptr + normalizingIndexOffsetPosition);
            this.maxValue = Unsafe.getUnsafe().getLong(ptr + maxValuePosition);
            this.minNonZeroValue = Unsafe.getUnsafe().getLong(ptr + minNonZeroValuePosition);
        } else {
            this.allocatedSize = 0;
            this.totalCount = 0;
            this.normalizingIndexOffset = 0;
            this.maxValue = 0;
            this.minNonZeroValue = Long.MAX_VALUE;
        }
        return this;
    }

    @Override
    public void clear() {
        ptr = 0;
        allocatedSize = 0;
        totalCount = 0;
        normalizingIndexOffset = 0;
        maxValue = 0;
        minNonZeroValue = Long.MAX_VALUE;
    }

    @Override
    public GroupByHistogramSink copy() {
        GroupByHistogramSink copy = new GroupByHistogramSink(this);
        copy.setAllocator(allocator);
        copy.add(this);
        return copy;
    }

    @Override
    public GroupByHistogramSink copyCorrectedForCoordinatedOmission(long expectedIntervalBetweenValueSamples) {
        GroupByHistogramSink copy = new GroupByHistogramSink(this);
        copy.setAllocator(allocator);
        copy.addWhileCorrectingForCoordinatedOmission(this, expectedIntervalBetweenValueSamples);
        return copy;
    }

    @Override
    public long getCountAtIndex(int index) {
        if (ptr == 0) {
            return 0;
        }
        return Unsafe.getUnsafe().getLong(ptr + headerSize + ((long) normalizeIndex(index, normalizingIndexOffset, countsArrayLength) << 3));
    }

    @Override
    public long getTotalCount() {
        return totalCount;
    }

    @Override
    public long getMaxValue() {
        if (ptr == 0) {
            return 0;
        }
        long maxVal = Unsafe.getUnsafe().getLong(ptr + maxValuePosition);
        return (maxVal == 0) ? 0 : highestEquivalentValue(maxVal);
    }

    @Override
    public long getMinNonZeroValue() {
        if (ptr == 0) {
            return Long.MAX_VALUE;
        }
        long minVal = Unsafe.getUnsafe().getLong(ptr + minNonZeroValuePosition);
        return (minVal == Long.MAX_VALUE) ? Long.MAX_VALUE : lowestEquivalentValue(minVal);
    }

    @Override
    public void setIntegerToDoubleValueConversionRatio(double ratio) {
        nonConcurrentSetIntegerToDoubleValueConversionRatio(ratio);
    }

    @Override
    int _getEstimatedFootprintInBytes() {
        return 512 + (int) allocatedSize;
    }

    @Override
    void addToCountAtIndex(int index, long value) {
        checkBounds(index);
        ensureCapacity();
        long addr = ptr + headerSize + ((long) normalizeIndex(index, normalizingIndexOffset, countsArrayLength) << 3);
        Unsafe.getUnsafe().putLong(addr, Unsafe.getUnsafe().getLong(addr) + value);
    }

    @Override
    void addToTotalCount(long value) {
        totalCount += value;
        if (ptr != 0) {
            Unsafe.getUnsafe().putLong(ptr, totalCount);
        }
    }

    @Override
    void clearCounts() {
        if (ptr != 0) {
            Vect.memset(ptr + headerSize, countsArrayLength * 8L, 0);
            Unsafe.getUnsafe().putLong(ptr, 0);
            Unsafe.getUnsafe().putLong(ptr + maxValuePosition, 0);
            Unsafe.getUnsafe().putLong(ptr + minNonZeroValuePosition, Long.MAX_VALUE);
        }
        totalCount = 0;
        maxValue = 0;
        minNonZeroValue = Long.MAX_VALUE;
    }

    @Override
    long getCountAtNormalizedIndex(int index) {
        if (ptr == 0) {
            return 0;
        }
        return Unsafe.getUnsafe().getLong(ptr + headerSize + ((long) index << 3));
    }

    @Override
    int getNormalizingIndexOffset() {
        return normalizingIndexOffset;
    }

    @Override
    void incrementCountAtIndex(int index) {
        checkBounds(index);
        ensureCapacity();
        long addr = ptr + headerSize + ((long) normalizeIndex(index, normalizingIndexOffset, countsArrayLength) << 3);
        Unsafe.getUnsafe().putLong(addr, Unsafe.getUnsafe().getLong(addr) + 1);
    }

    @Override
    void incrementTotalCount() {
        totalCount++;
        if (ptr != 0) {
            Unsafe.getUnsafe().putLong(ptr, totalCount);
        }
    }

    @Override
    void resize(long newHighestTrackableValue) {
        int oldNormalizedZeroIndex = normalizeIndex(0, normalizingIndexOffset, countsArrayLength);
        int oldCountsArrayLength = countsArrayLength;
        boolean hadPreviousAllocation = (ptr != 0);

        establishSize(newHighestTrackableValue);

        long newCapacity = headerSize + (countsArrayLength * 8L);

        if (hadPreviousAllocation) {
            reallocateAndExpandCounts(newCapacity, oldNormalizedZeroIndex, oldCountsArrayLength);
        } else {
            allocateNewCounts(newCapacity);
        }
    }

    private void reallocateAndExpandCounts(long newCapacity, int oldNormalizedZeroIndex, int oldCountsArrayLength) {
        ptr = allocator.realloc(ptr, allocatedSize, newCapacity);
        allocatedSize = newCapacity;

        int countsDelta = countsArrayLength - oldCountsArrayLength;

        long bytesToZero = countsDelta * 8L;
        Vect.memset(ptr + headerSize + (oldCountsArrayLength * 8L), bytesToZero, 0);

        if (oldNormalizedZeroIndex != 0) {
            shiftCountsArrayForResize(oldNormalizedZeroIndex, oldCountsArrayLength, countsDelta);
        }
    }

    private void allocateNewCounts(long newCapacity) {
        ptr = allocator.malloc(newCapacity);
        allocatedSize = newCapacity;

        Vect.memset(ptr, newCapacity, 0);
    }

    private void shiftCountsArrayForResize(int oldNormalizedZeroIndex, int oldCountsArrayLength, int countsDelta) {
        int newNormalizedZeroIndex = oldNormalizedZeroIndex + countsDelta;
        int lengthToCopy = oldCountsArrayLength - oldNormalizedZeroIndex;

        long srcAddr = ptr + headerSize + (oldNormalizedZeroIndex * 8L);
        long dstAddr = ptr + headerSize + (newNormalizedZeroIndex * 8L);
        long bytesToCopy = lengthToCopy * 8L;
        Vect.memcpy(dstAddr, srcAddr, bytesToCopy);

        long gapStart = ptr + headerSize + (oldNormalizedZeroIndex * 8L);
        long gapSize = countsDelta * 8L;
        Vect.memset(gapStart, gapSize, 0);
    }

    @Override
    void setCountAtIndex(int index, long value) {
        checkBounds(index);
        ensureCapacity();
        Unsafe.getUnsafe().putLong(ptr + headerSize + ((long) normalizeIndex(index, normalizingIndexOffset, countsArrayLength) << 3), value);
    }

    @Override
    void setCountAtNormalizedIndex(int index, long value) {
        checkBounds(index);
        ensureCapacity();
        Unsafe.getUnsafe().putLong(ptr + headerSize + ((long) index << 3), value);
    }

    @Override
    void setNormalizingIndexOffset(int offset) {
        this.normalizingIndexOffset = offset;
        if (ptr != 0) {
            Unsafe.getUnsafe().putInt(ptr + normalizingIndexOffsetPosition, offset);
        }
    }

    @Override
    void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
        if (ptr != 0) {
            Unsafe.getUnsafe().putLong(ptr, totalCount);
        }
    }

    @Override
    void shiftNormalizingIndexByOffset(int offsetToAdd, boolean lowestHalfBucketPopulated, double newRatio) {
        nonConcurrentNormalizingIndexShift(offsetToAdd, lowestHalfBucketPopulated);
    }

    @Override
    void updateMinAndMax(long value) {
        if (ptr == 0) {
            return;
        }

        long currentMax = Unsafe.getUnsafe().getLong(ptr + maxValuePosition);
        if (value > currentMax) {
            long newMax = value | unitMagnitudeMask;
            Unsafe.getUnsafe().putLong(ptr + maxValuePosition, newMax);
            maxValue = newMax;
        }

        long currentMin = Unsafe.getUnsafe().getLong(ptr + minNonZeroValuePosition);
        if ((value < currentMin) && (value != 0)) {
            if (value <= unitMagnitudeMask) {
                return;
            }
            long newMin = value & ~unitMagnitudeMask;
            Unsafe.getUnsafe().putLong(ptr + minNonZeroValuePosition, newMin);
            minNonZeroValue = newMin;
        }
    }

    @Override
    protected void updateMaxValue(long value) {
        if (ptr == 0) {
            return;
        }
        long currentMax = Unsafe.getUnsafe().getLong(ptr + maxValuePosition);
        long newMax = Math.max(currentMax, value | unitMagnitudeMask);
        Unsafe.getUnsafe().putLong(ptr + maxValuePosition, newMax);
        maxValue = newMax;
    }

    @Override
    protected void updateMinNonZeroValue(long value) {
        if (ptr == 0) {
            return;
        }
        if (value <= unitMagnitudeMask) {
            return;
        }
        long currentMin = Unsafe.getUnsafe().getLong(ptr + minNonZeroValuePosition);
        long newMin = Math.min(currentMin, value & ~unitMagnitudeMask);
        Unsafe.getUnsafe().putLong(ptr + minNonZeroValuePosition, newMin);
        minNonZeroValue = newMin;
    }

    private void ensureCapacity() {
        if (ptr == 0) {
            allocatedSize = headerSize + (countsArrayLength * 8L);
            ptr = allocator.malloc(allocatedSize);

            Unsafe.getUnsafe().putLong(ptr, 0);
            Unsafe.getUnsafe().putInt(ptr + normalizingIndexOffsetPosition, 0);
            Unsafe.getUnsafe().putLong(ptr + maxValuePosition, 0);
            Unsafe.getUnsafe().putLong(ptr + minNonZeroValuePosition, Long.MAX_VALUE);

            Vect.memset(ptr + headerSize, countsArrayLength * 8L, 0);
        }
    }

    private void checkBounds(int index) {
        if (index < 0 || index >= countsArrayLength) {
            throw CairoException.nonCritical()
                .put("index ").put(index)
                .put(" out of bounds [0, ").put(countsArrayLength).put(')');
        }
    }
}
