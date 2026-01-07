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
 * Specialized off-heap histogram used in {@link io.questdb.griffin.engine.functions.GroupByFunction}s.
 * <p>
 * Uses provided {@link GroupByAllocator} to allocate the underlying buffer. Grows the buffer when needed.
 * <p>
 * Buffer layout is the following:
 * <pre>
 * | totalCount | normalizingIndexOffset | maxValue | minNonZeroValue | countsArrayLength | bucketCount | highestTrackableValue | startTimeStampMsec | endTimeStampMsec | histogram counts array |
 * +------------+------------------------+----------+-----------------+-------------------+-------------+-----------------------+--------------------+------------------+------------------------+
 * |  8 bytes   |        4 bytes         | 8 bytes  |    8 bytes      |     4 bytes       |   4 bytes   |       8 bytes         |      8 bytes       |     8 bytes      | countsArrayLength * 8  |
 * +------------+------------------------+----------+-----------------+-------------------+-------------+-----------------------+--------------------+------------------+------------------------+
 * </pre>
 */
public class GroupByHistogram extends AbstractHistogram implements Mutable {

    private static final long normalizingIndexOffsetPosition = Long.BYTES;
    private static final long maxValuePosition = normalizingIndexOffsetPosition + Integer.BYTES;
    private static final long minNonZeroValuePosition = maxValuePosition + Long.BYTES;
    private static final long countsArrayLengthPosition = minNonZeroValuePosition + Long.BYTES;
    private static final long bucketCountPosition = countsArrayLengthPosition + Integer.BYTES;
    private static final long highestTrackableValuePosition = bucketCountPosition + Integer.BYTES;
    private static final long startTimeStampMsecPosition = highestTrackableValuePosition + Long.BYTES;
    private static final long endTimeStampMsecPosition = startTimeStampMsecPosition + Long.BYTES;
    private static final long headerSize = endTimeStampMsecPosition + Long.BYTES;

    private GroupByAllocator allocator;
    private long ptr;
    private long allocatedSize;

    public GroupByHistogram(int numberOfSignificantValueDigits) {
        this(1, 2, numberOfSignificantValueDigits);
        setAutoResize(true);
    }

    public GroupByHistogram(long lowestDiscernibleValue, long highestTrackableValue, int numberOfSignificantValueDigits) {
        super(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
        wordSizeInBytes = 8;
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    public long ptr() {
        return ptr;
    }

    public GroupByHistogram of(long ptr) {
        this.ptr = ptr;

        if (ptr != 0) {
            this.countsArrayLength = Unsafe.getUnsafe().getInt(ptr + countsArrayLengthPosition);
            this.bucketCount = Unsafe.getUnsafe().getInt(ptr + bucketCountPosition);
            this.highestTrackableValue = Unsafe.getUnsafe().getLong(ptr + highestTrackableValuePosition);
            this.allocatedSize = headerSize + (countsArrayLength * 8L);
        } else {
            this.allocatedSize = 0;
        }

        return this;
    }

    public void merge(GroupByHistogram other) {
        if (other.ptr == 0 || other.getTotalCount() == 0) {
            return;
        }

        if (this.ptr == 0) {
            ensureCapacity();
        }

        if (other.getMaxValue() > this.highestTrackableValue) {
            resize(other.getMaxValue());
        }

        this.add(other);
    }

    @Override
    public void clear() {
        ptr = 0;
        allocatedSize = 0;
    }

    @Override
    public GroupByHistogram copy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public GroupByHistogram copyCorrectedForCoordinatedOmission(long expectedIntervalBetweenValueSamples) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getCountAtIndex(int index) {
        if (ptr == 0) {
            return 0;
        }
        int normalizingIndexOffset = Unsafe.getUnsafe().getInt(ptr + normalizingIndexOffsetPosition);
        return Unsafe.getUnsafe().getLong(ptr + headerSize + ((long) normalizeIndex(index, normalizingIndexOffset, countsArrayLength) << 3));
    }

    @Override
    public long getTotalCount() {
        return (ptr != 0) ? Unsafe.getUnsafe().getLong(ptr) : 0;
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
    public long getStartTimeStamp() {
        return (ptr != 0) ? Unsafe.getUnsafe().getLong(ptr + startTimeStampMsecPosition) : Long.MAX_VALUE;
    }

    @Override
    public void setStartTimeStamp(long timeStampMsec) {
        if (ptr != 0) {
            Unsafe.getUnsafe().putLong(ptr + startTimeStampMsecPosition, timeStampMsec);
        }
    }

    @Override
    public long getEndTimeStamp() {
        return (ptr != 0) ? Unsafe.getUnsafe().getLong(ptr + endTimeStampMsecPosition) : 0;
    }

    @Override
    public void setEndTimeStamp(long timeStampMsec) {
        if (ptr != 0) {
            Unsafe.getUnsafe().putLong(ptr + endTimeStampMsecPosition, timeStampMsec);
        }
    }

    @Override
    public void setIntegerToDoubleValueConversionRatio(double ratio) {
        throw new UnsupportedOperationException();
    }

    @Override
    int _getEstimatedFootprintInBytes() {
        return 512 + (int) allocatedSize;
    }

    @Override
    void addToCountAtIndex(int index, long value) {
        checkBounds(index);
        ensureCapacity();
        int normalizingIndexOffset = Unsafe.getUnsafe().getInt(ptr + normalizingIndexOffsetPosition);
        long addr = ptr + headerSize + ((long) normalizeIndex(index, normalizingIndexOffset, countsArrayLength) << 3);
        Unsafe.getUnsafe().putLong(addr, Unsafe.getUnsafe().getLong(addr) + value);
    }

    @Override
    void addToTotalCount(long value) {
        if (ptr != 0) {
            long totalCount = Unsafe.getUnsafe().getLong(ptr);
            Unsafe.getUnsafe().putLong(ptr, totalCount + value);
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
        return (ptr != 0) ? Unsafe.getUnsafe().getInt(ptr + normalizingIndexOffsetPosition) : 0;
    }

    @Override
    void incrementCountAtIndex(int index) {
        checkBounds(index);
        ensureCapacity();
        int normalizingIndexOffset = Unsafe.getUnsafe().getInt(ptr + normalizingIndexOffsetPosition);
        long addr = ptr + headerSize + ((long) normalizeIndex(index, normalizingIndexOffset, countsArrayLength) << 3);
        Unsafe.getUnsafe().putLong(addr, Unsafe.getUnsafe().getLong(addr) + 1);
    }

    @Override
    void incrementTotalCount() {
        if (ptr != 0) {
            long totalCount = Unsafe.getUnsafe().getLong(ptr);
            Unsafe.getUnsafe().putLong(ptr, totalCount + 1);
        }
    }

    @Override
    void resize(long newHighestTrackableValue) {
        int oldNormalizingIndexOffset = (ptr != 0) ? Unsafe.getUnsafe().getInt(ptr + normalizingIndexOffsetPosition) : 0;
        int oldNormalizedZeroIndex = normalizeIndex(0, oldNormalizingIndexOffset, countsArrayLength);
        int oldCountsArrayLength = countsArrayLength;
        boolean hadPreviousAllocation = (ptr != 0);

        establishSize(newHighestTrackableValue);

        long newCapacity = headerSize + (countsArrayLength * 8L);

        if (hadPreviousAllocation) {
            reallocate(newCapacity, oldNormalizedZeroIndex, oldCountsArrayLength);
            writeSizeFieldsToHeader();
        } else {
            allocate(newCapacity);
        }
    }

    private void reallocate(long newCapacity, int oldNormalizedZeroIndex, int oldCountsArrayLength) {
        ptr = allocator.realloc(ptr, allocatedSize, newCapacity);
        allocatedSize = newCapacity;

        int countsDelta = countsArrayLength - oldCountsArrayLength;

        long bytesToZero = countsDelta * 8L;
        Vect.memset(ptr + headerSize + (oldCountsArrayLength * 8L), bytesToZero, 0);

        if (oldNormalizedZeroIndex != 0) {
            shiftCounts(oldNormalizedZeroIndex, oldCountsArrayLength, countsDelta);
        }
    }

    private void allocate(long newCapacity) {
        ptr = allocator.malloc(newCapacity);
        allocatedSize = newCapacity;

        Unsafe.getUnsafe().putLong(ptr, 0);
        Unsafe.getUnsafe().putInt(ptr + normalizingIndexOffsetPosition, 0);
        Unsafe.getUnsafe().putLong(ptr + maxValuePosition, 0);
        Unsafe.getUnsafe().putLong(ptr + minNonZeroValuePosition, Long.MAX_VALUE);
        Unsafe.getUnsafe().putInt(ptr + countsArrayLengthPosition, countsArrayLength);
        Unsafe.getUnsafe().putInt(ptr + bucketCountPosition, bucketCount);
        Unsafe.getUnsafe().putLong(ptr + highestTrackableValuePosition, highestTrackableValue);
        Unsafe.getUnsafe().putLong(ptr + startTimeStampMsecPosition, Long.MAX_VALUE);
        Unsafe.getUnsafe().putLong(ptr + endTimeStampMsecPosition, 0);

        Vect.memset(ptr + headerSize, countsArrayLength * 8L, 0);
    }

    private void shiftCounts(int oldNormalizedZeroIndex, int oldCountsArrayLength, int countsDelta) {
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
        int normalizingIndexOffset = Unsafe.getUnsafe().getInt(ptr + normalizingIndexOffsetPosition);
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
        if (ptr != 0) {
            Unsafe.getUnsafe().putInt(ptr + normalizingIndexOffsetPosition, offset);
        }
    }

    @Override
    void setTotalCount(long totalCount) {
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
        }

        long currentMin = Unsafe.getUnsafe().getLong(ptr + minNonZeroValuePosition);
        if ((value < currentMin) && (value != 0)) {
            if (value <= unitMagnitudeMask) {
                return;
            }
            long newMin = value & ~unitMagnitudeMask;
            Unsafe.getUnsafe().putLong(ptr + minNonZeroValuePosition, newMin);
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
    }

    private void ensureCapacity() {
        if (ptr == 0) {
            long newCapacity = headerSize + (countsArrayLength * 8L);
            allocate(newCapacity);
        }
    }

    private void writeSizeFieldsToHeader() {
        if (ptr != 0) {
            Unsafe.getUnsafe().putInt(ptr + countsArrayLengthPosition, countsArrayLength);
            Unsafe.getUnsafe().putInt(ptr + bucketCountPosition, bucketCount);
            Unsafe.getUnsafe().putLong(ptr + highestTrackableValuePosition, highestTrackableValue);
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
