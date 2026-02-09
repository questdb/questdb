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
 * | totalCount | normalizingIndexOffset | maxValue | minNonZeroValue | countsArrayLength | bucketCount | highestTrackableValue | startTimeStampMsec | endTimeStampMsec | lowestDiscernibleValue | numberOfSignificantValueDigits | histogram counts array |
 * +------------+------------------------+----------+-----------------+-------------------+-------------+-----------------------+--------------------+------------------+------------------------+--------------------------------+------------------------+
 * |  8 bytes   |        4 bytes         | 8 bytes  |    8 bytes      |     4 bytes       |   4 bytes   |       8 bytes         |      8 bytes       |     8 bytes      |        8 bytes         |            4 bytes             | countsArrayLength * 8  |
 * +------------+------------------------+----------+-----------------+-------------------+-------------+-----------------------+--------------------+------------------+------------------------+--------------------------------+------------------------+
 * </pre>
 */
public class GroupByHistogram implements Mutable {

    private static final long normalizingIndexOffsetPosition = Long.BYTES;
    private static final long maxValuePosition = normalizingIndexOffsetPosition + Integer.BYTES;
    private static final long minNonZeroValuePosition = maxValuePosition + Long.BYTES;
    private static final long countsArrayLengthPosition = minNonZeroValuePosition + Long.BYTES;
    private static final long bucketCountPosition = countsArrayLengthPosition + Integer.BYTES;
    private static final long highestTrackableValuePosition = bucketCountPosition + Integer.BYTES;
    private static final long startTimeStampMsecPosition = highestTrackableValuePosition + Long.BYTES;
    private static final long endTimeStampMsecPosition = startTimeStampMsecPosition + Long.BYTES;
    private static final long lowestDiscernibleValuePosition = endTimeStampMsecPosition + Long.BYTES;
    private static final long numberOfSignificantValueDigitsPosition = lowestDiscernibleValuePosition + Long.BYTES;
    private static final long headerSize = numberOfSignificantValueDigitsPosition + Integer.BYTES;

    private GroupByAllocator allocator;
    private long ptr;
    private long allocatedSize;

    private boolean autoResize = false;
    private int bucketCount;
    private int countsArrayLength;
    private long highestTrackableValue;
    private int subBucketCount;
    private int leadingZeroCountBase;
    private int subBucketHalfCount;
    private int subBucketHalfCountMagnitude;
    private long subBucketMask;
    private int unitMagnitude;
    private long unitMagnitudeMask;
    private long lowestDiscernibleValue;
    private int numberOfSignificantValueDigits;

    // AbstractHistogram(int) at lines 140-143
    public GroupByHistogram(int numberOfSignificantValueDigits) {
        this(1, 2, numberOfSignificantValueDigits);
        this.autoResize = true;
    }

    // AbstractHistogram(long, long, int) at lines 161-180
    public GroupByHistogram(long lowestDiscernibleValue, long highestTrackableValue, int numberOfSignificantValueDigits) {
        if (lowestDiscernibleValue < 1) {
            throw new IllegalArgumentException("lowestDiscernibleValue must be >= 1");
        }
        if (lowestDiscernibleValue > Long.MAX_VALUE / 2) {
            throw new IllegalArgumentException("lowestDiscernibleValue must be <= Long.MAX_VALUE / 2");
        }
        if (highestTrackableValue < 2L * lowestDiscernibleValue) {
            throw new IllegalArgumentException("highestTrackableValue must be >= 2 * lowestDiscernibleValue");
        }
        if ((numberOfSignificantValueDigits < 0) || (numberOfSignificantValueDigits > 5)) {
            throw new IllegalArgumentException("numberOfSignificantValueDigits must be between 0 and 5");
        }

        init(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
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
            this.lowestDiscernibleValue = Unsafe.getUnsafe().getLong(ptr + lowestDiscernibleValuePosition);
            this.numberOfSignificantValueDigits = Unsafe.getUnsafe().getInt(ptr + numberOfSignificantValueDigitsPosition);
            this.countsArrayLength = Unsafe.getUnsafe().getInt(ptr + countsArrayLengthPosition);
            this.bucketCount = Unsafe.getUnsafe().getInt(ptr + bucketCountPosition);
            this.highestTrackableValue = Unsafe.getUnsafe().getLong(ptr + highestTrackableValuePosition);
            this.allocatedSize = headerSize + (countsArrayLength * 8L);

            recalculateDerivedFields();
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

    public void clear() {
        ptr = 0;
        allocatedSize = 0;
    }

    public GroupByHistogram copy() {
        throw new UnsupportedOperationException();
    }

    public GroupByHistogram copyCorrectedForCoordinatedOmission(long expectedIntervalBetweenValueSamples) {
        throw new UnsupportedOperationException();
    }

    // See Histogram.getCountAtIndex(int) at lines 209-211
    public long getCountAtIndex(int index) {
        if (ptr == 0) {
            return 0;
        }
        int normalizingIndexOffset = Unsafe.getUnsafe().getInt(ptr + normalizingIndexOffsetPosition);
        return Unsafe.getUnsafe().getLong(ptr + headerSize + ((long) normalizeIndex(index, normalizingIndexOffset, countsArrayLength) << 3));
    }

    public long getTotalCount() {
        return (ptr != 0) ? Unsafe.getUnsafe().getLong(ptr) : 0;
    }

    // See AbstractHistogram.getMaxValue() at lines 637-639
    public long getMaxValue() {
        if (ptr == 0) {
            return 0;
        }
        long maxVal = Unsafe.getUnsafe().getLong(ptr + maxValuePosition);
        return (maxVal == 0) ? 0 : highestEquivalentValue(maxVal);
    }

    // See AbstractHistogram.getMinNonZeroValue() at lines 676-679
    public long getMinNonZeroValue() {
        if (ptr == 0) {
            return Long.MAX_VALUE;
        }
        long minVal = Unsafe.getUnsafe().getLong(ptr + minNonZeroValuePosition);
        return (minVal == Long.MAX_VALUE) ? Long.MAX_VALUE : lowestEquivalentValue(minVal);
    }

    public long getStartTimeStamp() {
        return (ptr != 0) ? Unsafe.getUnsafe().getLong(ptr + startTimeStampMsecPosition) : Long.MAX_VALUE;
    }

    public void setStartTimeStamp(long timeStampMsec) {
        if (ptr != 0) {
            Unsafe.getUnsafe().putLong(ptr + startTimeStampMsecPosition, timeStampMsec);
        }
    }

    public long getEndTimeStamp() {
        return (ptr != 0) ? Unsafe.getUnsafe().getLong(ptr + endTimeStampMsecPosition) : 0;
    }

    public void setEndTimeStamp(long timeStampMsec) {
        if (ptr != 0) {
            Unsafe.getUnsafe().putLong(ptr + endTimeStampMsecPosition, timeStampMsec);
        }
    }

    // Histogram.addToCountAtIndex(int, long) at lines 234-236
    void addToCountAtIndex(int index, long value) {
        checkBounds(index);
        ensureCapacity();
        int normalizingIndexOffset = Unsafe.getUnsafe().getInt(ptr + normalizingIndexOffsetPosition);
        long addr = ptr + headerSize + ((long) normalizeIndex(index, normalizingIndexOffset, countsArrayLength) << 3);
        Unsafe.getUnsafe().putLong(addr, Unsafe.getUnsafe().getLong(addr) + value);
    }

    // Histogram.addToTotalCount(long) at lines 239-241
    void addToTotalCount(long value) {
        if (ptr != 0) {
            long totalCount = Unsafe.getUnsafe().getLong(ptr);
            Unsafe.getUnsafe().putLong(ptr, totalCount + value);
        }
    }

    // Histogram.getCountAtNormalizedIndex(int) at lines 250-252
    long getCountAtNormalizedIndex(int index) {
        if (ptr == 0) {
            return 0;
        }
        return Unsafe.getUnsafe().getLong(ptr + headerSize + ((long) index << 3));
    }

    // Histogram.getNormalizingIndexOffset() at lines 255-257
    int getNormalizingIndexOffset() {
        return (ptr != 0) ? Unsafe.getUnsafe().getInt(ptr + normalizingIndexOffsetPosition) : 0;
    }

    // Histogram.incrementCountAtIndex(int) at lines 260-262
    void incrementCountAtIndex(int index) {
        checkBounds(index);
        ensureCapacity();
        int normalizingIndexOffset = Unsafe.getUnsafe().getInt(ptr + normalizingIndexOffsetPosition);
        long addr = ptr + headerSize + ((long) normalizeIndex(index, normalizingIndexOffset, countsArrayLength) << 3);
        Unsafe.getUnsafe().putLong(addr, Unsafe.getUnsafe().getLong(addr) + 1);
    }

    // Histogram.incrementTotalCount() at lines 265-267
    void incrementTotalCount() {
        if (ptr != 0) {
            long totalCount = Unsafe.getUnsafe().getLong(ptr);
            Unsafe.getUnsafe().putLong(ptr, totalCount + 1);
        }
    }

    // Histogram.resize(long) at lines 270-287
    void resize(long newHighestTrackableValue) {
        int oldNormalizingIndexOffset = (ptr != 0) ? Unsafe.getUnsafe().getInt(ptr + normalizingIndexOffsetPosition) : 0;
        int oldNormalizedZeroIndex = normalizeIndex(0, oldNormalizingIndexOffset, countsArrayLength);
        int oldCountsArrayLength = countsArrayLength;
        boolean hadPreviousAllocation = (ptr != 0);

        establishSize(newHighestTrackableValue);

        long newCapacity = headerSize + (countsArrayLength * 8L);

        if (hadPreviousAllocation) {
            reallocate(newCapacity, oldNormalizedZeroIndex, oldCountsArrayLength);
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

        // Update size fields in header for of() repointing to work correctly
        Unsafe.getUnsafe().putInt(ptr + countsArrayLengthPosition, countsArrayLength);
        Unsafe.getUnsafe().putInt(ptr + bucketCountPosition, bucketCount);
        Unsafe.getUnsafe().putLong(ptr + highestTrackableValuePosition, highestTrackableValue);
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
        Unsafe.getUnsafe().putLong(ptr + lowestDiscernibleValuePosition, lowestDiscernibleValue);
        Unsafe.getUnsafe().putInt(ptr + numberOfSignificantValueDigitsPosition, numberOfSignificantValueDigits);

        Vect.memset(ptr + headerSize, countsArrayLength * 8L, 0);
    }

    // Histogram.resize(long) at lines 279-284
    private void shiftCounts(int oldNormalizedZeroIndex, int oldCountsArrayLength, int countsDelta) {
        int newNormalizedZeroIndex = oldNormalizedZeroIndex + countsDelta;
        int lengthToCopy = oldCountsArrayLength - oldNormalizedZeroIndex;

        long srcAddr = ptr + headerSize + (oldNormalizedZeroIndex * 8L);
        long dstAddr = ptr + headerSize + (newNormalizedZeroIndex * 8L);
        long bytesToCopy = lengthToCopy * 8L;
        Vect.memmove(dstAddr, srcAddr, bytesToCopy);

        long gapStart = ptr + headerSize + (oldNormalizedZeroIndex * 8L);
        long gapSize = countsDelta * 8L;
        Vect.memset(gapStart, gapSize, 0);
    }

    // Histogram.setCountAtIndex(int, long) at lines 289-291
    void setCountAtIndex(int index, long value) {
        checkBounds(index);
        ensureCapacity();
        int normalizingIndexOffset = Unsafe.getUnsafe().getInt(ptr + normalizingIndexOffsetPosition);
        Unsafe.getUnsafe().putLong(ptr + headerSize + ((long) normalizeIndex(index, normalizingIndexOffset, countsArrayLength) << 3), value);
    }

    // Histogram.setCountAtNormalizedIndex(int, long) at lines 294-296
    void setCountAtNormalizedIndex(int index, long value) {
        checkBounds(index);
        ensureCapacity();
        Unsafe.getUnsafe().putLong(ptr + headerSize + ((long) index << 3), value);
    }

    // Histogram.setNormalizingIndexOffset(int) at lines 299-301
    void setNormalizingIndexOffset(int offset) {
        if (ptr != 0) {
            Unsafe.getUnsafe().putInt(ptr + normalizingIndexOffsetPosition, offset);
        }
    }

    // Histogram.setTotalCount(long) at lines 304-306
    void setTotalCount(long totalCount) {
        if (ptr != 0) {
            Unsafe.getUnsafe().putLong(ptr, totalCount);
        }
    }

    // See AbstractHistogram.updateMinAndMax(long) at lines 2301-2312
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

    // See AbstractHistogram.updateMaxValue(long) at lines 2297-2299
    private void updateMaxValue(long value) {
        if (ptr == 0) {
            return;
        }
        long currentMax = Unsafe.getUnsafe().getLong(ptr + maxValuePosition);
        long newMax = Math.max(currentMax, value | unitMagnitudeMask);
        Unsafe.getUnsafe().putLong(ptr + maxValuePosition, newMax);
    }

    // See AbstractHistogram.updateMinNonZeroValue(long) at lines 2320-2325
    private void updateMinNonZeroValue(long value) {
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

    private void checkBounds(int index) {
        if (index < 0 || index >= countsArrayLength) {
            throw CairoException.nonCritical()
                .put("index ").put(index)
                .put(" out of bounds [0, ").put(countsArrayLength).put(')');
        }
    }

    // AbstractHistogram.init() at lines 1620-1671
    private void init(final long lowestDiscernibleValue,
                      final long highestTrackableValue,
                      final int numberOfSignificantValueDigits) {
        this.lowestDiscernibleValue = lowestDiscernibleValue;
        this.highestTrackableValue = highestTrackableValue;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;

        recalculateDerivedFields();
        establishSize(highestTrackableValue);
    }

    // AbstractHistogram.init() at lines 1638-1667 for the derived field calculations
    private void recalculateDerivedFields() {
        final long largestValueWithSingleUnitResolution = 2 * (long) Math.pow(10, numberOfSignificantValueDigits);

        unitMagnitude = (int) (Math.log(lowestDiscernibleValue) / Math.log(2));
        unitMagnitudeMask = (1L << unitMagnitude) - 1;

        int subBucketCountMagnitude = (int) Math.ceil(Math.log(largestValueWithSingleUnitResolution) / Math.log(2));
        subBucketHalfCountMagnitude = subBucketCountMagnitude - 1;
        subBucketCount = 1 << subBucketCountMagnitude;
        subBucketHalfCount = subBucketCount / 2;
        subBucketMask = ((long) subBucketCount - 1) << unitMagnitude;

        if (subBucketCountMagnitude + unitMagnitude > 62) {
            throw new IllegalArgumentException("Cannot represent numberOfSignificantValueDigits worth of values beyond lowestDiscernibleValue");
        }

        leadingZeroCountBase = 64 - unitMagnitude - subBucketCountMagnitude;
    }

    // AbstractHistogram.establishSize(long) at lines 2014-2021
    private void establishSize(long newHighestTrackableValue) {
        countsArrayLength = determineArrayLengthNeeded(newHighestTrackableValue);
        bucketCount = getBucketsNeededToCoverValue(newHighestTrackableValue);
        highestTrackableValue = newHighestTrackableValue;
    }

    // AbstractHistogram.determineArrayLengthNeeded(long) at lines 1960-1967
    private int determineArrayLengthNeeded(long highestTrackableValue) {
        if (highestTrackableValue < 2L * lowestDiscernibleValue) {
            throw new IllegalArgumentException("highestTrackableValue (" + highestTrackableValue +
                    ") cannot be < (2 * lowestDiscernibleValue)");
        }
        return getLengthForNumberOfBuckets(getBucketsNeededToCoverValue(highestTrackableValue));
    }

    // AbstractHistogram.getBucketsNeededToCoverValue(long) at lines 2054-2071
    private int getBucketsNeededToCoverValue(final long value) {
        long smallestUntrackableValue = ((long) subBucketCount) << unitMagnitude;
        int bucketsNeeded = 1;
        while (smallestUntrackableValue <= value) {
            if (smallestUntrackableValue > (Long.MAX_VALUE / 2)) {
                return bucketsNeeded + 1;
            }
            smallestUntrackableValue <<= 1;
            bucketsNeeded++;
        }
        return bucketsNeeded;
    }

    // AbstractHistogram.getLengthForNumberOfBuckets(int) at lines 2081-2083
    private int getLengthForNumberOfBuckets(final int numberOfBuckets) {
        return (numberOfBuckets + 1) * (subBucketHalfCount);
    }

    // AbstractHistogram.highestEquivalentValue(long) at lines 896-898
    public long highestEquivalentValue(final long value) {
        return nextNonEquivalentValue(value) - 1;
    }

    // AbstractHistogram.nextNonEquivalentValue(long) at lines 988-990
    public long nextNonEquivalentValue(final long value) {
        return lowestEquivalentValue(value) + sizeOfEquivalentValueRange(value);
    }

    // AbstractHistogram.lowestEquivalentValue(long) at lines 951-955
    public long lowestEquivalentValue(final long value) {
        int bucketIndex = getBucketIndex(value);
        int subBucketIndex = getSubBucketIndex(value, bucketIndex);
        return valueFromIndex(bucketIndex, subBucketIndex);
    }

    // AbstractHistogram.sizeOfEquivalentValueRange(long) at lines 1332-1335
    private long sizeOfEquivalentValueRange(final long value) {
        int bucketIndex = getBucketIndex(value);
        return 1L << (unitMagnitude + bucketIndex);
    }

    // AbstractHistogram.getBucketIndex(long) at lines 533-538
    private int getBucketIndex(final long value) {
        return leadingZeroCountBase - Long.numberOfLeadingZeros(value | subBucketMask);
    }

    // AbstractHistogram.getSubBucketIndex(long, int) at lines 789-797
    private int getSubBucketIndex(final long value, final int bucketIndex) {
        return (int) (value >>> (bucketIndex + unitMagnitude));
    }

    // AbstractHistogram.valueFromIndex(int, int) at lines 1870-1872
    private long valueFromIndex(final int bucketIndex, final int subBucketIndex) {
        return ((long) subBucketIndex) << (bucketIndex + unitMagnitude);
    }

    // AbstractHistogram.normalizeIndex(int, int, int) at lines 2131-2151
    private int normalizeIndex(int index, int normalizingIndexOffset, int arrayLength) {
        if (normalizingIndexOffset == 0) {
            return index;
        }
        if ((index > arrayLength) || (index < 0)) {
            throw CairoException.nonCritical().put("index out of covered value range");
        }
        int normalizedIndex = index - normalizingIndexOffset;
        if (normalizedIndex < 0) {
            normalizedIndex += arrayLength;
        } else if (normalizedIndex >= arrayLength) {
            normalizedIndex -= arrayLength;
        }
        return normalizedIndex;
    }

    // AbstractHistogram.valueFromIndex(int) at lines 1393-1401
    private long valueFromIndex(final int index) {
        int bucketIndex = (index >> subBucketHalfCountMagnitude) - 1;
        int subBucketIndex = (index & (subBucketHalfCount - 1)) + subBucketHalfCount;
        if (bucketIndex < 0) {
            subBucketIndex -= subBucketHalfCount;
            bucketIndex = 0;
        }
        return valueFromIndex(bucketIndex, subBucketIndex);
    }

    // AbstractHistogram.countsArrayIndex(long) at lines 354-361
    private int countsArrayIndex(final long value) {
        if (value < 0) {
            throw CairoException.nonCritical().put("Histogram recorded value cannot be negative.");
        }
        int bucketIndex = getBucketIndex(value);
        int subBucketIndex = getSubBucketIndex(value, bucketIndex);
        return countsArrayIndex(bucketIndex, subBucketIndex);
    }

    // AbstractHistogram.countsArrayIndex(int, int) at lines 1534-1546
    private int countsArrayIndex(final int bucketIndex, final int subBucketIndex) {
        assert(subBucketIndex < subBucketCount);
        assert(bucketIndex == 0 || (subBucketIndex >= subBucketHalfCount));
        int bucketBaseIndex = (bucketIndex + 1) << subBucketHalfCountMagnitude;
        int offsetInBucket = subBucketIndex - subBucketHalfCount;
        return bucketBaseIndex + offsetInBucket;
    }

    private boolean isAutoResize() {
        return autoResize;
    }

    public void setAutoResize(boolean autoResize) {
        this.autoResize = autoResize;
    }

    // AbstractHistogram.recordValue(long) at lines 1152-1154
    public void recordValue(final long value) throws CairoException {
        recordSingleValue(value);
    }

    // AbstractHistogram.recordValueWithCount(long, long) at lines 1164-1166
    public void recordValueWithCount(final long value, final long count) throws CairoException {
        recordCountAtValue(count, value);
    }

    // AbstractHistogram.recordSingleValue(long) at lines 1726-1735
    private void recordSingleValue(final long value) throws CairoException {
        int countsIndex = countsArrayIndex(value);
        try {
            incrementCountAtIndex(countsIndex);
        } catch (CairoException | ArrayIndexOutOfBoundsException ex) {
            handleRecordException(1, value, ex);
        }
        updateMinAndMax(value);
        incrementTotalCount();
    }

    // AbstractHistogram.recordCountAtValue(long, long) at lines 1713-1722
    private void recordCountAtValue(final long count, final long value) throws CairoException {
        int countsIndex = countsArrayIndex(value);
        try {
            addToCountAtIndex(countsIndex, count);
        } catch (CairoException ex) {
            handleRecordException(count, value, ex);
        }
        updateMinAndMax(value);
        addToTotalCount(count);
    }

    // AbstractHistogram.handleRecordException(long, long, Exception) at lines 1607-1616
    private void handleRecordException(final long count, final long value, Exception ex) {
        if (!autoResize) {
            throw CairoException.nonCritical().put("value ").put(value)
                    .put(" outside of histogram covered range. Caused by: ").put(ex.getMessage());
        }
        resize(value);
        int countsIndex = countsArrayIndex(value);
        addToCountAtIndex(countsIndex, count);
        this.highestTrackableValue = highestEquivalentValue(valueFromIndex(countsArrayLength - 1));
    }

    // AbstractHistogram.add(AbstractHistogram) at lines 206-251
    public void add(final GroupByHistogram otherHistogram) throws CairoException {
        long highestRecordableValue = highestEquivalentValue(valueFromIndex(countsArrayLength - 1));
        if (highestRecordableValue < otherHistogram.getMaxValue()) {
            if (!isAutoResize()) {
                throw CairoException.nonCritical().put(
                        "The other histogram includes values that do not fit in this histogram's range.");
            }
            resize(otherHistogram.getMaxValue());
        }
        if ((bucketCount == otherHistogram.bucketCount) &&
                (subBucketCount == otherHistogram.subBucketCount) &&
                (unitMagnitude == otherHistogram.unitMagnitude) &&
                (getNormalizingIndexOffset() == otherHistogram.getNormalizingIndexOffset())) {
            long observedOtherTotalCount = 0;
            for (int i = 0; i < otherHistogram.countsArrayLength; i++) {
                long otherCount = otherHistogram.getCountAtIndex(i);
                if (otherCount > 0) {
                    addToCountAtIndex(i, otherCount);
                    observedOtherTotalCount += otherCount;
                }
            }
            setTotalCount(getTotalCount() + observedOtherTotalCount);
            updateMaxValue(Math.max(getMaxValue(), otherHistogram.getMaxValue()));
            updateMinNonZeroValue(Math.min(getMinNonZeroValue(), otherHistogram.getMinNonZeroValue()));
        } else {
            int otherMaxIndex = otherHistogram.countsArrayIndex(otherHistogram.getMaxValue());
            long otherCount = otherHistogram.getCountAtIndex(otherMaxIndex);
            recordValueWithCount(otherHistogram.valueFromIndex(otherMaxIndex), otherCount);

            for (int i = 0; i < otherMaxIndex; i++) {
                otherCount = otherHistogram.getCountAtIndex(i);
                if (otherCount > 0) {
                    recordValueWithCount(otherHistogram.valueFromIndex(i), otherCount);
                }
            }
        }
        setStartTimeStamp(Math.min(getStartTimeStamp(), otherHistogram.getStartTimeStamp()));
        setEndTimeStamp(Math.max(getEndTimeStamp(), otherHistogram.getEndTimeStamp()));
    }

    // AbstractHistogram.getMinValue() at lines 687-692
    public long getMinValue() {
        if ((getCountAtIndex(0) > 0) || (getTotalCount() == 0)) {
            return 0;
        }
        return getMinNonZeroValue();
    }

    // AbstractHistogram.getMean() at lines 656-668
    public double getMean() {
        if (getTotalCount() == 0) {
            return 0.0;
        }
        double totalValue = 0;
        for (int i = 0; i < countsArrayLength; i++) {
            long count = getCountAtIndex(i);
            if (count > 0) {
                totalValue += medianEquivalentValue(valueFromIndex(i)) * (double) count;
            }
        }
        return totalValue / getTotalCount();
    }

    // AbstractHistogram.getStdDeviation() at lines 774-787
    public double getStdDeviation() {
        if (getTotalCount() == 0) {
            return 0.0;
        }
        double mean = getMean();
        double variance = 0.0;
        long totalCount = getTotalCount();

        for (int i = 0; i < countsArrayLength; i++) {
            long count = getCountAtIndex(i);
            if (count > 0) {
                double deviation = (medianEquivalentValue(valueFromIndex(i)) * 1.0) - mean;
                variance += (deviation * deviation) * count;
            }
        }

        return Math.sqrt(variance / totalCount);
    }

    // AbstractHistogram.medianEquivalentValue(long) at lines 976-978
    private long medianEquivalentValue(final long value) {
        return (lowestEquivalentValue(value) + (sizeOfEquivalentValueRange(value) >> 1));
    }

    // AbstractHistogram.getValueAtPercentile(double) at lines 838-860
    public long getValueAtPercentile(final double percentile) {
        double requestedPercentile =
                Math.min(Math.max(Math.nextAfter(percentile, Double.NEGATIVE_INFINITY), 0.0D), 100.0D);
        double fpCountAtPercentile = (requestedPercentile * getTotalCount()) / 100.0D;
        long countAtPercentile = (long) (Math.ceil(fpCountAtPercentile)); // round up

        countAtPercentile = Math.max(countAtPercentile, 1); // Make sure we at least reach the first recorded entry
        long totalToCurrentIndex = 0;
        for (int i = 0; i < countsArrayLength; i++) {
            totalToCurrentIndex += getCountAtIndex(i);
            if (totalToCurrentIndex >= countAtPercentile) {
                long valueAtIndex = valueFromIndex(i);
                return (percentile == 0.0) ?
                        lowestEquivalentValue(valueAtIndex) :
                        highestEquivalentValue(valueAtIndex);
            }
        }
        return 0;
    }

    // AbstractHistogram.getCountAtValue(long) at lines 549-552
    public long getCountAtValue(final long value) throws CairoException {
        final int index = Math.min(Math.max(0, countsArrayIndex(value)), (countsArrayLength - 1));
        return getCountAtIndex(index);
    }

    // AbstractHistogram.getCountBetweenValues(long, long) at lines 584-592
    public long getCountBetweenValues(final long lowValue, final long highValue) throws CairoException {
        final int lowIndex = Math.max(0, countsArrayIndex(lowValue));
        final int highIndex = Math.min(countsArrayIndex(highValue), (countsArrayLength - 1));
        long count = 0;
        for (int i = lowIndex; i <= highIndex; i++) {
            count += getCountAtIndex(i);
        }
        return count;
    }
}
