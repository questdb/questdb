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

    public GroupByHistogram(int numberOfSignificantValueDigits) {
        this(1, 2, numberOfSignificantValueDigits);
        this.autoResize = true;
    }

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
            if (other.getMaxValue() > this.highestTrackableValue) {
                resize(other.getMaxValue());
            } else {
                ensureCapacity();
            }
        } else if (other.getMaxValue() > this.highestTrackableValue) {
            resize(other.getMaxValue());
        }

        this.add(other);
    }

    @Override
    public void clear() {
        ptr = 0;
        allocatedSize = 0;
    }

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

    public long getMaxValue() {
        if (ptr == 0) {
            return 0;
        }
        long maxVal = Unsafe.getUnsafe().getLong(ptr + maxValuePosition);
        return (maxVal == 0) ? 0 : highestEquivalentValue(maxVal);
    }

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

    void addToCountAtIndex(int index, long value) {
        checkBounds(index);
        ensureCapacity();
        int normalizingIndexOffset = Unsafe.getUnsafe().getInt(ptr + normalizingIndexOffsetPosition);
        long addr = ptr + headerSize + ((long) normalizeIndex(index, normalizingIndexOffset, countsArrayLength) << 3);
        Unsafe.getUnsafe().putLong(addr, Unsafe.getUnsafe().getLong(addr) + value);
    }

    void addToTotalCount(long value) {
        if (ptr != 0) {
            long totalCount = Unsafe.getUnsafe().getLong(ptr);
            Unsafe.getUnsafe().putLong(ptr, totalCount + value);
        }
    }

    int getNormalizingIndexOffset() {
        return (ptr != 0) ? Unsafe.getUnsafe().getInt(ptr + normalizingIndexOffsetPosition) : 0;
    }

    void incrementCountAtIndex(int index) {
        checkBounds(index);
        ensureCapacity();
        int normalizingIndexOffset = Unsafe.getUnsafe().getInt(ptr + normalizingIndexOffsetPosition);
        long addr = ptr + headerSize + ((long) normalizeIndex(index, normalizingIndexOffset, countsArrayLength) << 3);
        Unsafe.getUnsafe().putLong(addr, Unsafe.getUnsafe().getLong(addr) + 1);
    }

    void incrementTotalCount() {
        if (ptr != 0) {
            long totalCount = Unsafe.getUnsafe().getLong(ptr);
            Unsafe.getUnsafe().putLong(ptr, totalCount + 1);
        }
    }

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

    void setTotalCount(long totalCount) {
        if (ptr != 0) {
            Unsafe.getUnsafe().putLong(ptr, totalCount);
        }
    }

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

    private void updateMaxValue(long value) {
        if (ptr == 0) {
            return;
        }
        long currentMax = Unsafe.getUnsafe().getLong(ptr + maxValuePosition);
        long newMax = Math.max(currentMax, value | unitMagnitudeMask);
        Unsafe.getUnsafe().putLong(ptr + maxValuePosition, newMax);
    }

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

    private void init(final long lowestDiscernibleValue,
                      final long highestTrackableValue,
                      final int numberOfSignificantValueDigits) {
        this.lowestDiscernibleValue = lowestDiscernibleValue;
        this.highestTrackableValue = highestTrackableValue;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;

        recalculateDerivedFields();
        establishSize(highestTrackableValue);
    }

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

    private void establishSize(long newHighestTrackableValue) {
        countsArrayLength = determineArrayLengthNeeded(newHighestTrackableValue);
        bucketCount = getBucketsNeededToCoverValue(newHighestTrackableValue);
        highestTrackableValue = newHighestTrackableValue;
    }

    private int determineArrayLengthNeeded(long highestTrackableValue) {
        if (highestTrackableValue < 2L * lowestDiscernibleValue) {
            throw new IllegalArgumentException("highestTrackableValue (" + highestTrackableValue +
                    ") cannot be < (2 * lowestDiscernibleValue)");
        }
        return getLengthForNumberOfBuckets(getBucketsNeededToCoverValue(highestTrackableValue));
    }

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

    private int getLengthForNumberOfBuckets(final int numberOfBuckets) {
        return (numberOfBuckets + 1) * (subBucketHalfCount);
    }

    public long highestEquivalentValue(final long value) {
        return nextNonEquivalentValue(value) - 1;
    }

    public long nextNonEquivalentValue(final long value) {
        return lowestEquivalentValue(value) + sizeOfEquivalentValueRange(value);
    }

    public long lowestEquivalentValue(final long value) {
        int bucketIndex = getBucketIndex(value);
        int subBucketIndex = getSubBucketIndex(value, bucketIndex);
        return valueFromIndex(bucketIndex, subBucketIndex);
    }

    private long sizeOfEquivalentValueRange(final long value) {
        int bucketIndex = getBucketIndex(value);
        return 1L << (unitMagnitude + bucketIndex);
    }

    private int getBucketIndex(final long value) {
        return leadingZeroCountBase - Long.numberOfLeadingZeros(value | subBucketMask);
    }

    private int getSubBucketIndex(final long value, final int bucketIndex) {
        return (int) (value >>> (bucketIndex + unitMagnitude));
    }

    private long valueFromIndex(final int bucketIndex, final int subBucketIndex) {
        return ((long) subBucketIndex) << (bucketIndex + unitMagnitude);
    }

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

    private long valueFromIndex(final int index) {
        int bucketIndex = (index >> subBucketHalfCountMagnitude) - 1;
        int subBucketIndex = (index & (subBucketHalfCount - 1)) + subBucketHalfCount;
        if (bucketIndex < 0) {
            subBucketIndex -= subBucketHalfCount;
            bucketIndex = 0;
        }
        return valueFromIndex(bucketIndex, subBucketIndex);
    }

    private int countsArrayIndex(final long value) {
        if (value < 0) {
            throw CairoException.nonCritical().put("Histogram recorded value cannot be negative.");
        }
        int bucketIndex = getBucketIndex(value);
        int subBucketIndex = getSubBucketIndex(value, bucketIndex);
        return countsArrayIndex(bucketIndex, subBucketIndex);
    }

    private int countsArrayIndex(final int bucketIndex, final int subBucketIndex) {
        assert (subBucketIndex < subBucketCount);
        assert (bucketIndex == 0 || (subBucketIndex >= subBucketHalfCount));
        int bucketBaseIndex = (bucketIndex + 1) << subBucketHalfCountMagnitude;
        int offsetInBucket = subBucketIndex - subBucketHalfCount;
        return bucketBaseIndex + offsetInBucket;
    }

    public void recordValue(final long value) throws CairoException {
        recordSingleValue(value);
    }

    public void recordValueWithCount(final long value, final long count) throws CairoException {
        recordCountAtValue(count, value);
    }

    private void recordSingleValue(final long value) throws CairoException {
        int countsIndex = countsArrayIndex(value);
        try {
            incrementCountAtIndex(countsIndex);
        } catch (CairoException ex) {
            handleRecordException(1, value, ex);
        }
        updateMinAndMax(value);
        incrementTotalCount();
    }

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

    private void handleRecordException(final long count, final long value, Exception ex) {
        if (!autoResize) {
            throw CairoException.nonCritical().put("value ").put(value)
                    .put(" outside of histogram covered range. Caused by: ").put(ex.getMessage());
        }
        resize(value);
        int countsIndex = countsArrayIndex(value);
        addToCountAtIndex(countsIndex, count);
        this.highestTrackableValue = highestEquivalentValue(valueFromIndex(countsArrayLength - 1));
        Unsafe.getUnsafe().putLong(ptr + highestTrackableValuePosition, highestTrackableValue);
    }

    public void add(final GroupByHistogram otherHistogram) throws CairoException {
        long highestRecordableValue = highestEquivalentValue(valueFromIndex(countsArrayLength - 1));
        if (highestRecordableValue < otherHistogram.getMaxValue()) {
            if (!autoResize) {
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

    public long getMinValue() {
        if ((getCountAtIndex(0) > 0) || (getTotalCount() == 0)) {
            return 0;
        }
        return getMinNonZeroValue();
    }

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

    private long medianEquivalentValue(final long value) {
        return (lowestEquivalentValue(value) + (sizeOfEquivalentValueRange(value) >> 1));
    }

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

    public long getCountAtValue(final long value) throws CairoException {
        final int index = Math.min(Math.max(0, countsArrayIndex(value)), (countsArrayLength - 1));
        return getCountAtIndex(index);
    }

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
