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

public class GroupByHistogramSink extends AbstractHistogram implements Mutable {

    private GroupByAllocator allocator;
    private long address;
    private long capacity;
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

    @Override
    public void clear() {
        address = 0;
        capacity = 0;
        totalCount = 0;
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
        if (address == 0) {
            return 0;
        }
        return Unsafe.getUnsafe().getLong(address + ((long) normalizeIndex(index, normalizingIndexOffset, countsArrayLength) << 3));
    }

    @Override
    public long getTotalCount() {
        return totalCount;
    }

    @Override
    public void setIntegerToDoubleValueConversionRatio(double ratio) {
        nonConcurrentSetIntegerToDoubleValueConversionRatio(ratio);
    }

    @Override
    int _getEstimatedFootprintInBytes() {
        return 512 + (int) capacity;
    }

    @Override
    void addToCountAtIndex(int index, long value) {
        checkBounds(index);
        ensureCapacity();
        long addr = address + ((long) normalizeIndex(index, normalizingIndexOffset, countsArrayLength) << 3);
        Unsafe.getUnsafe().putLong(addr, Unsafe.getUnsafe().getLong(addr) + value);
    }

    @Override
    void addToTotalCount(long value) {
        totalCount += value;
    }

    @Override
    void clearCounts() {
        if (address != 0) {
            Vect.memset(address, countsArrayLength * 8L, 0);
        }
        totalCount = 0;
    }

    @Override
    long getCountAtNormalizedIndex(int index) {
        if (address == 0) {
            return 0;
        }
        return Unsafe.getUnsafe().getLong(address + ((long) index << 3));
    }

    @Override
    int getNormalizingIndexOffset() {
        return normalizingIndexOffset;
    }

    @Override
    void incrementCountAtIndex(int index) {
        checkBounds(index);
        ensureCapacity();
        long addr = address + ((long) normalizeIndex(index, normalizingIndexOffset, countsArrayLength) << 3);
        Unsafe.getUnsafe().putLong(addr, Unsafe.getUnsafe().getLong(addr) + 1);
    }

    @Override
    void incrementTotalCount() {
        totalCount++;
    }

    @Override
    void resize(long newHighestTrackableValue) {
        int oldNormalizedZeroIndex = normalizeIndex(0, normalizingIndexOffset, countsArrayLength);
        int oldCountsArrayLength = countsArrayLength;
        boolean hadPreviousAllocation = (address != 0);

        establishSize(newHighestTrackableValue);

        long newCapacity = countsArrayLength * 8L;

        if (hadPreviousAllocation) {
            address = allocator.realloc(address, capacity, newCapacity);
            capacity = newCapacity;

            int countsDelta = countsArrayLength - oldCountsArrayLength;

            long bytesToZero = countsDelta * 8L;
            Vect.memset(address + (oldCountsArrayLength * 8L), bytesToZero, 0);

            if (oldNormalizedZeroIndex != 0) {
                int newNormalizedZeroIndex = oldNormalizedZeroIndex + countsDelta;
                int lengthToCopy = oldCountsArrayLength - oldNormalizedZeroIndex;

                long srcAddr = address + (oldNormalizedZeroIndex * 8L);
                long dstAddr = address + (newNormalizedZeroIndex * 8L);
                long bytesToCopy = lengthToCopy * 8L;
                Vect.memcpy(dstAddr, srcAddr, bytesToCopy);

                long gapStart = address + (oldNormalizedZeroIndex * 8L);
                long gapSize = countsDelta * 8L;
                Vect.memset(gapStart, gapSize, 0);
            }
        } else {
            address = allocator.malloc(newCapacity);
            capacity = newCapacity;

            Vect.memset(address, newCapacity, 0);
        }
    }

    @Override
    void setCountAtIndex(int index, long value) {
        checkBounds(index);
        ensureCapacity();
        Unsafe.getUnsafe().putLong(address + ((long) normalizeIndex(index, normalizingIndexOffset, countsArrayLength) << 3), value);
    }

    @Override
    void setCountAtNormalizedIndex(int index, long value) {
        checkBounds(index);
        ensureCapacity();
        Unsafe.getUnsafe().putLong(address + ((long) index << 3), value);
    }

    @Override
    void setNormalizingIndexOffset(int offset) {
        this.normalizingIndexOffset = offset;
    }

    @Override
    void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    @Override
    void shiftNormalizingIndexByOffset(int offsetToAdd, boolean lowestHalfBucketPopulated, double newRatio) {
        nonConcurrentNormalizingIndexShift(offsetToAdd, lowestHalfBucketPopulated);
    }

    private void ensureCapacity() {
        if (address == 0) {
            capacity = countsArrayLength * 8L;
            address = allocator.malloc(capacity);
            clearCounts();
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
