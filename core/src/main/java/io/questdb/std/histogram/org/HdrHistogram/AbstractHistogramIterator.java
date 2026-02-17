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

// Written by Gil Tene of Azul Systems, and released to the public domain,
// as explained at http://creativecommons.org/publicdomain/zero/1.0/
//
// @author Gil Tene

package io.questdb.std.histogram.org.HdrHistogram;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Used for iterating through histogram values.
 */
abstract class AbstractHistogramIterator implements Iterator<HistogramIterationValue> {
    final HistogramIterationValue currentIterationValue = new HistogramIterationValue();
    long arrayTotalCount;
    long countAtThisValue;
    int currentIndex;
    long currentValueAtIndex;
    AbstractHistogram histogram;
    long nextValueAtIndex;
    long prevValueIteratedTo;
    long totalCountToCurrentIndex;
    long totalCountToPrevIndex;
    long totalValueToCurrentIndex;
    private boolean freshSubBucket;
    private double integerToDoubleValueConversionRatio;

    /**
     * Returns true if the iteration has more elements. (In other words, returns true if next would return an
     * element rather than throwing an exception.)
     *
     * @return true if the iterator has more elements.
     */
    @Override
    public boolean hasNext() {
        if (histogram.getTotalCount() != arrayTotalCount) {
            throw new ConcurrentModificationException();
        }
        return (totalCountToCurrentIndex < arrayTotalCount);
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the {@link HistogramIterationValue} associated with the next element in the iteration.
     */
    @Override
    public HistogramIterationValue next() {
        // Move through the sub buckets and buckets until we hit the next reporting level:
        while (!exhaustedSubBuckets()) {
            countAtThisValue = histogram.getCountAtIndex(currentIndex);
            if (freshSubBucket) { // Don't add unless we've incremented since last bucket...
                totalCountToCurrentIndex += countAtThisValue;
                totalValueToCurrentIndex += countAtThisValue * histogram.highestEquivalentValue(currentValueAtIndex);
                freshSubBucket = false;
            }
            if (reachedIterationLevel()) {
                long valueIteratedTo = getValueIteratedTo();
                currentIterationValue.set(valueIteratedTo, prevValueIteratedTo, countAtThisValue,
                        (totalCountToCurrentIndex - totalCountToPrevIndex), totalCountToCurrentIndex,
                        totalValueToCurrentIndex, ((100.0 * totalCountToCurrentIndex) / arrayTotalCount),
                        getPercentileIteratedTo(), integerToDoubleValueConversionRatio);
                prevValueIteratedTo = valueIteratedTo;
                totalCountToPrevIndex = totalCountToCurrentIndex;
                // move the next iteration level forward:
                incrementIterationLevel();
                if (histogram.getTotalCount() != arrayTotalCount) {
                    throw new ConcurrentModificationException();
                }
                return currentIterationValue;
            }
            incrementSubBucket();
        }
        // Should not reach here. But possible for concurrent modification or overflowed histograms
        // under certain conditions
        if ((histogram.getTotalCount() != arrayTotalCount) ||
                (totalCountToCurrentIndex > arrayTotalCount)) {
            throw new ConcurrentModificationException();
        }
        throw new NoSuchElementException();
    }

    /**
     * Not supported. Will throw an {@link UnsupportedOperationException}.
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private boolean exhaustedSubBuckets() {
        return (currentIndex >= histogram.countsArrayLength);
    }

    double getPercentileIteratedFrom() {
        return (100.0 * (double) totalCountToPrevIndex) / arrayTotalCount;
    }

    double getPercentileIteratedTo() {
        return (100.0 * (double) totalCountToCurrentIndex) / arrayTotalCount;
    }

    long getValueIteratedTo() {
        return histogram.highestEquivalentValue(currentValueAtIndex);
    }

    abstract void incrementIterationLevel();

    void incrementSubBucket() {
        freshSubBucket = true;
        // Take on the next index:
        currentIndex++;
        currentValueAtIndex = histogram.valueFromIndex(currentIndex);
        // Figure out the value at the next index (used by some iterators):
        nextValueAtIndex = histogram.valueFromIndex(currentIndex + 1);
    }

    /**
     * @return true if the current position's data should be emitted by the iterator
     */
    abstract boolean reachedIterationLevel();

    void resetIterator(final AbstractHistogram histogram) {
        this.histogram = histogram;
        this.arrayTotalCount = histogram.getTotalCount();
        this.integerToDoubleValueConversionRatio = histogram.getIntegerToDoubleValueConversionRatio();
        this.currentIndex = 0;
        this.currentValueAtIndex = 0;
        this.nextValueAtIndex = 1 << histogram.unitMagnitude;
        this.prevValueIteratedTo = 0;
        this.totalCountToPrevIndex = 0;
        this.totalCountToCurrentIndex = 0;
        this.totalValueToCurrentIndex = 0;
        this.countAtThisValue = 0;
        this.freshSubBucket = true;
        currentIterationValue.reset();
    }
}
