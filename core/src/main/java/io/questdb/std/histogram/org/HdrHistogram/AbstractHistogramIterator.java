/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * @author Gil Tene
 */

package io.questdb.std.histogram.org.HdrHistogram;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Used for iterating through histogram values.
 */
abstract class AbstractHistogramIterator implements Iterator<HistogramIterationValue> {
    AbstractHistogram histogram;
    long arrayTotalCount;

    int currentIndex;
    long currentValueAtIndex;

    long nextValueAtIndex;

    long prevValueIteratedTo;
    long totalCountToPrevIndex;

    long totalCountToCurrentIndex;
    long totalValueToCurrentIndex;

    long countAtThisValue;

    private boolean freshSubBucket;
    final HistogramIterationValue currentIterationValue = new HistogramIterationValue();

    private double integerToDoubleValueConversionRatio;

    void resetIterator(final AbstractHistogram histogram) {
        this.histogram = histogram;
        this.arrayTotalCount = histogram.getTotalCount();
        this.integerToDoubleValueConversionRatio = histogram.getIntegerToDoubleValueConversionRatio();
        this.currentIndex = 0;
        this.currentValueAtIndex = 0;
        this.nextValueAtIndex = 1L << histogram.unitMagnitude;
        this.prevValueIteratedTo = 0;
        this.totalCountToPrevIndex = 0;
        this.totalCountToCurrentIndex = 0;
        this.totalValueToCurrentIndex = 0;
        this.countAtThisValue = 0;
        this.freshSubBucket = true;
        currentIterationValue.reset();
    }

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

    abstract void incrementIterationLevel();

    /**
     * @return true if the current position's data should be emitted by the iterator
     */
    abstract boolean reachedIterationLevel();

    double getPercentileIteratedTo() {
        return (100.0 * (double) totalCountToCurrentIndex) / arrayTotalCount;
    }

    double getPercentileIteratedFrom() {
        return (100.0 * (double) totalCountToPrevIndex) / arrayTotalCount;
    }

    long getValueIteratedTo() {
        return histogram.highestEquivalentValue(currentValueAtIndex);
    }

    private boolean exhaustedSubBuckets() {
        return (currentIndex >= histogram.countsArrayLength);
    }

    void incrementSubBucket() {
        freshSubBucket = true;
        // Take on the next index:
        currentIndex++;
        currentValueAtIndex = histogram.valueFromIndex(currentIndex);
        // Figure out the value at the next index (used by some iterators):
        nextValueAtIndex = histogram.valueFromIndex(currentIndex + 1);
    }
}
