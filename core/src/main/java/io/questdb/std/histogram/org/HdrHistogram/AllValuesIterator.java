/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * @author Gil Tene
 */

package io.questdb.std.histogram.org.HdrHistogram;

import java.util.ConcurrentModificationException;
import java.util.Iterator;

/**
 * Used for iterating through histogram values using the finest granularity steps supported by the underlying
 * representation. The iteration steps through all possible unit value levels, regardless of whether or not
 * there were recorded values for that value level, and terminates when all recorded histogram values are exhausted.
 */

public class AllValuesIterator extends AbstractHistogramIterator implements Iterator<HistogramIterationValue> {
    int visitedIndex;

    /**
     * @param histogram The histogram this iterator will operate on
     */
    public AllValuesIterator(final AbstractHistogram histogram) {
        reset(histogram);
    }

    @Override
    public boolean hasNext() {
        if (histogram.getTotalCount() != arrayTotalCount) {
            throw new ConcurrentModificationException();
        }
        // Unlike other iterators AllValuesIterator is only done when we've exhausted the indices:
        return (currentIndex < (histogram.countsArrayLength - 1));
    }

    /**
     * Reset iterator for re-use in a fresh iteration over the same histogram data set.
     */
    public void reset() {
        reset(histogram);
    }

    private void reset(final AbstractHistogram histogram) {
        super.resetIterator(histogram);
        visitedIndex = -1;
    }

    @Override
    void incrementIterationLevel() {
        visitedIndex = currentIndex;
    }

    @Override
    boolean reachedIterationLevel() {
        return (visitedIndex != currentIndex);
    }
}
