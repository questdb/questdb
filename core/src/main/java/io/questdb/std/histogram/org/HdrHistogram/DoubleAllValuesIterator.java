/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * @author Gil Tene
 */

package io.questdb.std.histogram.org.HdrHistogram;

import java.util.Iterator;

/**
 * Used for iterating through {@link DoubleHistogram} values using the finest granularity steps supported by the
 * underlying representation. The iteration steps through all possible unit value levels, regardless of whether or not
 * there were recorded values for that value level, and terminates when all recorded histogram values are exhausted.
 */
public class DoubleAllValuesIterator implements Iterator<DoubleHistogramIterationValue> {
    private final AllValuesIterator integerAllValuesIterator;
    private final DoubleHistogramIterationValue iterationValue;
    DoubleHistogram histogram;

    /**
     * @param histogram The histogram this iterator will operate on
     */
    public DoubleAllValuesIterator(final DoubleHistogram histogram) {
        this.histogram = histogram;
        integerAllValuesIterator = new AllValuesIterator(histogram.integerValuesHistogram);
        iterationValue = new DoubleHistogramIterationValue(integerAllValuesIterator.currentIterationValue);
    }

    @Override
    public boolean hasNext() {
        return integerAllValuesIterator.hasNext();
    }

    @Override
    public DoubleHistogramIterationValue next() {
        integerAllValuesIterator.next();
        return iterationValue;
    }

    @Override
    public void remove() {
        integerAllValuesIterator.remove();
    }

    /**
     * Reset iterator for re-use in a fresh iteration over the same histogram data set.
     */
    public void reset() {
        integerAllValuesIterator.reset();
    }
}
