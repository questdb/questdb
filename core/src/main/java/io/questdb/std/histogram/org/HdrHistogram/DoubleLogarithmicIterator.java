/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * @author Gil Tene
 */

package io.questdb.std.histogram.org.HdrHistogram;

import java.util.Iterator;

/**
 * Used for iterating through {@link DoubleHistogram} values values in logarithmically increasing levels. The
 * iteration is performed in steps that start at <i>valueUnitsInFirstBucket</i> and increase exponentially according to
 * <i>logBase</i>, terminating when all recorded histogram values are exhausted. Note that each iteration "bucket"
 * includes values up to and including the next bucket boundary value.
 */
public class DoubleLogarithmicIterator implements Iterator<DoubleHistogramIterationValue> {
    private final LogarithmicIterator integerLogarithmicIterator;
    private final DoubleHistogramIterationValue iterationValue;
    DoubleHistogram histogram;

    /**
     * Reset iterator for re-use in a fresh iteration over the same histogram data set.
     * @param valueUnitsInFirstBucket the size (in value units) of the first value bucket step
     * @param logBase the multiplier by which the bucket size is expanded in each iteration step.
     */
    public void reset(final double valueUnitsInFirstBucket, final double logBase) {
        integerLogarithmicIterator.reset(
                (long) (valueUnitsInFirstBucket * histogram.getDoubleToIntegerValueConversionRatio()),
                logBase
        );
    }

    /**
     * @param histogram The histogram this iterator will operate on
     * @param valueUnitsInFirstBucket the size (in value units) of the first value bucket step
     * @param logBase the multiplier by which the bucket size is expanded in each iteration step.
     */
    public DoubleLogarithmicIterator(final DoubleHistogram histogram, final double valueUnitsInFirstBucket,
                                     final double logBase) {
        this.histogram = histogram;
        integerLogarithmicIterator = new LogarithmicIterator(
                histogram.integerValuesHistogram,
                (long) (valueUnitsInFirstBucket * histogram.getDoubleToIntegerValueConversionRatio()),
                logBase
        );
        iterationValue = new DoubleHistogramIterationValue(integerLogarithmicIterator.currentIterationValue);
    }

    @Override
    public boolean hasNext() {
        return integerLogarithmicIterator.hasNext();
    }

    @Override
    public DoubleHistogramIterationValue next() {
        integerLogarithmicIterator.next();
        return iterationValue;
    }

    @Override
    public void remove() {
        integerLogarithmicIterator.remove();
    }
}