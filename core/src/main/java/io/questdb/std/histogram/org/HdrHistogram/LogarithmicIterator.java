/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * @author Gil Tene
 */

package io.questdb.std.histogram.org.HdrHistogram;

import java.util.Iterator;

/**
 * Used for iterating through histogram values in logarithmically increasing levels. The iteration is
 * performed in steps that start at <i>valueUnitsInFirstBucket</i> and increase exponentially according to
 * <i>logBase</i>, terminating when all recorded histogram values are exhausted. Note that each iteration "bucket"
 * includes values up to and including the next bucket boundary value.
 */
public class LogarithmicIterator extends AbstractHistogramIterator implements Iterator<HistogramIterationValue> {
    long valueUnitsInFirstBucket;
    double logBase;
    double nextValueReportingLevel;
    long currentStepHighestValueReportingLevel;
    long currentStepLowestValueReportingLevel;

    /**
     * Reset iterator for re-use in a fresh iteration over the same histogram data set.
     * @param valueUnitsInFirstBucket the size (in value units) of the first value bucket step
     * @param logBase the multiplier by which the bucket size is expanded in each iteration step.
     */
    public void reset(final long valueUnitsInFirstBucket, final double logBase) {
        reset(histogram, valueUnitsInFirstBucket, logBase);
    }

    private void reset(final AbstractHistogram histogram, final long valueUnitsInFirstBucket, final double logBase) {
        super.resetIterator(histogram);
        this.logBase = logBase;
        this.valueUnitsInFirstBucket = valueUnitsInFirstBucket;
        nextValueReportingLevel = valueUnitsInFirstBucket;
        this.currentStepHighestValueReportingLevel = ((long) nextValueReportingLevel) - 1;
        this.currentStepLowestValueReportingLevel = histogram.lowestEquivalentValue(currentStepHighestValueReportingLevel);
    }

    /**
     * @param histogram The histogram this iterator will operate on
     * @param valueUnitsInFirstBucket the size (in value units) of the first value bucket step
     * @param logBase the multiplier by which the bucket size is expanded in each iteration step.
     */
    public LogarithmicIterator(final AbstractHistogram histogram, final long valueUnitsInFirstBucket, final double logBase) {
        reset(histogram, valueUnitsInFirstBucket, logBase);
    }

    @Override
    public boolean hasNext() {
        if (super.hasNext()) {
            return true;
        }
        // If the next iterate will not move to the next sub bucket index (which is empty if
        // if we reached this point), then we are not yet done iterating (we want to iterate
        // until we are no longer on a value that has a count, rather than util we first reach
        // the last value that has a count. The difference is subtle but important)...
        return (histogram.lowestEquivalentValue((long) nextValueReportingLevel) < nextValueAtIndex);
    }

    @Override
    void incrementIterationLevel() {
        nextValueReportingLevel *= logBase;
        this.currentStepHighestValueReportingLevel = ((long)nextValueReportingLevel) - 1;
        currentStepLowestValueReportingLevel = histogram.lowestEquivalentValue(currentStepHighestValueReportingLevel);
    }

    @Override
    long getValueIteratedTo() {
        return currentStepHighestValueReportingLevel;
    }

    @Override
    boolean reachedIterationLevel() {
        return ((currentValueAtIndex >= currentStepLowestValueReportingLevel) ||
                (currentIndex >= histogram.countsArrayLength - 1)) ;
    }
}
