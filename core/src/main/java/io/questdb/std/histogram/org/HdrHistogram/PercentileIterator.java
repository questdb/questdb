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

import java.util.Iterator;

/**
 * Used for iterating through histogram values according to percentile levels. The iteration is
 * performed in steps that start at 0% and reduce their distance to 100% according to the
 * <i>percentileTicksPerHalfDistance</i> parameter, ultimately reaching 100% when all recorded histogram
 * values are exhausted.
 */
public class PercentileIterator extends AbstractHistogramIterator implements Iterator<HistogramIterationValue> {
    double percentileLevelToIterateFrom;
    double percentileLevelToIterateTo;
    int percentileTicksPerHalfDistance;
    boolean reachedLastRecordedValue;

    /**
     * @param histogram                      The histogram this iterator will operate on
     * @param percentileTicksPerHalfDistance The number of equal-sized iteration steps per half-distance to 100%.
     */
    public PercentileIterator(final AbstractHistogram histogram, final int percentileTicksPerHalfDistance) {
        reset(histogram, percentileTicksPerHalfDistance);
    }

    @Override
    public boolean hasNext() {
        if (super.hasNext())
            return true;
        // We want one additional last step to 100%
        if (!reachedLastRecordedValue && (arrayTotalCount > 0)) {
            percentileLevelToIterateTo = 100.0;
            reachedLastRecordedValue = true;
            return true;
        }
        return false;
    }

    /**
     * Reset iterator for re-use in a fresh iteration over the same histogram data set.
     *
     * @param percentileTicksPerHalfDistance The number of iteration steps per half-distance to 100%.
     */
    public void reset(final int percentileTicksPerHalfDistance) {
        reset(histogram, percentileTicksPerHalfDistance);
    }

    private void reset(final AbstractHistogram histogram, final int percentileTicksPerHalfDistance) {
        super.resetIterator(histogram);
        this.percentileTicksPerHalfDistance = percentileTicksPerHalfDistance;
        this.percentileLevelToIterateTo = 0.0;
        this.percentileLevelToIterateFrom = 0.0;
        this.reachedLastRecordedValue = false;
    }

    @Override
    double getPercentileIteratedFrom() {
        return percentileLevelToIterateFrom;
    }

    @Override
    double getPercentileIteratedTo() {
        return percentileLevelToIterateTo;
    }

    @Override
    void incrementIterationLevel() {
        percentileLevelToIterateFrom = percentileLevelToIterateTo;

        // The choice to maintain fixed-sized "ticks" in each half-distance to 100% [starting
        // from 0%], as opposed to a "tick" size that varies with each interval, was made to
        // make the steps easily comprehensible and readable to humans. The resulting percentile
        // steps are much easier to browse through in a percentile distribution output, for example.
        //
        // We calculate the number of equal-sized "ticks" that the 0-100 range will be divided
        // by at the current scale. The scale is detemined by the percentile level we are
        // iterating to. The following math determines the tick size for the current scale,
        // and maintain a fixed tick size for the remaining "half the distance to 100%"
        // [from either 0% or from the previous half-distance]. When that half-distance is
        // crossed, the scale changes and the tick size is effectively cut in half.

        long percentileReportingTicks =
                percentileTicksPerHalfDistance *
                        (long) Math.pow(2,
                                (long) (Math.log(100.0 / (100.0 - (percentileLevelToIterateTo))) / Math.log(2)) + 1);
        percentileLevelToIterateTo += 100.0 / percentileReportingTicks;
    }

    @Override
    boolean reachedIterationLevel() {
        if (countAtThisValue == 0)
            return false;
        double currentPercentile = (100.0 * (double) totalCountToCurrentIndex) / arrayTotalCount;
        return (currentPercentile >= percentileLevelToIterateTo);
    }
}
