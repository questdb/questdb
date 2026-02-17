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
 * Used for iterating through histogram values in linear steps. The iteration is
 * performed in steps of <i>valueUnitsPerBucket</i> in size, terminating when all recorded histogram
 * values are exhausted. Note that each iteration "bucket" includes values up to and including
 * the next bucket boundary value.
 */
public class LinearIterator extends AbstractHistogramIterator implements Iterator<HistogramIterationValue> {
    private long currentStepHighestValueReportingLevel;
    private long currentStepLowestValueReportingLevel;
    private long valueUnitsPerBucket;

    /**
     * @param histogram           The histogram this iterator will operate on
     * @param valueUnitsPerBucket The size (in value units) of each bucket iteration.
     */
    public LinearIterator(final AbstractHistogram histogram, final long valueUnitsPerBucket) {
        reset(histogram, valueUnitsPerBucket);
    }

    @Override
    public boolean hasNext() {
        if (super.hasNext()) {
            return true;
        }
        // If the next iteration will not move to the next sub bucket index (which is empty if
        // if we reached this point), then we are not yet done iterating (we want to iterate
        // until we are no longer on a value that has a count, rather than util we first reach
        // the last value that has a count. The difference is subtle but important)...
        // When this is called, we're about to begin the "next" iteration, so
        // currentStepHighestValueReportingLevel has already been incremented, and we use it
        // without incrementing its value.
        return (currentStepHighestValueReportingLevel < nextValueAtIndex);
    }

    /**
     * Reset iterator for re-use in a fresh iteration over the same histogram data set.
     *
     * @param valueUnitsPerBucket The size (in value units) of each bucket iteration.
     */
    public void reset(final long valueUnitsPerBucket) {
        reset(histogram, valueUnitsPerBucket);
    }

    private void reset(final AbstractHistogram histogram, final long valueUnitsPerBucket) {
        super.resetIterator(histogram);
        this.valueUnitsPerBucket = valueUnitsPerBucket;
        this.currentStepHighestValueReportingLevel = valueUnitsPerBucket - 1;
        this.currentStepLowestValueReportingLevel = histogram.lowestEquivalentValue(currentStepHighestValueReportingLevel);
    }

    @Override
    long getValueIteratedTo() {
        return currentStepHighestValueReportingLevel;
    }

    @Override
    void incrementIterationLevel() {
        currentStepHighestValueReportingLevel += valueUnitsPerBucket;
        currentStepLowestValueReportingLevel = histogram.lowestEquivalentValue(currentStepHighestValueReportingLevel);
    }

    @Override
    boolean reachedIterationLevel() {
        return ((currentValueAtIndex >= currentStepLowestValueReportingLevel) ||
                (currentIndex >= histogram.countsArrayLength - 1));
    }
}
