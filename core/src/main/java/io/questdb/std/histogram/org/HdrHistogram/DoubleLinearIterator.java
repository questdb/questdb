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
 * Used for iterating through {@link DoubleHistogram} values in linear steps. The iteration is
 * performed in steps of <i>valueUnitsPerBucket</i> in size, terminating when all recorded histogram
 * values are exhausted. Note that each iteration "bucket" includes values up to and including
 * the next bucket boundary value.
 */
public class DoubleLinearIterator implements Iterator<DoubleHistogramIterationValue> {
    private final LinearIterator integerLinearIterator;
    private final DoubleHistogramIterationValue iterationValue;
    DoubleHistogram histogram;

    /**
     * @param histogram           The histogram this iterator will operate on
     * @param valueUnitsPerBucket The size (in value units) of each bucket iteration.
     */
    public DoubleLinearIterator(final DoubleHistogram histogram, final double valueUnitsPerBucket) {
        this.histogram = histogram;
        integerLinearIterator = new LinearIterator(
                histogram.integerValuesHistogram,
                (long) (valueUnitsPerBucket * histogram.getDoubleToIntegerValueConversionRatio())
        );
        iterationValue = new DoubleHistogramIterationValue(integerLinearIterator.currentIterationValue);
    }

    @Override
    public boolean hasNext() {
        return integerLinearIterator.hasNext();
    }

    @Override
    public DoubleHistogramIterationValue next() {
        integerLinearIterator.next();
        return iterationValue;
    }

    @Override
    public void remove() {
        integerLinearIterator.remove();
    }

    /**
     * Reset iterator for re-use in a fresh iteration over the same histogram data set.
     *
     * @param valueUnitsPerBucket The size (in value units) of each bucket iteration.
     */
    public void reset(final double valueUnitsPerBucket) {
        integerLinearIterator.reset((long) (valueUnitsPerBucket * histogram.getDoubleToIntegerValueConversionRatio()));
    }
}
