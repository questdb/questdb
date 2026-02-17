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
 * Used for iterating through {@link DoubleHistogram} values values using the finest granularity steps supported by
 * the underlying representation. The iteration steps through all possible unit value levels, regardless of whether
 * or not there were recorded values for that value level, and terminates when all recorded histogram values are
 * exhausted.
 */
public class DoubleRecordedValuesIterator implements Iterator<DoubleHistogramIterationValue> {
    private final RecordedValuesIterator integerRecordedValuesIterator;
    private final DoubleHistogramIterationValue iterationValue;
    DoubleHistogram histogram;

    /**
     * @param histogram The histogram this iterator will operate on
     */
    public DoubleRecordedValuesIterator(final DoubleHistogram histogram) {
        this.histogram = histogram;
        integerRecordedValuesIterator = new RecordedValuesIterator(histogram.integerValuesHistogram);
        iterationValue = new DoubleHistogramIterationValue(integerRecordedValuesIterator.currentIterationValue);
    }

    @Override
    public boolean hasNext() {
        return integerRecordedValuesIterator.hasNext();
    }

    @Override
    public DoubleHistogramIterationValue next() {
        integerRecordedValuesIterator.next();
        return iterationValue;
    }

    @Override
    public void remove() {
        integerRecordedValuesIterator.remove();
    }

    /**
     * Reset iterator for re-use in a fresh iteration over the same histogram data set.
     */
    public void reset() {
        integerRecordedValuesIterator.reset();
    }
}
