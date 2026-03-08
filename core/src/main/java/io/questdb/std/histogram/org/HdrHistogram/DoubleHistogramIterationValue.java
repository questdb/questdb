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

/**
 * Represents a value point iterated through in a {@link DoubleHistogram}, with associated stats.
 * <ul>
 * <li><b><code>valueIteratedTo</code></b> :<br> The actual value level that was iterated to by the iterator</li>
 * <li><b><code>prevValueIteratedTo</code></b> :<br> The actual value level that was iterated from by the iterator</li>
 * <li><b><code>countAtValueIteratedTo</code></b> :<br> The count of recorded values in the histogram that
 * exactly match this [lowestEquivalentValue(valueIteratedTo)...highestEquivalentValue(valueIteratedTo)] value
 * range.</li>
 * <li><b><code>countAddedInThisIterationStep</code></b> :<br> The count of recorded values in the histogram that
 * were added to the totalCountToThisValue (below) as a result on this iteration step. Since multiple iteration
 * steps may occur with overlapping equivalent value ranges, the count may be lower than the count found at
 * the value (e.g. multiple linear steps or percentile levels can occur within a single equivalent value range)</li>
 * <li><b><code>totalCountToThisValue</code></b> :<br> The total count of all recorded values in the histogram at
 * values equal or smaller than valueIteratedTo.</li>
 * <li><b><code>totalValueToThisValue</code></b> :<br> The sum of all recorded values in the histogram at values
 * equal or smaller than valueIteratedTo.</li>
 * <li><b><code>percentile</code></b> :<br> The percentile of recorded values in the histogram at values equal
 * or smaller than valueIteratedTo.</li>
 * <li><b><code>percentileLevelIteratedTo</code></b> :<br> The percentile level that the iterator returning this
 * HistogramIterationValue had iterated to. Generally, percentileLevelIteratedTo will be equal to or smaller than
 * percentile, but the same value point can contain multiple iteration levels for some iterators. E.g. a
 * PercentileIterator can stop multiple times in the exact same value point (if the count at that value covers a
 * range of multiple percentiles in the requested percentile iteration points).</li>
 * </ul>
 */

public class DoubleHistogramIterationValue {
    private final HistogramIterationValue integerHistogramIterationValue;

    DoubleHistogramIterationValue(HistogramIterationValue integerHistogramIterationValue) {
        this.integerHistogramIterationValue = integerHistogramIterationValue;
    }

    public long getCountAddedInThisIterationStep() {
        return integerHistogramIterationValue.getCountAddedInThisIterationStep();
    }

    public long getCountAtValueIteratedTo() {
        return integerHistogramIterationValue.getCountAtValueIteratedTo();
    }

    public HistogramIterationValue getIntegerHistogramIterationValue() {
        return integerHistogramIterationValue;
    }

    public double getPercentile() {
        return integerHistogramIterationValue.getPercentile();
    }

    public double getPercentileLevelIteratedTo() {
        return integerHistogramIterationValue.getPercentileLevelIteratedTo();
    }

    public long getTotalCountToThisValue() {
        return integerHistogramIterationValue.getTotalCountToThisValue();
    }

    public double getTotalValueToThisValue() {
        return integerHistogramIterationValue.getTotalValueToThisValue() *
                integerHistogramIterationValue.getIntegerToDoubleValueConversionRatio();
    }

    public double getValueIteratedFrom() {
        return integerHistogramIterationValue.getValueIteratedFrom() *
                integerHistogramIterationValue.getIntegerToDoubleValueConversionRatio();
    }

    public double getValueIteratedTo() {
        return integerHistogramIterationValue.getValueIteratedTo() *
                integerHistogramIterationValue.getIntegerToDoubleValueConversionRatio();
    }

    public String toString() {
        return "valueIteratedTo:" + getValueIteratedTo() +
                ", prevValueIteratedTo:" + getValueIteratedFrom() +
                ", countAtValueIteratedTo:" + getCountAtValueIteratedTo() +
                ", countAddedInThisIterationStep:" + getCountAddedInThisIterationStep() +
                ", totalCountToThisValue:" + getTotalCountToThisValue() +
                ", totalValueToThisValue:" + getTotalValueToThisValue() +
                ", percentile:" + getPercentile() +
                ", percentileLevelIteratedTo:" + getPercentileLevelIteratedTo();
    }

    void reset() {
        integerHistogramIterationValue.reset();
    }
}
