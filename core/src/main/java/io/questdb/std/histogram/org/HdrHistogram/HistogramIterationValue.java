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
 * Represents a value point iterated through in a Histogram, with associated stats.
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

public class HistogramIterationValue {
    private long countAddedInThisIterationStep;
    private long countAtValueIteratedTo;
    private double integerToDoubleValueConversionRatio;
    private double percentile;
    private double percentileLevelIteratedTo;
    private long totalCountToThisValue;
    private long totalValueToThisValue;
    private long valueIteratedFrom;
    private long valueIteratedTo;

    HistogramIterationValue() {
    }

    public long getCountAddedInThisIterationStep() {
        return countAddedInThisIterationStep;
    }

    public long getCountAtValueIteratedTo() {
        return countAtValueIteratedTo;
    }

    public double getDoubleValueIteratedFrom() {
        return valueIteratedFrom * integerToDoubleValueConversionRatio;
    }

    public double getDoubleValueIteratedTo() {
        return valueIteratedTo * integerToDoubleValueConversionRatio;
    }

    public double getIntegerToDoubleValueConversionRatio() {
        return integerToDoubleValueConversionRatio;
    }

    public double getPercentile() {
        return percentile;
    }

    public double getPercentileLevelIteratedTo() {
        return percentileLevelIteratedTo;
    }

    public long getTotalCountToThisValue() {
        return totalCountToThisValue;
    }

    public long getTotalValueToThisValue() {
        return totalValueToThisValue;
    }

    public long getValueIteratedFrom() {
        return valueIteratedFrom;
    }

    public long getValueIteratedTo() {
        return valueIteratedTo;
    }

    public String toString() {
        return "valueIteratedTo:" + valueIteratedTo +
                ", prevValueIteratedTo:" + valueIteratedFrom +
                ", countAtValueIteratedTo:" + countAtValueIteratedTo +
                ", countAddedInThisIterationStep:" + countAddedInThisIterationStep +
                ", totalCountToThisValue:" + totalCountToThisValue +
                ", totalValueToThisValue:" + totalValueToThisValue +
                ", percentile:" + percentile +
                ", percentileLevelIteratedTo:" + percentileLevelIteratedTo;
    }

    void reset() {
        this.valueIteratedTo = 0;
        this.valueIteratedFrom = 0;
        this.countAtValueIteratedTo = 0;
        this.countAddedInThisIterationStep = 0;
        this.totalCountToThisValue = 0;
        this.totalValueToThisValue = 0;
        this.percentile = 0.0;
        this.percentileLevelIteratedTo = 0.0;
    }

    // Set is all-or-nothing to avoid the potential for accidental omission of some values...
    void set(final long valueIteratedTo, final long valueIteratedFrom, final long countAtValueIteratedTo,
             final long countInThisIterationStep, final long totalCountToThisValue, final long totalValueToThisValue,
             final double percentile, final double percentileLevelIteratedTo, double integerToDoubleValueConversionRatio) {
        this.valueIteratedTo = valueIteratedTo;
        this.valueIteratedFrom = valueIteratedFrom;
        this.countAtValueIteratedTo = countAtValueIteratedTo;
        this.countAddedInThisIterationStep = countInThisIterationStep;
        this.totalCountToThisValue = totalCountToThisValue;
        this.totalValueToThisValue = totalValueToThisValue;
        this.percentile = percentile;
        this.percentileLevelIteratedTo = percentileLevelIteratedTo;
        this.integerToDoubleValueConversionRatio = integerToDoubleValueConversionRatio;
    }
}
