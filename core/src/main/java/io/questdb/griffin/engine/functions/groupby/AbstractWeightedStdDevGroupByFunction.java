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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

/**
 * Base class to compute the unbiased weighted standard deviation.
 * <p>
 * According to
 * <a href="https://en.wikipedia.org/wiki/Weighted_arithmetic_mean#Weighted_sample_variance">
 * Wikipedia</a>, there are two variants for the unbiased estimator of population variance
 * based on a subset of it (a sample): one for frequency weights, and another for reliability
 * weights.
 * <p>
 * A frequency weight represents the number of occurrences of the sample in the dataset.
 * A reliability weight represents the "importance" or "trustworthiness" of the given sample.
 * <p>
 * We implement both functions, called <code>weighted_stddev_rel</code> for reliability weights,
 * and <code>weighted_stddev_freq</code> for frequency weights. We also define the shorthand
 * <code>weighted_stddev</code> for <code>weighted_stddev_rel</code>.
 * <p>
 * These two:
 * <pre>
 * SELECT weighted_stddev_rel(value, weight) FROM my_table;
 * SELECT weighted_stddev(value, weight) FROM my_table;
 * </pre>
 * calculate the equivalent of
 * <pre>
 *   SELECT sqrt(
 *     (
 *       sum(weight * value * value)
 *       - (sum(weight * value) * sum(weight * value) / sum(weight))
 *     )
 *     / (sum(weight) - sum(weight * weight) / sum(weight))
 *   ) FROM my_table;
 * </pre>
 * <p>
 * This one:
 * <pre>
 * SELECT weighted_stddev_freq(value, weight) FROM my_table;
 * </pre>
 * calculates the equivalent of
 * <pre>
 *   SELECT sqrt(
 *     (
 *       sum(weight * value * value)
 *       - (sum(weight * value) * sum(weight * value) / sum(weight))
 *     )
 *     / (sum(weight) - 1)
 *   ) FROM my_table;
 * </pre>
 *
 * @see <a href="https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Weighted_incremental_algorithm">
 * Weighted incremental algorithm
 * </a>
 * @see <a href="https://github.com/mraad/spark-stat/blob/master/src/main/scala/com/esri/spark/WeightedStatCounter.scala">
 * Merge function for weighted incremental algorithm
 * </a>
 */
public abstract class AbstractWeightedStdDevGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
    protected final Function sampleArg;
    private final Function weightArg;
    protected int valueIndex;

    protected AbstractWeightedStdDevGroupByFunction(@NotNull Function sampleArg, @NotNull Function weightArg) {
        this.sampleArg = sampleArg;
        this.weightArg = weightArg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final double sample = sampleArg.getDouble(record);
        final double weight = weightArg.getDouble(record);
        if (!Numbers.isFinite(sample) || !Numbers.isFinite(weight) || weight == 0.0) {
            mapValue.putDouble(valueIndex, 0.0); // w_sum
            mapValue.putDouble(valueIndex + 1, 0.0); // w_sum2
            mapValue.putDouble(valueIndex + 2, 0.0); // mean
            mapValue.putDouble(valueIndex + 3, 0.0); // S
            return;
        }
        mapValue.putDouble(valueIndex, weight); // w_sum
        mapValue.putDouble(valueIndex + 1, weight * weight); // w_sum2
        mapValue.putDouble(valueIndex + 2, sample); // mean
        mapValue.putDouble(valueIndex + 3, 0.0); // S
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        // Acquire values from record
        final double sample = sampleArg.getDouble(record);
        final double weight = weightArg.getDouble(record);
        if (!Numbers.isFinite(sample) || !Numbers.isFinite(weight) || weight == 0.0) {
            return;
        }
        // Acquire current computation state
        double wSum = mapValue.getDouble(valueIndex);
        double wSum2 = mapValue.getDouble(valueIndex + 1);
        double mean = mapValue.getDouble(valueIndex + 2);
        double s = mapValue.getDouble(valueIndex + 3);

        // Update computation state with values from record
        wSum += weight;
        wSum2 += weight * weight;
        double meanOld = mean;
        mean += (weight / wSum) * (sample - meanOld);
        s += weight * (sample - meanOld) * (sample - mean);

        // Store updated computation state
        mapValue.putDouble(valueIndex, wSum);
        mapValue.putDouble(valueIndex + 1, wSum2);
        mapValue.putDouble(valueIndex + 2, mean);
        mapValue.putDouble(valueIndex + 3, s);
    }

    @Override
    public Function getLeft() {
        return sampleArg;
    }

    @Override
    public Function getRight() {
        return weightArg;
    }

    @Override
    public int getSampleByFlags() {
        return GroupByFunction.SAMPLE_BY_FILL_ALL;
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.DOUBLE);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isThreadSafe() {
        return BinaryFunction.super.isThreadSafe();
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        // Acquire source computation state
        double srcWsum = srcValue.getDouble(valueIndex);
        double srcWsum2 = srcValue.getDouble(valueIndex + 1);
        double srcMean = srcValue.getDouble(valueIndex + 2);
        double srcS = srcValue.getDouble(valueIndex + 3);

        if (srcWsum == 0.0) {
            // srcValue has no data -- return with destValue untouched
            return;
        }

        // Acquire destination computation state
        double destWsum = destValue.getDouble(valueIndex);
        double destWsum2 = destValue.getDouble(valueIndex + 1);
        double destMean = destValue.getDouble(valueIndex + 2);
        double destS = destValue.getDouble(valueIndex + 3);

        if (destWsum == 0.0) {
            // srcValue has data, destValue doesn't. Copy entire srcValue to destValue.
            destValue.putDouble(valueIndex, srcWsum);
            destValue.putDouble(valueIndex + 1, srcWsum2);
            destValue.putDouble(valueIndex + 2, srcMean);
            destValue.putDouble(valueIndex + 3, srcS);
            return;
        }
        // Both srcValue and destValue have data -- merge them

        // Compute interim results
        double meanDelta = srcMean - destMean;

        // Compute merged computation state
        double mergedWsum = srcWsum + destWsum;
        double mergedWsum2 = srcWsum2 + destWsum2;
        double mergedMean = (srcWsum * srcMean + destWsum * destMean) / mergedWsum;
        double mergedS = srcS + destS + (srcWsum * meanDelta) / mergedWsum * (destWsum * meanDelta);

        // Store merged computation state to destination
        destValue.putDouble(valueIndex, mergedWsum);
        destValue.putDouble(valueIndex + 1, mergedWsum2);
        destValue.putDouble(valueIndex + 2, mergedMean);
        destValue.putDouble(valueIndex + 3, mergedS);
    }

    @Override
    public void setDouble(MapValue mapValue, double value) {
        // We set the state such that getDouble() in both Frequency and Reliability
        // subclasses end up returning `value`
        mapValue.putDouble(valueIndex, 2.0); // wSum
        mapValue.putDouble(valueIndex + 1, 2.0); // wSum2
        mapValue.putDouble(valueIndex + 2, Double.NaN); // mean
        mapValue.putDouble(valueIndex + 3, value * value); // S
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDouble(valueIndex, Double.NaN);
        mapValue.putDouble(valueIndex + 1, Double.NaN);
        mapValue.putDouble(valueIndex + 2, Double.NaN);
        mapValue.putDouble(valueIndex + 3, Double.NaN);
    }

    @Override
    public boolean supportsParallelism() {
        return BinaryFunction.super.supportsParallelism();
    }
}
