/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Example QuestDB Plugin - Weighted Average GROUP BY Function Implementation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 ******************************************************************************/

package io.questdb.plugin.example;

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
 * Weighted average aggregate function implementation.
 *
 * This function maintains two values in the map:
 *   - sumWeightedValues: sum of (value * weight)
 *   - sumWeights: sum of weights
 *
 * The final result is: sumWeightedValues / sumWeights
 *
 * For parallel GROUP BY support, the merge() method combines partial results.
 */
public class ExampleWeightedAvgDoubleGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {

    private final Function valueArg;
    private final Function weightArg;
    // Indices in the MapValue where we store our accumulated state
    private int sumWeightedValuesIndex;
    private int sumWeightsIndex;

    public ExampleWeightedAvgDoubleGroupByFunction(@NotNull Function valueArg, @NotNull Function weightArg) {
        this.valueArg = valueArg;
        this.weightArg = weightArg;
    }

    @Override
    public Function getLeft() {
        return valueArg;
    }

    @Override
    public Function getRight() {
        return weightArg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        // First row in a group - initialize state
        final double value = valueArg.getDouble(record);
        final double weight = weightArg.getDouble(record);

        if (Numbers.isFinite(value) && Numbers.isFinite(weight)) {
            mapValue.putDouble(sumWeightedValuesIndex, value * weight);
            mapValue.putDouble(sumWeightsIndex, weight);
        } else {
            // Handle null/NaN - start with zeros
            mapValue.putDouble(sumWeightedValuesIndex, 0.0);
            mapValue.putDouble(sumWeightsIndex, 0.0);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        // Subsequent rows in a group - accumulate state
        final double value = valueArg.getDouble(record);
        final double weight = weightArg.getDouble(record);

        if (Numbers.isFinite(value) && Numbers.isFinite(weight)) {
            final double currentSumWV = mapValue.getDouble(sumWeightedValuesIndex);
            final double currentSumW = mapValue.getDouble(sumWeightsIndex);
            mapValue.putDouble(sumWeightedValuesIndex, currentSumWV + (value * weight));
            mapValue.putDouble(sumWeightsIndex, currentSumW + weight);
        }
    }

    @Override
    public double getDouble(Record rec) {
        // Called to get the final result for a group
        final double sumWeightedValues = rec.getDouble(sumWeightedValuesIndex);
        final double sumWeights = rec.getDouble(sumWeightsIndex);

        if (sumWeights == 0.0) {
            return Double.NaN; // No valid data in group
        }
        return sumWeightedValues / sumWeights;
    }

    @Override
    public String getName() {
        return "example_weighted_avg";
    }

    @Override
    public int getValueIndex() {
        return sumWeightedValuesIndex;
    }

    @Override
    public void initValueIndex(int valueIndex) {
        // Called for cloned functions in parallel execution
        this.sumWeightedValuesIndex = valueIndex;
        this.sumWeightsIndex = valueIndex + 1;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        // Called to register the columns we need in the map
        // We need two doubles: sumWeightedValues and sumWeights
        this.sumWeightedValuesIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.DOUBLE);
        this.sumWeightsIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.DOUBLE);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        // Merge partial results from parallel execution
        final double destSumWV = destValue.getDouble(sumWeightedValuesIndex);
        final double destSumW = destValue.getDouble(sumWeightsIndex);
        final double srcSumWV = srcValue.getDouble(sumWeightedValuesIndex);
        final double srcSumW = srcValue.getDouble(sumWeightsIndex);

        destValue.putDouble(sumWeightedValuesIndex, destSumWV + srcSumWV);
        destValue.putDouble(sumWeightsIndex, destSumW + srcSumW);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDouble(sumWeightedValuesIndex, 0.0);
        mapValue.putDouble(sumWeightsIndex, 0.0);
    }

    @Override
    public void setDouble(MapValue mapValue, double value) {
        // Used for interpolation - set weighted sum, assume weight of 1
        mapValue.putDouble(sumWeightedValuesIndex, value);
        mapValue.putDouble(sumWeightsIndex, 1.0);
    }

    @Override
    public boolean supportsParallelism() {
        // Enable parallel GROUP BY by implementing merge()
        return true;
    }
}
