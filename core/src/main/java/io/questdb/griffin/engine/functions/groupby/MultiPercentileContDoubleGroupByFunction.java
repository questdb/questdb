/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import org.jetbrains.annotations.NotNull;

public class MultiPercentileContDoubleGroupByFunction extends MultiPercentileDiscDoubleGroupByFunction implements UnaryFunction, GroupByFunction {
    public MultiPercentileContDoubleGroupByFunction(@NotNull CairoConfiguration configuration, @NotNull Function arg, @NotNull Function percentileFunc, int percentilePos) {
        super(configuration, arg, percentileFunc, percentilePos);
    }

    @Override
    public ArrayView getArray(Record record) {
        long listPtr = record.getLong(valueIndex);
        if (listPtr <= 0) {
            if (out == null) {
                out = new DirectArray();
            }
            out.ofNull();
            return out;
        }
        listA.of(listPtr);
        int size = listA.size();
        if (size == 0) {
            if (out == null) {
                out = new DirectArray();
            }
            out.ofNull();
            return out;
        }

        ArrayView percentiles = percentileFunc.getArray(record);
        FlatArrayView view = percentiles.flatView();
        int view_length = view.length();

        if (out == null) {
            out = new DirectArray();
            out.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
            out.setDimLen(0, view_length);
            out.applyShape();
        }

        // Collect all unique indices needed for interpolation
        java.util.Set<Integer> indexSet = new java.util.HashSet<>();
        for (int i = 0, len = view.length(); i < len; i++) {
            double percentile = view.getDoubleAtAbsIndex(i);
            double multiplier = SqlUtil.getPercentileMultiplier(percentile, percentilePos);

            // Calculate the continuous percentile position
            double position = multiplier * (size - 1);
            int lowerIndex = (int) Math.floor(position);
            int upperIndex = (int) Math.ceil(position);

            indexSet.add(lowerIndex);
            if (lowerIndex != upperIndex) {
                indexSet.add(upperIndex);
            }
        }

        // Convert to sorted array for quickSelectMultiple
        int[] indices = new int[indexSet.size()];
        int idx = 0;
        for (int index : indexSet) {
            indices[idx++] = index;
        }
        java.util.Arrays.sort(indices);

        // Use optimized multi-select instead of full sorting
        listA.quickSelectMultiple(0, size - 1, indices, 0, indices.length);

        // Now compute interpolated values
        for (int i = 0, len = view.length(); i < len; i++) {
            double percentile = view.getDoubleAtAbsIndex(i);
            double multiplier = SqlUtil.getPercentileMultiplier(percentile, percentilePos);
            double position = multiplier * (size - 1);
            int lowerIndex = (int) Math.floor(position);
            int upperIndex = (int) Math.ceil(position);

            // If the position is exactly at an index, return that value
            if (lowerIndex == upperIndex) {
                out.putDouble(i, listA.getQuick(lowerIndex));
            } else {
                // Otherwise, perform linear interpolation between the two adjacent values
                double lowerValue = listA.getQuick(lowerIndex);
                double upperValue = listA.getQuick(upperIndex);
                double fraction = position - lowerIndex;
                out.putDouble(i, lowerValue + (upperValue - lowerValue) * fraction);
            }
        }
        return out;
    }

    @Override
    public String getName() {
        return "percentile_cont";
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("percentile_cont(").val(arg).val(')');
    }
}
