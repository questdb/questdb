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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import org.jetbrains.annotations.NotNull;

public class PercentileContDoubleGroupByFunction extends PercentileDiscDoubleGroupByFunction implements UnaryFunction, GroupByFunction {

    public PercentileContDoubleGroupByFunction(@NotNull CairoConfiguration configuration, @NotNull Function arg, @NotNull Function percentileFunc, int percentilePos) {
        super(configuration, arg, percentileFunc, percentilePos);
    }

    @Override
    public double getDouble(Record record) {
        long listPtr = record.getLong(valueIndex);
        if (listPtr <= 0) {
            return Double.NaN;
        }
        listA.of(listPtr);
        int size = listA.size();
        if (size == 0) {
            return Double.NaN;
        }
        double percentile = percentileFunc.getDouble(record);
        double multiplier = SqlUtil.getPercentileMultiplier(percentile, percentilePos);

        // Calculate the continuous percentile position
        double position = multiplier * (size - 1);
        int lowerIndex = (int) Math.floor(position);
        int upperIndex = (int) Math.ceil(position);

        // If the position is exactly at an index, return that value
        if (lowerIndex == upperIndex) {
            // Use QuickSelect to find the element - O(n) vs O(n log n)
            listA.quickSelect(0, size - 1, lowerIndex);
            return listA.getQuick(lowerIndex);
        }

        // Need to select both adjacent indices for interpolation
        // Use quickSelectMultiple for efficiency
        int[] indices = new int[]{lowerIndex, upperIndex};
        listA.quickSelectMultiple(0, size - 1, indices, 0, 2);

        // Perform linear interpolation between the two adjacent values
        double lowerValue = listA.getQuick(lowerIndex);
        double upperValue = listA.getQuick(upperIndex);
        double fraction = position - lowerIndex;
        return lowerValue + (upperValue - lowerValue) * fraction;
    }

    @Override
    public String getName() {
        return "percentile_cont";
    }

    public void toPlan(PlanSink sink) {
        sink.val("percentile_cont(").val(arg).val(')');
    }

}
