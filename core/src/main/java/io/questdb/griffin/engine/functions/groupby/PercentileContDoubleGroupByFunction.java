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
        listA.sort(0, size - 1);
        double percentile = percentileFunc.getDouble(record);
        if (percentile < 0.0d || percentile > 1.0d) {
            throw CairoException.nonCritical().position(percentilePos).put("invalid percentile [expected=range(0.0, 1.0), actual=").put(percentile).put(']');
        }

        // Calculate the continuous percentile position
        double position = percentile * (size - 1);
        int lowerIndex = (int) Math.floor(position);
        int upperIndex = (int) Math.ceil(position);

        // If the position is exactly at an index, return that value
        if (lowerIndex == upperIndex) {
            return listA.getQuick(lowerIndex);
        }

        // Otherwise, perform linear interpolation between the two adjacent values
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
