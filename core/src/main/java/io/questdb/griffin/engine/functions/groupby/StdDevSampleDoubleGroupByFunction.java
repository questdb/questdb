/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

/**
 * Standard deviation is calculated using an algorithm first proposed by B. P. Welford.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm">Welford's algorithm</a>
 */
public class StdDevSampleDoubleGroupByFunction extends DoubleFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private int valueIndex;

    public StdDevSampleDoubleGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        final double d = arg.getDouble(record);
        if (Numbers.isFinite(d)) {
            double mean = 0;
            double sum = 0;
            long count = 1;
            double oldMean = mean;
            mean += (d - mean) / count;
            sum += (d - mean) * (d - oldMean);
            mapValue.putDouble(valueIndex, mean);
            mapValue.putDouble(valueIndex + 1, sum);
            mapValue.putLong(valueIndex + 2, 1L);
        } else {
            mapValue.putDouble(valueIndex, 0);
            mapValue.putDouble(valueIndex + 1, 0);
            mapValue.putLong(valueIndex + 2, 0);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        final double d = arg.getDouble(record);
        if (Numbers.isFinite(d)) {
            double mean = mapValue.getDouble(valueIndex);
            double sum = mapValue.getDouble(valueIndex + 1);
            long count = mapValue.getLong(valueIndex + 2) + 1;
            double oldMean = mean;
            mean += (d - mean) / count;
            sum += (d - mean) * (d - oldMean);
            mapValue.putDouble(valueIndex, mean);
            mapValue.putDouble(valueIndex + 1, sum);
            mapValue.addLong(valueIndex + 2, 1L);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public double getDouble(Record rec) {
        long count = rec.getLong(valueIndex + 2);
        if (count - 1 > 0) {
            double sum = rec.getDouble(valueIndex + 1);
            double variance = sum / (count - 1);
            return Math.sqrt(variance);
        }
        return Double.NaN;
    }

    @Override
    public String getName() {
        return "stddev_samp";
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.LONG);
    }

    @Override
    public void setDouble(MapValue mapValue, double value) {
        mapValue.putDouble(valueIndex, value);
        mapValue.putLong(valueIndex + 2, 1L);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDouble(valueIndex, Double.NaN);
        mapValue.putDouble(valueIndex + 1, Double.NaN);
        mapValue.putLong(valueIndex + 2, 0);
    }
}
