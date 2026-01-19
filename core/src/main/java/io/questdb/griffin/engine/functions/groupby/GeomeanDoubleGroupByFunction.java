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
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

/**
 * Computes the geometric mean of a set of positive numbers.
 * <p>
 * The geometric mean is calculated as exp(avg(ln(x))) = exp(sum(ln(x)) / count).
 * <p>
 * Edge cases:
 * <ul>
 *   <li>Negative values: returns NaN (geometric mean is undefined for negative numbers)</li>
 *   <li>Zero values: returns NaN (ln(0) is undefined)</li>
 *   <li>Null values: skipped (standard SQL aggregate behavior)</li>
 *   <li>Empty groups: returns null (standard SQL aggregate behavior)</li>
 * </ul>
 */
public class GeomeanDoubleGroupByFunction extends DoubleFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private int valueIndex;

    public GeomeanDoubleGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final double d = arg.getDouble(record);
        if (Numbers.isNull(d)) {
            // Null value: initialize with zero count
            mapValue.putDouble(valueIndex, 0.0);
            mapValue.putLong(valueIndex + 1, 0L);
        } else if (d <= 0) {
            // Negative or zero: mark as invalid by setting sumLn to NaN
            mapValue.putDouble(valueIndex, Double.NaN);
            mapValue.putLong(valueIndex + 1, 1L);
        } else {
            // Valid positive value
            mapValue.putDouble(valueIndex, Math.log(d));
            mapValue.putLong(valueIndex + 1, 1L);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final double d = arg.getDouble(record);
        if (Numbers.isNull(d)) {
            // Null value: skip
            return;
        }
        if (d <= 0) {
            // Negative or zero: mark as invalid
            mapValue.putDouble(valueIndex, Double.NaN);
            mapValue.addLong(valueIndex + 1, 1L);
        } else {
            // Valid positive value: add ln(d) to sum
            // If sumLn is already NaN, adding to it will keep it NaN
            mapValue.addDouble(valueIndex, Math.log(d));
            mapValue.addLong(valueIndex + 1, 1L);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public double getDouble(Record rec) {
        final long count = rec.getLong(valueIndex + 1);
        if (count == 0) {
            // Empty group: return NaN (will be displayed as null)
            return Double.NaN;
        }
        final double sumLn = rec.getDouble(valueIndex);
        if (Double.isNaN(sumLn)) {
            // Invalid value was encountered
            return Double.NaN;
        }
        return Math.exp(sumLn / count);
    }

    @Override
    public String getName() {
        return "geomean";
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
        columnTypes.add(ColumnType.DOUBLE); // sumLn
        columnTypes.add(ColumnType.LONG);   // count
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isThreadSafe() {
        return UnaryFunction.super.isThreadSafe();
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        final long srcCount = srcValue.getLong(valueIndex + 1);
        if (srcCount == 0) {
            // Source has no valid values: nothing to merge
            return;
        }
        final double srcSumLn = srcValue.getDouble(valueIndex);
        final long destCount = destValue.getLong(valueIndex + 1);
        if (destCount == 0) {
            // Destination has no valid values: copy from source
            destValue.putDouble(valueIndex, srcSumLn);
            destValue.putLong(valueIndex + 1, srcCount);
        } else {
            // Both have values: combine sums and counts
            // If either is NaN, the result will be NaN (NaN propagates)
            destValue.putDouble(valueIndex, destValue.getDouble(valueIndex) + srcSumLn);
            destValue.putLong(valueIndex + 1, destCount + srcCount);
        }
    }

    @Override
    public void setDouble(MapValue mapValue, double value) {
        if (value <= 0) {
            mapValue.putDouble(valueIndex, Double.NaN);
        } else {
            mapValue.putDouble(valueIndex, Math.log(value));
        }
        mapValue.putLong(valueIndex + 1, 1);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDouble(valueIndex, Double.NaN);
        mapValue.putLong(valueIndex + 1, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }
}
