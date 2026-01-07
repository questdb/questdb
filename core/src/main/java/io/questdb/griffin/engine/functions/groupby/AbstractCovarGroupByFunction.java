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
 * The abstract class, in addition, provides a method to aggregate bivariate/regression statistics.
 * We use an algorithm similar to B.P. Welford's which works by first aggregating sum of squares of
 * independent and dependent variables Sxy = sum[(X - meanX) * (Y - meanY)].
 * Computation of covariance is then simple, e.g. covariance = Sxy / (n - 1)
 *
 * @see <a href="https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online">Welford's algorithm</a>
 */
public abstract class AbstractCovarGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
    /**
     * The X (independent variable) function.
     */
    protected final Function xFunc;
    /**
     * The Y (dependent variable) function.
     */
    protected final Function yFunc;
    /**
     * The value index in the map.
     */
    protected int valueIndex;

    /**
     * Constructs a new covariance group by function.
     *
     * @param arg0 the Y (dependent variable) function
     * @param arg1 the X (independent variable) function
     */
    protected AbstractCovarGroupByFunction(@NotNull Function arg0, @NotNull Function arg1) {
        this.yFunc = arg0;
        this.xFunc = arg1;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final double y = yFunc.getDouble(record);
        final double x = xFunc.getDouble(record);
        mapValue.putDouble(valueIndex, 0);
        mapValue.putDouble(valueIndex + 1, 0);
        mapValue.putDouble(valueIndex + 2, 0);
        mapValue.putLong(valueIndex + 3, 0);

        if (Numbers.isFinite(y) && Numbers.isFinite(x)) {
            aggregate(mapValue, y, x);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final double y = yFunc.getDouble(record);
        final double x = xFunc.getDouble(record);
        if (Numbers.isFinite(y) && Numbers.isFinite(x)) {
            aggregate(mapValue, y, x);
        }
    }

    @Override
    public Function getLeft() {
        return yFunc;
    }

    @Override
    public Function getRight() {
        return xFunc;
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
        columnTypes.add(ColumnType.LONG);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public void setDouble(MapValue mapValue, double value) {
        mapValue.putDouble(valueIndex + 2, value);
        mapValue.putLong(valueIndex + 3, 1L);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDouble(valueIndex, Double.NaN);
        mapValue.putDouble(valueIndex + 1, Double.NaN);
        mapValue.putDouble(valueIndex + 2, Double.NaN);
        mapValue.putLong(valueIndex + 3, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return false;
    }

    /**
     * Aggregates a data point into the covariance calculation.
     *
     * @param mapValue the map value to update
     * @param y        the dependent variable value
     * @param x        the independent variable value
     */
    protected void aggregate(MapValue mapValue, double y, double x) {
        double meanY = mapValue.getDouble(valueIndex);
        double meanX = mapValue.getDouble(valueIndex + 1);
        double sumXY = mapValue.getDouble(valueIndex + 2);
        long count = mapValue.getLong(valueIndex + 3) + 1;

        double oldMeanY = meanY;
        meanY += (y - meanY) / count;
        meanX += (x - meanX) / count;
        sumXY += (y - oldMeanY) * (x - meanX);

        mapValue.putDouble(valueIndex, meanY);
        mapValue.putDouble(valueIndex + 1, meanX);
        mapValue.putDouble(valueIndex + 2, sumXY);
        mapValue.addLong(valueIndex + 3, 1L);
    }
}

