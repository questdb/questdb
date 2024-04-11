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
 * We use an algorithm similar to B.P. Welford's which works by first aggregating sum of squares of
 * independent and dependent variables Sxx = sum[(X - meanX) ^ 2], Syy = sum[(Y - meanY) ^ 2], Sxy = sum[(X - meanX) * (Y - meanY)].
 * Computation of correlation is then simple, e.g. correlation = Sxy / sqrt(Sxx * Syy)
 *
 * @see <a href="https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online">Welford's algorithm</a>
 */
public class CorrGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
    protected final Function xFunction;
    protected final Function yFunction;
    protected int valueIndex;

    protected CorrGroupByFunction(@NotNull Function arg0, @NotNull Function arg1) {
        this.xFunction = arg0;
        this.yFunction = arg1;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final double x = xFunction.getDouble(record);
        final double y = yFunction.getDouble(record);
        mapValue.putDouble(valueIndex, 0);
        mapValue.putDouble(valueIndex + 1, 0);
        mapValue.putDouble(valueIndex + 2, 0);
        mapValue.putDouble(valueIndex + 3, 0);
        mapValue.putDouble(valueIndex + 4, 0);
        mapValue.putLong(valueIndex + 5, 0);

        if (Numbers.isFinite(x) && Numbers.isFinite(y)) {
            aggregate(mapValue, x, y);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final double x = xFunction.getDouble(record);
        final double y = yFunction.getDouble(record);
        if (Numbers.isFinite(x) && Numbers.isFinite(y)) {
            aggregate(mapValue, x, y);
        }
    }

    @Override
    public double getDouble(Record rec) {
        double sumX = rec.getDouble(valueIndex + 1);
        double sumY = rec.getDouble(valueIndex + 3);
        double sumXY = rec.getDouble(valueIndex + 4);
        double count = rec.getLong(valueIndex + 5);
        if (count <= 0) {
            return Double.NaN;
        }
        if (sumX == 0 || sumY == 0) {
            return Double.NaN;
        }
        return sumXY / Math.sqrt(sumX * sumY);
    }

    @Override
    public Function getLeft() {
        return xFunction;
    }

    @Override
    public String getName() {
        return "corr";
    }

    @Override
    public Function getRight() {
        return yFunction;
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
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.LONG);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public void setDouble(MapValue mapValue, double value) {
        mapValue.putDouble(valueIndex + 4, value);
        mapValue.putLong(valueIndex + 5, 1L);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDouble(valueIndex, Double.NaN);
        mapValue.putDouble(valueIndex + 1, Double.NaN);
        mapValue.putDouble(valueIndex + 2, Double.NaN);
        mapValue.putDouble(valueIndex + 3, Double.NaN);
        mapValue.putDouble(valueIndex + 4, Double.NaN);
        mapValue.putLong(valueIndex + 5, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return false;
    }

    protected void aggregate(MapValue mapValue, double x, double y) {
        double meanX = mapValue.getDouble(valueIndex);
        double sumX = mapValue.getDouble(valueIndex + 1);
        double meanY = mapValue.getDouble(valueIndex + 2);
        double sumY = mapValue.getDouble(valueIndex + 3);
        double sumXY = mapValue.getDouble(valueIndex + 4);
        long count = mapValue.getLong(valueIndex + 5) + 1;

        double oldMeanX = meanX;
        meanX += (x - meanX) / count;
        sumX += (x - meanX) * (x - oldMeanX);
        double oldMeanY = meanY;
        meanY += (y - meanY) / count;
        sumY += (y - meanY) * (y - oldMeanY);
        sumXY += (x - oldMeanX) * (y - meanY);

        mapValue.putDouble(valueIndex, meanX);
        mapValue.putDouble(valueIndex + 1, sumX);
        mapValue.putDouble(valueIndex + 2, meanY);
        mapValue.putDouble(valueIndex + 3, sumY);
        mapValue.putDouble(valueIndex + 4, sumXY);
        mapValue.addLong(valueIndex + 5, 1L);
    }
}
