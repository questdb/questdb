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
 * The abstract class, in addition, provides a method to aggregate univariate statistics.
 * We use the B.P. Welford algorithm which works by first aggregating sum of squares Sxx = sum[(X - mean) ^ 2].
 * Computation of standard deviation and variance is then simple (e.g. variance = Sxx / (n - 1), standard deviation = sqrt(variance))
 *
 * @see <a href="https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm">Welford's algorithm</a>
 */
public abstract class AbstractStdDevGroupByFunction extends DoubleFunction implements GroupByFunction, UnaryFunction {
    protected final Function arg;
    protected int valueIndex;

    protected AbstractStdDevGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final double d = arg.getDouble(record);
        mapValue.putDouble(valueIndex, 0);
        mapValue.putDouble(valueIndex + 1, 0);
        mapValue.putLong(valueIndex + 2, 0);
        if (Numbers.isFinite(d)) {
            aggregate(mapValue, d);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final double d = arg.getDouble(record);
        if (Numbers.isFinite(d)) {
            aggregate(mapValue, d);
        }
    }

    @Override
    public Function getArg() {
        return arg;
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
        columnTypes.add(ColumnType.LONG);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    // Chan et al. [CGL82; CGL83]
    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        double srcMean = srcValue.getDouble(valueIndex);
        double srcSum = srcValue.getDouble(valueIndex + 1);
        long srcCount = srcValue.getLong(valueIndex + 2);

        double destMean = destValue.getDouble(valueIndex);
        double destSum = destValue.getDouble(valueIndex + 1);
        long destCount = destValue.getLong(valueIndex + 2);

        long mergedCount = srcCount + destCount;
        double delta = destMean - srcMean;

        // This is only valid when countA is much larger than countB.
        // If both are large and similar sizes, delta is not scaled down.
        // double mergedMean = srcMean + delta * ((double) destCount / mergedCount);

        // So we use this instead:
        double mergedMean = (srcCount * srcMean + destCount * destMean) / mergedCount;
        double mergedSum = srcSum + destSum + (delta * delta) * ((double) (srcCount * destCount) / mergedCount);

        destValue.putDouble(valueIndex, mergedMean);
        destValue.putDouble(valueIndex + 1, mergedSum);
        destValue.putLong(valueIndex + 2, mergedCount);
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

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }

    protected void aggregate(MapValue mapValue, double value) {
        double mean = mapValue.getDouble(valueIndex);
        double sum = mapValue.getDouble(valueIndex + 1);
        long count = mapValue.getLong(valueIndex + 2) + 1;

        double oldMean = mean;
        mean += (value - mean) / count;
        sum += (value - mean) * (value - oldMean);
        mapValue.putDouble(valueIndex, mean);
        mapValue.putDouble(valueIndex + 1, sum);
        mapValue.addLong(valueIndex + 2, 1L);
    }
}
