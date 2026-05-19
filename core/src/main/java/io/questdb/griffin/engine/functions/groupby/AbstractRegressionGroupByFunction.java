/*+*****************************************************************************
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
 * Shared base for SQL regression aggregates over (Y, X) pairs. Maintains the
 * six Welford state slots required to derive any standard regression function:
 * <pre>
 *   slot 0  meanY     running mean of Y
 *   slot 1  sumY      Syy = sum((y - meanY)^2)   // Welford M2 for Y
 *   slot 2  meanX     running mean of X
 *   slot 3  sumX      Sxx = sum((x - meanX)^2)   // Welford M2 for X
 *   slot 4  sumXY     Sxy = sum((x - meanX)(y - meanY))
 *   slot 5  count     number of finite (y, x) pairs
 * </pre>
 * Note: the slot names {@code sumY} / {@code sumX} are kept for parity with
 * {@code CorrGroupByFunction}, but they hold Welford's second moments (sum of
 * squared deviations from the running mean), not arithmetic sums of {@code y}
 * / {@code x}.
 * <p>
 * Subclasses implement {@link #getDouble(Record)} to project the desired final
 * value (slope, intercept, r-squared, etc.) from this state.
 * <p>
 * Layout matches {@code CorrGroupByFunction} so the same parallel merge
 * formulas (Chan et al.) apply uniformly. The single-row update is Welford's
 * online algorithm.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online">Welford's algorithm</a>
 */
public abstract class AbstractRegressionGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
    protected final Function xFunc;
    protected final Function yFunc;
    protected int valueIndex;

    protected AbstractRegressionGroupByFunction(@NotNull Function arg0, @NotNull Function arg1) {
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
        mapValue.putDouble(valueIndex + 3, 0);
        mapValue.putDouble(valueIndex + 4, 0);
        mapValue.putLong(valueIndex + 5, 0);

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
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.LONG);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    // Parallel pairwise merge per Chan, Golub & LeVeque (1982),
    // "Updating Formulae and a Pairwise Algorithm for Computing Sample Variances".
    // See https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcCount = srcValue.getLong(valueIndex + 5);
        if (srcCount == 0) {
            // Source partial has no finite (y, x) pairs; nothing to contribute.
            // Skipping avoids 0/0 = NaN in the Chan formula when destCount is
            // also 0, which would otherwise poison the destination doubles and
            // leak NaN into the merged result once a real-data partial arrives.
            return;
        }
        long destCount = destValue.getLong(valueIndex + 5);
        if (destCount == 0) {
            // Destination is empty; copy source state directly.
            destValue.putDouble(valueIndex, srcValue.getDouble(valueIndex));
            destValue.putDouble(valueIndex + 1, srcValue.getDouble(valueIndex + 1));
            destValue.putDouble(valueIndex + 2, srcValue.getDouble(valueIndex + 2));
            destValue.putDouble(valueIndex + 3, srcValue.getDouble(valueIndex + 3));
            destValue.putDouble(valueIndex + 4, srcValue.getDouble(valueIndex + 4));
            destValue.putLong(valueIndex + 5, srcCount);
            return;
        }

        double srcMeanY = srcValue.getDouble(valueIndex);
        double srcSumY = srcValue.getDouble(valueIndex + 1);
        double srcMeanX = srcValue.getDouble(valueIndex + 2);
        double srcSumX = srcValue.getDouble(valueIndex + 3);
        double srcSumXY = srcValue.getDouble(valueIndex + 4);

        double destMeanY = destValue.getDouble(valueIndex);
        double destSumY = destValue.getDouble(valueIndex + 1);
        double destMeanX = destValue.getDouble(valueIndex + 2);
        double destSumX = destValue.getDouble(valueIndex + 3);
        double destSumXY = destValue.getDouble(valueIndex + 4);

        // The simpler Welford-pairwise mean update,
        //   double mergedMean = srcMean + delta * ((double) destCount / mergedCount);
        // is only valid when srcCount is much larger than destCount. When both
        // partials are comparable the delta is not scaled down enough and the
        // mean drifts, so we use the explicit Chan pairwise form below.
        long mergedCount = srcCount + destCount;
        double deltaY = destMeanY - srcMeanY;
        double deltaX = destMeanX - srcMeanX;

        // Cast first to double so srcCount*destCount cannot overflow long
        // when both partials are very large.
        double weighting = ((double) srcCount * destCount) / mergedCount;
        double mergedMeanY = (srcCount * srcMeanY + destCount * destMeanY) / mergedCount;
        double mergedSumY = srcSumY + destSumY + (deltaY * deltaY) * weighting;
        double mergedMeanX = (srcCount * srcMeanX + destCount * destMeanX) / mergedCount;
        double mergedSumX = srcSumX + destSumX + (deltaX * deltaX) * weighting;
        double mergedSumXY = srcSumXY + destSumXY + (deltaX * deltaY) * weighting;

        destValue.putDouble(valueIndex, mergedMeanY);
        destValue.putDouble(valueIndex + 1, mergedSumY);
        destValue.putDouble(valueIndex + 2, mergedMeanX);
        destValue.putDouble(valueIndex + 3, mergedSumX);
        destValue.putDouble(valueIndex + 4, mergedSumXY);
        destValue.putLong(valueIndex + 5, mergedCount);
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
        return true;
    }

    protected void aggregate(MapValue mapValue, double y, double x) {
        double meanY = mapValue.getDouble(valueIndex);
        double sumY = mapValue.getDouble(valueIndex + 1);
        double meanX = mapValue.getDouble(valueIndex + 2);
        double sumX = mapValue.getDouble(valueIndex + 3);
        double sumXY = mapValue.getDouble(valueIndex + 4);
        long count = mapValue.getLong(valueIndex + 5) + 1;

        double oldMeanY = meanY;
        meanY += (y - meanY) / count;
        sumY += (y - meanY) * (y - oldMeanY);
        double oldMeanX = meanX;
        meanX += (x - meanX) / count;
        sumX += (x - meanX) * (x - oldMeanX);
        sumXY += (y - oldMeanY) * (x - meanX);

        mapValue.putDouble(valueIndex, meanY);
        mapValue.putDouble(valueIndex + 1, sumY);
        mapValue.putDouble(valueIndex + 2, meanX);
        mapValue.putDouble(valueIndex + 3, sumX);
        mapValue.putDouble(valueIndex + 4, sumXY);
        mapValue.addLong(valueIndex + 5, 1L);
    }
}
