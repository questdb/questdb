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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class CorrGroupByFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "corr(DD)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new CorrGroupByFunction(args.getQuick(0), args.getQuick(1));
    }

    /**
     * We use an algorithm similar to B.P. Welford's which works by first aggregating sum of squares of
     * independent and dependent variables Sxx = sum[(X - mean_x) ^ 2], Syy = sum[(Y - mean_y) ^ 2], Sxy = sum[(X - mean_x) * (Y - mean_y)].
     * Computation of correlation is then simple, e.g. correlation = Sxy / sqrt(Sxx * Syy)
     *
     * @see <a href="https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online">Welford's algorithm</a>
     */
    private static class CorrGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
        protected final Function xFunc;
        protected final Function yFunc;
        protected int valueIndex;

        protected CorrGroupByFunction(@NotNull Function arg0, @NotNull Function arg1) {
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
        public double getDouble(Record rec) {
            double sumY = rec.getDouble(valueIndex + 1);
            double sumX = rec.getDouble(valueIndex + 3);
            double sumXY = rec.getDouble(valueIndex + 4);
            double count = rec.getLong(valueIndex + 5);
            if (count <= 0) {
                return Double.NaN;
            }
            if (sumY == 0 || sumX == 0) {
                return Double.NaN;
            }
            return sumXY / Math.sqrt(sumY * sumX);
        }

        @Override
        public Function getLeft() {
            return yFunc;
        }

        @Override
        public String getName() {
            return "corr";
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

        // Chan et al. [CGL82; CGL83]
        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            double srcMeanY = srcValue.getDouble(valueIndex);
            double srcSumY = srcValue.getDouble(valueIndex + 1);
            double srcMeanX = srcValue.getDouble(valueIndex + 2);
            double srcSumX = srcValue.getDouble(valueIndex + 3);
            double srcSumXY = srcValue.getDouble(valueIndex + 4);
            long srcCount = srcValue.getLong(valueIndex + 5);

            double destMeanY = destValue.getDouble(valueIndex);
            double destSumY = destValue.getDouble(valueIndex + 1);
            double destMeanX = destValue.getDouble(valueIndex + 2);
            double destSumX = destValue.getDouble(valueIndex + 3);
            double destSumXY = destValue.getDouble(valueIndex + 4);
            long destCount = destValue.getLong(valueIndex + 5);

            long mergedCount = srcCount + destCount;
            double deltaY = destMeanY - srcMeanY;
            double deltaX = destMeanX - srcMeanX;

            // This is only valid when countA is much larger than countB.
            // If both are large and similar sizes, delta is not scaled down.
            // double mergedMean = srcMean + delta * ((double) destCount / mergedCount);

            // So we use this instead:
            double mergedMeanY = (srcCount * srcMeanY + destCount * destMeanY) / mergedCount;
            double mergedSumY = srcSumY + destSumY + (deltaY * deltaY) * ((double) (srcCount * destCount) / mergedCount);
            double mergedMeanX = (srcCount * srcMeanX + destCount * destMeanX) / mergedCount;
            double mergedSumX = srcSumX + destSumX + (deltaX * deltaX) * ((double) (srcCount * destCount) / mergedCount);
            double mergedSumXY = srcSumXY + destSumXY + (deltaX * deltaY) * ((double) (srcCount * destCount) / mergedCount);

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

        // mean_x, sum_x, mean_y, sum_y, sum_xy
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
}
