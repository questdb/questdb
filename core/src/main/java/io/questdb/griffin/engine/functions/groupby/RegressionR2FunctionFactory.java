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

public class RegressionR2FunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "regr_r2(DD)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new RegressionR2Function(args.getQuick(0), args.getQuick(1));
    }


    private static class RegressionR2Function extends DoubleFunction implements GroupByFunction, BinaryFunction {
        protected final Function xFunc;
        protected final Function yFunc;
        protected int valueIndex;

        public RegressionR2Function(@NotNull Function arg0, @NotNull Function arg1) {
            this.yFunc = arg0;
            this.xFunc = arg1;
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final double y = yFunc.getDouble(record);
            final double x = xFunc.getDouble(record);
            mapValue.putDouble(valueIndex, 0);      // meanY
            mapValue.putDouble(valueIndex + 1, 0);  // meanX
            mapValue.putDouble(valueIndex + 2, 0);  // sumXY
            mapValue.putDouble(valueIndex + 3, 0);  // sumX
            mapValue.putDouble(valueIndex + 4, 0);  // sumY (sum of squared Y deviations)
            mapValue.putLong(valueIndex + 5, 0);    // count

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
            long count = rec.getLong(valueIndex + 5);

            if (count > 1) {
                double sumY = rec.getDouble(valueIndex + 4);  // sum of Y squared deviations
                double sumXY = rec.getDouble(valueIndex + 2); // covariance numerator
                double sumX = rec.getDouble(valueIndex + 3);  // X variance numerator

                // Avoid division by zero
                if (sumX == 0 || sumY == 0) {
                    return Double.NaN;
                }

                // R² = 1 - (SS_res / SS_tot)
                // where SS_res = sumY - (sumXY²/sumX) and SS_tot = sumY
                double ssRes = sumY - (sumXY * sumXY / sumX);
                double ssTot = sumY;

                // Ensure we don't have negative R² due to numerical errors
                if (ssRes < 0) {
                    ssRes = 0;
                }

                return 1.0 - (ssRes / ssTot);
            }
            return Double.NaN;
        }

        @Override
        public Function getLeft() {
            return yFunc;
        }

        @Override
        public String getName() {
            return "regr_r2";
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
            columnTypes.add(ColumnType.DOUBLE); // meanY
            columnTypes.add(ColumnType.DOUBLE); // meanX
            columnTypes.add(ColumnType.DOUBLE); // sumXY
            columnTypes.add(ColumnType.DOUBLE); // sumX
            columnTypes.add(ColumnType.DOUBLE); // sumY
            columnTypes.add(ColumnType.LONG);   // count
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public boolean isThreadSafe() {
            return BinaryFunction.super.isThreadSafe();
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            double srcMeanY = srcValue.getDouble(valueIndex);
            double srcMeanX = srcValue.getDouble(valueIndex + 1);
            double srcSumXY = srcValue.getDouble(valueIndex + 2);
            double srcSumX = srcValue.getDouble(valueIndex + 3);
            double srcSumY = srcValue.getDouble(valueIndex + 4);
            long srcCount = srcValue.getLong(valueIndex + 5);

            double destMeanY = destValue.getDouble(valueIndex);
            double destMeanX = destValue.getDouble(valueIndex + 1);
            double destSumXY = destValue.getDouble(valueIndex + 2);
            double destSumX = destValue.getDouble(valueIndex + 3);
            double destSumY = destValue.getDouble(valueIndex + 4);
            long destCount = destValue.getLong(valueIndex + 5);

            long mergedCount = srcCount + destCount;
            double deltaY = destMeanY - srcMeanY;
            double deltaX = destMeanX - srcMeanX;

            double weighting = ((double) srcCount * destCount) / mergedCount;
            double mergedMeanY = (srcCount * srcMeanY + destCount * destMeanY) / mergedCount;
            double mergedMeanX = (srcCount * srcMeanX + destCount * destMeanX) / mergedCount;
            double mergedSumXY = srcSumXY + destSumXY + (deltaY * deltaX) * weighting;
            double mergedSumX = srcSumX + destSumX + (deltaX * deltaX) * weighting;
            double mergedSumY = srcSumY + destSumY + (deltaY * deltaY) * weighting;

            destValue.putDouble(valueIndex, mergedMeanY);
            destValue.putDouble(valueIndex + 1, mergedMeanX);
            destValue.putDouble(valueIndex + 2, mergedSumXY);
            destValue.putDouble(valueIndex + 3, mergedSumX);
            destValue.putDouble(valueIndex + 4, mergedSumY);
            destValue.putLong(valueIndex + 5, mergedCount);
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
            double meanX = mapValue.getDouble(valueIndex + 1);
            double sumXY = mapValue.getDouble(valueIndex + 2);
            double sumX = mapValue.getDouble(valueIndex + 3);
            double sumY = mapValue.getDouble(valueIndex + 4);
            long count = mapValue.getLong(valueIndex + 5) + 1;

            double oldMeanY = meanY;
            double oldMeanX = meanX;
            meanY += (y - meanY) / count;
            meanX += (x - meanX) / count;
            sumXY += (y - oldMeanY) * (x - meanX);
            sumX += (x - meanX) * (x - oldMeanX);
            sumY += (y - meanY) * (y - oldMeanY);

            mapValue.putDouble(valueIndex, meanY);
            mapValue.putDouble(valueIndex + 1, meanX);
            mapValue.putDouble(valueIndex + 2, sumXY);
            mapValue.putDouble(valueIndex + 3, sumX);
            mapValue.putDouble(valueIndex + 4, sumY);
            mapValue.addLong(valueIndex + 5, 1L);
        }
    }

}
