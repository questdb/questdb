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

public class RegressionInterceptFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "regr_intercept(DD)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new RegressionInterceptFunction(args.getQuick(0), args.getQuick(1));
    }


    private static class RegressionInterceptFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
        protected final Function xFunc;
        protected final Function yFunc;
        protected int valueIndex;

        public RegressionInterceptFunction(@NotNull Function arg0, @NotNull Function arg1) {
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
            mapValue.putLong(valueIndex + 4, 0);

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
            long count = rec.getLong(valueIndex + 4);

            if (count > 0) {
                double sumXY = rec.getDouble(valueIndex + 2);
                double covar = sumXY / count;

                double sumX = rec.getDouble(valueIndex + 3);
                double var = sumX / count;

                double slope = covar / var;

                double meanY = rec.getDouble(valueIndex);
                double meanX = rec.getDouble(valueIndex + 1);

                return meanY - meanX * slope;
            }
            return Double.NaN;
        }

        @Override
        public Function getLeft() {
            return yFunc;
        }

        @Override
        public String getName() {
            return "regr_intercept";
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
            columnTypes.add(ColumnType.LONG);
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
            long srcCount = srcValue.getLong(valueIndex + 4);

            double destMeanY = destValue.getDouble(valueIndex);
            double destMeanX = destValue.getDouble(valueIndex + 1);
            double destSumXY = destValue.getDouble(valueIndex + 2);
            double destSumX = destValue.getDouble(valueIndex + 3);
            long destCount = destValue.getLong(valueIndex + 4);

            long mergedCount = srcCount + destCount;
            double deltaY = destMeanY - srcMeanY;
            double deltaX = destMeanX - srcMeanX;

            // This is only valid when countA is much larger than countB.
            // If both are large and similar sizes, delta is not scaled down.
            // double mergedMean = srcMean + delta * ((double) destCount / mergedCount);

            // So we use this instead:
            double mergedMeanY = (srcCount * srcMeanY + destCount * destMeanY) / mergedCount;
            double mergedMeanX = (srcCount * srcMeanX + destCount * destMeanX) / mergedCount;
            double mergedSumXY = srcSumXY + destSumXY + (deltaY * deltaX) * (((double) srcCount * destCount) / mergedCount);
            double mergedSumX = srcSumX + destSumX + (deltaX * deltaX) * ((double) (srcCount * destCount) / mergedCount);

            destValue.putDouble(valueIndex, mergedMeanY);
            destValue.putDouble(valueIndex + 1, mergedMeanX);
            destValue.putDouble(valueIndex + 2, mergedSumXY);
            destValue.putDouble(valueIndex + 3, mergedSumX);
            destValue.putLong(valueIndex + 4, mergedCount);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putDouble(valueIndex, Double.NaN);
            mapValue.putDouble(valueIndex + 1, Double.NaN);
            mapValue.putDouble(valueIndex + 2, Double.NaN);
            mapValue.putDouble(valueIndex + 3, Double.NaN);
            mapValue.putLong(valueIndex + 4, 0);
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
            long count = mapValue.getLong(valueIndex + 4) + 1;

            double oldMeanY = meanY;
            double oldMeanX = meanX;
            meanY += (y - meanY) / count;
            meanX += (x - meanX) / count;
            sumXY += (y - oldMeanY) * (x - meanX);
            sumX += (x - meanX) * (x - oldMeanX);

            mapValue.putDouble(valueIndex, meanY);
            mapValue.putDouble(valueIndex + 1, meanX);
            mapValue.putDouble(valueIndex + 2, sumXY);
            mapValue.putDouble(valueIndex + 3, sumX);
            mapValue.addLong(valueIndex + 4, 1L);
        }
    }
}