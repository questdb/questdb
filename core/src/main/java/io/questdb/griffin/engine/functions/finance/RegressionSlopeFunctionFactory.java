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

package io.questdb.griffin.engine.functions.finance;

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
import io.questdb.griffin.engine.functions.groupby.AbstractCovarGroupByFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class RegressionSlopeFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "regr_slope(DD)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new RegressionSlopeFunction(args.getQuick(0), args.getQuick(1));
    }


    private static class RegressionSlopeFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
        protected final Function xFunction;
        protected final Function yFunction;
        protected int valueIndex;

        public RegressionSlopeFunction(@NotNull Function arg0, @NotNull Function arg1) {
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
            mapValue.putLong(valueIndex + 4, 0);

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


        @Override
        public double getDouble(Record rec) {
            long count = rec.getLong(valueIndex + 4);

            if (count == 0) {
                return Double.NaN;
            }

            double sumXY = rec.getDouble(valueIndex + 3);
            double sumX = rec.getDouble(valueIndex + 2);

            if (sumX == 0) {
                return Double.NaN;
            }

            return sumXY / sumX;
        }

        @Override
        public Function getLeft() {
            return xFunction;
        }

        @Override
        public Function getRight() {
            return yFunction;
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public String getName() {
            return "regr_slope";
        }

        protected void aggregate(MapValue mapValue, double x, double y) {
            double meanX = mapValue.getDouble(valueIndex);
            double meanY = mapValue.getDouble(valueIndex + 1);
            double sumX = mapValue.getDouble(valueIndex + 2);
            double sumXY = mapValue.getDouble(valueIndex + 3);
            long count = mapValue.getLong(valueIndex + 4) + 1;

            double oldMeanX = meanX;
            meanX += (x - meanX) / count;
            meanY += (y - meanY) / count;
            sumX += (x - oldMeanX) * (x - meanX);
            sumXY += (x - oldMeanX) * (y - meanY);

            mapValue.putDouble(valueIndex, meanX);
            mapValue.putDouble(valueIndex + 1, meanY);
            mapValue.putDouble(valueIndex + 2, sumX);
            mapValue.putDouble(valueIndex + 3, sumXY);
            mapValue.putLong(valueIndex + 4, count);
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            double srcMeanX = srcValue.getDouble(valueIndex);
            double srcMeanY = srcValue.getDouble(valueIndex + 1);
            double srcSumX = srcValue.getDouble(valueIndex + 2);
            double srcSumXY = srcValue.getDouble(valueIndex + 3);
            long srcCount = srcValue.getLong(valueIndex + 4);
            double destMeanX = destValue.getDouble(valueIndex);
            double destMeanY = destValue.getDouble(valueIndex + 1);
            double destSumX = destValue.getDouble(valueIndex + 2);
            double destSumXY = destValue.getDouble(valueIndex + 3);
            long destCount = destValue.getLong(valueIndex + 4);

            long totalCount = srcCount + destCount;
            double srcRatio = (double) srcCount / totalCount;
            double destRatio = (double) destCount / totalCount;
            destMeanX = (srcMeanX * srcRatio) + (destMeanX * destRatio);
            destMeanY = (srcMeanY * srcRatio) + (destMeanY * destRatio);

            destValue.putDouble(valueIndex, destMeanX);
            destValue.putDouble(valueIndex + 1, destMeanY);
            destValue.putDouble(valueIndex + 2, srcSumX + destSumX);
            destValue.putDouble(valueIndex + 3, srcSumXY + destSumXY);
            destValue.putLong(valueIndex + 4, totalCount);
        }
    }

}
