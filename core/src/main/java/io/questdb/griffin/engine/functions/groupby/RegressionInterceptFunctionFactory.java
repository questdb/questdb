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
        protected final Function yFunc;
        protected final Function xFunc;
        protected int valueIndex;

        public RegressionInterceptFunction(@NotNull Function arg0, @NotNull Function arg1) {
            this.yFunc = arg0;
            this.xFunc = arg1;

        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final double x = xFunc.getDouble(record);
            final double y = yFunc.getDouble(record);
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
            if (count < 2) {
                return Double.NaN;
            }

            double sum_x = rec.getDouble(valueIndex);
            double sum_y = rec.getDouble(valueIndex + 1);
            double sum_xy = rec.getDouble(valueIndex + 2);
            double sum_x_squared = rec.getDouble(valueIndex + 3);
            double slope = (count * sum_xy - sum_x * sum_y) / (count * sum_x_squared - sum_x * sum_x);
            double avg_x = sum_x / count;
            double avg_y = sum_y / count;

            return avg_y - avg_x * slope;
        }

        @Override
        public int getValueIndex() {
            return valueIndex;
        }

        @Override
        public void initValueIndex(int valueIndex) {
            this.valueIndex = valueIndex;
        }

        // regr_slope is covar_pop / var_pop
        // regr_intercept = avg(y) - regr_slope(y, x) * avg(x)
        // Map is [ sumY, sumX, sumXY, sumYSquared, count ]
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
        public boolean isThreadSafe() {
            return BinaryFunction.super.isThreadSafe();
        }

        @Override
        public boolean supportsParallelism() {
            return true;
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            double srcSumY = srcValue.getDouble(valueIndex);
            double srcSumX = srcValue.getDouble(valueIndex + 1);
            double srcSumXY = srcValue.getDouble(valueIndex + 2);
            double srcSumYSquared = srcValue.getDouble(valueIndex + 3);
            long srcCount = srcValue.getLong(valueIndex + 4);

            double destSumY = destValue.getDouble(valueIndex);
            double destSumX = destValue.getDouble(valueIndex + 1);
            double destSumXY = destValue.getDouble(valueIndex + 2);
            double destSumYSquared = destValue.getDouble(valueIndex + 3);
            long destCount = destValue.getLong(valueIndex + 4);

            destValue.putDouble(valueIndex, srcSumY + destSumY);
            destValue.putDouble(valueIndex + 1, srcSumX + destSumX);
            destValue.putDouble(valueIndex + 2, srcSumXY + destSumXY);
            destValue.putDouble(valueIndex + 3, srcSumYSquared + destSumYSquared);
            destValue.putLong(valueIndex + 4, srcCount + destCount);
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
        public boolean isConstant() {
            return false;
        }

        @Override
        public String getName() {
            return "regr_slope";
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putDouble(valueIndex, Double.NaN);
            mapValue.putDouble(valueIndex + 1, Double.NaN);
            mapValue.putDouble(valueIndex + 2, Double.NaN);
            mapValue.putDouble(valueIndex + 3, Double.NaN);
            mapValue.putLong(valueIndex + 4, 0);
        }

        protected void aggregate(MapValue mapValue, double y, double x) {
            double sumY = mapValue.getDouble(valueIndex);
            double sumX = mapValue.getDouble(valueIndex + 1);
            double sumXY = mapValue.getDouble(valueIndex + 2);
            double sumYSquared = mapValue.getDouble(valueIndex + 3);
            long count = mapValue.getLong(valueIndex + 4);

            mapValue.putDouble(valueIndex, sumY + y);
            mapValue.putDouble(valueIndex + 1, sumX + x);
            mapValue.putDouble(valueIndex + 2, sumXY + x * y);
            mapValue.putDouble(valueIndex + 3, sumYSquared + y * y);
            mapValue.putLong(valueIndex + 4, count + 1);
        }
    }

}