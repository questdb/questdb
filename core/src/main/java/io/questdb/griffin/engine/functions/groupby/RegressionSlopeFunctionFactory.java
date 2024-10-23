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
            final double y = yFunction.getDouble(record);
            final double x = xFunction.getDouble(record);
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
            final double y = xFunction.getDouble(record);
            final double x = yFunction.getDouble(record);
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

            return (count * sum_xy - sum_x * sum_y) / (count * sum_x_squared - sum_x * sum_x);
        }

        @Override
        public Function getLeft() {
            return xFunction;
        }

        @Override
        public String getName() {
            return "regr_slope";
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
            double sum_x = srcValue.getDouble(valueIndex);
            double sum_y = srcValue.getDouble(valueIndex + 1);
            double sum_xy = srcValue.getDouble(valueIndex + 2);
            double sum_x_squared = srcValue.getDouble(valueIndex + 3);
            long count = srcValue.getLong(valueIndex + 4);

            double sum_x2 = destValue.getDouble(valueIndex);
            double sum_y2 = destValue.getDouble(valueIndex + 1);
            double sum_xy2 = destValue.getDouble(valueIndex + 2);
            double sum_x2_squared = destValue.getDouble(valueIndex + 3);
            long count2 = destValue.getLong(valueIndex + 4);

            destValue.putDouble(valueIndex, sum_x + sum_x2);
            destValue.putDouble(valueIndex + 1, sum_y + sum_y2);
            destValue.putDouble(valueIndex + 2, sum_xy + sum_xy2);
            destValue.putDouble(valueIndex + 3, sum_x_squared + sum_x2_squared);
            destValue.putLong(valueIndex + 4, count + count2);
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
            double sum_x = mapValue.getDouble(valueIndex);
            double sum_y = mapValue.getDouble(valueIndex + 1);
            double sum_xy = mapValue.getDouble(valueIndex + 2);
            double sum_x_squared = mapValue.getDouble(valueIndex + 3);
            long count = mapValue.getLong(valueIndex + 4);

            mapValue.putDouble(valueIndex, sum_x + x);
            mapValue.putDouble(valueIndex + 1, sum_y + y);
            mapValue.putDouble(valueIndex + 2, sum_xy + x * y);
            mapValue.putDouble(valueIndex + 3, sum_x_squared + x * x);
            mapValue.putLong(valueIndex + 4, count + 1);
        }
    }

}
