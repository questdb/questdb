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
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;


public class TwapGroupByFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "twap(DN)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new TwapGroupByFunction(args.getQuick(0), args.getQuick(1));
    }

    public static class TwapGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
        protected final Function priceFunction;
        protected final Function timestampFunction;
        protected int valueIndex;

        protected TwapGroupByFunction(@NotNull Function arg0, @NotNull Function arg1) {
            this.priceFunction = arg0;
            this.timestampFunction = arg1;
        }

        // Map Values
        // [ sum_j(T_j * P_j), sum_j(T_j), prevTimestamp ]
        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final double price = priceFunction.getDouble(record);
            final long timestamp = timestampFunction.getTimestamp(record);
            mapValue.putDouble(valueIndex, 0);
            mapValue.putLong(valueIndex + 1, 0);
            mapValue.putLong(valueIndex + 2, Long.MIN_VALUE);

            if (Numbers.isFinite(price) && Numbers.isFinite(timestamp)) {
                aggregate(mapValue, price, timestamp);
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            final double price = priceFunction.getDouble(record);
            final long timestamp = timestampFunction.getTimestamp(record);
            if (Numbers.isFinite(price) && Numbers.isFinite(timestamp)) {
                aggregate(mapValue, price, timestamp);
            }
        }

        @Override
        public double getDouble(Record rec) {
            double sumWeightedPrices = rec.getDouble(valueIndex);
            long sumTimeDeltas = rec.getTimestamp(valueIndex + 1);

            if (sumTimeDeltas == 0) {
                return Double.NaN;
            }

            return sumWeightedPrices / sumTimeDeltas;
        }

        @Override
        public Function getLeft() {
            return priceFunction;
        }

        @Override
        public String getName() {
            return "twap";
        }

        @Override
        public Function getRight() {
            return timestampFunction;
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
            columnTypes.add(ColumnType.LONG);
            columnTypes.add(ColumnType.TIMESTAMP);
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putDouble(valueIndex, 0);
            mapValue.putDouble(valueIndex + 1, 0);
            mapValue.putTimestamp(valueIndex + 2, Long.MIN_VALUE);
        }

        @Override
        public boolean supportsParallelism() {
            return false;
        }

        protected void aggregate(MapValue mapValue, double price, long timestamp) {
            double sumWeightedPrices = mapValue.getDouble(valueIndex);
            long sumTimeDeltas = mapValue.getLong(valueIndex + 1);
            long prevTimestamp = mapValue.getTimestamp(valueIndex + 2);

            long timestampDelta;
            double newSumWeightedPrices;
            long newSumTimeDeltas;

            if (prevTimestamp > timestamp) {
                throw new RuntimeException("twap requires timestamps to be in ascending order");
            }

            if (prevTimestamp == Long.MIN_VALUE) {
                newSumWeightedPrices = sumWeightedPrices + price;
                newSumTimeDeltas = 0;
            } else {
                timestampDelta = timestamp - prevTimestamp;
                newSumWeightedPrices = sumWeightedPrices + (timestampDelta * price);
                newSumTimeDeltas = sumTimeDeltas + timestampDelta;
            }

            mapValue.putDouble(valueIndex, newSumWeightedPrices);
            mapValue.putLong(valueIndex + 1, newSumTimeDeltas);
            mapValue.putTimestamp(valueIndex + 2, timestamp);
        }
    }
}

