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
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.QuinaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Timestamps;
import org.jetbrains.annotations.NotNull;


// tracks [ cumulative_volume, cumulative_weighted_price, currentDay ]


// against demo trades

/**
 * select timestamp, symbol, vwap(timestamp,min_price, max_price, closing_price, volume)
 * from (
 * select timestamp, symbol, min(price) min_price, max(price) max_price, last(price) closing_price, sum(amount) volume
 * from trades
 * where symbol = 'ETH-USD'
 * sample by 5m
 * )
 * group by timestamp, symbol
 * order by timestamp asc;
 */
public class VwapDoubleGroupByFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "vwap(NDDDD)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new VwapDoubleGroupByFunction(args.getQuick(0), args.getQuick(1), args.getQuick(2), args.getQuick(3), args.getQuick(4));
    }

    private static class VwapDoubleGroupByFunction extends DoubleFunction implements GroupByFunction, QuinaryFunction {
        private final Function closePriceFunc; // close price of time period
        private final Function maxPriceFunc; // low price of time period
        private final Function minPriceFunc; // high price of time period
        private final Function timestampFunc; // timestamp
        private final Function volumeFunc; // trading volume in time period
        private long nextDay = 0;
        private int valueIndex;
        private double volume = Double.NaN;
        private double vwap = Double.NaN;

        public VwapDoubleGroupByFunction(@NotNull Function timestampFunc, @NotNull Function minPriceFunc, @NotNull Function maxPriceFunc, @NotNull Function closePriceFunc, @NotNull Function volumeFunc) {
            this.timestampFunc = timestampFunc;
            this.minPriceFunc = minPriceFunc;
            this.maxPriceFunc = maxPriceFunc;
            this.closePriceFunc = closePriceFunc;
            this.volumeFunc = volumeFunc;
        }

        @Override
        public void clear() {
            this.vwap = Double.NaN;
            this.volume = Double.NaN;
            this.nextDay = 0;
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final long timestamp = timestampFunc.getTimestamp(record);
            final double min = minPriceFunc.getDouble(record);
            final double max = maxPriceFunc.getDouble(record);
            final double close = closePriceFunc.getDouble(record);
            final double volume = volumeFunc.getDouble(record);
            long day = Timestamps.floorDD(timestamp);

            if (timestamp >= nextDay) {
                // reset values
                this.vwap = Double.NaN;
                this.volume = Double.NaN;
                this.nextDay = Timestamps.addDays(day, 1);
            }

            if (Numbers.isFinite(min) && Numbers.isFinite(max) && Numbers.isFinite(close) && Numbers.isFinite(volume) && volume > 0.0d) {
                if (!Numbers.isFinite(this.volume) && !Numbers.isFinite(this.vwap)) {
                    this.vwap = ((min + max + close) / 3.0);
                    this.volume = volume;
                } else {
                    double cumulativePrice = this.vwap * this.volume;
                    cumulativePrice += (((min + max + close) / 3.0) * volume);
                    this.volume += volume;
                    this.vwap = cumulativePrice / this.volume;
                }
                mapValue.putDouble(valueIndex, this.vwap);
                mapValue.putDouble(valueIndex + 1, this.volume);
                return;
            }
            setNull(mapValue);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            throw new RuntimeException();
        }

        @Override
        public double getDouble(Record rec) {
            return rec.getDouble(valueIndex);
        }

        @Override
        public Function getFunc0() {
            return timestampFunc;
        }

        @Override
        public Function getFunc1() {
            return minPriceFunc;
        }

        @Override
        public Function getFunc2() {
            return maxPriceFunc;
        }

        @Override
        public Function getFunc3() {
            return closePriceFunc;
        }

        @Override
        public Function getFunc4() {
            return volumeFunc;
        }

        @Override
        public String getName() {
            return "vwap";
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
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public boolean isThreadSafe() {
            return QuinaryFunction.super.isThreadSafe();
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putDouble(valueIndex, Double.NaN);
            mapValue.putDouble(valueIndex + 1, Double.NaN);
        }

        @Override
        public boolean supportsParallelism() {
            return false;
        }

    }
}
