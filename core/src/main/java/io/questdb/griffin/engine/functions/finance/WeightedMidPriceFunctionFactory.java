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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class WeightedMidPriceFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "wmid(DDDD)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions,
                                CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return new WeightedMidPriceFunction(args.getQuick(0), args.getQuick(1), args.getQuick(2), args.getQuick(3));
    }

    private static class WeightedMidPriceFunction extends DoubleFunction implements BinaryFunction {
        private final Function bid;
        private final Function bidSize;
        private final Function ask;
        private final Function askSize;

        public WeightedMidPriceFunction(Function bid, Function bidSize, Function ask, Function askSize) {
            this.bid = bid;
            this.bidSize = bidSize;
            this.ask = ask;
            this.askSize = askSize;
        }

        @Override
        public double getDouble(Record rec) {
            final double b = bid.getDouble(rec);
            final double bs = Numbers.intToDouble(bidSize.getInt(rec));
            final double a = bid.getDouble(rec);
            final double as = Numbers.intToDouble(askSize.getInt(rec));

            if (Numbers.isNull(b) || Numbers.isNull(bs) || Numbers.isNull(a) || Numbers.isNull(as)) {
                return Double.NaN;
            }

            double imbalance = bs / (bs + as);

            return a * imbalance + b * (1 - imbalance);
        }

        @Override
        public Function getLeft() {
            return bid;
        }

        @Override
        public Function getRight() {
            return ask;
        }

        @Override
        public String getName() {
            return "weighted_mid";
        }
    }
}