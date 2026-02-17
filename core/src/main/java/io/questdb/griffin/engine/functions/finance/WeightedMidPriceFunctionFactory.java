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

package io.questdb.griffin.engine.functions.finance;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.QuaternaryFunction;
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
                                CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new WeightedMidPriceFunction(args.getQuick(0), args.getQuick(1), args.getQuick(2), args.getQuick(3));
    }

    private static class WeightedMidPriceFunction extends DoubleFunction implements QuaternaryFunction {
        private final Function askPrice;
        private final Function askSize;
        private final Function bidPrice;
        private final Function bidSize;

        // Argument order, e.g. `bidSize, bidPrice, askPrice, askSize`, follows the standard order commonly displayed on trading systems.
        public WeightedMidPriceFunction(Function bidSize, Function bidPrice, Function askPrice, Function askSize) {
            this.bidSize = bidSize;
            this.bidPrice = bidPrice;
            this.askPrice = askPrice;
            this.askSize = askSize;
        }

        @Override
        public double getDouble(Record rec) {
            final double bs = bidSize.getDouble(rec);
            final double bp = bidPrice.getDouble(rec);
            final double ap = askPrice.getDouble(rec);
            final double as = askSize.getDouble(rec);

            if (Numbers.isNull(bp) || Numbers.isNull(bs) || Numbers.isNull(ap) || Numbers.isNull(as)) {
                return Double.NaN;
            }

            double imbalance = bs / (bs + as);

            return ap * imbalance + bp * (1 - imbalance);
        }

        @Override
        public Function getFunc0() {
            return bidSize;
        }

        @Override
        public Function getFunc1() {
            return bidPrice;
        }

        @Override
        public Function getFunc2() {
            return askPrice;
        }

        @Override
        public Function getFunc3() {
            return askSize;
        }

        @Override
        public String getName() {
            return "wmid";
        }
    }
}