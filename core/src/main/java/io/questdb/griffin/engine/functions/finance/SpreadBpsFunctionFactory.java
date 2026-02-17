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
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class SpreadBpsFunctionFactory implements FunctionFactory {

    // (bid, ask)
    @Override
    public String getSignature() {
        return "spread_bps(DD)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new SpreadBpsFunction(args.getQuick(0), args.getQuick(1));
    }

    private static class SpreadBpsFunction extends DoubleFunction implements BinaryFunction {
        private final Function ask;
        private final Function bid;

        public SpreadBpsFunction(Function bid, Function ask) {
            this.bid = bid;
            this.ask = ask;
        }

        @Override
        public double getDouble(Record rec) {
            final double b = bid.getDouble(rec);
            final double a = ask.getDouble(rec);

            return FinanceUtils.spread(b, a) / FinanceUtils.mid(b, a) * 10_000;
        }

        @Override
        public Function getLeft() {
            return bid;
        }

        @Override
        public String getName() {
            return "spread_bps";
        }

        @Override
        public Function getRight() {
            return ask;
        }
    }
}
