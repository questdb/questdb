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
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * Expects a target size, and then pairs of (order size, order amount).
 */
public class LevelTwoPriceFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "l2price(LV)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (args.size() % 2 == 0) {
            throw SqlException.position(argPositions.getLast()).put("l2price requires an odd number of arguments.");
        }

        return new L2PriceFunction(new ObjList<>(args));
    }

    private static class L2PriceFunction extends DoubleFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public L2PriceFunction(ObjList<Function> args) {
            this.args = args;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public double getDouble(Record rec) {
            final long t = args.getQuick(0).getLong(rec);

            double ta = 0; // target accumulator
            double pa = 0; // result accumulator
            double rt = t; // reduced target
            // expect (size, value) pairs
            for (int i = 1, n = args.size(); i < n; i += 2) {
                // add size to acc
                ta += args.getQuick(i).getDouble(rec);

                // if target is met, return the aggregate value
                if (ta > t) {

                    // equation is
                    // ((size[0] * value[0]) + (size[n] * value[n]) + ((target - size[0] - size[n]) * value[n+1])) / target
                    // ((ra)             + (rt * value[n+1])) / t
                    if (i == 1) {
                        return args.getQuick(i + 1).getDouble(rec);
                    }

                    // sum up (size[n] * value[n]) pairs and sizes from target
                    for (int j = 1; j < i; j += 2) {
                        final double size = args.getQuick(j).getDouble(rec);
                        final double value = args.getQuick(j+1).getDouble(rec);
                        pa += size * value; // sum up the (size[n] * value[n] pairs)
                        rt -= size; // subtract size[n] from the target
                    }

                    // get final price by partially filling against the last bin
                    rt *= args.getQuick(i+1).getDouble(rec);

                    // get average price
                    return (pa + rt) / t;
                }

            }

            // if never exceeded the target, then no price, since it can't be fulfilled
            return 0;
        }
    }

}
