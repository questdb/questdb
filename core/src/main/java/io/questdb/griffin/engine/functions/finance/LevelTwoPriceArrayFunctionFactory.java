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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class LevelTwoPriceArrayFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "l2price(DD[]D[])";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        try {
            assert args.size() == 3 : "args.size() should be 3, but got " + args.size();
            for (int i = 1; i < 3; i++) {
                if (ColumnType.decodeArrayDimensionality(args.getQuick(i).getType()) != 1) {
                    throw SqlException.$(argPositions.getQuick(i), "not a one-dimensional array");
                }
            }
            return new LevelTwoPriceArrayFunction(
                    args.getQuick(0),
                    args.getQuick(1),
                    argPositions.getQuick(1),
                    args.getQuick(2)
            );
        } catch (Throwable e) {
            Misc.freeObjList(args);
            throw e;
        }
    }

    private static class LevelTwoPriceArrayFunction extends DoubleFunction {

        private final int pricesArgPos;
        private Function pricesArg;
        private Function sizesArg;
        private Function targetArg;

        public LevelTwoPriceArrayFunction(Function targetArg, Function pricesArg, int pricesArgPos, Function sizesArg) {
            this.targetArg = targetArg;
            this.pricesArg = pricesArg;
            this.pricesArgPos = pricesArgPos;
            this.sizesArg = sizesArg;
        }

        @Override
        public void close() {
            pricesArg = Misc.free(pricesArg);
            sizesArg = Misc.free(sizesArg);
            targetArg = Misc.free(targetArg);
        }

        @Override
        public double getDouble(Record rec) {
            final double t = targetArg.getDouble(rec);
            final ArrayView pricesArr = pricesArg.getArray(rec);
            final ArrayView sizesArr = sizesArg.getArray(rec);
            final int len = pricesArr.getDimLen(0);
            if (sizesArr.getDimLen(0) != len) {
                throw CairoException.nonCritical().position(pricesArgPos)
                        .put("sizes array length doesn't match prices array length");
            }
            FlatArrayView flatSizes = sizesArr.flatView();
            FlatArrayView flatPrices = pricesArr.flatView();
            final int sizesOffset = sizesArr.getFlatViewOffset();
            final int pricesOffset = pricesArr.getFlatViewOffset();

            double ta = 0; // target accumulator
            double pa = 0; // price accumulator
            double rt = t; // reduced target
            double pp; // partial price

            // expect (size, value) pairs
            // equation is
            // ((size[0] * price[0]) + (size[n] * price[n]) + ((target - size[0] - size[n]) * price[n+1])) / target
            // ((ra)             + (rt * price[n+1])) / t
            // get final price by partially filling against the last bin
            for (int i = 0; i < len; i++) {
                final double size = flatSizes.getDouble(sizesOffset + i);
                final double price = flatPrices.getDouble(pricesOffset + i);

                // add size to acc
                ta += size;

                if (ta >= t) {
                    // order is fulfilled
                    if (i == 0) {
                        return price;
                    }

                    pp = rt * price;
                    return (pa + pp) / t;
                } else {
                    // order is not fulfilled, so aggregate partial pricing
                    pa += (size * price);
                    rt -= size;
                }
            }

            // if never exceeded the target, then no price, since it can't be fulfilled
            return Double.NaN;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("l2price(").val(targetArg).val(", ").val(pricesArg).val(", ").val(sizesArg).val(')');
        }
    }
}
