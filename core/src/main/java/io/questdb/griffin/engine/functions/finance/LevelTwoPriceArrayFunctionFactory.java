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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class LevelTwoPriceArrayFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "l2price(DD[]D[])";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        try {
            for (int i = 1; i < 3; i++) {
                final int dims = ColumnType.decodeWeakArrayDimensionality(args.getQuick(i).getType());
                if (dims > 0 && dims != 1) {
                    throw SqlException.$(argPositions.getQuick(i), "not a one-dimensional array");
                }
            }
            return new LevelTwoPriceArrayFunction(
                    args.getQuick(0),
                    args.getQuick(1),
                    argPositions.getQuick(1),
                    args.getQuick(2),
                    argPositions.getQuick(2)
            );
        } catch (Throwable e) {
            Misc.freeObjList(args);
            throw e;
        }
    }

    private static class LevelTwoPriceArrayFunction extends DoubleFunction implements TernaryFunction {
        private final Function pricesArg;
        private final int pricesArgPos;
        private final Function sizesArg;
        private final int sizesArgPos;
        private final Function targetArg;

        public LevelTwoPriceArrayFunction(Function targetArg, Function sizesArg, int sizesArgPos, Function pricesArg, int pricesArgPos) {
            this.targetArg = targetArg;
            this.sizesArgPos = sizesArgPos;
            this.sizesArg = sizesArg;
            this.pricesArg = pricesArg;
            this.pricesArgPos = pricesArgPos;
        }

        @Override
        public Function getCenter() {
            return sizesArg;
        }

        @Override
        public double getDouble(Record rec) {
            final double t = targetArg.getDouble(rec);
            final ArrayView pricesArr = pricesArg.getArray(rec);
            final ArrayView sizesArr = sizesArg.getArray(rec);
            if (pricesArr.isNull() || sizesArr.isNull()) {
                return Double.NaN;
            }
            final int len = pricesArr.getDimLen(0);
            if (sizesArr.getDimLen(0) != len) {
                throw CairoException.nonCritical().position(sizesArgPos)
                        .put("prices array length doesn't match sizes array length");
            }
            double ta = 0; // target accumulator
            double pa = 0; // price accumulator
            double rt = t; // reduced target
            double pp; // partial price

            for (int i = 0; i < len; i++) {
                final double size = sizesArr.getDouble(i);
                final double price = pricesArr.getDouble(i);

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
        public Function getLeft() {
            return targetArg;
        }

        @Override
        public Function getRight() {
            return pricesArg;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            TernaryFunction.super.init(symbolTableSource, executionContext);
            if (ColumnType.decodeArrayDimensionality(sizesArg.getType()) != 1) {
                throw SqlException.$(sizesArgPos, "not a one-dimensional array");
            }
            if (ColumnType.decodeArrayDimensionality(pricesArg.getType()) != 1) {
                throw SqlException.$(pricesArgPos, "not a one-dimensional array");
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("l2price(").val(targetArg).val(", ").val(sizesArg).val(", ").val(pricesArg).val(')');
        }
    }
}
