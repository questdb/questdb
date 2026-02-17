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

package io.questdb.griffin.engine.functions.math;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

public class RoundHalfEvenDoubleFunctionFactory implements FunctionFactory {

    private static final String SYMBOL = "round_half_even";
    private static final String SIGNATURE = SYMBOL + "(DI)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        Function scale = args.getQuick(1);
        if (scale.isConstant()) {
            int scaleValue = scale.getInt(null);
            if (scaleValue != Numbers.INT_NULL) {
                if (scaleValue > -1 && scaleValue + 2 < Numbers.pow10max) {
                    return new FuncPosConst(args.getQuick(0), scaleValue);
                }
                if (scaleValue < 0 && scaleValue > -Numbers.pow10max) {
                    return new FuncNegConst(args.getQuick(0), -scaleValue);
                }
            }
            return DoubleConstant.NULL;
        }
        return new Func(args.getQuick(0), args.getQuick(1));
    }

    private static class Func extends DoubleFunction implements BinaryFunction {
        private final Function left;
        private final Function right;

        public Func(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public double getDouble(Record rec) {
            final double l = left.getDouble(rec);
            if (Numbers.isNull(l)) {
                return l;
            }

            final int r = right.getInt(rec);
            if (r == Numbers.INT_NULL) {
                return Double.NaN;
            }

            try {
                return Numbers.roundHalfEven(l, r);
            } catch (NumericException e) {
                return Double.NaN;
            }
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public String getName() {
            return SYMBOL;
        }

        @Override
        public Function getRight() {
            return right;
        }
    }

    private static class FuncNegConst extends DoubleFunction implements UnaryFunction {
        private final Function arg;
        private final int scale;

        public FuncNegConst(Function arg, int r) {
            this.arg = arg;
            this.scale = r;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public double getDouble(Record rec) {
            final double l = arg.getDouble(rec);
            if (Numbers.isNull(l)) {
                return l;
            }

            return Numbers.roundHalfEvenNegScale(l, scale);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SYMBOL).val('(').val(arg).val(',').val(scale).val(')');
        }
    }

    private static class FuncPosConst extends DoubleFunction implements UnaryFunction {
        private final Function arg;
        private final int scale;

        public FuncPosConst(Function arg, int r) {
            this.arg = arg;
            this.scale = r;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public double getDouble(Record rec) {
            final double l = arg.getDouble(rec);
            if (Numbers.isNull(l)) {
                return l;
            }

            return Numbers.roundHalfEvenPosScale(l, scale);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SYMBOL).val('(').val(arg).val(',').val(scale).val(')');
        }
    }


}
