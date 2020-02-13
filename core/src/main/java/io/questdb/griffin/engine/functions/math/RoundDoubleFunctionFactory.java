/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

public class RoundDoubleFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "round(DI)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        Function scale = args.getQuick(1);
        if (scale.isConstant()) {
            int scaleValue = scale.getInt(null);
            if (scaleValue != Numbers.INT_NaN) {
                if (scaleValue > -1 && scaleValue + 2 < Numbers.pow10max) {
                    return new FuncPosConst(position, args.getQuick(0), scaleValue);
                }
                if (scaleValue < 0 && scaleValue > -Numbers.pow10max) {
                    return new FuncNegConst(position, args.getQuick(0), -scaleValue);
                }
            }
            return new DoubleConstant(position, Double.NaN);
        }
        return new Func(position, args.getQuick(0), args.getQuick(1));
    }

    private static class Func extends DoubleFunction implements BinaryFunction {
        private final Function left;
        private final Function right;

        public Func(int position, Function left, Function right) {
            super(position);
            this.left = left;
            this.right = right;
        }

        @Override
        public double getDouble(Record rec) {
            final double l = left.getDouble(rec);
            if (l != l) {
                return l;
            }

            final int r = right.getInt(rec);
            if (r == Numbers.INT_NaN) {
                return Double.NaN;
            }

            try {
                return Numbers.roundHalfUp(l, r);
            } catch (NumericException e) {
                return Double.NaN;
            }
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public Function getRight() {
            return right;
        }
    }

    private static class FuncPosConst extends DoubleFunction implements UnaryFunction {
        private final Function arg;
        private final int scale;

        public FuncPosConst(int position, Function arg, int r) {
            super(position);
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
            if (l != l) {
                return l;
            }

            return Numbers.roundHalfUpPosScale(l, scale);
        }
    }

    private static class FuncNegConst extends DoubleFunction implements UnaryFunction {
        private final Function arg;
        private final int scale;

        public FuncNegConst(int position, Function arg, int r) {
            super(position);
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
            if (l != l) {
                return l;
            }

            return Numbers.roundHalfUpNegScale(l, scale);
        }
    }
}
