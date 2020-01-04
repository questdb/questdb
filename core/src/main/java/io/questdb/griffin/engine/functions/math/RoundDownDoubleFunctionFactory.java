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

import io.questdb.griffin.FunctionFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;


public class RoundDownDoubleFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "rounddown(DI)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        Function precision = args.getQuick(1);
        if (precision.isConstant()) {
            int precisionValue = precision.getInt(null);
            return (precisionValue != Numbers.INT_NaN && precisionValue * precisionValue <= Numbers.pow10max * Numbers.pow10max) ?
                    new FuncConst(position, args.getQuick(0), precision) :
                    new DoubleConstant(position, Double.NaN);
        }
        return new Func(position, args.getQuick(0), args.getQuick(1));
    }

    private static class FuncConst extends DoubleFunction implements BinaryFunction {
        private final Function left;
        private final Function right;

        public FuncConst(int position, Function left, Function right) {
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

            final int r = right.getInt(null);
            return (r > 0) ? Numbers.roundDownPosScale(l, r) : Numbers.roundDownNegScale(l, r);
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
                return Numbers.roundDown(l, r);
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
}

