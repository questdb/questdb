/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public final class EqUuidFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(ZZ)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        Function a = args.getQuick(0);
        Function b = args.getQuick(1);

        if (a.isConstant() && b.isConstant()) {
            return createConstant(a, b);
        }

        if (a.isConstant() && !b.isConstant()) {
            return createHalfConstantFunc(a, b);
        }

        if (!a.isConstant() && b.isConstant()) {
            return createHalfConstantFunc(b, a);
        }
        return new Func(a, b);
    }

    private static BooleanConstant createConstant(Function a, Function b) {
        long aLoc = a.getUuidLocation(null);
        long bLoc = b.getUuidLocation(null);
        long aHi = a.getUuidHi(null, aLoc);
        long aLo = a.getUuidLo(null, aLoc);
        long bHi = b.getUuidHi(null, bLoc);
        long bLo = b.getUuidLo(null, bLoc);
        return BooleanConstant.of(aHi == bHi && aLo == bLo);
    }

    private Function createHalfConstantFunc(Function constFunc, Function varFunc) {
        long loc = constFunc.getUuidLocation(null);
        return new ConstCheckFunc(varFunc, constFunc.getUuidHi(null, loc), constFunc.getUuidLo(null, loc));
    }

    private static class ConstCheckFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;
        private final long hiConstant;
        private final long loConstant;

        public ConstCheckFunc(Function arg, long hiConstant, long loConstant) {
            this.arg = arg;
            this.hiConstant = hiConstant;
            this.loConstant = loConstant;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            long loc = arg.getUuidLocation(rec);
            long hi = arg.getUuidHi(rec, loc);
            long lo = arg.getUuidLo(rec, loc);
            return negated != (hi == hiConstant && lo == loConstant);
        }
    }

    public static class Func extends NegatableBooleanFunction implements BinaryFunction {
        private final Function left;
        private final Function right;

        public Func(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean getBool(Record rec) {
            long leftLoc = left.getUuidLocation(rec);
            long rightLoc = right.getUuidLocation(rec);
            final long leftHi = left.getUuidHi(rec, leftLoc);
            final long leftLo = left.getUuidLo(rec, leftLoc);
            final long rightHi = right.getUuidHi(rec, rightLoc);
            final long rightLo = right.getUuidLo(rec, rightLoc);
            return negated != (leftHi == rightHi && leftLo == rightLo);
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
