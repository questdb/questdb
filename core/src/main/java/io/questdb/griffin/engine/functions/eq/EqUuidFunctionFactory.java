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
import io.questdb.std.Long128;
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
        Long128 aLong128 = a.getLong128A(null);
        Long128 bLong128 = b.getLong128A(null);
        return BooleanConstant.of(aLong128.equals(bLong128));
    }

    private Function createHalfConstantFunc(Function constFunc, Function varFunc) {
        Long128 long128const = constFunc.getLong128A(null);
        return new ConstCheckFunc(varFunc, long128const);
    }

    private static class ConstCheckFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;
        private final Long128 long128const;

        public ConstCheckFunc(Function arg, Long128 long128const) {
            this.arg = arg;
            this.long128const = long128const;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            Long128 long128 = arg.getLong128A(rec);
            return negated != (long128const.equals(long128));
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
            Long128 leftLong128 = left.getLong128A(rec);
            Long128 rightLong128 = right.getLong128B(rec);
            return negated != (leftLong128.equals(rightLong128));
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
