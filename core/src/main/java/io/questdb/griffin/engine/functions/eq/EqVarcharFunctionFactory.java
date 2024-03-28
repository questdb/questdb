/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

public class EqVarcharFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "=(ØØ)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        // Optimizations:
        // 1. If one arg is a compile-time constant, cache its value
        // 2. If one arg is not a column value, ensure it is the second argument because
        //    the 1st argument chooses the optimal varchar column data access pattern

        final Function a = args.getQuick(0);
        final Function b = args.getQuick(1);

        if (a.isConstant() && !b.isConstant()) {
            return createHalfConstantFunc(a, b);
        }
        if (!a.isConstant() && b.isConstant()) {
            return createHalfConstantFunc(b, a);
        }
        if (a.isRuntimeConstant() && !b.isRuntimeConstant()) {
            return new Func(b, a);
        }
        return new Func(a, b);
    }

    private Function createHalfConstantFunc(Function constFunc, Function varFunc) {
        Utf8Sequence constValue = constFunc.getVarcharA(null);
        if (constValue == null) {
            return new NullCheckFunc(varFunc);
        }
        return new ConstCheckFunc(varFunc, constValue);
    }

    public static class ConstCheckFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;
        private final Utf8Sequence constant;

        public ConstCheckFunc(Function arg, Utf8Sequence constant) {
            this.arg = arg;
            this.constant = constant;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            // argument order to equalsNc is important: the first argument can be either an inlined or
            // a split varchar. That implementation should choose the data access pattern that is optimal
            // for it, and the constant implementation can easily adapt to both patterns.
            final Utf8Sequence val = arg.getVarcharA(rec);
            return negated != (val != null && Utf8s.equals(val, constant));
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
            if (negated) {
                sink.val('!');
            }
            sink.val("='").val(constant).val('\'');
        }
    }

    private static class Func extends AbstractEqBinaryFunction {
        public Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            // important to compare A and B varchars in case
            // these are columns of the same record
            // records have re-usable character sequences
            final Utf8Sequence a = left.getVarcharA(rec);
            final Utf8Sequence b = right.getVarcharB(rec);

            if (a == null) {
                return negated != (b == null);
            }

            return negated != Utf8s.equalsNc(a, b);
        }

        @Override
        public String getName() {
            if (negated) {
                return "!=";
            } else {
                return "=";
            }
        }
    }

    public static class NullCheckFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;

        public NullCheckFunc(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (arg.getVarcharA(rec) == null);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
            if (negated) {
                sink.val(" is not null");
            } else {
                sink.val(" is null");
            }
        }
    }
}
