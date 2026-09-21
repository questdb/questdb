/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions.lt;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

public class LtVarcharFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "<(ØØ)";
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
        final Function a = args.getQuick(0);
        final Function b = args.getQuick(1);
        if (a.isConstant() && !b.isConstant()) {
            Utf8Sequence constValue = a.getVarcharA(null);
            if (constValue == null) {
                return new VarcharNullSideFunc(b, true);
            }
            return new LtStrVarcharFunctionFactory.ConstOnLeftFunc(constValue, b);
        }
        if (!a.isConstant() && b.isConstant()) {
            Utf8Sequence constValue = b.getVarcharA(null);
            if (constValue == null) {
                return new VarcharNullSideFunc(a, false);
            }
            return new LtVarcharStrFunctionFactory.ConstOnRightFunc(a, constValue);
        }
        return new Func(a, b);
    }

    /**
     * Short-circuit used when one operand is a constant NULL and the other side is a
     * VARCHAR-typed Function. See {@link LtStrFunctionFactory.StrNullSideFunc} for
     * the full rationale; this variant differs only in checking
     * {@link Function#getVarcharA(Record)} for null instead of
     * {@link Function#getStrA(Record)}.
     */
    static final class VarcharNullSideFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;
        private final boolean nullOnLeft;

        VarcharNullSideFunc(Function arg, boolean nullOnLeft) {
            this.arg = arg;
            this.nullOnLeft = nullOnLeft;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated && arg.getVarcharA(rec) == null;
        }

        @Override
        public String getName() {
            return negated ? ">=" : "<";
        }

        // See StrNullSideFunc.toPlan for the swap-form caveat -- EXPLAIN can
        // read `null >= s` for a user-written `s <= null`.
        @Override
        public void toPlan(PlanSink sink) {
            if (nullOnLeft) {
                sink.val("null").val(getName()).val(arg);
            } else {
                sink.val(arg).val(getName()).val("null");
            }
        }
    }

    static class Func extends NegatableBooleanFunction implements BinaryFunction {
        private final Function left;
        private final Function right;

        public Func(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean getBool(Record rec) {
            return Utf8s.lessThan(
                    left.getVarcharA(rec),
                    right.getVarcharB(rec),
                    negated
            );
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public String getName() {
            if (negated) {
                return ">=";
            } else {
                return "<";
            }
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(left);
            sink.val(getName());
            sink.val(right);
        }
    }
}
