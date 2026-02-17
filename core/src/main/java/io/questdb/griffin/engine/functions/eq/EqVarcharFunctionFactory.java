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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

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
        final Function a = args.getQuick(0);
        final Function b = args.getQuick(1);

        if (a.isConstant()) {
            return createHalfConstantFunc(a, b);
        }
        if (b.isConstant()) {
            return createHalfConstantFunc(b, a);
        }
        if (a.isRuntimeConstant()) {
            return new HalfRuntimeConstFunc(a, b);
        }
        if (b.isRuntimeConstant()) {
            return new HalfRuntimeConstFunc(b, a);
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

    static class ConstCheckFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;
        private final Utf8Sequence constant;
        private final long constantSixPrefix;

        ConstCheckFunc(Function arg, @NotNull Utf8Sequence constant) {
            this.arg = arg;
            this.constant = constant;
            this.constantSixPrefix = constant.zeroPaddedSixPrefix();
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            final Utf8Sequence val = arg.getVarcharA(rec);
            return negated != (val != null && Utf8s.equals(val, val.zeroPaddedSixPrefix(), constant, constantSixPrefix));
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

    static class Func extends AbstractEqBinaryFunction {
        Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            // getVarcharA/B returns a reusable sequence object.
            // When using two at once, it's important to use both A and B.
            final Utf8Sequence a = left.getVarcharA(rec);
            final Utf8Sequence b = right.getVarcharB(rec);
            if (a == null) {
                return negated != (b == null);
            }
            return negated != Utf8s.equals(a, b);
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

    static class HalfRuntimeConstFunc extends AbstractEqBinaryFunction {
        private Utf8Sequence cachedRuntimeConst;
        private long cachedSixPrefix;

        HalfRuntimeConstFunc(Function runtimeConst, Function arg) {
            super(arg, runtimeConst);
        }

        @Override
        public boolean getBool(Record rec) {
            final Utf8Sequence a = left.getVarcharA(rec);
            final Utf8Sequence b = cachedRuntimeConst;
            if (a == null || b == null) {
                return negated != (a == b);
            }
            return negated != Utf8s.equals(a, a.zeroPaddedSixPrefix(), b, cachedSixPrefix);
        }

        @Override
        public String getName() {
            if (negated) {
                return "!=";
            } else {
                return "=";
            }
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            cachedRuntimeConst = right.getVarcharA(null);
            cachedSixPrefix = cachedRuntimeConst != null ? cachedRuntimeConst.zeroPaddedSixPrefix() : 0L;
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
            return negated != (arg.getVarcharSize(rec) == -1L);
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
