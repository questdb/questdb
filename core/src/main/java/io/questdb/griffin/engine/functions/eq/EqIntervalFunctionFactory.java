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
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class EqIntervalFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "=(ΔΔ)";
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
        if (a.getType() != b.getType()) {
            return BooleanConstant.FALSE;
        }

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
        final Interval constValue = constFunc.getInterval(null);
        if (Interval.NULL.equals(constValue)) {
            return new NullCheckFunc(varFunc);
        }
        return new ConstCheckFunc(varFunc, constValue, constFunc.getType());
    }

    private static class ConstCheckFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;
        private final Interval constant;
        private final int intervalType;

        public ConstCheckFunc(Function arg, @NotNull Interval constant, int intervalType) {
            this.arg = arg;
            this.constant = constant;
            this.intervalType = intervalType;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != constant.equals(arg.getInterval(rec));
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
            if (negated) {
                sink.val('!');
            }

            sink.val("='").valInterval(constant, intervalType).val('\'');
        }
    }

    private static class Func extends AbstractEqBinaryFunction {

        public Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            final Interval a = left.getInterval(rec);
            final Interval b = right.getInterval(rec);
            return negated != a.equals(b);
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

    private static class HalfRuntimeConstFunc extends AbstractEqBinaryFunction {
        private Interval cachedRuntimeConst;

        public HalfRuntimeConstFunc(Function runtimeConst, Function arg) {
            super(arg, runtimeConst);
        }

        @Override
        public boolean getBool(Record rec) {
            final Interval a = left.getInterval(rec);
            return negated != a.equals(cachedRuntimeConst);
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
            cachedRuntimeConst = right.getInterval(null);
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
            return negated != (Interval.NULL.equals(arg.getInterval(rec)));
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
