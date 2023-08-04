/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import static io.questdb.std.Numbers.IPv4_NULL;
import static io.questdb.std.Numbers.parseIPv4Quiet;

public class EqIPv4FunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "=(XS)";
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
    ) throws SqlException {

        final Function a = args.getQuick(0);
        final Function b = args.getQuick(1);


        if (!a.isConstant() && b.isConstant()) {
            return createHalfConstantFunc(b, a, argPositions.get(1));
        }

        return new EqIPv4FunctionFactory.Func(a, b);
    }

    private Function createHalfConstantFunc(Function constFunc, Function varFunc, int constFuncPosition) throws SqlException {
        CharSequence constValue = constFunc.getStr(null);

        if (constValue == null) {
            return new NullCheckFunc(varFunc);
        }

        int constVal = parseIPv4Quiet(constValue);

        if (constVal == 0) {
            throw SqlException.$(constFuncPosition, "not a valid IP address: ").put(constValue);
        }

        return new ConstCheckFunc(varFunc, constVal);
    }

    private static class ConstCheckFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;
        private final int constant;

        public ConstCheckFunc(Function arg, int constant) {
            this.arg = arg;
            this.constant = constant;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (arg.getIPv4(rec) == constant);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
            if (negated) {
                sink.val('!');
            }
            sink.val('=').val(constant).val('\'');
        }
    }

    private static class Func extends AbstractEqBinaryFunction {
        public Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (left.getIPv4(rec) == parseIPv4Quiet(right.getStr(rec)));
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
            return negated != (arg.getIPv4(rec) == IPv4_NULL);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
            if (negated) {
                sink.val(" is not null ");
            } else {
                sink.val(" is null");
            }
        }
    }
}
