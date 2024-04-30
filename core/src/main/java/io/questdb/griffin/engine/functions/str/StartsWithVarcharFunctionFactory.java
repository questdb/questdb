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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

public class StartsWithVarcharFunctionFactory implements FunctionFactory {

    public static final String NAME = "starts_with";

    @Override
    public String getSignature() {
        return NAME + "(ØØ)";
    }

    @Override
    public Function newInstance(
            int position, ObjList<Function> args, IntList argPositions,
            CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext
    ) {
        Function varcharFunc = args.get(0);
        Function prefixFunc = args.get(1);
        if (prefixFunc.isConstant()) {
            return new ConstStartsWithVarcharFunction(varcharFunc, prefixFunc.getVarcharA(null));
        }
        return new Func(varcharFunc, prefixFunc);
    }

    public static class ConstStartsWithVarcharFunction extends BooleanFunction implements UnaryFunction {
        private final Utf8String prefix;
        private final Function value;

        public ConstStartsWithVarcharFunction(Function value, @Transient Utf8Sequence prefix) {
            this.value = value;
            this.prefix = Utf8String.newInstance(prefix);
        }

        public ConstStartsWithVarcharFunction(Function value, @Transient CharSequence prefix) {
            this.value = value;
            this.prefix = new Utf8String(prefix);
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            Utf8Sequence us = value.getVarcharA(rec);
            return us != null && Utf8s.startsWith(us, prefix);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" like ");
            sink.val(prefix);
            sink.val('%');
        }
    }

    private static class Func extends BooleanFunction implements BinaryFunction {
        private final Function prefixFunc;
        private final Function varcharFunc;

        public Func(Function varcharFunc, Function prefixFunc) {
            this.varcharFunc = varcharFunc;
            this.prefixFunc = prefixFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            Utf8Sequence varchar = varcharFunc.getSplitVarcharA(rec);
            Utf8Sequence prefix = prefixFunc.getSplitVarcharA(rec);
            if (varchar == null || prefix == null) {
                return false;
            }
            return Utf8s.startsWith(varchar, prefix);
        }

        @Override
        public Function getLeft() {
            return varcharFunc;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Function getRight() {
            return prefixFunc;
        }
    }
}
