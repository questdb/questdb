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
            return new ConstFunc(varcharFunc, prefixFunc.getVarcharA(null));
        }
        return new Func(varcharFunc, prefixFunc);
    }

    public static class ConstFunc extends BooleanFunction implements UnaryFunction {
        protected final Utf8String startsWith;
        protected final Function value;
        private final long startsWithSixPrefix;

        public ConstFunc(Function value, @Transient Utf8Sequence startsWith) {
            this.value = value;
            if (startsWith != null) {
                this.startsWith = Utf8String.newInstance(startsWith);
                this.startsWithSixPrefix = startsWith.zeroPaddedSixPrefix();
            } else {
                this.startsWith = null;
                this.startsWithSixPrefix = 0;
            }
        }

        public ConstFunc(Function value, @Transient CharSequence startsWith) {
            this.value = value;
            this.startsWith = new Utf8String(startsWith);
            this.startsWithSixPrefix = this.startsWith.zeroPaddedSixPrefix();
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            if (startsWith == null) {
                return false;
            }
            Utf8Sequence us = value.getVarcharA(rec);
            return us != null && Utf8s.startsWith(us, us.zeroPaddedSixPrefix(), startsWith, startsWithSixPrefix);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME).val('(');
            sink.val(value).val(", ");
            sink.val(startsWith).val(')');
        }
    }

    private static class Func extends BooleanFunction implements BinaryFunction {
        private final Function startsWith;
        private final Function value;

        public Func(Function value, Function startsWith) {
            this.value = value;
            this.startsWith = startsWith;
        }

        @Override
        public boolean getBool(Record rec) {
            Utf8Sequence varchar = value.getVarcharA(rec);
            Utf8Sequence prefix = startsWith.getVarcharA(rec);
            if (varchar == null || prefix == null) {
                return false;
            }
            return Utf8s.startsWith(varchar, prefix);
        }

        @Override
        public Function getLeft() {
            return value;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Function getRight() {
            return startsWith;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME).val('(');
            sink.val(value).val(", ");
            sink.val(startsWith).val(')');
        }
    }
}
