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
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;

public class LeftVarcharFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "left(ØI)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function varcharFunc = args.getQuick(0);
        final Function countFunc = args.getQuick(1);
        if (countFunc.isConstant()) {
            int count = countFunc.getInt(null);
            if (count != Numbers.INT_NULL) {
                return new ConstCountFunc(varcharFunc, count);
            } else {
                return VarcharConstant.NULL;
            }
        }
        return new Func(varcharFunc, countFunc);
    }

    private static int getCharPos(int len, int count) {
        return count > -1 ? Math.max(0, Math.min(len, count)) : Math.max(0, len + count);
    }

    private static class ConstCountFunc extends VarcharFunction implements UnaryFunction {
        private final int count;
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();
        private final Function varcharFunc;

        public ConstCountFunc(Function varcharFunc, int count) {
            this.varcharFunc = varcharFunc;
            this.count = count;
        }

        @Override
        public Function getArg() {
            return varcharFunc;
        }

        @Override
        public @Nullable Utf8Sequence getVarcharA(Record rec) {
            return getVarchar0(rec, sinkA);
        }

        @Override
        public @Nullable Utf8Sequence getVarcharB(Record rec) {
            return getVarchar0(rec, sinkB);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("left(").val(varcharFunc).val(',').val(count).val(')');
        }

        private int getCharPos(int len) {
            return LeftVarcharFunctionFactory.getCharPos(len, count);
        }

        @Nullable
        private Utf8StringSink getVarchar0(Record rec, Utf8StringSink sink) {
            Utf8Sequence value = varcharFunc.getVarcharA(rec);
            if (value != null) {
                final int len = Utf8s.validateUtf8(value);
                if (len == -1) {
                    // Invalid UTF-8.
                    return null;
                }
                final int charHi = getCharPos(len);
                sink.clear();
                Utf8s.strCpy(value, 0, charHi, sink);
                return sink;
            }
            return null;
        }
    }

    private static class Func extends VarcharFunction implements BinaryFunction {
        private final Function countFunc;
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();
        private final Function varcharFunc;

        public Func(Function varcharFunc, Function countFunc) {
            this.varcharFunc = varcharFunc;
            this.countFunc = countFunc;
        }

        @Override
        public Function getLeft() {
            return varcharFunc;
        }

        @Override
        public String getName() {
            return "left";
        }

        @Override
        public Function getRight() {
            return countFunc;
        }

        @Override
        public @Nullable Utf8Sequence getVarcharA(Record rec) {
            return getVarchar0(rec, sinkA);
        }

        @Override
        public @Nullable Utf8Sequence getVarcharB(Record rec) {
            return getVarchar0(rec, sinkB);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Nullable
        private Utf8StringSink getVarchar0(Record rec, Utf8StringSink sink) {
            final Utf8Sequence value = varcharFunc.getVarcharA(rec);
            final int count = countFunc.getInt(rec);
            if (value != null && count != Numbers.INT_NULL) {
                final int len = Utf8s.validateUtf8(value);
                if (len == -1) {
                    // Invalid UTF-8.
                    return null;
                }
                final int charHi = getCharPos(len, count);
                sink.clear();
                Utf8s.strCpy(value, 0, charHi, sink);
                return sink;
            }
            return null;
        }
    }
}
