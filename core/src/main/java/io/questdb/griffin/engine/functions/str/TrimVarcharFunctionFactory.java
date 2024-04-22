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
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.str.Utf8s.trim;

public class TrimVarcharFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "trim(Ø)";
    }

    @Override
    public Function newInstance(
            int position, ObjList<Function> args, IntList argPositions,
            CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext
    ) {
        final Function arg = args.get(0);
        if (!arg.isConstant()) {
            return new Func(args.getQuick(0), trimType());
        }
        if (arg.getStrA(null) == null) {
            return VarcharConstant.NULL;
        }
        return new ConstFunc(args.getQuick(0), trimType());
    }

    protected TrimType trimType() {
        return TrimType.TRIM;
    }

    private static class ConstFunc extends VarcharFunction implements UnaryFunction {

        private final Function arg;
        private final DirectUtf8Sink sink1;
        private final DirectUtf8Sink sink2;

        ConstFunc(Function arg, TrimType type) {
            this.arg = arg;
            Utf8Sequence value = getArg().getVarcharA(null);
            if (value == null) {
                this.sink1 = new DirectUtf8Sink(0);
                this.sink2 = new DirectUtf8Sink(0);
            } else {
                this.sink1 = new DirectUtf8Sink(value.size());
                trim(type, value, sink1);
                this.sink2 = new DirectUtf8Sink(sink1.size());
                sink2.put(sink1);
            }
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
            sink1.close();
            sink2.close();
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void getVarchar(Record rec, Utf8Sink utf8Sink) {
            utf8Sink.put(sink1);
        }

        @Override
        public @Nullable Utf8Sequence getVarcharA(Record rec) {
            return sink1;
        }

        @Override
        public @Nullable Utf8Sequence getVarcharB(Record rec) {
            return sink2;
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val('\'').val(sink1).val('\'');
        }
    }

    private static class Func extends VarcharFunction implements UnaryFunction {
        private final Function arg;
        private final DirectUtf8Sink sink1 = new DirectUtf8Sink(4);
        private final DirectUtf8Sink sink2 = new DirectUtf8Sink(4);
        private final TrimType type;

        Func(Function arg, TrimType type) {
            this.arg = arg;
            this.type = type;
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
            sink1.close();
            sink2.close();
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public String getName() {
            switch (type) {
                case LTRIM:
                    return "ltrim";
                case RTRIM:
                    return "rtrim";
                default:
                    return "trim";
            }
        }

        @Override
        public void getVarchar(Record rec, Utf8Sink utf8Sink) {
            trim(type, getArg().getVarcharA(rec), utf8Sink);
        }

        @Override
        public Utf8Sequence getVarcharA(final Record rec) {
            final Utf8Sequence utf8Sequence = getArg().getVarcharA(rec);
            if (utf8Sequence == null) {
                return null;
            }
            sink1.clear();
            trim(type, utf8Sequence, sink1);
            return sink1;
        }

        @Override
        public Utf8Sequence getVarcharB(final Record rec) {
            final Utf8Sequence charSequence = getArg().getVarcharA(rec);
            if (charSequence == null) {
                return null;
            }
            sink2.clear();
            trim(type, charSequence, sink2);
            return sink2;
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }
    }
}
