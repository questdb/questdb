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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.*;

import java.lang.ThreadLocal;

import static io.questdb.std.Long256FromCharSequenceDecoder.decode;

public class EqLong256StrFunctionFactory implements FunctionFactory {
    private static final ThreadLocal<Long256Decoder> DECODER = ThreadLocal.withInitial(Long256Decoder::new);

    @Override
    public String getSignature() {
        return "=(Hs)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        Function leftArg = args.getQuick(0);
        Function rightArg = args.getQuick(1);
        if (ColumnType.isNull(rightArg.getType())) {
            return new Func(leftArg);
        }
        return DECODER.get().newInstance(leftArg, rightArg.getStr(null));
    }

    private static class Func extends NegatableBooleanFunction implements UnaryFunction {
        private static final Long256 NULL = Long256Impl.NULL_LONG256;

        private final Function arg;
        private final long long0;
        private final long long1;
        private final long long2;
        private final long long3;

        public Func(Function arg) {
            this(arg, NULL.getLong0(), NULL.getLong1(), NULL.getLong2(), NULL.getLong3());
        }

        public Func(Function arg, long long0, long long1, long long2, long long3) {
            this.arg = arg;
            this.long0 = long0;
            this.long1 = long1;
            this.long2 = long2;
            this.long3 = long3;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            final Long256 value = arg.getLong256A(rec);
            return negated != (value.getLong0() == long0 &&
                    value.getLong1() == long1 &&
                    value.getLong2() == long2 &&
                    value.getLong3() == long3);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
            if (negated) {
                sink.val('!');
            }
            sink.val('=');
            sink.val(long0, long1, long2, long3);
        }
    }

    private static class Long256Decoder {
        private long long0;
        private long long1;
        private long long2;
        private long long3;
        private final Long256Acceptor decoder = this::setAll;

        public void setAll(long l0, long l1, long l2, long l3) {
            long0 = l0;
            long1 = l1;
            long2 = l2;
            long3 = l3;
        }

        private Func newInstance(Function arg, CharSequence hexLong256) {
            decode(hexLong256, 2, hexLong256.length(), decoder);
            return new Func(arg, long0, long1, long2, long3);
        }
    }
}
