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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

public class CastLong256ToStrFunctionFactory implements FunctionFactory {

    public static void value(CharSink<?> sink, Long256 operand) {
        Numbers.appendLong256Value(operand.getLong0(), operand.getLong1(), operand.getLong2(), operand.getLong3(), sink);
    }

    @Override
    public String getSignature() {
        return "cast(Hs)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new Func(args.get(0));
    }

    public static class Func extends StrFunction implements UnaryFunction {
        private final Function arg;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public Func(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            sinkA.clear();
            final Long256 long256 = arg.getLong256A(rec);
            if (Long256Impl.isNull(long256)) {
                return null;
            }
            value(sinkA, long256);
            return sinkA;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            sinkB.clear();
            final Long256 long256 = arg.getLong256A(rec);
            if (Long256Impl.isNull(long256)) {
                return null;
            }
            value(sinkB, long256);
            return sinkB;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val("::string");
        }
    }
}
