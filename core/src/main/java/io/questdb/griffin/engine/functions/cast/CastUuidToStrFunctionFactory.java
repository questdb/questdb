/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.UuidConstant;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

public final class CastUuidToStrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Zs)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        Function func = args.getQuick(0);
        if (func.isConstant()) {
            StringSink sink = Misc.getThreadLocalBuilder();
            uuidToSink(func.getUuidHi(null), func.getUuidLo(null), sink);
            return new StrConstant(Chars.toString(sink));
        }
        return new Func(func);
    }

    private static void uuidToSink(long hi, long lo, CharSink sink) {
        if (hi == UuidConstant.NULL_HI_AND_LO && lo == UuidConstant.NULL_HI_AND_LO) {
            return;
        }
        Numbers.appendUuid(hi, lo, sink);
    }

    private static class Func extends StrFunction implements UnaryFunction {
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
        public CharSequence getStr(Record rec) {
            sinkA.clear();
            uuidToSink(arg.getUuidHi(rec), arg.getUuidLo(rec), sinkA);
            return sinkA;
        }

        @Override
        public void getStr(Record rec, CharSink sink) {
            uuidToSink(arg.getUuidHi(rec), arg.getUuidLo(rec), sink);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            sinkB.clear();
            uuidToSink(arg.getUuidHi(rec), arg.getUuidLo(rec), sinkB);
            return sinkB;
        }
    }
}
