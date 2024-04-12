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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;
import org.jetbrains.annotations.Nullable;

public class CastIPv4ToStrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(Xs)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        Function ipv4Func = args.getQuick(0);
        if (ipv4Func.isConstant()) {
            StringSink sink = Misc.getThreadLocalSink();
            Numbers.intToIPv4Sink(sink, ipv4Func.getIPv4(null));
            return new StrConstant(Chars.toString(sink));
        }
        return new Func(args.getQuick(0));
    }

    public static class Func extends AbstractCastToStrFunction {
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public Func(Function arg) {
            super(arg);
        }

        @Override
        public CharSequence getStrA(Record rec) {
            final int value = arg.getIPv4(rec);
            return toSink(value, sinkA);
        }

        @Override
        public void getStr(Record rec, Utf16Sink utf16Sink) {
            final int value = arg.getIPv4(rec);
            if (value != Numbers.IPv4_NULL) {
                Numbers.intToIPv4Sink(utf16Sink, value);
            }
        }

        @Override
        public CharSequence getStrB(Record rec) {
            final int value = arg.getIPv4(rec);
            return toSink(value, sinkB);
        }

        @Nullable
        private StringSink toSink(int value, StringSink sinkB) {
            if (value != Numbers.IPv4_NULL) {
                sinkB.clear();
                Numbers.intToIPv4Sink(sinkB, value);
                return sinkB;
            }
            return null;
        }
    }
}
