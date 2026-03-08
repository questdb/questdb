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

package io.questdb.griffin.engine.functions.bin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.Nullable;

public class Base64DecodeFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "from_base64(S)";
    }

    @Override
    public Function newInstance(int position,
                                ObjList<Function> args,
                                IntList argPositions,
                                CairoConfiguration configuration,
                                SqlExecutionContext sqlExecutionContext) {

        Function func = args.get(0);
        return new Base64DecodeFunc(func);
    }

    private static class Base64DecodeFunc extends BinFunction implements UnaryFunction {
        private final Function data;
        private final BinarySequenceUtf8Sink sink = new BinarySequenceUtf8Sink();

        public Base64DecodeFunc(final Function data) {
            this.data = data;
        }

        @Override
        public Function getArg() {
            return data;
        }

        @Override
        public BinarySequence getBin(Record rec) {
            CharSequence str = data.getStrA(rec);
            if (str == null) {
                return null;
            }
            sink.clear();
            Chars.base64Decode(str, sink);
            return sink;
        }

        @Override
        public long getBinLen(Record rec) {
            BinarySequence bs = getBin(rec);
            if (bs == null) {
                return -1;
            }
            return bs.length();
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("from_base64(").val(data).val(')');
        }

        private static class BinarySequenceUtf8Sink implements BinarySequence, Utf8Sink {
            private final Utf8StringSink sink = new Utf8StringSink();

            @Override
            public byte byteAt(long index) {
                return sink.byteAt((int) index);
            }

            public void clear() {
                sink.clear();
            }

            @Override
            public long length() {
                return sink.size();
            }

            @Override
            public Utf8Sink put(@Nullable Utf8Sequence us) {
                return sink.put(us);
            }

            @Override
            public Utf8Sink put(byte b) {
                return sink.putAny(b);
            }

            @Override
            public Utf8Sink putNonAscii(long lo, long hi) {
                return sink.putNonAscii(lo, hi);
            }
        }
    }
}
