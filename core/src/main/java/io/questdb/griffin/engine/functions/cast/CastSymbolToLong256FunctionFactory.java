/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;

public class CastSymbolToLong256FunctionFactory extends AbstractEntityCastFunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Kh)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new Func(args.getQuick(0));
    }

    public static void charSequenceToLong256(CharSink sink, CharSequence value) {
        if (value == null) {
            return;
        }
        final int len = value.length();
        if (Long256Util.isValidString(value, len)) {
            try {
                final long a = Numbers.parseHexLong(value, 2, Math.min(len, 18));
                long b = 0;
                long c = 0;
                long d = 0;
                if (len > 18) {
                    b = Numbers.parseHexLong(value, 18, Math.min(len, 34));
                }
                if (len > 34) {
                    c = Numbers.parseHexLong(value, 34, Math.min(len, 42));
                }
                if (len > 42) {
                    d = Numbers.parseHexLong(value, 42, Math.min(len, 66));
                }
                Numbers.appendLong256(a, b, c, d, sink);
            } catch (NumericException ignored) {
            }
        }
    }

    private static class Func extends Long256Function implements UnaryFunction {
        private final Function arg;
        private final Long256Impl long256a = new Long256Impl();
        private final Long256Impl long256b = new Long256Impl();

        public Func(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public Long256 getLong256A(Record rec) {
            final CharSequence value = arg.getSymbol(rec);
            if (value == null) {
                return Long256Impl.NULL_LONG256;
            }
            return Z(value, value.length(), long256a);
        }

        @Override
        public Long256 getLong256B(Record rec) {
            final CharSequence value = arg.getSymbol(rec);
            if (value == null) {
                return Long256Impl.NULL_LONG256;
            }
            return Z(value, value.length(), long256b);
        }

        @Override
        public void getLong256(Record rec, CharSink sink) {
            final CharSequence value = arg.getSymbol(rec);
            charSequenceToLong256(sink, value);
        }

        private Long256Impl Z(CharSequence text, int len, Long256Impl long256) {
            if (Long256Util.isValidString(text, len)) {
                try {
                    final long a = Numbers.parseHexLong(text, 2, Math.min(len, 18));
                    long b = 0;
                    long c = 0;
                    long d = 0;
                    if (len > 18) {
                        b = Numbers.parseHexLong(text, 18, Math.min(len, 34));
                    }
                    if (len > 34) {
                        c = Numbers.parseHexLong(text, 34, Math.min(len, 42));
                    }
                    if (len > 42) {
                        d = Numbers.parseHexLong(text, 42, Math.min(len, 66));
                    }
                    long256.setAll(a, b, c, d);
                    return long256;
                } catch (NumericException ignored) {
                }
            }
            return Long256Impl.NULL_LONG256;
        }
    }
}
