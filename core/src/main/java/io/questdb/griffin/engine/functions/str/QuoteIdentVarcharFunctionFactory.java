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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;


// Returns the given varchar suitably quoted to be used as an identifier in an SQL statement string.
// Quotes are added only if necessary (i.e., if the varchar contains non-identifier characters or would be case-folded)
public class QuoteIdentVarcharFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "quote_ident(Ø)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        Function arg = args.getQuick(0);
        if (arg.isConstant()) {
            Utf8Sequence val = arg.getVarcharA(null);
            if (val == null) {
                return VarcharConstant.NULL;
            } else {
                Utf8StringSink quotedVal = QuoteIdentVarcharFunctionFactory
                        .QuoteIdentVarcharFunction
                        .quote(Misc.getThreadLocalUtf8Sink(), val);
                return new VarcharConstant(quotedVal);
            }
        }
        return new QuoteIdentVarcharFunctionFactory.QuoteIdentVarcharFunction(arg);
    }

    static class QuoteIdentVarcharFunction extends VarcharFunction implements UnaryFunction {
        private final Function arg;
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();

        public QuoteIdentVarcharFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public String getName() {
            return "quote_ident";
        }

        @Override
        public @Nullable Utf8Sequence getVarcharA(Record rec) {
            return quote(sinkA, arg.getVarcharA(rec));
        }

        @Override
        public @Nullable Utf8Sequence getVarcharB(Record rec) {
            return quote(sinkB, arg.getVarcharB(rec));
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        /**
         * Quotes the varchar.
         * Surrogate pairs are NOT handled. Surrogate pairs will be converted to `0` ie `NUL`.
         *
         * @param sink    output varchar sink
         * @param utf8Str input varchar
         * @return a possibly quoted varchar
         */
        private static Utf8StringSink quote(Utf8StringSink sink, Utf8Sequence utf8Str) {
            if (utf8Str == null) {
                return null;
            }

            sink.clear();

            boolean needsQuoting = false;
            int len = Utf8s.length(utf8Str);

            for (int i = 0, n; i < len; i += n) {
                final int pc = Utf8s.utf8CharDecode(utf8Str, i); // current tuple packed char
                n = Numbers.decodeLowShort(pc); // number of decoded bytes
                final char c = (char) Numbers.decodeHighShort(pc); // utf16 char

                if (!(Character.isLetter(c) ||
                        Character.isDigit(c) ||
                        c == '_' ||
                        c == '$')) {
                    needsQuoting = true;
                    break;
                }
            }

            if (!needsQuoting) {
                sink.put(utf8Str);
            } else {
                sink.put('"');
                for (int i = 0, n; i < len; i += n) {
                    final int pc = Utf8s.utf8CharDecode(utf8Str, i); // current tuple packed char
                    n = Numbers.decodeLowShort(pc); // number of decoded bytes
                    final char c = (char) Numbers.decodeHighShort(pc); // utf16 char
                    if (c != '"') {
                        sink.put(c);
                    } else {
                        sink.put('"').put('"');
                    }
                }
                sink.put('"');
            }

            return sink;
        }
    }
}
