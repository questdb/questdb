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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;


// Returns the given string suitably quoted to be used as an identifier in an SQL statement string.
// Quotes are added only if necessary (i.e., if the string contains non-identifier characters or would be case-folded)
public class QuoteIdentFunctionFactory implements FunctionFactory {

    private static final String NAME = "quote_ident";

    @Override
    public String getSignature() {
        return NAME + "(S)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function arg = args.getQuick(0);
        if (arg.isConstant()) {
            CharSequence val = arg.getStrA(null);
            if (val == null) {
                return StrConstant.NULL;
            } else {
                StringSink quotedVal = QuoteIdentFunction.quote(Misc.getThreadLocalSink(), val);
                return new StrConstant(quotedVal.toString());
            }
        }
        return new QuoteIdentFunction(arg);
    }

    static class QuoteIdentFunction extends StrFunction implements UnaryFunction {
        private final Function arg;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public QuoteIdentFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return quote(sinkA, arg.getStrA(rec));
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return quote(sinkB, arg.getStrA(rec));
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }

        private static StringSink quote(StringSink sink, CharSequence str) {
            if (str == null) {
                return null;
            }

            sink.clear();

            boolean needsQuoting = false;
            int len = str.length();
            for (int i = 0; i < len; i++) {
                char c = str.charAt(i);

                if (!(Character.isLetter(c) ||
                        Character.isDigit(c) ||
                        c == '_' ||
                        c == '$')) {
                    needsQuoting = true;
                    break;
                }
            }

            if (!needsQuoting) {
                sink.put(str);
            } else {
                sink.put('"');
                for (int i = 0; i < len; i++) {
                    char c = str.charAt(i);
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
