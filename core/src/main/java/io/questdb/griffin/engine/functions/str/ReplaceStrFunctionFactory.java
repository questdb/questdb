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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

public class ReplaceStrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "replace(SSS)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        final Function term = args.getQuick(1);
        if (term.isConstant()) {
            if (term.getStrLen(null) < 1) {
                return args.getQuick(0);
            }
        }

        final Function value = args.getQuick(0);
        if (value.isConstant()) {
            int len = value.getStrLen(null);
            if (len < 1) {
                return value;
            }
        }

        return new Func(position, value, term, args.getQuick(2));
    }

    private static class Func extends StrFunction {

        private final StringSink sink = new StringSink();
        private final StringSink sinkB = new StringSink();
        private final Function value;
        private final Function oldSubStr;
        private final Function newSubStr;

        public Func(int position, Function value, Function oldSubStr, Function newSubStr) {
            super(position);
            this.value = value;
            this.oldSubStr = oldSubStr;
            this.newSubStr = newSubStr;
        }

        @Override
        public CharSequence getStr(Record rec) {
            final CharSequence value = this.value.getStr(rec);
            if (value != null) {
                sink.clear();
                replace(value, oldSubStr.getStr(rec), newSubStr.getStr(rec), sink);
                return sink;
            }
            return null;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            final CharSequence value = this.value.getStrB(rec);
            if (value != null) {
                sinkB.clear();
                replace(value, oldSubStr.getStrB(rec), newSubStr.getStrB(rec), sinkB);
                return sinkB;
            }
            return null;
        }

        @Override
        public void getStr(Record rec, CharSink sink) {
            final CharSequence value = this.value.getStrB(rec);
            if (value != null) {
                replace(value, oldSubStr.getStr(rec), newSubStr.getStr(rec), sink);
            }
        }

        private void replace(@NotNull CharSequence value, CharSequence term, CharSequence withWhat, CharSink sink) {
            int valueLen;
            if ((valueLen = value.length()) < 1) {
                return;
            }
            final int termLen;
            if (term == null || (termLen = term.length()) < 1) {
                sink.put(value);
                return;
            }

            OUTER:
            for (int i = 0; i < valueLen; i++) {
                final char c = value.charAt(i);
                if (c == term.charAt(0)) {
                    if (valueLen - i < termLen) {
                        sink.put(value, i, valueLen);
                        break;
                    }

                    for (int k = 1; k < termLen && k + i < valueLen; k++) {
                        if (value.charAt(i + k) != term.charAt(k)) {
                            sink.put(c);
                            continue OUTER;
                        }
                    }

                    sink.put(withWhat);
                    sink.put(value, i + termLen, valueLen);
                    break;
                } else {
                    sink.put(c);
                }
            }
        }

        @Override
        public boolean isConstant() {
            return value.isConstant() && oldSubStr.isConstant() && newSubStr.isConstant();
        }
    }
}
