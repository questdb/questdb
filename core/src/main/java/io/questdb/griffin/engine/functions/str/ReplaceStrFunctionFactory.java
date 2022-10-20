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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

public class ReplaceStrFunctionFactory implements FunctionFactory {

    private static final String SIGNATURE = "replace(SSS)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function withWhat = args.getQuick(2);
        if (withWhat.isConstant() &&
                withWhat.getStrLen(null) < 0) {
            return StrConstant.NULL;
        }

        final Function term = args.getQuick(1);
        if (term.isConstant()) {
            if (term.getStrLen(null) < 0) {
                return StrConstant.NULL;
            } else if (term.getStrLen(null) == 0) {
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

        final int maxLength = configuration.getStrFunctionMaxBufferLength();
        return new Func(value, term, withWhat, maxLength);
    }

    private static class Func extends StrFunction {

        private final StringSink sink = new StringSink();
        private final StringSink sinkB = new StringSink();
        private final Function value;
        private final Function oldSubStr;
        private final Function newSubStr;
        private final int maxLength;

        public Func(Function value, Function oldSubStr, Function newSubStr, int maxLength) {
            this.value = value;
            this.oldSubStr = oldSubStr;
            this.newSubStr = newSubStr;
            this.maxLength = maxLength;
        }

        @Override
        public CharSequence getStr(Record rec) {
            final CharSequence value = this.value.getStr(rec);
            if (value != null) {
                sink.clear();
                return (CharSequence) replace(value, oldSubStr.getStr(rec), newSubStr.getStr(rec), sink);
            }
            return null;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            final CharSequence value = this.value.getStrB(rec);
            if (value != null) {
                sinkB.clear();
                return (CharSequence) replace(value, oldSubStr.getStrB(rec), newSubStr.getStrB(rec), sinkB);
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

        //if result is null then return null; otherwise return sink
        private CharSink replace(@NotNull CharSequence value, CharSequence term, CharSequence withWhat, CharSink sink) throws CairoException {
            int valueLen = value.length();
            if (valueLen < 1) {
                return sink;
            }
            if (term == null || withWhat == null) {
                return null;
            }

            checkLengthLimit(valueLen);

            final int termLen = term.length();
            if (termLen < 1) {
                sink.put(value);
                return sink;
            }

            final int replLen = withWhat.length();

            OUTER:
            for (int i = 0, curLen = 0; i < valueLen; i++) {
                final char c = value.charAt(i);
                if (c == term.charAt(0)) {
                    if (valueLen - i < termLen) {
                        curLen++;
                        checkLengthLimit(curLen);
                        sink.put(value, i, valueLen);
                        break;
                    }

                    for (int k = 1; k < termLen; k++) {
                        if (value.charAt(i + k) != term.charAt(k)) {
                            curLen++;
                            checkLengthLimit(curLen);
                            sink.put(c);
                            continue OUTER;
                        }
                    }

                    curLen += replLen;
                    checkLengthLimit(curLen);
                    sink.put(withWhat);
                    i += termLen - 1;
                } else {
                    curLen++;
                    checkLengthLimit(curLen);
                    sink.put(c);
                }
            }

            return sink;
        }

        private void checkLengthLimit(int length) {
            if (length > maxLength) {
                throw CairoException.nonCritical()
                        .put("breached memory limit set for ").put(SIGNATURE)
                        .put(" [maxLength=").put(maxLength)
                        .put(", requiredLength=").put(length).put(']');
            }
        }

        @Override
        public boolean isConstant() {
            return value.isConstant() && oldSubStr.isConstant() && newSubStr.isConstant();
        }
    }
}
