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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;
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

    private static class Func extends StrFunction implements TernaryFunction {
        private final StringSink aliasSink = new StringSink();
        private final boolean isNewSubStrConstant;
        private final int maxLength;
        private final Function newSubStr;
        private final StringSink newSubStrSink;
        private final Function oldSubStr;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();
        private final Function value;

        public Func(Function value, Function oldSubStr, Function newSubStr, int maxLength) {
            this.value = value;
            this.oldSubStr = oldSubStr;
            this.newSubStr = newSubStr;
            this.maxLength = maxLength;
            this.isNewSubStrConstant = newSubStr.isConstant();
            this.newSubStrSink = isNewSubStrConstant ? null : new StringSink();
        }

        @Override
        public int getComplexity() {
            return Function.addComplexity(COMPLEXITY_STRING_OP, TernaryFunction.super.getComplexity());
        }

        @Override
        public Function getCenter() {
            return oldSubStr;
        }

        @Override
        public Function getLeft() {
            return value;
        }

        @Override
        public Function getRight() {
            return newSubStr;
        }

        // All three arguments may resolve through one symbol table, e.g. lag(s) and s::string
        // over a NOCACHE column, and a non-static table hands out a single view per A/B slot.
        // A non-constant replacement is copied out before the other two reads. When the term
        // read lands on the object the value read returned, it has overwritten the value: copy
        // the term out and read the value again. Each path touches only its own slot, so a
        // caller holding the other slot's value is not disturbed.
        @Override
        public CharSequence getStrA(Record rec) {
            final CharSequence withWhat = copyNewSubStr(newSubStr.getStrA(rec));
            CharSequence value = this.value.getStrA(rec);
            if (value != null) {
                CharSequence term = oldSubStr.getStrA(rec);
                if (value == term) {
                    term = copyAlias(term);
                    value = this.value.getStrA(rec);
                }
                sinkA.clear();
                return (CharSequence) replace(value, term, withWhat, sinkA);
            }
            return null;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            final CharSequence withWhat = copyNewSubStr(newSubStr.getStrB(rec));
            CharSequence value = this.value.getStrB(rec);
            if (value != null) {
                CharSequence term = oldSubStr.getStrB(rec);
                if (value == term) {
                    term = copyAlias(term);
                    value = this.value.getStrB(rec);
                }
                sinkB.clear();
                return (CharSequence) replace(value, term, withWhat, sinkB);
            }
            return null;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("replace(").val(value).val(',').val(oldSubStr).val(',').val(newSubStr).val(')');
        }

        private void checkLengthLimit(int length) {
            if (length > maxLength) {
                throw CairoException.nonCritical()
                        .put("breached memory limit set for ").put(SIGNATURE)
                        .put(" [maxLength=").put(maxLength)
                        .put(", requiredLength=").put(length).put(']');
            }
        }

        // if result is null then return null; otherwise return sink
        private CharSequence copyAlias(CharSequence cs) {
            aliasSink.clear();
            aliasSink.put(cs);
            return aliasSink;
        }

        private CharSequence copyNewSubStr(CharSequence withWhat) {
            if (isNewSubStrConstant || withWhat == null) {
                return withWhat;
            }
            newSubStrSink.clear();
            newSubStrSink.put(withWhat);
            return newSubStrSink;
        }

        private Utf16Sink replace(@NotNull CharSequence value, CharSequence term, CharSequence withWhat, Utf16Sink sink) throws CairoException {
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
    }
}
