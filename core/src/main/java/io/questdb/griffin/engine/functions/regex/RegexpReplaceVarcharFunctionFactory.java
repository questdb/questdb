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

package io.questdb.griffin.engine.functions.regex;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.std.*;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

import java.util.regex.Matcher;

/**
 * This is tactical implementation of regex replace over varchar column.
 * It exploits the ability of a varchar column to return a CharSequence view of the sequence.
 */
public class RegexpReplaceVarcharFunctionFactory extends RegexpReplaceStrFunctionFactory {
    @Override
    public String getSignature() {
        return "regexp_replace(ØSS)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function value = args.getQuick(0);
        final Function pattern = args.getQuick(1);
        final int patternPos = argPositions.getQuick(1);
        final Function replacement = args.getQuick(2);
        final int replacementPos = argPositions.getQuick(2);
        validateInputs(pattern, patternPos, replacement, replacementPos);

        if (!pattern.isRuntimeConstant() && !replacement.isRuntimeConstant()) {
            final CharSequence patternStr = pattern.getStrA(null);
            if (patternStr == null) {
                return VarcharConstant.NULL;
            }
            CharSequence replacementStr = replacement.getStrA(null);
            if (replacementStr == null) {
                return VarcharConstant.NULL;
            }
            // Optimize for patterns like "^https?://(?:www\.)?([^/]+)/.*$" and replacements like "$1".
            if (
                    patternStr.length() > 2
                            && patternStr.charAt(0) == '^'
                            && patternStr.charAt(patternStr.length() - 1) == '$'
                            && replacementStr.length() > 1
                            && replacementStr.charAt(0) == '$'
            ) {
                final Matcher matcher = RegexUtils.createMatcher(pattern, patternPos);
                if (matcher == null) {
                    return VarcharConstant.NULL;
                }
                try {
                    final int group = Numbers.parseInt(replacementStr, 1, replacementStr.length());
                    if (group > matcher.groupCount()) {
                        throw SqlException.$(replacementPos, "no group ").put(group);
                    }
                    return new SingleGroupFunc(value, matcher, Chars.toString(replacementStr), group, position);
                } catch (NumericException ignore) {
                }
            }
        }

        final int maxLength = configuration.getStrFunctionMaxBufferLength();
        return new RegexpReplaceStrFunction(value, pattern, patternPos, replacement, maxLength, position);
    }

    private static class SingleGroupFunc extends StrFunction implements UnaryFunction {
        private static final int INITIAL_SINK_CAPACITY = 16;
        private final int functionPos;
        private final int group;
        private final Matcher matcher;
        private final String replacement;
        private final DirectUtf16Sink utf16SinkA;
        private final DirectUtf16Sink utf16SinkB;
        private final Function value;

        public SingleGroupFunc(Function value, Matcher matcher, String replacement, int group, int functionPos) {
            try {
                this.value = value;
                this.matcher = matcher;
                this.replacement = replacement;
                this.group = group;
                this.functionPos = functionPos;
                this.utf16SinkA = new DirectUtf16Sink(INITIAL_SINK_CAPACITY);
                this.utf16SinkB = new DirectUtf16Sink(INITIAL_SINK_CAPACITY);
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
            Misc.free(utf16SinkA);
            Misc.free(utf16SinkB);
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return getStr(rec, utf16SinkA);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStr(rec, utf16SinkB);
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }

        @Override
        public boolean isRuntimeConstant() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("regexp_replace(").val(value).val(',').val(matcher.pattern().toString()).val(',').val(replacement).val(')');
        }

        private CharSequence getStr(Record rec, DirectUtf16Sink utf16Sink) {
            Utf8Sequence us = value.getVarcharA(rec);
            if (us == null) {
                return null;
            }

            utf16Sink.clear();
            if (us.isAscii()) {
                // We want monomorphism in later matcher.reset() call,
                // so we always deal with the sink.
                utf16Sink.putAscii(us);
            } else {
                Utf8s.utf8ToUtf16(us, utf16Sink);
            }

            matcher.reset(utf16Sink);
            try {
                if (!matcher.find()) {
                    // Same here: we want monomorphism for the returned types.
                    return utf16Sink.subSequence(0, utf16Sink.length());
                }
                return utf16Sink.subSequence(matcher.start(group), matcher.end(group));
            } catch (Exception e) {
                throw CairoException.nonCritical().put("regexp_replace failed [position=").put(functionPos).put(", ex=").put(e.getMessage()).put(']');
            }
        }
    }
}

