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

package io.questdb.griffin.engine.functions.regex;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

import java.util.regex.Matcher;

public class RegexpReplaceStrFunctionFactory implements FunctionFactory {

    private static final String SIGNATURE = "regexp_replace(SSS)";

    @Override
    public String getSignature() {
        return SIGNATURE;
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
        if (!pattern.isConstant() && !pattern.isRuntimeConstant()) {
            throw SqlException.$(patternPos, "not implemented: dynamic pattern would be very slow to execute");
        }

        final Function replacement = args.getQuick(2);
        final int replacementPos = argPositions.getQuick(2);
        if (!replacement.isConstant() && !replacement.isRuntimeConstant()) {
            throw SqlException.$(patternPos, "not implemented: dynamic replacement would be slow to execute");
        }

        final int maxLength = configuration.getStrFunctionMaxBufferLength();
        return new Func(value, pattern, patternPos, replacement, replacementPos, maxLength);
    }

    private static class Func extends StrFunction implements UnaryFunction {
        private final Function value;
        private final Function pattern;
        private final int patternPos;
        private final Function replacement;
        private final int replacementPos;
        private final int maxLength;
        private Matcher matcher;
        private String replacementStr;
        private final StringBufferSink sink = new StringBufferSink();
        private final StringBufferSink sinkB = new StringBufferSink();

        public Func(Function value, Function pattern, int patternPos, Function replacement, int replacementPos, int maxLength) {
            this.value = value;
            this.pattern = pattern;
            this.patternPos = patternPos;
            this.replacement = replacement;
            this.replacementPos = replacementPos;
            this.maxLength = maxLength;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public CharSequence getStr(Record rec) {
            return getStr(rec, sink);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStr(rec, sinkB);
        }

        public CharSequence getStr(Record rec, StringBufferSink sink) {
            CharSequence cs = value.getStr(rec);
            if (cs == null) {
                return null;
            }

            matcher.reset(cs);
            sink.clear();

            boolean result = matcher.find();
            if (!result) {
                sink.buffer.append(cs);
            } else {
                do {
                    if (sink.length() > maxLength) {
                        throw CairoException.nonCritical()
                                .put("breached memory limit set for ").put(SIGNATURE)
                                .put(" [maxLength=").put(maxLength).put(']');
                    }
                    matcher.appendReplacement(sink.buffer, replacementStr);
                    result = matcher.find();
                } while (result);
                matcher.appendTail(sink.buffer);
            }
            return sink;
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public boolean isRuntimeConstant() {
            return false;
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            pattern.init(symbolTableSource, executionContext);
            matcher = RegexUtils.createMatcher(pattern, patternPos);
            replacement.init(symbolTableSource, executionContext);
            CharSequence cs = replacement.getStr(null);
            if (cs == null) {
                throw SqlException.$(replacementPos, "NULL replacement");
            }
            replacementStr = cs.toString();
        }
    }

    // TODO(puzpuzpuz):
    //  Get rid of this class in favor of StringSink once we drop support for Java 8 where j.u.r.Matcher
    //  has no method overloads for StringBuilder.
    private static class StringBufferSink implements CharSequence {
        private final StringBuffer buffer = new StringBuffer();

        public void clear() {
            buffer.setLength(0);
        }

        @Override
        public int hashCode() {
            return Chars.hashCode(buffer);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof CharSequence && Chars.equals(buffer, (CharSequence) obj);
        }


        @Override
        public @NotNull String toString() {
            return buffer.toString();
        }

        @Override
        public int length() {
            return buffer.length();
        }

        @Override
        public char charAt(int index) {
            return buffer.charAt(index);
        }

        @Override
        public @NotNull CharSequence subSequence(int lo, int hi) {
            return buffer.subSequence(lo, hi);
        }
    }
}
