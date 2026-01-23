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

package io.questdb.griffin.engine.functions.regex;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
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
        final Function pattern = args.getQuick(1);
        final int patternPos = argPositions.getQuick(1);
        final Function replacement = args.getQuick(2);
        final int replacementPos = argPositions.getQuick(2);
        validateInputs(pattern, patternPos, replacement, replacementPos);

        final Function value = args.getQuick(0);
        final int maxLength = configuration.getStrFunctionMaxBufferLength();
        return new RegexpReplaceStrFunction(value, pattern, patternPos, replacement, maxLength, position);
    }

    protected void validateInputs(Function pattern, int patternPos, Function replacement, int replacementPos) throws SqlException {
        if (!pattern.isConstant() && !pattern.isRuntimeConstant()) {
            throw SqlException.$(patternPos, "not implemented: dynamic pattern would be very slow to execute");
        }
        if (!replacement.isConstant() && !replacement.isRuntimeConstant()) {
            throw SqlException.$(replacementPos, "not implemented: dynamic replacement would be slow to execute");
        }
    }

    protected static class RegexpReplaceStrFunction extends StrFunction implements UnaryFunction {
        private final int functionPos;
        private final int maxLength;
        private final Function pattern;
        private final int patternPos;
        private final Function replacement;
        private final StringBuilderSink sinkA = new StringBuilderSink();
        private final StringBuilderSink sinkB = new StringBuilderSink();
        private final Function value;
        private Matcher matcher;
        private String replacementStr;

        public RegexpReplaceStrFunction(
                Function value,
                Function pattern,
                int patternPos,
                Function replacement,
                int maxLength,
                int functionPos
        ) {
            this.value = value;
            this.pattern = pattern;
            this.patternPos = patternPos;
            this.replacement = replacement;
            this.maxLength = maxLength;
            this.functionPos = functionPos;
        }

        @Override
        public Function getArg() {
            return value;
        }

        public CharSequence getStr(Record rec, StringBuilderSink sink) {
            if (matcher == null || replacementStr == null) {
                return null;
            }

            CharSequence cs = value.getStrA(rec);
            if (cs == null) {
                return null;
            }

            matcher.reset(cs);
            sink.clear();

            try {
                boolean result = matcher.find();
                if (!result) {
                    sink.buffer.append(cs);
                } else {
                    do {
                        if (sink.length() > maxLength) {
                            throw CairoException.critical(0)
                                    .put("breached memory limit set for ").put(SIGNATURE)
                                    .put(" [maxLength=").put(maxLength).put(']');
                        }
                        matcher.appendReplacement(sink.buffer, replacementStr);
                        result = matcher.find();
                    } while (result);
                    matcher.appendTail(sink.buffer);
                }
                return sink;
            } catch (CairoException e) {
                throw e;
            } catch (Throwable e) {
                throw CairoException.critical(0)
                        .put("regexp_replace failed [position=").put(functionPos)
                        .put(", ex=").put(e.getMessage())
                        .put(']');
            }
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return getStr(rec, sinkA);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStr(rec, sinkB);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            pattern.init(symbolTableSource, executionContext);
            matcher = RegexUtils.createMatcher(pattern, patternPos);
            replacement.init(symbolTableSource, executionContext);
            CharSequence cs = replacement.getStrA(null);
            if (cs == null) {
                replacementStr = null;
            } else {
                replacementStr = cs.toString();
            }
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
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("regexp_replace(").val(value).val(',').val(pattern).val(',').val(replacement).val(')');
        }
    }

    private static class StringBuilderSink implements CharSequence {
        private final StringBuilder buffer = new StringBuilder();

        @Override
        public char charAt(int index) {
            return buffer.charAt(index);
        }

        public void clear() {
            buffer.setLength(0);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof CharSequence && Chars.equals(buffer, (CharSequence) obj);
        }

        @Override
        public int hashCode() {
            return Chars.hashCode(buffer);
        }

        @Override
        public int length() {
            return buffer.length();
        }

        @Override
        public @NotNull CharSequence subSequence(int lo, int hi) {
            return buffer.subSequence(lo, hi);
        }

        @Override
        public @NotNull String toString() {
            return buffer.toString();
        }
    }
}
