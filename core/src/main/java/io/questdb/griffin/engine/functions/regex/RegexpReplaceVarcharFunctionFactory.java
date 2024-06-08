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
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.*;
import io.questdb.std.str.*;
import org.jetbrains.annotations.NotNull;

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
                throw SqlException.$(patternPos, "NULL regex");
            }
            CharSequence replacementStr = replacement.getStrA(null);
            if (replacementStr == null) {
                throw SqlException.$(replacementPos, "NULL replacement");
            }
            // Optimize for patterns like "^https?://(?:www\.)?([^/]+)/.*$" and replacements like "$1".
            if (canSkipDecoding(patternStr)
                    && patternStr.length() > 2 && patternStr.charAt(0) == '^' && patternStr.charAt(patternStr.length() - 1) == '$'
                    && replacementStr.length() > 1 && replacementStr.charAt(0) == '$') {
                final Matcher matcher = RegexUtils.createMatcher(pattern, patternPos);
                try {
                    final int group = Numbers.parseInt(replacementStr, 1, replacementStr.length());
                    if (group > matcher.groupCount()) {
                        throw SqlException.$(replacementPos, "No group ").put(group);
                    }
                    return new SingleGroupFunc(value, matcher, Chars.toString(replacementStr), group, position);
                } catch (NumericException ignore) {
                }
            }
        }

        final int maxLength = configuration.getStrFunctionMaxBufferLength();
        return new Func(value, pattern, patternPos, replacement, replacementPos, maxLength, position);
    }

    private boolean canSkipDecoding(CharSequence pattern) {
        // TODO: come up with the strictest possible check
        return Chars.isAscii(pattern);
    }

    private static class DirectAsciiStringView implements CharSequence, DirectUtf8Sequence {
        private boolean ascii;
        private long ptr;
        private int size;
        private boolean stable;

        @Override
        public @NotNull CharSequence asAsciiCharSequence() {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte byteAt(int i) {
            return Unsafe.getUnsafe().getByte(ptr + i);
        }

        @Override
        public char charAt(int i) {
            return (char) Unsafe.getUnsafe().getByte(ptr + i);
        }

        @Override
        public boolean isAscii() {
            return ascii;
        }

        @Override
        public boolean isStable() {
            return stable;
        }

        @Override
        public int length() {
            return size;
        }

        public DirectAsciiStringView of(long ptr, int size, boolean ascii, boolean stable) {
            this.ptr = ptr;
            this.size = size;
            this.ascii = ascii;
            this.stable = stable;
            return this;
        }

        @Override
        public long ptr() {
            return ptr;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public @NotNull CharSequence subSequence(int start, int end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull String toString() {
            return Utf8s.stringFromUtf8Bytes(this);
        }
    }

    /**
     * Should be used only for patterns that can operate on a UTF-16 view of a UTF-8 string,
     * i.e. patterns that only case about ASCII chars.
     */
    private static class SingleGroupFunc extends VarcharFunction implements UnaryFunction {
        private static final int INITIAL_SINK_CAPACITY = 16;
        private final int functionPos;
        private final int group;
        private final Matcher matcher;
        private final String replacement;
        private final DirectUtf8Sink utf8SinkA;
        private final DirectUtf8Sink utf8SinkB;
        private final Function value;
        private final DirectAsciiStringView viewA = new DirectAsciiStringView();
        private final DirectAsciiStringView viewB = new DirectAsciiStringView();

        public SingleGroupFunc(Function value, Matcher matcher, String replacement, int group, int functionPos) {
            try {
                this.value = value;
                this.matcher = matcher;
                this.replacement = replacement;
                this.group = group;
                this.functionPos = functionPos;
                this.utf8SinkA = new DirectUtf8Sink(INITIAL_SINK_CAPACITY);
                this.utf8SinkB = new DirectUtf8Sink(INITIAL_SINK_CAPACITY);
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
            Misc.free(utf8SinkA);
            Misc.free(utf8SinkB);
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public void getVarchar(Record rec, Utf8Sink utf8Sink) {
            utf8Sink.put(getVarchar(rec, utf8SinkA, viewA));
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            return getVarchar(rec, utf8SinkA, viewA);
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            return getVarchar(rec, utf8SinkB, viewB);
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

        private Utf8Sequence getVarchar(Record rec, DirectUtf8Sink utf8Sink, DirectAsciiStringView view) {
            Utf8Sequence us = value.getVarcharA(rec);
            if (us == null) {
                return null;
            }

            if (us.ptr() != -1) {
                view.of(us.ptr(), us.size(), us.isAscii(), us.isStable());
            } else {
                utf8Sink.clear();
                utf8Sink.put(us);
                view.of(utf8Sink.ptr(), utf8Sink.size(), utf8Sink.isAscii(), false);
            }

            matcher.reset(view);
            try {
                if (!matcher.find()) {
                    return view;
                }
                final int start = matcher.start(group);
                final int end = matcher.end(group);
                long ptr = view.ptr() + start;
                int size = end - start;
                // If the string is non-ASCII, we need to recalculate
                // the ASCII flag for the matched substring.
                boolean ascii = view.isAscii() || Utf8s.isAscii(ptr, size);
                return view.of(ptr, size, ascii, view.isStable());
            } catch (Exception e) {
                throw CairoException.nonCritical().put("regexp_replace failed [position=").put(functionPos).put(", ex=").put(e.getMessage()).put(']');
            }
        }
    }
}
