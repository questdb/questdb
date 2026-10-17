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

package io.questdb.griffin.engine.functions.regex;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

import java.util.regex.Matcher;

/**
 * Regex match ({@code ~}) over a VARCHAR column.
 * <p>
 * VARCHAR is stored as UTF-8 in native memory, so the primary implementation matches the column's
 * bytes directly: it reads {@link Function#getVarcharA} and hands the native pointer to the Rust
 * {@code regex} engine via FFM (see {@link NativeRegex}), with no UTF-8&rarr;UTF-16 step.
 * <p>
 * This factory does not depend on {@link MatchStrFunctionFactory} at all. The {@code java.util.regex}
 * fallback (used when the native backend is disabled/unavailable, for runtime/dynamic patterns, or
 * for patterns the Rust engine cannot compile) is implemented here as dedicated VARCHAR functions
 * that read {@link Function#getVarcharA} and perform the UTF-8&rarr;{@link CharSequence} bridge
 * inline only at the matcher boundary &mdash; zero-copy {@code asAsciiCharSequence()} for ASCII,
 * reusable-sink transcode otherwise &mdash; because {@code java.util.regex} can only consume a
 * {@link CharSequence}.
 */
public class MatchVarcharFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "~(ØS)";
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
        final int patternPosition = argPositions.getQuick(1);

        if (pattern.isConstant()) {
            if (configuration.isVarcharRegexNativeEnabled() && NativeRegex.AVAILABLE) {
                final CharSequence regex = pattern.getStrA(null);
                if (regex == null) {
                    return BooleanConstant.FALSE;
                }
                final long handle = NativeRegex.compile(regex);
                if (handle != 0) {
                    return new MatchVarcharFunction(value, handle, regex);
                }
                // handle == 0: pattern unsupported/invalid for the Rust engine -> JDK fallback below.
            }
            final Matcher matcher = RegexUtils.createMatcher(pattern, patternPosition);
            if (matcher == null) {
                return BooleanConstant.FALSE;
            }
            return new MatchVarcharConstFunction(value, matcher);
        } else if (pattern.isRuntimeConstant()) {
            return new MatchVarcharRuntimeConstFunction(value, pattern, patternPosition);
        }
        throw SqlException.$(patternPosition, "not implemented: dynamic pattern would be very slow to execute");
    }

    /**
     * UTF-8 -> CharSequence bridge for the JDK fallback, identical in spirit to
     * {@code VarcharFunction.getStrA}: zero-copy for ASCII, transcode into {@code utf16Sink} otherwise.
     */
    private static CharSequence asCharSequence(@NotNull Utf8Sequence us, StringSink utf16Sink) {
        if (us.isAscii()) {
            return us.asAsciiCharSequence();
        }
        utf16Sink.clear();
        Utf8s.utf8ToUtf16(us, utf16Sink);
        return utf16Sink;
    }

    /**
     * Native (Rust {@code regex} via FFM) matcher over the column's UTF-8 bytes.
     */
    private static class MatchVarcharFunction extends BooleanFunction implements UnaryFunction {
        private final String patternStr;
        private final DirectUtf8Sink utf8Sink;
        private final Function value;
        private long handle;

        private MatchVarcharFunction(Function value, long handle, @NotNull CharSequence pattern) {
            this.value = value;
            this.handle = handle;
            this.patternStr = pattern.toString();
            this.utf8Sink = new DirectUtf8Sink(32);
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
            utf8Sink.close();
            if (handle != 0) {
                NativeRegex.free(handle);
                handle = 0;
            }
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            final Utf8Sequence us = value.getVarcharA(rec);
            if (us == null) {
                return false;
            }
            final int size = us.size();
            if (size == 0) {
                return NativeRegex.find(handle, 0, 0);
            }
            final long ptr = us.ptr();
            if (ptr != -1) {
                // Zero-copy fast path: off-heap varchars expose `size` contiguous UTF-8 bytes at
                // `ptr`, valid for the duration of this synchronous `critical` downcall (the pointer
                // is not retained, so the sequence need not be `isStable()`).
                return NativeRegex.find(handle, ptr, size);
            }
            // On-heap sequence (ptr == -1): materialise into a reusable native buffer.
            utf8Sink.clear();
            utf8Sink.put(us);
            return NativeRegex.find(handle, utf8Sink.ptr(), size);
        }

        @Override
        public int getComplexity() {
            return Function.addComplexity(COMPLEXITY_REGEX, UnaryFunction.super.getComplexity());
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value).val(" ~ ").val(patternStr);
        }
    }

    /**
     * java.util.regex fallback for a constant pattern.
     */
    private static class MatchVarcharConstFunction extends BooleanFunction implements UnaryFunction {
        private final Matcher matcher;
        private final StringSink utf16Sink = new StringSink();
        private final Function value;

        private MatchVarcharConstFunction(Function value, @NotNull Matcher matcher) {
            this.value = value;
            this.matcher = matcher;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            final Utf8Sequence us = value.getVarcharA(rec);
            return us != null && matcher.reset(asCharSequence(us, utf16Sink)).find();
        }

        @Override
        public int getComplexity() {
            return Function.addComplexity(COMPLEXITY_REGEX, UnaryFunction.super.getComplexity());
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value).val(" ~ ").val(matcher.pattern().toString());
        }
    }

    /**
     * java.util.regex fallback for a runtime-constant pattern (matcher compiled in {@link #init}).
     */
    private static class MatchVarcharRuntimeConstFunction extends BooleanFunction implements UnaryFunction {
        private final Function pattern;
        private final int patternPosition;
        private final StringSink utf16Sink = new StringSink();
        private final Function value;
        private Matcher matcher;

        private MatchVarcharRuntimeConstFunction(Function value, Function pattern, int patternPosition) {
            this.value = value;
            this.pattern = pattern;
            this.patternPosition = patternPosition;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            if (matcher != null) {
                final Utf8Sequence us = value.getVarcharA(rec);
                return us != null && matcher.reset(asCharSequence(us, utf16Sink)).find();
            }
            return false;
        }

        @Override
        public int getComplexity() {
            return Function.addComplexity(COMPLEXITY_REGEX, UnaryFunction.super.getComplexity());
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            pattern.init(symbolTableSource, executionContext);
            this.matcher = RegexUtils.createMatcher(pattern, patternPosition);
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
            sink.val(value).val(" ~ ").val(pattern.toString());
        }
    }
}
