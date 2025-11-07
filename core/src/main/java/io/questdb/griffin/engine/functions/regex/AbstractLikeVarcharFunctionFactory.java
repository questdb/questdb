/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.eq.EqVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.StartsWithVarcharFunctionFactory;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.SwarUtils;
import io.questdb.std.Transient;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.questdb.griffin.engine.functions.regex.AbstractLikeStrFunctionFactory.countChar;
import static io.questdb.griffin.engine.functions.regex.AbstractLikeStrFunctionFactory.escapeSpecialChars;

public abstract class AbstractLikeVarcharFunctionFactory implements FunctionFactory {

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

        if (pattern.isConstant()) {
            final CharSequence likeSeq = pattern.getStrA(null);
            int len;
            if (likeSeq != null && (len = likeSeq.length()) > 0) {
                if (countChar(likeSeq, '_') == 0 && countChar(likeSeq, '\\') == 0) {
                    final int anyCount = countChar(likeSeq, '%');

                    if (anyCount == 0) {
                        final Utf8String target = new Utf8String(isCaseInsensitive() ? likeSeq.toString().toLowerCase() : likeSeq.toString());
                        return new ConstEqualsVarcharFunction(value, target, isCaseInsensitive());
                    }

                    if (anyCount == 1) {
                        if (len == 1) {
                            // LIKE '%' case
                            final NegatableBooleanFunction notNullFunc = new EqVarcharFunctionFactory.NullCheckFunc(value);
                            notNullFunc.setNegated();
                            return notNullFunc;
                        } else if (likeSeq.charAt(0) == '%') {
                            // LIKE/ILIKE '%abc' case
                            final CharSequence subPattern = likeSeq.subSequence(1, len);
                            if (isCaseInsensitive()) {
                                if (Chars.isAscii(subPattern)) {
                                    return new ConstIEndsWithAsciiFunction(value, subPattern);
                                } else {
                                    return new AbstractLikeStrFunctionFactory.ConstIEndsWithStrFunction(value, subPattern.toString());
                                }
                            } else {
                                return new ConstEndsWithVarcharFunction(value, subPattern);
                            }
                        } else if (likeSeq.charAt(len - 1) == '%') {
                            // LIKE/ILIKE 'abc%' case
                            final CharSequence subPattern = likeSeq.subSequence(0, len - 1);
                            if (isCaseInsensitive()) {
                                if (Chars.isAscii(subPattern)) {
                                    return new ConstIStartsWithAsciiFunction(value, subPattern);
                                } else {
                                    return new AbstractLikeStrFunctionFactory.ConstIStartsWithStrFunction(value, subPattern.toString());
                                }
                            } else {
                                return new ConstStartsWithVarcharFunction(value, subPattern);
                            }
                        }
                    } else if (anyCount == 2) {
                        if (len == 2) {
                            // LIKE '%%' case
                            final NegatableBooleanFunction notNullFunc = new EqVarcharFunctionFactory.NullCheckFunc(value);
                            notNullFunc.setNegated();
                            return notNullFunc;
                        } else if (likeSeq.charAt(0) == '%' && likeSeq.charAt(len - 1) == '%') {
                            // LIKE/ILIKE '%abc%' case
                            final CharSequence subPattern = likeSeq.subSequence(1, len - 1);
                            if (isCaseInsensitive()) {
                                if (Chars.isAscii(subPattern)) {
                                    return new ConstIContainsAsciiFunction(value, subPattern);
                                } else {
                                    return new AbstractLikeStrFunctionFactory.ConstIContainsStrFunction(value, subPattern.toString());
                                }
                            } else {
                                Utf8String u8subPattern = new Utf8String(subPattern);
                                if (u8subPattern.size() <= ConstContainsSwarVarcharFunction.MAX_SIZE) {
                                    return new ConstContainsSwarVarcharFunction(value, u8subPattern);
                                }
                                return new ConstContainsVarcharFunction(value, u8subPattern);
                            }
                        }
                    }
                }

                String p = escapeSpecialChars(likeSeq, null);
                assert p != null;
                int flags = Pattern.DOTALL;
                if (isCaseInsensitive()) {
                    flags |= Pattern.CASE_INSENSITIVE;
                    p = p.toLowerCase();
                }
                return new AbstractLikeStrFunctionFactory.ConstLikeStrFunction(
                        value,
                        Pattern.compile(p, flags).matcher("")
                );
            }
            return BooleanConstant.FALSE;
        }

        if (pattern.isRuntimeConstant()) {
            return new BindLikeVarcharFunction(value, pattern, isCaseInsensitive())
                    ;
        }

        if (pattern.isRuntimeConstant()) {
            // bind variable
            return new AbstractLikeStrFunctionFactory.BindLikeStrFunction(value, pattern, isCaseInsensitive());
        }

        throw SqlException.$(argPositions.getQuick(1), "use constant or bind variable");
    }

    protected abstract boolean isCaseInsensitive();

    /**
     * Optimized variant of {@link ConstContainsVarcharFunction} with SWAR-based contains implementation.
     * Works only for patterns up to 8 bytes in size.
     */
    private static class ConstContainsSwarVarcharFunction extends BooleanFunction implements UnaryFunction {
        private static final int MAX_SIZE = Long.BYTES;
        private final Utf8Sequence pattern; // only used in toPlan
        private final long patternMask;
        private final int patternSize;
        private final long patternWord;
        private final byte searchFirstByte;
        private final long searchWord;
        private final Function value;

        public ConstContainsSwarVarcharFunction(Function value, Utf8Sequence pattern) {
            assert pattern.size() > 0 && pattern.size() <= MAX_SIZE;
            this.value = value;
            this.patternSize = pattern.size();
            this.patternMask = patternSize == MAX_SIZE ? -1L : (1L << 8 * pattern.size()) - 1L;
            long patternWord = 0;
            for (int i = 0, n = pattern.size(); i < n; i++) {
                patternWord |= (long) (pattern.byteAt(i) & 0xff) << (8 * i);
            }
            this.patternWord = patternWord;
            this.searchFirstByte = pattern.byteAt(0);
            this.searchWord = SwarUtils.broadcast(pattern.byteAt(0));
            this.pattern = pattern;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            Utf8Sequence us = value.getVarcharA(rec);
            if (us == null || us.size() < patternSize) {
                return false;
            }

            int size = us.size();

            int i = 0;
            for (int n = size - 7 - patternSize + 1; i < n; i += 8) {
                long zeroBytesWord = SwarUtils.markZeroBytes(us.longAt(i) ^ searchWord);
                while (zeroBytesWord != 0) {
                    // We've found a match for the first pattern byte,
                    // slow down and check the full pattern.
                    int pos = SwarUtils.indexOfFirstMarkedByte(zeroBytesWord);
                    // Check if the pattern matches only for matched first bytes.
                    if (size - i - pos > 7) {
                        // It's safe to load full word.
                        if ((us.longAt(i + pos) & patternMask) == patternWord) {
                            return true;
                        }
                        // Else, we can't call longAt safely near the sequence end,
                        // so construct the word from individual bytes.
                    } else if ((tailWord(us, i + pos) & patternMask) == patternWord) {
                        return true;
                    }
                    zeroBytesWord &= zeroBytesWord - 1;
                }
            }

            // tail
            for (int n = size - patternSize + 1; i < n; i++) {
                if (us.byteAt(i) == searchFirstByte && (tailWord(us, i) & patternMask) == patternWord) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" like ");
            sink.val('%');
            sink.val(pattern);
            sink.val('%');
        }

        private long tailWord(Utf8Sequence us, int i) {
            long tailWord = 0;
            for (int j = 0; j < patternSize; j++) {
                tailWord |= (us.byteAt(i + j) & 0xffL) << (8 * j);
            }
            return tailWord;
        }
    }

    private static class ConstContainsVarcharFunction extends BooleanFunction implements UnaryFunction {
        private final Utf8String pattern;
        private final Function value;

        public ConstContainsVarcharFunction(Function value, Utf8String pattern) {
            this.value = value;
            this.pattern = pattern;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            Utf8Sequence us = value.getVarcharA(rec);
            return us != null && Utf8s.contains(us, pattern);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" like ");
            sink.val('%');
            sink.val(pattern);
            sink.val('%');
        }
    }

    private static class ConstEndsWithVarcharFunction extends BooleanFunction implements UnaryFunction {
        private final Utf8String pattern;
        private final Function value;

        public ConstEndsWithVarcharFunction(Function value, @Transient CharSequence pattern) {
            this.value = value;
            this.pattern = new Utf8String(pattern);
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            Utf8Sequence us = value.getVarcharA(rec);
            return us != null && Utf8s.endsWith(us, pattern);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" like ");
            sink.val('%');
            sink.val(pattern);
        }
    }

    private static class ConstIContainsAsciiFunction extends BooleanFunction implements UnaryFunction {
        private final Utf8String pattern;
        private final Function value;

        public ConstIContainsAsciiFunction(Function value, @Transient CharSequence pattern) {
            this.value = value;
            this.pattern = new Utf8String(pattern.toString().toLowerCase());
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            Utf8Sequence us = value.getVarcharA(rec);
            return us != null && Utf8s.containsLowerCaseAscii(us, pattern);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" ilike ");
            sink.val('%');
            sink.val(pattern);
            sink.val('%');
        }
    }

    private static class ConstIEndsWithAsciiFunction extends BooleanFunction implements UnaryFunction {
        private final Utf8String pattern;
        private final Function value;

        public ConstIEndsWithAsciiFunction(Function value, @Transient CharSequence pattern) {
            this.value = value;
            this.pattern = new Utf8String(pattern.toString().toLowerCase());
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            Utf8Sequence us = value.getVarcharA(rec);
            return us != null && Utf8s.endsWithLowerCaseAscii(us, pattern);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" ilike ");
            sink.val('%');
            sink.val(pattern);
        }
    }

    private static class ConstIStartsWithAsciiFunction extends BooleanFunction implements UnaryFunction {
        private final Utf8String pattern;
        private final Function value;

        public ConstIStartsWithAsciiFunction(Function value, @Transient CharSequence pattern) {
            this.value = value;
            this.pattern = new Utf8String(pattern.toString().toLowerCase());
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            Utf8Sequence us = value.getVarcharA(rec);
            return us != null && Utf8s.startsWithLowerCaseAscii(us, pattern);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" ilike ");
            sink.val(pattern);
            sink.val('%');
        }
    }

    private static class ConstStartsWithVarcharFunction extends StartsWithVarcharFunctionFactory.ConstFunc {
        ConstStartsWithVarcharFunction(Function value, CharSequence startsWith) {
            super(value, startsWith);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" like ");
            sink.val(startsWith);
            sink.val('%');
        }
    }

    private static class BindLikeVarcharFunction extends BooleanFunction implements BinaryFunction {
        private final boolean caseInsensitive;
        private final Function pattern;
        private final Function value;
        private String lastPattern = null;
        private Matcher matcher;
        private boolean useEquality = false;
        private String exactPattern = null;

        public BindLikeVarcharFunction(Function value, Function pattern, boolean caseInsensitive) {
            this.value = value;
            this.pattern = pattern;
            this.caseInsensitive = caseInsensitive;
        }

        @Override
        public boolean getBool(Record rec) {
            if (useEquality) {
                Utf8Sequence us = value.getVarcharA(rec);
                if (us == null) {
                    return false;
                }
                if (caseInsensitive) {
                    return us.toString().toLowerCase().equals(exactPattern);
                } else {
                    return us.toString().equals(exactPattern);
                }
            }

            if (matcher == null) {
                Utf8Sequence us = value.getVarcharA(rec);
                return us != null && matcher.reset(us.toString()).matches();
            }

            return false;
        }

        @Override
        public Function getLeft() {
            return value;
        }

        @Override
        public Function getRight() {
            return pattern;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);
            final CharSequence patternValue = pattern.getStrA(null);
            if (patternValue != null && patternValue.length() > 0) {
                if (countChar(patternValue, '_') == 0 && countChar(patternValue, '\\') == 0 && countChar(patternValue, '%') == 0) {
                    this.useEquality = true;
                    this.matcher = null;
                    this.exactPattern = caseInsensitive ? patternValue.toString().toLowerCase() : patternValue.toString();
                    this.lastPattern = exactPattern;
                    return;
                }

                String p = escapeSpecialChars(patternValue, lastPattern);
                if (p != null) {
                    int flags = Pattern.DOTALL;
                    if (caseInsensitive) {
                        flags |= Pattern.CASE_INSENSITIVE;
                        p = p.toLowerCase();
                    }
                    matcher = Pattern.compile(p, flags).matcher("");
                    lastPattern = p;
                }
            } else {
                lastPattern = null;
                matcher = null;
            }
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            if (useEquality) {
                sink.val(" = ");
                sink.val(exactPattern);
                if (caseInsensitive) {
                    sink.val(" [case-insensitive] ");
                } else {
                    sink.val(" [case-sensitive] ");
                }
            }

            sink.val(" ~ ");
            sink.val(pattern);
            if (!caseInsensitive) {
                sink.val(" [case-sensitive] ");
            }
        }
    }

    private static class ConstEqualsVarcharFunction extends BooleanFunction implements UnaryFunction {
        private final Utf8String pattern;
        private final Function value;
        private final boolean caseInsensitive;

        public ConstEqualsVarcharFunction(Function value, Utf8String pattern, boolean caseInsensitive) {
            this.value = value;
            this.pattern = pattern;
            this.caseInsensitive = caseInsensitive;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            Utf8Sequence us = value.getVarcharA(rec);
            if (us == null) {
                return false;
            }
            if (caseInsensitive) {
                return us.toString().toLowerCase().equals(pattern.toString());
            } else {
                return us.toString().equals(pattern.toString());
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" = ");
            sink.val(pattern);
            if (caseInsensitive) {
                sink.val(" [case-insensitive] ");
            } else {
                sink.val(" [case-sensitive] ");
            }
        }
    }
}
