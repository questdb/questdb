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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

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
                int oneCount = countChar(likeSeq, '_');
                if (oneCount == 0) {
                    if (likeSeq.charAt(len - 1) == '\\') {
                        throw SqlException.parserErr(len - 1, likeSeq, "LIKE pattern must not end with escape character");
                    }

                    int anyCount = countChar(likeSeq, '%');
                    if (anyCount == 1) {
                        if (len == 1) {
                            return BooleanConstant.TRUE; // LIKE '%' case
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
                            return BooleanConstant.TRUE; // LIKE '%%' case
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
                                return new ConstContainsVarcharFunction(value, subPattern);
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
            // bind variable
            return new AbstractLikeStrFunctionFactory.BindLikeStrFunction(value, pattern, isCaseInsensitive());
        }

        throw SqlException.$(argPositions.getQuick(1), "use constant or bind variable");
    }

    protected abstract boolean isCaseInsensitive();

    private static class ConstContainsVarcharFunction extends BooleanFunction implements UnaryFunction {
        private final Utf8String pattern;
        private final Function value;

        public ConstContainsVarcharFunction(Function value, @Transient CharSequence pattern) {
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
            return us != null && Utf8s.contains(us, pattern);
        }

        @Override
        public boolean isReadThreadSafe() {
            return true;
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
        public boolean isReadThreadSafe() {
            return true;
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
        public boolean isReadThreadSafe() {
            return true;
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
        public boolean isReadThreadSafe() {
            return true;
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
        public boolean isReadThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" ilike ");
            sink.val(pattern);
            sink.val('%');
        }
    }

    private static class ConstStartsWithVarcharFunction extends BooleanFunction implements UnaryFunction {
        private final Utf8String pattern;
        private final Function value;

        public ConstStartsWithVarcharFunction(Function value, @Transient CharSequence pattern) {
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
            return us != null && Utf8s.startsWith(us, pattern);
        }

        @Override
        public boolean isReadThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" like ");
            sink.val(pattern);
            sink.val('%');
        }
    }
}
