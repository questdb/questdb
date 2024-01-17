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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractLikeStrFunctionFactory implements FunctionFactory {

    public static String escapeSpecialChars(CharSequence pattern, CharSequence prev) throws SqlException {
        int len = pattern.length();

        StringSink sink = Misc.getThreadLocalSink();
        for (int i = 0; i < len; i++) {
            char c = pattern.charAt(i);
            if (c == '_') {
                sink.put('.');
            } else if (c == '%') {
                sink.put(".*?");
            } else if ("[](){}.*+?$^|#".indexOf(c) != -1) {
                sink.put("\\");
                sink.put(c);
            } else if (c == '\\') {
                i++;
                if (i >= len) {
                    throw SqlException.parserErr(i - 1, pattern, "LIKE pattern must not end with escape character");
                }
                c = pattern.charAt(i);
                if ("[](){}.*+?$^|#\\".indexOf(c) != -1) {
                    sink.put("\\");
                    sink.put(c);
                } else {
                    sink.put(c);
                }
            } else {
                sink.put(c);
            }
        }

        if (Chars.equalsNc(sink, prev)) {
            return null;
        }
        return Chars.toString(sink);
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

        if (pattern.isConstant()) {
            final CharSequence likeSeq = pattern.getStr(null);
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
                            final String patternStr = likeSeq.subSequence(1, len).toString();
                            if (isCaseInsensitive()) {
                                return new ConstIEndsWithStrFunction(value, patternStr);
                            } else {
                                return new ConstEndsWithStrFunction(value, patternStr);
                            }
                        } else if (likeSeq.charAt(len - 1) == '%') {
                            // LIKE/ILIKE 'abc%' case
                            final String patternStr = likeSeq.subSequence(0, len - 1).toString();
                            if (isCaseInsensitive()) {
                                return new ConstIStartsWithStrFunction(value, patternStr);
                            } else {
                                return new ConstStartsWithStrFunction(value, patternStr);
                            }
                        }
                    } else if (anyCount == 2) {
                        if (len == 2) {
                            return BooleanConstant.TRUE; // LIKE '%%' case
                        } else if (likeSeq.charAt(0) == '%' && likeSeq.charAt(len - 1) == '%') {
                            // LIKE/ILIKE '%abc%' case
                            final String patternStr = likeSeq.subSequence(1, len - 1).toString();
                            if (isCaseInsensitive()) {
                                return new ConstIContainsStrFunction(value, patternStr);
                            } else {
                                return new ConstContainsStrFunction(value, patternStr);
                            }
                        }
                    }
                }

                String p = escapeSpecialChars(likeSeq, null);
                assert p != null;
                int flags = Pattern.DOTALL;
                if (isCaseInsensitive()) {
                    flags |= Pattern.CASE_INSENSITIVE;
                }
                return new ConstLikeStrFunction(
                        value,
                        Pattern.compile(p, flags).matcher("")
                );
            }
            return BooleanConstant.FALSE;
        }

        if (pattern.isRuntimeConstant()) {
            // bind variable
            return new BindLikeStrFunction(value, pattern, isCaseInsensitive());
        }

        throw SqlException.$(argPositions.getQuick(1), "use constant or bind variable");
    }

    private static int countChar(@NotNull CharSequence seq, char c) {
        int count = 0;
        for (int i = 0, n = seq.length(); i < n; i++) {
            if (seq.charAt(i) == c) {
                count++;
            }
        }
        return count;
    }

    protected abstract boolean isCaseInsensitive();

    private static class BindLikeStrFunction extends BooleanFunction implements UnaryFunction {
        private final boolean caseInsensitive;
        private final Function pattern;
        private final Function value;
        private String lastPattern = null;
        private Matcher matcher;

        public BindLikeStrFunction(Function value, Function pattern, boolean caseInsensitive) {
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
            if (matcher != null) {
                CharSequence cs = getArg().getStr(rec);
                return cs != null && matcher.reset(cs).matches();
            }
            return false;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            value.init(symbolTableSource, executionContext);
            pattern.init(symbolTableSource, executionContext);
            // this is bind variable, we can use it as constant
            final CharSequence patternValue = pattern.getStr(null);
            if (patternValue != null && patternValue.length() > 0) {
                final String p = escapeSpecialChars(patternValue, lastPattern);
                if (p != null) {
                    int flags = Pattern.DOTALL;
                    if (caseInsensitive) {
                        flags |= Pattern.CASE_INSENSITIVE;
                    }
                    this.matcher = Pattern.compile(p, flags).matcher("");
                    this.lastPattern = p;
                }
            } else {
                lastPattern = null;
                matcher = null;
            }
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            // impl is regex
            sink.val(" ~ ");
            sink.val(pattern);
            if (!caseInsensitive) {
                sink.val(" [case-sensitive]");
            }
        }
    }

    private static class ConstContainsStrFunction extends BooleanFunction implements UnaryFunction {
        private final String pattern;
        private final Function value;

        public ConstContainsStrFunction(Function value, String pattern) {
            this.value = value;
            this.pattern = pattern;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence cs = getArg().getStr(rec);
            if (cs == null) {
                return false;
            }
            return Chars.contains(cs, pattern);
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

    private static class ConstEndsWithStrFunction extends BooleanFunction implements UnaryFunction {
        private final String pattern;
        private final Function value;

        public ConstEndsWithStrFunction(Function value, String pattern) {
            this.value = value;
            this.pattern = pattern;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence cs = getArg().getStr(rec);
            return Chars.endsWith(cs, pattern);
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

    private static class ConstIContainsStrFunction extends BooleanFunction implements UnaryFunction {
        private final String pattern;
        private final Function value;

        public ConstIContainsStrFunction(Function value, String pattern) {
            this.value = value;
            this.pattern = pattern.toLowerCase();
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence cs = getArg().getStr(rec);
            if (cs == null) {
                return false;
            }
            return Chars.containsLowerCase(cs, pattern);
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

    private static class ConstIEndsWithStrFunction extends BooleanFunction implements UnaryFunction {
        private final String pattern;
        private final Function value;

        public ConstIEndsWithStrFunction(Function value, String pattern) {
            this.value = value;
            this.pattern = pattern.toLowerCase();
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence cs = getArg().getStr(rec);
            return Chars.endsWithLowerCase(cs, pattern);
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

    private static class ConstIStartsWithStrFunction extends BooleanFunction implements UnaryFunction {
        private final String pattern;
        private final Function value;

        public ConstIStartsWithStrFunction(Function value, String pattern) {
            this.value = value;
            this.pattern = pattern.toLowerCase();
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence cs = getArg().getStr(rec);
            return Chars.startsWithLowerCase(cs, pattern);
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

    private static class ConstLikeStrFunction extends BooleanFunction implements UnaryFunction {
        private final Matcher matcher;
        private final Function value;

        public ConstLikeStrFunction(Function value, Matcher matcher) {
            this.value = value;
            this.matcher = matcher;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence cs = getArg().getStr(rec);
            return cs != null && matcher.reset(cs).matches();
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            // impl is regex
            sink.val(" ~ ");
            sink.val(matcher.pattern().toString());
            if ((matcher.pattern().flags() & Pattern.CASE_INSENSITIVE) != 0) {
                sink.val(" [case-sensitive]");
            }
        }
    }

    private static class ConstStartsWithStrFunction extends BooleanFunction implements UnaryFunction {
        private final String pattern;
        private final Function value;

        public ConstStartsWithStrFunction(Function value, String pattern) {
            this.value = value;
            this.pattern = pattern;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence cs = getArg().getStr(rec);
            return Chars.startsWith(cs, pattern);
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
