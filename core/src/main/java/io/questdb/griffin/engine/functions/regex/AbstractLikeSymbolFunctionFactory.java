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
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.*;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.eq.EqSymStrFunctionFactory;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.questdb.griffin.engine.functions.regex.MatchSymbolFunctionFactory.symbolMatches;

public abstract class AbstractLikeSymbolFunctionFactory extends AbstractLikeStrFunctionFactory {

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final SymbolFunction value = (SymbolFunction) args.getQuick(0);
        final Function pattern = args.getQuick(1);

        if (value.isSymbolTableStatic()) {
            if (pattern.isConstant()) {
                final CharSequence likeSeq = pattern.getStrA(null);
                int len;
                if (likeSeq != null && (len = likeSeq.length()) > 0) {
                    if (countChar(likeSeq, '_') == 0 && countChar(likeSeq, '\\') == 0) {
                        final int anyCount = countChar(likeSeq, '%');
                        if (anyCount == 1) {
                            if (len == 1) {
                                // LIKE '%' case
                                final NegatableBooleanFunction notNullFunc = new EqSymStrFunctionFactory.NullCheckFunc(value);
                                notNullFunc.setNegated();
                                return notNullFunc;
                            } else if (likeSeq.charAt(0) == '%') {
                                // LIKE/ILIKE '%abc' case
                                final String patternStr = likeSeq.subSequence(1, len).toString();
                                if (isCaseInsensitive()) {
                                    return new ConstIEndsWithStaticSymbolTableFunction(value, patternStr);
                                } else {
                                    return new ConstEndsWithStaticSymbolTableFunction(value, patternStr);
                                }
                            } else if (likeSeq.charAt(len - 1) == '%') {
                                // LIKE/ILIKE 'abc%' case
                                final String patternStr = likeSeq.subSequence(0, len - 1).toString();
                                if (isCaseInsensitive()) {
                                    return new ConstIStartsWithStaticSymbolTableFunction(value, patternStr);
                                } else {
                                    return new ConstStartsWithStaticSymbolTableFunction(value, patternStr);
                                }
                            }
                        } else if (anyCount == 2) {
                            if (len == 2) {
                                // LIKE '%%' case
                                final NegatableBooleanFunction notNullFunc = new EqSymStrFunctionFactory.NullCheckFunc(value);
                                notNullFunc.setNegated();
                                return notNullFunc;
                            } else if (likeSeq.charAt(0) == '%' && likeSeq.charAt(len - 1) == '%') {
                                // LIKE/ILIKE '%abc%' case
                                final String patternStr = likeSeq.subSequence(1, len - 1).toString();
                                if (isCaseInsensitive()) {
                                    return new ConstIContainsStaticSymbolTableFunction(value, patternStr);
                                } else {
                                    return new ConstContainsStaticSymbolTableFunction(value, patternStr);
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
                    return new ConstLikeStaticSymbolTableFunction(
                            value,
                            Pattern.compile(p, flags).matcher("")
                    );
                }
                return BooleanConstant.FALSE;
            }

            if (pattern.isRuntimeConstant()) {
                // bind variable
                return new BindLikeStaticSymbolTableFunction(value, pattern, isCaseInsensitive());
            }

            throw SqlException.$(argPositions.getQuick(1), "use constant or bind variable");
        }

        return super.newInstance(position, args, argPositions, configuration, sqlExecutionContext);
    }

    private static void extractSymbolKeys(SymbolFunction symbolFun, IntList symbolKeys, Matcher matcher) {
        final StaticSymbolTable symbolTable = symbolFun.getStaticSymbolTable();
        assert symbolTable != null;
        symbolKeys.clear();
        if (matcher != null) {
            for (int i = 0, n = symbolTable.getSymbolCount(); i < n; i++) {
                if (matcher.reset(symbolTable.valueOf(i)).matches()) {
                    symbolKeys.add(i);
                }
            }
        }
    }

    protected abstract boolean isCaseInsensitive();

    private static class BindLikeStaticSymbolTableFunction extends BooleanFunction implements BinaryFunction {
        private final boolean caseInsensitive;
        private final Function pattern;
        private final IntList symbolKeys = new IntList();
        private final SymbolFunction value;
        private boolean initialized;
        private String lastPattern = null;
        private Matcher matcher;

        public BindLikeStaticSymbolTableFunction(SymbolFunction value, Function pattern, boolean caseInsensitive) {
            this.value = value;
            this.pattern = pattern;
            this.caseInsensitive = caseInsensitive;
        }

        @Override
        public boolean getBool(Record rec) {
            if (!initialized) {
                extractSymbolKeys(value, symbolKeys, matcher);
                initialized = true;
            }
            return symbolMatches(value, rec, symbolKeys);
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
            // this is bind variable, we can use it as constant
            final CharSequence patternValue = pattern.getStrA(null);
            if (patternValue != null && patternValue.length() > 0) {
                String p = escapeSpecialChars(patternValue, lastPattern);
                if (p != null) {
                    int flags = Pattern.DOTALL;
                    if (caseInsensitive) {
                        flags |= Pattern.CASE_INSENSITIVE;
                        p = p.toLowerCase();
                    }
                    this.matcher = Pattern.compile(p, flags).matcher("");
                    this.lastPattern = p;
                }
            } else {
                lastPattern = null;
                matcher = null;
            }
            initialized = false;
        }

        @Override
        public boolean isThreadSafe() {
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

    private static class ConstContainsStaticSymbolTableFunction extends BooleanFunction implements UnaryFunction {
        private final String pattern;
        private final IntList symbolKeys = new IntList();
        private final SymbolFunction value;
        private boolean initialized;

        public ConstContainsStaticSymbolTableFunction(SymbolFunction value, String pattern) {
            this.value = value;
            this.pattern = pattern;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            if (!initialized) {
                final StaticSymbolTable symbolTable = value.getStaticSymbolTable();
                assert symbolTable != null;
                symbolKeys.clear();
                for (int i = 0, n = symbolTable.getSymbolCount(); i < n; i++) {
                    if (Chars.contains(symbolTable.valueOf(i), pattern)) {
                        symbolKeys.add(i);
                    }
                }
                initialized = true;
            }
            return symbolMatches(value, rec, symbolKeys);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            initialized = false;
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

    private static class ConstEndsWithStaticSymbolTableFunction extends BooleanFunction implements UnaryFunction {
        private final String pattern;
        private final IntList symbolKeys = new IntList();
        private final SymbolFunction value;
        private boolean initialized;

        public ConstEndsWithStaticSymbolTableFunction(SymbolFunction value, String pattern) {
            this.value = value;
            this.pattern = pattern;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            if (!initialized) {
                final StaticSymbolTable symbolTable = value.getStaticSymbolTable();
                assert symbolTable != null;
                symbolKeys.clear();
                for (int i = 0, n = symbolTable.getSymbolCount(); i < n; i++) {
                    if (Chars.endsWith(symbolTable.valueOf(i), pattern)) {
                        symbolKeys.add(i);
                    }
                }
                initialized = true;
            }
            return symbolMatches(value, rec, symbolKeys);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            initialized = false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" like ");
            sink.val('%');
            sink.val(pattern);
        }
    }

    private static class ConstIContainsStaticSymbolTableFunction extends BooleanFunction implements UnaryFunction {
        private final String pattern;
        private final IntList symbolKeys = new IntList();
        private final SymbolFunction value;
        private boolean initialized;

        public ConstIContainsStaticSymbolTableFunction(SymbolFunction value, String pattern) {
            this.value = value;
            this.pattern = pattern.toLowerCase();
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            if (!initialized) {
                final StaticSymbolTable symbolTable = value.getStaticSymbolTable();
                assert symbolTable != null;
                symbolKeys.clear();
                for (int i = 0, n = symbolTable.getSymbolCount(); i < n; i++) {
                    if (Chars.containsLowerCase(symbolTable.valueOf(i), pattern)) {
                        symbolKeys.add(i);
                    }
                }
                initialized = true;
            }
            return symbolMatches(value, rec, symbolKeys);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            initialized = false;
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

    private static class ConstIEndsWithStaticSymbolTableFunction extends BooleanFunction implements UnaryFunction {
        private final String pattern;
        private final IntList symbolKeys = new IntList();
        private final SymbolFunction value;
        private boolean initialized;

        public ConstIEndsWithStaticSymbolTableFunction(SymbolFunction value, String pattern) {
            this.value = value;
            this.pattern = pattern.toLowerCase();
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            if (!initialized) {
                final StaticSymbolTable symbolTable = value.getStaticSymbolTable();
                assert symbolTable != null;
                symbolKeys.clear();
                for (int i = 0, n = symbolTable.getSymbolCount(); i < n; i++) {
                    if (Chars.endsWithLowerCase(symbolTable.valueOf(i), pattern)) {
                        symbolKeys.add(i);
                    }
                }
                initialized = true;
            }
            return symbolMatches(value, rec, symbolKeys);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            initialized = false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" ilike ");
            sink.val('%');
            sink.val(pattern);
        }
    }

    private static class ConstIStartsWithStaticSymbolTableFunction extends BooleanFunction implements UnaryFunction {
        private final String pattern;
        private final IntList symbolKeys = new IntList();
        private final SymbolFunction value;
        private boolean initialized;

        public ConstIStartsWithStaticSymbolTableFunction(SymbolFunction value, String pattern) {
            this.value = value;
            this.pattern = pattern.toLowerCase();
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            if (!initialized) {
                final StaticSymbolTable symbolTable = value.getStaticSymbolTable();
                assert symbolTable != null;
                symbolKeys.clear();
                for (int i = 0, n = symbolTable.getSymbolCount(); i < n; i++) {
                    if (Chars.startsWithLowerCase(symbolTable.valueOf(i), pattern)) {
                        symbolKeys.add(i);
                    }
                }
                initialized = true;
            }
            return symbolMatches(value, rec, symbolKeys);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            initialized = false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" ilike ");
            sink.val(pattern);
            sink.val('%');
        }
    }

    private static class ConstLikeStaticSymbolTableFunction extends BooleanFunction implements UnaryFunction {
        private final Matcher matcher;
        private final IntList symbolKeys = new IntList();
        private final SymbolFunction value;
        private boolean initialized;

        public ConstLikeStaticSymbolTableFunction(SymbolFunction value, Matcher matcher) {
            this.value = value;
            this.matcher = matcher;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            if (!initialized) {
                extractSymbolKeys(value, symbolKeys, matcher);
                initialized = true;
            }
            return symbolMatches(value, rec, symbolKeys);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            initialized = false;
        }

        @Override
        public boolean isThreadSafe() {
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

    private static class ConstStartsWithStaticSymbolTableFunction extends BooleanFunction implements UnaryFunction {
        private final String pattern;
        private final IntList symbolKeys = new IntList();
        private final SymbolFunction value;
        private boolean initialized;

        public ConstStartsWithStaticSymbolTableFunction(SymbolFunction value, String pattern) {
            this.value = value;
            this.pattern = pattern;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public boolean getBool(Record rec) {
            if (!initialized) {
                final StaticSymbolTable symbolTable = value.getStaticSymbolTable();
                assert symbolTable != null;
                symbolKeys.clear();
                for (int i = 0, n = symbolTable.getSymbolCount(); i < n; i++) {
                    if (Chars.startsWith(symbolTable.valueOf(i), pattern)) {
                        symbolKeys.add(i);
                    }
                }
                initialized = true;
            }
            return symbolMatches(value, rec, symbolKeys);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            initialized = false;
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
