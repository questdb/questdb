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
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.eq.EqSymStrFunctionFactory;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.questdb.griffin.engine.functions.regex.MatchSymbolFunctionFactory.symbolMatches;

public abstract class AbstractLikeSymbolFunctionFactory extends AbstractLikeStrFunctionFactory {

    // Counts the full O(symbolCount) dictionary regex scans extractSymbolKeys() performs. The
    // per-worker clones of a non-thread-safe pattern predicate inherit the owner's matched key set
    // (offerStateTo -> stateInherited) precisely so that a cursor open costs ONE scan, not one per
    // clone, and only a counter can tell an inherited key set from a re-derived one - both produce
    // the same rows. A plain static boolean guards it: the JIT folds the always-false production
    // branch away, and the tests that flip it drive their queries on the calling thread.
    @TestOnly
    public static boolean isSymbolKeyScanCounterEnabled = false;
    @TestOnly
    public static final AtomicLong testSymbolKeyScans = new AtomicLong();

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
            if (isSymbolKeyScanCounterEnabled) {
                testSymbolKeyScans.incrementAndGet();
            }
            for (int i = 0, n = symbolTable.getSymbolCount(); i < n; i++) {
                if (matcher.reset(symbolTable.valueOf(i)).matches()) {
                    symbolKeys.add(i);
                }
            }
        }
    }

    protected abstract boolean isCaseInsensitive();

    private static class BindLikeStaticSymbolTableFunction extends BooleanFunction implements BinaryFunction, SymbolKeySetProvider {
        private final boolean caseInsensitive;
        private final Function pattern;
        private final IntList symbolKeys = new IntList();
        private final SymbolFunction value;
        private String lastPattern = null;
        private int lastSymbolCount = -1;
        private StaticSymbolTable lastSymbolTable;
        private long lastSymbolTableGeneration = StaticSymbolTable.NO_SYMBOL_TABLE_GENERATION;
        private Matcher matcher;
        private boolean stateInherited = false;
        private boolean stateShared = false;

        public BindLikeStaticSymbolTableFunction(SymbolFunction value, Function pattern, boolean caseInsensitive) {
            this.value = value;
            this.pattern = pattern;
            this.caseInsensitive = caseInsensitive;
        }

        @Override
        public boolean getBool(Record rec) {
            return symbolMatches(value, rec, symbolKeys);
        }

        @Override
        public IntList getMatchedSymbolKeys() {
            return symbolKeys;
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
            if (stateInherited) {
                // A per-worker clone: the owner ran this method against the CURRENT bind value earlier
                // in this same init cycle and donated its key set (offerStateTo), so re-deriving it
                // here would only repeat the owner's O(symbolCount) dictionary scan. The donation is
                // what re-arms the shortcut - every open of every donating atom offers state to each
                // clone immediately before initializing it - so a re-bound variable cannot leave a
                // clone holding the previous pattern's keys.
                stateInherited = false;
                return;
            }
            this.stateShared = false;
            final CharSequence patternValue = pattern.getStrA(null);
            if (patternValue != null && patternValue.length() > 0) {
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

                final StaticSymbolTable symbolTable = value.getStaticSymbolTable();
                assert symbolTable != null;
                final int symbolCount = symbolTable.getSymbolCount();
                final long symbolTableGeneration = symbolTable.getSymbolTableGeneration();
                if (p != null
                        || symbolTableGeneration == StaticSymbolTable.NO_SYMBOL_TABLE_GENERATION
                        || symbolTable != lastSymbolTable
                        || symbolCount != lastSymbolCount
                        || symbolTableGeneration != lastSymbolTableGeneration) {
                    extractSymbolKeys(value, symbolKeys, matcher);
                    lastSymbolCount = symbolCount;
                    lastSymbolTable = symbolTable;
                    lastSymbolTableGeneration = symbolTableGeneration;
                }
            } else {
                lastPattern = null;
                lastSymbolCount = -1;
                lastSymbolTable = null;
                lastSymbolTableGeneration = StaticSymbolTable.NO_SYMBOL_TABLE_GENERATION;
                matcher = null;
                symbolKeys.clear();
            }
        }

        @Override
        public boolean isThreadSafe() {
            return value.isThreadSafe();
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof BindLikeStaticSymbolTableFunction thatP) {
                thatP.symbolKeys.clear();
                thatP.symbolKeys.addAll(this.symbolKeys);
                thatP.stateInherited = this.stateShared = true;
            }
            BinaryFunction.super.offerStateTo(that);
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

            if (stateShared) {
                sink.val(" [state-shared]");
            }
        }
    }

    private static class ConstContainsStaticSymbolTableFunction extends BooleanFunction implements UnaryFunction, SymbolKeySetProvider {
        private final String pattern;
        private final IntList symbolKeys = new IntList();
        private final SymbolFunction value;
        private boolean stateInherited = false;
        private boolean stateShared = false;

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
            return symbolMatches(value, rec, symbolKeys);
        }

        @Override
        public IntList getMatchedSymbolKeys() {
            return symbolKeys;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            if (stateInherited) {
                return;
            }
            this.stateShared = false;
            final StaticSymbolTable symbolTable = value.getStaticSymbolTable();
            assert symbolTable != null;
            symbolKeys.clear();
            for (int i = 0, n = symbolTable.getSymbolCount(); i < n; i++) {
                if (Chars.contains(symbolTable.valueOf(i), pattern)) {
                    symbolKeys.add(i);
                }
            }
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof ConstContainsStaticSymbolTableFunction thatP) {
                thatP.symbolKeys.clear();
                thatP.symbolKeys.addAll(this.symbolKeys);
                thatP.stateInherited = this.stateShared = true;
            }
            UnaryFunction.super.offerStateTo(that);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" like ");
            sink.val('%');
            sink.val(pattern);
            sink.val('%');
            if (stateShared) {
                sink.val(" [state-shared]");
            }
        }
    }

    private static class ConstEndsWithStaticSymbolTableFunction extends BooleanFunction implements UnaryFunction, SymbolKeySetProvider {
        private final String pattern;
        private final IntList symbolKeys = new IntList();
        private final SymbolFunction value;
        private boolean stateInherited = false;
        private boolean stateShared = false;

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
            return symbolMatches(value, rec, symbolKeys);
        }

        @Override
        public IntList getMatchedSymbolKeys() {
            return symbolKeys;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            if (stateInherited) {
                return;
            }
            this.stateShared = false;
            final StaticSymbolTable symbolTable = value.getStaticSymbolTable();
            assert symbolTable != null;
            symbolKeys.clear();
            for (int i = 0, n = symbolTable.getSymbolCount(); i < n; i++) {
                if (Chars.endsWith(symbolTable.valueOf(i), pattern)) {
                    symbolKeys.add(i);
                }
            }
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof ConstEndsWithStaticSymbolTableFunction thatP) {
                thatP.symbolKeys.clear();
                thatP.symbolKeys.addAll(this.symbolKeys);
                thatP.stateInherited = this.stateShared = true;
            }
            UnaryFunction.super.offerStateTo(that);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" like ");
            sink.val('%');
            sink.val(pattern);
            if (stateShared) {
                sink.val(" [state-shared]");
            }
        }
    }

    private static class ConstIContainsStaticSymbolTableFunction extends BooleanFunction implements UnaryFunction, SymbolKeySetProvider {
        private final String pattern;
        private final IntList symbolKeys = new IntList();
        private final SymbolFunction value;
        private boolean stateInherited = false;
        private boolean stateShared = false;

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
            return symbolMatches(value, rec, symbolKeys);
        }

        @Override
        public IntList getMatchedSymbolKeys() {
            return symbolKeys;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            if (stateInherited) {
                return;
            }
            this.stateShared = false;
            final StaticSymbolTable symbolTable = value.getStaticSymbolTable();
            assert symbolTable != null;
            symbolKeys.clear();
            for (int i = 0, n = symbolTable.getSymbolCount(); i < n; i++) {
                if (Chars.containsLowerCase(symbolTable.valueOf(i), pattern)) {
                    symbolKeys.add(i);
                }
            }
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof ConstIContainsStaticSymbolTableFunction thatP) {
                thatP.symbolKeys.clear();
                thatP.symbolKeys.addAll(this.symbolKeys);
                thatP.stateInherited = this.stateShared = true;
            }
            UnaryFunction.super.offerStateTo(that);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" ilike ");
            sink.val('%');
            sink.val(pattern);
            sink.val('%');
            if (stateShared) {
                sink.val(" [state-shared]");
            }
        }
    }

    private static class ConstIEndsWithStaticSymbolTableFunction extends BooleanFunction implements UnaryFunction, SymbolKeySetProvider {
        private final String pattern;
        private final IntList symbolKeys = new IntList();
        private final SymbolFunction value;
        private boolean stateInherited = false;
        private boolean stateShared = false;

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
            return symbolMatches(value, rec, symbolKeys);
        }

        @Override
        public IntList getMatchedSymbolKeys() {
            return symbolKeys;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            if (stateInherited) {
                return;
            }
            this.stateShared = false;
            final StaticSymbolTable symbolTable = value.getStaticSymbolTable();
            assert symbolTable != null;
            symbolKeys.clear();
            for (int i = 0, n = symbolTable.getSymbolCount(); i < n; i++) {
                if (Chars.endsWithLowerCase(symbolTable.valueOf(i), pattern)) {
                    symbolKeys.add(i);
                }
            }
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof ConstIEndsWithStaticSymbolTableFunction thatP) {
                thatP.symbolKeys.clear();
                thatP.symbolKeys.addAll(this.symbolKeys);
                thatP.stateInherited = this.stateShared = true;
            }
            UnaryFunction.super.offerStateTo(that);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" ilike ");
            sink.val('%');
            sink.val(pattern);
            if (stateShared) {
                sink.val(" [state-shared]");
            }
        }
    }

    private static class ConstIStartsWithStaticSymbolTableFunction extends BooleanFunction implements UnaryFunction, SymbolKeySetProvider {
        private final String pattern;
        private final IntList symbolKeys = new IntList();
        private final SymbolFunction value;
        private boolean stateInherited = false;
        private boolean stateShared = false;

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
            return symbolMatches(value, rec, symbolKeys);
        }

        @Override
        public IntList getMatchedSymbolKeys() {
            return symbolKeys;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            if (stateInherited) {
                return;
            }
            this.stateShared = false;
            final StaticSymbolTable symbolTable = value.getStaticSymbolTable();
            assert symbolTable != null;
            symbolKeys.clear();
            for (int i = 0, n = symbolTable.getSymbolCount(); i < n; i++) {
                if (Chars.startsWithLowerCase(symbolTable.valueOf(i), pattern)) {
                    symbolKeys.add(i);
                }
            }
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof ConstIStartsWithStaticSymbolTableFunction thatP) {
                thatP.symbolKeys.clear();
                thatP.symbolKeys.addAll(this.symbolKeys);
                thatP.stateInherited = this.stateShared = true;
            }
            UnaryFunction.super.offerStateTo(that);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" ilike ");
            sink.val(pattern);
            sink.val('%');
            if (stateShared) {
                sink.val(" [state-shared]");
            }
        }
    }

    private static class ConstLikeStaticSymbolTableFunction extends BooleanFunction implements UnaryFunction, SymbolKeySetProvider {
        private final Matcher matcher;
        private final IntList symbolKeys = new IntList();
        private final SymbolFunction value;
        private boolean stateInherited = false;
        private boolean stateShared = false;

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
            return symbolMatches(value, rec, symbolKeys);
        }

        @Override
        public IntList getMatchedSymbolKeys() {
            return symbolKeys;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            if (stateInherited) {
                return;
            }
            this.stateShared = false;
            extractSymbolKeys(value, symbolKeys, matcher);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof ConstLikeStaticSymbolTableFunction thatP) {
                thatP.symbolKeys.clear();
                thatP.symbolKeys.addAll(this.symbolKeys);
                thatP.stateInherited = this.stateShared = true;
            }
            UnaryFunction.super.offerStateTo(that);
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

            if (stateShared) {
                sink.val(" [state-shared]");
            }
        }
    }

    private static class ConstStartsWithStaticSymbolTableFunction extends BooleanFunction implements UnaryFunction, SymbolKeySetProvider {
        private final String pattern;
        private final IntList symbolKeys = new IntList();
        private final SymbolFunction value;
        private boolean stateInherited = false;
        private boolean stateShared = false;

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
            return symbolMatches(value, rec, symbolKeys);
        }

        @Override
        public IntList getMatchedSymbolKeys() {
            return symbolKeys;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            if (stateInherited) {
                return;
            }
            this.stateShared = false;
            final StaticSymbolTable symbolTable = value.getStaticSymbolTable();
            assert symbolTable != null;
            symbolKeys.clear();
            for (int i = 0, n = symbolTable.getSymbolCount(); i < n; i++) {
                if (Chars.startsWith(symbolTable.valueOf(i), pattern)) {
                    symbolKeys.add(i);
                }
            }
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof ConstStartsWithStaticSymbolTableFunction thatP) {
                thatP.symbolKeys.clear();
                thatP.symbolKeys.addAll(this.symbolKeys);
                thatP.stateInherited = this.stateShared = true;
            }
            UnaryFunction.super.offerStateTo(that);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
            sink.val(" like ");
            sink.val(pattern);
            sink.val('%');
            if (stateShared) {
                sink.val(" [state-shared]");
            }
        }
    }
}
