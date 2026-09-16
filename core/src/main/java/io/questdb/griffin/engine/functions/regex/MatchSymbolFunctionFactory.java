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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;

public class MatchSymbolFunctionFactory implements FunctionFactory {

    // Counts the full O(symbolCount) dictionary regex scans extractSymbolKeys() performs, exactly
    // like AbstractLikeSymbolFunctionFactory's counter does for the LIKE/ILIKE siblings. Retained,
    // donated and re-derived key sets all produce the same rows - only a counter can tell them
    // apart. A plain static boolean guards it: the JIT folds the always-false production branch
    // away, and the tests that flip it drive their queries on the calling thread.
    @TestOnly
    public static boolean isSymbolKeyScanCounterEnabled = false;
    @TestOnly
    public static final AtomicLong testSymbolKeyScans = new AtomicLong();

    public static boolean symbolMatches(Function arg, Record rec, IntList symbolKeys) {
        final int key = arg.getInt(rec);
        if (key != SymbolTable.VALUE_IS_NULL) {
            return symbolKeys.binarySearchUniqueList(key) > -1;
        }
        return false;
    }

    @Override
    public String getSignature() {
        return "~(KS)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final SymbolFunction func = (SymbolFunction) args.getQuick(0);
        final Function pattern = args.getQuick(1);
        final int patternPosition = argPositions.getQuick(1);
        if (func.isSymbolTableStatic()) {
            if (pattern.isConstant()) {
                final Matcher matcher = RegexUtils.createMatcher(pattern, patternPosition);
                if (matcher == null) {
                    return BooleanConstant.FALSE;
                }
                return new MatchStaticSymbolTableConstPatternFunction(func, matcher);
            } else if (pattern.isRuntimeConstant()) {
                return new MatchStaticSymbolTableRuntimeConstPatternFunction(func, pattern, patternPosition);
            }
        } else {
            if (pattern.isConstant()) {
                final Matcher matcher = RegexUtils.createMatcher(pattern, patternPosition);
                if (matcher == null) {
                    return BooleanConstant.FALSE;
                }
                return new MatchStrFunctionFactory.MatchStrConstPatternFunction(func, matcher);
            } else if (pattern.isRuntimeConstant()) {
                return new MatchStrFunctionFactory.MatchStrRuntimeConstPatternFunction(func, pattern, patternPosition);
            }
        }
        throw SqlException.$(patternPosition, "not implemented: dynamic pattern would be very slow to execute");
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
                if (matcher.reset(symbolTable.valueOf(i)).find()) {
                    symbolKeys.add(i);
                }
            }
        }
    }

    private static class MatchStaticSymbolTableConstPatternFunction extends BooleanFunction implements UnaryFunction, SymbolKeySetProvider {
        private final Matcher matcher;
        private final SymbolFunction symbolFun;
        private final IntList symbolKeys = new IntList();
        private boolean initialized;
        private int lastSymbolCount = -1;
        private StaticSymbolTable lastSymbolTable;
        private long lastSymbolTableGeneration = StaticSymbolTable.NO_SYMBOL_TABLE_GENERATION;
        private boolean isStateInherited;

        public MatchStaticSymbolTableConstPatternFunction(SymbolFunction symbolFun, Matcher matcher) {
            this.symbolFun = symbolFun;
            this.matcher = matcher;
        }

        @Override
        public Function getArg() {
            return symbolFun;
        }

        @Override
        public boolean getBool(Record rec) {
            // Retained deliberately, even though no production caller reaches it: init() below sets
            // initialized eagerly, and every route into this function calls init() first. That is exactly
            // what makes AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter.isThreadSafe()
            // able to report true while this function reports false -- its getBool() asserts
            // hasPreparedKeySet for that reason. Workers of an async filter built with
            // perWorkerFilters == null share ONE instance of this function, so a lazy rebuild here would
            // race several threads through the shared Matcher and symbolKeys. The guard is the defence if
            // that invariant is ever broken; do not remove it as dead code.
            if (!initialized) {
                extractSymbolKeys(symbolFun, symbolKeys, matcher);
                initialized = true;
            }
            return symbolMatches(symbolFun, rec, symbolKeys);
        }

        @Override
        public IntList getMatchedSymbolKeys() {
            return symbolKeys;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            if (isStateInherited) {
                isStateInherited = false;
            } else {
                final StaticSymbolTable symbolTable = symbolFun.getStaticSymbolTable();
                assert symbolTable != null;
                final int symbolCount = symbolTable.getSymbolCount();
                final long symbolTableGeneration = symbolTable.getSymbolTableGeneration();
                // The pattern is a compile-time constant, so the matched key set can only change with
                // the dictionary. Mirrors BindLikeStaticSymbolTableFunction: rescan unless the
                // dictionary can prove it is the one the retained keys were derived from.
                if (symbolTableGeneration == StaticSymbolTable.NO_SYMBOL_TABLE_GENERATION
                        || symbolTable != lastSymbolTable
                        || symbolCount != lastSymbolCount
                        || symbolTableGeneration != lastSymbolTableGeneration) {
                    extractSymbolKeys(symbolFun, symbolKeys, matcher);
                    lastSymbolCount = symbolCount;
                    lastSymbolTable = symbolTable;
                    lastSymbolTableGeneration = symbolTableGeneration;
                }
            }
            // Eager: retires getBool()'s lazy-init branch on the query thread before any worker can
            // reach it. See the comment there.
            initialized = true;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof MatchStaticSymbolTableConstPatternFunction target) {
                target.symbolKeys.clear();
                target.symbolKeys.addAll(symbolKeys);
                target.initialized = initialized;
                target.isStateInherited = true;
            }
            UnaryFunction.super.offerStateTo(that);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(symbolFun).val(" ~ ").val(matcher.pattern().toString());
        }
    }

    private static class MatchStaticSymbolTableRuntimeConstPatternFunction extends BooleanFunction implements UnaryFunction, SymbolKeySetProvider {
        private final Function pattern;
        private final int patternPosition;
        private final SymbolFunction symbolFun;
        private final IntList symbolKeys = new IntList();
        private boolean initialized;
        private int lastSymbolCount = -1;
        private StaticSymbolTable lastSymbolTable;
        private long lastSymbolTableGeneration = StaticSymbolTable.NO_SYMBOL_TABLE_GENERATION;
        private Matcher matcher;
        private boolean isStateInherited;

        public MatchStaticSymbolTableRuntimeConstPatternFunction(SymbolFunction symbolFun, Function pattern, int patternPosition) {
            this.symbolFun = symbolFun;
            this.pattern = pattern;
            this.patternPosition = patternPosition;
        }

        @Override
        public Function getArg() {
            return symbolFun;
        }

        @Override
        public boolean getBool(Record rec) {
            // Retained deliberately: see the identical guard in
            // MatchStaticSymbolTableConstPatternFunction.getBool(). init() sets initialized eagerly, so no
            // production caller reaches this branch, and that is the precondition
            // AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter.isThreadSafe() rests on.
            // Here the shared state is also the lazily created matcher field. Do not remove as dead code.
            if (!initialized) {
                extractSymbolKeys(symbolFun, symbolKeys, matcher);
                initialized = true;
            }
            return symbolMatches(symbolFun, rec, symbolKeys);
        }

        @Override
        public IntList getMatchedSymbolKeys() {
            return symbolKeys;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            pattern.init(symbolTableSource, executionContext);
            // RegexUtils.createMatcher() compiles the bind value verbatim, so the compiled pattern's
            // own source doubles as the last-value memo: recompile only when the text changed, and a
            // changed text forces the key set rebuild below no matter what the dictionary says.
            final CharSequence regex = pattern.getStrA(null);
            final boolean isPatternChanged;
            if (regex == null) {
                isPatternChanged = matcher != null;
                matcher = null;
            } else if (matcher == null || !Chars.equals(matcher.pattern().pattern(), regex)) {
                matcher = RegexUtils.createMatcher(pattern, patternPosition);
                isPatternChanged = true;
            } else {
                isPatternChanged = false;
            }
            if (isStateInherited) {
                isStateInherited = false;
            } else {
                final StaticSymbolTable symbolTable = symbolFun.getStaticSymbolTable();
                assert symbolTable != null;
                final int symbolCount = symbolTable.getSymbolCount();
                final long symbolTableGeneration = symbolTable.getSymbolTableGeneration();
                if (isPatternChanged
                        || symbolTableGeneration == StaticSymbolTable.NO_SYMBOL_TABLE_GENERATION
                        || symbolTable != lastSymbolTable
                        || symbolCount != lastSymbolCount
                        || symbolTableGeneration != lastSymbolTableGeneration) {
                    extractSymbolKeys(symbolFun, symbolKeys, matcher);
                    lastSymbolCount = symbolCount;
                    lastSymbolTable = symbolTable;
                    lastSymbolTableGeneration = symbolTableGeneration;
                }
            }
            // Eager: retires getBool()'s lazy-init branch on the query thread before any worker can
            // reach it. See the comment there.
            initialized = true;
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public boolean isRuntimeConstant() {
            return false;
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof MatchStaticSymbolTableRuntimeConstPatternFunction target) {
                target.symbolKeys.clear();
                target.symbolKeys.addAll(symbolKeys);
                target.initialized = initialized;
                target.isStateInherited = true;
            }
            UnaryFunction.super.offerStateTo(that);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(symbolFun).val(" ~ ").val(pattern.toString());
        }
    }
}
