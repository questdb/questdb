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
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import java.util.regex.Matcher;

public class MatchSymbolFunctionFactory implements FunctionFactory {

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
            for (int i = 0, n = symbolTable.getSymbolCount(); i < n; i++) {
                if (matcher.reset(symbolTable.valueOf(i)).find()) {
                    symbolKeys.add(i);
                }
            }
        }
    }

    private static class MatchStaticSymbolTableConstPatternFunction extends BooleanFunction implements UnaryFunction {
        private final Matcher matcher;
        private final SymbolFunction symbolFun;
        private final IntList symbolKeys = new IntList();
        private boolean initialized;

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
            if (!initialized) {
                extractSymbolKeys(symbolFun, symbolKeys, matcher);
                initialized = true;
            }
            return symbolMatches(symbolFun, rec, symbolKeys);
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
            sink.val(symbolFun).val(" ~ ").val(matcher.pattern().toString());
        }
    }

    private static class MatchStaticSymbolTableRuntimeConstPatternFunction extends BooleanFunction implements UnaryFunction {
        private final Function pattern;
        private final int patternPosition;
        private final SymbolFunction symbolFun;
        private final IntList symbolKeys = new IntList();
        private boolean initialized;
        private Matcher matcher;

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
            if (!initialized) {
                extractSymbolKeys(symbolFun, symbolKeys, matcher);
                initialized = true;
            }
            return symbolMatches(symbolFun, rec, symbolKeys);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            pattern.init(symbolTableSource, executionContext);
            matcher = RegexUtils.createMatcher(pattern, patternPosition);
            initialized = false;
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
        public void toPlan(PlanSink sink) {
            sink.val(symbolFun).val(" ~ ").val(pattern.toString());
        }
    }
}
