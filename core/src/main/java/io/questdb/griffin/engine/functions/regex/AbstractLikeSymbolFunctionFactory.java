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
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
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
                if (likeSeq != null && likeSeq.length() > 0) {
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
        private String lastPattern = null;
        private Matcher matcher;

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
            extractSymbolKeys(value, symbolKeys, matcher);
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

    private static class ConstLikeStaticSymbolTableFunction extends BooleanFunction implements UnaryFunction {
        private final Matcher matcher;
        private final IntList symbolKeys = new IntList();
        private final SymbolFunction value;

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
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            extractSymbolKeys(value, symbolKeys, matcher);
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
}
