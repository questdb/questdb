/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class MatchStrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "~(SS)";
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
            return new MatchConstPatternFunction(value, createMatcher(pattern, patternPosition));
        } else if (pattern.isRuntimeConstant()) {
            return new MatchRuntimeConstPatternFunction(value, pattern, patternPosition);
        }
        throw SqlException.$(patternPosition, "not implemented: dynamic patter would be very slow to execute");
    }

    @NotNull
    private static Matcher createMatcher(Function pattern, int position) throws SqlException {
        final CharSequence regex = pattern.getStr(null);
        if (regex == null) {
            throw SqlException.$(position, "NULL regex");
        }
        try {
            return Pattern.compile(Chars.toString(regex)).matcher("");
        } catch (PatternSyntaxException e) {
            throw SqlException.$(position + e.getIndex() + 1, e.getMessage());
        }
    }

    private static class MatchConstPatternFunction extends BooleanFunction implements UnaryFunction {
        private final Function value;
        private final Matcher matcher;

        public MatchConstPatternFunction(Function value, Matcher matcher) {
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
            return cs != null && matcher.reset(cs).find();
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }
    }

    private static class MatchRuntimeConstPatternFunction extends BooleanFunction implements UnaryFunction {
        private final Function value;
        private final Function pattern;
        private final int patternPosition;
        private Matcher matcher;

        public MatchRuntimeConstPatternFunction(Function value, Function pattern, int patternPosition) {
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
            CharSequence cs = getArg().getStr(rec);
            return cs != null && matcher.reset(cs).find();
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
        public boolean isReadThreadSafe() {
            return false;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            pattern.init(symbolTableSource, executionContext);
            this.matcher = createMatcher(pattern, patternPosition);
        }
    }
}
