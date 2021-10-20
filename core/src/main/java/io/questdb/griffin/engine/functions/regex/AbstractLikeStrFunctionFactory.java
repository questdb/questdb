/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.griffin.engine.functions.bind.IndexedParameterLinkFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractLikeStrFunctionFactory implements FunctionFactory {
    public static String escapeSpecialChars(CharSequence pattern, CharSequence prev) {
        int len = pattern.length();

        StringSink sink = Misc.getThreadLocalBuilder();
        for (int i = 0; i < len; i++) {
            char c = pattern.charAt(i);
            if (c == '_')
                sink.put(".");
            else if (c == '%')
                sink.put(".*?");
            else if ("[](){}.*+?$^|#\\".indexOf(c) != -1) {
                sink.put("\\");
                sink.put(c);
            } else
                sink.put(c);

        }

        if (Chars.equalsNc(sink, prev)) {
            return null;
        }
        return Chars.toString(sink);
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final Function value = args.getQuick(0);
        final Function pattern = args.getQuick(1);

        if (pattern.isConstant()) {
            final CharSequence likeString = pattern.getStr(null);
            if (likeString != null && likeString.length() > 0) {
                String p = escapeSpecialChars(likeString, null);
                assert p != null;
                return new ConstLikeStrFunction(
                        value,
                        Pattern.compile(p, Pattern.DOTALL).matcher("")
                );
            }
            return BooleanConstant.FALSE;
        }

        if (pattern instanceof IndexedParameterLinkFunction) {
            // bind variable
            return new BindLikeStrFunction(value, pattern);
        }

        throw SqlException.$(argPositions.getQuick(1), "use constant or bind variable");
    }

    private static class ConstLikeStrFunction extends BooleanFunction implements UnaryFunction {
        private final Function value;
        private final Matcher matcher;

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
    }

    private static class BindLikeStrFunction extends BooleanFunction implements UnaryFunction {
        private final Function value;
        private final Function pattern;
        private Matcher matcher;
        private String lastPattern = null;

        public BindLikeStrFunction(Function value, Function pattern) {
            this.value = value;
            this.pattern = pattern;
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
                    this.matcher = Pattern.compile(p, Pattern.DOTALL).matcher("");
                    this.lastPattern = p;
                }
            } else {
                lastPattern = null;
                matcher = null;
            }
        }
    }
}




