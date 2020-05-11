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
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class MatchStrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "~=(Ss)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        Function value = args.getQuick(0);
        CharSequence regex = args.getQuick(1).getStr(null);

        if (regex == null) {
            throw SqlException.$(args.getQuick(1).getPosition(), "NULL regex");
        }

        try {
            Matcher matcher = Pattern.compile(Chars.toString(regex)).matcher("");
            return new MatchFunction(position, value, matcher);
        } catch (PatternSyntaxException e) {
            throw SqlException.$(args.getQuick(1).getPosition() + e.getIndex() + 1, e.getMessage());
        }
    }

    private static class MatchFunction extends BooleanFunction implements UnaryFunction {
        private final Function value;
        private final Matcher matcher;

        public MatchFunction(int position, Function value, Matcher matcher) {
            super(position);
            this.value = value;
            this.matcher = matcher;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence cs = getArg().getStr(rec);
            return cs != null && matcher.reset(cs).find();
        }

        @Override
        public Function getArg() {
            return value;
        }
    }
}
