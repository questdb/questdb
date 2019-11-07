/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
import io.questdb.griffin.engine.functions.regex.impl.Matcher;
import io.questdb.griffin.engine.functions.regex.impl.Pattern;
import io.questdb.griffin.engine.functions.regex.impl.PatternSyntaxException;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;

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
