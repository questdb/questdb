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

package io.questdb.griffin.engine.functions.bool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class InStrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "in(Sv)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {

        CharSequenceHashSet set = new CharSequenceHashSet();
        int n = args.size();

        if (n == 1) {
            return BooleanConstant.FALSE;
        }

        for (int i = 1; i < n; i++) {
            Function func = args.getQuick(i);
            switch (func.getType()) {
                case ColumnType.STRING:
                case ColumnType.SYMBOL:
                    CharSequence value = func.getStr(null);
                    if (value == null) {
                        throw SqlException.$(argPositions.getQuick(i), "NULL is not allowed");
                    }
                    set.add(Chars.toString(value));
                    break;
                case ColumnType.CHAR:
                    set.add(new String(new char[]{func.getChar(null)}));
                    break;
                default:
                    throw SqlException.$(argPositions.getQuick(i), "STRING constant expected");
            }
        }
        Function var = args.getQuick(0);
        if (var.isConstant()) {
            return BooleanConstant.of(set.contains(var.getStr(null)));
        }
        return new Func(var, set);
    }

    private static class Func extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private final CharSequenceHashSet set;

        public Func(Function arg, CharSequenceHashSet set) {
            this.arg = arg;
            this.set = set;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return set.contains(arg.getStr(rec));
        }
    }
}
