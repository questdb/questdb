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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndStringListFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rnd_str(V)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (args == null) {
            return new RndStrFunction(3, 10, 1);
        }

        final ObjList<String> symbols = new ObjList<>(args.size());
        copyConstants(args, argPositions, symbols);
        return new Func(symbols);
    }

    static void copyConstants(
            ObjList<Function> args,
            IntList argPositions,
            ObjList<String> symbols
    ) throws SqlException {
        for (int i = 0, n = args.size(); i < n; i++) {
            final Function f = args.getQuick(i);
            if (f.isConstant()) {
                if (f.getType() == ColumnType.STRING || f.getType() == ColumnType.NULL) {
                    symbols.add(Chars.toString(f.getStr(null)));
                    continue;
                }
                if (f.getType() == ColumnType.CHAR) {
                    symbols.add(new java.lang.String(new char[]{f.getChar(null)}));
                    continue;
                }
            }
            throw SqlException.$(argPositions.getQuick(i), "STRING constant expected");
        }
    }

    private static final class Func extends StrFunction implements Function {
        private final ObjList<String> symbols;
        private final int count;
        private Rnd rnd;

        public Func(ObjList<String> symbols) {
            this.symbols = symbols;
            this.count = symbols.size();
        }

        @Override
        public CharSequence getStr(Record rec) {
            return symbols.getQuick(rnd.nextPositiveInt() % count);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStr(rec);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
        }
    }
}
