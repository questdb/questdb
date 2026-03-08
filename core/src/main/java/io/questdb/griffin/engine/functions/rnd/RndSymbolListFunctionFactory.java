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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Transient;
import io.questdb.std.str.Sinkable;

public class RndSymbolListFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rnd_symbol(V)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (args == null || args.size() == 0) {
            throw SqlException.$(position, "no arguments provided");
        }
        final ObjList<String> symbols = new ObjList<>(args.size());
        RndStringListFunctionFactory.copyConstants(args, argPositions, symbols);
        return new Func(symbols);
    }

    private static final class Func extends SymbolFunction implements Function {
        private final int count;
        private final ObjList<String> symbols;
        private Rnd rnd;

        public Func(ObjList<String> symbols) {
            this.symbols = symbols;
            this.count = symbols.size();
        }

        @Override
        public int getInt(Record rec) {
            return next();
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            return symbols.getQuick(next());
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return getSymbol(rec);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
        }

        @Override
        public boolean isNonDeterministic() {
            return true;
        }

        @Override
        public boolean isRandom() {
            return true;
        }

        @Override
        public boolean isSymbolTableStatic() {
            return false;
        }

        @Override
        public SymbolTable newSymbolTable() {
            Func func = new Func(symbols);
            func.rnd = new Rnd(this.rnd.getSeed0(), this.rnd.getSeed1());
            return func;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("rnd_symbol(").val((Sinkable) symbols).val(')');
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int symbolKey) {
            return symbolKey != -1 ? symbols.getQuick(symbolKey) : null;
        }

        private int next() {
            return Math.abs(rnd.nextPositiveInt() % count);
        }
    }
}
