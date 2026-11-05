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


package io.questdb.griffin.engine.functions.bool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.FunctionExtension;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class AllNotEqSymbolFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "<>all(Kw)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final FunctionExtension arrayFunction = args.getQuick(1).extendedOps();
        final int arraySize = arrayFunction.getArrayLength();
        if (arraySize == 0) {
            return BooleanConstant.TRUE;
        }

        final CharSequenceHashSet set = new CharSequenceHashSet();
        for (int i = 0; i < arraySize; i++) {
            set.add(arrayFunction.getStrA(null, i));
        }

        final SymbolFunction var = (SymbolFunction) args.getQuick(0);
        if (var.isConstant()) {
            final CharSequence symbol = var.getSymbol(null);
            return BooleanConstant.of(symbol != null && set.excludes(symbol));
        }
        return new Func(var, set);
    }

    private static class Func extends BooleanFunction implements UnaryFunction {
        private final SymbolFunction arg;
        private final IntHashSet keys = new IntHashSet();
        private final CharSequenceHashSet set;
        private boolean isKeyed;

        private Func(SymbolFunction arg, CharSequenceHashSet set) {
            this.arg = arg;
            this.set = set;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            if (isKeyed) {
                final int key = arg.getInt(rec);
                return key != SymbolTable.VALUE_IS_NULL && keys.excludes(key);
            }
            final CharSequence symbol = arg.getSymbol(rec);
            return symbol != null && set.excludes(symbol);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            final StaticSymbolTable symbolTable = arg.getStaticSymbolTable();
            isKeyed = symbolTable != null;
            if (isKeyed) {
                keys.clear();
                for (int i = 0, n = set.size(); i < n; i++) {
                    keys.add(symbolTable.keyOf(set.get(i)));
                }
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val(" <> all ").val(set);
        }
    }
}
