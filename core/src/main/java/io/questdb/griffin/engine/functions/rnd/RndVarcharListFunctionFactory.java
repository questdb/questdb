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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Transient;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;

public class RndVarcharListFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rnd_varchar(V)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (args == null) {
            return new RndVarcharFunction(3, 10, 1);
        }

        final ObjList<Utf8String> symbols = new ObjList<>(args.size());
        copyConstants(args, argPositions, symbols);
        return new RndVarcharListFunction(symbols);
    }

    static void copyConstants(
            ObjList<Function> args,
            IntList argPositions,
            ObjList<Utf8String> symbols
    ) throws SqlException {
        for (int i = 0, n = args.size(); i < n; i++) {
            final Function f = args.getQuick(i);
            if (f.isConstant()) {
                final int typeTag = ColumnType.tagOf(f.getType());
                if (typeTag == ColumnType.STRING || typeTag == ColumnType.NULL) {
                    final CharSequence value = f.getStrA(null);
                    symbols.add(value != null ? new Utf8String(value) : null);
                    continue;
                }
                if (typeTag == ColumnType.CHAR) {
                    final char value = f.getChar(null);
                    symbols.add(value != 0 ? new Utf8String(value) : null);
                    continue;
                }
            }
            throw SqlException.$(argPositions.getQuick(i), "STRING constant expected");
        }
    }

    private static final class RndVarcharListFunction extends VarcharFunction implements Function {
        private final int count;
        private final ObjList<Utf8String> symbols;
        private Rnd rnd;

        public RndVarcharListFunction(ObjList<Utf8String> symbols) {
            this.symbols = symbols;
            this.count = symbols.size();
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            return rndSymbol();
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            return rndSymbol();
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
        public void toPlan(PlanSink sink) {
            sink.val("rnd_varchar(").val((Sinkable) symbols).val(')');
        }

        private Utf8String rndSymbol() {
            return symbols.getQuick(rnd.nextPositiveInt() % count);
        }
    }
}
