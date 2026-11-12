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

package io.questdb.griffin.engine.functions.cast;

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
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;

public class CastVarcharToSymbolFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(Øk)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function arg = args.getQuick(0);
        if (arg.isConstant()) {
            final Utf8Sequence value = arg.getVarcharA(null);
            if (value == null) {
                return SymbolConstant.NULL;
            }
            final String symbol = utf8ToString(value);
            return symbol != null ? SymbolConstant.newInstance(symbol) : SymbolConstant.NULL;
        }
        return new Func(arg);
    }

    private static @Nullable String utf8ToString(Utf8Sequence value) {
        return Chars.toString(Utf8s.utf8ToUtf16OrView(value, Misc.getThreadLocalSink()));
    }

    private static class Func extends SymbolFunction implements UnaryFunction {
        private final Function arg;
        private final StringSink decodeSink = new StringSink();
        private final CastToSymbolDictionary dictionary = new CastToSymbolDictionary(true);
        private final DirectString symbolA = new DirectString();
        private final DirectString symbolB = new DirectString();

        public Func(Function arg) {
            this.arg = arg;
        }

        @Override
        public void close() {
            try {
                releaseDictionary();
            } finally {
                arg.close();
            }
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public int getInt(Record rec) {
            return intern(arg.getVarcharA(rec));
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            // Interning finds a repeated value by its UTF-8 bytes and decodes only a new one, so
            // returning the dictionary's text costs no per-row UTF-8 to UTF-16 conversion.
            return dictionary.valueOf(intern(arg.getVarcharA(rec)), symbolA);
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return dictionary.valueOf(intern(arg.getVarcharB(rec)), symbolB);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            releaseDictionary();
            arg.init(symbolTableSource, executionContext);
            dictionary.init(executionContext);
        }

        @Override
        public boolean isSymbolTableStatic() {
            return false;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public @Nullable SymbolTable newSymbolTable() {
            return new CastToSymbolTable(dictionary);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val("::symbol");
        }

        @Override
        public CharSequence valueBOf(int key) {
            return dictionary.valueOf(key, symbolB);
        }

        @Override
        public CharSequence valueOf(int symbolKey) {
            return dictionary.valueOf(symbolKey, symbolA);
        }

        private int intern(@Nullable Utf8Sequence value) {
            if (value == null) {
                return SymbolTable.VALUE_IS_NULL;
            }
            return dictionary.intern(value, decodeSink);
        }

        private void releaseDictionary() {
            dictionary.clear();
            symbolA.clear();
            symbolB.clear();
        }
    }
}
