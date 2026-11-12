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
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectString;
import org.jetbrains.annotations.Nullable;

public class CastStrToSymbolFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Sk)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function arg = args.getQuick(0);
        if (arg.isConstant()) {
            return SymbolConstant.newInstance(arg.getStrA(null));
        }
        return new Func(arg);
    }

    public static class Func extends SymbolFunction implements UnaryFunction {
        private final Function arg;
        private final CastToSymbolDictionary dictionary = new CastToSymbolDictionary(false);
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
            return intern(arg.getStrA(rec));
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            // Pass-through: getSymbol never needs the dictionary (it returns the string
            // directly). Only getInt/valueOf build one. Interning here would grow an
            // unnecessary cardinality-dependent dictionary for values no key consumer uses.
            return arg.getStrA(rec);
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return arg.getStrB(rec);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            // A parent can initialize this function again within the same execution.
            releaseDictionary();
            arg.init(symbolTableSource, executionContext);
            dictionary.init(executionContext);
        }

        public int intern(@Nullable CharSequence value) {
            if (value == null) {
                return SymbolTable.VALUE_IS_NULL;
            }
            return dictionary.intern(value);
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
            // A view owns its A/B flyweights but reads the live dictionary. Unlike copying
            // the dictionary, it needs no native allocation with an otherwise unclear owner.
            // The values it yields point into append-only text chunks, so they stay valid across
            // further intern()/getInt() calls on the source function (see
            // CastToSymbolDictionary.valueOf) and remain readable until the dictionary is
            // released. The view only ever grows: a key minted after the view was taken resolves
            // through it just as well.
            return new SymbolTable() {
                private final DirectString viewA = new DirectString();
                private final DirectString viewB = new DirectString();

                @Override
                public CharSequence valueBOf(int key) {
                    return dictionary.valueOf(key, viewB);
                }

                @Override
                public CharSequence valueOf(int key) {
                    return dictionary.valueOf(key, viewA);
                }
            };
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

        private void releaseDictionary() {
            dictionary.clear();
            symbolA.clear();
            symbolB.clear();
        }
    }
}
