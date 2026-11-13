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
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

public class CastBooleanToSymbolFunctionFactory implements FunctionFactory {
    // A BOOLEAN has no null, so getInt() answers 0 for false or 1 for true and nothing else. Every
    // other key still has to resolve to null: a consumer that borrows this table mints VALUE_IS_NULL
    // on its own - lag()/lead() do it for a missing neighbor - and resolves that key here. Func
    // resolves its keys through this table too, so the function and the table it hands out cannot
    // disagree; a caller resolving keys off the table compares them against what the function itself
    // returns. The table holds no state, so every function shares it and any thread may read it.
    private static final SymbolTable SYMBOL_TABLE = new SymbolTable() {
        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return switch (key) {
                case 0 -> "false";
                case 1 -> "true";
                default -> null;
            };
        }
    };

    @Override
    public String getSignature() {
        return "cast(Tk)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function arg = args.getQuick(0);
        if (arg.isConstant()) {
            return SymbolConstant.newInstance(arg.getSymbol(null));
        }
        return new Func(arg);
    }

    private static class Func extends SymbolFunction implements UnaryFunction {
        private final Function arg;

        public Func(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public int getInt(Record rec) {
            return arg.getInt(rec);
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            return arg.getSymbol(rec);
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return arg.getSymbolB(rec);
        }

        @Override
        public boolean isSymbolTableStatic() {
            return false;
        }

        @Override
        public @Nullable SymbolTable newSymbolTable() {
            // Not "this": whatever newSymbolTable() hands out may be freed by its caller, and
            // closing this function would close the argument the live projection still reads.
            return SYMBOL_TABLE;
        }

        @Override
        public boolean supportsKeyValueAccess() {
            // getInt() mints a key with one probe on the decoded scalar, never by hashing the row's
            // text, and valueOf() resolves it with a switch on the key. A key consumer such as QWP
            // egress should therefore encode each distinct value once instead of re-encoding it per
            // row.
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val("::symbol");
        }

        @Override
        public CharSequence valueBOf(int key) {
            return SYMBOL_TABLE.valueBOf(key);
        }

        @Override
        public CharSequence valueOf(int symbolKey) {
            return SYMBOL_TABLE.valueOf(symbolKey);
        }
    }
}
