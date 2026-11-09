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
import io.questdb.cairo.TableUtils;
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
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

public class CastDoubleToSymbolFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Dk)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function arg = args.getQuick(0);
        if (arg.isConstant()) {
            final StringSink sink = Misc.getThreadLocalSink();
            sink.put(arg.getDouble(null));
            return SymbolConstant.newInstance(sink);
        }
        return new Func(arg);
    }

    private static class Func extends SymbolFunction implements UnaryFunction {
        private final Function arg;
        private final StringSink sink = new StringSink();
        private int next = 1;
        private LongIntHashMap symbolTableShortcut = new LongIntHashMap();
        private ObjList<String> symbols = new ObjList<>();

        public Func(Function arg) {
            this.arg = arg;
            symbols.add(null);
        }

        @Override
        public void cursorClosed() {
            try {
                arg.cursorClosed();
            } finally {
                releaseDictionary();
            }
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public int getInt(Record rec) {
            final double value = arg.getDouble(rec);
            if (Numbers.isNull(value)) {
                return SymbolTable.VALUE_IS_NULL;
            }

            final long key = Double.doubleToLongBits(value);
            final int keyIndex = symbolTableShortcut.keyIndex(key);
            if (keyIndex < 0) {
                return symbolTableShortcut.valueAt(keyIndex) - 1;
            }

            symbolTableShortcut.putAt(keyIndex, key, next);
            sink.clear();
            sink.put(value);
            symbols.add(Chars.toString(sink));
            return next++ - 1;
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            final double value = arg.getDouble(rec);
            if (Numbers.isNull(value)) {
                return null;
            }

            final long key = Double.doubleToLongBits(value);
            final int keyIndex = symbolTableShortcut.keyIndex(key);
            if (keyIndex < 0) {
                return symbols.getQuick(symbolTableShortcut.valueAt(keyIndex));
            }

            symbolTableShortcut.putAt(keyIndex, key, next++);
            sink.clear();
            sink.put(value);
            final String str = Chars.toString(sink);
            symbols.add(str);
            return str;
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return getSymbol(rec);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            releaseDictionary();
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
            return new CastToSymbolTable(symbols);
        }

        @Override
        public boolean supportsKeyValueAccess() {
            // getInt() mints a key with one probe on the decoded scalar, never by hashing the row's
            // text, and valueOf() resolves it by indexing symbols. A key consumer such as QWP egress
            // should therefore encode each distinct value once instead of re-encoding it per row.
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val("::symbol");
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int symbolKey) {
            return symbols.getQuick(TableUtils.toIndexKey(symbolKey));
        }

        // A cached factory outlives its cursor. Replacing the collections, rather than clearing
        // them, also gives back the capacity they grew to. newSymbolTable() hands out a copy, so
        // nothing else holds on to them.
        private void releaseDictionary() {
            if (next > 1) {
                symbolTableShortcut = new LongIntHashMap();
                symbols = new ObjList<>();
                symbols.add(null);
                next = 1;
            }
        }
    }
}
