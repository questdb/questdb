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
import io.questdb.std.*;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

public class CastTimestampToSymbolFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Nk)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function arg = args.getQuick(0);
        if (arg.isConstant()) {
            final StringSink sink = Misc.getThreadLocalSink();
            sink.put(arg.getTimestamp(null));
            return SymbolConstant.newInstance(sink);
        }
        return new Func(arg);
    }

    private static class Func extends SymbolFunction implements UnaryFunction {
        private final Function arg;
        private final CastToSymbolDictionary dictionary = new CastToSymbolDictionary(true);
        private final StringSink sink = new StringSink();
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
        public void cursorClosed() {
            try {
                arg.cursorClosed();
            } finally {
                // A cached factory can outlive its cursor. Free the query-accounted
                // dictionary before the query's MemoryTracker is returned to its pool.
                releaseDictionary();
            }
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public int getInt(Record rec) {
            final long value = arg.getTimestamp(rec);
            if (value == Numbers.LONG_NULL) {
                return SymbolTable.VALUE_IS_NULL;
            }
            final int key = dictionary.keyOf(value);
            if (key > -1) {
                return key;
            }
            sink.clear();
            sink.put(value);
            return dictionary.put(value, sink);
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            return valueOf(getInt(rec));
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return valueBOf(getInt(rec));
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            releaseDictionary();
            arg.init(symbolTableSource, executionContext);
            dictionary.setMemoryTracker(executionContext.getMemoryTracker());
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
        public boolean supportsKeyValueAccess() {
            // getInt() mints a key with one probe on the decoded scalar, never by hashing the row's
            // text, and valueOf() resolves it with one descriptor read. A key consumer such as QWP
            // egress should therefore encode each distinct value once instead of re-encoding it per row.
            return true;
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
