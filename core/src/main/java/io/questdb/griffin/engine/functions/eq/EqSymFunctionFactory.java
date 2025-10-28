/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.Chars;
import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class EqSymFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "=(KK)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function leftFunc = args.getQuick(0);
        final Function rightFunc = args.getQuick(1);
        if (isSymbolTableStatic(leftFunc) && isSymbolTableStatic(rightFunc)) {
            return new Func(configuration, leftFunc, rightFunc);
        }
        // SYMBOL cannot be constant, so we can't use any of the half-constant functions
        return new EqSymStrFunctionFactory.Func(leftFunc, rightFunc);
    }

    private static boolean isSymbolTableStatic(Function symFunc) {
        return ((SymbolFunction) symFunc).isSymbolTableStatic();
    }

    private static class Func extends AbstractEqBinaryFunction {
        private final DirectIntIntHashMap lookupCache;
        private StaticSymbolTable leftTable;
        private StaticSymbolTable rightTable;

        public Func(CairoConfiguration configuration, Function left, Function right) {
            super(left, right);
            this.lookupCache = new DirectIntIntHashMap(
                    configuration.getSqlSmallMapKeyCapacity(),
                    configuration.getSqlFastMapLoadFactor(),
                    0,
                    MemoryTag.NATIVE_UNORDERED_MAP
            );
        }

        @Override
        public void close() {
            super.close();
            Misc.free(lookupCache);
        }

        @Override
        public void cursorClosed() {
            super.cursorClosed();
            lookupCache.restoreInitialCapacity();
        }

        @Override
        public boolean getBool(Record rec) {
            // important to compare A and B strings in case
            // these are columns of the same record
            // records have re-usable character sequences
            final CharSequence a = left.getSymbol(rec);
            final CharSequence b = right.getStrA(rec);

            if (a == null) {
                return negated != (b == null);
            }
            return negated != Chars.equalsNc(a, b);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            this.leftTable = ((SymbolFunction) left).getStaticSymbolTable();
            this.rightTable = ((SymbolFunction) right).getStaticSymbolTable();
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }
}
