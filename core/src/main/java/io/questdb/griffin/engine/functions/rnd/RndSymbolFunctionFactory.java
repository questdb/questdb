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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndSymbolFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rnd_symbol(iiii)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final int count = args.getQuick(0).getInt(null);
        final int lo = args.getQuick(1).getInt(null);
        final int hi = args.getQuick(2).getInt(null);
        final int nullRate = args.getQuick(3).getInt(null);

        if (count < 1) {
            throw SqlException.$(args.getQuick(0).getPosition(), "invalid symbol count");
        }

        if (lo > hi || lo < 1) {
            throw SqlException.$(position, "invalid range");
        }

        if (nullRate < 0) {
            throw SqlException.position(args.getQuick(3).getPosition()).put("rate must be positive");
        }

        return new Func(position, count, lo, hi, nullRate);
    }

    private static final class Func extends SymbolFunction implements Function {
        private final int count;
        private final int lo;
        private final int hi;
        private final int nullRate;
        private final ObjList<String> symbols;
        private Rnd rnd;

        public Func(int position, int count, int lo, int hi, int nullRate) {
            super(position);
            this.count = count;
            this.lo = lo;
            this.hi = hi;
            this.nullRate = nullRate + 1;
            this.symbols = new ObjList<>(count);
        }

        @Override
        public int getInt(Record rec) {
            return next();
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            return symbols.getQuick(TableUtils.toIndexKey(next()));
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return getSymbol(rec);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
            seedSymbols();
        }

        @Override
        public boolean isSymbolTableStatic() {
            return false;
        }

        @Override
        public CharSequence valueOf(int symbolKey) {
            return symbols.getQuick(TableUtils.toIndexKey(symbolKey));
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        private int next() {
            if (rnd.nextPositiveInt() % nullRate == 1) {
                return SymbolTable.VALUE_IS_NULL;
            }
            return rnd.nextPositiveInt() % count;
        }

        private void seedFixed() {
            for (int i = 0; i < count; i++) {
                symbols.add(Chars.toString(rnd.nextChars(lo)));
            }
        }

        private void seedSymbols() {
            symbols.clear();
            symbols.add(null);
            if (lo == hi) {
                seedFixed();
            } else {
                seedVariable();
            }
        }

        private void seedVariable() {
            int range = hi - lo + 1;
            for (int i = 0; i < count; i++) {
                symbols.add(rnd.nextChars(lo + rnd.nextPositiveInt() % range).toString());
            }
        }
    }
}
