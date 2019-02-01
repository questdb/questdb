/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.functions.rnd;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.functions.StatelessFunction;
import com.questdb.griffin.engine.functions.SymbolFunction;
import com.questdb.std.ObjList;
import com.questdb.std.Rnd;

public class RndSymbolFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rnd_symbol(iiii)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
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

        return new Func(position, count, lo, hi, nullRate, configuration);
    }

    private static final class Func extends SymbolFunction implements StatelessFunction {
        private final int count;
        private final int lo;
        private final int hi;
        private final int nullRate;
        private final ObjList<String> symbols;
        private final Rnd rnd;

        public Func(int position, int count, int lo, int hi, int nullRate, CairoConfiguration configuration) {
            super(position);
            this.count = count;
            this.lo = lo;
            this.hi = hi;
            this.nullRate = nullRate + 1;
            this.rnd = SharedRandom.getRandom(configuration);
            this.symbols = new ObjList<>(count);
            seedSymbols();
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            if (rnd.nextPositiveInt() % nullRate == 1) {
                return null;
            }
            return symbols.getQuick(rnd.nextPositiveInt() % count);
        }

        private void seedFixed() {
            for (int i = 0; i < count; i++) {
                symbols.add(rnd.nextChars(lo).toString());
            }
        }

        private void seedSymbols() {
            symbols.clear();
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
