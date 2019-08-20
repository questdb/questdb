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
import com.questdb.griffin.engine.functions.StrFunction;
import com.questdb.std.ObjList;
import com.questdb.std.Rnd;

public class RndStrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "rnd_str(iii)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {

        int lo = args.getQuick(0).getInt(null);
        int hi = args.getQuick(1).getInt(null);
        int nullRate = args.getQuick(2).getInt(null);

        if (nullRate < 0) {
            throw SqlException.position(args.getQuick(2).getPosition()).put("rate must be positive");
        }

        if (lo < hi && lo > 0) {
            return new RndStrFunction(position, lo, hi, nullRate + 1, configuration);
        } else if (lo == hi) {
            return new FixedFunction(position, lo, nullRate + 1, configuration);
        }

        throw SqlException.position(position).put("invalid range");
    }

    private static class FixedFunction extends StrFunction implements StatelessFunction {
        private final int len;
        private final int nullRate;
        private final Rnd rnd;

        public FixedFunction(int position, int len, int nullRate, CairoConfiguration configuration) {
            super(position);
            this.len = len;
            this.rnd = SharedRandom.getRandom(configuration);
            this.nullRate = nullRate;
        }

        @Override
        public CharSequence getStr(Record rec) {
            if ((rnd.nextInt() % nullRate) == 1) {
                return null;
            }
            return rnd.nextChars(len);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStr(rec);
        }
    }
}
