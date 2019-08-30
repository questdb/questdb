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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.StatelessFunction;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndIntCCFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "rnd_int(iii)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {

        int lo = args.getQuick(0).getInt(null);
        int hi = args.getQuick(1).getInt(null);
        int nanRate = args.getQuick(2).getInt(null);

        if (nanRate < 0) {
            throw SqlException.$(args.getQuick(2).getPosition(), "invalid NaN rate");
        }

        if (lo < hi) {
            return new RndFunction(position, lo, hi, nanRate, configuration);
        }

        throw SqlException.position(position).put("invalid range");
    }

    private static class RndFunction extends IntFunction implements StatelessFunction {
        private final int lo;
        private final int range;
        private final int nanRate;
        private final Rnd rnd;

        public RndFunction(int position, int lo, int hi, int nanRate, CairoConfiguration configuration) {
            super(position);
            this.lo = lo;
            this.range = hi - lo + 1;
            this.nanRate = nanRate + 1;
            this.rnd = SharedRandom.getRandom(configuration);
        }

        @Override
        public int getInt(Record rec) {
            if ((rnd.nextInt() % nanRate) == 1) {
                return Numbers.INT_NaN;
            }
            return lo + rnd.nextPositiveInt() % range;
        }
    }
}
