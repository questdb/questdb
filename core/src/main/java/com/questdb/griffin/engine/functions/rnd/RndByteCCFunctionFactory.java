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
import com.questdb.griffin.engine.functions.ByteFunction;
import com.questdb.griffin.engine.functions.StatelessFunction;
import com.questdb.std.ObjList;
import com.questdb.std.Rnd;

public class RndByteCCFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "rnd_byte(ii)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {

        byte lo = (byte) args.getQuick(0).getInt(null);
        byte hi = (byte) args.getQuick(1).getInt(null);

        if (lo < hi) {
            return new RndFunction(position, lo, hi, configuration);
        }

        throw SqlException.position(position).put("invalid range");
    }

    private static class RndFunction extends ByteFunction implements StatelessFunction {
        private final byte lo;
        private final byte range;
        private final Rnd rnd;

        public RndFunction(int position, byte lo, byte hi, CairoConfiguration configuration) {
            super(position);
            this.lo = lo;
            this.range = (byte) (hi - lo + 1);
            this.rnd = SharedRandom.getRandom(configuration);
        }

        @Override
        public byte getByte(Record rec) {
            short s = rnd.nextShort();
            if (s < 0) {
                return (byte) (lo - s % range);
            }
            return (byte) (lo + s % range);
        }
    }
}
