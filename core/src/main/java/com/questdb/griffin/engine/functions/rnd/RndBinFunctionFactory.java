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
import com.questdb.griffin.engine.functions.BinFunction;
import com.questdb.griffin.engine.functions.StatelessFunction;
import com.questdb.std.BinarySequence;
import com.questdb.std.ObjList;
import com.questdb.std.Rnd;

public class RndBinFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rnd_bin(lli)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        final long lo = args.getQuick(0).getLong(null);
        final long hi = args.getQuick(1).getLong(null);
        final int nullRate = args.getQuick(2).getInt(null);

        if (nullRate < 0) {
            throw SqlException.$(args.getQuick(2).getPosition(), "invalid null rate");
        }

        if (lo > hi) {
            throw SqlException.$(position, "invalid range");
        }

        if (lo < 1) {
            throw SqlException.$(args.getQuick(0).getPosition(), "minimum has to be grater than 0");
        }

        if (lo < hi) {
            return new VarLenFunction(position, lo, hi, nullRate, configuration);
        }

        // lo == hi
        return new FixLenFunction(position, lo, nullRate, configuration);
    }

    private static final class VarLenFunction extends BinFunction implements StatelessFunction {
        private final Sequence sequence = new Sequence();
        private final Rnd rnd;
        private final long lo;
        private final long range;
        private final int nullRate;

        public VarLenFunction(int position, long lo, long hi, int nullRate, CairoConfiguration configuration) {
            super(position);
            this.lo = lo;
            this.range = hi - lo + 1;
            this.nullRate = nullRate + 1;
            this.sequence.rnd = rnd = SharedRandom.getRandom(configuration);
        }

        @Override
        public BinarySequence getBin(Record rec) {
            if ((rnd.nextPositiveInt() % nullRate) == 1) {
                return null;
            }
            sequence.len = lo + sequence.rnd.nextPositiveLong() % range;
            return sequence;
        }

        @Override
        public long getBinLen(Record rec) {
            return sequence.len;
        }
    }

    private static final class FixLenFunction extends BinFunction implements StatelessFunction {
        private final Sequence sequence = new Sequence();
        private final Rnd rnd;
        private final int nullRate;

        public FixLenFunction(int position, long len, int nullRate, CairoConfiguration configuration) {
            super(position);
            this.nullRate = nullRate + 1;
            this.sequence.rnd = rnd = SharedRandom.getRandom(configuration);
            this.sequence.len = len;
        }

        @Override
        public BinarySequence getBin(Record rec) {
            if ((rnd.nextPositiveInt() % nullRate) == 1) {
                return null;
            }
            return sequence;
        }

        @Override
        public long getBinLen(Record rec) {
            return sequence.len;
        }
    }

    private static class Sequence implements BinarySequence {
        private Rnd rnd;
        private long len;

        @Override
        public byte byteAt(long index) {
            return rnd.nextByte();
        }

        @Override
        public long length() {
            return len;
        }
    }
}
