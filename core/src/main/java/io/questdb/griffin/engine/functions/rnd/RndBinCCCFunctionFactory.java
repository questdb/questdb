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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.BinFunction;
import io.questdb.griffin.engine.functions.StatelessFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndBinCCCFunctionFactory implements FunctionFactory {
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
