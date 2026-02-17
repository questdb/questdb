/*******************************************************************************
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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndBinCCCFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rnd_bin(lli)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final long lo = args.getQuick(0).getLong(null);
        final long hi = args.getQuick(1).getLong(null);
        final int nullRate = args.getQuick(2).getInt(null);

        if (nullRate < 0) {
            throw SqlException.$(argPositions.getQuick(2), "invalid null rate");
        }

        if (lo > hi) {
            throw SqlException.$(position, "invalid range");
        }

        if (lo < 1) {
            throw SqlException.$(argPositions.getQuick(0), "minimum has to be grater than 0");
        }

        if (lo < hi) {
            return new VarLenFunction(lo, hi, nullRate);
        }

        // lo == hi
        return new FixLenFunction(lo, nullRate);
    }

    private static final class FixLenFunction extends BinFunction implements Function {
        private final int nullRate;
        private final Sequence sequence = new Sequence();

        public FixLenFunction(long len, int nullRate) {
            this.nullRate = nullRate + 1;
            this.sequence.len = len;
        }

        @Override
        public BinarySequence getBin(Record rec) {
            if ((sequence.rnd.nextPositiveInt() % nullRate) == 1) {
                return null;
            }
            return sequence;
        }

        @Override
        public long getBinLen(Record rec) {
            return sequence.len;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.sequence.rnd = executionContext.getRandom();
        }

        @Override
        public boolean isNonDeterministic() {
            return true;
        }

        @Override
        public boolean isRandom() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("rnd_bin(").val(this.sequence.len).val(',').val(this.sequence.len).val(',').val(nullRate - 1).val(')');
        }
    }

    private static class Sequence implements BinarySequence {
        private long len;
        private Rnd rnd;

        @Override
        public byte byteAt(long index) {
            return rnd.nextByte();
        }

        @Override
        public long length() {
            return len;
        }
    }

    private static final class VarLenFunction extends BinFunction implements Function {
        private final long lo;
        private final int nullRate;
        private final long range;
        private final Sequence sequence = new Sequence();

        public VarLenFunction(long lo, long hi, int nullRate) {
            this.lo = lo;
            this.range = hi - lo + 1;
            this.nullRate = nullRate + 1;
        }

        @Override
        public BinarySequence getBin(Record rec) {
            if ((this.sequence.rnd.nextPositiveInt() % nullRate) == 1) {
                return null;
            }
            sequence.len = lo + sequence.rnd.nextPositiveLong() % range;
            return sequence;
        }

        @Override
        public long getBinLen(Record rec) {
            return sequence.len;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.sequence.rnd = executionContext.getRandom();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("rnd_bin(").val(lo).val(',').val(range + lo - 1).val(',').val(nullRate - 1).val(')');
        }
    }
}
