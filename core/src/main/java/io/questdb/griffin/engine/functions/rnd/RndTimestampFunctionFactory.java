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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndTimestampFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rnd_timestamp(nni)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final long lo = args.getQuick(0).getTimestamp(null);
        final long hi = args.getQuick(1).getTimestamp(null);
        final int nanRate = args.getQuick(2).getInt(null);

        if (nanRate < 0) {
            throw SqlException.$(argPositions.getQuick(2), "invalid NaN rate");
        }

        if (lo < hi) {
            return new Func(lo, hi, nanRate);
        }

        throw SqlException.$(position, "invalid range");
    }

    private static class Func extends TimestampFunction implements Function {
        private final long lo;
        private final long range;
        private final int nanRate;
        private Rnd rnd;

        public Func(long lo, long hi, int nanRate) {
            this.lo = lo;
            this.range = hi - lo + 1;
            this.nanRate = nanRate + 1;
        }

        @Override
        public long getTimestamp(Record rec) {
            if ((rnd.nextInt() % nanRate) == 1) {
                return Numbers.LONG_NaN;
            }
            return lo + rnd.nextPositiveLong() % range;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
        }
    }
}
