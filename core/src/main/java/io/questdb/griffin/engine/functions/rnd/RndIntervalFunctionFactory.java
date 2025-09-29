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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.FunctionExtension;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.IntervalFunction;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import org.jetbrains.annotations.NotNull;

public class RndIntervalFunctionFactory implements FunctionFactory {
    private static final long LO_BOUNDARY = 1999999000000000L; // Wednesday, May 18, 2033 3:16:40 AM
    private static final long MAX_RANGE = 199999000000000L; // ~6 years

    @Override
    public String getSignature() {
        return "rnd_interval()";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new RndFunction();
    }

    private static class RndFunction extends IntervalFunction implements Function, FunctionExtension {
        private final Interval interval = new Interval();
        private Rnd rnd;

        protected RndFunction() {
            super(ColumnType.INTERVAL_TIMESTAMP_MICRO);
        }

        @Override
        public FunctionExtension extendedOps() {
            return this;
        }

        @Override
        public int getArrayLength() {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull Interval getInterval(Record rec) {
            final long lo = rnd.nextLong(LO_BOUNDARY);
            final long range = rnd.nextLong(MAX_RANGE);
            return interval.of(lo, lo + range);
        }

        @Override
        public String getName() {
            return "rnd_interval";
        }

        @Override
        public Record getRecord(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence getStrA(Record rec, int arrayIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence getStrB(Record rec, int arrayIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getStrLen(Record rec, int arrayIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            rnd = executionContext.getRandom();
        }

        @Override
        public boolean isRandom() {
            return true;
        }
    }
}
