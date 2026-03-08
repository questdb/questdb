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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UuidFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndUuidCCFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rnd_uuid4(i)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new RndFunction(args.getQuick(0).getInt(null));
    }

    private static class RndFunction extends UuidFunction implements Function {
        private final int nanRate;
        private boolean isNull = false;
        private Rnd rnd;

        public RndFunction(int nanRate) {
            this.nanRate = nanRate + 1;
        }

        @Override
        public long getLong128Hi(Record rec) {
            if (isNull) {
                isNull = false;
                return Numbers.LONG_NULL;
            }
            long hi = rnd.nextLong();
            // set version to 4
            hi &= 0xffffffffffff0fffL;
            hi |= 0x0000000000004000L;
            return hi;
        }

        @Override
        public long getLong128Lo(Record rec) {
            long lo = rnd.nextLong();
            if ((lo % nanRate) == 1) {
                isNull = true;
                return Numbers.LONG_NULL;
            }
            // set variant to 1
            lo &= 0x3fffffffffffffffL;
            lo |= 0x8000000000000000L;
            return lo;
        }

        @Override
        public String getName() {
            return "rnd_uuid4";
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            rnd = executionContext.getRandom();
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
        public boolean shouldMemoize() {
            return true;
        }
    }
}
