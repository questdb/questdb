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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

/**
 * Generates random CIDR values encoded as long.
 * <p>
 * The encoding format is: ((uint64_t)normalized_ipv4 &lt;&lt; 6) | (uint64_t)prefix
 * <p>
 * Generates prefix lengths between 8 and 30 (inclusive) to produce reasonable
 * network sizes for testing.
 */
public class RndCidrFunctionFactory implements FunctionFactory {
    private static final String SIGNATURE = "rnd_cidr()";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new RndCidrFunction();
    }

    private static class RndCidrFunction extends LongFunction {
        // Prefix range: 8 to 30 (inclusive)
        // This gives networks from /8 (16M hosts) to /30 (4 hosts)
        private static final int MAX_PREFIX = 30;
        private static final int MIN_PREFIX = 8;
        private static final int PREFIX_RANGE = MAX_PREFIX - MIN_PREFIX + 1;

        private Rnd rnd;

        @Override
        public long getLong(Record rec) {
            // Generate random prefix between MIN_PREFIX and MAX_PREFIX
            int prefix = MIN_PREFIX + rnd.nextPositiveInt() % PREFIX_RANGE;
            // Generate random IPv4 address
            int ipv4 = rnd.nextInt();
            return Numbers.toCidr(ipv4, prefix);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
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

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SIGNATURE);
        }
    }
}
