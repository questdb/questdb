/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.engine.functions.IPv4Function;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndIPv4CCFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "rnd_ipv4(ii)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {

        CharSequence subnetStr = args.getQuick(0).getStr(null);
        int nullRate = args.getQuick(1).getInt(null);
        int lo = Numbers.getIPv4Subnet(subnetStr);

        // first try getting subnet from ip, if arg is subnet (not ip) then try parsing again
        if (lo == -2) {
            lo = Numbers.parseSubnet(subnetStr);
        }

        int hi = Numbers.getBroadcastAddress(subnetStr);

        // invalid subnet
        if (lo == -2 || hi == -2) {
            throw SqlException.$(argPositions.getQuick(0), "invalid subnet: ").put(subnetStr);
        }

        return new RndFunction(lo, hi, nullRate);
    }

    private static class RndFunction extends IPv4Function implements Function {
        private final int hi;
        private final int lo;
        private final int nullRate;
        private final int range;
        private Rnd rnd;

        public RndFunction(int lo, int hi, int nullRate) {
            super();
            this.lo = lo;
            this.hi = hi;
            this.range = hi - lo + 1;
            this.nullRate = nullRate + 1;
        }

        @Override
        public int getIPv4(Record rec) {
            if ((rnd.nextInt() % nullRate) == 1) {
                return Numbers.IPv4_NULL;
            }
            return lo + rnd.nextPositiveInt() % range;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("rnd_ipv4(").val(lo).val(',').val(range + lo - 1).val(nullRate - 1).val(')');
        }

    }
}

