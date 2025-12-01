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

package io.questdb.griffin.engine.functions.math;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

/**
 * Checks if an IPv4 address is contained within a CIDR network.
 * <p>
 * Takes a long-encoded CIDR value and an IPv4 address, returns true if the
 * address belongs to the network.
 * <p>
 * The CIDR encoding format:
 * <pre>
 * uint64_t v = ((uint64_t)normalized_ipv4 &lt;&lt; 6) | (uint64_t)prefix;
 * </pre>
 */
public class CidrContainsFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cidr_contains(LX)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new CidrContainsFunc(args.getQuick(0), args.getQuick(1));
    }

    private static class CidrContainsFunc extends BooleanFunction implements BinaryFunction {
        private final Function cidrFunc;
        private final Function ipv4Func;

        public CidrContainsFunc(Function cidrFunc, Function ipv4Func) {
            this.cidrFunc = cidrFunc;
            this.ipv4Func = ipv4Func;
        }

        @Override
        public Function getLeft() {
            return cidrFunc;
        }

        @Override
        public Function getRight() {
            return ipv4Func;
        }

        @Override
        public boolean getBool(Record rec) {
            final long cidr = cidrFunc.getLong(rec);
            final int ipv4 = ipv4Func.getIPv4(rec);

            if (cidr == Numbers.LONG_NULL || ipv4 == Numbers.IPv4_NULL) {
                return false;
            }

            // Decode CIDR: addr = v >> 6, prefix = v & 0x3F
            int networkAddr = (int) (cidr >> 6);
            int prefix = (int) (cidr & 0x3F);

            // Create netmask from prefix length
            // prefix=24 -> netmask=0xFFFFFF00
            int netmask = prefix == 0 ? 0 : (0xFFFFFFFF << (32 - prefix));

            // Check if IPv4 address belongs to the network
            return (ipv4 & netmask) == (networkAddr & netmask);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("cidr_contains(").val(cidrFunc).val(',').val(ipv4Func).val(')');
        }
    }
}
