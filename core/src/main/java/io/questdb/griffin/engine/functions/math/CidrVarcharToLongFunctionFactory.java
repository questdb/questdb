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
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;

/**
 * Encodes a CIDR varchar (e.g., "192.168.1.0/24") into a 64-bit long value.
 * <p>
 * Encoding format:
 * <pre>
 * uint64_t v = ((uint64_t)normalized_ipv4 &lt;&lt; 6) | (uint64_t)prefix;
 * </pre>
 * Where:
 * <ul>
 *   <li>normalized_ipv4 is the IPv4 address with host bits zeroed</li>
 *   <li>prefix is the CIDR prefix length (0-32), stored in the lower 6 bits</li>
 * </ul>
 */
public class CidrVarcharToLongFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cidr(Ø)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new CidrVarcharToLongFunc(args.getQuick(0));
    }

    private static class CidrVarcharToLongFunc extends LongFunction implements UnaryFunction {
        private final Function arg;

        public CidrVarcharToLongFunc(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getLong(Record rec) {
            final Utf8Sequence value = arg.getVarcharA(rec);
            if (value == null) {
                return Numbers.LONG_NULL;
            }
            try {
                return Numbers.parseCidr(value);
            } catch (NumericException e) {
                return Numbers.LONG_NULL;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("cidr(").val(arg).val(')');
        }
    }
}
