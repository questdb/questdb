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
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;

/**
 * Decodes a 64-bit long value back to a CIDR string (e.g., "192.168.1.0/24").
 * <p>
 * Decoding format:
 * <pre>
 * uint32_t addr = (uint32_t)(v &gt;&gt; 6);
 * uint8_t  pfx  = (uint8_t)(v &amp; 0x3F);  // 0x3F = 6 bits
 * </pre>
 */
public class LongToCidrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cidr(L)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new LongToCidrFunc(args.getQuick(0));
    }

    private static class LongToCidrFunc extends VarcharFunction implements UnaryFunction {
        private final Function arg;
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();

        public LongToCidrFunc(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            return toCidr(rec, sinkA);
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            return toCidr(rec, sinkB);
        }

        private Utf8Sequence toCidr(Record rec, Utf8StringSink sink) {
            final long value = arg.getLong(rec);
            if (value == Numbers.LONG_NULL) {
                return null;
            }

            // Decode: addr = v >> 6, prefix = v & 0x3F
            int addr = (int) (value >> 6);
            int prefix = (int) (value & 0x3F);

            sink.clear();
            Numbers.intToIPv4Sink(sink, addr);
            sink.putAscii('/');
            sink.put(prefix);
            return sink;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("cidr(").val(arg).val(')');
        }
    }
}
