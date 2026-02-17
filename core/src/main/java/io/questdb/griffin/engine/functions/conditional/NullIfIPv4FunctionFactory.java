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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.IPv4Function;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class NullIfIPv4FunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "nullif(XS)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new NullIfIPv4FunctionFactory.NullIfIPv4Function(args.getQuick(0), args.getQuick(1));
    }

    private static class NullIfIPv4Function extends IPv4Function implements BinaryFunction {
        private final Function arg1;
        private final Function arg2;

        public NullIfIPv4Function(Function arg1, Function arg2) {
            this.arg1 = arg1;
            this.arg2 = arg2;
        }

        @Override
        public int getIPv4(Record rec) {
            return arg1.getIPv4(rec) == arg2.getIPv4(rec) ? Numbers.IPv4_NULL : arg1.getIPv4(rec);
        }

        @Override
        public Function getLeft() {
            return arg1;
        }

        @Override
        public Function getRight() {
            return arg2;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("nullif(").val(arg1).val(',').val(arg2).val(')');
        }
    }
}
