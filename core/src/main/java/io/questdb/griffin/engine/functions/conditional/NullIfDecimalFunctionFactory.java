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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.ToDecimalFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class NullIfDecimalFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "nullif(ΞΞ)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new Func(args.getQuick(0), args.getQuick(1));
    }

    private static class Func extends ToDecimalFunction implements BinaryFunction {
        private final Function arg0;
        private final Function arg1;

        public Func(Function arg0, Function arg1) {
            super(arg0.getType());
            this.arg0 = arg0;
            this.arg1 = arg1;
        }

        @Override
        public Function getLeft() {
            return arg0;
        }

        @Override
        public Function getRight() {
            return arg1;
        }

        public boolean store(Record record) {
            DecimalUtil.load(decimal, arg1, record);
            long hh = decimal.getHh();
            long hl = decimal.getHl();
            long lh = decimal.getLh();
            long ll = decimal.getLl();
            int scale = decimal.getScale();
            DecimalUtil.load(decimal, arg0, record);
            return decimal.compareTo(hh, hl, lh, ll, scale) != 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("nullif(").val(arg0).val(',').val(arg1).val(')');
        }
    }
}
