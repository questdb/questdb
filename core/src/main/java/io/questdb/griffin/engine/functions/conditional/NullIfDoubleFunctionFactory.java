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
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class NullIfDoubleFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "nullif(DD)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new NullIfDoubleFunction(args.getQuick(0), args.getQuick(1));
    }

    private static class NullIfDoubleFunction extends DoubleFunction implements BinaryFunction {
        private final Function doubleFunc1;
        private final Function doubleFunc2;

        public NullIfDoubleFunction(Function doubleFunc1, Function doubleFunc2) {
            this.doubleFunc1 = doubleFunc1;
            this.doubleFunc2 = doubleFunc2;
        }

        @Override
        public double getDouble(Record rec) {
            return Numbers.equals(doubleFunc1.getDouble(rec), doubleFunc2.getDouble(rec)) ? Double.NaN : doubleFunc1.getDouble(rec);

        }

        @Override
        public Function getLeft() {
            return doubleFunc1;
        }

        @Override
        public Function getRight() {
            return doubleFunc2;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("nullif(").val(doubleFunc1).val(',').val(doubleFunc2).val(')');
        }
    }
}
