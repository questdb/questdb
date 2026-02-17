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
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class NullIfLongFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "nullif(LL)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new NullIfLongFunction(args.getQuick(0), args.getQuick(1));
    }

    private static class NullIfLongFunction extends LongFunction implements BinaryFunction {
        private final Function longFunc1;
        private final Function longFunc2;

        public NullIfLongFunction(Function longFunc1, Function longFunc2) {
            this.longFunc1 = longFunc1;
            this.longFunc2 = longFunc2;
        }

        @Override
        public Function getLeft() {
            return longFunc1;
        }

        @Override
        public long getLong(Record rec) {
            return longFunc1.getLong(rec) == longFunc2.getLong(rec) ? Numbers.LONG_NULL : longFunc1.getLong(rec);
        }

        @Override
        public Function getRight() {
            return longFunc2;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("nullif(").val(longFunc1).val(',').val(longFunc2).val(')');
        }
    }
}
