/*+*****************************************************************************
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
import io.questdb.griffin.engine.functions.LongWidthIntFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class NullIfIntFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "nullif(II)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new NullIfIntFunction(args.getQuick(0), args.getQuick(1));
    }

    private static class NullIfIntFunction extends LongWidthIntFunction implements BinaryFunction {
        private final Function intFunc1;
        private final Function intFunc2;
        private final boolean isFunc1IntWidthStable;

        public NullIfIntFunction(Function intFunc1, Function intFunc2) {
            this.intFunc1 = intFunc1;
            this.intFunc2 = intFunc2;
            this.isFunc1IntWidthStable = intFunc1.isIntWidthStable();
        }

        @Override
        public int getInt(Record rec) {
            return intFunc1.getInt(rec) == intFunc2.getInt(rec) ? Numbers.INT_NULL : intFunc1.getInt(rec);
        }

        @Override
        public Function getLeft() {
            return intFunc1;
        }

        @Override
        public long getLong(Record rec) {
            // Compares at INT width, exactly as getInt() does, so both getters null out the same
            // rows. A width-stable first argument carries its whole value in that read, so it is
            // not read again - a second read of a non-deterministic one would be a fresh draw.
            final int value = intFunc1.getInt(rec);
            if (value == intFunc2.getInt(rec)) {
                return Numbers.LONG_NULL;
            }
            return isFunc1IntWidthStable ? value : intFunc1.getLong(rec);
        }

        @Override
        public Function getRight() {
            return intFunc2;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("nullif(").val(intFunc1).val(',').val(intFunc2).val(')');
        }
    }
}
