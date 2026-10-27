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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * The OFFSET of a SAMPLE BY, which reads a NULL offset as the default offset, so that a NULL offset aligns the buckets
 * as an omitted one does. SampleByBinder binds every offset but the default and a literal through this function, so
 * that a runtime-constant offset, such as a bind variable bound to NULL, takes the default when a cursor reads it. The
 * function reads its argument's text otherwise and is transparent in plans.
 */
public class SampleByOffsetFunctionFactory implements FunctionFactory {
    public static final String NAME = "sample_by_offset";
    private static final String DEFAULT_OFFSET = "00:00";

    @Override
    public String getSignature() {
        return NAME + "(S)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new Func(args.getQuick(0));
    }

    private static class Func extends StrFunction implements UnaryFunction {
        private final Function arg;

        Func(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            final CharSequence offset = arg.getStrA(rec);
            return offset != null ? offset : DEFAULT_OFFSET;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            final CharSequence offset = arg.getStrB(rec);
            return offset != null ? offset : DEFAULT_OFFSET;
        }

        @Override
        public int getStrLen(Record rec) {
            final int len = arg.getStrLen(rec);
            return len >= 0 ? len : DEFAULT_OFFSET.length();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }
}
