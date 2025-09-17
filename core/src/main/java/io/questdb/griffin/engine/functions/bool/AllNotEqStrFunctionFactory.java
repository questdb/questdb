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

package io.questdb.griffin.engine.functions.bool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.FunctionExtension;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class AllNotEqStrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "<>all(Sw)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        FunctionExtension arrayFunction = args.getQuick(1).extendedOps();
        int arraySize = arrayFunction.getArrayLength();
        if (arraySize == 0) {
            return BooleanConstant.TRUE;
        }

        CharSequenceHashSet set = new CharSequenceHashSet();
        for (int i = 0; i < arraySize; i++) {
            set.add(arrayFunction.getStrA(null, i));
        }

        Function var = args.getQuick(0);
        if (var.isConstant()) {
            CharSequence str = var.getStrA(null);
            return BooleanConstant.of(str != null && set.excludes(str));
        }

        return new AllNotEqualStrFunction(var, set);
    }

    private static class AllNotEqualStrFunction extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private final CharSequenceHashSet set;

        private AllNotEqualStrFunction(Function arg, CharSequenceHashSet set) {
            this.arg = arg;
            this.set = set;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence str = arg.getStrA(rec);
            return str != null && set.excludes(str);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val(" <> all ").val(set);
        }
    }
}
