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

package io.questdb.griffin.engine.functions.math;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class AddLongFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "+(LL)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function left = args.getQuick(0);
        final Function right = args.getQuick(1);
        // null + x and x + null always evaluate to null. Fold at construction time so the
        // non-null operand (potentially a column reference) is never evaluated with a null
        // record via FunctionParser.functionToConstant().
        if (left.isConstant() && left.getLong(null) == Numbers.LONG_NULL) {
            Misc.free(right);
            return LongConstant.NULL;
        }
        if (right.isConstant() && right.getLong(null) == Numbers.LONG_NULL) {
            Misc.free(left);
            return LongConstant.NULL;
        }
        return new AddLongFunc(left, right);
    }

    private static class AddLongFunc extends LongFunction implements ArithmeticBinaryFunction {
        private final Function left;
        private final Function right;

        public AddLongFunc(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public long getLong(Record rec) {
            final long l = left.getLong(rec);
            final long r = right.getLong(rec);
            if (l == Numbers.LONG_NULL || r == Numbers.LONG_NULL) {
                return Numbers.LONG_NULL;
            }
            return l + r;
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(left).val('+').val(right);
        }
    }
}
