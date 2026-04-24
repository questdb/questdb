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
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class AddIntFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "+(II)";
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
        // null + x and x + null always evaluate to null. Fold to an int null constant at
        // construction time, so the non-null operand (which may be a column reference that
        // needs a record to evaluate) is never asked for its value with a null record.
        if (left.isConstant() && left.getInt(null) == Numbers.INT_NULL) {
            Misc.free(right);
            return IntConstant.NULL;
        }
        if (right.isConstant() && right.getInt(null) == Numbers.INT_NULL) {
            Misc.free(left);
            return IntConstant.NULL;
        }
        return new AddIntFunc(left, right);
    }

    private static class AddIntFunc extends IntFunction implements ArithmeticBinaryFunction {
        private final Function left;
        private final Function right;

        public AddIntFunc(Function left, Function right) {
            super();
            this.left = left;
            this.right = right;
        }

        @Override
        public int getInt(Record rec) {
            final int left = this.left.getInt(rec);
            final int right = this.right.getInt(rec);
            if (left == Numbers.INT_NULL || right == Numbers.INT_NULL) {
                return Numbers.INT_NULL;
            }
            return left + right;
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public long getLong(Record rec) {
            final int left = this.left.getInt(rec);
            final int right = this.right.getInt(rec);
            if (left == Numbers.INT_NULL || right == Numbers.INT_NULL) {
                return Numbers.LONG_NULL;
            }
            return ((long) left) + right;
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
