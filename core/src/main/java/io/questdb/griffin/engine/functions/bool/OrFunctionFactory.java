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

package io.questdb.griffin.engine.functions.bool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class OrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "or(TT)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function leftFunc = args.getQuick(0);
        final Function rightFunc = args.getQuick(1);
        if (leftFunc.isConstant()) {
            try {
                if (leftFunc.getBool(null)) {
                    Misc.free(rightFunc);
                    return BooleanConstant.TRUE;
                }
                return rightFunc;
            } finally {
                leftFunc.close();
            }
        }

        if (rightFunc.isConstant()) {
            try {
                if (rightFunc.getBool(null)) {
                    Misc.free(leftFunc);
                    return BooleanConstant.TRUE;
                }
                return leftFunc;
            } finally {
                Misc.free(rightFunc);
            }
        }
        return new MyBooleanFunction(leftFunc, rightFunc);
    }

    private static class MyBooleanFunction extends BooleanFunction implements BinaryFunction {
        final Function left;
        final Function right;

        public MyBooleanFunction(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean getBool(Record rec) {
            return left.getBool(rec) || right.getBool(rec);
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val('(').val(left).val(" or ").val(right).val(')');
        }
    }
}
