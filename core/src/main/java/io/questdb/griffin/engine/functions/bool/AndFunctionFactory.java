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
import io.questdb.griffin.engine.functions.QuaternaryFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class AndFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "and(TT)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        Function leftFunc = args.getQuick(0);
        Function rightFunc = args.getQuick(1);
        if (leftFunc.isConstant()) {
            try (Function ignore = leftFunc) {
                if (leftFunc.getBool(null)) {
                    return rightFunc;
                }
                Misc.free(rightFunc);
                return BooleanConstant.FALSE;
            }
        }

        if (rightFunc.isConstant()) {
            try (Function ignore = rightFunc) {
                if (rightFunc.getBool(null)) {
                    return leftFunc;
                }
                Misc.free(leftFunc);
                return BooleanConstant.FALSE;
            }
        }

        if (leftFunc instanceof AndBooleanFunction leftAndFunc) {
            return new TernaryAndBooleanFunction(leftAndFunc.left, leftAndFunc.right, rightFunc);
        }

        if (leftFunc instanceof TernaryAndBooleanFunction leftAndFunc) {
            return new QuaternaryAndBooleanFunction(leftAndFunc.left, leftAndFunc.center, leftAndFunc.right, rightFunc);
        }

        return new AndBooleanFunction(leftFunc, rightFunc);
    }

    private static class AndBooleanFunction extends BooleanFunction implements BinaryFunction {
        final Function left;
        final Function right;

        public AndBooleanFunction(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean getBool(Record rec) {
            return left.getBool(rec) && right.getBool(rec);
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
            sink.val('(');
            sink.val(left);
            sink.val(" and ");
            sink.val(right);
            sink.val(')');
        }
    }

    private static class QuaternaryAndBooleanFunction extends BooleanFunction implements QuaternaryFunction {
        final Function func0;
        final Function func1;
        final Function func2;
        final Function func3;

        public QuaternaryAndBooleanFunction(Function func0, Function func1, Function func2, Function func3) {
            this.func0 = func0;
            this.func1 = func1;
            this.func2 = func2;
            this.func3 = func3;
        }

        @Override
        public boolean getBool(Record rec) {
            return func0.getBool(rec) && func1.getBool(rec) && func2.getBool(rec) && func3.getBool(rec);
        }

        @Override
        public Function getFunc0() {
            return func0;
        }

        @Override
        public Function getFunc1() {
            return func1;
        }

        @Override
        public Function getFunc2() {
            return func2;
        }

        @Override
        public Function getFunc3() {
            return func3;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val('(');
            sink.val(func0);
            sink.val(" and ");
            sink.val(func1);
            sink.val(" and ");
            sink.val(func2);
            sink.val(" and ");
            sink.val(func3);
            sink.val(')');
        }
    }

    private static class TernaryAndBooleanFunction extends BooleanFunction implements TernaryFunction {
        final Function center;
        final Function left;
        final Function right;

        public TernaryAndBooleanFunction(Function left, Function center, Function right) {
            this.left = left;
            this.center = center;
            this.right = right;
        }

        @Override
        public boolean getBool(Record rec) {
            return left.getBool(rec) && center.getBool(rec) && right.getBool(rec);
        }

        @Override
        public Function getCenter() {
            return center;
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
            sink.val('(');
            sink.val(left);
            sink.val(" and ");
            sink.val(center);
            sink.val(" and ");
            sink.val(right);
            sink.val(')');
        }
    }
}
