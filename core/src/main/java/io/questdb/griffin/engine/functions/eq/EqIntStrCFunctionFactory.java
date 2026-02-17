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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

public class EqIntStrCFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(Is)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        try {
            final CharSequence value = args.getQuick(1).getStrA(null);
            if (value == null) {
                return new Func(args.getQuick(0), Numbers.INT_NULL);
            }
            return new Func(args.getQuick(0), Numbers.parseInt(value));
        } catch (NumericException e) {
            return new NegatedAwareBooleanConstantFunc();
        }
    }

    private static class Func extends NegatableBooleanFunction implements UnaryFunction {
        private final Function left;
        private final int right;

        public Func(Function left, int right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Function getArg() {
            return left;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (left.getInt(rec) == right);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(left);
            if (negated) {
                sink.val('!');
            }
            sink.val('=');
            if (right != Numbers.INT_NULL) {
                sink.val(right);
            } else {
                sink.val("null");
            }
        }
    }

    private static class NegatedAwareBooleanConstantFunc extends NegatableBooleanFunction implements ConstantFunction {
        @Override
        public boolean getBool(Record rec) {
            return negated;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(negated);
        }
    }
}
