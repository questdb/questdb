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

package io.questdb.griffin.engine.functions.lt;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

public class LtVarcharStrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "<(ØS)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function a = args.getQuick(0);
        final Function b = args.getQuick(1);
        if (a.isConstant() && !b.isConstant()) {
            CharSequence constValue = a.getStrA(null);
            if (constValue == null) {
                return BooleanConstant.FALSE;
            }
            return new LtStrFunctionFactory.ConstOnLeftFunc(constValue, b);
        }
        if (!a.isConstant() && b.isConstant()) {
            Utf8Sequence constValue = b.getVarcharA(null);
            if (constValue == null) {
                return BooleanConstant.FALSE;
            }
            return new ConstOnRightFunc(a, constValue);
        }
        // This implementation does not handle runtime constant for optimisations.
        // This is deemed to be unpopular function, so we don't need to optimise it.
        return new LtStrFunctionFactory.Func(a, b);
    }

    static class ConstOnRightFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final Utf8Sequence constant;
        private final Function left;

        public ConstOnRightFunc(Function left, Utf8Sequence constant) {
            this.constant = constant;
            this.left = left;
        }

        @Override
        public Function getArg() {
            return left;
        }

        @Override
        public boolean getBool(Record rec) {
            return Utf8s.lessThan(
                    left.getVarcharA(rec),
                    constant,
                    negated
            );
        }

        @Override
        public String getName() {
            if (negated) {
                return ">=";
            } else {
                return "<";
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(left);
            sink.val(getName());
            sink.val('\'').val(constant).val('\'');
        }
    }
}
