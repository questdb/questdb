/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class LtStrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "<(SS)";
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
            CharSequence constValue = a.getStr(null);
            if (constValue == null) {
                return BooleanConstant.FALSE;
            }
            return new ConstOnLeftFunc(constValue, b);
        }
        if (!a.isConstant() && b.isConstant()) {
            CharSequence constValue = b.getStr(null);
            if (constValue == null) {
                return BooleanConstant.FALSE;
            }
            return new ConstOnRightFunc(a, constValue);
        }
        return new Func(a, b);
    }

    private static class ConstOnLeftFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final CharSequence constant;
        private final Function right;

        public ConstOnLeftFunc(CharSequence constant, Function right) {
            this.constant = constant;
            this.right = right;
        }

        @Override
        public Function getArg() {
            return right;
        }

        @Override
        public boolean getBool(Record rec) {
            final CharSequence r = right.getStrB(rec);
            if (r == null) {
                return false;
            }
            return negated == (Chars.compare(constant, r) >= 0);
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
            sink.val('\'').val(constant).val('\'');
            if (negated) {
                sink.val(">=");
            } else {
                sink.val("<");
            }
            sink.val(right);
        }
    }

    private static class ConstOnRightFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final CharSequence constant;
        private final Function left;

        public ConstOnRightFunc(Function left, CharSequence constant) {
            this.left = left;
            this.constant = constant;
        }

        @Override
        public Function getArg() {
            return left;
        }

        @Override
        public boolean getBool(Record rec) {
            final CharSequence l = left.getStrB(rec);
            if (l == null) {
                return false;
            }
            return negated == (Chars.compare(l, constant) >= 0);
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
            if (negated) {
                sink.val(">=");
            } else {
                sink.val("<");
            }
            sink.val('\'').val(constant).val('\'');
        }
    }

    private static class Func extends NegatableBooleanFunction implements BinaryFunction {
        private final Function left;
        private final Function right;

        public Func(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean getBool(Record rec) {
            // important to compare A and B strings in case
            // these are columns of the same record
            // records have re-usable character sequences
            final CharSequence l = left.getStr(rec);
            final CharSequence r = right.getStrB(rec);
            if (l == null || r == null) {
                return false;
            }
            return negated == (Chars.compare(l, r) >= 0);
        }

        @Override
        public Function getLeft() {
            return left;
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
        public Function getRight() {
            return right;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(left);
            if (negated) {
                sink.val(">=");
            } else {
                sink.val('<');
            }
            sink.val(right);
        }
    }
}
