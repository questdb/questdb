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

package io.questdb.griffin.engine.functions.lt;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class LtDoubleVVFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "<(DD)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function left = args.getQuick(0);
        final Function right = args.getQuick(1);
        // Nullability is static (table-level) information, and a constant's value is
        // known here, so the null handling is resolved ONCE at bind time: a never-null
        // operand's side keeps its raw value (a non-finite value is DATA there), and
        // only a nullable operand's side folds non-finite values onto NaN so they
        // compare as NULL. No per-row isNotNull() calls, no per-row nullability flags.
        final boolean leftNeverNull = isNeverNull(left);
        final boolean rightNeverNull = isNeverNull(right);
        if (leftNeverNull && rightNeverNull) {
            return new DataFunc(left, right);
        }
        if (leftNeverNull) {
            return new RightNullableFunc(left, right);
        }
        if (rightNeverNull) {
            return new LeftNullableFunc(left, right);
        }
        return new NullableFunc(left, right);
    }

    private static boolean isNeverNull(Function f) {
        return f.isNotNull() || (f.isConstant() && Numbers.isFinite(f.getDouble(null)));
    }

    private static class DataFunc extends AbstractLtBinaryFunction {
        public DataFunc(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            final double l = left.getDouble(rec);
            final double r = right.getDouble(rec);
            final boolean eq = Numbers.equals(l, r);
            return negated ? (eq || l > r) : (!eq && l < r);
        }
    }

    private static class LeftNullableFunc extends AbstractLtBinaryFunction {
        public LeftNullableFunc(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            final double leftValue = left.getDouble(rec);
            final double l = Numbers.isFinite(leftValue) ? leftValue : Double.NaN;
            final double r = right.getDouble(rec);
            final boolean eq = Numbers.equals(l, r);
            return negated ? (eq || l > r) : (!eq && l < r);
        }
    }

    private static class NullableFunc extends AbstractLtBinaryFunction {
        public NullableFunc(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            final double leftValue = left.getDouble(rec);
            final double rightValue = right.getDouble(rec);
            final double l = Numbers.isFinite(leftValue) ? leftValue : Double.NaN;
            final double r = Numbers.isFinite(rightValue) ? rightValue : Double.NaN;
            final boolean eq = Numbers.equals(l, r);
            return negated ? (eq || l > r) : (!eq && l < r);
        }
    }

    private static class RightNullableFunc extends AbstractLtBinaryFunction {
        public RightNullableFunc(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            final double l = left.getDouble(rec);
            final double rightValue = right.getDouble(rec);
            final double r = Numbers.isFinite(rightValue) ? rightValue : Double.NaN;
            final boolean eq = Numbers.equals(l, r);
            return negated ? (eq || l > r) : (!eq && l < r);
        }
    }
}
