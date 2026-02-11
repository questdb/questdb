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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class EqDoubleFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(DD)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        // this is probably a special case factory
        // NaN is always a double, so this could lead comparisons of all primitive types
        // to NaN route to this factory. Obviously comparing naively will not work
        // We have to check arg types and when NaN is present we would generate special case
        // functions for NaN checks.

        Function left = args.getQuick(0);
        Function right = args.getQuick(1);

        int leftType = left.getType();
        int rightType = right.getType();

        if (isNullConstant(left, leftType)) {
            return dispatchUnaryFunc(right, rightType);
        }
        if (isNullConstant(right, rightType)) {
            return dispatchUnaryFunc(left, leftType);
        }
        return new Func(args.getQuick(0), args.getQuick(1));
    }

    private static Function dispatchUnaryFunc(Function operand, int operandType) {
        return switch (ColumnType.tagOf(operandType)) {
            case ColumnType.INT -> new FuncIntIsNaN(operand);
            case ColumnType.LONG -> new FuncLongIsNaN(operand);
            case ColumnType.DATE -> new FuncDateIsNaN(operand);
            case ColumnType.TIMESTAMP -> new FuncTimestampIsNaN(operand);
            case ColumnType.FLOAT -> new FuncFloatIsNaN(operand);
            default ->
                // double
                    new FuncDoubleIsNaN(operand);
        };
    }

    private static boolean isNullConstant(Function operand, int operandType) {
        return operand.isConstant() &&
                (ColumnType.isDouble(operandType) && Numbers.isNull(operand.getDouble(null))
                        ||
                        operandType == ColumnType.NULL);
    }

    protected static abstract class AbstractIsNaNFunction extends NegatableBooleanFunction implements UnaryFunction {
        protected final Function arg;

        public AbstractIsNaNFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
            if (negated) {
                sink.val(" is not null");
            } else {
                sink.val(" is null");
            }
        }
    }

    protected static class Func extends AbstractEqBinaryFunction {
        // This function class uses both subtraction and equality to judge whether two parameters
        // are equal because subtraction is to prevent the judging mistakes with different
        // precision, and equality is for the comparison of Infinity. In java,
        // Infinity - Infinity = NaN, so it won't satisfy the subtraction situation. But
        // Infinity = Infinity, so use equality could solve this problem.

        public Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            final double l = left.getDouble(rec);
            final double r = right.getDouble(rec);
            return negated != Numbers.equals(l, r);
        }
    }

    protected static class FuncDateIsNaN extends AbstractIsNaNFunction {
        public FuncDateIsNaN(Function arg) {
            super(arg);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (arg.getDate(rec) == Numbers.LONG_NULL);
        }
    }

    protected static class FuncDoubleIsNaN extends AbstractIsNaNFunction {
        public FuncDoubleIsNaN(Function arg) {
            super(arg);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (Numbers.isNull(arg.getDouble(rec)));
        }
    }

    protected static class FuncFloatIsNaN extends AbstractIsNaNFunction {
        public FuncFloatIsNaN(Function arg) {
            super(arg);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (Numbers.isNull(arg.getFloat(rec)));
        }
    }

    protected static class FuncIntIsNaN extends AbstractIsNaNFunction {
        public FuncIntIsNaN(Function arg) {
            super(arg);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (arg.getInt(rec) == Numbers.INT_NULL);
        }
    }

    protected static class FuncLongIsNaN extends AbstractIsNaNFunction {
        public FuncLongIsNaN(Function arg) {
            super(arg);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (arg.getLong(rec) == Numbers.LONG_NULL);
        }
    }

    protected static class FuncTimestampIsNaN extends AbstractIsNaNFunction {
        public FuncTimestampIsNaN(Function arg) {
            super(arg);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (arg.getTimestamp(rec) == Numbers.LONG_NULL);
        }
    }
}
