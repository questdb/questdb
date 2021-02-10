/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
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
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        // this is probably a special case factory
        // NaN is always a double, so this could lead comparisons of all primitive types
        // to NaN route to this factory. Obviously comparing naively will not work
        // We have to check arg types and when NaN is present we would generate special case
        // functions for NaN checks.

        Function left = args.getQuick(0);
        Function right = args.getQuick(1);

        if (left.isConstant() && left.getType() == ColumnType.DOUBLE && Double.isNaN(left.getDouble(null))) {
            switch (right.getType()) {
                case ColumnType.INT:
                    return new FuncIntIsNaN(position, right);
                case ColumnType.LONG:
                    return new FuncLongIsNaN(position, right);
                case ColumnType.DATE:
                    return new FuncDateIsNaN(position, right);
                case ColumnType.TIMESTAMP:
                    return new FuncTimestampIsNaN(position, right);
                case ColumnType.FLOAT:
                    return new FuncFloatIsNaN(position, right);
                default:
                    // double
                    return new FuncDoubleIsNaN(position, right);
            }
        } else if (right.isConstant() && right.getType() == ColumnType.DOUBLE && Double.isNaN(right.getDouble(null))) {
            switch (left.getType()) {
                case ColumnType.INT:
                    return new FuncIntIsNaN(position, left);
                case ColumnType.LONG:
                    return new FuncLongIsNaN(position, left);
                case ColumnType.DATE:
                    return new FuncDateIsNaN(position, left);
                case ColumnType.TIMESTAMP:
                    return new FuncTimestampIsNaN(position, left);
                case ColumnType.FLOAT:
                    return new FuncFloatIsNaN(position, left);
                default:
                    // double
                    return new FuncDoubleIsNaN(position, left);
            }
        }
        return new Func(position, args.getQuick(0), args.getQuick(1));
    }

    protected static class Func extends NegatableBooleanFunction implements BinaryFunction {
        protected final Function left;
        protected final Function right;

        public Func(int position, Function left, Function right) {
            super(position);
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean getBool(Record rec) {
            final double l = left.getDouble(rec);
            final double r = right.getDouble(rec);
            return negated != (l != l && r != r || Math.abs(l - r) < 0.0000000001);
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public Function getRight() {
            return right;
        }
    }

    protected static class FuncIntIsNaN extends NegatableBooleanFunction implements UnaryFunction {
        protected final Function arg;

        public FuncIntIsNaN(int position, Function arg) {
            super(position);
            this.arg = arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (arg.getInt(rec) == Numbers.INT_NaN);
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }

    protected static class FuncLongIsNaN extends NegatableBooleanFunction implements UnaryFunction {
        protected final Function arg;

        public FuncLongIsNaN(int position, Function arg) {
            super(position);
            this.arg = arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (arg.getLong(rec) == Numbers.LONG_NaN);
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }

    protected static class FuncDateIsNaN extends NegatableBooleanFunction implements UnaryFunction {
        protected final Function arg;

        public FuncDateIsNaN(int position, Function arg) {
            super(position);
            this.arg = arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (arg.getDate(rec) == Numbers.LONG_NaN);
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }

    protected static class FuncTimestampIsNaN extends NegatableBooleanFunction implements UnaryFunction {
        protected final Function arg;

        public FuncTimestampIsNaN(int position, Function arg) {
            super(position);
            this.arg = arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (arg.getTimestamp(rec) == Numbers.LONG_NaN);
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }

    protected static class FuncFloatIsNaN extends NegatableBooleanFunction implements UnaryFunction {
        protected final Function arg;

        public FuncFloatIsNaN(int position, Function arg) {
            super(position);
            this.arg = arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (Float.isNaN(arg.getFloat(rec)));
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }

    protected static class FuncDoubleIsNaN extends NegatableBooleanFunction implements UnaryFunction {
        protected final Function arg;

        public FuncDoubleIsNaN(int position, Function arg) {
            super(position);
            this.arg = arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (Double.isNaN(arg.getDouble(rec)));
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }
}
