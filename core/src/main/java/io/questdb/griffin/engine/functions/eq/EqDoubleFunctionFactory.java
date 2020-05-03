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
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class EqDoubleFunctionFactory extends FunctionFactory {
    @Override
    public String getSignature() {
        return "=(DD)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
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
                    return new FuncIntIsNaN(position, right, isNegated);
                case ColumnType.LONG:
                    return new FuncLongIsNaN(position, right, isNegated);
                case ColumnType.DATE:
                    return new FuncDateIsNaN(position, right, isNegated);
                case ColumnType.TIMESTAMP:
                    return new FuncTimestampIsNaN(position, right, isNegated);
                case ColumnType.FLOAT:
                    return new FuncFloatIsNaN(position, right, isNegated);
                default:
                    // double
                    return new FuncDoubleIsNaN(position, right, isNegated);
            }
        } else if (right.isConstant() && right.getType() == ColumnType.DOUBLE && Double.isNaN(right.getDouble(null))) {
            switch (left.getType()) {
                case ColumnType.INT:
                    return new FuncIntIsNaN(position, left, isNegated);
                case ColumnType.LONG:
                    return new FuncLongIsNaN(position, left, isNegated);
                case ColumnType.DATE:
                    return new FuncDateIsNaN(position, left, isNegated);
                case ColumnType.TIMESTAMP:
                    return new FuncTimestampIsNaN(position, left, isNegated);
                case ColumnType.FLOAT:
                    return new FuncFloatIsNaN(position, left, isNegated);
                default:
                    // double
                    return new FuncDoubleIsNaN(position, left, isNegated);
            }
        }
        return new Func(position, args.getQuick(0), args.getQuick(1), isNegated);
    }

    @Override
    public boolean isNegatable() { return true; }

    protected class Func extends BooleanFunction implements BinaryFunction {
        private final boolean isNegated;
        protected final Function left;
        protected final Function right;

        public Func(int position, Function left, Function right, boolean isNegated) {
            super(position);
            this.left = left;
            this.right = right;
            this.isNegated = isNegated;
        }

        @Override
        public boolean getBool(Record rec) {
            final double l = left.getDouble(rec);
            final double r = right.getDouble(rec);
            return isNegated != (l != l && r != r || Math.abs(l - r) < 0.0000000001);
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

    protected class FuncIntIsNaN extends BooleanFunction implements UnaryFunction {
        private final boolean isNegated;
        protected final Function arg;

        public FuncIntIsNaN(int position, Function arg, boolean isNegated) {
            super(position);
            this.arg = arg;
            this.isNegated = isNegated;
        }

        @Override
        public boolean getBool(Record rec) {
            return isNegated != (arg.getInt(rec) == Numbers.INT_NaN);
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }

    protected class FuncLongIsNaN extends BooleanFunction implements UnaryFunction {
        private final boolean isNegated;
        protected final Function arg;

        public FuncLongIsNaN(int position, Function arg, boolean isNegated) {
            super(position);
            this.arg = arg;
            this.isNegated = isNegated;
        }

        @Override
        public boolean getBool(Record rec) {
            return isNegated != (arg.getLong(rec) == Numbers.LONG_NaN);
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }

    protected class FuncDateIsNaN extends BooleanFunction implements UnaryFunction {
        private final boolean isNegated;
        protected final Function arg;

        public FuncDateIsNaN(int position, Function arg, boolean isNegated) {
            super(position);
            this.arg = arg;
            this.isNegated = isNegated;
        }

        @Override
        public boolean getBool(Record rec) {
            return isNegated != (arg.getDate(rec) == Numbers.LONG_NaN);
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }

    protected class FuncTimestampIsNaN extends BooleanFunction implements UnaryFunction {
        private final boolean isNegated;
        protected final Function arg;

        public FuncTimestampIsNaN(int position, Function arg, boolean isNegated) {
            super(position);
            this.arg = arg;
            this.isNegated = isNegated;
        }

        @Override
        public boolean getBool(Record rec) {
            return isNegated != (arg.getTimestamp(rec) == Numbers.LONG_NaN);
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }

    protected class FuncFloatIsNaN extends BooleanFunction implements UnaryFunction {
        private final boolean isNegated;
        protected final Function arg;

        public FuncFloatIsNaN(int position, Function arg, boolean isNegated) {
            super(position);
            this.arg = arg;
            this.isNegated = isNegated;
        }

        @Override
        public boolean getBool(Record rec) {
            return isNegated != (Float.isNaN(arg.getFloat(rec)));
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }

    protected class FuncDoubleIsNaN extends BooleanFunction implements UnaryFunction {
        private final boolean isNegated;
        protected final Function arg;

        public FuncDoubleIsNaN(int position, Function arg, boolean isNegated) {
            super(position);
            this.arg = arg;
            this.isNegated = isNegated;
        }

        @Override
        public boolean getBool(Record rec) {
            return isNegated != (Double.isNaN(arg.getDouble(rec)));
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }
}
