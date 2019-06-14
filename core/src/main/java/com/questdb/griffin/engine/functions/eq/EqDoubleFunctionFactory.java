/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.functions.eq;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.engine.functions.BinaryFunction;
import com.questdb.griffin.engine.functions.BooleanFunction;
import com.questdb.griffin.engine.functions.UnaryFunction;
import com.questdb.std.Numbers;
import com.questdb.std.ObjList;

public class EqDoubleFunctionFactory implements FunctionFactory {
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
        Function rigt = args.getQuick(1);

        if (left.isConstant() && left.getType() == ColumnType.DOUBLE && Double.isNaN(left.getDouble(null))) {
            switch (rigt.getType()) {
                case ColumnType.INT:
                    return new FuncIntIsNaN(position, rigt);
                case ColumnType.LONG:
                    return new FuncLongIsNaN(position, rigt);
                case ColumnType.DATE:
                    return new FuncDateIsNaN(position, rigt);
                case ColumnType.TIMESTAMP:
                    return new FuncTimestampIsNaN(position, rigt);
                case ColumnType.FLOAT:
                    return new FuncFloatIsNaN(position, rigt);
                default:
                    // double
                    return new FuncDoubleIsNaN(position, rigt);
            }
        } else if (rigt.isConstant() && rigt.getType() == ColumnType.DOUBLE && Double.isNaN(rigt.getDouble(null))) {
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

    private static class Func extends BooleanFunction implements BinaryFunction {
        private final Function left;
        private final Function right;

        public Func(int position, Function left, Function right) {
            super(position);
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean getBool(Record rec) {
            final double l = left.getDouble(rec);
            final double r = right.getDouble(rec);
            return l != l && r != r || Math.abs(l - r) < 0.0000000001;
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

    private static class FuncIntIsNaN extends BooleanFunction implements UnaryFunction {
        private final Function arg;

        public FuncIntIsNaN(int position, Function arg) {
            super(position);
            this.arg = arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return arg.getInt(rec) == Numbers.INT_NaN;
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }

    private static class FuncLongIsNaN extends BooleanFunction implements UnaryFunction {
        private final Function arg;

        public FuncLongIsNaN(int position, Function arg) {
            super(position);
            this.arg = arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return arg.getLong(rec) == Numbers.LONG_NaN;
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }

    private static class FuncDateIsNaN extends BooleanFunction implements UnaryFunction {
        private final Function arg;

        public FuncDateIsNaN(int position, Function arg) {
            super(position);
            this.arg = arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return arg.getDate(rec) == Numbers.LONG_NaN;
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }

    private static class FuncTimestampIsNaN extends BooleanFunction implements UnaryFunction {
        private final Function arg;

        public FuncTimestampIsNaN(int position, Function arg) {
            super(position);
            this.arg = arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return arg.getTimestamp(rec) == Numbers.LONG_NaN;
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }

    private static class FuncFloatIsNaN extends BooleanFunction implements UnaryFunction {
        private final Function arg;

        public FuncFloatIsNaN(int position, Function arg) {
            super(position);
            this.arg = arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return Float.isNaN(arg.getFloat(rec));
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }

    private static class FuncDoubleIsNaN extends BooleanFunction implements UnaryFunction {
        private final Function arg;

        public FuncDoubleIsNaN(int position, Function arg) {
            super(position);
            this.arg = arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return Double.isNaN(arg.getDouble(rec));
        }

        @Override
        public Function getArg() {
            return arg;
        }
    }
}
