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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;

public class EqSymStrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(KS)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        // there are optimisation opportunities
        // 1. when one of args is constant null comparison can boil down to checking
        //    length of non-constant (must be -1)
        // 2. when one of arguments is constant, save method call and use a field

        Function symFunc = args.getQuick(0);
        Function strFunc = args.getQuick(1);

        // SYMBOL cannot be constant
        if (strFunc.isConstant()) {
            return createHalfConstantFunc(position, strFunc, symFunc);
        }
        return new Func(position, symFunc, strFunc);
    }

    private Function createHalfConstantFunc(int position, Function constFunc, Function varFunc) {
        CharSequence constValue = constFunc.getStr(null);
        if (varFunc instanceof SymbolColumn) {
            if (constValue == null) {
                return new NullCheckColumnFunc(position, varFunc);
            }
            return new ConstCheckColumnFunc(position, (SymbolColumn) varFunc, constValue);
        } else {
            if (constValue == null) {
                return new NullCheckFunc(position, varFunc);
            }
            return new ConstCheckFunc(position, varFunc, constValue);
        }
    }

    private static class NullCheckFunc extends BooleanFunction implements UnaryFunction {
        private final Function arg;

        public NullCheckFunc(int position, Function arg) {
            super(position);
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return arg.getSymbol(rec) == null;
        }
    }

    private static class NullCheckColumnFunc extends BooleanFunction implements UnaryFunction {
        private final Function arg;

        public NullCheckColumnFunc(int position, Function arg) {
            super(position);
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return arg.getInt(rec) == SymbolTable.VALUE_IS_NULL;
        }

        @Override
        public void init(RecordCursor recordCursor, SqlExecutionContext executionContext) {

        }
    }

    private static class ConstCheckFunc extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private final CharSequence constant;

        public ConstCheckFunc(int position, Function arg, CharSequence constant) {
            super(position);
            this.arg = arg;
            this.constant = constant;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return Chars.equalsNc(constant, arg.getSymbol(rec));
        }
    }

    private static class ConstCheckColumnFunc extends BooleanFunction implements UnaryFunction {
        private final SymbolColumn arg;
        private final CharSequence constant;
        private int valueIndex;

        public ConstCheckColumnFunc(int position, SymbolColumn arg, CharSequence constant) {
            super(position);
            this.arg = arg;
            this.constant = constant;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            if (valueIndex == SymbolTable.VALUE_NOT_FOUND) {
                return false;
            }
            return arg.getInt(rec) == valueIndex;
        }

        @Override
        public void init(RecordCursor recordCursor, SqlExecutionContext executionContext) {
            valueIndex = recordCursor.getSymbolTable(arg.getColumnIndex()).getQuick(constant);
        }
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
        public Function getLeft() {
            return left;
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public boolean getBool(Record rec) {
            // important to compare A and B strings in case
            // these are columns of the same record
            // records have re-usable character sequences
            final CharSequence a = left.getSymbol(rec);
            final CharSequence b = right.getStr(rec);

            if (a == null) {
                return b == null;
            }

            return Chars.equalsNc(a, b);
        }
    }
}
