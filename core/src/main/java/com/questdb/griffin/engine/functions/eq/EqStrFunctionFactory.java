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
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.engine.functions.BinaryFunction;
import com.questdb.griffin.engine.functions.BooleanFunction;
import com.questdb.griffin.engine.functions.UnaryFunction;
import com.questdb.std.Chars;
import com.questdb.std.ObjList;

public class EqStrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(SS)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        // there are optimisation opportunities
        // 1. when one of args is constant null comparison can boil down to checking
        //    length of non-constant (must be -1)
        // 2. when one of arguments is constant, save method call and use a field

        Function a = args.getQuick(0);
        Function b = args.getQuick(1);

        if (a.isConstant() && !b.isConstant()) {
            return createHalfConstantFunc(position, a, b);
        }

        if (!a.isConstant() && b.isConstant()) {
            return createHalfConstantFunc(position, b, a);
        }

        return new Func(position, a, b);
    }

    private Function createHalfConstantFunc(int position, Function constFunc, Function varFunc) {
        CharSequence constValue = constFunc.getStr(null);

        if (constValue == null) {
            return new NullCheckFunc(position, varFunc);
        }

        return new ConstCheckFunc(position, varFunc, constValue);
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
            return arg.getStrLen(rec) == -1L;
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
            return Chars.equalsNc(constant, arg.getStr(rec));
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
            final CharSequence a = left.getStr(rec);
            final CharSequence b = right.getStrB(rec);

            if (a == null) {
                return b == null;
            }

            return b != null && Chars.equals(a, b);
        }
    }
}
