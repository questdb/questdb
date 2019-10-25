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
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.ObjList;

public class EqStrCharFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(SA)";
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
        return new ConstCheckFunc(position, varFunc, constFunc.getChar(null));
    }

    private static class ConstCheckFunc extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private final char constant;

        public ConstCheckFunc(int position, Function arg, char constant) {
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
            CharSequence value = arg.getStr(rec);
            if (value == null) {
                return false;
            }

            final int len = value.length();
            if (len == 0) {
                return false;
            }
            return value.charAt(0) == constant;
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
            final char b = right.getChar(rec);

            if (a == null) {
                return false;
            }

            int len = a.length();
            if (len == 0) {
                return false;
            }

            return a.charAt(0) == b;
        }
    }
}
