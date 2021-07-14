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
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class EqStrCharFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(SA)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        // there are optimisation opportunities
        // 1. when one of args is constant null comparison can boil down to checking
        //    length of non-constant (must be -1)
        // 2. when one of arguments is constant, save method call and use a field

        Function strFunc = args.getQuick(0);
        Function charFunc = args.getQuick(1);

        if (strFunc.getType() == ColumnType.NULL || charFunc.getType() == ColumnType.NULL) {
            return new Func(strFunc, charFunc);
        }

        if (strFunc.isConstant() && !charFunc.isConstant()) {
            CharSequence str = strFunc.getStr(null);
            if (str == null || str.length() != 1) {
                return new NegatedAwareBooleanConstantFunc();
            }
            return new ConstStrFunc(charFunc, str.charAt(0));
        }

        if (!strFunc.isConstant() && charFunc.isConstant()) {
            return new ConstChrFunc(strFunc, charFunc.getChar(null));
        }

        if (strFunc.isConstant() && charFunc.isConstant()) {
            return new ConstStrConstChrFunc(Chars.equalsNc(strFunc.getStr(null), charFunc.getChar(null)));
        }

        return new Func(strFunc, charFunc);
    }

    private static class ConstChrFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final Function strFunc;
        private final char chrConst;

        public ConstChrFunc(Function strFunc, char chrConst) {
            this.strFunc = strFunc;
            this.chrConst = chrConst;
        }

        @Override
        public Function getArg() {
            return strFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != Chars.equalsNc(strFunc.getStr(rec), chrConst);
        }
    }

    private static class ConstStrFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final Function chrFunc;
        private final char chrConst;

        public ConstStrFunc(Function chrFunc, char chrConst) {
            this.chrFunc = chrFunc;
            this.chrConst = chrConst;
        }

        @Override
        public Function getArg() {
            return chrFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (chrFunc.getChar(rec) == chrConst);
        }
    }

    private static class ConstStrConstChrFunc extends NegatableBooleanFunction implements ConstantFunction {
        private final boolean equals;

        public ConstStrConstChrFunc(boolean equals) {
            this.equals = equals;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != equals;
        }
    }

    private static class NegatedAwareBooleanConstantFunc extends NegatableBooleanFunction implements ConstantFunction {

        @Override
        public boolean getBool(Record rec) {
            return negated;
        }
    }

    private static class Func extends NegatableBooleanFunction implements BinaryFunction {
        private final Function strFunc;
        private final Function chrFunc;

        public Func(Function strFunc, Function chrFunc) {
            this.strFunc = strFunc;
            this.chrFunc = chrFunc;
        }

        @Override
        public Function getLeft() {
            return strFunc;
        }

        @Override
        public Function getRight() {
            return chrFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (Chars.equalsNc(strFunc.getStr(rec), chrFunc.getChar(rec)));
        }
    }
}
