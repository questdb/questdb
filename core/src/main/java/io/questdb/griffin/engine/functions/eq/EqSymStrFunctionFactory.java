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
import io.questdb.cairo.sql.*;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class EqSymStrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(KS)";
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

        Function symFunc = args.getQuick(0);
        Function strFunc = args.getQuick(1);

        // SYMBOL cannot be constant
        if (strFunc.isConstant()) {
            return createHalfConstantFunc(strFunc, symFunc);
        }
        return new Func(symFunc, strFunc);
    }

    private Function createHalfConstantFunc(Function constFunc, Function varFunc) {
        CharSequence constValue = constFunc.getStr(null);
        SymbolFunction func = (SymbolFunction) varFunc;
        if (func.getStaticSymbolTable() != null) {
            return new ConstCheckColumnFunc(func, constValue);
        } else {
            if (constValue == null) {
                return new NullCheckFunc(varFunc);
            }
            if (func.isSymbolTableStatic()) {
                return new ConstSymIntCheckFunc(func, constValue);
            }
            return new ConstCheckFunc(func, constValue);
        }
    }

    private static class NullCheckFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;

        public NullCheckFunc(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (arg.getSymbol(rec) == null);
        }
    }

    private static class ConstCheckFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;
        private final CharSequence constant;

        public ConstCheckFunc(Function arg, CharSequence constant) {
            this.arg = arg;
            this.constant = constant;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != Chars.equalsNc(constant, arg.getSymbol(rec));
        }
    }

    private static class ConstSymIntCheckFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final SymbolFunction arg;
        private final CharSequence constant;
        private int valueIndex;
        private boolean exists;

        public ConstSymIntCheckFunc(SymbolFunction arg, CharSequence constant) {
            this.arg = arg;
            this.constant = constant;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (exists && arg.getInt(rec) == valueIndex);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            StaticSymbolTable staticSymbolTable = arg.getStaticSymbolTable();
            assert staticSymbolTable != null : "Static symbol table is null for func with static isSymbolTableStatic returning true";
            valueIndex = staticSymbolTable.keyOf(constant);
            exists = valueIndex != SymbolTable.VALUE_NOT_FOUND;
        }
    }

    private static class ConstCheckColumnFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final SymbolFunction arg;
        private final CharSequence constant;
        private int valueIndex;

        public ConstCheckColumnFunc(SymbolFunction arg, CharSequence constant) {
            this.arg = arg;
            this.constant = constant;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (arg.getInt(rec) == valueIndex);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            final StaticSymbolTable symbolTable = arg.getStaticSymbolTable();
            assert symbolTable != null;
            valueIndex = symbolTable.keyOf(constant);
        }

        @Override
        public boolean isConstant() {
            return valueIndex == SymbolTable.VALUE_NOT_FOUND;
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
                return negated != (b == null);
            }

            return negated != Chars.equalsNc(a, b);
        }
    }
}
