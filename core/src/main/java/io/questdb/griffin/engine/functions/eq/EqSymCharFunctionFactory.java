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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.str.SingleCharCharSequence;

public class EqSymCharFunctionFactory extends FunctionFactory {
    @Override
    public String getSignature() {
        return "=(KA)";
    }

    @Override
    public Function newInstance(
            ObjList<Function> args,
            int position, CairoConfiguration configuration
    ) {
        // there are optimisation opportunities
        // 1. when one of args is constant null comparison can boil down to checking
        //    length of non-constant (must be -1)
        // 2. when one of arguments is constant, save method call and use a field

        SymbolFunction symFunc = (SymbolFunction) args.getQuick(0);
        Function chrFunc = args.getQuick(1);

        if (chrFunc.isConstant()) {
            final char constValue = chrFunc.getChar(null);
            if (symFunc.getStaticSymbolTable() != null) {
                return new ConstCheckColumnFunc(position, symFunc, constValue, isNegated);
            } else {
                return new ConstCheckFunc(position, symFunc, constValue, isNegated);
            }
        }

        return new Func(position, symFunc, chrFunc, isNegated);
    }

    @Override
    public boolean isNegatable() { return true; }

    private class ConstCheckFunc extends BooleanFunction implements UnaryFunction {
        private final boolean isNegated;
        private final Function arg;
        private final char constant;

        public ConstCheckFunc(int position, Function arg, char constant, boolean isNegated) {
            super(position);
            this.arg = arg;
            this.constant = constant;
            this.isNegated = isNegated;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return isNegated != Chars.equalsNc(arg.getSymbol(rec), constant);
        }
    }

    private class ConstCheckColumnFunc extends BooleanFunction implements UnaryFunction {
        private final boolean isNegated;
        private final SymbolFunction arg;
        private final char constant;
        private int valueIndex;

        public ConstCheckColumnFunc(int position, SymbolFunction arg, char constant, boolean isNegated) {
            super(position);
            this.arg = arg;
            this.constant = constant;
            this.isNegated = isNegated;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return isNegated != (arg.getInt(rec) == valueIndex);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            arg.init(symbolTableSource, executionContext);
            final StaticSymbolTable symbolTable = arg.getStaticSymbolTable();
            assert symbolTable != null;
            valueIndex = symbolTable.keyOf(SingleCharCharSequence.get(constant));
        }
    }

    private class Func extends BooleanFunction implements BinaryFunction {
        private final boolean isNegated;
        private final Function symFunc;
        private final Function chrFunc;

        public Func(int position, Function symFunc, Function chrFunc, boolean isNegated) {
            super(position);
            this.symFunc = symFunc;
            this.chrFunc = chrFunc;
            this.isNegated = isNegated;
        }

        @Override
        public Function getLeft() {
            return symFunc;
        }

        @Override
        public Function getRight() {
            return chrFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            return isNegated != Chars.equalsNc(symFunc.getSymbol(rec), chrFunc.getChar(rec));
        }
    }
}
