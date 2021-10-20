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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.SingleCharCharSequence;

public class EqSymCharFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(KA)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(
            int position, ObjList<Function> args,
            IntList argPositions, CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext) {
        // there are optimisation opportunities
        // 1. when one of args is constant null comparison can boil down to checking
        //    length of non-constant (must be -1)
        // 2. when one of arguments is constant, save method call and use a field

        SymbolFunction symFunc = (SymbolFunction) args.getQuick(0);
        Function chrFunc = args.getQuick(1);

        if (chrFunc.isConstant()) {
            final char constValue = chrFunc.getChar(null);
            if (symFunc.getStaticSymbolTable() != null) {
                return new ConstCheckColumnFunc(symFunc, constValue);
            } else {
                return new ConstCheckFunc(symFunc, constValue);
            }
        }

        return new Func(symFunc, chrFunc);
    }

    private static class ConstCheckFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;
        private final char constant;

        public ConstCheckFunc(Function arg, char constant) {
            this.arg = arg;
            this.constant = constant;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != Chars.equalsNc(arg.getSymbol(rec), constant);
        }
    }

    private static class ConstCheckColumnFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final SymbolFunction arg;
        private final char constant;
        private int valueIndex;

        public ConstCheckColumnFunc(SymbolFunction arg, char constant) {
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
            valueIndex = symbolTable.keyOf(SingleCharCharSequence.get(constant));
        }
    }

    private static class Func extends NegatableBooleanFunction implements BinaryFunction {
        private final Function symFunc;
        private final Function chrFunc;

        public Func(Function symFunc, Function chrFunc) {
            this.symFunc = symFunc;
            this.chrFunc = chrFunc;
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
            return negated != Chars.equalsNc(symFunc.getSymbol(rec), chrFunc.getChar(rec));
        }
    }
}
