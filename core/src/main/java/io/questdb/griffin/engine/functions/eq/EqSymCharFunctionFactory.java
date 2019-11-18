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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.str.SingleCharCharSequence;

public class EqSymCharFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(KA)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        // there are optimisation opportunities
        // 1. when one of args is constant null comparison can boil down to checking
        //    length of non-constant (must be -1)
        // 2. when one of arguments is constant, save method call and use a field

        Function symFunc = args.getQuick(0);
        Function chrFunc = args.getQuick(1);

        if (chrFunc.isConstant()) {
            final char constValue = chrFunc.getChar(null);
            if (symFunc instanceof SymbolColumn) {
                return new ConstCheckColumnFunc(position, (SymbolColumn) symFunc, constValue);
            } else {
                return new ConstCheckFunc(position, symFunc, constValue);
            }
        }

        return new Func(position, symFunc, chrFunc);
    }

    private static class ConstCheckFunc extends BooleanFunction implements UnaryFunction {
        private final Function symFunc;
        private final char constant;

        public ConstCheckFunc(int position, Function symFunc, char constant) {
            super(position);
            this.symFunc = symFunc;
            this.constant = constant;
        }

        @Override
        public Function getArg() {
            return symFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            return Chars.equalsNc(symFunc.getSymbol(rec), constant);
        }
    }

    private static class ConstCheckColumnFunc extends BooleanFunction implements UnaryFunction {
        private final SymbolColumn arg;
        private final char constant;
        private int valueIndex;

        public ConstCheckColumnFunc(int position, SymbolColumn arg, char constant) {
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
            return arg.getInt(rec) == valueIndex;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            valueIndex = symbolTableSource.getSymbolTable(arg.getColumnIndex()).getQuick(SingleCharCharSequence.get(constant));
        }
    }

    private static class Func extends BooleanFunction implements BinaryFunction {

        private final Function symFunc;
        private final Function chrFunc;

        public Func(int position, Function symFunc, Function chrFunc) {
            super(position);
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
            return Chars.equalsNc(symFunc.getSymbol(rec), chrFunc.getChar(rec));
        }
    }
}
