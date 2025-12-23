/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.functions.bool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

public class InSymbolVarcharArrayFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "in(KØ[])";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        SymbolFunction symbolFunc = (SymbolFunction) args.getQuick(0);
        Function arrayFunc = args.getQuick(1);
        if (!arrayFunc.isConstant() && !arrayFunc.isRuntimeConstant()) {
            throw SqlException.$(argPositions.getQuick(1), "constant or bind variable expected");
        }
        return new Func(symbolFunc, arrayFunc);
    }

    private static class Func extends BooleanFunction implements BinaryFunction {
        private final Function arrayFunc;
        private final IntHashSet intSet = new IntHashSet();
        private final StringSink sink = new StringSink();
        private final CharSequenceHashSet strSet = new CharSequenceHashSet();
        private final SymbolFunction symbolFunc;
        private final InSymbolFunctionFactory.TestFunc intTest = this::testAsInt;
        private final InSymbolFunctionFactory.TestFunc strTest = this::testAsStr;
        private InSymbolFunctionFactory.TestFunc testFunc;

        public Func(SymbolFunction symbolFunc, Function arrayFunc) {
            this.symbolFunc = symbolFunc;
            this.arrayFunc = arrayFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            return testFunc.test(rec);
        }

        @Override
        public Function getLeft() {
            return symbolFunc;
        }

        @Override
        public Function getRight() {
            return arrayFunc;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);
            intSet.clear();
            strSet.clear();
            ArrayView arrayView = arrayFunc.getArray(null);
            StaticSymbolTable symbolTable = symbolFunc.getStaticSymbolTable();

            if (symbolTable != null) {
                for (int i = 0, n = arrayView.getCardinality(); i < n; i++) {
                    Utf8Sequence value = arrayView.getVarchar(i);
                    if (value == null) {
                        intSet.add(symbolTable.keyOf(null));
                    } else if (value.isAscii()) {
                        intSet.add(symbolTable.keyOf(value.asAsciiCharSequence()));
                    } else {
                        sink.clear();
                        sink.put(value);
                        intSet.add(symbolTable.keyOf(sink));
                    }
                }
                testFunc = intTest;
            } else {
                for (int i = 0, n = arrayView.getCardinality(); i < n; i++) {
                    Utf8Sequence element = arrayView.getVarchar(i);
                    strSet.add(Utf8s.toString(element));
                }
                testFunc = strTest;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(symbolFunc).val(" in ").val(arrayFunc);
        }

        private boolean testAsInt(Record rec) {
            int key = symbolFunc.getInt(rec);
            return intSet.contains(key);
        }

        private boolean testAsStr(Record rec) {
            CharSequence symbol = symbolFunc.getSymbol(rec);
            return strSet.contains(symbol);
        }
    }
}
