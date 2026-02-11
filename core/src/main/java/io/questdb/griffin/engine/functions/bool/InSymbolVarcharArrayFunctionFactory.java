/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.sql.SymbolTable;
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
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;

/**
 * Handles "symbol IN $1" where $1 is a VARCHAR[] bind variable.
 * <p>
 * VARCHAR[] is currently only supported as a bind variable type in PGWire protocol,
 * passed via the BIND message. There is no SQL literal syntax for VARCHAR arrays.
 */
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
        if (symbolFunc.isSymbolTableStatic()) {
            return new IntSetFunc(symbolFunc, arrayFunc);
        }
        return new StrSetFunc(symbolFunc, arrayFunc);
    }

    /**
     * Used when the symbol column has a static symbol table.
     * Converts array values to symbol keys at init() time for O(1) int comparison at runtime.
     */
    private static class IntSetFunc extends BooleanFunction implements BinaryFunction {
        private final Function arrayFunc;
        private final IntHashSet intSet = new IntHashSet();
        private final SymbolFunction symbolFunc;
        private boolean stateInherited = false;

        public IntSetFunc(SymbolFunction symbolFunc, Function arrayFunc) {
            this.symbolFunc = symbolFunc;
            this.arrayFunc = arrayFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            return intSet.contains(symbolFunc.getInt(rec));
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
            if (stateInherited) {
                return;
            }

            intSet.clear();
            ArrayView arrayView = arrayFunc.getArray(null);
            StaticSymbolTable symbolTable = symbolFunc.getStaticSymbolTable();
            assert symbolTable != null;

            for (int i = 0, n = arrayView.getCardinality(); i < n; i++) {
                Utf8Sequence value = arrayView.getVarchar(i);
                if (value == null) {
                    intSet.add(symbolTable.keyOf(null));
                } else if (value.isAscii()) {
                    int key = symbolTable.keyOf(value.asAsciiCharSequence());
                    if (key != SymbolTable.VALUE_NOT_FOUND) {
                        intSet.add(key);
                    }
                } else {
                    StringSink sink = Misc.getThreadLocalSink();
                    sink.put(value);
                    int key = symbolTable.keyOf(sink);
                    if (key != SymbolTable.VALUE_NOT_FOUND) {
                        intSet.add(key);
                    }
                }
            }
        }

        @Override
        public void offerStateTo(Function that) {
            BinaryFunction.super.offerStateTo(that);
            if (that instanceof IntSetFunc other) {
                other.intSet.clear();
                other.intSet.addAll(intSet);
                other.stateInherited = true;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(symbolFunc).val(" in ").val(arrayFunc);
        }
    }

    /**
     * Used when the symbol column does not have a static symbol table.
     * Stores array values as strings and compares symbol values as strings at runtime.
     */
    private static class StrSetFunc extends BooleanFunction implements BinaryFunction {
        private final Function arrayFunc;
        private final CharSequenceHashSet strSet = new CharSequenceHashSet();
        private final SymbolFunction symbolFunc;
        private boolean stateInherited = false;

        public StrSetFunc(SymbolFunction symbolFunc, Function arrayFunc) {
            this.symbolFunc = symbolFunc;
            this.arrayFunc = arrayFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            return strSet.contains(symbolFunc.getSymbol(rec));
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
            if (stateInherited) {
                return;
            }

            strSet.clear();
            ArrayView arrayView = arrayFunc.getArray(null);
            for (int i = 0, n = arrayView.getCardinality(); i < n; i++) {
                strSet.add(arrayView.getVarchar(i));
            }
        }

        @Override
        public void offerStateTo(Function that) {
            BinaryFunction.super.offerStateTo(that);
            if (that instanceof StrSetFunc other) {
                other.strSet.clear();
                other.strSet.addAll(strSet);
                other.stateInherited = true;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(symbolFunc).val(" in ").val(arrayFunc);
        }
    }
}
