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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Utf8SequenceIntHashMap;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory for casting VARCHAR to SYMBOL type
 */
public class CastVarcharToSymbolFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(Øk)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        Function inputFunction = args.getQuick(0);
        if (inputFunction.isConstant()) {
            return createConstantSymbol(inputFunction);
        }
        return new VarcharToSymbolFunction(inputFunction);
    }

    private Function createConstantSymbol(Function inputFunction) {
        Utf8Sequence value = inputFunction.getVarcharA(null);
        if (value == null) {
            return SymbolConstant.NULL;
        }
        StringSink sink = Misc.getThreadLocalSink();
        sink.put(value);
        return SymbolConstant.newInstance(sink);
    }

    /**
     * Function to convert VARCHAR to SYMBOL
     */
    private static class VarcharToSymbolFunction extends SymbolFunction implements UnaryFunction {
        private final Function inputFunction;
        private final SymbolTableManager symbolTableManager;

        public VarcharToSymbolFunction(Function inputFunction) {
            this.inputFunction = inputFunction;
            this.symbolTableManager = new SymbolTableManager();
        }

        @Override
        public Function getArg() {
            return inputFunction;
        }

        @Override
        public int getInt(Record rec) {
            Utf8Sequence value = inputFunction.getVarcharA(rec);
            return value == null ? SymbolTable.VALUE_IS_NULL : symbolTableManager.getSymbolIndex(value);
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            Utf8Sequence value = inputFunction.getVarcharA(rec);
            return value == null ? null : symbolTableManager.lookupSymbol(value);
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            Utf8Sequence value = inputFunction.getVarcharB(rec);
            return value == null ? null : symbolTableManager.lookupSymbol(value);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            inputFunction.init(symbolTableSource, executionContext);
            symbolTableManager.reset();
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public boolean isSymbolTableStatic() {
            return false;
        }

        @Override
        public @Nullable SymbolTable newSymbolTable() {
            VarcharToSymbolFunction copy = new VarcharToSymbolFunction(inputFunction);
            copy.symbolTableManager.copyFrom(this.symbolTableManager);
            return copy;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(inputFunction).val("::symbol");
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int symbolKey) {
            return symbolTableManager.getSymbolValue(TableUtils.toIndexKey(symbolKey));
        }
    }

    /**
     * Manages symbol table operations
     */
    private static class SymbolTableManager {
        private final Map<Utf8Sequence, Integer> symbolIndexMap;
        private final ObjList<CharSequence> symbolValues;
        private int nextSymbolId;

        public SymbolTableManager() {
            symbolIndexMap = new HashMap<>();
            symbolValues = new ObjList<>();
            symbolValues.add(null); // Reserve index 0 for null
            nextSymbolId = 1;
        }

        public void reset() {
            symbolIndexMap.clear();
            symbolValues.clear();
            symbolValues.add(null);
            nextSymbolId = 1;
        }

        public int getSymbolIndex(Utf8Sequence value) {
            return symbolIndexMap.computeIfAbsent(value, k -> {
                String symbol = Utf8s.toString(value);
                symbolValues.add(symbol);
                return nextSymbolId++;
            }) - 1;
        }

        public CharSequence lookupSymbol(Utf8Sequence value) {
            Integer index = symbolIndexMap.get(value);
            if (index == null) {
                String symbol = Utf8s.toString(value);
                index = nextSymbolId++;
                symbolIndexMap.put(value, index);
                symbolValues.add(symbol);
                return symbol;
            }
            return symbolValues.getQuick(index);
        }

        public CharSequence getSymbolValue(int index) {
            return symbolValues.getQuick(index);
        }

        public void copyFrom(SymbolTableManager other) {
            symbolIndexMap.putAll(other.symbolIndexMap);
            symbolValues.clear();
            symbolValues.addAll(other.symbolValues);
            nextSymbolId = other.nextSymbolId;
        }
    }
}