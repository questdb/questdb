/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.std.*;

public class InSymbolCursorFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "in(KC)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function valueFunction = args.getQuick(0);
        final Function cursorFunction = args.getQuick(1);

        final int zeroColumnType = !cursorFunction.isNullConstant()
                ? cursorFunction.getRecordCursorFactory().getMetadata().getColumnType(0)
                : ColumnType.NULL;
        if (ColumnType.isNull(zeroColumnType)) {
            if (valueFunction.isNullConstant()) {
                return BooleanConstant.TRUE;
            }
            final SymbolFunction symbolFunction = (SymbolFunction) args.getQuick(0);
            if (symbolFunction.isSymbolTableStatic()) {
                return new SymbolInNullCursorFunction(symbolFunction);
            }
            return new StrInNullCursorFunction(symbolFunction);
        } else if (!ColumnType.isSymbolOrString(zeroColumnType)) {
            throw SqlException.position(position).put("supported column types are STRING and SYMBOL, found: ").put(ColumnType.nameOf(zeroColumnType));
        }

        // use first column to create list of values (over multiple records)
        // supported column types are STRING and SYMBOL

        final Record.CharSequenceFunction func = ColumnType.isString(zeroColumnType) ? Record.GET_STR : Record.GET_SYM;

        if (valueFunction.isNullConstant()) {
            return new StrInCursorFunction(NullConstant.NULL, cursorFunction, func);
        }

        final SymbolFunction symbolFunction = (SymbolFunction) args.getQuick(0);
        if (symbolFunction.isSymbolTableStatic()) {
            return new SymbolInCursorFunction(symbolFunction, cursorFunction, func);
        }
        return new StrInCursorFunction(symbolFunction, cursorFunction, func);
    }

    private static class StrInCursorFunction extends BooleanFunction implements BinaryFunction {

        private final Function cursorArg;
        private final Record.CharSequenceFunction func;
        private final Function valueArg;
        private final CharSequenceHashSet valueSetA = new CharSequenceHashSet();
        private final CharSequenceHashSet valueSetB = new CharSequenceHashSet();
        private RecordCursor cursor;
        private CharSequenceHashSet valueSet;

        public StrInCursorFunction(Function valueArg, Function cursorArg, Record.CharSequenceFunction func) {
            this.valueArg = valueArg;
            this.cursorArg = cursorArg;
            this.valueSet = valueSetA;
            this.func = func;
        }

        @Override
        public void close() {
            cursor = Misc.free(cursor);
            BinaryFunction.super.close();
        }

        @Override
        public boolean getBool(Record rec) {
            initCursor();
            return valueSet.contains(valueArg.getSymbol(rec));
        }

        @Override
        public Function getLeft() {
            return valueArg;
        }

        @Override
        public Function getRight() {
            return cursorArg;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            if (cursor != null) {
                cursor = Misc.free(cursor);
            }
            valueArg.init(symbolTableSource, executionContext);
            cursorArg.init(symbolTableSource, executionContext);

            CharSequenceHashSet valueSet;
            if (this.valueSet == this.valueSetA) {
                valueSet = this.valueSetB;
            } else {
                valueSet = this.valueSetA;
            }

            valueSet.clear();
            this.valueSet = valueSet;

            RecordCursorFactory factory = cursorArg.getRecordCursorFactory();
            cursor = factory.getCursor(executionContext);
        }

        @Override
        public void initCursor() {
            if (cursor != null) {
                BinaryFunction.super.initCursor();
                buildValueSet();
                cursor = Misc.free(cursor);
            }
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(valueArg).val(" in ").val(cursorArg);
        }

        private void buildValueSet() {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                CharSequence value = func.get(record, 0);
                if (value == null) {
                    valueSet.addNull();
                } else {
                    int toIndex = valueSet.keyIndex(value);
                    if (toIndex > -1) {
                        int index = this.valueSet.keyIndex(value);
                        if (index < 0) {
                            valueSet.addAt(toIndex, this.valueSet.keyAt(index));
                        } else {
                            valueSet.addAt(toIndex, Chars.toString(value));
                        }
                    }
                }
            }
        }
    }

    private static class StrInNullCursorFunction extends BooleanFunction implements UnaryFunction {

        private final Function valueArg;

        public StrInNullCursorFunction(Function valueArg) {
            this.valueArg = valueArg;
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
        }

        @Override
        public Function getArg() {
            return valueArg;
        }

        @Override
        public boolean getBool(Record rec) {
            return valueArg.getSymbol(rec) == null;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            valueArg.init(symbolTableSource, executionContext);
        }

        @Override
        public boolean isReadThreadSafe() {
            return valueArg.isReadThreadSafe();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(valueArg).val(" in null");
        }
    }

    private static class SymbolInCursorFunction extends BooleanFunction implements BinaryFunction {

        private final Function cursorArg;
        private final Record.CharSequenceFunction func;
        private final IntHashSet symbolKeys = new IntHashSet();
        private final SymbolFunction valueArg;
        private RecordCursor cursor;

        public SymbolInCursorFunction(SymbolFunction valueArg, Function cursorArg, Record.CharSequenceFunction func) {
            this.valueArg = valueArg;
            this.cursorArg = cursorArg;
            this.func = func;
        }

        @Override
        public void close() {
            cursor = Misc.free(cursor);
            BinaryFunction.super.close();
        }

        @Override
        public boolean getBool(Record rec) {
            initCursor();
            return symbolKeys.keyIndex(valueArg.getInt(rec) + 1) < 0;
        }

        @Override
        public Function getLeft() {
            return valueArg;
        }

        @Override
        public Function getRight() {
            return cursorArg;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            if (cursor != null) {
                cursor = Misc.free(cursor);
            }
            valueArg.init(symbolTableSource, executionContext);
            cursorArg.init(symbolTableSource, executionContext);

            symbolKeys.clear();

            RecordCursorFactory factory = cursorArg.getRecordCursorFactory();
            cursor = factory.getCursor(executionContext);
        }

        @Override
        public void initCursor() {
            if (cursor != null) {
                BinaryFunction.super.initCursor();
                buildSymbolKeys();
                cursor = Misc.free(cursor);
            }
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(valueArg).val(" in ").val(cursorArg);
        }

        private void buildSymbolKeys() {
            final StaticSymbolTable symbolTable = valueArg.getStaticSymbolTable();
            assert symbolTable != null;

            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                int key = symbolTable.keyOf(func.get(record, 0));
                if (key != SymbolTable.VALUE_NOT_FOUND) {
                    symbolKeys.add(key + 1);
                }
            }
        }
    }

    private static class SymbolInNullCursorFunction extends BooleanFunction implements UnaryFunction {

        private final Function valueArg;

        public SymbolInNullCursorFunction(Function valueArg) {
            this.valueArg = valueArg;
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
        }

        @Override
        public Function getArg() {
            return valueArg;
        }

        @Override
        public boolean getBool(Record rec) {
            return valueArg.getInt(rec) == SymbolTable.VALUE_IS_NULL;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            valueArg.init(symbolTableSource, executionContext);
        }

        @Override
        public boolean isReadThreadSafe() {
            return valueArg.isReadThreadSafe();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(valueArg).val(" in null");
        }
    }
}
