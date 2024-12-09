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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
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
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

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
                return new SymbolInNullCursorFunc(symbolFunction);
            }
            return new StrInNullCursorFunc(symbolFunction);
        }

        // use first column to create list of values (over multiple records)
        // supported column types are VARCHAR, STRING and SYMBOL
        final Record.CharSequenceFunction func;
        switch (zeroColumnType) {
            case ColumnType.STRING:
                func = Record.GET_STR;
                break;
            case ColumnType.SYMBOL:
                func = Record.GET_SYM;
                break;
            case ColumnType.VARCHAR:
                func = Record.GET_VARCHAR;
                break;
            default:
                throw SqlException.position(position).put("supported column types are VARCHAR, SYMBOL and STRING, found: ").put(ColumnType.nameOf(zeroColumnType));
        }

        if (valueFunction.isNullConstant()) {
            return new StrInCursorFunc(NullConstant.NULL, cursorFunction, func);
        }

        final SymbolFunction symbolFunction = (SymbolFunction) args.getQuick(0);
        if (symbolFunction.isSymbolTableStatic()) {
            return new SymbolInCursorFunc(symbolFunction, cursorFunction, func);
        }
        return new StrInCursorFunc(symbolFunction, cursorFunction, func);
    }

    private static class StrInCursorFunc extends BooleanFunction implements BinaryFunction {
        private final Function cursorArg;
        private final Record.CharSequenceFunction func;
        private final Function valueArg;
        private final CharSequenceHashSet valueSetA = new CharSequenceHashSet();
        private final CharSequenceHashSet valueSetB = new CharSequenceHashSet();
        private RecordCursor cursor;
        private CharSequenceHashSet valueSet;

        public StrInCursorFunc(Function valueArg, Function cursorArg, Record.CharSequenceFunction func) {
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
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(valueArg).val(" in ").val(cursorArg);
        }

        private void buildValueSet() {
            final Record record = cursor.getRecord();
            StringSink sink = Misc.getThreadLocalSink();
            while (cursor.hasNext()) {
                CharSequence value = func.get(record, 0, sink);
                if (value == null) {
                    valueSet.addNull();
                } else {
                    int toIndex = valueSet.keyIndex(value);
                    if (toIndex > -1) {
                        valueSet.addAt(toIndex, Chars.toString(value));
                    }
                }
            }
        }
    }

    private static class StrInNullCursorFunc extends BooleanFunction implements UnaryFunction {
        private final Function valueArg;

        public StrInNullCursorFunc(Function valueArg) {
            this.valueArg = valueArg;
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
        public void toPlan(PlanSink sink) {
            sink.val(valueArg).val(" in null");
        }
    }

    private static class SymbolInCursorFunc extends BooleanFunction implements BinaryFunction {
        private final Function cursorArg;
        private final Record.CharSequenceFunction func;
        private final IntHashSet symbolKeys = new IntHashSet();
        private final SymbolFunction valueArg;
        private RecordCursor cursor;

        public SymbolInCursorFunc(SymbolFunction valueArg, Function cursorArg, Record.CharSequenceFunction func) {
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
        public boolean isThreadSafe() {
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
            StringSink sink = Misc.getThreadLocalSink();
            while (cursor.hasNext()) {
                int key = symbolTable.keyOf(func.get(record, 0, sink));
                if (key != SymbolTable.VALUE_NOT_FOUND) {
                    symbolKeys.add(key + 1);
                }
            }
        }
    }

    private static class SymbolInNullCursorFunc extends BooleanFunction implements UnaryFunction {
        private final Function valueArg;

        public SymbolInNullCursorFunc(Function valueArg) {
            this.valueArg = valueArg;
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
        public void toPlan(PlanSink sink) {
            sink.val(valueArg).val(" in null");
        }
    }
}
