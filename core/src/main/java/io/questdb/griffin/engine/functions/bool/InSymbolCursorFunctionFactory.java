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
import io.questdb.std.Transient;
import io.questdb.std.str.StringSink;

public class InSymbolCursorFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "in(KC)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
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
        final Record.CharSequenceFunction func = switch (zeroColumnType) {
            case ColumnType.STRING -> Record.GET_STR;
            case ColumnType.SYMBOL -> Record.GET_SYM;
            case ColumnType.VARCHAR -> Record.GET_VARCHAR;
            default ->
                    throw SqlException.position(position).put("supported column types are VARCHAR, SYMBOL and STRING, found: ").put(ColumnType.nameOf(zeroColumnType));
        };

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
        private final StringSink sink = new StringSink();
        private final Function valueArg;
        private final CharSequenceHashSet valueSet = new CharSequenceHashSet();
        private boolean stateInherited = false;
        private boolean stateShared = false;

        public StrInCursorFunc(Function valueArg, Function cursorArg, Record.CharSequenceFunction func) {
            this.valueArg = valueArg;
            this.cursorArg = cursorArg;
            this.func = func;
        }

        @Override
        public boolean getBool(Record rec) {
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
            valueArg.init(symbolTableSource, executionContext);
            cursorArg.init(symbolTableSource, executionContext);

            if (stateInherited) {
                return;
            }

            stateShared = false;
            valueSet.clear();

            RecordCursorFactory factory = cursorArg.getRecordCursorFactory();
            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                final Record record = cursor.getRecord();
                sink.clear();
                while (cursor.hasNext()) {
                    CharSequence value = func.get(record, 0, sink);
                    if (value == null) {
                        this.valueSet.addNull();
                    } else {
                        int toIndex = this.valueSet.keyIndex(value);
                        if (toIndex > -1) {
                            this.valueSet.addAt(toIndex, Chars.toString(value));
                        }
                    }
                }
            }
        }

        @Override
        public boolean isThreadSafe() {
            return valueArg.isThreadSafe();
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof StrInCursorFunc thatF) {
                thatF.valueSet.clear();
                thatF.valueSet.addAll(valueSet);
                thatF.stateInherited = this.stateShared = true;
            }
            BinaryFunction.super.offerStateTo(that);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(valueArg).val(" in ").val(cursorArg);
            if (stateShared) {
                sink.val(" [state-shared]");
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
        private boolean stateInherited = false;
        private boolean stateShared = false;

        public SymbolInCursorFunc(SymbolFunction valueArg, Function cursorArg, Record.CharSequenceFunction func) {
            this.valueArg = valueArg;
            this.cursorArg = cursorArg;
            this.func = func;
        }

        @Override
        public boolean getBool(Record rec) {
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
            valueArg.init(symbolTableSource, executionContext);
            cursorArg.init(symbolTableSource, executionContext);
            if (stateInherited) {
                return;
            }
            stateShared = false;
            symbolKeys.clear();
            RecordCursorFactory factory = cursorArg.getRecordCursorFactory();
            try (RecordCursor cursor = factory.getCursor(executionContext)) {
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

        @Override
        public boolean isThreadSafe() {
            return valueArg.isThreadSafe();
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof SymbolInCursorFunc thatF) {
                thatF.symbolKeys.clear();
                thatF.symbolKeys.addAll(symbolKeys);
                thatF.stateInherited = this.stateShared = true;
            }
            BinaryFunction.super.offerStateTo(that);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(valueArg).val(" in ").val(cursorArg);
            if (stateShared) {
                sink.val(" [state-shared]");
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
        public boolean isThreadSafe() {
            return valueArg.isThreadSafe();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(valueArg).val(" in null");
        }
    }
}
