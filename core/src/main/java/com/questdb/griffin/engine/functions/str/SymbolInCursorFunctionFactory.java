/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.functions.str;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.sql.*;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.SqlExecutionContext;
import com.questdb.griffin.engine.StrTypeCaster;
import com.questdb.griffin.engine.SymbolTypeCaster;
import com.questdb.griffin.engine.TypeCaster;
import com.questdb.griffin.engine.functions.BinaryFunction;
import com.questdb.griffin.engine.functions.BooleanFunction;
import com.questdb.griffin.engine.functions.columns.SymbolColumn;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.Chars;
import com.questdb.std.IntHashSet;
import com.questdb.std.ObjList;

public class SymbolInCursorFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "in(KC)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        Function symbolFunction = args.getQuick(0);
        Function cursorFunction = args.getQuick(1);

        // use first column to create list of values (over multiple records)
        // supported column types are STRING and SYMBOL

        final int zeroColumnType = cursorFunction.getRecordCursorFactory().getMetadata().getColumnType(0);
        final TypeCaster typeCaster;
        switch (zeroColumnType) {
            case ColumnType.STRING:
                typeCaster = StrTypeCaster.INSTANCE;
                break;
            case ColumnType.SYMBOL:
                typeCaster = SymbolTypeCaster.INSTANCE;
                break;
            default:
                throw SqlException.position(position).put("supported column types are STRING and SYMBOL, found: ").put(ColumnType.nameOf(zeroColumnType));
        }

        if (symbolFunction instanceof SymbolColumn) {
            return new SymbolInCursorFunction(position, (SymbolColumn) symbolFunction, cursorFunction, typeCaster);
        }
        return new StrInCursorFunction(position, symbolFunction, cursorFunction, typeCaster);
    }

    private static class SymbolInCursorFunction extends BooleanFunction implements BinaryFunction {

        private final SymbolColumn valueArg;
        private final Function cursorArg;
        private final IntHashSet symbolKeys = new IntHashSet();
        private final int columnIndex;
        private final TypeCaster typeCaster;

        public SymbolInCursorFunction(int position, SymbolColumn valueArg, Function cursorArg, TypeCaster typeCaster) {
            super(position);
            this.valueArg = valueArg;
            this.cursorArg = cursorArg;
            this.columnIndex = valueArg.getColumnIndex();
            this.typeCaster = typeCaster;
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
        public void init(RecordCursor recordCursor, SqlExecutionContext executionContext) {
            valueArg.init(recordCursor, executionContext);
            cursorArg.init(recordCursor, executionContext);
            symbolKeys.clear();

            SymbolTable symbolTable = recordCursor.getSymbolTable(columnIndex);

            RecordCursorFactory factory = cursorArg.getRecordCursorFactory();
            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    int key = symbolTable.getQuick(typeCaster.getValue(record, 0));
                    if (key != SymbolTable.VALUE_NOT_FOUND) {
                        symbolKeys.add(key + 1);
                    }
                }
            }
        }


    }

    private static class StrInCursorFunction extends BooleanFunction implements BinaryFunction {

        private final Function valueArg;
        private final Function cursorArg;
        private final CharSequenceHashSet valueSetA = new CharSequenceHashSet();
        private final CharSequenceHashSet valueSetB = new CharSequenceHashSet();
        private final TypeCaster typeCaster;
        private CharSequenceHashSet valueSet;

        public StrInCursorFunction(int position, Function valueArg, Function cursorArg, TypeCaster typeCaster) {
            super(position);
            this.valueArg = valueArg;
            this.cursorArg = cursorArg;
            this.typeCaster = typeCaster;
            this.valueSet = valueSetA;
        }

        @Override
        public boolean getBool(Record rec) {
            return valueSet.contains(valueArg.getSymbol(rec));
        }

        @Override
        public void init(RecordCursor recordCursor, SqlExecutionContext executionContext) {
            valueArg.init(recordCursor, executionContext);
            cursorArg.init(recordCursor, executionContext);

            CharSequenceHashSet valueSet;
            if (this.valueSet == this.valueSetA) {
                valueSet = this.valueSetB;
            } else {
                valueSet = this.valueSetA;
            }

            valueSet.clear();


            RecordCursorFactory factory = cursorArg.getRecordCursorFactory();
            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    CharSequence value = typeCaster.getValue(record, 0);
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
            this.valueSet = valueSet;
        }


        @Override
        public Function getLeft() {
            return valueArg;
        }

        @Override
        public Function getRight() {
            return cursorArg;
        }
    }
}
