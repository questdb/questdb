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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class CursorDereferenceFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return ".(Cs)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        Function cursorFunction = args.getQuick(0);
        Function columnNameFunction = args.getQuick(1);
        RecordCursorFactory factory = cursorFunction.getRecordCursorFactory();
        // name is always constant
        int columnIndex = factory.getMetadata().getColumnIndexQuiet(columnNameFunction.getStr(null));
        if (columnIndex == -1) {
            throw SqlException.invalidColumn(columnNameFunction.getPosition(), columnNameFunction.getStr(null));
        }

        int columnType = factory.getMetadata().getColumnType(columnIndex);

        if (columnType == ColumnType.INT) {
            return new IntColumnFunction(
                    position,
                    cursorFunction,
                    columnNameFunction,
                    factory,
                    columnIndex
            );
        }

        throw SqlException.$(columnNameFunction.getPosition(), "unsupported column type: ").put(ColumnType.nameOf(columnType));
    }

    private static class IntColumnFunction extends IntFunction implements BinaryFunction {
        private final Function cursorFunction;
        private final Function columnNameFunction;
        private final RecordCursorFactory factory;
        private final int columnIndex;
        private RecordCursor cursor;
        private Record record;

        public IntColumnFunction(
                int position,
                Function cursorFunction,
                Function columnNameFunction,
                RecordCursorFactory factory,
                int columnIndex
        ) {
            super(position);
            this.cursorFunction = cursorFunction;
            this.columnNameFunction = columnNameFunction;
            this.factory = factory;
            this.columnIndex = columnIndex;
        }

        @Override
        public Function getLeft() {
            return cursorFunction;
        }

        @Override
        public Function getRight() {
            return columnNameFunction;
        }

        @Override
        public int getInt(Record rec) {
            if (cursor.hasNext()) {
                return record.getInt(columnIndex);
            }
            return Numbers.INT_NaN;
        }

        @Override
        public void toTop() {
            cursor.toTop();
        }

        @Override
        public boolean supportsRandomAccess() {
            return false;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            super.init(symbolTableSource, executionContext);
            cursor = factory.getCursor(executionContext);
            record = cursor.getRecord();
        }

        @Override
        public void close() {
            Misc.free(factory);
        }
    }
}
