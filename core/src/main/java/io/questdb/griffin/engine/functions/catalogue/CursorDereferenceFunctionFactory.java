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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class CursorDereferenceFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return ".(Rs)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function cursorFunction = args.getQuick(0);
        Function columnNameFunction = args.getQuick(1);
        RecordMetadata metadata = cursorFunction.getMetadata();
        // name is always constant
        final CharSequence columnName = columnNameFunction.getStrA(null);
        final int columnIndex = metadata.getColumnIndexQuiet(columnName);
        if (columnIndex == -1) {
            throw SqlException.invalidColumn(argPositions.getQuick(1), columnName);
        }
        final int columnType = metadata.getColumnType(columnIndex);

        if (ColumnType.isInt(columnType)) {
            return new IntColumnFunction(
                    cursorFunction,
                    columnNameFunction,
                    columnIndex
            );
        }

        throw SqlException.$(argPositions.getQuick(1), "unsupported column type: ").put(ColumnType.nameOf(columnType));
    }

    private static class IntColumnFunction extends IntFunction implements BinaryFunction {
        private final int columnIndex;
        private final Function columnNameFunction;
        private final Function cursorFunction;

        public IntColumnFunction(
                Function cursorFunction,
                Function columnNameFunction,
                int columnIndex
        ) {
            super();
            this.cursorFunction = cursorFunction;
            this.columnNameFunction = columnNameFunction;
            this.columnIndex = columnIndex;
        }

        @Override
        public int getInt(Record rec) {
            return cursorFunction.extendedOps().getRecord(rec).getInt(columnIndex);
        }

        @Override
        public Function getLeft() {
            return cursorFunction;
        }

        @Override
        public String getName() {
            return ".";
        }

        @Override
        public Function getRight() {
            return columnNameFunction;
        }

        @Override
        public boolean isOperator() {
            return true;
        }
    }
}
