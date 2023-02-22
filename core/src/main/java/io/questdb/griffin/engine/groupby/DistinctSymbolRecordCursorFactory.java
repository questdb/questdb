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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

public class DistinctSymbolRecordCursorFactory extends AbstractRecordCursorFactory {
    private final DistinctSymbolRecordCursor cursor;
    private final TableToken tableToken;
    private final long tableVersion;

    public DistinctSymbolRecordCursorFactory(
            final GenericRecordMetadata metadata,
            final TableToken tableToken,
            final int columnIndex,
            final long tableVersion
    ) {
        super(metadata);
        this.tableToken = tableToken;
        this.tableVersion = tableVersion;
        this.cursor = new DistinctSymbolRecordCursor(columnIndex);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        TableReader reader = executionContext.getReader(tableToken, tableVersion);
        cursor.of(reader);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("DistinctSymbol");
        sink.meta("tableName").val(tableToken);
        sink.attr("columnIndex").val(cursor.columnIndex);
    }

    @Override
    protected void _close() {
        Misc.free(cursor);
    }

    private static class DistinctSymbolRecordCursor implements RecordCursor {
        private final int columnIndex;
        private final DistinctSymbolRecord recordA = new DistinctSymbolRecord();
        private int numberOfSymbols;
        private TableReader reader;
        private DistinctSymbolRecord recordB = null;
        private SymbolMapReader symbolMapReader;

        public DistinctSymbolRecordCursor(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void close() {
            reader = Misc.free(reader);
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public Record getRecordB() {
            if (recordB == null) {
                recordB = new DistinctSymbolRecord();
            }
            recordB.reset();
            return recordB;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            // this is single column cursor
            return symbolMapReader;
        }

        @Override
        public boolean hasNext() {
            if (recordA.getAndIncrementRecordIndex() < numberOfSymbols) {
                return true;
            }
            recordA.decrementRecordIndex();
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            // this is single column cursor
            return reader.newSymbolTable(this.columnIndex);
        }

        public void of(TableReader reader) {
            this.reader = reader;
            this.symbolMapReader = reader.getSymbolMapReader(columnIndex);
            this.numberOfSymbols = symbolMapReader.getSymbolCount() + (symbolMapReader.containsNullValue() ? 1 : 0);
            this.recordA.reset();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            ((DistinctSymbolRecord) record).recordIndex = (int) atRowId;
        }

        @Override
        public long size() {
            return numberOfSymbols;
        }

        @Override
        public void toTop() {
            recordA.reset();
        }

        public class DistinctSymbolRecord implements Record {
            private int recordIndex = -1;

            public void decrementRecordIndex() {
                recordIndex--;
            }

            public long getAndIncrementRecordIndex() {
                return ++recordIndex;
            }

            @Override
            public int getInt(int col) {
                return recordIndex;
            }

            @Override
            public long getRowId() {
                return recordIndex;
            }

            @Override
            public CharSequence getStr(int col) {
                return getSym(col);
            }

            @Override
            public CharSequence getStrB(int col) {
                return getSym(col);
            }

            @Override
            public int getStrLen(int col) {
                return getSym(col).length();
            }

            @Override
            public CharSequence getSym(int col) {
                return symbolMapReader.valueOf(recordIndex);
            }

            @Override
            public CharSequence getSymB(int col) {
                return symbolMapReader.valueBOf(recordIndex);
            }

            public void reset() {
                this.recordIndex = -1;
            }
        }
    }
}
