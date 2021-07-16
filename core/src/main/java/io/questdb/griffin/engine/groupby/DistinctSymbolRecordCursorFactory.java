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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

public class DistinctSymbolRecordCursorFactory implements RecordCursorFactory {
    private final DistinctSymbolRecordCursor cursor;
    private final CairoEngine engine;
    private final GenericRecordMetadata metadata;
    private final String tableName;
    private final int columnIndex;
    private final long tableVersion;
    private final int tableId;

    public DistinctSymbolRecordCursorFactory(
            final CairoEngine engine,
            final GenericRecordMetadata metadata,
            final String tableName,
            final int columnIndex,
            final int tableId,
            final long tableVersion) {
        this.engine = engine;
        this.metadata = metadata;
        this.tableName = tableName;
        this.columnIndex = columnIndex;
        this.tableVersion = tableVersion;
        this.tableId = tableId;
        this.cursor = new DistinctSymbolRecordCursor();
    }

    @Override
    public void close() {
        cursor.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        TableReader reader = engine.getReader(executionContext.getCairoSecurityContext(), tableName, tableId, tableVersion);
        cursor.of(reader, columnIndex);
        return cursor;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    private static class DistinctSymbolRecordCursor implements RecordCursor {
        private final DistinctSymbolRecord recordA = new DistinctSymbolRecord();
        private DistinctSymbolRecord recordB = null;
        private TableReader reader;
        private int numberOfSymbols;
        private SymbolMapReader symbolMapReader;

        @Override
        public void close() {
            reader = Misc.free(reader);
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
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
        public Record getRecordB() {
            if (recordB == null) {
                recordB = new DistinctSymbolRecord();
            }
            recordB.reset();
            return recordB;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            ((DistinctSymbolRecord) record).recordIndex = (int) atRowId;
        }

        @Override
        public void toTop() {
            recordA.reset();
        }

        public void of(TableReader reader, int columnIndex) {
            this.reader = reader;
            this.symbolMapReader = reader.getSymbolMapReader(columnIndex);
            this.numberOfSymbols = symbolMapReader.size() + (symbolMapReader.containsNullValue() ? 1 : 0);
            this.recordA.reset();
        }

        @Override
        public long size() {
            return numberOfSymbols;
        }

        public class DistinctSymbolRecord implements Record {
            private int recordIndex = -1;

            public void decrementRecordIndex() {
                recordIndex--;
            }

            @Override
            public CharSequence getSym(int col) {
                return symbolMapReader.valueOf(recordIndex);
            }

            @Override
            public CharSequence getSymB(int col) {
                return symbolMapReader.valueBOf(recordIndex);
            }

            @Override
            public int getInt(int col) {
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
            public long getRowId() {
                return recordIndex;
            }

            public void reset() {
                this.recordIndex = -1;
            }

            public long getAndIncrementRecordIndex() {
                return ++recordIndex;
            }
        }
    }
}
