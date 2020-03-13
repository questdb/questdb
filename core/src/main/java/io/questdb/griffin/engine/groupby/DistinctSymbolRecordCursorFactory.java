/* ******************************************************************************
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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

public class DistinctSymbolRecordCursorFactory implements RecordCursorFactory {
    private final DistinctSymbolRecordCursor cursor;
    private final CairoEngine engine;
    private final GenericRecordMetadata metadata;
    private final CharSequence tableName;
    private final int columnIndex;
    private final long tableVersion;

    public DistinctSymbolRecordCursorFactory(
            final CairoEngine engine,
            final GenericRecordMetadata metadata,
            final CharSequence tableName,
            final int columnIndex,
            final long tableVersion) {
        this.engine = engine;
        this.metadata = metadata;
        this.tableName = tableName;
        this.columnIndex = columnIndex;
        this.tableVersion = tableVersion;
        this.cursor = new DistinctSymbolRecordCursor();
    }

    @Override
    public void close() {
        cursor.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        TableReader reader = engine.getReader(executionContext.getCairoSecurityContext(), tableName, tableVersion);
        cursor.of(reader, columnIndex);
        return cursor;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean isRandomAccessCursor() {
        return true;
    }

    private static class DistinctSymbolRecordCursor implements RecordCursor {
        private DistinctSymbolRecord recordA = new DistinctSymbolRecord();
        private DistinctSymbolRecord recordB = null;
        private TableReader reader;
        private int columnIndex;
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
            if (recordA.getRecordIndex() + 1 < numberOfSymbols) {
                recordA.incrementRecordIndex();
                return true;
            }
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
            recordA.recordIndex = -1;
        }

        public void of(TableReader reader, int columnIndex) {
            this.reader = reader;
            this.columnIndex = columnIndex;
            this.symbolMapReader = reader.getSymbolMapReader(columnIndex);
            this.numberOfSymbols = symbolMapReader.size();
            this.recordA.reset();
        }

        @Override
        public long size() {
            return numberOfSymbols;
        }

        public class DistinctSymbolRecord implements Record {
            private int recordIndex = -1;

            @Override
            public CharSequence getSym(int col) {
                return reader.getSymbolMapReader(columnIndex).valueOf(recordIndex);
            }

            @Override
            public int getInt(int col) {
                return recordIndex;
            }

            @Override
            public long getRowId() {
                return recordIndex;
            }

            public void reset() {
                this.recordIndex = -1;
            }

            public long getRecordIndex() {
                return recordIndex;
            }

            public void incrementRecordIndex() {
                recordIndex++;
            }
        }
    }
}
