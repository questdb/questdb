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

import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;

public class DistinctSymbolRecordCursorFactory implements RecordCursorFactory {
    private final DistinctSymbolRecordCursor cursor;
    private final RecordMetadata metadata;
    private final TableReader reader;
    private final CharSequence columnName;

    public DistinctSymbolRecordCursorFactory(
            RecordCursorFactory base,
            EntityColumnFilter columnFilter,
            TableReader reader,
            CharSequence columnName) {
        final RecordMetadata metadata = base.getMetadata();
        columnFilter.of(metadata.getColumnCount());
        this.metadata = metadata;
        this.cursor = new DistinctSymbolRecordCursor();
        this.reader = reader;
        this.columnName = columnName;
    }

    @Override
    public void close() {
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        cursor.of(reader, columnName);
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
        private DistinctSymbolRecord recordB = new DistinctSymbolRecord();
        private TableReader reader;
        private SymbolMapReader symbolMapReader;
        private int numberOfSymbols;

        DistinctSymbolRecordCursor() {
        }

        @Override
        public void close() {
            if (reader != null) {
                reader.close();
                reader = null;
            }
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

        public void of(TableReader reader, CharSequence columnName) {
            int columnIndex = reader.getMetadata().getColumnIndex(columnName);
            this.symbolMapReader = reader.getSymbolMapReader(columnIndex);
            this.numberOfSymbols = symbolMapReader.size();
            this.reader = reader;
            this.recordA.of(reader, columnIndex);
            this.recordB.of(reader, columnIndex);
        }

        @Override
        public long size() {
            return numberOfSymbols;
        }
    }

    public static class DistinctSymbolRecord implements Record {
        private int columnIndex;
        private int recordIndex = -1;
        private TableReader reader;

        @Override
        public CharSequence getSym(int col) {
            if (reader != null) {
                return reader.getSymbolMapReader(columnIndex).valueOf(recordIndex);
            }
            return null;
        }

        @Override
        public int getInt(int col) {
            return recordIndex;
        }

        @Override
        public long getRowId() {
            return recordIndex;
        }

        public void of(TableReader reader, int columnIndex) {
            this.reader = reader;
            this.columnIndex = columnIndex;
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
