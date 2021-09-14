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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRecordCursor;
import io.questdb.std.Misc;

public class CrossJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final CrossJoinRecordCursor cursor;

    public CrossJoinRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit

    ) {
        super(metadata);
        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        this.cursor = new CrossJoinRecordCursor(columnSplit);
    }

    @Override
    public void close() {
        ((JoinRecordMetadata) getMetadata()).close();
        masterFactory.close();
        slaveFactory.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        RecordCursor slaveCursor = slaveFactory.getCursor(executionContext);
        if (masterCursor.hasNext()) {
            cursor.of(masterCursor, slaveCursor);
            return cursor;
        }

        return EmptyTableRecordCursor.INSTANCE;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    private static class CrossJoinRecordCursor implements NoRandomAccessRecordCursor {
        private final JoinRecord record;
        private final int columnSplit;
        private RecordCursor masterCursor;
        private RecordCursor slaveCursor;

        public CrossJoinRecordCursor(int columnSplit) {
            this.record = new JoinRecord(columnSplit);
            this.columnSplit = columnSplit;
        }

        @Override
        public void close() {
            masterCursor = Misc.free(masterCursor);
            slaveCursor = Misc.free(slaveCursor);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex < columnSplit) {
                return masterCursor.getSymbolTable(columnIndex);
            }
            return slaveCursor.getSymbolTable(columnIndex - columnSplit);
        }

        @Override
        public long size() {
            long sizeA = masterCursor.size();
            long sizeB = slaveCursor.size();
            if (sizeA == -1 || sizeB == -1) {
                return -1;
            }
            final long result = sizeA * sizeB;
            return result < sizeA ? Long.MAX_VALUE : result;
        }

        @Override
        public boolean hasNext() {

            if (slaveCursor.hasNext()) {
                return true;
            }

            if (masterCursor.hasNext()) {
                slaveCursor.toTop();
                return slaveCursor.hasNext();
            }

            return false;
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            masterCursor.hasNext();
            slaveCursor.toTop();
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor) {
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            record.of(masterCursor.getRecord(), slaveCursor.getRecord());
        }
    }
}
