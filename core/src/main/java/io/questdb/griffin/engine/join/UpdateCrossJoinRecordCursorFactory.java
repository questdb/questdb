/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

public class UpdateCrossJoinRecordCursorFactory implements UpdateStatementMasterCursorFactory {
    private final RecordMetadata metadata;
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final UpdateCrosJoinRecordCursor cursor;

    public UpdateCrossJoinRecordCursorFactory(RecordMetadata joinMetadata,
                                              RecordCursorFactory masterFactory,
                                              RecordCursorFactory slaveFactory,
                                              int columnSplit

    ) {
        this.metadata = joinMetadata;
        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        this.cursor = new UpdateCrosJoinRecordCursor(columnSplit);
    }

    @Override
    public void close() {
        ((JoinRecordMetadata) metadata).close();
        masterFactory.close();
        slaveFactory.close();
    }

    @Override
    public UpdateStatementMasterCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor slaveCursor = slaveFactory.getCursor(executionContext);
        cursor.of(slaveCursor);
        return cursor;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    private class UpdateCrosJoinRecordCursor implements UpdateStatementMasterCursor {
        private final JoinRecord recordA;
        private RecordCursor masterCursor;
        private RecordCursor slaveCursor;
        private Record masterRecord;

        public UpdateCrosJoinRecordCursor(int columnSplit) {
            this.recordA = new JoinRecord(columnSplit);
        }

        public void of(RecordCursor slaveCursor) {
            this.slaveCursor = slaveCursor;
        }

        @Override
        public void setMaster(Record masterRecord) {
            this.masterRecord = masterRecord;
            slaveCursor.toTop();
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = masterRecord != null && slaveCursor.hasNext();
            if (hasNext) {
                recordA.of(masterRecord, slaveCursor.getRecord());
            }
            return hasNext;
        }

        @Override
        public void close() {
            masterCursor = Misc.free(masterCursor);
            slaveCursor = Misc.free(slaveCursor);
        }
    }
}
