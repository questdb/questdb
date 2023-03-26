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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * Nested Loop with filter join.
 * Iterates on master factory in outer loop and on slave factory in inner loop
 * and returns all row pairs matching filter plus all unmatched rows from master factory.
 */
public class NestedLoopLeftJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private final NestedLoopLeftRecordCursor cursor;
    private final Function filter;
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;

    public NestedLoopLeftJoinRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            @NotNull Function filter,
            @NotNull Record nullRecord
    ) {
        super(metadata);
        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        this.filter = filter;
        this.cursor = new NestedLoopLeftRecordCursor(columnSplit, filter, nullRecord);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        RecordCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getCursor(executionContext);
            cursor.of(masterCursor, slaveCursor, executionContext);
            return cursor;
        } catch (Throwable ex) {
            Misc.free(masterCursor);
            Misc.free(slaveCursor);
            throw ex;
        }
    }

    @Override
    public int getScanDirection() {
        return masterFactory.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return masterFactory.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Nested Loop Left Join");
        sink.attr("filter").val(filter);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        ((JoinRecordMetadata) getMetadata()).close();
        masterFactory.close();
        slaveFactory.close();
        filter.close();
    }

    private static class NestedLoopLeftRecordCursor extends AbstractJoinCursor {
        private final Function filter;
        private final OuterJoinRecord record;
        private boolean isMasterHasNextPending;
        private boolean isMatch;
        private boolean masterHasNext;

        public NestedLoopLeftRecordCursor(int columnSplit, Function filter, Record nullRecord) {
            super(columnSplit);
            this.record = new OuterJoinRecord(columnSplit, nullRecord);
            this.filter = filter;
            this.isMatch = false;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (isMasterHasNextPending) {
                    masterHasNext = masterCursor.hasNext();
                    isMasterHasNextPending = false;
                }

                if (!masterHasNext) {
                    return false;
                }

                while (slaveCursor.hasNext()) {
                    if (filter.getBool(record)) {
                        isMatch = true;
                        return true;
                    }
                }

                if (!isMatch) {
                    isMatch = true;
                    record.hasSlave(false);
                    return true;
                }

                isMatch = false;
                slaveCursor.toTop();
                record.hasSlave(true);
                isMasterHasNextPending = true;
            }
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            slaveCursor.toTop();
            filter.toTop();
            isMatch = false;
            isMasterHasNextPending = true;
            record.hasSlave(true);
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext executionContext) throws SqlException {
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            filter.init(this, executionContext);
            record.of(masterCursor.getRecord(), slaveCursor.getRecord());
            isMasterHasNextPending = true;
        }
    }
}
