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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRecordCursor;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * Nested Loop with filter join.
 * Iterates on master factory in outer loop and on slave factory in inner loop
 * and returns all row pairs matching filter plus all unmatched rows from master factory.
 **/
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
            if (masterCursor.hasNext()) {
                cursor.of(masterCursor, slaveCursor, executionContext);
                return cursor;
            }
            slaveCursor = Misc.free(slaveCursor);
            Misc.free(masterCursor);
        } catch (Throwable ex) {
            Misc.free(masterCursor);
            Misc.free(slaveCursor);
            throw ex;
        }

        return EmptyTableRecordCursor.INSTANCE;
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
    protected void _close() {
        ((JoinRecordMetadata) getMetadata()).close();
        masterFactory.close();
        slaveFactory.close();
        filter.close();
    }

    private static class NestedLoopLeftRecordCursor extends AbstractJoinCursor {
        private final Function filter;
        private final OuterJoinRecord record;
        private boolean hasMore;
        private boolean isMatch;

        public NestedLoopLeftRecordCursor(int columnSplit, Function filter, Record nullRecord) {
            super(columnSplit);
            this.record = new OuterJoinRecord(columnSplit, nullRecord);
            this.filter = filter;
            this.isMatch = false;
            this.hasMore = true;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (!hasMore) {
                return false;
            }

            do {
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
                } else {
                    isMatch = false;
                    slaveCursor.toTop();
                    record.hasSlave(true);
                }
            } while (masterCursor.hasNext());

            hasMore = false;
            return false;
        }

        @Override
        public long size() {
            return -1L;
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            masterCursor.hasNext();
            slaveCursor.toTop();
            filter.toTop();
            isMatch = false;
            hasMore = true;
            record.hasSlave(true);
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext executionContext) throws SqlException {
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            this.filter.init(this, executionContext);
            this.record.of(masterCursor.getRecord(), slaveCursor.getRecord());
            this.hasMore = true;
        }
    }
}
