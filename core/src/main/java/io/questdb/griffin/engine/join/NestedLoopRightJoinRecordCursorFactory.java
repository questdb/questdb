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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * Nested Loop with filter join.
 * Iterates on slave factory in outer loop and on master factory in inner loop
 * and returns all row pairs matching filter plus all unmatched rows from slave factory.
 */
public class NestedLoopRightJoinRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final NestedLoopRightRecordCursor cursor;
    private final Function filter;

    public NestedLoopRightJoinRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            @NotNull Function filter,
            @NotNull Record nullRecord
    ) {
        super(metadata, null, masterFactory, slaveFactory);
        this.filter = filter;
        this.cursor = new NestedLoopRightRecordCursor(columnSplit, filter, nullRecord);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return slaveFactory.followedOrderByAdvice();
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
        sink.type("Nested Loop Right Join");
        sink.attr("filter").val(filter);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.free(filter);
        Misc.free(cursor);
    }

    private static class NestedLoopRightRecordCursor extends AbstractJoinCursor {
        private final Function filter;
        private final RightOuterJoinRecord record;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMatch;
        private boolean isSlaveHasNextPending;
        private boolean slaveHasNext;

        public NestedLoopRightRecordCursor(int columnSplit, Function filter, Record nullRecord) {
            super(columnSplit);
            this.record = new RightOuterJoinRecord(columnSplit, nullRecord);
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
                circuitBreaker.statefulThrowExceptionIfTripped();
                if (isSlaveHasNextPending) {
                    slaveHasNext = slaveCursor.hasNext();
                    isSlaveHasNextPending = false;
                }

                if (!slaveHasNext) {
                    return false;
                }

                while (masterCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    if (filter.getBool(record)) {
                        isMatch = true;
                        return true;
                    }
                }

                if (!isMatch) {
                    isMatch = true;
                    record.hasMaster(false);
                    return true;
                }

                isMatch = false;
                masterCursor.toTop();
                record.hasMaster(true);
                isSlaveHasNextPending = true;
            }
        }

        @Override
        public long preComputedStateSize() {
            return masterCursor.preComputedStateSize() + slaveCursor.preComputedStateSize();
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
            isSlaveHasNextPending = true;
            record.hasMaster(true);
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext executionContext) throws SqlException {
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            filter.init(this, executionContext);
            record.of(masterCursor.getRecord(), slaveCursor.getRecord());
            isSlaveHasNextPending = true;
            circuitBreaker = executionContext.getCircuitBreaker();
        }
    }
}
