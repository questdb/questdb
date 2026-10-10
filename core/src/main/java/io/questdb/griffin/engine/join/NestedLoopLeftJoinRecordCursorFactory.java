/*+*****************************************************************************
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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.ParquetDecodeHint;
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
 * Iterates on master factory in outer loop and on slave factory in inner loop
 * and returns all row pairs matching filter plus all unmatched rows from master factory.
 */
public class NestedLoopLeftJoinRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private NestedLoopLeftRecordCursor cursor;
    private Function filter;

    public NestedLoopLeftJoinRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            @NotNull Function filter,
            @NotNull Record nullRecord
    ) {
        super(metadata, null, masterFactory, slaveFactory);
        this.filter = filter;
        this.cursor = new NestedLoopLeftRecordCursor(columnSplit, filter, nullRecord);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return masterFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        RecordCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getCursor(executionContext);
            slaveCursor.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
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

    // Skips the master rows whose key the INNER hash join that reads this join's rows cannot match, see
    // SqlCodeGenerator.generateJoins(). The hash join drops every row of such a master row, matched or
    // NULL-extended alike.
    public void setJoinKeyFilter(JoinKeyFilter filter) {
        cursor.keyFilterGate.setFilter(filter);
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return masterFactory.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Nested Loop Left Join");
        sink.attr("filter").val(filter);
        if (cursor.keyFilterGate.hasFilter()) {
            sink.attr("joinKeyCheck").val(true);
        }
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        final NestedLoopLeftRecordCursor cursor = this.cursor;
        this.cursor = null;
        final Function filter = this.filter;
        this.filter = null;
        Throwable failure = closeJoinOwnersBestEffort();
        failure = Misc.freeBestEffort(failure, filter);
        failure = Misc.freeBestEffort(failure, cursor);
        CairoException.rethrowCleanupFailure(failure);
    }

    private static class NestedLoopLeftRecordCursor extends AbstractJoinCursor {
        private final Function filter;
        private final JoinKeyFilterGate keyFilterGate = new JoinKeyFilterGate();
        private final OuterJoinRecord record;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMasterHasNextPending;
        private boolean isMatch;
        private boolean masterHasNext;
        // the slave rows that the scan of the current master row has read, which JoinKeyFilterGate takes as
        // the slave's row count once a scan finishes
        private long slaveRowsInPass;

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
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                if (isMasterHasNextPending) {
                    masterHasNext = nextMasterRow();
                    isMasterHasNextPending = false;
                    slaveRowsInPass = 0;
                }

                if (!masterHasNext) {
                    return false;
                }

                while (slaveCursor.hasNext()) {
                    slaveRowsInPass++;
                    circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                    if (filter.getBool(record)) {
                        isMatch = true;
                        return true;
                    }
                }
                if (keyFilterGate.isCountingSlaveRows()) {
                    keyFilterGate.setSlaveRowCount(slaveRowsInPass);
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
            isMasterHasNextPending = true;
            record.hasSlave(true);
        }

        private boolean nextMasterRow() {
            while (masterCursor.hasNext()) {
                if (!keyFilterGate.isRowDropped(record)) {
                    return true;
                }
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            }
            return false;
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext executionContext) throws SqlException {
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            filter.init(this, executionContext);
            record.of(masterCursor.getRecord(), slaveCursor.getRecord());
            isMasterHasNextPending = true;
            // not the slave's size: a parent may still be opening its other cursors, see JoinKeyFilterGate
            keyFilterGate.of(slaveCursor);
            circuitBreaker = executionContext.getCircuitBreaker();
        }
    }
}
