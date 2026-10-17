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
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * Nested Loop with filter join.
 * Iterates on master factory in outer loop and on slave factory in inner loop
 * and returns all row pairs matching filter plus all unmatched rows from master and slave factory.
 */
public class NestedLoopFullJoinRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private NestedLoopFullRecordCursor cursor;
    private Function filter;

    public NestedLoopFullJoinRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            @NotNull Function filter,
            @NotNull Record masterNullRecord,
            @NotNull Record slaveNullRecord
    ) {
        super(metadata, null, masterFactory, slaveFactory);
        this.filter = filter;
        try {
            this.cursor = new NestedLoopFullRecordCursor(columnSplit, filter, masterNullRecord, slaveNullRecord);
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
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
            slaveCursor.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
            cursor.of(masterCursor, slaveCursor, executionContext);
            return cursor;
        } catch (Throwable ex) {
            Misc.free(masterCursor);
            Misc.free(slaveCursor);
            // of() reopens the matched-row bitmap before it assigns the master/slave cursors, so a
            // breach there leaves them unset; close() frees the bitmap and resets isOpen for reuse.
            Misc.free(cursor);
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
        sink.type("Nested Loop Full Join");
        sink.attr("filter").val(filter);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        final NestedLoopFullRecordCursor cursor = this.cursor;
        this.cursor = null;
        final Function filter = this.filter;
        this.filter = null;
        Throwable failure = closeJoinOwnersBestEffort();
        failure = Misc.freeBestEffort(failure, filter);
        failure = Misc.freeBestEffort(failure, cursor);
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Replays the slave from the top for every master row, so the position of a slave row within a pass identifies
     * it; a bitmap over those positions records the slave rows a master row matched.
     */
    private static class NestedLoopFullRecordCursor extends AbstractJoinCursor {
        private final Function filter;
        private final JoinSymbolTableSource filterSymbolTableSource;
        private final DirectLongList matchedSlaveRows = new DirectLongList(16, MemoryTag.NATIVE_BIT_SET, true);
        private final FullOuterJoinRecord record;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMasterHasNextPending;
        private boolean isMatch;
        private boolean isOpen;
        private boolean masterHasNext;
        private long slaveRowIndex;

        public NestedLoopFullRecordCursor(int columnSplit, Function filter, Record masterNullRecord, Record slaveNullRecord) {
            super(columnSplit);
            this.record = new FullOuterJoinRecord(columnSplit, masterNullRecord, slaveNullRecord);
            this.filter = filter;
            this.filterSymbolTableSource = new JoinSymbolTableSource(columnSplit);
            this.isMatch = false;
            isOpen = false;
        }

        @Override
        public void close() {
            if (isOpen) {
                super.close();
                Misc.free(matchedSlaveRows);
                isOpen = false;
            }
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
                    masterHasNext = masterCursor.hasNext();
                    isMasterHasNextPending = false;
                }

                if (!masterHasNext) {
                    while (slaveCursor.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                        if (!isMatched(slaveRowIndex++)) {
                            record.hasMaster(false);
                            return true;
                        }
                    }
                    return false;
                }

                while (slaveCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                    final long slaveRow = slaveRowIndex++;
                    if (filter.getBool(record)) {
                        markMatched(slaveRow);
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
                slaveRowIndex = 0;
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
            record.hasMaster(true);
            record.hasSlave(true);
            matchedSlaveRows.clear();
            slaveRowIndex = 0;
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext executionContext) throws SqlException {
            if (!isOpen) {
                isOpen = true;
                matchedSlaveRows.setMemoryTracker(executionContext.getMemoryTracker());
                matchedSlaveRows.reopen();
            }
            // filter.init() resolves symbols through a source over the master/slave cursors. Adopt the
            // cursors last so an init() throw above leaves them unset for the getCursor() catch.
            filterSymbolTableSource.of(masterCursor, slaveCursor);
            filter.init(filterSymbolTableSource, executionContext);
            record.of(masterCursor.getRecord(), slaveCursor.getRecord());
            isMasterHasNextPending = true;
            isMatch = false;
            slaveRowIndex = 0;
            circuitBreaker = executionContext.getCircuitBreaker();
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
        }

        private boolean isMatched(long slaveRow) {
            final long wordIndex = slaveRow >>> 6;
            return wordIndex < matchedSlaveRows.size() && (matchedSlaveRows.get(wordIndex) & 1L << slaveRow) != 0;
        }

        private void markMatched(long slaveRow) {
            final long wordIndex = slaveRow >>> 6;
            while (matchedSlaveRows.size() <= wordIndex) {
                matchedSlaveRows.add(0);
            }
            matchedSlaveRows.set(wordIndex, matchedSlaveRows.get(wordIndex) | 1L << slaveRow);
        }
    }
}
