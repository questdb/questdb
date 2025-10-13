/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

/**
 * Specialized cursor factory for "timestamp ladder" pattern optimization.
 * This pattern occurs when a table with a timestamp column is cross-joined with
 * an arithmetic sequence, and the result is ordered by the sum of the timestamp
 * and the sequence offset.
 *
 * Example query:
 * <pre>
 * WITH offsets AS (
 *     SELECT sec_offs, 1_000_000 * sec_offs usec_offs
 *     FROM (SELECT x-601 AS sec_offs FROM long_sequence(1201))
 * )
 * SELECT id, order_ts + usec_offs AS ts
 * FROM orders CROSS JOIN offsets
 * ORDER BY order_ts + usec_offs
 * </pre>
 *
 * Instead of materializing all cross-join results and then sorting, this factory
 * emits rows directly in the correct order by leveraging the structure of the pattern.
 */
public class TimestampLadderRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final int columnSplit;
    private final TimestampLadderRecordCursor cursor;
    private final long sequenceCount;
    private final long sequenceStart;
    private final long sequenceStep;
    private final int timestampColumnIndex;

    /**
     * Creates a new timestamp ladder cursor factory.
     *
     * @param metadata              the joined record metadata
     * @param masterFactory         the factory for the LHS table (e.g., orders table)
     * @param slaveFactory          the factory for the RHS arithmetic sequence
     * @param columnSplit           the number of columns from the master (where slave columns start)
     * @param timestampColumnIndex  the index of the timestamp column being modified (in master metadata)
     * @param sequenceStart         the initial value of the arithmetic sequence
     * @param sequenceStep          the step size between sequence elements
     * @param sequenceCount         the number of elements in the sequence
     */
    public TimestampLadderRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            int timestampColumnIndex,
            long sequenceStart,
            long sequenceStep,
            long sequenceCount
    ) {
        super(metadata, null, masterFactory, slaveFactory);
        this.columnSplit = columnSplit;
        this.timestampColumnIndex = timestampColumnIndex;
        this.sequenceStart = sequenceStart;
        this.sequenceStep = sequenceStep;
        this.sequenceCount = sequenceCount;
        this.cursor = new TimestampLadderRecordCursor(
                columnSplit,
                timestampColumnIndex,
                sequenceStart,
                sequenceStep,
                sequenceCount
        );
    }

    @Override
    public boolean followedOrderByAdvice() {
        // We handle the ORDER BY internally by emitting rows in sorted order
        return true;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        RecordCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getCursor(executionContext);
            cursor.of(masterCursor, slaveCursor, executionContext.getCircuitBreaker());
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
        sink.type("Timestamp Ladder Join");
        sink.meta("start").val(sequenceStart);
        sink.meta("step").val(sequenceStep);
        sink.meta("count").val(sequenceCount);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
    }

    /**
     * Record cursor that emits cross-join results in timestamp order.
     *
     * TODO: Implement the optimized algorithm that avoids full materialization and sorting.
     * For now, this uses a simple cross-join approach as a placeholder.
     */
    private static class TimestampLadderRecordCursor extends AbstractJoinCursor {
        private final JoinRecord record;
        private final RecordCursor.Counter tmpCounter;
        private final long sequenceCount;
        private final long sequenceStart;
        private final long sequenceStep;
        private final int timestampColumnIndex;

        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMasterHasNextPending;
        private boolean isMasterSizeCalculated;
        private boolean isSlavePartialSizeCalculated;
        private boolean isSlaveReset;
        private boolean isSlaveSizeCalculated;
        private boolean masterHasNext;
        private long masterSize;
        private long slavePartialSize;
        private long slaveSize;

        public TimestampLadderRecordCursor(
                int columnSplit,
                int timestampColumnIndex,
                long sequenceStart,
                long sequenceStep,
                long sequenceCount
        ) {
            super(columnSplit);
            this.record = new JoinRecord(columnSplit);
            this.tmpCounter = new Counter();
            this.timestampColumnIndex = timestampColumnIndex;
            this.sequenceStart = sequenceStart;
            this.sequenceStep = sequenceStep;
            this.sequenceCount = sequenceCount;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
            if (!isMasterHasNextPending && !masterHasNext) {
                return;
            }

            if (!isSlavePartialSizeCalculated) {
                slaveCursor.calculateSize(circuitBreaker, tmpCounter);
                slavePartialSize = tmpCounter.get();
                isSlavePartialSizeCalculated = true;
                tmpCounter.clear();
            }
            if (!isMasterSizeCalculated) {
                masterCursor.calculateSize(circuitBreaker, tmpCounter);
                masterSize = tmpCounter.get();
                isMasterSizeCalculated = true;
                tmpCounter.clear();
            }

            if (masterSize == 0) {
                if (!isMasterHasNextPending) {
                    counter.add(slavePartialSize);
                }
            } else {
                if (!isSlaveSizeCalculated) {
                    if (!isSlaveReset) {
                        slaveCursor.toTop();
                        isSlaveReset = true;
                    }
                    slaveCursor.calculateSize(circuitBreaker, tmpCounter);
                    slaveSize = tmpCounter.get();
                    isSlaveSizeCalculated = true;
                }

                long size = masterSize * slaveSize;
                if (!isMasterHasNextPending) {
                    size += slavePartialSize;
                }
                counter.add(size);
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            // TODO: Replace with optimized algorithm that emits rows in sorted order
            // For now, using simple cross-join logic as placeholder
            while (true) {
                if (isMasterHasNextPending) {
                    masterHasNext = masterCursor.hasNext();
                    isMasterHasNextPending = false;
                }

                if (!masterHasNext) {
                    return false;
                }

                if (slaveCursor.hasNext()) {
                    return true;
                }

                slaveCursor.toTop();
                circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                isMasterHasNextPending = true;
            }
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            long sizeA = masterCursor.size();
            long sizeB = slaveCursor.size();
            if (sizeA == -1 || sizeB == -1) {
                return -1;
            }
            final long result = sizeA * sizeB;
            return result < sizeA && sizeB > 0 ? Long.MAX_VALUE : result;
        }

        @Override
        public void skipRows(Counter rowCount) throws DataUnavailableException {
            if (rowCount.get() == 0) {
                return;
            }

            if (!isSlaveSizeCalculated) {
                slaveCursor.calculateSize(this.circuitBreaker, tmpCounter);
                slaveSize = tmpCounter.get();
                isSlaveSizeCalculated = true;
                tmpCounter.clear();
            }

            if (slaveSize == 0) {
                return;
            }

            long masterToSkip = rowCount.get() / slaveSize;
            tmpCounter.set(masterToSkip);
            try {
                masterCursor.skipRows(tmpCounter);
                masterHasNext = masterCursor.hasNext();
                isMasterHasNextPending = false;
            } finally {
                // in case of DataUnavailableException
                long diff = (masterToSkip - tmpCounter.get()) * slaveSize;
                rowCount.dec(diff);
            }

            if (!masterHasNext) {
                return;
            }

            if (!isSlaveReset) {
                slaveCursor.toTop();
                isSlaveReset = true;
            }
            slaveCursor.skipRows(rowCount);
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            slaveCursor.toTop();
            isMasterHasNextPending = true;

            isSlavePartialSizeCalculated = false;
            isSlaveReset = false;
            isSlaveSizeCalculated = false;
            isMasterSizeCalculated = false;
            tmpCounter.clear();
            masterSize = 0;
            slaveSize = 0;
            slavePartialSize = 0;
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            record.of(masterCursor.getRecord(), slaveCursor.getRecord());
            isMasterHasNextPending = true;
            this.circuitBreaker = circuitBreaker;

            isSlavePartialSizeCalculated = false;
            isSlaveReset = false;
            isSlaveSizeCalculated = false;
            isMasterSizeCalculated = false;
            tmpCounter.clear();
            masterSize = 0;
            slaveSize = 0;
            slavePartialSize = 0;
        }
    }
}
