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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.RecordArray;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * Specialized cursor factory for "timestamp ladder" pattern optimization.
 * This pattern occurs when a table with a timestamp column is cross-joined with
 * an arithmetic sequence, and the result is ordered by the sum of the timestamp
 * and the sequence offset.
 * <p>
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
 * <p>
 * Instead of materializing all cross-join results and then sorting, this factory
 * emits rows directly in the correct order by leveraging the structure of the pattern.
 */
public class TimestampLadderRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final TimestampLadderRecordCursor cursor;
    private final int slaveColumnIndex;
    private final int timestampColumnIndex;

    /**
     * Creates a new timestamp ladder cursor factory.
     *
     * @param configuration        the Cairo configuration
     * @param metadata             the joined record metadata
     * @param masterFactory        the factory for the LHS table (e.g., orders table)
     * @param slaveFactory         the factory for the RHS arithmetic sequence
     * @param columnSplit          the number of columns from the master (where slave columns start)
     * @param timestampColumnIndex the index of the timestamp column being modified (in master metadata)
     * @param slaveColumnIndex     the index of the arithmetic sequence column in slave metadata
     * @param slaveRecordSink      the RecordSink for materializing slave records
     */
    public TimestampLadderRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            int timestampColumnIndex,
            int slaveColumnIndex,
            RecordSink slaveRecordSink,
            RecordSink masterRecordSink
    ) {
        super(metadata, null, masterFactory, slaveFactory);
        this.timestampColumnIndex = timestampColumnIndex;
        this.slaveColumnIndex = slaveColumnIndex;

        RecordArray slaveRecordArray = null;
        try {
            slaveRecordArray = new RecordArray(
                    slaveFactory.getMetadata(),
                    slaveRecordSink,
                    configuration.getSqlHashJoinValuePageSize(),
                    configuration.getSqlHashJoinValueMaxPages()
            );
            this.cursor = new TimestampLadderRecordCursor(
                    configuration,
                    columnSplit,
                    masterFactory.getMetadata(),
                    timestampColumnIndex,
                    slaveColumnIndex,
                    slaveRecordArray,
                    masterRecordSink
            );
        } catch (Throwable th) {
            Misc.free(slaveRecordArray);
            throw th;
        }
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
        sink.meta("timestampColumn").val(timestampColumnIndex);
        sink.meta("slaveColumn").val(slaveColumnIndex);
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
     * <p>
     * The slave cursor (arithmetic sequence) is fully materialized into a RecordArray
     * for efficient random access during the optimized iteration.
     * <p>
     * Uses a merge algorithm similar to ArithmeticSequenceMerger, maintaining a circular
     * linked list of SlaveRowIterator objects (one per master row) that emit rows in
     * sorted timestamp order.
     */
    private static class TimestampLadderRecordCursor extends AbstractJoinCursor {
        private final CairoConfiguration configuration;
        private final RecordMetadata masterMetadata;
        private final RecordSink masterRecordSink;
        private final int masterTimestampColumnIndex;
        private final JoinRecord record;
        private final RecordArray slaveRecordArray;
        private final LongList slaveRecordOffsets;
        private final int slaveSequenceColumnIndex;
        private final RecordCursor.Counter tmpCounter;
        private SqlExecutionCircuitBreaker circuitBreaker;
        // Merge algorithm state
        private SlaveRowIterator currentIter;
        private long firstSlaveTimeOffset;  // Cache the first slave's offset value
        private boolean isMasterHasNextPending;
        private boolean masterHasNext;
        private SlaveRowIterator prevIter;
        private long slaveRowCount;

        public TimestampLadderRecordCursor(
                CairoConfiguration configuration,
                int columnSplit,
                RecordMetadata masterMetadata,
                int masterTimestampColumnIndex,
                int slaveSequenceColumnIndex,
                RecordArray slaveRecordArray,
                RecordSink masterRecordSink
        ) {
            super(columnSplit);
            this.configuration = configuration;
            this.masterMetadata = masterMetadata;
            this.record = new JoinRecord(columnSplit);
            this.tmpCounter = new Counter();
            this.masterTimestampColumnIndex = masterTimestampColumnIndex;
            this.slaveSequenceColumnIndex = slaveSequenceColumnIndex;
            this.slaveRecordArray = slaveRecordArray;
            this.slaveRecordOffsets = new LongList();
            this.masterRecordSink = masterRecordSink;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
        }

        @Override
        public void close() {
            // Free all iterators in the circular list
            if (currentIter != null) {
                SlaveRowIterator iter = currentIter;
                SlaveRowIterator start = currentIter;
                do {
                    SlaveRowIterator next = iter.nextIterator();
                    iter.close();
                    iter = next;
                } while (iter != start && iter != null);
                currentIter = null;
                prevIter = null;
            }
            Misc.free(slaveRecordArray);
            super.close();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (currentIter == null) {
                advanceMasterIfPending();
                if (!masterHasNext) {
                    return false;
                }
                isMasterHasNextPending = true;
                currentIter = prevIter = new SlaveRowIterator(masterCursor.getRecord());
                setupJoinRecord(currentIter.getMasterRecord(), currentIter.currentRowNum());
                return true;
            }

            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();

            advanceMasterIfPending();
            SlaveRowIterator nextIter = currentIter.nextIterator();
            if (currentIter.isEmpty() && prevIter.removeNextIterator() == null) {
                // prevIter.removeNextIterator() removed currentIter from the circular list.
                // If it returned null, it implies that nextIter == currentIter (we got it from
                // the circular list of size one). So, there's no actual next iterator.
                if (masterHasNext) {
                    // Activate the next master row, and use this iterator as the beginning of a new circular list.
                    nextIter = activateMasterRow();
                } else {
                    // No more iterators, no more master rows, we're all done.
                    prevIter = currentIter = null;
                    return false;
                }
            }
            if (masterHasNext && initialTimestampOfNextMaster() < nextIter.peekNextTimestamp()) {
                nextIter = activateMasterRow();
            } else {
                nextIter.gotoNextRow();
            }
            prevIter = currentIter;
            currentIter = nextIter;
            setupJoinRecord(currentIter.getMasterRecord(), currentIter.currentRowNum());
            return true;
        }

        @Override
        public long preComputedStateSize() {
            return slaveRecordArray.size() + slaveRecordOffsets.size();
        }

        @Override
        public long size() {
            long sizeA = masterCursor.size();
            long sizeB = slaveRecordArray.size();
            if (sizeA == -1 || sizeB == -1) {
                return -1;
            }
            final long result = sizeA * sizeB;
            return result < sizeA && sizeB > 0 ? Long.MAX_VALUE : result;
        }

        @Override
        public void skipRows(Counter rowCount) throws DataUnavailableException {
        }

        @Override
        public void toTop() {
            // Free all existing iterators
            if (currentIter != null) {
                SlaveRowIterator iter = currentIter;
                SlaveRowIterator start = currentIter;
                do {
                    SlaveRowIterator next = iter.nextIterator();
                    iter.close();
                    iter = next;
                } while (iter != start && iter != null);
            }

            // Reset state
            currentIter = null;
            prevIter = null;
            isMasterHasNextPending = true;
            masterHasNext = false;
            masterCursor.toTop();
            slaveRecordArray.toTop();
            tmpCounter.clear();
        }

        private @NotNull SlaveRowIterator activateMasterRow() {
            isMasterHasNextPending = true;
            return currentIter.postInsert(masterCursor.getRecord());
        }

        private void advanceMasterIfPending() {
            if (isMasterHasNextPending) {
                masterHasNext = masterCursor.hasNext();
                isMasterHasNextPending = false;
            }
        }

        private long initialTimestampOfNextMaster() {
            Record nextMasterRecord = masterCursor.getRecord();
            long nextMasterTimestamp = nextMasterRecord.getTimestamp(masterTimestampColumnIndex);
            return nextMasterTimestamp + firstSlaveTimeOffset;
        }

        private void setupJoinRecord(Record masterRecord, int slaveRowNum) {
            long slaveOffset = slaveRecordOffsets.getQuick(slaveRowNum);
            Record slaveRecord = slaveRecordArray.getRecordAt(slaveOffset);
            record.of(masterRecord, slaveRecord);
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            this.circuitBreaker = circuitBreaker;

            // Materialize the slave cursor into RecordArray for efficient random access
            slaveRecordArray.clear();
            slaveRecordOffsets.clear();
            final Record slaveRecord = slaveCursor.getRecord();
            while (slaveCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                long offset = slaveRecordArray.put(slaveRecord);
                slaveRecordOffsets.add(offset);
            }
            slaveRecordArray.toTop();

            // Cache the slave row count for iteration
            slaveRowCount = slaveRecordOffsets.size();

            // Cache the first slave's offset value to avoid repositioning recordC later
            if (slaveRowCount > 0) {
                long firstSlaveRecordOffset = slaveRecordOffsets.getQuick(0);
                Record firstSlaveRecord = slaveRecordArray.getRecordAt(firstSlaveRecordOffset);
                firstSlaveTimeOffset = firstSlaveRecord.getLong(slaveSequenceColumnIndex);
            }

            // Reset iterator state for new execution
            currentIter = null;
            prevIter = null;
            isMasterHasNextPending = true;
            masterHasNext = false;

            record.of(masterCursor.getRecord(), slaveRecordArray.getRecord());
            tmpCounter.clear();
        }

        /**
         * Iterator for a single master row, iterating through all slave records.
         * Forms a circular linked list with other iterators for the merge algorithm.
         */
        private class SlaveRowIterator {
            private final RecordChain masterRecordChain;
            private SlaveRowIterator nextIter;
            private int nextSlaveRowNum;
            private long nextTimestamp = Long.MIN_VALUE;

            SlaveRowIterator(Record masterRecord) {
                // Create a RecordChain to hold a single master record
                this.masterRecordChain = new RecordChain(
                        masterMetadata,
                        masterRecordSink,
                        configuration.getSqlHashJoinValuePageSize(),
                        configuration.getSqlHashJoinValueMaxPages()
                );
                // Store the master record
                this.masterRecordChain.put(masterRecord, -1);
                this.masterRecordChain.toTop();
                this.masterRecordChain.hasNext(); // Position at the record

                // Initialize iteration state
                this.nextSlaveRowNum = 0;
                this.nextIter = this;  // Initially points to itself (circular list of one)
                gotoNextRow();
            }

            void close() {
                Misc.free(masterRecordChain);
            }

            int currentRowNum() {
                return nextSlaveRowNum - 1;
            }

            Record getMasterRecord() {
                return masterRecordChain.getRecord();
            }

            void gotoNextRow() {
                if (nextSlaveRowNum == slaveRowCount) {
                    return;
                }
                nextSlaveRowNum++;
                long masterTimestamp = getMasterRecord().getTimestamp(masterTimestampColumnIndex);
                long slaveRecordOffset = slaveRecordOffsets.getQuick(nextSlaveRowNum);
                Record slaveRec = slaveRecordArray.getRecordAt(slaveRecordOffset);
                long slaveOffset = slaveRec.getLong(slaveSequenceColumnIndex);
                nextTimestamp = masterTimestamp + slaveOffset;
            }

            boolean isEmpty() {
                return nextSlaveRowNum == slaveRowCount;
            }

            SlaveRowIterator nextIterator() {
                return nextIter;
            }

            /**
             * Returns the timestamp of the current row.
             * If the row has not been consumed, the
             * timestamp is returned immediately. If the row has been consumed, the iterator
             * advances to the next row and returns the timestamp of that row.
             */
            long peekNextTimestamp() {
                if (nextSlaveRowNum == slaveRowCount) {
                    return Long.MIN_VALUE;
                }
                return nextTimestamp;
            }

            SlaveRowIterator postInsert(Record masterRecord) {
                SlaveRowIterator iter = new SlaveRowIterator(masterRecord);
                iter.nextIter = this.nextIter;
                this.nextIter = iter;
                return iter;
            }

            SlaveRowIterator removeNextIterator() {
                SlaveRowIterator toRemove = nextIter;
                // Free the next iterator's resources
                nextIter = toRemove.nextIter;
                toRemove.close();
                if (toRemove == this) {
                    return null;  // This is the only iterator in the list, and we just closed it
                }
                return nextIter;
            }
        }
    }
}
