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
 * Specialized cursor factory to optimize the "timestamp ladder" cross-join used
 * in markout analysis. This is a cross-join between a table with a designated
 * timestamp, and an arithmetic sequence of time offsets, where the result is
 * ordered by the sum of the LHS timestamp and RHS time offset.
 * <p>
 * Example query:
 * <pre>
 * WITH offsets AS (
 *     SELECT 1_000_000 * sec_offs usec_offs
 *     FROM (SELECT x-601 AS sec_offs FROM long_sequence(1201))
 * )
 * SELECT id, order_ts + usec_offs AS ts
 * FROM orders CROSS JOIN offsets
 * ORDER BY order_ts + usec_offs
 * </pre>
 * Instead of emitting the cross-join results in the usual order, materializing all of
 * them, and then sorting them, this factory emits rows directly in the correct order.
 *
 * <h3>Detailed explanation of the algorithm</h3>
 * <p>
 * In this special-case cross-join, the slave table contains an integer column whose
 * values form an arithmetic sequence (numbers advancing at a steady rate). These
 * numbers represent time offsets that are added to the master row's timestamp. The
 * output of the cross-join is sorted on (<code>master_row_timestamp + slave_time_offset</code>).
 * <p>
 * The algorithm deals with {@link TimestampLadderRecordCursor.SlaveRowIterator
 * SlaveRowIterator} objects. Each iterator represents all the output rows that originate
 * from a single master row. At any point in the algorithm, we deal with two things:
 * <ol>
 *     <li>the master cursor positioned at the master row that we haven't yet started using
 *     <li>a circular list of active iterators, representing all the master rows that we are
 *     currently using to emit the output
 * </ol>
 * The algorithm runs as follows:
 * <p>
 * Go through the circular list, and emit one row from each iterator in turn,
 * round-robin fashion. Every time before taking a row from an iterator, inspect
 * the next master row's timestamp, to see which timestamp is less. If the iterator's
 * timestamp is less, take it and move on to the next iterator. If the master row's
 * timestamp is less, <em>activate</em> it: create an iterator for it, insert it
 * into the circular list, and emit its first row. Every time after taking a value from
 * an iterator, check if it's exhausted, and if so, remove it from the list. Continue
 * this until both the master cursor and all the iterators are fully exhausted.
 * <p>
 * One invocation of {@link TimestampLadderRecordCursor#hasNext() hasNext()}
 * executes one step of the algorithm, resulting in a single output row.
 */
public class TimestampLadderRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final TimestampLadderRecordCursor cursor;
    private final int masterColumnIndex;
    private final int slaveColumnIndex;

    /**
     * Creates a new timestamp ladder cursor factory.
     *
     * @param configuration     Cairo configuration
     * @param metadata          joined record metadata
     * @param masterFactory     factory for the LHS table (e.g., orders table)
     * @param slaveFactory      factory for the RHS arithmetic sequence
     * @param columnSplit       number of columns from the master (where slave columns start)
     * @param masterColumnIndex index of master's designated timestamp column
     * @param slaveColumnIndex  index of slave's time offset column
     * @param slaveRecordSink   RecordSink for materializing slave records
     */
    public TimestampLadderRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            int masterColumnIndex,
            int slaveColumnIndex,
            RecordSink slaveRecordSink,
            RecordSink masterRecordSink
    ) {
        super(metadata, null, masterFactory, slaveFactory);
        this.masterColumnIndex = masterColumnIndex;
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
                    masterColumnIndex,
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
        sink.meta("timestampColumn").val(masterColumnIndex);
        sink.meta("offsetColumn").val(slaveColumnIndex);
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
     * We materialize the slave cursor (arithmetic sequence) into a RecordArray
     * because we need random access to its rows.
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
            this.masterTimestampColumnIndex = masterTimestampColumnIndex;
            this.slaveSequenceColumnIndex = slaveSequenceColumnIndex;
            this.slaveRecordArray = slaveRecordArray;
            this.slaveRecordOffsets = new LongList();
            this.masterRecordSink = masterRecordSink;
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
                if (!masterHasNext || slaveRecordArray.size() == 0) {
                    return false;
                }
                isMasterHasNextPending = true;
                currentIter = prevIter = new SlaveRowIterator(masterCursor.getRecord());
                setupJoinRecord(currentIter.getMasterRecord(), currentIter.currentRowNum());
                return true;
            }

            circuitBreaker.statefulThrowExceptionIfTripped();

            advanceMasterIfPending();
            SlaveRowIterator nextIter = currentIter.nextIterator();
            final boolean nextIterNeedsSetup;
            if (currentIter.isEmpty()) {
                if (prevIter.removeNextIterator() == null) {
                    currentIter = null;
                    // prevIter.removeNextIterator() removed currentIter from the circular list.
                    // If it returned null, it implies that nextIter == currentIter (we got it from
                    // the circular list of size one). So, there's no actual next iterator.
                    if (!masterHasNext) {
                        // No more iterators, no more master rows, we're all done.
                        prevIter = null;
                        return false;
                    }
                    // Activate the next master row, and use this iterator as the beginning of a new circular list.
                    nextIter = activateMasterRow();
                    prevIter = nextIter;
                    nextIterNeedsSetup = false;
                } else {
                    nextIterNeedsSetup = true;
                }
            } else {
                prevIter = currentIter;
                nextIterNeedsSetup = true;
            }
            if (nextIterNeedsSetup) {
                if (masterHasNext && initialTimestampOfMasterRow() < nextIter.peekNextTimestamp()) {
                    nextIter = activateMasterRow();
                } else {
                    nextIter.gotoNextRow();
                }
            }
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
        }

        private @NotNull SlaveRowIterator activateMasterRow() {
            isMasterHasNextPending = true;
            if (currentIter != null) {
                return currentIter.postInsertNewIterator(masterCursor.getRecord());
            }
            return new SlaveRowIterator(masterCursor.getRecord());
        }

        private void advanceMasterIfPending() {
            if (isMasterHasNextPending) {
                masterHasNext = masterCursor.hasNext();
                isMasterHasNextPending = false;
            }
        }

        private long initialTimestampOfMasterRow() {
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
        }

        // Specifically designed for this algorithm:
        // - initially positioned at the first slave row
        // - you can get the current slave row number, but can't access that row at all
        // - you can peek the timestamp of the next slave row
        // - also works as a node in a circular list of iterators
        private class SlaveRowIterator {
            private final RecordChain masterRecordChain;
            private final long masterTimestamp;
            private SlaveRowIterator nextIter;
            private int nextSlaveRowNum;
            private long nextTimestamp = Long.MIN_VALUE;

            SlaveRowIterator(Record masterRecord) {
                // Create a RecordChain to hold a single master record
                masterRecordChain = new RecordChain(
                        masterMetadata,
                        masterRecordSink,
                        configuration.getSqlHashJoinValuePageSize(),
                        configuration.getSqlHashJoinValueMaxPages()
                );
                // Put the master record in the record chain
                masterRecordChain.put(masterRecord, -1);
                masterRecordChain.toTop();
                masterRecordChain.hasNext(); // Position at the record
                masterTimestamp = getMasterRecord().getTimestamp(masterTimestampColumnIndex);

                // Initialize iteration state
                nextSlaveRowNum = 0;
                nextIter = this;  // Initially points to itself (circular list of one)
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

            long peekNextTimestamp() {
                if (nextSlaveRowNum == slaveRowCount) {
                    return Long.MIN_VALUE;
                }
                return nextTimestamp;
            }

            SlaveRowIterator postInsertNewIterator(Record masterRecord) {
                SlaveRowIterator iter = new SlaveRowIterator(masterRecord);
                iter.nextIter = this.nextIter;
                this.nextIter = iter;
                return iter;
            }

            SlaveRowIterator removeNextIterator() {
                SlaveRowIterator toRemove = nextIter;
                nextIter = toRemove.nextIter;
                toRemove.close();
                if (toRemove == this) {
                    return null;  // this is the only iterator in the list, and we just closed it
                }
                return nextIter;
            }
        }
    }
}
