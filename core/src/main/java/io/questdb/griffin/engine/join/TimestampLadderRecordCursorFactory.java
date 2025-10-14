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
import io.questdb.std.Misc;

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
     * linked list of TimestampIterator objects (one per master row) that emit rows in
     * sorted timestamp order.
     */
    private static class TimestampLadderRecordCursor extends AbstractJoinCursor {
        private final CairoConfiguration configuration;
        private final RecordMetadata masterMetadata;
        private final RecordSink masterRecordSink;
        private final int masterTimestampColumnIndex;
        private final JoinRecord record;
        private final RecordArray slaveRecordArray;
        private final int slaveSequenceColumnIndex;
        private final RecordCursor.Counter tmpCounter;
        private SqlExecutionCircuitBreaker circuitBreaker;
        // Merge algorithm state
        private TimestampIterator currentIter;
        private boolean isMasterHasNextPending;
        private boolean isSlaveMaterialized;
        private boolean masterHasNext;
        private TimestampIterator prevIter;
        private long slaveSize;

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
            this.masterRecordSink = masterRecordSink;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
        }

        @Override
        public void close() {
            // Free all iterators in the circular list
            if (currentIter != null) {
                TimestampIterator iter = currentIter;
                TimestampIterator start = currentIter;
                do {
                    TimestampIterator next = iter.nextIterator();
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
            // Lazy initialization: create first iterator if needed
            if (currentIter == null) {
                if (isMasterHasNextPending) {
                    masterHasNext = masterCursor.hasNext();
                    isMasterHasNextPending = false;
                }
                if (!masterHasNext) {
                    return false;  // No master rows at all
                }
                // Create the first iterator
                currentIter = new TimestampIterator(masterCursor.getRecord());
                prevIter = currentIter;
                isMasterHasNextPending = true;  // We consumed a master row
            }

            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();

            currentIter.next();

            // Update the JoinRecord to point to current iterator's records
            record.of(currentIter.getMasterRecord(), currentIter.getSlaveRecord());

            // Determine the next iterator to use
            TimestampIterator nextIter = currentIter.nextIterator();

            // Check if we should activate a new iterator from the next master row
            if (isMasterHasNextPending) {
                masterHasNext = masterCursor.hasNext();
                isMasterHasNextPending = false;
            }

            if (masterHasNext) {
                long nextIterTimestamp = nextIter.peekNext();
                if (nextIterTimestamp != Long.MIN_VALUE) {
                    // Peek at the next master row's first timestamp
                    Record nextMasterRecord = masterCursor.getRecord();
                    long nextMasterTimestamp = nextMasterRecord.getTimestamp(masterTimestampColumnIndex);
                    // Get first slave offset
                    Record firstSlaveRecord = slaveRecordArray.getRecordAt(0);
                    long firstSlaveOffset = firstSlaveRecord.getLong(slaveSequenceColumnIndex);
                    long nextMasterFirstTimestamp = nextMasterTimestamp + firstSlaveOffset;

                    if (nextMasterFirstTimestamp < nextIterTimestamp) {
                        // Activate the new master row by inserting after current iterator
                        nextIter = currentIter.insertAfter(nextMasterRecord);
                        // Mark that we need to check for next master row
                        isMasterHasNextPending = true;
                    }
                } else {
                    // nextIter is exhausted, and it's the last one in the circular list
                    // Free the old exhausted iterator
                    currentIter.close();
                    // Re-initialize everything to the new master row
                    nextIter = new TimestampIterator(masterCursor.getRecord());
                    prevIter = currentIter = nextIter;
                    // Mark that we need to check for next master row
                    isMasterHasNextPending = true;
                }
            }

            // Remove exhausted iterators from the circular list
            if (currentIter.isEmpty()) {
                nextIter = prevIter.removeNextIterator();
                if (nextIter == null) {
                    // Last iterator exhausted, we're done
                    currentIter = null;
                    return true;  // But return the current row first
                }
            } else {
                prevIter = currentIter;
            }

            currentIter = nextIter;
            return true;  // We have a row to emit
        }

        @Override
        public long preComputedStateSize() {
            return 0;
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
                TimestampIterator iter = currentIter;
                TimestampIterator start = currentIter;
                do {
                    TimestampIterator next = iter.nextIterator();
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

        void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            this.circuitBreaker = circuitBreaker;

            // Materialize the slave cursor into RecordArray for efficient random access
            if (!isSlaveMaterialized) {
                slaveRecordArray.clear();
                final Record slaveRecord = slaveCursor.getRecord();
                while (slaveCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    slaveRecordArray.put(slaveRecord);
                }
                slaveRecordArray.toTop();
                isSlaveMaterialized = true;

                // Cache the slave size for iteration
                tmpCounter.clear();
                slaveRecordArray.calculateSize(circuitBreaker, tmpCounter);
                slaveSize = tmpCounter.get();
                tmpCounter.clear();
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
        private class TimestampIterator {
            private final RecordChain masterRecordChain;
            private TimestampIterator nextIter;
            private long nextTimestamp;
            private int slaveRecordIndex;

            TimestampIterator(Record masterRecord) {
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
                this.slaveRecordIndex = 0;
                this.nextIter = this;  // Initially points to itself (circular list of one)

                // Compute initial timestamp
                updateNextTimestamp();
            }

            private void updateNextTimestamp() {
                if (slaveRecordIndex >= slaveSize) {
                    nextTimestamp = Long.MIN_VALUE;  // Exhausted
                } else {
                    // Get master timestamp
                    long masterTimestamp = getMasterRecord().getTimestamp(masterTimestampColumnIndex);
                    // Get slave offset value at current index
                    Record slaveRec = slaveRecordArray.getRecordAt(slaveRecordIndex);
                    long slaveOffset = slaveRec.getLong(slaveSequenceColumnIndex);
                    // Compute combined timestamp
                    nextTimestamp = masterTimestamp + slaveOffset;
                }
            }

            void close() {
                Misc.free(masterRecordChain);
            }

            Record getMasterRecord() {
                return masterRecordChain.getRecord();
            }

            Record getSlaveRecord() {
                // Position the slaveRecordArray at our current index and return the record
                // We need to use getRecordAt since slaveRecordArray is shared
                return slaveRecordArray.getRecordAt(slaveRecordIndex);
            }

            TimestampIterator insertAfter(Record masterRecord) {
                TimestampIterator iter = new TimestampIterator(masterRecord);
                iter.nextIter = this.nextIter;
                this.nextIter = iter;
                return iter;
            }

            boolean isEmpty() {
                return slaveRecordIndex >= slaveSize;
            }

            /**
             * Advances to the next slave record.
             */
            void next() {
                if (nextTimestamp != Long.MIN_VALUE) {
                    slaveRecordIndex++;
                    updateNextTimestamp();
                }
            }

            TimestampIterator nextIterator() {
                return nextIter;
            }

            long peekNext() {
                return nextTimestamp;
            }

            TimestampIterator removeNextIterator() {
                if (nextIter == this) {
                    return null;  // Last iterator in the list
                }
                // Free the next iterator's resources
                nextIter.close();
                nextIter = nextIter.nextIter;
                return nextIter;
            }
        }
    }
}
