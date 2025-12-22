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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.RecordArray;
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
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;

/**
 * Specialized cursor factory to optimize the cross-join used to create the
 * time-offset grid for markout analysis. This is a cross-join between a table
 * with a designated timestamp, and an arithmetic sequence of time offsets,
 * where the result is ordered by the sum of the LHS timestamp and RHS time
 * offset.
 * <p>
 * Example query:
 * <pre>
 * WITH offsets AS (
 *     SELECT 1_000_000 * sec_offs usec_offs
 *     FROM (SELECT x-601 AS sec_offs FROM long_sequence(1201))
 * )
 * SELECT /*+ markout_horizon(orders offsets) id, order_ts + usec_offs AS ts
 * FROM orders CROSS JOIN offsets
 * ORDER BY order_ts + usec_offs
 * </pre>
 * Instead of emitting the cross-join results in the usual order, materializing all of
 * them, and then sorting them, this factory emits rows directly in the correct order.
 *
 * <h2>Detailed explanation of the algorithm</h2>
 * <p>
 * In this special-case cross-join, the slave table contains an integer column whose
 * values form an arithmetic sequence (numbers advancing at a steady rate). These
 * numbers represent time offsets that are added to the master row's timestamp. The
 * output of the cross-join is sorted on (<code>master_row_timestamp + slave_time_offset</code>).
 * <p>
 * The algorithm deals with slave row iterators. Each iterator represents all the
 * output rows that originate from a single master row. At any point in the algorithm,
 * we deal with two things:
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
 * One invocation of {@link MarkoutHorizonRecordCursor#hasNext() hasNext()}
 * executes one step of the algorithm, resulting in a single output row.
 */
public class MarkoutHorizonRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final MarkoutHorizonRecordCursor cursor;
    private final int masterColumnIndex;
    private final int slaveColumnIndex;

    /**
     * Creates a new markout horizon cursor factory.
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
    public MarkoutHorizonRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            int masterColumnIndex,
            int slaveColumnIndex,
            RecordSink slaveRecordSink
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
            this.cursor = new MarkoutHorizonRecordCursor(
                    columnSplit,
                    masterColumnIndex,
                    slaveColumnIndex,
                    slaveRecordArray
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
        sink.type("Markout Horizon Join");
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
    private static class MarkoutHorizonRecordCursor extends AbstractJoinCursor {
        // Native memory layout constants
        private static final int BLOCK_HEADER_SIZE = 16;
        private static final int BLOCK_OFFSET_NEXT_BLOCK_ADDR = 0;    // long (8 bytes)
        private static final int BLOCK_OFFSET_NEXT_FREE_SLOT = 8;     // int (4 bytes)
        private static final int BLOCK_OFFSET_USED_SLOT_COUNT = 12;   // int (4 bytes)
        private static final int ITERATORS_PER_BLOCK = 1024;
        private static final int ITERATOR_OFFSET_MASTER_ROW_ID = 0;            // long (8 bytes)
        private static final int ITERATOR_OFFSET_MASTER_TIMESTAMP = 8;         // long (8 bytes)
        private static final int ITERATOR_OFFSET_NEXT_ITER_ADDR = 16;          // long (8 bytes)
        private static final int ITERATOR_OFFSET_NEXT_SLAVE_ROW_NUM = 32;      // int (4 bytes)
        private static final int ITERATOR_OFFSET_NEXT_TIMESTAMP = 24;          // long (8 bytes)
        private static final int ITERATOR_OFFSET_OFFSET_FROM_BLOCK_START = 36; // int (4 bytes)
        private static final int ITERATOR_SIZE = 40;
        private static final long BLOCK_SIZE = BLOCK_HEADER_SIZE + (ITERATORS_PER_BLOCK * ITERATOR_SIZE);
        private final JoinRecord joinRecord;
        private final int masterTimestampColumnIndex;
        private final RecordArray slaveRecordArray;
        private final LongList slaveRecordOffsets = new LongList();
        private final int slaveSequenceColumnIndex;
        private SqlExecutionCircuitBreaker circuitBreaker;

        private long currMasterRowId = -1;
        private long currentIterAddr;
        private long emittedRowCount;
        private long firstIteratorBlockAddr;
        private long firstSlaveTimeOffset;
        private boolean isMasterHasNextPending;
        private long lastIteratorBlockAddr;
        private boolean masterHasNext;
        private Record masterRecord;
        private long prevIterAddr;
        private long slaveRowCount;

        public MarkoutHorizonRecordCursor(
                int columnSplit,
                int masterTimestampColumnIndex,
                int slaveSequenceColumnIndex,
                RecordArray slaveRecordArray
        ) {
            super(columnSplit);
            this.joinRecord = new JoinRecord(columnSplit);
            this.masterTimestampColumnIndex = masterTimestampColumnIndex;
            this.slaveSequenceColumnIndex = slaveSequenceColumnIndex;
            this.slaveRecordArray = slaveRecordArray;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            long size = size();
            if (size != -1) {
                isMasterHasNextPending = false;
                masterHasNext = false;
                freeAllIteratorBlocks();
                counter.add(size - emittedRowCount);
            } else {
                // This can be optimized more by inspecting nextSlaveRowNum in all active iterators.
                // Use that to sum up remaining rows in all iterators, then add
                // masterCursor.calculateSize() * slaveRowCount
                super.calculateSize(circuitBreaker, counter);
            }
        }

        @Override
        public void close() {
            // Free all iterator blocks in the linked list
            freeAllIteratorBlocks();
            Misc.free(slaveRecordArray);
            super.close();
        }

        @Override
        public Record getRecord() {
            return joinRecord;
        }

        @Override
        public boolean hasNext() {
            if (currentIterAddr == 0) {
                advanceMasterIfPending();
                if (!masterHasNext || slaveRowCount == 0) {
                    return false;
                }
                isMasterHasNextPending = true;
                currentIterAddr = prevIterAddr = createIterator(masterRecord);
                emitJoinRecord(iter_masterRowId(currentIterAddr), currentRowNum(currentIterAddr));
                return true;
            }

            circuitBreaker.statefulThrowExceptionIfTripped();

            advanceMasterIfPending();
            long nextIterAddr = iter_nextIterAddr(currentIterAddr);
            final boolean nextIterNeedsSetup;
            if (isEmpty(currentIterAddr)) {
                long removedIterAddr = removeNextIterator(prevIterAddr);
                if (removedIterAddr == 0) {
                    currentIterAddr = 0;
                    // removeNextIterator removed currentIterAddr from the circular list.
                    // If it returned 0, it implies that nextIterAddr == currentIterAddr (we got it from
                    // the circular list of size one). So, there's no actual next iterator.
                    if (!masterHasNext) {
                        // No more iterators, no more master rows, we're all done.
                        prevIterAddr = 0;
                        return false;
                    }
                    // Activate the next master row, and use this iterator as the beginning of a new circular list.
                    nextIterAddr = activateMasterRow();
                    prevIterAddr = nextIterAddr;
                    nextIterNeedsSetup = false;
                } else {
                    nextIterNeedsSetup = true;
                }
            } else {
                prevIterAddr = currentIterAddr;
                nextIterNeedsSetup = true;
            }
            if (nextIterNeedsSetup) {
                if (masterHasNext && initialTimestampOfMasterRow() < peekNextTimestamp(nextIterAddr)) {
                    nextIterAddr = activateMasterRow();
                } else {
                    gotoNextRow(nextIterAddr);
                }
            }
            currentIterAddr = nextIterAddr;
            emitJoinRecord(iter_masterRowId(currentIterAddr), currentRowNum(currentIterAddr));
            return true;
        }

        @Override
        public long preComputedStateSize() {
            return slaveRecordArray.size() + slaveRecordOffsets.size();
        }

        @Override
        public long size() {
            long sizeA = masterCursor.size();
            long sizeB = slaveRowCount;
            if (sizeA == -1 || sizeB == -1) {
                return -1;
            }
            final long result = sizeA * sizeB;
            return result < sizeA && sizeB > 0 ? Long.MAX_VALUE : result;
        }

        @Override
        public void toTop() {
            freeAllIteratorBlocks();
            resetLocalState();
            masterCursor.toTop();
            slaveRecordArray.toTop();
        }

        private static int block_decUsedSlotCount(long blockAddr) {
            int count = Unsafe.getUnsafe().getInt(blockAddr + BLOCK_OFFSET_USED_SLOT_COUNT);
            count--;
            Unsafe.getUnsafe().putInt(blockAddr + BLOCK_OFFSET_USED_SLOT_COUNT, count);
            return count;
        }

        private static void block_incUsedSlotCount(long blockAddr) {
            int count = Unsafe.getUnsafe().getInt(blockAddr + BLOCK_OFFSET_USED_SLOT_COUNT);
            Unsafe.getUnsafe().putInt(blockAddr + BLOCK_OFFSET_USED_SLOT_COUNT, count + 1);
        }

        private static long block_iteratorAddr(long blockAddr, int slot) {
            return blockAddr + BLOCK_HEADER_SIZE + ((long) slot * ITERATOR_SIZE);
        }

        private static long block_nextBlockAddr(long blockAddr) {
            return Unsafe.getUnsafe().getLong(blockAddr + BLOCK_OFFSET_NEXT_BLOCK_ADDR);
        }

        private static int block_nextFreeSlot(long blockAddr) {
            return Unsafe.getUnsafe().getInt(blockAddr + BLOCK_OFFSET_NEXT_FREE_SLOT);
        }

        private static void block_setNextBlockAddr(long blockAddr, long nextAddr) {
            Unsafe.getUnsafe().putLong(blockAddr + BLOCK_OFFSET_NEXT_BLOCK_ADDR, nextAddr);
        }

        private static void block_setNextFreeSlot(long blockAddr, int slot) {
            Unsafe.getUnsafe().putInt(blockAddr + BLOCK_OFFSET_NEXT_FREE_SLOT, slot);
        }

        private static long iter_masterRowId(long iterAddr) {
            return Unsafe.getUnsafe().getLong(iterAddr + ITERATOR_OFFSET_MASTER_ROW_ID);
        }

        private static long iter_masterTimestamp(long iterAddr) {
            return Unsafe.getUnsafe().getLong(iterAddr + ITERATOR_OFFSET_MASTER_TIMESTAMP);
        }

        private static long iter_nextIterAddr(long iterAddr) {
            return Unsafe.getUnsafe().getLong(iterAddr + ITERATOR_OFFSET_NEXT_ITER_ADDR);
        }

        private static int iter_nextSlaveRowNum(long iterAddr) {
            return Unsafe.getUnsafe().getInt(iterAddr + ITERATOR_OFFSET_NEXT_SLAVE_ROW_NUM);
        }

        private static long iter_nextTimestamp(long iterAddr) {
            return Unsafe.getUnsafe().getLong(iterAddr + ITERATOR_OFFSET_NEXT_TIMESTAMP);
        }

        private static int iter_offsetFromBlockStart(long iterAddr) {
            return Unsafe.getUnsafe().getInt(iterAddr + ITERATOR_OFFSET_OFFSET_FROM_BLOCK_START);
        }

        private static void iter_setMasterRowId(long iterAddr, long value) {
            Unsafe.getUnsafe().putLong(iterAddr + ITERATOR_OFFSET_MASTER_ROW_ID, value);
        }

        private static void iter_setMasterTimestamp(long iterAddr, long value) {
            Unsafe.getUnsafe().putLong(iterAddr + ITERATOR_OFFSET_MASTER_TIMESTAMP, value);
        }

        private static void iter_setNextIterAddr(long iterAddr, long value) {
            Unsafe.getUnsafe().putLong(iterAddr + ITERATOR_OFFSET_NEXT_ITER_ADDR, value);
        }

        private static void iter_setNextSlaveRowNum(long iterAddr, int value) {
            Unsafe.getUnsafe().putInt(iterAddr + ITERATOR_OFFSET_NEXT_SLAVE_ROW_NUM, value);
        }

        private static void iter_setNextTimestamp(long iterAddr, long value) {
            Unsafe.getUnsafe().putLong(iterAddr + ITERATOR_OFFSET_NEXT_TIMESTAMP, value);
        }

        private static void iter_setOffsetFromBlockStart(long iterAddr, int value) {
            Unsafe.getUnsafe().putInt(iterAddr + ITERATOR_OFFSET_OFFSET_FROM_BLOCK_START, value);
        }

        private long activateMasterRow() {
            isMasterHasNextPending = true;
            if (currentIterAddr != 0) {
                return postInsertNewIterator(currentIterAddr, masterRecord);
            }
            return createIterator(masterRecord);
        }

        private void advanceMasterIfPending() {
            if (currMasterRowId != -1) {
                masterCursor.recordAt(masterRecord, currMasterRowId);
            }
            if (isMasterHasNextPending) {
                masterHasNext = masterCursor.hasNext();
                if (masterHasNext) {
                    currMasterRowId = masterRecord.getRowId();
                }
                isMasterHasNextPending = false;
            }
        }

        private long block_alloc() {
            long blockAddr = Unsafe.malloc(BLOCK_SIZE, MemoryTag.NATIVE_DEFAULT);
            Unsafe.getUnsafe().setMemory(blockAddr, BLOCK_HEADER_SIZE, (byte) 0);
            return blockAddr;
        }

        private void block_free(long blockAddr) {
            Unsafe.free(blockAddr, BLOCK_SIZE, MemoryTag.NATIVE_DEFAULT);
        }

        private long createIterator(Record masterRecord) {
            // Get or create the last iterator block
            if (lastIteratorBlockAddr == 0) {
                // No iterator blocks so far. Allocate the first one.
                lastIteratorBlockAddr = block_alloc();
                firstIteratorBlockAddr = lastIteratorBlockAddr;
            }
            int nextFreeSlot = block_nextFreeSlot(lastIteratorBlockAddr);
            if (nextFreeSlot == ITERATORS_PER_BLOCK) {
                // Block is full, allocate a new one
                long newBlockAddr = block_alloc();
                block_setNextBlockAddr(lastIteratorBlockAddr, newBlockAddr);
                lastIteratorBlockAddr = newBlockAddr;
                nextFreeSlot = 0;
            }

            // Get the iterator address and initialize it
            long iterAddr = block_iteratorAddr(lastIteratorBlockAddr, nextFreeSlot);
            int offsetFromBlockStart = BLOCK_HEADER_SIZE + (nextFreeSlot * ITERATOR_SIZE);

            // Initialize iterator fields
            iter_setMasterRowId(iterAddr, masterRecord.getRowId());
            iter_setMasterTimestamp(iterAddr, masterRecord.getTimestamp(masterTimestampColumnIndex));
            iter_setNextIterAddr(iterAddr, iterAddr); // Initially points to itself (circular list of one)
            iter_setNextSlaveRowNum(iterAddr, 0);
            iter_setOffsetFromBlockStart(iterAddr, offsetFromBlockStart);

            // Compute and set the next timestamp
            gotoNextRow(iterAddr);

            // Update block metadata
            block_setNextFreeSlot(lastIteratorBlockAddr, nextFreeSlot + 1);
            block_incUsedSlotCount(lastIteratorBlockAddr);

            return iterAddr;
        }

        private int currentRowNum(long iterAddr) {
            return iter_nextSlaveRowNum(iterAddr) - 1;
        }

        private void discardIterator(long iterAddr) {
            long blockAddr = iterAddr - iter_offsetFromBlockStart(iterAddr);
            int newCount = block_decUsedSlotCount(blockAddr);
            if (newCount == 0) {
                assert blockAddr == firstIteratorBlockAddr : "blockAddr != firstIteratorBlockAddr";
                if (blockAddr != lastIteratorBlockAddr) {
                    long nextBlockAddr = block_nextBlockAddr(blockAddr);
                    block_free(blockAddr);
                    firstIteratorBlockAddr = nextBlockAddr;
                } else {
                    block_setNextFreeSlot(blockAddr, 0);
                }
            }
        }

        private void emitJoinRecord(long masterRowId, int slaveRowNum) {
            long slaveOffset = slaveRecordOffsets.getQuick(slaveRowNum);
            Record slaveRecord = slaveRecordArray.getRecordAt(slaveOffset);
            masterCursor.recordAt(masterRecord, masterRowId);
            joinRecord.of(masterRecord, slaveRecord);
            emittedRowCount++;
        }

        private void freeAllIteratorBlocks() {
            long blockAddr = firstIteratorBlockAddr;
            while (blockAddr != 0) {
                long nextBlockAddr = block_nextBlockAddr(blockAddr);
                Unsafe.free(blockAddr, BLOCK_SIZE, MemoryTag.NATIVE_DEFAULT);
                blockAddr = nextBlockAddr;
            }
            firstIteratorBlockAddr = 0;
            lastIteratorBlockAddr = 0;
            currentIterAddr = 0;
            prevIterAddr = 0;
        }

        private void gotoNextRow(long iterAddr) {
            int nextSlaveRowNum = iter_nextSlaveRowNum(iterAddr);
            assert nextSlaveRowNum < slaveRowCount : "nextSlaveRowNum >= slaveRowCount";
            nextSlaveRowNum++;
            iter_setNextSlaveRowNum(iterAddr, nextSlaveRowNum);
            if (nextSlaveRowNum == slaveRowCount) {
                iter_setNextTimestamp(iterAddr, Long.MIN_VALUE);
                return;
            }
            long slaveRecordOffset = slaveRecordOffsets.getQuick(nextSlaveRowNum);
            Record slaveRec = slaveRecordArray.getRecordAt(slaveRecordOffset);
            long slaveOffset = slaveRec.getLong(slaveSequenceColumnIndex);
            long masterTimestamp = iter_masterTimestamp(iterAddr);
            iter_setNextTimestamp(iterAddr, masterTimestamp + slaveOffset);
        }

        private long initialTimestampOfMasterRow() {
            long nextMasterTimestamp = masterRecord.getTimestamp(masterTimestampColumnIndex);
            return nextMasterTimestamp + firstSlaveTimeOffset;
        }

        private boolean isEmpty(long iterAddr) {
            return iter_nextSlaveRowNum(iterAddr) == slaveRowCount;
        }

        private long peekNextTimestamp(long iterAddr) {
            return iter_nextTimestamp(iterAddr);
        }

        private long postInsertNewIterator(long iterAddr, Record masterRecord) {
            long newIterAddr = createIterator(masterRecord);
            long nextIterAddr = iter_nextIterAddr(iterAddr);
            iter_setNextIterAddr(newIterAddr, nextIterAddr);
            iter_setNextIterAddr(iterAddr, newIterAddr);
            return newIterAddr;
        }

        private long removeNextIterator(long iterAddr) {
            long toRemoveAddr = iter_nextIterAddr(iterAddr);
            long nextAddr = iter_nextIterAddr(toRemoveAddr);
            iter_setNextIterAddr(iterAddr, nextAddr);
            discardIterator(toRemoveAddr);
            if (toRemoveAddr == iterAddr) {
                return 0;  // this is the only iterator in the list, and we just closed it
            }
            return nextAddr;
        }

        private void resetLocalState() {
            isMasterHasNextPending = true;
            masterHasNext = false;
            currMasterRowId = -1;
            emittedRowCount = 0;
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            this.masterCursor = masterCursor;
            this.masterRecord = masterCursor.getRecord();
            this.slaveCursor = slaveCursor;
            this.circuitBreaker = circuitBreaker;

            // Materialize the slave cursor into RecordArray to enable random access
            slaveRecordArray.clear();
            slaveRecordOffsets.clear();
            if (slaveCursor.size() > Integer.MAX_VALUE) {
                throw CairoException.critical(-1)
                        .put("markout horizon offset count too large [offsetCount=").put(slaveCursor.size())
                        .put(", maxCount=").put(Integer.MAX_VALUE)
                        .put(']');
            }
            final Record slaveRecord = slaveCursor.getRecord();
            int offsetCount = 0;
            while (slaveCursor.hasNext()) {
                if (offsetCount == Integer.MAX_VALUE) {
                    throw CairoException.critical(-1)
                            .put("markout horizon: reached the offset count limit without exhausting cursor [countLimit=")
                            .put(Integer.MAX_VALUE).put(']');
                }
                offsetCount++;
                circuitBreaker.statefulThrowExceptionIfTripped();
                long offset = slaveRecordArray.put(slaveRecord);
                slaveRecordOffsets.add(offset);
            }
            slaveRecordArray.toTop();
            slaveRowCount = slaveRecordOffsets.size();

            // Cache the first slave's offset value to avoid repositioning recordC later
            if (slaveRowCount > 0) {
                long firstSlaveRecordOffset = slaveRecordOffsets.getQuick(0);
                Record firstSlaveRecord = slaveRecordArray.getRecordAt(firstSlaveRecordOffset);
                firstSlaveTimeOffset = firstSlaveRecord.getLong(slaveSequenceColumnIndex);
            } else {
                firstSlaveTimeOffset = 0;
            }

            // Reset iterator state for new execution
            // Free any existing iterator blocks from previous execution
            freeAllIteratorBlocks();
            resetLocalState();
        }
    }
}
