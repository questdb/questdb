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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.RecordArray;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.table.AsyncMarkoutGroupByAtom;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

/**
 * Worker logic for parallel markout query execution.
 * <p>
 * This class implements the k-way merge algorithm that emits horizon rows in timestamp order,
 * enabling efficient forward-only ASOF JOIN traversal.
 * <p>
 * Each worker processes a batch of master rows, builds a circular list of iterators,
 * emits horizons in timestamp order, performs ASOF lookups, and aggregates results.
 */
public class MarkoutReducer {
    // Native memory layout constants
    private static final int BLOCK_HEADER_SIZE = 16;
    private static final int BLOCK_OFFSET_NEXT_BLOCK_ADDR = 0;    // long (8 bytes)
    private static final int BLOCK_OFFSET_NEXT_FREE_SLOT = 8;     // int (4 bytes)
    private static final int BLOCK_OFFSET_USED_SLOT_COUNT = 12;   // int (4 bytes)
    private static final int ITERATORS_PER_BLOCK = 1024;
    private static final int ITERATOR_OFFSET_MASTER_ROW_INDEX = 0;              // int (4 bytes)
    private static final int ITERATOR_OFFSET_MASTER_TIMESTAMP = 8;         // long (8 bytes)
    private static final int ITERATOR_OFFSET_NEXT_ITER_ADDR = 16;          // long (8 bytes)
    private static final int ITERATOR_OFFSET_NEXT_SLAVE_ROW_NUM = 32;      // int (4 bytes)
    private static final int ITERATOR_OFFSET_NEXT_TIMESTAMP = 24;          // long (8 bytes)
    private static final int ITERATOR_OFFSET_OFFSET_FROM_BLOCK_START = 36; // int (4 bytes)
    private static final int ITERATOR_SIZE = 40;
    private static final long BLOCK_SIZE = BLOCK_HEADER_SIZE + (ITERATORS_PER_BLOCK * ITERATOR_SIZE);

    // Per-batch state
    private MasterRowBatch batch;
    // Iterator block management
    private long firstIteratorBlockAddr;
    private GroupByFunctionsUpdater functionUpdater;
    private long lastIteratorBlockAddr;
    private Map partialMap;
    // Shared slave data (read-only)
    private RecordArray slaveRecordArray;
    private LongList slaveRecordOffsets;
    private int slaveRowCount;
    private int slaveSequenceColumnIndex;

    // ==================== Block accessor methods ====================

    /**
     * Process a batch of master rows through the markout pipeline.
     * <p>
     * This method implements the k-way merge algorithm:
     * 1. Maintains a circular list of iterators, one per active master row
     * 2. Emits horizon rows in timestamp order
     * 3. For each horizon, performs ASOF JOIN lookup (TODO)
     * 4. Aggregates results into the partial map
     */
    public void reduce(
            AsyncMarkoutGroupByAtom atom,
            MasterRowBatch batch,
            int slotId,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        if (batch.size() == 0) {
            return;
        }

        // Initialize shared slave data
        this.slaveRecordArray = atom.getSlaveRecordArray();
        this.slaveRecordOffsets = atom.getSlaveRecordOffsets();
        this.slaveSequenceColumnIndex = atom.getSlaveSequenceColumnIndex();
        this.slaveRowCount = (int) atom.getSlaveRowCount();
        long firstSlaveTimeOffset = atom.getFirstSlaveTimeOffset();
        int masterTimestampColumnIndex = atom.getMasterTimestampColumnIndex();

        if (slaveRowCount == 0) {
            return;
        }

        // Initialize per-batch state
        this.batch = batch;
        this.partialMap = atom.getMap(slotId);
        this.functionUpdater = atom.getFunctionUpdater(slotId);

        // Initialize block management
        this.firstIteratorBlockAddr = 0;

        try {
            // Initialize with first master row
            int nextMasterRowIndex = 0;
            Record firstMasterRow = batch.getRowAt(nextMasterRowIndex++);
            long firstMasterTs = firstMasterRow.getTimestamp(masterTimestampColumnIndex);

            lastIteratorBlockAddr = block_alloc();
            firstIteratorBlockAddr = lastIteratorBlockAddr;

            long iter = createIterator(0, firstMasterTs);
            long prevIter = iter;

            // K-way merge loop
            while (true) {
                circuitBreaker.statefulThrowExceptionIfTripped();

                // Emit current row and aggregate
                emitAndAggregate(iter);

                // Advance iterator to next row
                gotoNextRow(iter);

                long nextIter = iter_nextIterAddr(iter);

                // Check if next master row should be activated
                if (nextMasterRowIndex < batch.size()) {
                    long nextIterTs = iter_nextTimestamp(nextIter);
                    Record pendingTrade = batch.getRowAt(nextMasterRowIndex);
                    long pendingInitialTs = pendingTrade.getTimestamp(masterTimestampColumnIndex) + firstSlaveTimeOffset;
                    // Insert if pending should come before next, OR if next is exhausted
                    if (nextIterTs == Long.MIN_VALUE || pendingInitialTs < nextIterTs) {
                        nextIter = insertIteratorAfter(iter, nextMasterRowIndex, pendingTrade.getTimestamp(masterTimestampColumnIndex));
                        nextMasterRowIndex++;
                    }
                }

                // Remove current iterator if exhausted
                if (isEmpty(iter)) {
                    nextIter = removeIterator(prevIter, iter);
                    if (nextIter == 0) {
                        break;  // All done
                    }
                } else {
                    prevIter = iter;
                }

                iter = nextIter;
            }
        } finally {
            freeAllBlocks();
        }
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

    private static long block_nextBlockAddr(long blockAddr) {
        return Unsafe.getUnsafe().getLong(blockAddr + BLOCK_OFFSET_NEXT_BLOCK_ADDR);
    }

    private static int block_nextFreeSlot(long blockAddr) {
        return Unsafe.getUnsafe().getInt(blockAddr + BLOCK_OFFSET_NEXT_FREE_SLOT);
    }

    private static void block_setNextBlockAddr(long blockAddr, long nextAddr) {
        Unsafe.getUnsafe().putLong(blockAddr + BLOCK_OFFSET_NEXT_BLOCK_ADDR, nextAddr);
    }

    // ==================== Iterator accessor methods ====================

    private static void block_setNextFreeSlot(long blockAddr, int slot) {
        Unsafe.getUnsafe().putInt(blockAddr + BLOCK_OFFSET_NEXT_FREE_SLOT, slot);
    }

    private static int iter_currentRowNum(long iterAddr) {
        return iter_nextSlaveRowNum(iterAddr) - 1;
    }

    private static int iter_masterRowIndex(long iterAddr) {
        return Unsafe.getUnsafe().getInt(iterAddr + ITERATOR_OFFSET_MASTER_ROW_INDEX);
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

    // ==================== Main algorithm ====================

    private static void iter_setTradeIndex(long iterAddr, int value) {
        Unsafe.getUnsafe().putInt(iterAddr + ITERATOR_OFFSET_MASTER_ROW_INDEX, value);
    }

    // ==================== Block management ====================

    private long block_alloc() {
        long blockAddr = Unsafe.malloc(BLOCK_SIZE, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(blockAddr, BLOCK_HEADER_SIZE, (byte) 0);
        return blockAddr;
    }

    private void block_free(long blockAddr) {
        Unsafe.free(blockAddr, BLOCK_SIZE, MemoryTag.NATIVE_DEFAULT);
    }

    /**
     * Compute and store the next timestamp based on current row number.
     * Sets Long.MIN_VALUE if exhausted.
     */
    private void computeNextTimestamp(long iterAddr) {
        int nextSlaveRowNum = iter_nextSlaveRowNum(iterAddr);

        if (nextSlaveRowNum >= slaveRowCount) {
            iter_setNextTimestamp(iterAddr, Long.MIN_VALUE);
            return;
        }

        long slaveRecordOffset = slaveRecordOffsets.getQuick(nextSlaveRowNum);
        Record slaveRec = slaveRecordArray.getRecordAt(slaveRecordOffset);
        long slaveOffset = slaveRec.getLong(slaveSequenceColumnIndex);
        long masterTimestamp = iter_masterTimestamp(iterAddr);
        iter_setNextTimestamp(iterAddr, masterTimestamp + slaveOffset);
    }

    // ==================== Iterator management ====================

    /**
     * Create a new iterator and add it to the block. Points to itself (circular list of one).
     */
    private long createIterator(int tradeIndex, long masterTimestamp) {
        int slotIndex = block_nextFreeSlot(lastIteratorBlockAddr);

        // Check if current block is full, allocate new one if needed
        if (slotIndex >= ITERATORS_PER_BLOCK) {
            long newBlockAddr = block_alloc();
            block_setNextBlockAddr(lastIteratorBlockAddr, newBlockAddr);
            lastIteratorBlockAddr = newBlockAddr;
            slotIndex = 0;
        }

        int offsetFromBlockStart = BLOCK_HEADER_SIZE + (slotIndex * ITERATOR_SIZE);
        long iterAddr = lastIteratorBlockAddr + offsetFromBlockStart;

        // Initialize iterator fields
        iter_setTradeIndex(iterAddr, tradeIndex);
        iter_setMasterTimestamp(iterAddr, masterTimestamp);
        iter_setNextIterAddr(iterAddr, iterAddr); // Points to itself
        iter_setNextSlaveRowNum(iterAddr, 0);
        iter_setOffsetFromBlockStart(iterAddr, offsetFromBlockStart);

        // Compute first timestamp
        computeNextTimestamp(iterAddr);

        // Update block metadata
        block_setNextFreeSlot(lastIteratorBlockAddr, slotIndex + 1);
        block_incUsedSlotCount(lastIteratorBlockAddr);

        return iterAddr;
    }

    private void discardIterator(long iterAddr) {
        long blockAddr = iterAddr - iter_offsetFromBlockStart(iterAddr);
        int usedCount = block_decUsedSlotCount(blockAddr);

        if (usedCount == 0) {
            // Block is empty
            if (blockAddr == firstIteratorBlockAddr && blockAddr == lastIteratorBlockAddr) {
                // This is the only block - reset it for reuse
                block_setNextFreeSlot(blockAddr, 0);
            } else if (blockAddr == firstIteratorBlockAddr) {
                // First block is empty but there are more blocks - free it and update firstIteratorBlockAddr
                long nextBlockAddr = block_nextBlockAddr(blockAddr);
                block_free(blockAddr);
                firstIteratorBlockAddr = nextBlockAddr;
            } else {
                // Non-first block is empty - find predecessor, update its link, and free this block
                long prevBlockAddr = firstIteratorBlockAddr;
                while (block_nextBlockAddr(prevBlockAddr) != blockAddr) {
                    prevBlockAddr = block_nextBlockAddr(prevBlockAddr);
                }
                long nextBlockAddr = block_nextBlockAddr(blockAddr);
                block_setNextBlockAddr(prevBlockAddr, nextBlockAddr);
                if (blockAddr == lastIteratorBlockAddr) {
                    lastIteratorBlockAddr = prevBlockAddr;
                }
                block_free(blockAddr);
            }
        }
    }

    private void emitAndAggregate(long currentIterAddr) {
        // Get current horizon row data
        Record tradeRecord = batch.getRowAt(iter_masterRowIndex(currentIterAddr));

        int slaveRowNum = iter_currentRowNum(currentIterAddr);
        long slaveOffset = slaveRecordOffsets.getQuick(slaveRowNum);
        Record slaveRecord = slaveRecordArray.getRecordAt(slaveOffset);

        // TODO: Add ASOF JOIN lookup here
        // For now, we just aggregate without the price lookup
        // Record priceRecord = asofLookup(pricesCursor, horizonTs, tradeRecord);

        // Create map key: (symbol, offset)
        // For now using a simplified key - actual implementation would use the configured key columns
        MapKey key = partialMap.withKey();
        // The actual key would be populated from trade record and slave record
        // key.putInt(tradeRecord.getInt(symbolColumnIndex));
        // key.putLong(slaveRecord.getLong(slaveSequenceColumnIndex));

        // For now, just use the slave offset as a key (simplified)
        key.putLong(slaveRecord.getLong(slaveSequenceColumnIndex));

        MapValue value = key.createValue();
        if (value.isNew()) {
            // Initialize aggregation
            functionUpdater.updateNew(value, null, 0);
        } else {
            // Update aggregation
            functionUpdater.updateExisting(value, null, 0);
        }
    }

    private void freeAllBlocks() {
        long blockAddr = firstIteratorBlockAddr;
        while (blockAddr != 0) {
            long nextBlockAddr = block_nextBlockAddr(blockAddr);
            block_free(blockAddr);
            blockAddr = nextBlockAddr;
        }
    }

    /**
     * Advance iterator to the next row and compute its timestamp.
     */
    private void gotoNextRow(long iterAddr) {
        int nextSlaveRowNum = iter_nextSlaveRowNum(iterAddr);
        iter_setNextSlaveRowNum(iterAddr, nextSlaveRowNum + 1);
        computeNextTimestamp(iterAddr);
    }

    /**
     * Insert a new iterator right after the given iterator in the circular list.
     */
    private long insertIteratorAfter(long iterAddr, int tradeIndex, long masterTimestamp) {
        long newIterAddr = createIterator(tradeIndex, masterTimestamp);
        // Insert into circular list after iterAddr
        long nextIterAddr = iter_nextIterAddr(iterAddr);
        iter_setNextIterAddr(newIterAddr, nextIterAddr);
        iter_setNextIterAddr(iterAddr, newIterAddr);
        return newIterAddr;
    }

    /**
     * Check if iterator is exhausted (no more rows to emit).
     */
    private boolean isEmpty(long iterAddr) {
        return iter_nextSlaveRowNum(iterAddr) >= slaveRowCount;
    }

    /**
     * Remove an iterator from the circular list.
     *
     * @return the next iterator, or 0 if this was the only one
     */
    private long removeIterator(long prevIterAddr, long toRemoveAddr) {
        long nextAddr = iter_nextIterAddr(toRemoveAddr);
        iter_setNextIterAddr(prevIterAddr, nextAddr);
        discardIterator(toRemoveAddr);
        if (toRemoveAddr == prevIterAddr) {
            return 0;  // This was the only iterator in the list
        }
        return nextAddr;
    }

}
