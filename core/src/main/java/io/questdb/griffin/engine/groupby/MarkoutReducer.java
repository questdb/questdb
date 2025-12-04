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
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.table.AsyncMarkoutGroupByAtom;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf16Sink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

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
    private static final Log LOG = LogFactory.getLog(MarkoutReducer.class);
    // Native memory layout constants
    private static final int BLOCK_HEADER_SIZE = 16;
    private static final int BLOCK_OFFSET_NEXT_BLOCK_ADDR = 0;    // long (8 bytes)
    private static final int BLOCK_OFFSET_NEXT_FREE_SLOT = 8;     // int (4 bytes)
    private static final int BLOCK_OFFSET_USED_SLOT_COUNT = 12;   // int (4 bytes)
    private static final int ITERATORS_PER_BLOCK = 1024;
    private static final int ITERATOR_OFFSET_MASTER_ROW_INDEX = 0;         // int (4 bytes)
    private static final int ITERATOR_OFFSET_MASTER_TIMESTAMP = 8;         // long (8 bytes)
    private static final int ITERATOR_OFFSET_NEXT_ITER_ADDR = 16;          // long (8 bytes)
    private static final int ITERATOR_OFFSET_NEXT_SEQUENCE_ROW_NUM = 32;   // int (4 bytes)
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
    // Shared sequence data (read-only)
    private RecordArray sequenceRecordArray;
    private LongList sequenceRecordOffsets;
    private int sequenceRowCount;
    private int sequenceColumnIndex;

    // ASOF JOIN lookup state
    private RecordCursor pricesCursor;
    private Record pricesRecord;
    private Record pricesRecordB;
    private Map asofJoinMap;
    private RecordSink masterKeyCopier;
    private RecordSink pricesKeyCopier;
    private int pricesTimestampIndex;
    private long lastPricesTimestamp;
    private long lastPricesRowId;
    private boolean hasBufferedPrice;  // True if we read a price but didn't store it yet

    // Combined record for aggregation - maps baseMetadata columns to source records
    // baseMetadata: [0: sec_offs (LONG), 1: price (DOUBLE)]
    // sequenceRecord: [0: usec_offs, 1: sec_offs]
    // pricesRecord: [0: price, 1: sym, 2: price_ts]
    private final CombinedRecord combinedRecord = new CombinedRecord();

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
        this.sequenceRowCount = (int) atom.getSequenceRowCount();
        if (sequenceRowCount == 0) {
            return;
        }
        this.sequenceRecordArray = atom.getSequenceRecordArray();
        this.sequenceRecordOffsets = atom.getSequenceRecordOffsets();
        this.sequenceColumnIndex = atom.getSequenceColumnIndex();
        long firstSequenceTimeOffset = atom.getFirstSequenceTimeOffset();
        int masterTimestampColumnIndex = atom.getMasterTimestampColumnIndex();
        this.batch = batch;
        this.partialMap = atom.getMap(slotId);
        this.functionUpdater = atom.getFunctionUpdater(slotId);
        this.firstIteratorBlockAddr = 0;

        // Initialize ASOF lookup state
        this.pricesCursor = atom.getPricesCursor();
        this.asofJoinMap = atom.getAsofJoinMap();
        this.masterKeyCopier = atom.getMasterKeyCopier();
        this.pricesKeyCopier = atom.getPricesKeyCopier();
        this.pricesTimestampIndex = atom.getPricesTimestampIndex();
        if (pricesCursor != null) {
            this.pricesRecord = pricesCursor.getRecord();
            this.pricesRecordB = pricesCursor.getRecordB();
        }
        this.lastPricesTimestamp = Long.MIN_VALUE;
        this.lastPricesRowId = Numbers.LONG_NULL;
        this.hasBufferedPrice = false;

        try {
            int nextMasterRowIndex = 0;
            Record firstMasterRow = batch.getRowAt(nextMasterRowIndex++);
            long firstMasterTs = firstMasterRow.getTimestamp(masterTimestampColumnIndex);

            lastIteratorBlockAddr = block_alloc();
            firstIteratorBlockAddr = lastIteratorBlockAddr;

            long iter = createIterator(0, firstMasterTs);
            long prevIter = iter;

            while (true) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                emitAndAggregate(iter);
                gotoNextRow(iter);
                long nextIter = iter_nextIterAddr(iter);
                if (nextMasterRowIndex < batch.size()) {
                    long nextIterTs = iter_nextTimestamp(nextIter);
                    long nextMasterTs = batch.getRowAt(nextMasterRowIndex).getTimestamp(masterTimestampColumnIndex);
                    long nextInitialTs = nextMasterTs + firstSequenceTimeOffset;
                    if (nextIterTs == Long.MIN_VALUE || nextInitialTs < nextIterTs) {
                        nextIter = insertIteratorAfter(iter, nextMasterRowIndex, nextMasterTs);
                        nextMasterRowIndex++;
                    }
                }
                if (isEmpty(iter)) {
                    // iterator still being empty here implies master batch is exhausted.
                    // If this is the last iterator, we're all done.
                    nextIter = removeIterator(prevIter, iter);
                    if (nextIter == 0) {
                        break;
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

    private static int iter_masterRowIndex(long iterAddr) {
        return Unsafe.getUnsafe().getInt(iterAddr + ITERATOR_OFFSET_MASTER_ROW_INDEX);
    }

    private static long iter_masterTimestamp(long iterAddr) {
        return Unsafe.getUnsafe().getLong(iterAddr + ITERATOR_OFFSET_MASTER_TIMESTAMP);
    }

    private static long iter_nextIterAddr(long iterAddr) {
        return Unsafe.getUnsafe().getLong(iterAddr + ITERATOR_OFFSET_NEXT_ITER_ADDR);
    }

    private static int iter_nextSequenceRowNum(long iterAddr) {
        return Unsafe.getUnsafe().getInt(iterAddr + ITERATOR_OFFSET_NEXT_SEQUENCE_ROW_NUM);
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

    private static void iter_setNextSequenceRowNum(long iterAddr, int value) {
        Unsafe.getUnsafe().putInt(iterAddr + ITERATOR_OFFSET_NEXT_SEQUENCE_ROW_NUM, value);
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
        int nextSequenceRowNum = iter_nextSequenceRowNum(iterAddr);

        if (nextSequenceRowNum >= sequenceRowCount) {
            iter_setNextTimestamp(iterAddr, Long.MIN_VALUE);
            return;
        }

        long sequenceRecordOffset = sequenceRecordOffsets.getQuick(nextSequenceRowNum);
        Record sequenceRec = sequenceRecordArray.getRecordAt(sequenceRecordOffset);
        long sequenceOffset = sequenceRec.getLong(sequenceColumnIndex);
        long masterTimestamp = iter_masterTimestamp(iterAddr);
        iter_setNextTimestamp(iterAddr, masterTimestamp + sequenceOffset);
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
        iter_setNextSequenceRowNum(iterAddr, 0);
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
        Record masterRecord = batch.getRowAt(iter_masterRowIndex(currentIterAddr));

        // nextSequenceRowNum is the current row to emit (we haven't called gotoNextRow yet)
        int sequenceRowNum = iter_nextSequenceRowNum(currentIterAddr);
        long sequenceOffset = sequenceRecordOffsets.getQuick(sequenceRowNum);
        Record sequenceRecord = sequenceRecordArray.getRecordAt(sequenceOffset);

        // Get horizon timestamp
        long horizonTs = iter_nextTimestamp(currentIterAddr);

        // ASOF JOIN lookup: find matching prices record
        Record matchedPricesRecord = null;
        if (asofJoinMap != null && pricesCursor != null && masterKeyCopier != null) {
            // Advance prices cursor forward-only up to horizon timestamp
            advancePricesCursorTo(horizonTs);

            // Look up master's join key
            MapKey lookupKey = asofJoinMap.withKey();
            lookupKey.put(masterRecord, masterKeyCopier);
            MapValue lookupValue = lookupKey.findValue();

            if (lookupValue != null) {
                long pricesRowId = lookupValue.getLong(0);
                if (pricesRowId != Numbers.LONG_NULL) {
                    pricesCursor.recordAt(pricesRecordB, pricesRowId);
                    matchedPricesRecord = pricesRecordB;
                }
            }
        }

        // Create map key for GROUP BY aggregation
        // The GROUP BY key column (sec_offs) is column 1 in the compiled sequence record
        // Column 0 is usec_offs (the timestamp offset for horizon computation)
        // TODO: Make this configurable based on actual GROUP BY columns
        MapKey key = partialMap.withKey();
        key.putLong(sequenceRecord.getLong(1));

        // Use combined record that maps baseMetadata columns to source records
        combinedRecord.of(sequenceRecord, matchedPricesRecord);

        MapValue value = key.createValue();
        if (value.isNew()) {
            functionUpdater.updateNew(value, combinedRecord, 0);
        } else {
            functionUpdater.updateExisting(value, combinedRecord, 0);
        }
    }

    /**
     * Advance prices cursor forward-only up to the given horizon timestamp.
     * Stores joinKey -> rowId mappings in the asofJoinMap.
     * <p>
     * This method maintains the cursor position between calls, so it can only
     * be used when horizons are emitted in strictly increasing timestamp order.
     */
    private void advancePricesCursorTo(long horizonTs) {
        // First, check if we have a buffered price that we can now store
        if (hasBufferedPrice) {
            if (lastPricesTimestamp <= horizonTs) {
                // Store the buffered price
                MapKey joinKey = asofJoinMap.withKey();
                joinKey.put(pricesRecord, pricesKeyCopier);
                MapValue joinValue = joinKey.createValue();
                joinValue.putLong(0, lastPricesRowId);
                hasBufferedPrice = false;
            } else {
                // Buffered price is still past the horizon, nothing more to do
                return;
            }
        }

        // Advance cursor forward-only
        while (pricesCursor.hasNext()) {
            // Note: Column order in factory metadata may differ from table definition
            // pricesTimestampIndex gives the timestamp column
            long priceTs = pricesRecord.getTimestamp(pricesTimestampIndex);
            long rowId = pricesRecord.getRowId();
            lastPricesTimestamp = priceTs;
            lastPricesRowId = rowId;

            if (priceTs <= horizonTs) {
                // Store joinKey -> rowId mapping
                MapKey joinKey = asofJoinMap.withKey();
                joinKey.put(pricesRecord, pricesKeyCopier);
                MapValue joinValue = joinKey.createValue();
                joinValue.putLong(0, rowId);
            } else {
                // Past the horizon, stop and mark as buffered
                hasBufferedPrice = true;
                break;
            }
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
        int nextSequenceRowNum = iter_nextSequenceRowNum(iterAddr);
        iter_setNextSequenceRowNum(iterAddr, nextSequenceRowNum + 1);
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
        return iter_nextSequenceRowNum(iterAddr) >= sequenceRowCount;
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

    /**
     * Combined record that maps baseMetadata column indices to source records.
     * <p>
     * This is a simplified implementation for the markout curve query pattern where:
     * - baseMetadata column 0 (sec_offs, LONG) maps to sequenceRecord column 1
     * - baseMetadata column 1 (price, DOUBLE) maps to pricesRecord column 0
     */
    private static class CombinedRecord implements Record {
        private Record sequenceRecord;
        private Record pricesRecord;

        void of(Record sequenceRecord, Record pricesRecord) {
            this.sequenceRecord = sequenceRecord;
            this.pricesRecord = pricesRecord;
        }

        @Override
        public long getLong(int col) {
            // Column 0 (sec_offs) comes from sequence record column 1
            if (col == 0 && sequenceRecord != null) {
                return sequenceRecord.getLong(1);
            }
            return 0;
        }

        @Override
        public double getDouble(int col) {
            // Column 1 (price) comes from prices record column 0
            if (col == 1 && pricesRecord != null) {
                return pricesRecord.getDouble(0);
            }
            return Double.NaN;
        }

        // Implement other Record methods as needed (returning defaults for unused types)
        @Override
        public int getInt(int col) { return 0; }

        @Override
        public byte getByte(int col) { return 0; }

        @Override
        public short getShort(int col) { return 0; }

        @Override
        public char getChar(int col) { return 0; }

        @Override
        public float getFloat(int col) { return Float.NaN; }

        @Override
        public boolean getBool(int col) { return false; }

        @Override
        public long getTimestamp(int col) { return Numbers.LONG_NULL; }

        @Override
        public long getDate(int col) { return Numbers.LONG_NULL; }

        @Override
        public @Nullable CharSequence getSymA(int col) { return null; }

        @Override
        public @Nullable CharSequence getSymB(int col) { return null; }

        @Override
        public @Nullable CharSequence getStrA(int col) { return null; }

        @Override
        public @Nullable CharSequence getStrB(int col) { return null; }

        @Override
        public int getStrLen(int col) { return -1; }

        @Override
        public long getRowId() { return Numbers.LONG_NULL; }

        @Override
        public void getLong256(int col, CharSink<?> sink) { }

        @Override
        public @Nullable Long256 getLong256A(int col) { return null; }

        @Override
        public @Nullable Long256 getLong256B(int col) { return null; }

        @Override
        public @Nullable BinarySequence getBin(int col) { return null; }

        @Override
        public long getBinLen(int col) { return -1; }

        @Override
        public byte getGeoByte(int col) { return 0; }

        @Override
        public short getGeoShort(int col) { return 0; }

        @Override
        public int getGeoInt(int col) { return 0; }

        @Override
        public long getGeoLong(int col) { return 0; }

        @Override
        public @Nullable Utf8Sequence getVarcharA(int col) { return null; }

        @Override
        public @Nullable Utf8Sequence getVarcharB(int col) { return null; }

        @Override
        public int getVarcharSize(int col) { return -1; }

        @Override
        public int getIPv4(int col) { return 0; }

        @Override
        public long getUpdateRowId() { return Numbers.LONG_NULL; }
    }
}
