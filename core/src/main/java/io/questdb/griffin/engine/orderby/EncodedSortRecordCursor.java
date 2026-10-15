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

package io.questdb.griffin.engine.orderby;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordBlock;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

class EncodedSortRecordCursor implements DelegatingRecordCursor {
    // the most rows of a block over the sorted rows, see peekRecordBlock()
    private static final int SORTED_BLOCK_MAX_ROWS = 64 * 1024;
    private final SortKeyEncoder encoder;
    private final DirectLongList entryMem;
    private final MemoryCARW keyHeap;
    private final long maxEntryMemBytes;
    private final long parallelThreshold;
    private final RecordChain recordChain;
    private RecordCursor baseCursor;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private long count;
    // the previous execution's entry count, a capacity hint for the next
    private long lastCount;
    // the entry count at which this execution grows its entries towards the previous one's, -1
    // for none
    private long hintAt = -1;
    // the sample the key spread reads, see SortKeyEncoder.spreadKeyWords()
    private long[] spreadSample;
    private long lastChainBytes;
    // the value sizes of the chain's columns when every one is of a fixed-size, non-symbol type
    // the chain lays out at a fixed offset, so that a block of the base's rows is copied into it
    // column by column; null otherwise
    private final int[] blockColumnSizes;
    // the chain ordinals of a sorted block's rows, see peekRecordBlock(); null until the first block
    private DirectLongList sortedBlockRows;
    // the LSD sort's scratch, see sortLsd(); null until the first
    private DirectLongList lsdScratch;
    private SortedBlock sortedBlock;
    private MemoryTracker memoryTracker;
    private long currentAddr;
    private long endAddr;
    private int entrySize;
    private boolean isOpen;
    private boolean isSorted;
    private SortKeyType keyType;
    private int longsPerEntry;
    private int rowIdOffset;
    private long startAddr;

    public EncodedSortRecordCursor(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            IntList sortColumnFilter,
            RecordSink recordSink
    ) {
        try {
            this.encoder = new SortKeyEncoder(metadata, sortColumnFilter);
            this.entryMem = new DirectLongList(16 * 1024, MemoryTag.NATIVE_DEFAULT, true); // 128KB
            this.maxEntryMemBytes = Math.min(
                    configuration.getSqlSortKeyMaxBytes(),
                    SortKeyEncoder.MAX_ENTRY_HEAP_BYTES
            );
            this.parallelThreshold = configuration.getSqlSortEncodedParallelThreshold();
            final long keyHeapPageSize = configuration.getSqlSortKeyPageSize();
            this.keyHeap = new MemoryCARWImpl(
                    keyHeapPageSize,
                    (int) Math.min(Integer.MAX_VALUE, maxEntryMemBytes / Numbers.ceilPow2(keyHeapPageSize) + 1),
                    MemoryTag.NATIVE_DEFAULT,
                    PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath()
            );
            final long valuePageSize = configuration.getSqlSortValuePageSize();
            // RecordChain ceilPow2's the page before allocating; divide by the rounded unit to honor the cap.
            final long valueMaxPagesFromBytes = Math.max(1L, configuration.getSqlSortValueMaxBytes() / Numbers.ceilPow2(valuePageSize));
            this.recordChain = new RecordChain(
                    metadata,
                    recordSink,
                    valuePageSize,
                    (int) Math.min(valueMaxPagesFromBytes, Integer.MAX_VALUE),
                    PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_BYTES.getPropertyPath()
            );
            this.blockColumnSizes = blockColumnSizes(metadata);
            this.isOpen = true;
        } finally {
            if (!this.isOpen) forceClose();
        }
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            forceClose();
        }
    }

    @Override
    public Record getRecord() {
        return recordChain.getRecord();
    }

    @Override
    public Record getRecordB() {
        return recordChain.getRecordB();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return baseCursor.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (!isSorted) {
            buildAndSort();
            isSorted = true;
        }
        if (currentAddr < endAddr) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            long chainOffset = Unsafe.getLong(currentAddr);
            currentAddr += entrySize;
            recordChain.recordAt(recordChain.getRecord(), chainOffset);
            return true;
        }
        return false;
    }

    /**
     * The next sorted rows, when every column is of a fixed size and not a SYMBOL (see
     * {@link #blockColumnSizes}): the chain's records then lie at a constant stride from its
     * start, so a block offers each column at the chain's first record's value, at the stride, and
     * gathered by the rows' chain ordinals in sorted order.
     */
    @Override
    public RecordBlock peekRecordBlock(int maxRows) {
        if (blockColumnSizes == null) {
            return null;
        }
        if (!isSorted) {
            buildAndSort();
            isSorted = true;
        }
        final long remaining = (endAddr - currentAddr) / entrySize;
        // a block holds at least one row
        if (remaining < 1 || maxRows < 1) {
            return null;
        }
        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
        final int n = (int) Math.min(Math.min(remaining, maxRows), SORTED_BLOCK_MAX_ROWS);
        final long stride = recordChain.getFixedRecordStride();
        if (sortedBlockRows == null) {
            // charged to the query, as the entries and the chain are; freed by close()
            sortedBlockRows = new DirectLongList(n, MemoryTag.NATIVE_DEFAULT, true);
            sortedBlockRows.setMemoryTracker(memoryTracker);
            sortedBlockRows.reopen();
        }
        if (sortedBlock == null) {
            sortedBlock = new SortedBlock();
        }
        sortedBlockRows.clear();
        sortedBlockRows.ensureCapacity(n);
        final long rowsAddr = sortedBlockRows.getAddress();
        for (int r = 0; r < n; r++) {
            final long chainOffset = Unsafe.getLong(currentAddr + (long) r * entrySize);
            // a chain of fixed-size records only, appended one after another from offset 0
            assert chainOffset % stride == 0;
            Unsafe.putLong(rowsAddr + ((long) r << 3), chainOffset / stride);
        }
        sortedBlockRows.setPos(n);
        sortedBlock.of(n, stride);
        return sortedBlock;
    }

    @Override
    public void skipRecordBlock(int rowCount) {
        assert currentAddr + (long) rowCount * entrySize <= endAddr;
        currentAddr += (long) rowCount * entrySize;
    }

    @Override
    public boolean supportsRecordBlocks() {
        return blockColumnSizes != null;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return baseCursor.newSymbolTable(columnIndex);
    }

    @Override
    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        // The tracker is rebound unconditionally below (outside the !isOpen guard) because
        // the ctor opens eagerly with isOpen=true, so an inside-guard bind would leave the
        // first query untracked. A second of() without an intervening close() would then
        // rebind onto still-charged backing and underflow the per-query counter on free.
        // close()/forceClose() null baseCursor, so a null field here means fresh-or-closed.
        assert this.baseCursor == null : "of() without intervening close(): rebinding the memory tracker would underflow the per-query counter";
        this.baseCursor = baseCursor;
        // Wire only the row-count-scaled allocators; keyHeap stays global-counter only as it is
        // bounded by maxEntryMemBytes (see the throwLimitOverflow check below).
        entryMem.setMemoryTracker(executionContext.getMemoryTracker());
        memoryTracker = executionContext.getMemoryTracker();
        recordChain.setMemoryTracker(executionContext.getMemoryTracker());
        if (!isOpen) {
            isOpen = true;
            entryMem.reopen();
        } else {
            recordChain.clear();
        }
        recordChain.setSymbolTableResolver(baseCursor);
        keyType = encoder.init(baseCursor);
        assert keyType != SortKeyType.UNSUPPORTED;
        if (keyType.isVariable()) {
            encoder.setKeyHeap(keyHeap);
        }
        entrySize = keyType.entrySize();
        rowIdOffset = keyType.rowIdOffset();
        longsPerEntry = entrySize / Long.BYTES;
        circuitBreaker = executionContext.getCircuitBreaker();
        isSorted = false;
        count = 0;
    }

    @Override
    public long preComputedStateSize() {
        return RecordCursor.fromBool(isSorted);
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        recordChain.recordAt(record, atRowId);
    }

    @Override
    public long size() {
        return baseCursor.size();
    }

    @Override
    public void toTop() {
        currentAddr = startAddr;
        endAddr = startAddr + count * entrySize;
    }

    private void buildAndSort() {
        // Consult the breaker before consuming the base, so an empty base scan still observes cancellation.
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        final boolean isVariable = keyType.isVariable();
        long estimatedSize = baseCursor.size();
        long maxEntries = maxEntryMemBytes / entrySize;
        if (estimatedSize > 0) {
            if (estimatedSize > maxEntries) {
                throwLimitOverflow();
            }
            entryMem.setCapacity(estimatedSize * longsPerEntry);
        }
        // A size the base does not know is guessed from the previous execution's, see
        // EncodedSortCapacityHint
        hintAt = -1;
        if (estimatedSize <= 0 && lastCount > 0 && !isVariable) {
            final long start = EncodedSortCapacityHint.start(lastCount, maxEntries);
            entryMem.setCapacity(start * longsPerEntry);
            hintAt = Math.min(lastCount, maxEntries) > start ? start : -1;
        }

        entryMem.clear();
        count = 0;
        Record record = baseCursor.getRecord();
        if (isVariable) {
            keyHeap.close();
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                long chainOffset = recordChain.put(record, -1L);
                entryMem.ensureCapacity(longsPerEntry);
                long addr = entryMem.getAppendAddress();
                encoder.encode(record, addr, chainOffset);
                entryMem.skip(longsPerEntry);
                count++;
                if (count * entrySize + keyHeap.getAppendOffset() > maxEntryMemBytes) {
                    throwLimitOverflow();
                }
            }
        } else if (blockColumnSizes != null && baseCursor.supportsRecordBlocks()) {
            copyBlocks(record, maxEntries);
        } else if (estimatedSize > 0) {
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                long chainOffset = recordChain.put(record, -1L);
                long addr = entryMem.getAppendAddress();
                encoder.encode(record, addr, chainOffset);
                entryMem.skip(longsPerEntry);
                count++;
            }
        } else {
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                if (count >= maxEntries) {
                    throwLimitOverflow();
                }
                long chainOffset = recordChain.put(record, -1L);
                entryMem.ensureCapacity(longsPerEntry);
                long addr = entryMem.getAppendAddress();
                encoder.encode(record, addr, chainOffset);
                entryMem.skip(longsPerEntry);
                if (++count == hintAt) {
                    applyCapacityHint(maxEntries);
                }
            }
        }
        // Success-path free of the encoder's rank maps; a mid-build throw leaves them
        // for close(). The cursor is not retryable: buildAndSort resets state at entry.
        Misc.free(encoder);

        if (count > 1) {
            if (isVariable) {
                long heapAddr = keyHeap.getAppendOffset() == 0 ? 0 : keyHeap.addressOf(0);
                Vect.sortEncodedVarEntries(entryMem.getAddress(), count, heapAddr, parallelThreshold);
            } else if (keyType.keyLength() != Long.BYTES || count < parallelThreshold || !sortLsd()) {
                if (keyType.keyLength() == Long.BYTES && count >= parallelThreshold) {
                    if (spreadSample == null) {
                        spreadSample = new long[SortKeyEncoder.SPREAD_SAMPLE_SIZE];
                    }
                    SortKeyEncoder.spreadKeyWords(entryMem.getAddress(), count, spreadSample);
                }
                Vect.sortEncodedEntries(entryMem.getAddress(), count, keyType.keyLength() / Long.BYTES, parallelThreshold);
            }
            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottleOrYield();
        }
        if (isVariable) {
            // emit reads only chain offsets; the key heap is not needed past the sort
            keyHeap.close();
        }
        lastCount = count;
        lastChainBytes = recordChain.usedBytes();
        startAddr = entryMem.getAddress() + rowIdOffset;
        toTop();
    }

    private static int[] blockColumnSizes(RecordMetadata metadata) {
        final int n = metadata.getColumnCount();
        final int[] sizes = new int[n];
        for (int c = 0; c < n; c++) {
            sizes[c] = switch (ColumnType.tagOf(metadata.getColumnType(c))) {
                case ColumnType.BOOLEAN, ColumnType.BYTE -> 1;
                case ColumnType.SHORT, ColumnType.CHAR -> 2;
                case ColumnType.INT, ColumnType.FLOAT -> 4;
                case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.DOUBLE -> 8;
                default -> -1;
            };
            if (sizes[c] < 0) {
                return null;
            }
        }
        return n > 0 ? sizes : null;
    }

    /**
     * Fills the chain and the entries from the base's record blocks: a block's rows are appended
     * to the chain together and their values copied in column by column, then their keys are
     * encoded from the chain's records. The rows of a block that does not lay a column out in
     * memory are taken one by one, as without blocks.
     */
    private void copyBlocks(Record record, long maxEntries) {
        final int columnCount = blockColumnSizes.length;
        final long stride = recordChain.getFixedRecordStride();
        final Record chainRecord = recordChain.getRecordB();
        while (true) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            final RecordBlock block = baseCursor.peekRecordBlock(4096);
            if (block == null) {
                if (!baseCursor.hasNext()) {
                    return;
                }
                putRow(record, maxEntries);
                continue;
            }
            final int n = block.getRowCount();
            boolean laidOut = true;
            for (int c = 0; c < columnCount; c++) {
                if (block.getColumnAddress(c) == 0) {
                    laidOut = false;
                    break;
                }
            }
            if (!laidOut) {
                // the block's rows are the next n rows of the cursor
                for (int r = 0; r < n && baseCursor.hasNext(); r++) {
                    putRow(record, maxEntries);
                }
                continue;
            }
            if (count + n > maxEntries) {
                throwLimitOverflow();
            }
            final long first = recordChain.appendFixedRecords(-1L, n);
            final long base = recordChain.addressOf(first);
            for (int c = 0; c < columnCount; c++) {
                final long src = block.getColumnAddress(c);
                final long srcStride = block.getColumnStride(c);
                final long rowIndexes = block.getColumnRowIndexesAddress(c);
                final long dst = base + recordChain.getOffsetOfColumn(0, c);
                switch (blockColumnSizes[c]) {
                    case 1:
                        for (int r = 0; r < n; r++) {
                            final long i = rowIndexes != 0 ? Unsafe.getLong(rowIndexes + ((long) r << 3)) : r;
                            Unsafe.putByte(dst + r * stride, Unsafe.getByte(src + i * srcStride));
                        }
                        break;
                    case 2:
                        for (int r = 0; r < n; r++) {
                            final long i = rowIndexes != 0 ? Unsafe.getLong(rowIndexes + ((long) r << 3)) : r;
                            Unsafe.putShort(dst + r * stride, Unsafe.getShort(src + i * srcStride));
                        }
                        break;
                    case 4:
                        for (int r = 0; r < n; r++) {
                            final long i = rowIndexes != 0 ? Unsafe.getLong(rowIndexes + ((long) r << 3)) : r;
                            Unsafe.putInt(dst + r * stride, Unsafe.getInt(src + i * srcStride));
                        }
                        break;
                    default:
                        for (int r = 0; r < n; r++) {
                            final long i = rowIndexes != 0 ? Unsafe.getLong(rowIndexes + ((long) r << 3)) : r;
                            Unsafe.putLong(dst + r * stride, Unsafe.getLong(src + i * srcStride));
                        }
                        break;
                }
            }
            baseCursor.skipRecordBlock(n);
            entryMem.ensureCapacity((long) n * longsPerEntry);
            if (longsPerEntry == 2 && encoder.encodeFixed8Strided(
                    base + recordChain.getOffsetOfColumn(0, encoder.getFirstKeyColumnIndex()),
                    stride,
                    n,
                    entryMem.getAppendAddress(),
                    first
            )) {
                // the keys straight from the block's chain records, see SortKeyEncoder.encodeFixed8Strided()
                entryMem.skip((long) n * longsPerEntry);
            } else {
                for (int r = 0; r < n; r++) {
                    final long chainOffset = first + r * stride;
                    recordChain.recordAt(chainRecord, chainOffset);
                    encoder.encode(chainRecord, entryMem.getAppendAddress(), chainOffset);
                    entryMem.skip(longsPerEntry);
                }
            }
            count += n;
            if (hintAt > 0 && count >= hintAt) {
                applyCapacityHint(maxEntries);
            }
        }
    }

    private void putRow(Record record, long maxEntries) {
        if (count >= maxEntries) {
            throwLimitOverflow();
        }
        final long chainOffset = recordChain.put(record, -1L);
        entryMem.ensureCapacity(longsPerEntry);
        encoder.encode(record, entryMem.getAppendAddress(), chainOffset);
        entryMem.skip(longsPerEntry);
        if (++count == hintAt) {
            applyCapacityHint(maxEntries);
        }
    }

    private void applyCapacityHint(long maxEntries) {
        final long entries = EncodedSortCapacityHint.next(count, lastCount, maxEntries);
        entryMem.setCapacity(Math.max(entryMem.getCapacity(), entries * longsPerEntry));
        recordChain.reserve(EncodedSortCapacityHint.chainBytes(entries, lastCount, lastChainBytes));
        hintAt = entries < Math.min(lastCount, maxEntries) ? entries : -1;
    }

    // The stable parallel LSD sort of the entries (one key word, chain offsets ascending as appended),
    // see Vect.sortEncodedEntriesLsd(); false leaves them to the MSD sort. Its scratch, as many entries
    // again, is charged to the query and freed once the entries are sorted.
    private boolean sortLsd() {
        if (lsdScratch == null) {
            lsdScratch = new DirectLongList(count * longsPerEntry, MemoryTag.NATIVE_DEFAULT, true);
        }
        lsdScratch.setMemoryTracker(memoryTracker);
        lsdScratch.reopen();
        try {
            lsdScratch.ensureCapacity(count * longsPerEntry);
            return Vect.sortEncodedEntriesLsd(entryMem.getAddress(), count, lsdScratch.getAddress());
        } finally {
            lsdScratch.close();
        }
    }

    private void forceClose() {
        lsdScratch = Misc.free(lsdScratch);
        sortedBlockRows = Misc.free(sortedBlockRows);
        Misc.free(entryMem);
        Misc.free(keyHeap);
        Misc.free(encoder);
        Misc.free(recordChain);
        baseCursor = Misc.free(baseCursor);
    }

    private void throwLimitOverflow() {
        throw LimitOverflowException.instance()
                .put("limit of ").put(maxEntryMemBytes)
                .put(" memory exceeded in EncodedSort (raise ")
                .put(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath())
                .put(')');
    }

    // the next sorted rows, see peekRecordBlock()
    private class SortedBlock implements RecordBlock {
        private int rowCount;
        private long stride;

        @Override
        public long getColumnAddress(int columnIndex) {
            return recordChain.addressOf(0) + recordChain.getOffsetOfColumn(0, columnIndex);
        }

        @Override
        public long getColumnStride(int columnIndex) {
            return stride;
        }

        @Override
        public Record getRecordAt(int row) {
            final Record record = recordChain.getRecord();
            recordChain.recordAt(record, sortedBlockRows.get(row) * stride);
            return record;
        }

        @Override
        public int getRowCount() {
            return rowCount;
        }

        @Override
        public long getRowIndexesAddress() {
            return sortedBlockRows.getAddress();
        }

        void of(int rowCount, long stride) {
            this.rowCount = rowCount;
            this.stride = stride;
        }
    }
}
