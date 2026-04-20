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

package io.questdb.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Sorted native-memory staging buffer that holds live view rows until they are
 * old enough to publish past the view's LAG window.
 * <p>
 * Rows are stored in a columnar {@link InMemoryTable} in arrival order, with a parallel
 * native index of 16-byte {@code (timestamp, localRowId)} pairs matching the layout
 * {@link Vect#radixSortLongIndexAscInPlace} operates on. On each {@link #drain(long)}:
 * <ul>
 *     <li>The index is radix-sorted in place.</li>
 *     <li>A binary search splits it at the watermark; rows with {@code ts <= watermark}
 *         are emitted in timestamp-ascending order via the returned cursor.</li>
 *     <li>On cursor close, retained rows (ts > watermark) are compacted into a
 *         secondary {@link InMemoryTable}; the two buffers then swap so subsequent
 *         appends keep writing dense, zero-based row ids.</li>
 * </ul>
 * After a drain, the watermark is recorded so that any subsequently-arriving row
 * with {@code ts <= lastDrainedWatermark} can be counted as "late" via
 * {@link #getLateRowCount()} — the window function has already emitted state for
 * that timestamp range.
 */
public class MergeBuffer implements QuietCloseable {
    private static final int INDEX_ENTRY_BYTES = 2 * Long.BYTES;
    private static final long INDEX_PAGE_SIZE = 64 * 1024;

    private final DrainCursor drainCursor = new DrainCursor();
    private final LiveViewRecord record;
    private final MemoryCARW sortIndex;
    private final MemoryCARW sortScratch;
    // One StaticSymbolTable per SYMBOL column, dereferencing the current {@code pending}
    // buffer's interned strings. Entry is null for non-SYMBOL columns. Survives the
    // pending/retained swap because lookups go through {@link #pending} at call time.
    private final ObjList<PendingSymbolTable> symbolTables;
    private final int timestampColumnIndex;
    private long lastDrainedWatermark = Long.MIN_VALUE;
    private long lateRowCount;
    private long maxTsSeen = Long.MIN_VALUE;
    private InMemoryTable pending = new InMemoryTable();
    private InMemoryTable retained = new InMemoryTable();

    public MergeBuffer(RecordMetadata metadata) {
        this.timestampColumnIndex = metadata.getTimestampIndex();
        if (timestampColumnIndex < 0) {
            throw new IllegalArgumentException("live view requires a designated timestamp column");
        }
        pending.init(metadata);
        retained.init(metadata);
        // Share symbol dictionaries so that the pending/retained swap during drain
        // compaction does not reassign symbol keys for already-buffered rows; the
        // window function's partition state would otherwise be invalidated.
        retained.shareSymbolTablesWith(pending);
        this.record = new LiveViewRecord(pending);
        this.sortIndex = Vm.getCARWInstance(INDEX_PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
        this.sortScratch = Vm.getCARWInstance(INDEX_PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);

        int columnCount = metadata.getColumnCount();
        this.symbolTables = new ObjList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            if (ColumnType.tagOf(metadata.getColumnType(i)) == ColumnType.SYMBOL) {
                symbolTables.extendAndSet(i, new PendingSymbolTable(i));
            } else {
                symbolTables.extendAndSet(i, null);
            }
        }
    }

    /**
     * Appends a row from the cursor's current record. Returns {@code true} if the
     * row arrived "late" — i.e. its timestamp falls at or below the watermark of
     * the most recent drain, meaning the window function has already processed
     * that time range.
     */
    public boolean addRow(Record record) {
        long ts = record.getTimestamp(timestampColumnIndex);
        long rowId = pending.getRowCount();
        pending.appendRow(record);
        long addr = sortIndex.appendAddressFor(INDEX_ENTRY_BYTES);
        Unsafe.getUnsafe().putLong(addr, ts);
        Unsafe.getUnsafe().putLong(addr + Long.BYTES, rowId);
        if (ts > maxTsSeen) {
            maxTsSeen = ts;
        }
        if (ts <= lastDrainedWatermark) {
            lateRowCount++;
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        pending = Misc.free(pending);
        retained = Misc.free(retained);
        Misc.free(sortIndex);
        Misc.free(sortScratch);
    }

    /**
     * Sorts the index and returns a single-use cursor over rows with
     * {@code ts <= watermark} in timestamp-ascending order. Call {@link RecordCursor#close()}
     * to trigger compaction of retained rows before the next {@link #addRow} or
     * {@link #drain} call.
     */
    public RecordCursor drain(long watermark) {
        long count = pending.getRowCount();
        if (count > 0) {
            sortScratch.jumpTo(count * INDEX_ENTRY_BYTES);
            Vect.radixSortLongIndexAscInPlace(
                    sortIndex.getPageAddress(0),
                    count,
                    sortScratch.addressOf(0)
            );
        }
        long split = findUpperBound(count, watermark);
        if (watermark > lastDrainedWatermark) {
            lastDrainedWatermark = watermark;
        }
        drainCursor.of(split, count);
        return drainCursor;
    }

    public long getLastDrainedWatermark() {
        return lastDrainedWatermark;
    }

    public long getLateRowCount() {
        return lateRowCount;
    }

    public long getMaxTsSeen() {
        return maxTsSeen;
    }

    public boolean isEmpty() {
        return pending.getRowCount() == 0;
    }

    /**
     * Clears all buffered state, including symbol dictionaries. The next
     * {@link #addRow} starts a fresh LAG cycle with no history.
     */
    public void reset() {
        // pending.clear() drops the shared symbol dictionaries; clear retained's rows only
        // so we don't free the same ObjList twice.
        pending.clear();
        retained.clearRows();
        sortIndex.jumpTo(0);
        maxTsSeen = Long.MIN_VALUE;
        lastDrainedWatermark = Long.MIN_VALUE;
        lateRowCount = 0;
    }

    public long size() {
        return pending.getRowCount();
    }

    /**
     * Compacts retained rows (sorted indices [from, to)) into the secondary buffer
     * and swaps it with {@code pending}, then rewrites the sort index to cover the
     * retained rows only. The index entries produced here are already in
     * timestamp-ascending order — they are a prefix of the freshly-sorted index —
     * so no re-sort is needed until new rows are appended.
     */
    private void compactRetained(long from, long to) {
        if (from >= to) {
            pending.clearRows();
            sortIndex.jumpTo(0);
            return;
        }
        if (from == 0) {
            // Nothing drained.
            return;
        }
        retained.clearRows();
        long indexAddr = sortIndex.getPageAddress(0);
        for (long i = from; i < to; i++) {
            long rowId = Unsafe.getUnsafe().getLong(indexAddr + i * INDEX_ENTRY_BYTES + Long.BYTES);
            record.setRow(rowId);
            retained.appendRow(record);
        }
        // Swap pending <-> retained. The shared LiveViewRecord must track the new pending.
        InMemoryTable tmp = pending;
        pending = retained;
        retained = tmp;
        record.setTable(pending);

        // Rewrite sortIndex to (ts, newRowId) for the retained rows in sorted order.
        // Read positions are ahead of write positions, so a forward in-place rewrite is safe.
        long retainedCount = to - from;
        for (long i = 0; i < retainedCount; i++) {
            long src = indexAddr + (from + i) * INDEX_ENTRY_BYTES;
            long dst = indexAddr + i * INDEX_ENTRY_BYTES;
            long ts = Unsafe.getUnsafe().getLong(src);
            Unsafe.getUnsafe().putLong(dst, ts);
            Unsafe.getUnsafe().putLong(dst + Long.BYTES, i);
        }
        sortIndex.jumpTo(retainedCount * INDEX_ENTRY_BYTES);
    }

    private long findUpperBound(long count, long watermark) {
        // Returns the first index i in [0, count) with ts(i) > watermark.
        long lo = 0;
        long hi = count;
        long addr = sortIndex.getPageAddress(0);
        while (lo < hi) {
            long mid = (lo + hi) >>> 1;
            long ts = Unsafe.getUnsafe().getLong(addr + mid * INDEX_ENTRY_BYTES);
            if (ts <= watermark) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /**
     * Static symbol table over the MergeBuffer's currently active {@code pending} buffer.
     * Symbol values are interned into the buffer's {@link ObjList} by {@link InMemoryTable#putSymbol};
     * keys are positions within that list. Resolves through the live {@code pending}
     * reference so that lookups stay valid after the drain-time pending/retained swap.
     */
    private class PendingSymbolTable implements StaticSymbolTable {
        private final int columnIndex;

        PendingSymbolTable(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCount() {
            ObjList<String> entries = pending.getSymbolTable(columnIndex);
            return entries != null ? entries.size() : 0;
        }

        @Override
        public int keyOf(CharSequence value) {
            if (value == null) {
                return SymbolTable.VALUE_IS_NULL;
            }
            ObjList<String> entries = pending.getSymbolTable(columnIndex);
            if (entries == null) {
                return SymbolTable.VALUE_NOT_FOUND;
            }
            for (int i = 0, n = entries.size(); i < n; i++) {
                if (entries.getQuick(i).contentEquals(value)) {
                    return i;
                }
            }
            return SymbolTable.VALUE_NOT_FOUND;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            if (key < 0) {
                return null;
            }
            ObjList<String> entries = pending.getSymbolTable(columnIndex);
            if (entries == null || key >= entries.size()) {
                return null;
            }
            return entries.getQuick(key);
        }
    }

    private class DrainCursor implements RecordCursor {
        private long cursor;
        private long drainCount;
        private long totalCount;

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            counter.add(drainCount - cursor);
            cursor = drainCount;
        }

        @Override
        public void close() {
            compactRetained(drainCount, totalCount);
            cursor = 0;
            drainCount = 0;
            totalCount = 0;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return symbolTables.getQuick(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (cursor < drainCount) {
                long rowId = Unsafe.getUnsafe().getLong(
                        sortIndex.getPageAddress(0) + cursor * INDEX_ENTRY_BYTES + Long.BYTES
                );
                record.setRow(rowId);
                cursor++;
                return true;
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return symbolTables.getQuick(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return totalCount;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            ((LiveViewRecord) record).setRow(atRowId);
        }

        @Override
        public long size() {
            return drainCount;
        }

        @Override
        public void toTop() {
            cursor = 0;
        }

        void of(long drainCount, long totalCount) {
            this.drainCount = drainCount;
            this.totalCount = totalCount;
            this.cursor = 0;
        }
    }
}
