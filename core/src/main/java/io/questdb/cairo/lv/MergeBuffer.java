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
 * Retention-bounded native-memory store of base-table rows feeding the live view.
 * Acts as both the LAG staging buffer (via the drain watermark) and the input-column
 * replay buffer for warm-path rollback (via {@link #replay}): one sorted store sized
 * to the RETENTION window, with the drain boundary expressed as a watermark rather
 * than an index position so a late-row re-sort cannot move the already-emitted region.
 * <p>
 * Storage layout:
 * <ul>
 *     <li>{@link #rows} — columnar {@link InMemoryTable} in arrival order.</li>
 *     <li>{@link #sortIndex} — native vector of 16-byte {@code (timestamp, physicalRowId)}
 *         pairs, one per row in {@link #rows}. Matches the layout
 *         {@link Vect#radixSortLongIndexAscInPlace} operates on.</li>
 *     <li>{@link #sortStart} — first live entry in the sort index; entries below it
 *         refer to physical rows evicted by {@link #applyRetention}, kept alive until
 *         {@link #compact} reclaims them.</li>
 *     <li>{@link #lastDrainedWatermark} — high water mark of the most recent
 *         successful drain. Rows with {@code ts <= lastDrainedWatermark} have
 *         already been emitted; rows at or below this watermark that arrive
 *         afterwards are counted as late via {@link #pendingLateCount}.</li>
 * </ul>
 * On each {@link #drain(long)}:
 * <ul>
 *     <li>If new rows arrived since the last drain, the live range
 *         {@code sortIndex[sortStart..count)} is radix-sorted in place.</li>
 *     <li>Binary search locates the first entry with {@code ts > lastDrainedWatermark}
 *         (drain floor) and the first entry with {@code ts > watermark} (drain
 *         ceiling); rows in between are emitted in timestamp-ascending order via the
 *         returned cursor.</li>
 *     <li>On cursor close (after a fully consumed iteration), {@code lastDrainedWatermark}
 *         advances to the drain's watermark. An aborted iteration leaves it untouched
 *         so the next drain re-emits the same range.</li>
 * </ul>
 * The ring-buffer semantic is driven by the caller: {@link #applyRetention} advances
 * {@link #sortStart} (logical eviction; no memmoves), and {@link #compact} periodically
 * rebuilds {@link #rows} and the sort index to reclaim the evicted prefix.
 */
public class MergeBuffer implements QuietCloseable {
    private static final int INDEX_ENTRY_BYTES = 2 * Long.BYTES;
    private static final long INDEX_PAGE_SIZE = 64 * 1024;

    private final DrainCursor drainCursor = new DrainCursor();
    private final LiveViewRecord record;
    private final MemoryCARW sortIndex;
    private final MemoryCARW sortScratch;
    // One StaticSymbolTable per SYMBOL column, dereferencing the current {@code rows}
    // buffer's interned strings. Entry is null for non-SYMBOL columns. Survives the
    // rows/compactScratch swap because lookups go through {@link #rows} at call time.
    private final ObjList<ActiveSymbolTable> symbolTables;
    private final int timestampColumnIndex;
    // Compaction scratch: takes the place of {@link #rows} during {@link #compact}.
    // Shares symbol dictionaries with {@link #rows} so SYMBOL keys are stable across
    // the post-compaction swap.
    private InMemoryTable compactScratch = new InMemoryTable();
    // Total entries currently written into the sort index. Grows monotonically
    // between compactions. Entries [sortStart, count) are live; [0, sortStart) refer
    // to physical rows that {@link #applyRetention} has logically evicted.
    private long count;
    // Watermark of the most recent successful drain; {@code Long.MIN_VALUE} before
    // the first drain. Rows with {@code ts <= lastDrainedWatermark} have been emitted
    // by a prior drain and must not be re-emitted on a subsequent hot-path drain.
    // Advanced only on drain cursor close after a fully-consumed iteration.
    private long lastDrainedWatermark = Long.MIN_VALUE;
    // Cumulative count of rows ever observed with {@code ts <= lastDrainedWatermark}
    // at append time. Reported via catalog introspection; never reset.
    private long lateRowCount;
    // Max timestamp observed across all rows ever added. Drives the caller-computed
    // LAG watermark; retained across drains.
    private long maxTsSeen = Long.MIN_VALUE;
    // Per-drain late count: rows added since the previous {@link #drain}/{@link #replay}
    // call whose ts triggered the "late" branch. Zeroed on drain cursor close. Callers
    // use this to decide between the hot path and warm-path replay before committing.
    private long pendingLateCount;
    // Primary row store, base-table schema. Grows append-only between compactions.
    private InMemoryTable rows = new InMemoryTable();
    // True when the live sort range has entries out of ts order relative to the
    // sorted prefix. Reset by {@link #sortLiveRange} / {@link #compact}; set by
    // {@link #addRow} when a new row's ts is lower than the previous tail. Skipping
    // the radix sort in the monotonic-arrival steady state avoids O(liveCount) work
    // per drain.
    private boolean sortDirty;
    // First live entry in the sort index. Advances on {@link #applyRetention}; reset
    // to 0 by {@link #compact} after the evicted prefix is physically reclaimed.
    private long sortStart;
    // Timestamp of the most recent row appended via {@link #addRow}; used to detect
    // out-of-order arrivals that flip {@link #sortDirty}. Reset on {@link #reset} and
    // to the last sort-index entry's ts on {@link #compact}.
    private long tailTs = Long.MIN_VALUE;

    public MergeBuffer(RecordMetadata metadata) {
        this.timestampColumnIndex = metadata.getTimestampIndex();
        if (timestampColumnIndex < 0) {
            throw new IllegalArgumentException("live view requires a designated timestamp column");
        }
        rows.init(metadata);
        compactScratch.init(metadata);
        // Share symbol dictionaries so the compact-time swap does not reassign symbol
        // keys for already-buffered rows; the window function's partition state would
        // otherwise be invalidated.
        compactScratch.shareSymbolTablesWith(rows);
        this.record = new LiveViewRecord(rows);
        this.sortIndex = Vm.getCARWInstance(INDEX_PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
        this.sortScratch = Vm.getCARWInstance(INDEX_PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);

        int columnCount = metadata.getColumnCount();
        this.symbolTables = new ObjList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            if (ColumnType.tagOf(metadata.getColumnType(i)) == ColumnType.SYMBOL) {
                symbolTables.extendAndSet(i, new ActiveSymbolTable(i));
            } else {
                symbolTables.extendAndSet(i, null);
            }
        }
    }

    /**
     * Appends a row from the cursor's current record. Returns {@code true} if the
     * row arrived "late" — i.e. its timestamp falls at or below the watermark of
     * the most recent drain, meaning the window function has already emitted state
     * for that time range and the caller must take the warm-path replay branch on
     * the next drain.
     */
    public boolean addRow(Record record) {
        long ts = record.getTimestamp(timestampColumnIndex);
        long rowId = rows.getPhysicalRowCount();
        rows.appendRow(record);
        long addr = sortIndex.appendAddressFor(INDEX_ENTRY_BYTES);
        Unsafe.getUnsafe().putLong(addr, ts);
        Unsafe.getUnsafe().putLong(addr + Long.BYTES, rowId);
        count++;
        if (ts > maxTsSeen) {
            maxTsSeen = ts;
        }
        if (ts < tailTs) {
            sortDirty = true;
        }
        tailTs = ts;
        if (ts <= lastDrainedWatermark) {
            lateRowCount++;
            pendingLateCount++;
            return true;
        }
        return false;
    }

    /**
     * Evicts the prefix of the live sort range whose ts falls at or below
     * {@code maxTsSeen - retentionMicros}. Advances {@link #sortStart} only; the
     * physical rows stay allocated in {@link #rows} until {@link #compact} runs.
     * Safe to call after either {@link #drain} or {@link #replay}: both ensure the
     * live sort range is fully sorted by ts.
     */
    public void applyRetention(long retentionMicros) {
        if (retentionMicros <= 0 || count == sortStart || maxTsSeen == Long.MIN_VALUE) {
            return;
        }
        long cutoff = maxTsSeen - retentionMicros;
        long indexAddr = sortIndex.getPageAddress(0);
        long lo = sortStart;
        long hi = count;
        while (lo < hi) {
            long mid = (lo + hi) >>> 1;
            long ts = Unsafe.getUnsafe().getLong(indexAddr + mid * INDEX_ENTRY_BYTES);
            if (ts <= cutoff) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo > sortStart) {
            sortStart = lo;
        }
    }

    @Override
    public void close() {
        rows = Misc.free(rows);
        compactScratch = Misc.free(compactScratch);
        Misc.free(sortIndex);
        Misc.free(sortScratch);
    }

    /**
     * Rebuilds {@link #rows} and the sort index in timestamp-ascending order,
     * reclaiming the prefix that {@link #applyRetention} marked evicted. Physical
     * row ids change: the sort index is rewritten so that row id matches the
     * sorted position in the new {@link #rows}. SYMBOL keys are preserved because
     * the dictionaries are shared with {@link #compactScratch}.
     * <p>
     * Caller is responsible for driving this — {@link #needsCompact} reports when
     * the eviction wastage is high enough to be worth reclaiming. Must not be
     * called while a drain cursor is open.
     */
    public void compact() {
        long liveCount = count - sortStart;
        compactScratch.clearRows();
        long indexAddr = sortIndex.getPageAddress(0);
        long maxTs = Long.MIN_VALUE;
        // TODO(live-view): per-row appendRow dispatches a type switch for every column
        //  on every compaction. For wide base tables this dominates compact cost.
        //  A columnar "copy rows in sort-index order" helper on InMemoryTable would
        //  iterate columns outer and rows inner, making the dispatch per-column.
        for (long i = sortStart; i < count; i++) {
            long rowId = Unsafe.getUnsafe().getLong(indexAddr + i * INDEX_ENTRY_BYTES + Long.BYTES);
            record.setRow(rowId);
            compactScratch.appendRow(record);
        }
        // Rewrite sort index to match the new physical layout: row {@code i} in the
        // compacted {@code rows} holds the entry formerly at sortStart + i, so the
        // new (ts, rowId) entry is (ts, i). Read positions are ahead of write
        // positions, so the forward in-place rewrite is safe.
        for (long i = 0; i < liveCount; i++) {
            long src = indexAddr + (sortStart + i) * INDEX_ENTRY_BYTES;
            long dst = indexAddr + i * INDEX_ENTRY_BYTES;
            long ts = Unsafe.getUnsafe().getLong(src);
            Unsafe.getUnsafe().putLong(dst, ts);
            Unsafe.getUnsafe().putLong(dst + Long.BYTES, i);
            if (ts > maxTs) {
                maxTs = ts;
            }
        }
        sortIndex.jumpTo(liveCount * INDEX_ENTRY_BYTES);
        // Swap rows <-> compactScratch. The shared LiveViewRecord must track the new rows.
        InMemoryTable tmp = rows;
        rows = compactScratch;
        compactScratch = tmp;
        record.setTable(rows);
        compactScratch.clearRows();

        count = liveCount;
        sortStart = 0;
        // Post-compact layout is dense and sorted; subsequent drains can skip the sort
        // until a later out-of-order addRow flips sortDirty again.
        sortDirty = false;
        tailTs = liveCount == 0 ? Long.MIN_VALUE : maxTs;
    }

    /**
     * Convenience: triggers {@link #compact} when {@link #needsCompact} reports that
     * the evicted prefix is large enough to reclaim. Callers drive compaction on
     * their own cadence (typically at the end of a refresh cycle), so invoking this
     * after {@link #applyRetention} is the usual pattern.
     */
    public void compactIfNeeded() {
        if (needsCompact()) {
            compact();
        }
    }

    /**
     * Sorts the live range and returns a single-use cursor over rows with ts in
     * {@code (lastDrainedWatermark, watermark]} — new rows since the previous drain
     * — in timestamp-ascending order. The lower bound is derived by binary-searching
     * the post-sort index, so a late-row insertion that rearranges the sort order
     * does not cause previously-emitted rows to re-emit. Call
     * {@link RecordCursor#close} after fully consuming to advance
     * {@code lastDrainedWatermark}.
     */
    public RecordCursor drain(long watermark) {
        sortLiveRange();
        long from = Math.max(sortStart, findUpperBound(lastDrainedWatermark));
        long to = findUpperBound(watermark);
        drainCursor.of(from, to, watermark);
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

    /**
     * Returns the number of rows added since the previous drain cursor close whose
     * ts triggered the "late" branch in {@link #addRow}. Callers inspect this before
     * calling {@link #drain} to decide between the hot path and the warm-path
     * {@link #replay}: a non-zero count means the window functions must reset and
     * re-iterate all retained rows instead of appending only the delta.
     */
    public long getPendingLateCount() {
        return pendingLateCount;
    }

    /**
     * Returns {@code true} when there are no rows waiting to be emitted past the
     * current {@link #lastDrainedWatermark}. Drained-but-retained rows that exist
     * only to serve warm-path replay still report empty — an idle flush has nothing
     * new to publish.
     */
    public boolean isEmpty() {
        // maxTsSeen tracks the high water mark of every row added since reset.
        // If it hasn't advanced past lastDrainedWatermark, every live row has been
        // emitted. Late-row arrivals are caught by pendingLateCount — those rows
        // have ts <= lastDrainedWatermark but still need a warm-path replay.
        return pendingLateCount == 0 && maxTsSeen <= lastDrainedWatermark;
    }

    /**
     * Returns {@code true} when the evicted prefix is large enough relative to the
     * live range that {@link #compact} would meaningfully shrink memory usage.
     * Threshold is 50%: compaction amortizes to one rewrite per 2x growth of the
     * retention window.
     */
    public boolean needsCompact() {
        return count > 0 && sortStart * 2 >= count;
    }

    /**
     * Sorts the live range and returns a single-use cursor over rows in
     * {@code [sortStart, split)} — every retained row with ts at or below
     * {@code watermark}, including rows already emitted by prior drains — in
     * timestamp-ascending order. Used by the warm-path rollback branch: the
     * caller resets window function state before consuming the cursor, so
     * re-iterating drained rows rebuilds the accumulator from scratch.
     */
    public RecordCursor replay(long watermark) {
        sortLiveRange();
        long to = findUpperBound(watermark);
        drainCursor.of(sortStart, to, watermark);
        return drainCursor;
    }

    /**
     * Clears all buffered state, including symbol dictionaries. The next
     * {@link #addRow} starts a fresh retention cycle with no history.
     */
    public void reset() {
        // rows.clear() drops the shared symbol dictionaries; clear compactScratch's
        // rows only and re-establish the sharing so SYMBOL keys stay consistent
        // across the next compact-swap.
        rows.clear();
        compactScratch.clearRows();
        compactScratch.shareSymbolTablesWith(rows);
        sortIndex.jumpTo(0);
        maxTsSeen = Long.MIN_VALUE;
        lastDrainedWatermark = Long.MIN_VALUE;
        lateRowCount = 0;
        pendingLateCount = 0;
        sortStart = 0;
        count = 0;
        sortDirty = false;
        tailTs = Long.MIN_VALUE;
    }

    /**
     * Returns the number of rows waiting to be emitted by the next drain. Reported
     * via the {@code live_views} catalog as {@code buffered_row_count}. The result
     * assumes the live range is sorted, which {@link #drain} / {@link #replay}
     * ensure; calling between an {@link #addRow} and the next drain reports a value
     * computed against the pre-sort index and can be slightly off for out-of-order
     * arrivals.
     */
    public long size() {
        if (count == sortStart) {
            return 0;
        }
        return count - Math.max(sortStart, findUpperBound(lastDrainedWatermark));
    }

    private long findUpperBound(long watermark) {
        // Returns the first index i in [sortStart, count) with ts(i) > watermark.
        long lo = sortStart;
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

    private void sortLiveRange() {
        long liveCount = count - sortStart;
        if (liveCount <= 1 || !sortDirty) {
            return;
        }
        sortScratch.jumpTo(liveCount * INDEX_ENTRY_BYTES);
        Vect.radixSortLongIndexAscInPlace(
                sortIndex.getPageAddress(0) + sortStart * INDEX_ENTRY_BYTES,
                liveCount,
                sortScratch.addressOf(0)
        );
        sortDirty = false;
    }

    /**
     * Static symbol table over the MergeBuffer's currently active {@link #rows} buffer.
     * Symbol values are interned into the buffer's {@link ObjList} by {@link InMemoryTable#putSymbol};
     * keys are positions within that list. Resolves through the live {@code rows}
     * reference so that lookups stay valid after the compact-time rows/compactScratch swap.
     */
    private class ActiveSymbolTable implements StaticSymbolTable {
        private final int columnIndex;

        ActiveSymbolTable(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCount() {
            ObjList<String> entries = rows.getSymbolTable(columnIndex);
            return entries != null ? entries.size() : 0;
        }

        @Override
        public int keyOf(CharSequence value) {
            if (value == null) {
                return SymbolTable.VALUE_IS_NULL;
            }
            ObjList<String> entries = rows.getSymbolTable(columnIndex);
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
            ObjList<String> entries = rows.getSymbolTable(columnIndex);
            if (entries == null || key >= entries.size()) {
                return null;
            }
            return entries.getQuick(key);
        }
    }

    private class DrainCursor implements RecordCursor {
        private long cursor;
        private long emitEnd;
        private long emitStart;
        private long pendingWatermark;

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            counter.add(emitEnd - cursor);
            cursor = emitEnd;
        }

        @Override
        public void close() {
            // Advance watermark and clear pendingLateCount only on a fully-consumed
            // iteration. An aborted iteration leaves the buffer's drain state intact
            // so the next refresh re-attempts the same emit range (hot path) or
            // warm-path replay.
            if (cursor == emitEnd) {
                if (pendingWatermark > lastDrainedWatermark) {
                    lastDrainedWatermark = pendingWatermark;
                }
                pendingLateCount = 0;
            }
            cursor = 0;
            emitStart = 0;
            emitEnd = 0;
            pendingWatermark = 0;
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
            if (cursor < emitEnd) {
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
            return emitEnd - cursor;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            ((LiveViewRecord) record).setRow(atRowId);
        }

        @Override
        public long size() {
            return emitEnd - cursor;
        }

        @Override
        public void toTop() {
            cursor = emitStart;
        }

        void of(long emitStart, long emitEnd, long pendingWatermark) {
            this.emitStart = emitStart;
            this.cursor = emitStart;
            this.emitEnd = emitEnd;
            this.pendingWatermark = pendingWatermark;
        }
    }
}
