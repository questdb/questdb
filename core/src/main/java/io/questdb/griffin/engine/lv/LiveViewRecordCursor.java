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

package io.questdb.griffin.engine.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.sql.DelegatingRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.table.PageFrameRecordCursor;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.TestOnly;

/**
 * The cursor returned by {@link LiveViewRecordCursorFactory}. Pins the LV's
 * in-memory tier slot at open and releases it on close. Keeping the reader
 * visible to the refresh worker's slow-path {@code tryAcquireWrite} ensures
 * the writer trails rather than progressing past a slow reader.
 * <p>
 * Routing (Mode B, seam_ts): when the consistency fence holds
 * ({@link #routingEligible}), the cursor serves disk rows with
 * {@code ts < seamTs}, stops the disk scan at the first row with
 * {@code ts >= seamTs} (skipping the hot tail partition(s) of the LV table),
 * then serves the entire pinned in-mem slot, which holds every output row with
 * {@code ts >= seamTs}. Disk is strictly below the seam and the slot is at or
 * above it (ties at {@code seamTs} included), so the seam boundary has neither
 * a duplicate nor a gap. The fence ({@code slot.lvSeqTxn == diskReader.seqTxn})
 * makes this safe: equal seqTxns mean the slot and the disk snapshot reflect
 * the identical LV-table version, so the band agrees row-for-row even across an
 * O3 rewrite. The seam split also assumes the disk scan is ascending, so a
 * backward / index scan (e.g. {@code ORDER BY ts DESC} pushed into the base)
 * routes disk-only. When the fence does not hold (tier absent / empty, pruned
 * projection, a disk cursor that is not a plain table scan, a non-ascending
 * scan, or a seqTxn mismatch) the cursor falls back to disk-only, which is
 * always correct because in-mem is a subset of disk in steady state. O3 replay
 * rewrites the disk tier
 * and resets the in-mem tier (see {@code LiveViewRefreshJob.resetInMemoryTier}),
 * so a post-O3 cursor whose slot no longer matches reads from disk until the
 * tier refills.
 * <p>
 * The in-mem tier stores the full output row, so the cursor routes through it
 * only when the read projects every output column in declared order (see
 * {@link #isFullSchemaProjection}). Pruned or reordered projections - e.g.
 * {@code SELECT max(rn)}, where column pruning drops the timestamp and leaves
 * {@code timestampColumnIndex < 0} - serve from disk alone, which is correct
 * given the steady-state in-mem-as-subset-of-disk property. A SYMBOL output
 * column also routes disk-only: the tier holds WAL-segment-local symbol ids that
 * the disk reader's symbol table cannot resolve (see
 * {@link #isFullSchemaProjection}).
 * <p>
 * In-mem rows synthesize a tagged rowId (the sign bit set over the buffer row
 * index); {@link #recordAt(Record, long)} decodes it back to a buffer row
 * against the still-pinned slot. Disk rowIds are non-negative, so the tag never
 * collides. Random-access readers (ASOF JOIN as RHS, etc.) can therefore land
 * on an in-mem row and round-trip correctly within the cursor's lifetime.
 * <p>
 * Single-shot lifecycle: the factory allocates a fresh instance per
 * {@link LiveViewRecordCursorFactory#getCursor(io.questdb.griffin.SqlExecutionContext)}.
 * {@link #of} is invoked exactly once during construction.
 */
public class LiveViewRecordCursor implements RecordCursor {

    // In-mem rows carry no disk rowId; getRowId() synthesizes one by setting the
    // sign bit over the buffer row index. Disk rowIds are non-negative, so the
    // tag never collides; recordAt() decodes it back against the pinned slot.
    private static final long IN_MEM_ROW_ID_FLAG = Long.MIN_VALUE;
    private final MergedRecord recordA = new MergedRecord();
    private final MergedRecord recordB = new MergedRecord();
    private RecordCursor diskCursor;
    private boolean diskExhausted;
    private boolean inMemEligible;
    private long inMemRow;
    // Test-only count of in-mem rows served over this cursor's lifetime; lets
    // tests confirm Mode B actually routed through the tier, not disk alone.
    private long inMemRowsServed;
    private LiveViewInMemoryBuffer pinnedSlot;
    // True when the fence holds (pinned slot and disk reader share an LV-table
    // seqTxn) and the read is a full-schema identity projection. Mode B routing
    // in hasNext() serves the slot for ts >= seamTs only when this is true.
    private boolean routingEligible;
    private int slotIdx;
    private LiveViewInMemoryTier tier;
    private int timestampColumnIndex;

    public LiveViewRecordCursor() {
        this.slotIdx = -1;
    }

    @Override
    public void close() {
        releaseSlot();
        pinnedSlot = null;
        diskCursor = Misc.free(diskCursor);
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return diskCursor.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (routingEligible) {
            // Mode B (seam routing): serve disk rows strictly below the slot's
            // seam timestamp, then serve the entire pinned slot. The slot holds
            // every output row with ts >= seamTs and agrees with disk over that
            // band (the seqTxn fence guarantees it), so stopping the disk scan at
            // the seam skips reading the hot tail partition(s) from the LV table.
            // Disk is strictly < seamTs and the slot is >= seamTs (ties at seamTs
            // included), so there is neither a duplicate nor a gap at the seam.
            if (!diskExhausted) {
                if (diskCursor.hasNext()) {
                    long ts = diskCursor.getRecord().getTimestamp(timestampColumnIndex);
                    if (ts < pinnedSlot.seamTs()) {
                        recordA.toDiskMode();
                        return true;
                    }
                    // Reached the seam: this row and everything after it lives in
                    // the slot. Stop scanning disk - the perf win.
                    diskExhausted = true;
                } else {
                    diskExhausted = true;
                }
            }
            if (inMemRow + 1 < pinnedSlot.rowCount()) {
                inMemRow++;
                inMemRowsServed++;
                recordA.toInMemMode(inMemRow);
                return true;
            }
            return false;
        }
        // Disk-only: the fence did not engage (tier absent/empty, pruned
        // projection, non-table disk cursor, or a seqTxn mismatch). In steady
        // state in-mem is a subset of disk, so disk alone is complete.
        if (diskCursor.hasNext()) {
            recordA.toDiskMode();
            return true;
        }
        return false;
    }

    @TestOnly
    public long inMemRowsServed() {
        return inMemRowsServed;
    }

    @TestOnly
    public boolean isRoutingEligible() {
        return routingEligible;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return diskCursor.newSymbolTable(columnIndex);
    }

    public void of(RecordCursor diskCursor, RecordMetadata baseMetadata, LiveViewInstance instance, int timestampColumnIndex, boolean diskScanAscending) {
        releaseSlot();
        this.diskCursor = diskCursor;
        this.timestampColumnIndex = timestampColumnIndex;
        this.inMemRowsServed = 0;
        this.diskExhausted = false;
        this.inMemRow = -1;
        this.pinnedSlot = null;
        this.inMemEligible = false;
        this.routingEligible = false;
        if (instance != null) {
            LiveViewInMemoryTier candidate = instance.getInMemoryTier();
            if (candidate != null) {
                int pin = candidate.acquireRead();
                if (pin >= 0) {
                    // acquireRead succeeded: keep the tier reference so close()
                    // can call releaseRead with the matching index. A return of
                    // -1 means the tier was concurrently closed (LV dropped);
                    // in that case we hold neither the global pin lease nor a
                    // per-slot rc and must not touch the tier again.
                    this.tier = candidate;
                    this.slotIdx = pin;
                    this.pinnedSlot = candidate.getSlot(pin);
                    this.inMemEligible = isFullSchemaProjection(baseMetadata, pinnedSlot, timestampColumnIndex);
                    // Fence: serve the slot only when (a) the disk scan is
                    // ascending (Mode B's seam split assumes ascending ts), and
                    // (b) the slot and the disk reader share an LV-table seqTxn
                    // (same version => rows agree). Mismatch / unstamped /
                    // non-table cursor / non-ascending scan => disk-only.
                    this.routingEligible = inMemEligible
                            && diskScanAscending
                            && pinnedSlot.rowCount() > 0
                            && pinnedSlot.lvSeqTxn() != Numbers.LONG_NULL
                            && pinnedSlot.lvSeqTxn() == diskReaderSeqTxn(diskCursor);
                }
            }
        }
        recordA.bindDisk(diskCursor.getRecord(), this, pinnedSlot);
        recordB.bindDisk(diskCursor.getRecordB(), this, pinnedSlot);
    }

    @Override
    public long preComputedStateSize() {
        return diskCursor == null ? 0 : diskCursor.preComputedStateSize();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        MergedRecord mr = (MergedRecord) record;
        if (atRowId < 0) {
            // Tagged in-mem rowId (sign bit set over the buffer row index).
            // Decode back to a buffer row and position the record against the
            // still-pinned slot. Valid for the cursor's lifetime, during which
            // the slot stays pinned and frozen.
            mr.toInMemMode(atRowId & Long.MAX_VALUE);
            return;
        }
        // Disk rowId (non-negative): delegate to the disk cursor.
        mr.toDiskMode();
        diskCursor.recordAt(mr.diskRecord(), atRowId);
    }

    @Override
    public long size() {
        // In steady state in-mem is a subset of disk, so disk.size()
        // is the cursor's actual size. Returning -1 (unknown) would defeat
        // LIMIT pushdown for the disk slice.
        return diskCursor.size();
    }

    @Override
    public void toTop() {
        // Restart both sides; the next hasNext() re-finds the seam by re-scanning
        // disk from the top. routingEligible is unchanged - the slot stays pinned
        // at the same seqTxn for the cursor's lifetime.
        if (diskCursor != null) {
            diskCursor.toTop();
        }
        diskExhausted = false;
        inMemRow = -1;
        recordA.toDiskMode();
    }

    // Returns the disk cursor's LV-table seqTxn, or LONG_NULL when the cursor is
    // not a plain FULL table-reader scan we can fence cheaply. An interval filter
    // (e.g. a WHERE on the designated timestamp, pushed into the scan) makes the
    // disk side return only a sub-range, so disk[ts < seamTs] would be missing
    // rows while the unfiltered slot over-returns - Mode B must not engage. A
    // non-page-frame plan (LATEST BY, complex factory) likewise returns LONG_NULL.
    private static long diskReaderSeqTxn(RecordCursor diskCursor) {
        if (diskCursor instanceof PageFrameRecordCursor pfrc
                && pfrc.getPageFrameCursor() instanceof TablePageFrameCursor tpfc
                && !tpfc.hasIntervalFilter()) {
            TableReader reader = tpfc.getTableReader();
            if (reader != null) {
                return reader.getSeqTxn();
            }
        }
        return Numbers.LONG_NULL;
    }

    /**
     * The in-mem tier stores the live view's full output row. A read whose
     * projection prunes or reorders columns would index the buffer by the wrong
     * column, and a read that prunes the timestamp leaves
     * {@code timestampColumnIndex < 0}, which would address the buffer and the
     * disk record out of bounds. Such reads serve from disk only, which is
     * correct because the in-mem tier is a subset of disk in steady state. Only
     * an identity projection (every output column, in declared order) may route
     * through the tier; the type-by-type match below establishes that.
     * <p>
     * SYMBOL columns also force disk-only: the tier stores the staging record's
     * raw int, which is a WAL-segment-local symbol id (the disk write path
     * re-interns the value into the LV table's own symbol space, so the same
     * string can carry a different id per segment). That id is not resolvable
     * against the disk reader's symbol table at read time, so any SYMBOL output
     * column disqualifies the tier read until the tier stores LV-space ids.
     */
    private static boolean isFullSchemaProjection(
            RecordMetadata baseMetadata,
            LiveViewInMemoryBuffer buffer,
            int timestampColumnIndex
    ) {
        if (timestampColumnIndex < 0 || buffer == null) {
            return false;
        }
        final int columnCount = buffer.columnCount();
        if (baseMetadata.getColumnCount() != columnCount) {
            return false;
        }
        for (int i = 0; i < columnCount; i++) {
            if (baseMetadata.getColumnType(i) != buffer.columnType(i)) {
                return false;
            }
            if (ColumnType.tagOf(buffer.columnType(i)) == ColumnType.SYMBOL) {
                return false;
            }
        }
        return true;
    }

    private void releaseSlot() {
        if (tier != null && slotIdx >= 0) {
            // Safe even after the LV's DROP marked the tier closed: the deferred-
            // close protocol on LiveViewInMemoryTier keeps native memory alive
            // until the last pin drains (DROP LIVE VIEW "modulo cursor pins").
            tier.releaseRead(slotIdx);
        }
        tier = null;
        slotIdx = -1;
    }

    /**
     * Mode-switching record proxy. In disk mode every accessor delegates to
     * the bound {@link Record} from the disk cursor via {@link DelegatingRecord}.
     * In in-mem mode the supported fixed-width accessors read directly from
     * the pinned buffer.
     * <p>
     * {@link #getSymA}/{@link #getSymB} keep an in-mem branch that resolves the
     * buffer's stored int via the cursor's
     * {@link RecordCursor#getSymbolTable(int)}, but a SYMBOL output column routes
     * disk-only (see {@link #isFullSchemaProjection}) because the tier stores
     * WAL-segment-local ids, so that branch is currently unreachable; it stays as
     * the shape a future LV-space symbol id would use.
     * <p>
     * Var-length accessors (STRING / VARCHAR / BINARY / ARRAY) inherit the
     * disk-only delegation. Those columns prevent the in-mem tier from being
     * allocated in the first place
     * (see {@link LiveViewInMemoryBuffer#areColumnTypesSupported}), so
     * {@code inMemMode == true} is unreachable for LVs whose schema contains
     * them.
     */
    private static class MergedRecord extends DelegatingRecord {
        private LiveViewInMemoryBuffer buffer;
        private long bufferRow;
        private RecordCursor cursor;
        private boolean inMemMode;

        @Override
        public boolean getBool(int col) {
            return inMemMode ? buffer.getBool(bufferRow, col) : super.getBool(col);
        }

        @Override
        public byte getByte(int col) {
            return inMemMode ? buffer.getByte(bufferRow, col) : super.getByte(col);
        }

        @Override
        public char getChar(int col) {
            return inMemMode ? (char) buffer.getShort(bufferRow, col) : super.getChar(col);
        }

        @Override
        public long getDate(int col) {
            return inMemMode ? buffer.getLong(bufferRow, col) : super.getDate(col);
        }

        @Override
        public double getDouble(int col) {
            return inMemMode ? buffer.getDouble(bufferRow, col) : super.getDouble(col);
        }

        @Override
        public float getFloat(int col) {
            return inMemMode ? buffer.getFloat(bufferRow, col) : super.getFloat(col);
        }

        @Override
        public byte getGeoByte(int col) {
            return inMemMode ? buffer.getByte(bufferRow, col) : super.getGeoByte(col);
        }

        @Override
        public int getGeoInt(int col) {
            return inMemMode ? buffer.getInt(bufferRow, col) : super.getGeoInt(col);
        }

        @Override
        public long getGeoLong(int col) {
            return inMemMode ? buffer.getLong(bufferRow, col) : super.getGeoLong(col);
        }

        @Override
        public short getGeoShort(int col) {
            return inMemMode ? buffer.getShort(bufferRow, col) : super.getGeoShort(col);
        }

        @Override
        public int getIPv4(int col) {
            return inMemMode ? buffer.getInt(bufferRow, col) : super.getIPv4(col);
        }

        @Override
        public int getInt(int col) {
            return inMemMode ? buffer.getInt(bufferRow, col) : super.getInt(col);
        }

        @Override
        public long getLong(int col) {
            return inMemMode ? buffer.getLong(bufferRow, col) : super.getLong(col);
        }

        @Override
        public long getRowId() {
            // In-mem rows synthesize a tagged rowId: the sign bit set over the
            // buffer row index. recordAt() decodes it back against the still-
            // pinned slot. Disk rowIds are non-negative, so the tag never
            // collides and random access stays self-consistent within the
            // cursor's lifetime.
            if (inMemMode) {
                return IN_MEM_ROW_ID_FLAG | bufferRow;
            }
            return base.getRowId();
        }

        @Override
        public short getShort(int col) {
            return inMemMode ? buffer.getShort(bufferRow, col) : super.getShort(col);
        }

        @Override
        public CharSequence getSymA(int col) {
            if (!inMemMode) {
                return super.getSymA(col);
            }
            return cursor.getSymbolTable(col).valueOf(buffer.getInt(bufferRow, col));
        }

        @Override
        public CharSequence getSymB(int col) {
            if (!inMemMode) {
                return super.getSymB(col);
            }
            return cursor.getSymbolTable(col).valueOf(buffer.getInt(bufferRow, col));
        }

        @Override
        public long getTimestamp(int col) {
            return inMemMode ? buffer.getLong(bufferRow, col) : super.getTimestamp(col);
        }

        void bindDisk(Record diskRecord, RecordCursor cursor, LiveViewInMemoryBuffer buffer) {
            this.base = diskRecord;
            this.cursor = cursor;
            this.buffer = buffer;
            this.bufferRow = -1;
            this.inMemMode = false;
        }

        Record diskRecord() {
            return base;
        }

        void toDiskMode() {
            this.inMemMode = false;
        }

        void toInMemMode(long row) {
            this.bufferRow = row;
            this.inMemMode = true;
        }
    }
}
