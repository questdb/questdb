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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewSymbolCache;
import io.questdb.cairo.lv.LiveViewSymbolTable;
import io.questdb.cairo.sql.DelegatingRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.table.PageFrameRecordCursor;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.std.BinarySequence;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.TestOnly;

/**
 * The cursor returned by {@link LiveViewRecordCursorFactory}. Pins the LV's
 * in-memory tier slot at open and releases it on close. Keeping the reader
 * visible to the refresh worker's slow-path {@code tryAcquireWrite} ensures
 * the writer trails rather than progressing past a slow reader.
 * <p>
 * Seam routing: when the consistency fence holds ({@link #routingEligible}),
 * the cursor serves disk rows with {@code ts < seamTs}, stops the disk scan at
 * the first row with {@code ts >= seamTs} (skipping the hot tail partition(s) of
 * the LV table), then serves the entire pinned in-mem slot, which holds every
 * output row with {@code ts >= seamTs}. The slot's lower band is the overlap -
 * rows already on disk, served from RAM in place of the hot tail - and any rows
 * above the applied point are the un-flushed lead, which disk does not have yet,
 * so the slot can lead disk. Disk is strictly below the seam and the slot is at
 * or above it (ties at {@code seamTs} included), so the seam boundary has neither
 * a duplicate nor a gap. The fence ({@code slot.lvSeqTxn == diskReader.seqTxn})
 * makes the overlap safe: equal seqTxns mean the slot's overlap and the disk
 * snapshot reflect the identical LV-table version, so that band agrees row-for-row
 * even across an O3 rewrite. The seam split also assumes the disk scan is
 * ascending, so a backward / index scan (e.g. {@code ORDER BY ts DESC} pushed
 * into the base) routes disk-only. When the fence does not hold (tier absent /
 * empty, pruned projection, a disk cursor that is not a plain table scan, a
 * non-ascending scan, or a seqTxn mismatch) the cursor falls back to disk-only,
 * which serves the applied prefix - always correct, at worst one flush cycle
 * behind the lead. O3 replay rewrites the disk tier and atomically rebuilds the
 * in-mem tier from the rewritten LV table (see
 * {@code LiveViewRefreshJob.rebuildInMemoryTier}), stamping the fresh slot with
 * the post-O3 LV-table seqTxn; a cursor opened after the replay therefore regains
 * tier routing once the rebuild publishes. Should the rebuild be skipped (both
 * slots reader-pinned), the stale slot's pre-O3 seqTxn no longer matches the
 * rewritten disk, so the fence routes those reads disk-only until a later cycle
 * republishes.
 * <p>
 * The in-mem tier stores the full output row, so the cursor routes through it
 * only when the read projects every output column in declared order (see
 * {@link #isFullSchemaProjection}). Pruned or reordered projections - e.g.
 * {@code SELECT max(rn)}, where column pruning drops the timestamp and leaves
 * {@code timestampColumnIndex < 0} - serve from disk alone, which is correct
 * because disk holds every applied row (such reads simply do not see the lead).
 * SYMBOL output columns route through the tier too: the refresh worker
 * eager-interns the un-flushed lead's symbols into the LV table's id space (see
 * {@link io.questdb.cairo.lv.LiveViewSymbolCache}), and {@link #getSymbolTable}
 * returns a {@link io.questdb.cairo.lv.LiveViewSymbolTable} overlay that resolves
 * a committed id via the disk reader's symbol table and a lead-only id via the
 * cache - one LV-table id space, so both per-record reads and raw-int-key reads
 * (WHERE / GROUP BY / static ORDER BY) stay correct (see
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
    // tests confirm a read actually routed through the tier, not disk alone.
    private long inMemRowsServed;
    // Test-only count of LEAD rows (in-mem rows not yet on disk) served over this
    // cursor's lifetime; lets tests confirm the un-flushed lead was served from RAM,
    // not just the disk-backed overlap. A subset of inMemRowsServed.
    private long leadRowsServed;
    // Tier index of the first lead row in the pinned slot (rowCount - leadRowCount):
    // rows [0, leadStart) are overlap (also on disk), [leadStart, rowCount) are the
    // un-flushed lead. Snapshotted at of() from the slot's stamped leadRowCount.
    // Drives size() (disk.size() + lead) and the leadRowsServed counter.
    private long leadStart;
    private LiveViewInMemoryBuffer pinnedSlot;
    // True when the fence holds (pinned slot and disk reader share an LV-table
    // seqTxn) and the read is a full-schema identity projection. Seam routing in
    // hasNext() serves the slot for ts >= seamTs only when this is true.
    private boolean routingEligible;
    private int slotIdx;
    // The pinned tier's eager-interning symbol cache. Non-null only while routing
    // through the tier; used to resolve the un-flushed lead's symbols that are not
    // yet on disk. Reset on of(); cleared on close.
    private LiveViewSymbolCache symbolCache;
    // Per-column symbol-resolution overlays (disk symbol table + the lead's cache),
    // lazily created for SYMBOL columns while routing so getSymbolTable() resolves
    // both bands in one LV-table id space. Null until first needed; entries null for
    // non-SYMBOL columns. Freed on close.
    private ObjList<LiveViewSymbolTable> symbolTableOverlays;
    private LiveViewInMemoryTier tier;
    private int timestampColumnIndex;

    public LiveViewRecordCursor() {
        this.slotIdx = -1;
    }

    @Override
    public void close() {
        // The getSymbolTable() overlays borrow the disk cursor's symbol tables
        // (closed via diskCursor below), so closing them only drops references.
        if (symbolTableOverlays != null) {
            Misc.freeObjListIfCloseable(symbolTableOverlays);
            symbolTableOverlays.clear();
        }
        symbolCache = null;
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
        // While routing through the tier, a SYMBOL column resolves through an
        // overlay that adds the un-flushed lead's eager-interned symbols on top of
        // the disk reader's committed table - so both a getSymA per-record read and
        // a raw-int-key read (WHERE / GROUP BY / static ORDER BY) see the lead's
        // values. Disk-only reads (no routing) resolve straight from disk.
        if (routingEligible && symbolCache != null && symbolCache.isSymbolColumn(columnIndex)) {
            return symbolTableOverlay(columnIndex);
        }
        return diskCursor.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (routingEligible) {
            // Seam routing: serve disk rows strictly below the slot's seam
            // timestamp, then serve the entire pinned slot. The slot holds every
            // output row with ts >= seamTs: the overlap band [seamTs, applied]
            // agrees with disk row-for-row (the seqTxn fence guarantees it) and is
            // served from RAM instead of the hot tail partition(s); any rows above
            // the applied point are the un-flushed lead, served only from RAM since
            // disk does not have them yet. Disk is strictly < seamTs and the slot
            // is >= seamTs (ties at seamTs included), so the seam boundary has
            // neither a duplicate nor a gap.
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
                if (inMemRow >= leadStart) {
                    // A lead row: in the slot but not yet on the LV's on-disk tier.
                    leadRowsServed++;
                }
                recordA.toInMemMode(inMemRow);
                return true;
            }
            return false;
        }
        // Disk-only: the fence did not engage (tier absent/empty, pruned
        // projection, non-table disk cursor, or a seqTxn mismatch). Disk holds
        // every applied row, so this serves the applied prefix - always correct,
        // at worst one flush cycle behind the in-mem lead.
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
    public long leadRowsServed() {
        return leadRowsServed;
    }

    @TestOnly
    public boolean isRoutingEligible() {
        return routingEligible;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        if (routingEligible && symbolCache != null && symbolCache.isSymbolColumn(columnIndex)) {
            // Owning overlay: it closes the freshly cloned disk table the caller
            // would otherwise free directly (parallel execution clones tables).
            return new LiveViewSymbolTable().of(
                    (StaticSymbolTable) diskCursor.newSymbolTable(columnIndex),
                    symbolCache,
                    columnIndex,
                    pinnedSlot.newSymbolMaxId(columnIndex),
                    true
            );
        }
        return diskCursor.newSymbolTable(columnIndex);
    }

    public void of(RecordCursor diskCursor, RecordMetadata baseMetadata, LiveViewInstance instance, int timestampColumnIndex, boolean diskScanAscending) {
        releaseSlot();
        this.diskCursor = diskCursor;
        this.timestampColumnIndex = timestampColumnIndex;
        this.inMemRowsServed = 0;
        this.leadRowsServed = 0;
        this.leadStart = 0;
        this.diskExhausted = false;
        this.inMemRow = -1;
        this.pinnedSlot = null;
        this.inMemEligible = false;
        this.routingEligible = false;
        this.symbolCache = null;
        if (symbolTableOverlays != null) {
            symbolTableOverlays.clear();
        }
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
                    this.symbolCache = candidate.getSymbolCache();
                    this.inMemEligible = isFullSchemaProjection(baseMetadata, pinnedSlot, timestampColumnIndex);
                    // Fence: serve the slot only when (a) the disk scan is
                    // ascending (the seam split assumes ascending ts), and
                    // (b) the slot and the disk reader share an LV-table seqTxn
                    // (same version => rows agree). Mismatch / unstamped /
                    // non-table cursor / non-ascending scan => disk-only.
                    this.routingEligible = inMemEligible
                            && diskScanAscending
                            && pinnedSlot.rowCount() > 0
                            && pinnedSlot.lvSeqTxn() != Numbers.LONG_NULL
                            && pinnedSlot.lvSeqTxn() == diskReaderSeqTxn(diskCursor);
                    // Snapshot the overlap/lead boundary. Rows [0, leadStart) are
                    // the overlap (also on disk, served via the seam split); rows
                    // [leadStart, rowCount) are the un-flushed lead, served only
                    // from RAM. The slot is frozen for the cursor's lifetime, so
                    // this snapshot stays valid.
                    this.leadStart = pinnedSlot.rowCount() - pinnedSlot.leadRowCount();
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
        if (routingEligible) {
            // The cursor serves disk rows below the seam plus every row in the
            // pinned slot. The slot's overlap (rows [0, leadStart)) is also on
            // disk, so it is already counted in disk.size(); only the un-flushed
            // lead (rows [leadStart, rowCount)) sits on top. Hence
            // size() = disk.size() + (rowCount - leadStart) = disk.size() +
            // leadRowCount. When the slot holds no lead this collapses to
            // disk.size(). Returning -1 (unknown) would defeat LIMIT pushdown.
            return diskCursor.size() + (pinnedSlot.rowCount() - leadStart);
        }
        // Disk-only: the fence did not engage, so the read serves the applied
        // prefix straight from disk.
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
    // rows while the unfiltered slot over-returns - seam routing must not engage.
    // A non-page-frame plan (LATEST BY, complex factory) returns LONG_NULL too.
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
     * correct because disk holds every applied row - they simply do not see the
     * un-flushed lead, trailing it by at most one flush cycle. Only an identity
     * projection (every output column, in declared order) may route through the
     * tier; the type-by-type match below establishes that.
     * <p>
     * SYMBOL columns are routable: the tier stores LV-table-consistent symbol ids
     * (eager-interned by the refresh worker, see
     * {@link io.questdb.cairo.lv.LiveViewSymbolCache}), so the in-mem branch in
     * {@link MergedRecord#getSymA}/{@link MergedRecord#getSymB} resolves them via
     * {@link #getSymbolTable}'s overlay - committed ids against the disk reader,
     * lead-only ids against the cache.
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

    // Lazily creates (and caches per column) the symbol-resolution overlay for a
    // SYMBOL column while routing. The overlay borrows the disk cursor's symbol
    // table (the cursor closes that via diskCursor), so it does not own it.
    private LiveViewSymbolTable symbolTableOverlay(int columnIndex) {
        if (symbolTableOverlays == null) {
            symbolTableOverlays = new ObjList<>();
        }
        LiveViewSymbolTable overlay = columnIndex < symbolTableOverlays.size() ? symbolTableOverlays.getQuick(columnIndex) : null;
        if (overlay == null) {
            overlay = new LiveViewSymbolTable().of(
                    (StaticSymbolTable) diskCursor.getSymbolTable(columnIndex),
                    symbolCache,
                    columnIndex,
                    pinnedSlot.newSymbolMaxId(columnIndex),
                    false
            );
            symbolTableOverlays.extendAndSet(columnIndex, overlay);
        }
        return overlay;
    }

    /**
     * Mode-switching record proxy. In disk mode every accessor delegates to
     * the bound {@link Record} from the disk cursor via {@link DelegatingRecord}.
     * In in-mem mode the supported fixed-width accessors read directly from
     * the pinned buffer.
     * <p>
     * {@link #getSymA}/{@link #getSymB} resolve the buffer's stored int via the
     * cursor's {@link RecordCursor#getSymbolTable(int)}, which while routing
     * returns the {@link io.questdb.cairo.lv.LiveViewSymbolTable} overlay. The tier
     * stores LV-table-consistent symbol ids (eager-interned by the refresh worker),
     * so a committed id resolves against the disk reader's table and a lead-only id
     * against the tier's symbol cache.
     * <p>
     * The STRING, BINARY and VARCHAR accessors read from the pinned buffer's per-row
     * offset/header vector while in in-mem mode, mirroring the fixed-width accessors.
     * The remaining var-length accessor (ARRAY) inherits the disk-only delegation:
     * that column prevents the in-mem tier from being allocated in the first place
     * (see {@link LiveViewInMemoryBuffer#areColumnTypesSupported}), so
     * {@code inMemMode == true} is unreachable for LVs whose schema contains it.
     */
    private static class MergedRecord extends DelegatingRecord {
        private LiveViewInMemoryBuffer buffer;
        private long bufferRow;
        private RecordCursor cursor;
        private boolean inMemMode;

        @Override
        public BinarySequence getBin(int col) {
            return inMemMode ? buffer.getBin(bufferRow, col) : super.getBin(col);
        }

        @Override
        public long getBinLen(int col) {
            return inMemMode ? buffer.getBinLen(bufferRow, col) : super.getBinLen(col);
        }

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
        public CharSequence getStrA(int col) {
            return inMemMode ? buffer.getStrA(bufferRow, col) : super.getStrA(col);
        }

        @Override
        public CharSequence getStrB(int col) {
            return inMemMode ? buffer.getStrB(bufferRow, col) : super.getStrB(col);
        }

        @Override
        public int getStrLen(int col) {
            return inMemMode ? buffer.getStrLen(bufferRow, col) : super.getStrLen(col);
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

        @Override
        public Utf8Sequence getVarcharA(int col) {
            return inMemMode ? buffer.getVarcharA(bufferRow, col) : super.getVarcharA(col);
        }

        @Override
        public Utf8Sequence getVarcharB(int col) {
            return inMemMode ? buffer.getVarcharB(bufferRow, col) : super.getVarcharB(col);
        }

        @Override
        public int getVarcharSize(int col) {
            return inMemMode ? buffer.getVarcharSize(bufferRow, col) : super.getVarcharSize(col);
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
