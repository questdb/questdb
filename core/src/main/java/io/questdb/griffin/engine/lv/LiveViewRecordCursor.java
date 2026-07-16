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

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewSymbolCache;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.DelegatingRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.table.PageFrameRecordCursor;
import io.questdb.griffin.engine.table.PageFrameRecordCursorImpl;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectByteSequenceView;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.Utf16Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8SplitString;
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
 * empty, a timestamp-pruned projection, a disk cursor that is not a plain table
 * scan, a non-ascending scan, or a seqTxn mismatch) the cursor falls back to disk-only,
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
 * The in-mem tier stores the full output row, so a read that prunes or reorders
 * columns projects a subset of what the slot already holds. The cursor resolves
 * each projected column to its tier column through the disk scan's
 * {@link ColumnMapping} (see {@link #isTierAddressableProjection}), so such reads
 * route too. A read that prunes the designated timestamp - e.g.
 * {@code SELECT max(rn)}, which leaves {@code timestampColumnIndex < 0} - still
 * serves from disk alone, because the seam split has no timestamp to cut on; that
 * is correct because disk holds every applied row (the read simply does not see
 * the lead). SYMBOL output columns route through the tier too: the refresh worker
 * eager-interns the un-flushed lead's symbols into the LV table's id space (see
 * {@link io.questdb.cairo.lv.LiveViewSymbolCache}), and the cursor answers
 * {@link #getSymbolTable} / {@link #newSymbolTable} through a
 * {@link LiveViewSymbolTableSource}, which resolves a committed id via the disk
 * reader's symbol table and a lead-only id via the cache - one LV-table id space, so
 * both per-record reads and raw-int-key reads (WHERE / GROUP BY / static ORDER BY)
 * stay correct (see {@link #isTierAddressableProjection}). That source is a
 * standalone {@link io.questdb.cairo.sql.SymbolTableSource} rather than cursor state,
 * so a page-frame cursor over the tier can bind the same overlay and let a filter
 * worker resolve the lead without a record.
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
    // Resolves the read's SYMBOL columns: the disk cursor's tables overlaid with the
    // pinned tier's eager-interned lead symbols while routing, the disk cursor's
    // tables alone otherwise. Bound in of() once the routing decision is made.
    private final LiveViewSymbolTableSource symbolTableSource = new LiveViewSymbolTableSource();
    // Output column -> tier column, one entry per projected column, built in of()
    // from the disk scan's ColumnMapping. The tier stores the LV's full output row,
    // so a pruned or reordered projection reads a subset of the slot's columns
    // through this indirection instead of indexing the buffer by output position.
    // Empty unless the projection is tier-addressable; only read while routing.
    private final IntList tierColumns = new IntList();
    private RecordCursor diskCursor;
    private boolean diskExhausted;
    // Set on the first hasNext() after of()/toTop(): once true the disk cursor
    // may have advanced, so skipRows() can no longer take the fresh frame-skip
    // fast path and falls back to the row-by-row default.
    private boolean hasStartedIteration;
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
    // seqTxn) and every projected column resolves to a tier column. Seam routing in
    // hasNext() serves the slot for ts >= seamTs only when this is true.
    private boolean routingEligible;
    private int slotIdx;
    private LiveViewInMemoryTier tier;
    private int timestampColumnIndex;

    public LiveViewRecordCursor() {
        this.slotIdx = -1;
    }

    @Override
    public void close() {
        // Drops the shared getSymbolTable() overlays, which borrow the disk cursor's
        // symbol tables (closed via diskCursor below).
        symbolTableSource.close();
        // Per-record getSym overlays own their cloned disk tables; free them.
        recordA.clearSymbolTables();
        recordB.clearSymbolTables();
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
        // While routing through the tier, a SYMBOL column resolves through an overlay
        // that adds the un-flushed lead's eager-interned symbols on top of the disk
        // reader's committed table - so both a getSymA per-record read and a
        // raw-int-key read (WHERE / GROUP BY / static ORDER BY) see the lead's values.
        // Disk-only reads bind the source in pass-through mode and resolve straight
        // from disk. See LiveViewSymbolTableSource.
        return symbolTableSource.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        hasStartedIteration = true;
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
                    // leadStart == 0: the slot carries NO overlap (every row is un-flushed lead), so
                    // disk holds none of them and there is nothing to cut against - disk must serve
                    // every row it has. Cutting at seamTs (then the lead's own minimum) would drop a
                    // disk row at exactly that ts, served by neither tier. Reachable: an additive
                    // commit whose min ts equals the frontier is not diverted to O3 (strict
                    // below-frontier compare), and a post-restart slot is pure lead. size() and
                    // skipRows() already use diskRouted = diskSize - leadStart, i.e. the whole disk
                    // when leadStart == 0; this restores hasNext() to that same contract.
                    if (leadStart == 0 || ts < pinnedSlot.seamTs()) {
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

    /**
     * Reports whether the pinned in-mem slot is stamped with an LV-table seqTxn
     * strictly NEWER than the disk cursor's snapshot - i.e. the disk reader was
     * opened before the flush that produced the slot. Serving such a read would
     * disengage the fence (slot seqTxn != disk seqTxn) and route disk-only against
     * a STALE, smaller disk snapshot: a live view would then appear to shrink
     * relative to an earlier read that already reflected the flush's rows (the
     * un-flushed lead the earlier read served vanishes without the flushed rows
     * replacing it). {@link LiveViewRecordCursorFactory#getCursor} re-opens the
     * disk cursor against a fresh snapshot while this holds; the slot's flush is
     * already applied (the flush stamps the slot only after applyWalDirect), so a
     * re-opened reader observes at least the slot's seqTxn and the retry converges.
     */
    boolean isSlotNewerThanDisk() {
        return LiveViewRouting.isSlotNewerThanDisk(pinnedSlot, diskReaderSeqTxn(diskCursor));
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return symbolTableSource.newSymbolTable(columnIndex);
    }

    public void of(RecordCursor diskCursor, RecordMetadata baseMetadata, LiveViewInstance instance, int timestampColumnIndex, boolean diskScanAscending) {
        // Take ownership of diskCursor before anything that can throw, so a later
        // failure in of() closes it via close() rather than leaking it back to
        // the caller (getCursor relies on this hand-off point).
        this.diskCursor = diskCursor;
        releaseSlot();
        this.timestampColumnIndex = timestampColumnIndex;
        this.inMemRowsServed = 0;
        this.leadRowsServed = 0;
        this.leadStart = 0;
        this.diskExhausted = false;
        this.hasStartedIteration = false;
        this.inMemRow = -1;
        this.pinnedSlot = null;
        this.inMemEligible = false;
        this.routingEligible = false;
        this.tierColumns.clear();
        // Prior-use overlays are stamped with the previous slot; free them. The
        // shared ones go with the symbolTableSource rebind at the end of of().
        recordA.clearSymbolTables();
        recordB.clearSymbolTables();
        LiveViewSymbolCache symbolCache = null;
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
                    symbolCache = candidate.getSymbolCache();
                    this.inMemEligible = isTierAddressableProjection(diskCursor, baseMetadata, pinnedSlot, timestampColumnIndex, tierColumns);
                    // LONG_NULL means the disk cursor exposes no LV-table seqTxn to fence
                    // against at all: it is not a plain forward table scan (an index scan,
                    // an interval-filtered or pushdown-filtered page-frame cursor, a
                    // non-table cursor). Distinct from a seqTxn that simply disagrees with
                    // the slot's - see the release decision below.
                    final long diskSeqTxn = inMemEligible && diskScanAscending
                            ? diskReaderSeqTxn(diskCursor)
                            : Numbers.LONG_NULL;
                    if (!inMemEligible || !diskScanAscending || diskSeqTxn == Numbers.LONG_NULL) {
                        // Statically disk-only: the projection drops the timestamp or
                        // does not resolve against the tier's columns, or the disk
                        // cursor is non-table (inMemEligible false), the scan is not
                        // ascending (the seam split assumes ascending ts), or the
                        // cursor carries no seqTxn to fence against (diskSeqTxn
                        // LONG_NULL - e.g. SELECT * FROM lv WHERE ts >= '...', whose
                        // interval filter the optimiser pushes into the page-frame scan).
                        // The fence can never engage for this cursor regardless of the
                        // slot's version, and no serving path (hasNext / size /
                        // skipRows / getSymbolTable / newSymbolTable / recordAt) reads
                        // the slot while routingEligible is false. Release the tier
                        // slot now instead of holding the global pin lease + per-slot
                        // rc for the whole cursor lifetime: sustained concurrent
                        // disk-only reads straddling a tier swap would otherwise pin
                        // BOTH slots, so publishToInMemoryTier fails and the refresh
                        // worker emergency-flushes the lead every cycle.
                        //
                        // A version-fence miss (schema + direction OK and the disk cursor
                        // DOES report a seqTxn, but the slot is newer than that snapshot)
                        // is deliberately NOT released here: getCursor's
                        // isSlotNewerThanDisk() staleness retry needs the slot pinned to
                        // detect it and re-open against a fresh snapshot, so that path
                        // keeps the pin (routingEligible stays false and it serves
                        // disk-only for this attempt). A LONG_NULL disk seqTxn cannot
                        // reach that retry - isSlotNewerThanDisk() reads it too - so
                        // holding the pin there would buy nothing.
                        releaseSlot();
                        this.pinnedSlot = null;
                        symbolCache = null;
                    } else {
                        // schema + direction are fine; serve the slot only when it and
                        // the disk reader share an LV-table seqTxn (same version => rows
                        // agree) and the slot actually holds rows. Mismatch / unstamped
                        // => disk-only, but the slot stays pinned for the staleness
                        // retry noted above.
                        this.routingEligible = LiveViewRouting.isFenced(pinnedSlot, diskSeqTxn);
                        // Snapshot the overlap/lead boundary. Rows [0, leadStart) are
                        // the overlap (also on disk, served via the seam split); rows
                        // [leadStart, rowCount) are the un-flushed lead, served only
                        // from RAM. The slot is frozen for the cursor's lifetime, so
                        // this snapshot stays valid.
                        this.leadStart = pinnedSlot.rowCount() - pinnedSlot.leadRowCount();
                    }
                }
            }
        }
        // Overlay the lead's symbols only while the read routes: a disk-only read
        // serves no lead rows, so it has no lead symbols to resolve, and a
        // pass-through binding also keeps getSymbolTable() off a slot that the
        // statically-disk-only branch above already released. A version-fence miss
        // keeps the slot pinned for getCursor's staleness retry but does not route
        // either, so it binds pass-through too.
        symbolTableSource.of(diskCursor, routingEligible ? symbolCache : null, pinnedSlot, tierColumns);
        recordA.bindDisk(diskCursor.getRecord(), this, pinnedSlot, tierColumns);
        recordB.bindDisk(diskCursor.getRecordB(), this, pinnedSlot, tierColumns);
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
            final long diskSize = diskCursor.size();
            if (diskSize < 0) {
                // Never negative for the plain entity scan the fence admits, but
                // propagate "unknown" rather than fold -1 into the seam sum
                // (skipRows guards the same way).
                return -1;
            }
            // leadStart <= rowCount under a passing fence; assert to fail safe.
            assert leadStart <= pinnedSlot.rowCount()
                    : "leadStart " + leadStart + " exceeds slot rowCount " + pinnedSlot.rowCount();
            return diskSize + (pinnedSlot.rowCount() - leadStart);
        }
        // Disk-only: the fence did not engage, so the read serves the applied
        // prefix straight from disk.
        return diskCursor.size();
    }

    @Override
    public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
        if (!routingEligible) {
            // Disk-only: the read is a pure pass-through of the disk cursor, so
            // its own (frame-level) skip applies directly and tracks its own
            // position. This is what a plain table scan already gets, restoring
            // O(frames) skipping for pruned/fenced-off reads through the view.
            diskCursor.skipRows(rowCount, maxRowsAfterSkip);
            return;
        }
        final long toSkip = rowCount.get();
        if (toSkip <= 0) {
            return;
        }
        // Seam routing serves [disk rows with ts < seamTs] then the whole pinned
        // slot [0, rowCount). The frame-level split below assumes a fresh cursor
        // (disk at its top, nothing served yet); the LIMIT rewrite always skips
        // right after toTop(), so that holds. A mid-iteration call (disk already
        // advanced) falls back to the safe row-by-row default, as does a disk
        // cursor that cannot report its size (never a plain page-frame scan while
        // routing, but guard rather than compute a bogus split).
        final long diskSize = diskCursor.size();
        if (hasStartedIteration || diskSize < 0) {
            RecordCursor.super.skipRows(rowCount, maxRowsAfterSkip);
            return;
        }
        // Disk rows before the seam number diskSize - leadStart: the overlap band
        // [0, leadStart) sits at ts >= seamTs and is served from the slot, not
        // disk. This is the same identity size() relies on (size == diskSize +
        // leadRowCount), so the split stays consistent with LIMIT bound math.
        // The overlap band is a subset of the disk prefix; assert to fail safe.
        assert leadStart <= diskSize : "leadStart " + leadStart + " exceeds disk size " + diskSize;
        final long diskRoutedCount = diskSize - leadStart;
        if (toSkip < diskRoutedCount) {
            // Landing inside the disk region: hand the skip to the disk cursor's
            // frame skip. maxRowsAfterSkip (the consumer's post-skip bound) also
            // covers the single seam-probe read hasNext() makes at the boundary
            // (disk reads after the skip never exceed the consumer's bound), so
            // the disk decode window is never clamped short.
            diskCursor.skipRows(rowCount, maxRowsAfterSkip);
            return;
        }
        // The skip spans the entire disk region and lands in the slot. Never walk
        // disk row-by-row: mark it exhausted (hasNext() short-circuits the disk
        // side) and position within the slot directly.
        diskExhausted = true;
        rowCount.dec(diskRoutedCount);
        final long slotSkip = Math.min(rowCount.get(), pinnedSlot.rowCount());
        // hasNext() pre-increments inMemRow, so land one row before the first row
        // to serve. slotSkip == rowCount() leaves the slot exhausted (empty tail).
        inMemRow = slotSkip - 1;
        rowCount.dec(slotSkip);
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
        hasStartedIteration = false;
        inMemRow = -1;
        recordA.toDiskMode();
    }

    // Returns the disk cursor's LV-table seqTxn, or LONG_NULL when the cursor is
    // not a plain FULL table-reader scan we can fence cheaply. Seam routing
    // assumes the disk side yields every LV-table row below the seam in ascending
    // ts order, so any scan shape that under-returns or reorders rows must
    // disengage the fence (fail safe to disk-only). The record path adds two
    // checks LiveViewRouting.diskReaderSeqTxn cannot make from a frame cursor
    // alone: a non-entity row cursor (an indexed or row-filtered scan, should a
    // future change push either into the LV base) under-returns rows the frames
    // still carry, and a backward row cursor breaks the ascending assumption. A
    // non-page-frame plan (LATEST BY, complex factory) returns LONG_NULL too.
    private static long diskReaderSeqTxn(RecordCursor diskCursor) {
        if (diskCursor instanceof PageFrameRecordCursorImpl pfrc
                && pfrc.getRowCursorFactory().isEntity()
                && pfrc.getRowCursorFactory().isForwardScan()) {
            return LiveViewRouting.diskReaderSeqTxn(pfrc.getPageFrameCursor());
        }
        return Numbers.LONG_NULL;
    }

    /**
     * The in-mem tier stores the live view's full output row, so a read that prunes
     * or reorders columns projects a subset of what the slot already holds - the
     * data is there, only the indirection is missing. This resolves every projected
     * column to its tier column through the disk scan's {@link ColumnMapping} and
     * records the result in {@code tierColumnsOut}, which {@link MergedRecord}'s
     * in-mem accessors then read the buffer through. Pruned projections
     * ({@code SELECT ts, x FROM lv}) and reordered ones ({@code SELECT x, ts FROM
     * lv}) therefore route through the tier and see the un-flushed lead.
     * <p>
     * Two shapes stay disk-only. A projection that prunes the designated timestamp
     * leaves {@code timestampColumnIndex < 0}, and the seam split has no timestamp
     * to cut the disk scan on - {@code SELECT max(rn) FROM lv} is the common case.
     * A read whose base is not a plain page-frame scan (an aliasing or expression
     * projection the optimiser fronts with a {@code SelectedRecord} /
     * {@code VirtualRecord}) exposes no column mapping to resolve against. Both
     * serve from disk alone, which is correct because disk holds every applied row -
     * they simply do not see the lead, trailing it by at most one flush cycle.
     * <p>
     * SYMBOL columns are routable: the tier stores LV-table-consistent symbol ids
     * (eager-interned by the refresh worker, see
     * {@link io.questdb.cairo.lv.LiveViewSymbolCache}), so the in-mem branch in
     * {@link MergedRecord#getSymA}/{@link MergedRecord#getSymB} resolves them via
     * {@link #getSymbolTable}'s overlay - committed ids against the disk reader,
     * lead-only ids against the cache. The overlay and the cache key off the TIER
     * column, so they resolve through {@code tierColumnsOut} too.
     */
    private static boolean isTierAddressableProjection(
            RecordCursor diskCursor,
            RecordMetadata baseMetadata,
            LiveViewInMemoryBuffer buffer,
            int timestampColumnIndex,
            IntList tierColumnsOut
    ) {
        if (timestampColumnIndex < 0 || buffer == null) {
            return false;
        }
        // A cursor that is not a page-frame scan (an aliasing or expression projection the
        // optimiser fronts with a SelectedRecord / VirtualRecord) exposes no frame cursor,
        // and so no column mapping to resolve against.
        if (!(diskCursor instanceof PageFrameRecordCursor pfrc)) {
            return false;
        }
        return LiveViewRouting.buildTierColumnMapping(pfrc.getPageFrameCursor(), baseMetadata, buffer, tierColumnsOut);
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
     * {@link #getSymA}/{@link #getSymB} resolve the buffer's stored int via a
     * per-record overlay from {@link RecordCursor#newSymbolTable(int)} (an owning
     * clone), NOT the shared {@link RecordCursor#getSymbolTable(int)} view, so
     * recordA and recordB never share a symbol flyweight (as PageFrameMemoryRecord
     * caches a cloned table per record). The overlay resolves a committed id via the
     * disk reader's table and a lead-only id via the tier's symbol cache.
     * <p>
     * The STRING, BINARY, VARCHAR and ARRAY accessors read from the pinned buffer's
     * per-row offset/header vector while in in-mem mode, mirroring the fixed-width
     * accessors. ARRAY also overrides {@link #getArrayDouble1d2d}, the direct-index
     * fast path a nested {@code SELECT arr[i] FROM (SELECT * FROM lv)} can reach
     * through the routed cursor, so it reads from RAM too rather than delegating to
     * the disk record.
     */
    private static class MergedRecord extends DelegatingRecord {
        // Per-column, A/B var-size read flyweights OWNED by this record. The in-mem
        // tier buffer is shared across every reader cursor pinning a slot (and this
        // cursor's recordA vs recordB), so routing the var-size getters through the
        // buffer's own reusable views would let two consumers re-point and clobber
        // each other's in-flight value - a torn read. Each record instead points its
        // own view into the (pinned, stable) buffer memory, mirroring the disk read
        // path where recordA and recordB delegate to two independent disk records.
        // Lists are lazily populated per column, exactly like PageFrameMemoryRecord.
        private final ObjList<BorrowedArray> arrayViews = new ObjList<>();
        private final ObjList<DirectByteSequenceView> bsViews = new ObjList<>();
        private final ObjList<DirectString> csViewsA = new ObjList<>();
        private final ObjList<DirectString> csViewsB = new ObjList<>();
        private final ObjList<Long256Impl> longs256A = new ObjList<>();
        private final ObjList<Long256Impl> longs256B = new ObjList<>();
        // Per-record OWNING symbol overlays (one clone per SYMBOL column, lazily from
        // cursor.newSymbolTable) so recordA/recordB use independent flyweights; freed
        // via clearSymbolTables on cursor of()/close().
        private final ObjList<SymbolTable> symbolTableCache = new ObjList<>();
        private final ObjList<Utf8SplitString> utf8ViewsA = new ObjList<>();
        private final ObjList<Utf8SplitString> utf8ViewsB = new ObjList<>();
        private LiveViewInMemoryBuffer buffer;
        private long bufferRow;
        private RecordCursor cursor;
        private boolean inMemMode;
        // Output column -> tier column; see LiveViewRecordCursor.tierColumns. Shared
        // with the cursor and recordB - the mapping is fixed for the cursor's life.
        private IntList tierColumns;

        @Override
        public ArrayView getArray(int col, int columnType) {
            return inMemMode ? buffer.getArray(bufferRow, tierCol(col), arrayView(col)) : super.getArray(col, columnType);
        }

        @Override
        public double getArrayDouble1d2d(int col, int columnType, int idx0, int idx1) {
            if (!inMemMode) {
                return super.getArrayDouble1d2d(col, columnType, idx0, idx1);
            }
            // Mirror Record's default getArrayDouble1d2d over the buffer's array view
            // (DelegatingRecord's override would otherwise index the disk record).
            final ArrayView array = buffer.getArray(bufferRow, tierCol(col), arrayView(col));
            if (array.isNull() || idx0 >= array.getDimLen(0)) {
                return Double.NaN;
            }
            if (array.getDimCount() == 1) {
                return array.getDouble(idx0);
            }
            if (idx1 >= array.getDimLen(1)) {
                return Double.NaN;
            }
            return array.getDouble(idx0 * array.getStride(0) + idx1);
        }

        @Override
        public BinarySequence getBin(int col) {
            return inMemMode ? buffer.getBin(bufferRow, tierCol(col), bsView(col)) : super.getBin(col);
        }

        @Override
        public long getBinLen(int col) {
            return inMemMode ? buffer.getBinLen(bufferRow, tierCol(col)) : super.getBinLen(col);
        }

        @Override
        public boolean getBool(int col) {
            return inMemMode ? buffer.getBool(bufferRow, tierCol(col)) : super.getBool(col);
        }

        @Override
        public byte getByte(int col) {
            return inMemMode ? buffer.getByte(bufferRow, tierCol(col)) : super.getByte(col);
        }

        @Override
        public char getChar(int col) {
            return inMemMode ? (char) buffer.getShort(bufferRow, tierCol(col)) : super.getChar(col);
        }

        @Override
        public long getDate(int col) {
            return inMemMode ? buffer.getLong(bufferRow, tierCol(col)) : super.getDate(col);
        }

        @Override
        public void getDecimal128(int col, Decimal128 sink) {
            if (inMemMode) {
                buffer.getDecimal128(bufferRow, tierCol(col), sink);
            } else {
                super.getDecimal128(col, sink);
            }
        }

        @Override
        public short getDecimal16(int col) {
            return inMemMode ? buffer.getDecimal16(bufferRow, tierCol(col)) : super.getDecimal16(col);
        }

        @Override
        public void getDecimal256(int col, Decimal256 sink) {
            if (inMemMode) {
                buffer.getDecimal256(bufferRow, tierCol(col), sink);
            } else {
                super.getDecimal256(col, sink);
            }
        }

        @Override
        public int getDecimal32(int col) {
            return inMemMode ? buffer.getDecimal32(bufferRow, tierCol(col)) : super.getDecimal32(col);
        }

        @Override
        public long getDecimal64(int col) {
            return inMemMode ? buffer.getDecimal64(bufferRow, tierCol(col)) : super.getDecimal64(col);
        }

        @Override
        public byte getDecimal8(int col) {
            return inMemMode ? buffer.getDecimal8(bufferRow, tierCol(col)) : super.getDecimal8(col);
        }

        @Override
        public double getDouble(int col) {
            return inMemMode ? buffer.getDouble(bufferRow, tierCol(col)) : super.getDouble(col);
        }

        @Override
        public float getFloat(int col) {
            return inMemMode ? buffer.getFloat(bufferRow, tierCol(col)) : super.getFloat(col);
        }

        @Override
        public byte getGeoByte(int col) {
            return inMemMode ? buffer.getByte(bufferRow, tierCol(col)) : super.getGeoByte(col);
        }

        @Override
        public int getGeoInt(int col) {
            return inMemMode ? buffer.getInt(bufferRow, tierCol(col)) : super.getGeoInt(col);
        }

        @Override
        public long getGeoLong(int col) {
            return inMemMode ? buffer.getLong(bufferRow, tierCol(col)) : super.getGeoLong(col);
        }

        @Override
        public short getGeoShort(int col) {
            return inMemMode ? buffer.getShort(bufferRow, tierCol(col)) : super.getGeoShort(col);
        }

        @Override
        public int getIPv4(int col) {
            return inMemMode ? buffer.getInt(bufferRow, tierCol(col)) : super.getIPv4(col);
        }

        @Override
        public int getInt(int col) {
            return inMemMode ? buffer.getInt(bufferRow, tierCol(col)) : super.getInt(col);
        }

        @Override
        public long getLong(int col) {
            return inMemMode ? buffer.getLong(bufferRow, tierCol(col)) : super.getLong(col);
        }

        @Override
        public long getLong128Hi(int col) {
            return inMemMode ? buffer.getLong128Hi(bufferRow, tierCol(col)) : super.getLong128Hi(col);
        }

        @Override
        public long getLong128Lo(int col) {
            return inMemMode ? buffer.getLong128Lo(bufferRow, tierCol(col)) : super.getLong128Lo(col);
        }

        @Override
        public void getLong256(int col, CharSink<?> sink) {
            if (inMemMode) {
                buffer.getLong256(bufferRow, tierCol(col), sink);
            } else {
                super.getLong256(col, sink);
            }
        }

        @Override
        public Long256 getLong256A(int col) {
            return inMemMode ? buffer.getLong256(bufferRow, tierCol(col), long256A(col)) : super.getLong256A(col);
        }

        @Override
        public Long256 getLong256B(int col) {
            return inMemMode ? buffer.getLong256(bufferRow, tierCol(col), long256B(col)) : super.getLong256B(col);
        }

        @Override
        public long getLongIPv4(int col) {
            // Override DelegatingRecord's disk-record delegation so an in-mem IPv4
            // (tier-stored as an int) resolves from RAM. No caller today; tier type.
            return inMemMode ? Numbers.ipv4ToLong(buffer.getInt(bufferRow, tierCol(col))) : super.getLongIPv4(col);
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
            return inMemMode ? buffer.getShort(bufferRow, tierCol(col)) : super.getShort(col);
        }

        @Override
        public CharSequence getStrA(int col) {
            return inMemMode ? buffer.getStr(bufferRow, tierCol(col), csViewA(col)) : super.getStrA(col);
        }

        @Override
        public CharSequence getStrB(int col) {
            return inMemMode ? buffer.getStr(bufferRow, tierCol(col), csViewB(col)) : super.getStrB(col);
        }

        @Override
        public int getStrLen(int col) {
            return inMemMode ? buffer.getStrLen(bufferRow, tierCol(col)) : super.getStrLen(col);
        }

        @Override
        public CharSequence getSymA(int col) {
            if (!inMemMode) {
                return super.getSymA(col);
            }
            return recordSymbolTable(col).valueOf(buffer.getInt(bufferRow, tierCol(col)));
        }

        @Override
        public CharSequence getSymB(int col) {
            if (!inMemMode) {
                return super.getSymB(col);
            }
            // valueBOf (not valueOf) so getSymA and getSymB of the same row use two
            // flyweights; with a NOCACHE column they are distinct reused instances.
            // The overlay is per-record, so recordA/recordB do not clobber either.
            return recordSymbolTable(col).valueBOf(buffer.getInt(bufferRow, tierCol(col)));
        }

        @Override
        public long getTimestamp(int col) {
            return inMemMode ? buffer.getLong(bufferRow, tierCol(col)) : super.getTimestamp(col);
        }

        @Override
        public void getVarchar(int col, Utf16Sink utf16Sink) {
            // Override DelegatingRecord's disk delegation so an in-mem varchar resolves
            // from RAM (Record's default over getVarcharA). No caller today; tier type.
            if (inMemMode) {
                utf16Sink.put(getVarcharA(col));
            } else {
                super.getVarchar(col, utf16Sink);
            }
        }

        @Override
        public Utf8Sequence getVarcharA(int col) {
            return inMemMode ? buffer.getVarchar(bufferRow, tierCol(col), utf8ViewA(col)) : super.getVarcharA(col);
        }

        @Override
        public Utf8Sequence getVarcharB(int col) {
            return inMemMode ? buffer.getVarchar(bufferRow, tierCol(col), utf8ViewB(col)) : super.getVarcharB(col);
        }

        @Override
        public int getVarcharSize(int col) {
            return inMemMode ? buffer.getVarcharSize(bufferRow, tierCol(col)) : super.getVarcharSize(col);
        }

        void bindDisk(Record diskRecord, RecordCursor cursor, LiveViewInMemoryBuffer buffer, IntList tierColumns) {
            this.base = diskRecord;
            this.cursor = cursor;
            this.buffer = buffer;
            this.tierColumns = tierColumns;
            this.bufferRow = -1;
            this.inMemMode = false;
        }

        void clearSymbolTables() {
            // Overlays own their cloned disk tables (ownsBase=true) and are stamped
            // with the pinned slot, so free them - they must not outlive a cursor of().
            Misc.freeObjListIfCloseable(symbolTableCache);
            symbolTableCache.clear();
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

        private BorrowedArray arrayView(int col) {
            BorrowedArray view = arrayViews.getQuiet(col);
            if (view == null) {
                arrayViews.extendAndSet(col, view = new BorrowedArray());
            }
            return view;
        }

        private DirectByteSequenceView bsView(int col) {
            DirectByteSequenceView view = bsViews.getQuiet(col);
            if (view == null) {
                bsViews.extendAndSet(col, view = new DirectByteSequenceView());
            }
            return view;
        }

        private DirectString csViewA(int col) {
            DirectString view = csViewsA.getQuiet(col);
            if (view == null) {
                csViewsA.extendAndSet(col, view = new DirectString());
            }
            return view;
        }

        private DirectString csViewB(int col) {
            DirectString view = csViewsB.getQuiet(col);
            if (view == null) {
                csViewsB.extendAndSet(col, view = new DirectString());
            }
            return view;
        }

        private Long256Impl long256A(int col) {
            Long256Impl view = longs256A.getQuiet(col);
            if (view == null) {
                longs256A.extendAndSet(col, view = new Long256Impl());
            }
            return view;
        }

        private Long256Impl long256B(int col) {
            Long256Impl view = longs256B.getQuiet(col);
            if (view == null) {
                longs256B.extendAndSet(col, view = new Long256Impl());
            }
            return view;
        }

        private SymbolTable recordSymbolTable(int col) {
            SymbolTable symbolTable = symbolTableCache.getQuiet(col);
            if (symbolTable == null) {
                // Owning per-record overlay (disk clone + lead cache), as
                // PageFrameMemoryRecord does, so cross-record getSymA does not tear
                // (a NOCACHE disk table reuses one DirectString per band).
                symbolTable = cursor.newSymbolTable(col);
                symbolTableCache.extendAndSet(col, symbolTable);
            }
            return symbolTable;
        }

        // The tier column output column col reads. Only ever called in in-mem mode,
        // which the cursor enters solely for a tier-addressable projection, so the
        // mapping always covers col. The per-column read flyweights above stay keyed
        // by OUTPUT column: a projection may repeat a tier column (SELECT ts, x, x),
        // and two output columns must not share one flyweight.
        private int tierCol(int col) {
            return tierColumns.getQuick(col);
        }

        private Utf8SplitString utf8ViewA(int col) {
            Utf8SplitString view = utf8ViewsA.getQuiet(col);
            if (view == null) {
                utf8ViewsA.extendAndSet(col, view = new Utf8SplitString());
            }
            return view;
        }

        private Utf8SplitString utf8ViewB(int col) {
            Utf8SplitString view = utf8ViewsB.getQuiet(col);
            if (view == null) {
                utf8ViewsB.extendAndSet(col, view = new Utf8SplitString());
            }
            return view;
        }
    }
}
