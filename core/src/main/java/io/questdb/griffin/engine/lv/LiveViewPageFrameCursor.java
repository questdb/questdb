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
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.ReaderScanProfile;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewSymbolCache;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.FwdTableReaderPageFrameCursor;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.griffin.engine.table.parquet.ParquetDecoder;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;

/**
 * Exposes a live view's two tiers as one page-frame stream: the LV table's own
 * disk frames plus synthetic frames over the pinned in-memory slot. It is the
 * page-frame twin of {@link LiveViewRecordCursor}'s routing modes, and serves
 * exactly the same rows in the same order - so a read routed through here sees the
 * un-flushed lead without giving up the engine's frame machinery (the parallel
 * filter, the JIT filter, LIMIT pushdown), which the record-cursor path cannot
 * offer.
 * <p>
 * <b>The modes</b>, picked in {@link #of} from the direction the base's frames arrive in
 * and whether it carries an interval filter, mirror the record path's own (see
 * {@link LiveViewRecordCursor}'s {@code ROUTING_*} constants). A slot's rows split at
 * {@code leadStart = rowCount - leadRowCount}: rows {@code [0, leadStart)} are the
 * overlap (also on disk), rows {@code [leadStart, rowCount)} the un-flushed lead.
 * <ul>
 *   <li><b>Seam split</b>, for an ascending unfiltered frame stream: the disk scan's
 *   leading {@code base.size() - leadStart} frames, then the whole slot. Under the seqTxn
 *   fence the overlap band IS the disk scan's trailing {@code leadStart} rows - the
 *   ones at or above {@code seamTs} - so this covers every row exactly once, disk
 *   below the seam and slot at or above it, and serves the LV table's hot tail
 *   partition(s) from RAM instead.</li>
 *   <li><b>Lead-only descending</b>, for a descending one: the lead band alone,
 *   reversed, then the disk scan in FULL. Disk holds every applied row and the lead
 *   holds exactly what disk lacks, so the union still covers every row once - with no
 *   cut to take. It gives up the hot-tail skip, which is the right trade for a read the
 *   seam cannot serve at all: the seam's cut takes the scan's LEADING rows, which over a
 *   descending stream are the newest rather than the oldest, so it would serve the slot
 *   on top of rows it already yielded.</li>
 *   <li><b>Lead-only forward</b>, for an ascending stream carrying an INTERVAL filter: the
 *   disk scan in full, then the lead band. The interval narrows the base scan BENEATH this
 *   cursor, so the scan's trailing rows are no longer the slot's overlap and the seam's
 *   row-count cut has nothing sound to cut against - it also leaves the scan unable to
 *   size itself at all. Lead-only depends on neither.</li>
 * </ul>
 * <b>The interval filter cuts the slot too</b>, which is what makes such a read routable at
 * all rather than merely orderable. The base applies it to its own frames; {@link #of}
 * applies the same list to the slot's band through {@link LiveViewIntervalBands}, so the
 * frames tile the surviving row sub-bands and the two tiers answer to one filter. Serving
 * the band whole instead would emit lead rows the query excluded - wrong results, not stale
 * ones. Every other mode's band is uncut, which is simply the one-band case of the same
 * machinery.
 * <p>
 * <b>The overlap/lead split is taken by ROW COUNT</b> rather than by comparing each row's
 * timestamp against {@code seamTs}. The two are the same boundary, but the row count is the
 * identity {@link LiveViewRecordCursor#size} and {@code skipRows} already use, so all
 * three agree by construction rather than by an invariant holding; it also needs no
 * timestamp read, which a parquet frame or a metadata-only skip frame cannot serve
 * anyway. It disposes of the lead/disk timestamp tie for free - the split is by row, so
 * a lead row sharing a timestamp with a disk row is still a distinct row, and no mode may
 * assert a strict {@code >} on that boundary. {@code leadStart == 0} (a slot that is pure
 * lead) collapses the seam to "disk serves everything", matching
 * {@link LiveViewRecordCursor#hasNext}'s branch for it. The INTERVAL cut above is the one
 * timestamp-driven boundary here, and it is a different question - which rows the query
 * asked for, not which tier holds them - so it is taken against the slot's own ts ladder
 * and never against disk.
 * <p>
 * <b>Why descending serves the lead FIRST.</b> The bound is
 * {@code min(lead ts) >= max(disk ts)}, so the reversed lead runs down to the on-disk
 * maximum and the disk scan continues down from it: the stream stays non-increasing
 * across the band boundary. Equal-ts neighbours across it need no particular relative
 * order.
 * <p>
 * <b>Slot lifetime.</b> The cursor carries the tier pin for its whole life and
 * drops it in {@link #close()}. The synthetic frame publishes the slot's native
 * addresses directly, so anything that can still read a frame - a parallel filter
 * worker above all - must not outlive this cursor. That is a memory-safety
 * constraint, not a staleness one: a worker touching a released slot reads freed
 * native memory.
 * <p>
 * <b>SYMBOL resolution</b> goes through {@link LiveViewSymbolTableSource}, bound
 * once here, so a frame consumer that resolves symbols without a record (again, a
 * filter worker) still sees the lead's eager-interned values.
 * <p>
 * The cursor takes ownership of the base cursor and the tier pin in {@link #of},
 * first thing, so any later failure releases both through {@link #close()}.
 * <p>
 * <b>Why it is a {@link TablePageFrameCursor}.</b> Both tiers belong to the LV's own
 * table, but the reason is mechanical: consumers that treat a frame source as a table -
 * a WINDOW / HORIZON JOIN slave, the {@code SelectedRecordCursorFactory} and
 * {@code ExtraNullColumnCursorFactory} projections - cast what
 * {@link LiveViewRecordCursorFactory#getPageFrameCursor} hands back to this interface.
 * The table-shaped members delegate to the base cursor, which the routing fence has
 * already established is a plain table scan.
 * <p>
 * <b>The two scoped walks.</b> A consumer that models this cursor as a table asks for its
 * frames one group at a time rather than as the mode's stream: {@link #toPartition} hands
 * out one disk partition's frames, {@link #toLeadFrames} the un-flushed lead's. Together
 * they cover every row exactly once - disk holds every applied row, the lead exactly what
 * disk lacks - which is lead-only's own union argument, reached through a different door.
 * The seam has no place there and neither scope offers it: a caller assembling the whole
 * disk scan from partitions has already counted the slot's overlap.
 */
public class LiveViewPageFrameCursor implements TablePageFrameCursor {
    // Partition index the synthetic slot frame reports. Its only consumer is the
    // frame's rowIdOffset (PageFrameAddressCache.add -> Rows.toRowID(partitionIndex,
    // partitionLo)), which in turn feeds PageFrameMemoryRecord.getUpdateRowId() alone -
    // an LV is not updatable, so nothing reads it today. Row ids on this path are
    // FRAME-encoded (PageFrameMemoryRecord.getRowId is Rows.toRowID(frameIndex,
    // rowIndex)), so the slot frame needs no partition of its own to be addressable;
    // it gets a frame index like any other frame. Reserving the top safe partition
    // index rather than reusing 0 keeps a lead row's update row id from aliasing a
    // real disk row's should a consumer ever read one.
    private static final int SLOT_PARTITION_INDEX = Rows.MAX_SAFE_PARTITION_INDEX;
    // The scopes next() can walk in, set by toLeadFrames() / toPartition() and reset by
    // toTop(). One field rather than a flag each, so they are exclusive by construction: a
    // consumer that walks the table asks for one disk partition or the lead, never both at
    // once, and nothing rests on the order two flags happen to be tested in.
    private static final int WALK_LEAD = 2;
    private static final int WALK_PARTITION = 1;
    private static final int WALK_ROUTED = 0;
    // The slot's LEAD rows - the un-flushed ones, held nowhere on disk - cut by the base
    // scan's interval filter, as flat half-open (lo, hi) row pairs. Same shape and same cut
    // as slotBands, and identical to it under either lead-only mode; they part company under
    // the seam, whose band is the whole slot - though only when the slot carries a flushed
    // prefix, since a seam with leadStart == 0 leaves the two bands identical again. Built
    // in of() regardless of the mode, because
    // the consumer that walks these does not take its disk side from this cursor's stream at
    // all - see toLeadFrames().
    private final LongList leadBands = new LongList();
    private final SeamCutPageFrame seamCutFrame = new SeamCutPageFrame();
    private final SlotPageFrame slotFrame = new SlotPageFrame();
    // The slot rows this cursor serves, as flat half-open (lo, hi) row pairs, ascending and
    // disjoint: the mode's band ([0, rowCount) under the seam, [leadStart, rowCount) under
    // lead-only) cut down by the base scan's interval filter, if it carries one. Built in
    // of(); the frames tile these bands and nothing else. One band is the common case - an
    // interval filter is the only thing that ever produces more, or fewer.
    private final LongList slotBands = new LongList();
    // Resolves the read's SYMBOL columns: the base frame cursor's tables overlaid with
    // the pinned slot's eager-interned lead symbols. Bound once in of().
    private final LiveViewSymbolTableSource symbolTableSource = new LiveViewSymbolTableSource();
    // Output column -> tier column, one entry per projected column. The tier stores the
    // LV's full output row, so a pruned or reordered projection reads a subset of the
    // slot's columns through this indirection. Copied in of() so the cursor owns a
    // snapshot the caller cannot mutate underneath it.
    private final IntList tierColumns = new IntList();
    private TablePageFrameCursor base;
    // Disk rows this cursor serves, snapshotted in of(): the scan's leading rows strictly
    // below the seam (base.size() - leadStart) under the seam, the whole scan under
    // lead-only. NEGATIVE when the base cannot size itself, which an interval-filtered scan
    // never can - that is the signal the disk band is bounded by the base running out
    // rather than by a row count, and every read of this field forks on it.
    private long diskRoutedRows;
    // Disk rows covered by the frames returned so far. Meaningless while diskRoutedRows is
    // negative: an unmeasured band has nothing to measure progress against.
    private long diskRowsEmitted;
    private boolean isDiskExhausted;
    // The routing mode, snapshotted in of() from the direction the base's frames arrive
    // in: lead-only descending when set (the reversed lead band, then the disk scan in
    // full), the seam split when not. See the class doc.
    private boolean isLeadOnlyDescending;
    // Index of the band in leadBands the lead-scoped walk is tiling, and the rows it has
    // covered within that band. Kept apart from the slotBand* pair so a lead-scoped walk
    // leaves the mode's own walk exactly where it found it.
    private int leadBandIdx;
    private long leadBandRowsEmitted;
    // Rows per synthetic frame over the lead band, sized in of() by the same helper - and so
    // by the same bounds - a disk partition holding that many rows gets. Always >= 1.
    private long leadRowLimit;
    private LiveViewInMemoryBuffer slot;
    // Index of the band in slotBands the frames are currently tiling. Moves up under an
    // ascending walk and down under a descending one; out of range means the slot side is
    // exhausted.
    private int slotBandIdx;
    // Slot rows covered by the frames returned so far WITHIN the current band. Reset on
    // every band change; slotRowsEmitted carries the total across bands.
    private long slotBandRowsEmitted;
    private int slotIdx;
    // The pinned slot's row count, snapshotted in of(). The slot is frozen for the
    // cursor's life, but snapshotting keeps the frame's row range and size() consistent
    // with the diskRoutedRows the same call computed.
    private long slotRowCount;
    // Rows per synthetic slot frame, sized in of() by the same helper the native cursor
    // sizes a partition's frames with. Always >= 1.
    private long slotRowLimit;
    // Slot rows covered by the frames returned so far.
    private long slotRowsEmitted;
    private LiveViewInMemoryTier tier;
    // Which frames next() hands out: the routing mode's own stream (WALK_ROUTED), one disk
    // partition's (WALK_PARTITION), or the lead's (WALK_LEAD). See the constants.
    private int walkScope;

    public LiveViewPageFrameCursor() {
        this.slotIdx = -1;
    }

    /**
     * Counts the rows this cursor has NOT yet returned frames for, MINUS the rows
     * {@link #getRemainingRowsInInterval()} already reports. That split is the
     * contract {@code PageFrameRecordCursorImpl.calculateSize} implements - it adds
     * {@code getRemainingRowsInInterval()} itself and then calls this - and it is what
     * the base cursor does too, so this mirrors it rather than the interface javadoc's
     * looser wording. The two together add whatever is left of the mode's two bands, and
     * they say nothing about the order the bands go out in - a lead-only descending read
     * has already served its slot band by the time it reaches the disk one.
     */
    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        if (!isDiskExhausted) {
            if (diskRoutedRows >= 0) {
                // Order matters: getRemainingRowsInInterval() reads the state this branch
                // then advances.
                final long remainingInInterval = getRemainingRowsInInterval();
                counter.add(diskRoutedRows - diskRowsEmitted - remainingInInterval);
                diskRowsEmitted = diskRoutedRows;
            } else {
                // No row count to subtract from: hand the whole question to the base, which
                // nets off its own getRemainingRowsInInterval() exactly as this does. This
                // is the ONLY way to count an interval-filtered scan - no size() reports its
                // rows - and equally it is the only case that may delegate, since a base
                // that CAN size itself counts through size() and leaves calculateSize a
                // no-op (AbstractFullPartitionFrameCursor never overrides it).
                base.calculateSize(counter);
            }
            isDiskExhausted = true;
        }
        final long slotRoutedRows = LiveViewIntervalBands.countRows(slotBands);
        if (slotRowsEmitted < slotRoutedRows) {
            counter.add(slotRoutedRows - slotRowsEmitted);
            slotRowsEmitted = slotRoutedRows;
            // The bands are counted whole now, so leave the walk with nothing to hand out.
            slotBandIdx = isLeadOnlyDescending ? -1 : slotBands.size() / 2;
        }
    }

    @Override
    public void close() {
        // Drops the shared overlays first: they borrow the base cursor's symbol tables,
        // which the base frees on its own close() below.
        symbolTableSource.close();
        releaseSlot();
        slot = null;
        base = Misc.free(base);
    }

    @Override
    public ColumnMapping getColumnMapping() {
        // The slot frame resolves through tierColumns rather than through a mapping, so
        // the base's is the only one in play - and it is the mapping tierColumns was
        // itself built from.
        return base.getColumnMapping();
    }

    @Override
    public LongList getIntervals() {
        // See hasIntervalFilter(): the base's intervals describe both tiers' rows, because
        // of() cut the slot's band by this very list.
        return base.getIntervals();
    }

    @Override
    public long getRemainingRowsInInterval() {
        if (isDiskExhausted) {
            // Nothing is left behind the slot for this to report: the un-emitted slot rows
            // are what calculateSize counts, and the two must not both count them. The
            // contract only binds their SUM - PageFrameRecordCursorImpl.calculateSize adds
            // this and then calls calculateSize - so leaving the whole slot to the latter
            // keeps the two halves disjoint whether the slot is one frame or many.
            return 0;
        }
        if (diskRoutedRows < 0) {
            // Nothing to clamp against: the base's frames are this cursor's disk band in
            // full, so its answer is already this cursor's.
            return base.getRemainingRowsInInterval();
        }
        // The base counts every row left in its CURRENT partition; this cursor may serve
        // fewer (the seam's band stops short of the scan's end), so clamp to what is left
        // of the disk band. A lead-only descending read that is still on its slot band has
        // not asked the base for a frame yet, so the base reports 0 and so does this -
        // which leaves both bands to calculateSize, exactly as the sum contract wants.
        return Math.min(base.getRemainingRowsInInterval(), diskRoutedRows - diskRowsEmitted);
    }

    @Override
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        // Safe by construction: the source hands back either a LiveViewSymbolTable
        // overlay (a StaticSymbolTable) or the base's own table, and the base is a
        // PageFrameCursor, whose getSymbolTable is already declared StaticSymbolTable.
        return (StaticSymbolTable) symbolTableSource.getSymbolTable(columnIndex);
    }

    @Override
    public TableReader getTableReader() {
        // The LV's on-disk tier. The in-mem slot has no reader of its own, and the only
        // consumers that ask are the partition-scoped ones - see toPartition().
        return base.getTableReader();
    }

    @Override
    public boolean hasActivePushdownFilter() {
        return base.hasActivePushdownFilter();
    }

    @Override
    public boolean hasIntervalFilter() {
        // The interval this reports is applied to BOTH tiers: the base applies it to its
        // own frames, and of() cut the slot's band by the same list. So the answer is the
        // base's, and it describes this whole cursor's stream.
        return base.hasIntervalFilter();
    }

    @Override
    public boolean hasLeadFrames() {
        // The un-flushed lead: rows the refresh worker has computed but not yet written to
        // the LV's on-disk tier, so no partition of the LV table holds them. Empty right
        // after a flush, and empty when an interval filter admits none of them - the
        // ordinary steady state, not a corner.
        return leadBands.size() > 0;
    }

    @Override
    public boolean isExternal() {
        // Both tiers belong to the LV's own table; neither is an external parquet file.
        return base.isExternal();
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return symbolTableSource.newSymbolTable(columnIndex);
    }

    @Override
    public @Nullable PageFrame next(long skipTarget) {
        return switch (walkScope) {
            // A partition-scoped walk is a disk-tier walk; see toPartition().
            case WALK_PARTITION -> base.next(skipTarget);
            // A lead-scoped walk is a tier walk, and the disk side stays out of it; see
            // toLeadFrames().
            case WALK_LEAD -> nextLeadFrame();
            default -> nextRoutedFrame(skipTarget);
        };
    }

    /**
     * Binds the cursor to a read. Takes ownership of {@code base} and of the tier pin
     * ({@code tier} / {@code slotIdx}) before anything that can throw, so a later
     * failure releases both through {@link #close()} rather than stranding them.
     * <p>
     * The caller must have pinned {@code slotIdx} through
     * {@link LiveViewInMemoryTier#acquireRead()} and must have established the routing
     * preconditions {@link LiveViewRouting} holds: a tier-addressable projection (which is
     * what {@code tierColumns} records), a base scan that either applies no filter or
     * applies an interval it can hand back, and the seqTxn fence (slot and disk reader on
     * the same LV-table version). Routing a read that fails any of them through here would
     * over-return the slot's rows against a disk scan they do not line up with. Neither the
     * scan direction nor the interval is among them - they pick the MODE instead, through
     * {@code isDescending} and {@link LiveViewRouting#diskIntervals} respectively.
     *
     * @param executionContext the read's context; sizes the slot's frames through the row
     *                         bounds {@code changePageFrameSizes} may have narrowed
     * @param base             the LV table's own frame cursor; owned from here on
     * @param isDescending     whether {@code base} yields its frames newest-first, which
     *                         selects lead-only descending over the seam split. It is the
     *                         consumer's requested frame order, not the base factory's
     *                         natural scan direction - see
     *                         {@link LiveViewRecordCursorFactory#getPageFrameCursor}.
     * @param tier             the pinned tier; its pin is released by {@link #close()}
     * @param slotIdx          the pinned slot's index, as {@link LiveViewInMemoryTier#acquireRead()} returned it
     * @param slot             the pinned slot, i.e. {@code tier.getSlot(slotIdx)}
     * @param symbolCache      the tier's eager-interning symbol cache, for lead-only SYMBOL ids
     * @param tierColumns      output column -> tier column; copied
     */
    public LiveViewPageFrameCursor of(
            SqlExecutionContext executionContext,
            TablePageFrameCursor base,
            boolean isDescending,
            LiveViewInMemoryTier tier,
            int slotIdx,
            LiveViewInMemoryBuffer slot,
            LiveViewSymbolCache symbolCache,
            @Transient IntList tierColumns
    ) {
        // Ownership hand-off first: close() is the only thing that releases either, and
        // it can only do so once these are set.
        this.base = base;
        this.tier = tier;
        this.slotIdx = slotIdx;
        this.slot = slot;
        this.tierColumns.clear();
        this.tierColumns.addAll(tierColumns);
        this.slotRowCount = slot.rowCount();
        final long diskSize = base.size();
        final long rawLeadStart = slotRowCount - slot.leadRowCount();
        // The overlap band is a suffix of the disk scan, so it cannot run negative. Assert to
        // fail loudly under -ea, but clamp as well: -ea is off in production, and a negative
        // leadStart inflates the seam's disk band past the scan.
        assert rawLeadStart >= 0 : "leadStart " + rawLeadStart + " is negative";
        final long leadStart = Math.max(0, rawLeadStart);
        // The intervals the base scan confines its rows to, if any. They select the mode as
        // much as the direction does: an interval narrows the base scan BENEATH this cursor,
        // so the scan's trailing rows are no longer the slot's overlap band and the seam's
        // row-count cut has nothing sound to cut against. Lead-only needs no such identity.
        final LongList intervals = LiveViewRouting.diskIntervals(base);
        this.isLeadOnlyDescending = isDescending;
        final long slotBandLo;
        // The seam needs a SIZED base - the disk band's row count IS its cut - and an
        // unfiltered ascending one. An interval-filtered scan is neither: it cannot size
        // itself, and it narrows the base out from under the cut. Lead-only needs neither,
        // so every other shape falls back to it rather than to disk-only, which also makes
        // an unsized base degrade instead of mis-cutting.
        final boolean isSeamShape = !isDescending && intervals == null && diskSize >= 0;
        // Under the seam shape the overlap band is a suffix of the disk scan, so it cannot
        // exceed it. Assert to fail loudly under -ea, and degrade to lead-only otherwise: with
        // -ea off a negative cut makes every reader read diskRoutedRows as "cannot size", so the
        // disk frames pass through in full AND the slot band is served on top - rows emitted
        // twice, with size() returning -1 so nothing cross-checks it.
        assert !isSeamShape || leadStart <= diskSize : "leadStart " + leadStart + " exceeds disk size " + diskSize;
        if (!isSeamShape || leadStart > diskSize) {
            // Lead-only: disk serves every row it kept and the slot adds the lead band
            // alone, so there is no cut to take and no overlap to skip. diskRoutedRows
            // carries the base's own -1 through when it cannot size itself.
            slotBandLo = leadStart;
            this.diskRoutedRows = diskSize;
        } else {
            // The seam: the slot's overlap band stands in for the disk scan's trailing
            // leadStart rows, so the disk band stops leadStart rows short of its end.
            slotBandLo = 0;
            this.diskRoutedRows = diskSize - leadStart;
        }
        // The slot rows this read serves: the mode's band, cut down by the base scan's own
        // intervals so both tiers answer to the same filter. Without that cut a routed
        // interval-filtered read would emit lead rows the query excluded - wrong results,
        // not stale ones.
        slotBands.clear();
        LiveViewIntervalBands.cut(slot, slot.getTimestampColumnIndex(), slotBandLo, slotRowCount, intervals, slotBands);
        // The lead band, cut by the same intervals, for the lead-scoped walk toLeadFrames()
        // hands out. It is the mode's own band again under either lead-only mode, and a
        // suffix of it under the seam.
        //
        // Keyed on the bounds, not on the mode flag: that keeps this independent of which
        // fork of() picked - the reason the two cuts were kept separate - while dropping the
        // redundant pass. Under lead-only slotBandLo IS leadStart, so the second cut walks
        // the same interval list over the same rows and lands on the same bands. That is
        // precisely the case worth skipping: the seam shape only exists when intervals ==
        // null, where cut() returns in O(1), whereas lead-only is the mode an
        // interval-filtered scan falls back to - the one where cut() actually binary-searches.
        leadBands.clear();
        if (slotBandLo == leadStart) {
            leadBands.addAll(slotBands);
        } else {
            LiveViewIntervalBands.cut(slot, slot.getTimestampColumnIndex(), leadStart, slotRowCount, intervals, leadBands);
        }
        // The slot band is this scan's last partition, so it splits into frames the same
        // way a disk partition of the same row count does - same helper, same bounds, same
        // trailing-frame rounding. Sharing it is not just tidiness: those bounds carry the
        // engine's hard cap on a frame's row count (Map.BATCH_ROW_INDEX_MASK + 1, the width
        // of the frame-relative row index a batched GROUP BY packs into its entries), which
        // a slot published as one frame would breach for a wide enough IN MEMORY window.
        this.slotRowLimit = FwdTableReaderPageFrameCursor.calculatePageFrameRowLimit(
                0,
                LiveViewIntervalBands.countRows(slotBands),
                executionContext.getPageFrameMinRows(),
                executionContext.getPageFrameMaxRows(),
                executionContext.getSharedQueryWorkerCount()
        );
        // Sized off the LEAD band's own row count rather than shared with slotRowLimit: the
        // seam's band is the whole slot, so its limit answers to a bigger partition than the
        // lead-scoped walk actually tiles.
        this.leadRowLimit = FwdTableReaderPageFrameCursor.calculatePageFrameRowLimit(
                0,
                LiveViewIntervalBands.countRows(leadBands),
                executionContext.getPageFrameMinRows(),
                executionContext.getPageFrameMaxRows(),
                executionContext.getSharedQueryWorkerCount()
        );
        // The overlay resolves against the slot this cursor pins, for as long as it pins
        // it - which is exactly the cursor's life, and the life of every frame it hands
        // out.
        symbolTableSource.of(base, symbolCache, slot, this.tierColumns);
        toTop();
        return this;
    }

    /**
     * Unsupported: the base factory's {@code getPageFrameCursor()} opens the disk scan and
     * this cursor wraps the already-opened result through {@link #of}, so there is no
     * partition frame cursor to bind here. Same shape as the other page-frame wrappers
     * (see {@code SelectedRecordCursorFactory.SelectedPageFrameCursor}).
     */
    @Override
    public TablePageFrameCursor of(SqlExecutionContext executionContext, PartitionFrameCursor partitionFrameCursor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseOpenPartitions() {
        base.releaseOpenPartitions();
    }

    @Override
    public void setScanProfile(ReaderScanProfile profile) {
        base.setScanProfile(profile);
    }

    @Override
    public long size() {
        if (diskRoutedRows < 0) {
            // The base cannot size itself, and an INTERVAL-filtered scan never can - the
            // native interval cursor would have to walk its partitions to know. Propagate
            // "unknown" rather than fold -1 into the sum; calculateSize() still answers.
            return -1;
        }
        // The mode's disk band plus its slot band. With no interval the two modes agree on
        // the total - the seam serves (diskSize - leadStart) disk rows plus the whole slot,
        // lead-only every disk row plus the lead alone, and both come to base.size() +
        // leadRowCount, because the slot's overlap is on disk either way and only the
        // un-flushed lead sits on top. That agreement is why this used to need no mode
        // switch; it holds only while the slot's band is served WHOLE, so the slot side is
        // taken from the bands now that an interval can cut it. Same quantity
        // LiveViewRecordCursor.size() reports, so a routed read sizes the same through
        // either path and either mode.
        return diskRoutedRows + LiveViewIntervalBands.countRows(slotBands);
    }

    @Override
    public boolean supportsSizeCalculation() {
        return base.supportsSizeCalculation();
    }

    /**
     * Scopes the walk to the un-flushed LEAD - the rows no partition of the LV table holds -
     * which drops the disk tier out of it: {@link #next} tiles {@link #leadBands} and nothing
     * else until the next {@link #toTop}. The mirror image of {@link #toPartition}, and the
     * other half of what a time-frame consumer needs.
     * <p>
     * Such a consumer ({@code TimeFrameCursorImpl}, {@code ConcurrentTimeFrameState})
     * addresses frames by PARTITION index and builds its disk side from the table reader's
     * own per-partition row counts rather than from this cursor's stream. So it takes the
     * whole disk scan whatever mode {@link #of} picked, and the only thing left for the tier
     * to add is the lead - never the seam's whole slot band, whose overlap that consumer
     * already counted on disk. That is why the band this hands out is {@link #leadBands}
     * rather than {@link #slotBands}, and why it is right regardless of the mode.
     * <p>
     * The frames are the same synthetic ones the unscoped walk publishes, so the pin's
     * lifetime rule is unchanged: nothing that can still read one may outlive this cursor.
     */
    @Override
    public void toLeadFrames() {
        walkScope = WALK_LEAD;
        leadBandIdx = 0;
        leadBandRowsEmitted = 0;
    }

    /**
     * Scopes the walk to one of the LV table's disk partitions, which drops the tier out of
     * the read: {@link #next} hands straight to the base until the next {@link #toTop}.
     * <p>
     * The in-mem slot is not a partition of that table and has no index in its space, so
     * there is no partition this cursor could answer with the slot frame. The consumer that
     * asks is a time-frame model ({@code ConcurrentTimeFrameState}, {@code
     * TimeFrameCursorImpl}), which derives its whole frame model from the table reader's
     * per-partition row counts before walking a partition to patch in the addresses - a
     * model the slot frame is invisible to, and one whose frame-count assert a surprise
     * extra frame would trip. It reaches the lead through {@link #toLeadFrames} instead,
     * which gives those rows a pseudo-partition of their own rather than smuggling them into
     * a real partition's frames.
     * <p>
     * The tier pin stays held for the cursor's life regardless. It buys this walk nothing,
     * but the symbol overlay {@link #of} bound resolves against the slot, and a consumer may
     * still call {@link #getSymbolTable}.
     */
    @Override
    public void toPartition(int partitionIndex) {
        walkScope = WALK_PARTITION;
        base.toPartition(partitionIndex);
    }

    @Override
    public void toTop() {
        base.toTop();
        walkScope = WALK_ROUTED;
        isDiskExhausted = false;
        diskRowsEmitted = 0;
        slotRowsEmitted = 0;
        slotBandRowsEmitted = 0;
        // Park the band walk at the end the mode starts from. An empty band list (an
        // interval filter admitting none of the slot's rows) leaves the index out of range
        // either way, which reads as exhausted straight away.
        slotBandIdx = isLeadOnlyDescending ? slotBands.size() / 2 - 1 : 0;
    }

    // The disk band's next frame, or null once the band is spent. Under the seam the band
    // is the scan's LEADING diskRoutedRows rows, stopping at the cut and leaving the hot
    // tail to the slot's frames; under lead-only it is every frame the base hands out, and
    // the base's own end is what ends it.
    private @Nullable PageFrame nextDiskFrame(long skipTarget) {
        if (isDiskExhausted) {
            return null;
        }
        if (diskRoutedRows < 0) {
            // The base cannot size itself, so there is no row count to measure against and
            // nothing to cut: pass its frames straight through until it runs out. Only
            // lead-only reaches here - of() routes an unsized base that way for this reason.
            final PageFrame frame = base.next(skipTarget);
            if (frame != null) {
                return frame;
            }
            isDiskExhausted = true;
            return null;
        }
        final long remainingDiskRows = diskRoutedRows - diskRowsEmitted;
        if (remainingDiskRows > 0) {
            final PageFrame frame = base.next(skipTarget);
            if (frame != null) {
                final long frameRows = frame.getPartitionHi() - frame.getPartitionLo();
                if (frameRows <= remainingDiskRows) {
                    diskRowsEmitted += frameRows;
                    return frame;
                }
                // The frame straddles the seam: serve its prefix and stop the disk side -
                // the rest of it is the slot's overlap, which the slot frame serves from
                // RAM. This is the hot-tail skip the seam exists for, and it is why the
                // band is a row count rather than the base's own end.
                isDiskExhausted = true;
                diskRowsEmitted = diskRoutedRows;
                return seamCutFrame.of(frame, remainingDiskRows);
            }
            // The base ran out before the band did. Under the fence this means the disk
            // snapshot is smaller than of() measured it, which cannot happen for the
            // frozen reader this cursor holds; fail safe by serving what disk gave.
            assert false : "disk scan ended " + remainingDiskRows + " rows short of the routed band";
        }
        isDiskExhausted = true;
        return null;
    }

    // The next frame over the lead rows, or null once they are spent. Tiles leadBands
    // ascending, which is the only order a time-frame model accepts - it addresses frames by
    // index and expects their timestamps to ascend with it. Deliberately does NOT touch the
    // mode's own walk state (slotBandIdx, slotRowsEmitted), so size() and calculateSize()
    // keep answering for the unscoped stream.
    private @Nullable PageFrame nextLeadFrame() {
        final int bandCount = leadBands.size() / 2;
        while (leadBandIdx < bandCount) {
            final long bandLo = leadBands.getQuick(2 * leadBandIdx);
            final long bandHi = leadBands.getQuick(2 * leadBandIdx + 1);
            if (leadBandRowsEmitted >= bandHi - bandLo) {
                leadBandIdx++;
                leadBandRowsEmitted = 0;
                continue;
            }
            final long lo = bandLo + leadBandRowsEmitted;
            final long hi = Math.min(bandHi, lo + leadRowLimit);
            leadBandRowsEmitted += hi - lo;
            return slotFrame.of(lo, hi);
        }
        return null;
    }

    // The next frame of the routing mode's own stream - the union of both tiers, in the
    // order the mode serves them. This is what an ordinary read walks; the two scoped walks
    // above exist for consumers that want the tiers apart.
    private @Nullable PageFrame nextRoutedFrame(long skipTarget) {
        if (isLeadOnlyDescending) {
            // The reversed lead, then the disk scan in full: the lead sits at or above the
            // on-disk maximum, so serving it first keeps the stream non-increasing.
            final PageFrame slotFrame = nextSlotFrame();
            return slotFrame != null ? slotFrame : nextDiskFrame(skipTarget);
        }
        // The seam: the disk band below the cut, then the whole slot above it.
        final PageFrame diskFrame = nextDiskFrame(skipTarget);
        return diskFrame != null ? diskFrame : nextSlotFrame();
    }

    // The next frame over the slot rows this mode serves, or null once they are spent. Each
    // band in slotBands tiles into slotRowLimit-row frames, walking UP through the bands
    // under the seam and lead-only forward, and DOWN through them under lead-only
    // descending - each in the order its mode serves them.
    private @Nullable PageFrame nextSlotFrame() {
        final int bandCount = slotBands.size() / 2;
        while (slotBandIdx >= 0 && slotBandIdx < bandCount) {
            final long bandLo = slotBands.getQuick(2 * slotBandIdx);
            final long bandHi = slotBands.getQuick(2 * slotBandIdx + 1);
            if (slotBandRowsEmitted >= bandHi - bandLo) {
                // Band spent; step to the next one in the direction this mode walks.
                slotBandIdx += isLeadOnlyDescending ? -1 : 1;
                slotBandRowsEmitted = 0;
                continue;
            }
            final long lo;
            final long hi;
            if (isLeadOnlyDescending) {
                // Rounding down from the band's top leaves its LOWEST frame the short one,
                // which is the frame boundary BwdTableReaderPageFrameCursor gives a disk
                // partition of the same row count.
                hi = bandHi - slotBandRowsEmitted;
                lo = Math.max(bandLo, hi - slotRowLimit);
            } else {
                lo = bandLo + slotBandRowsEmitted;
                hi = Math.min(bandHi, lo + slotRowLimit);
            }
            slotBandRowsEmitted += hi - lo;
            slotRowsEmitted += hi - lo;
            return slotFrame.of(lo, hi);
        }
        return null;
    }

    private void releaseSlot() {
        if (tier != null && slotIdx >= 0) {
            // Safe even after the LV's DROP marked the tier closed: the deferred-close
            // protocol on LiveViewInMemoryTier keeps the native memory the frames
            // published alive until the last pin drains.
            tier.releaseRead(slotIdx);
        }
        tier = null;
        slotIdx = -1;
    }

    /**
     * A base frame narrowed to its leading {@code rows} rows, i.e. the part of it that
     * falls below the seam. Everything but the row range delegates to the wrapped frame.
     * <p>
     * The column extents ({@link #getPageSize} / {@link #getAuxPageSize}) deliberately
     * stay the WHOLE frame's. Frame consumers read them only as bounds guards - every
     * var-size read in {@code PageFrameMemoryRecord} is
     * {@code if (pageLim < offset + n) throw} - and nothing derives a row count from
     * them (a vector aggregate takes its count from {@code getFrameSize}, i.e. the row
     * range narrowed here). So an un-narrowed extent only loosens a guard by the rows
     * this frame drops, and the row range caps {@code rowIndex} below them regardless.
     * <p>
     * A covered (posting-index sidecar) frame cannot reach here - the fence admits only
     * a plain entity scan over a {@code TablePageFrameCursor} - which is why the covered
     * accessors delegate verbatim rather than narrowing {@code getCoveredRowLo/Hi} to
     * match. They must all delegate together regardless: {@code PageFrameAddressCache}
     * asserts a frame's per-column {@code DataSource.COVERED} flags agree with its
     * per-frame covered accessors, and a wrapper that forwards some but not all of them
     * would record the frame as non-covered and silently drop the covered decode.
     */
    private static class SeamCutPageFrame implements PageFrame {
        private PageFrame base;
        // The rows of the wrapped frame that fall below the seam; its row range is
        // [base.getPartitionLo(), base.getPartitionLo() + rows).
        private long rows;

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return base.getAuxPageAddress(columnIndex);
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            return base.getAuxPageSize(columnIndex);
        }

        @Override
        public int getColumnCount() {
            return base.getColumnCount();
        }

        @Override
        public byte getColumnSource(int columnIndex) {
            return base.getColumnSource(columnIndex);
        }

        @Override
        public int getCoveredIncludeIndex(int columnIndex) {
            return base.getCoveredIncludeIndex(columnIndex);
        }

        @Override
        public int[] getCoveredIncludeIndices() {
            return base.getCoveredIncludeIndices();
        }

        @Override
        public int getCoveredKey() {
            return base.getCoveredKey();
        }

        @Override
        public long getCoveredRowHi() {
            return base.getCoveredRowHi();
        }

        @Override
        public long getCoveredRowLo() {
            return base.getCoveredRowLo();
        }

        @Override
        public byte getFormat() {
            return base.getFormat();
        }

        @Override
        public IndexReader getIndexReader(int columnIndex, int direction) {
            return base.getIndexReader(columnIndex, direction);
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return base.getPageAddress(columnIndex);
        }

        @Override
        public long getPageSize(int columnIndex) {
            return base.getPageSize(columnIndex);
        }

        @Override
        public ParquetDecoder getParquetDecoder() {
            return base.getParquetDecoder();
        }

        @Override
        public int getParquetRowGroup() {
            return base.getParquetRowGroup();
        }

        @Override
        public int getParquetRowGroupHi() {
            final int lo = base.getParquetRowGroupLo();
            // A native frame reports the -1 sentinel for both bounds and has no row
            // group to narrow. A parquet frame's rows map 1:1 onto [lo, hi), so the
            // narrowed range ends where the narrowed row range does - the pool decodes
            // exactly the rows this frame now claims.
            return lo < 0 ? base.getParquetRowGroupHi() : lo + (int) rows;
        }

        @Override
        public int getParquetRowGroupLo() {
            return base.getParquetRowGroupLo();
        }

        @Override
        public long getPartitionHi() {
            return base.getPartitionLo() + rows;
        }

        @Override
        public int getPartitionIndex() {
            return base.getPartitionIndex();
        }

        @Override
        public long getPartitionLo() {
            return base.getPartitionLo();
        }

        PageFrame of(PageFrame base, long rows) {
            this.base = base;
            this.rows = rows;
            return this;
        }
    }

    /**
     * A synthetic frame over rows {@code [lo, hi)} of the pinned slot. The band the mode
     * serves - the whole slot under the seam, whose cut already stopped disk where row 0
     * starts, or the lead alone under lead-only - is tiled by {@code slotRowLimit}-row
     * frames, so a parallel filter's workers share the tier the way they share a disk
     * partition.
     * <p>
     * It publishes the slot's own column regions as the frame's page addresses. The
     * buffer's layout is the native one a frame already wants: {@code dataMem} carries
     * the payload at {@code row * stride} for a fixed-width or SYMBOL column and the
     * appended bytes for a var-size one, and {@code auxMem} carries the driver's own
     * per-row offset/header vector (the 0 sentinel for a fixed-width column, which is
     * exactly what {@code PageFrameAddressCache} stores for a column with no aux page).
     * So the addresses pass straight through with no copy and no repacking.
     * <p>
     * <b>What {@code lo > 0} rebases, and what it must not.</b> A frame consumer indexes a
     * column by the frame-RELATIVE row, so a fixed-width column's page starts at this
     * frame's first row and a var-size column's aux vector likewise starts at its first
     * entry. Its data page does NOT move: an aux entry carries the payload's offset from
     * the data vector's BASE, not from the frame, so a rebased data address would resolve
     * every value {@code lo} rows too far in. The data page's SIZE stays absolute for the
     * same reason - it bounds an offset measured from that base, so it is where this
     * frame's LAST row's payload ends rather than how many bytes the frame's own rows
     * occupy. This is exactly what a native frame publishes (see
     * {@code FwdTableReaderPageFrameCursor.computeNativeFrame}), and it is the asymmetry
     * that makes the whole-slot case ({@code lo == 0}) look like it needs no arithmetic.
     * <p>
     * The aux extents are the driver's own, not {@link LiveViewInMemoryBuffer#auxSize} -
     * which reports the whole slot, and for STRING / BINARY reports the layout's trailing
     * terminator too. Deriving both bounds from {@code getAuxVectorOffset} keeps the extent
     * relative to the rebased base, and lands on the {@code rows * 8} a native frame
     * publishes rather than one entry past it.
     */
    private class SlotPageFrame implements PageFrame {
        private long hi;
        private long lo;

        @Override
        public long getAuxPageAddress(int columnIndex) {
            final int tierColumn = tierColumns.getQuick(columnIndex);
            final int columnType = slot.columnType(tierColumn);
            if (!ColumnType.isVarSize(columnType)) {
                // No aux vector at all; 0 is the sentinel a frame publishes for one.
                return 0;
            }
            return slot.auxAddress(tierColumn) + ColumnType.getDriver(columnType).getAuxVectorOffset(lo);
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            final int columnType = slot.columnType(tierColumns.getQuick(columnIndex));
            if (!ColumnType.isVarSize(columnType)) {
                return 0;
            }
            final ColumnTypeDriver driver = ColumnType.getDriver(columnType);
            return driver.getAuxVectorOffset(hi) - driver.getAuxVectorOffset(lo);
        }

        @Override
        public int getColumnCount() {
            return tierColumns.size();
        }

        @Override
        public byte getFormat() {
            return PartitionFormat.NATIVE;
        }

        @Override
        public IndexReader getIndexReader(int columnIndex, int direction) {
            throw new UnsupportedOperationException("the live view in-memory tier carries no indices");
        }

        @Override
        public long getPageAddress(int columnIndex) {
            final int tierColumn = tierColumns.getQuick(columnIndex);
            final int columnType = slot.columnType(tierColumn);
            final long dataAddress = slot.dataAddress(tierColumn);
            // A var-size column's data page stays on row 0; see the class doc.
            return ColumnType.isVarSize(columnType) ? dataAddress : dataAddress + lo * ColumnType.sizeOf(columnType);
        }

        @Override
        public long getPageSize(int columnIndex) {
            final int tierColumn = tierColumns.getQuick(columnIndex);
            final int columnType = slot.columnType(tierColumn);
            if (!ColumnType.isVarSize(columnType)) {
                return (hi - lo) * ColumnType.sizeOf(columnType);
            }
            // Absolute, to match the un-rebased data address above: where this frame's last
            // row's payload ends. hi > lo >= 0 holds for every frame next() builds, so the
            // driver never reads a negative row.
            return ColumnType.getDriver(columnType).getDataVectorSizeAt(slot.auxAddress(tierColumn), hi - 1);
        }

        @Override
        public int getParquetRowGroup() {
            return -1;
        }

        @Override
        public int getParquetRowGroupHi() {
            return -1;
        }

        @Override
        public int getParquetRowGroupLo() {
            return -1;
        }

        @Override
        public long getPartitionHi() {
            return hi;
        }

        @Override
        public int getPartitionIndex() {
            return SLOT_PARTITION_INDEX;
        }

        @Override
        public long getPartitionLo() {
            return lo;
        }

        PageFrame of(long lo, long hi) {
            this.lo = lo;
            this.hi = hi;
            return this;
        }
    }
}
