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
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;

/**
 * Exposes a live view's two tiers as one page-frame stream: the LV table's own
 * disk frames cut at the seam, then synthetic frames over the pinned in-memory
 * slot. It is the page-frame twin of {@link LiveViewRecordCursor}'s
 * seam split, and serves exactly the same rows in the same order - so a read
 * routed through here sees the un-flushed lead without giving up the engine's
 * frame machinery (the parallel filter, the JIT filter, LIMIT pushdown), which
 * the record-cursor path cannot offer.
 * <p>
 * <b>Where the cut falls.</b> Under the seqTxn fence the slot's overlap band -
 * rows {@code [0, leadStart)}, where {@code leadStart = rowCount - leadRowCount} -
 * is exactly the disk scan's trailing {@code leadStart} rows, the ones at or above
 * {@code seamTs}. So this cursor serves the disk scan's leading
 * {@code base.size() - leadStart} rows and then the whole slot, which covers every
 * row exactly once: disk below the seam, slot at or above it. The cut is taken by
 * ROW COUNT rather than by comparing each row's timestamp against {@code seamTs}.
 * The two are the same boundary, but the row count is the identity
 * {@link LiveViewRecordCursor#size} and {@code skipRows} already use, so all three
 * agree by construction rather than by an invariant holding; it also needs no
 * timestamp read, which a parquet frame or a metadata-only skip frame cannot serve
 * anyway. The tie at {@code seamTs} is handled for free - the split is by row, so a
 * lead row sharing a timestamp with a disk row is still a distinct row.
 * {@code leadStart == 0} (a slot that is pure lead) collapses to "disk serves
 * everything", matching {@link LiveViewRecordCursor#hasNext}'s branch for it.
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
 * already established is a plain table scan. See {@link #toPartition} for the one that
 * carries a behavioural decision.
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
    private final SeamCutPageFrame seamCutFrame = new SeamCutPageFrame();
    private final SlotPageFrame slotFrame = new SlotPageFrame();
    // Resolves the read's SYMBOL columns: the base frame cursor's tables overlaid with
    // the pinned slot's eager-interned lead symbols. Bound once in of().
    private final LiveViewSymbolTableSource symbolTableSource = new LiveViewSymbolTableSource();
    // Output column -> tier column, one entry per projected column. The tier stores the
    // LV's full output row, so a pruned or reordered projection reads a subset of the
    // slot's columns through this indirection. Copied in of() so the cursor owns a
    // snapshot the caller cannot mutate underneath it.
    private final IntList tierColumns = new IntList();
    private TablePageFrameCursor base;
    // Disk rows this cursor serves: the scan's leading rows, strictly below the seam.
    // base.size() - leadStart, snapshotted in of().
    private long diskRoutedRows;
    // Disk rows covered by the frames returned so far.
    private long diskRowsEmitted;
    private boolean isDiskExhausted;
    // Set by toPartition(), cleared by toTop(): the walk is scoped to one disk partition,
    // so next() hands straight to the base and the tier stays out of it. See toPartition().
    private boolean isPartitionScoped;
    private LiveViewInMemoryBuffer slot;
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

    public LiveViewPageFrameCursor() {
        this.slotIdx = -1;
    }

    /**
     * Counts the rows this cursor has NOT yet returned frames for, MINUS the rows
     * {@link #getRemainingRowsInInterval()} already reports. That split is the
     * contract {@code PageFrameRecordCursorImpl.calculateSize} implements - it adds
     * {@code getRemainingRowsInInterval()} itself and then calls this - and it is what
     * the base cursor does too, so this mirrors it rather than the interface javadoc's
     * looser wording. The two together add the disk rows left below the seam plus the
     * whole slot.
     */
    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        if (!isDiskExhausted) {
            // Order matters: getRemainingRowsInInterval() reads the state this branch
            // then advances.
            final long remainingInInterval = getRemainingRowsInInterval();
            counter.add(diskRoutedRows - diskRowsEmitted - remainingInInterval);
            isDiskExhausted = true;
            diskRowsEmitted = diskRoutedRows;
        }
        if (slotRowsEmitted < slotRowCount) {
            counter.add(slotRowCount - slotRowsEmitted);
            slotRowsEmitted = slotRowCount;
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
    public long getRemainingRowsInInterval() {
        if (isDiskExhausted) {
            // Nothing is left behind the slot for this to report: the un-emitted slot rows
            // are what calculateSize counts, and the two must not both count them. The
            // contract only binds their SUM - PageFrameRecordCursorImpl.calculateSize adds
            // this and then calls calculateSize - so leaving the whole slot to the latter
            // keeps the two halves disjoint whether the slot is one frame or many.
            return 0;
        }
        // The base counts every row left in its current partition; this cursor only
        // serves the ones below the seam, so clamp to what is left of the disk band.
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
        // Always false while routing: the fence admits no interval-filtered scan, since a
        // filtered disk band next to the unfiltered slot would over-return the excluded
        // rows. Delegate anyway rather than hard-code the fence's conclusion here.
        return base.hasIntervalFilter();
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
        if (isPartitionScoped) {
            // A partition-scoped walk is a disk-tier walk; see toPartition().
            return base.next(skipTarget);
        }
        if (!isDiskExhausted) {
            final long remainingDiskRows = diskRoutedRows - diskRowsEmitted;
            if (remainingDiskRows > 0) {
                final PageFrame frame = base.next(skipTarget);
                if (frame != null) {
                    final long frameRows = frame.getPartitionHi() - frame.getPartitionLo();
                    if (frameRows <= remainingDiskRows) {
                        diskRowsEmitted += frameRows;
                        return frame;
                    }
                    // The frame straddles the seam: serve its prefix and stop the disk
                    // side - the rest of it is the slot's overlap, which the slot frame
                    // serves from RAM. This is the hot-tail skip the seam exists for.
                    isDiskExhausted = true;
                    diskRowsEmitted = diskRoutedRows;
                    return seamCutFrame.of(frame, remainingDiskRows);
                }
                // The base ran out before the seam. Under the fence this means the disk
                // snapshot is smaller than of() measured it, which cannot happen for the
                // frozen reader this cursor holds; fail safe by serving what disk gave.
                assert false : "disk scan ended " + remainingDiskRows + " rows short of the seam";
            }
            isDiskExhausted = true;
        }
        if (slotRowsEmitted < slotRowCount) {
            final long lo = slotRowsEmitted;
            final long hi = Math.min(slotRowCount, lo + slotRowLimit);
            slotRowsEmitted = hi;
            return slotFrame.of(lo, hi);
        }
        return null;
    }

    /**
     * Binds the cursor to a read. Takes ownership of {@code base} and of the tier pin
     * ({@code tier} / {@code slotIdx}) before anything that can throw, so a later
     * failure releases both through {@link #close()} rather than stranding them.
     * <p>
     * The caller must have pinned {@code slotIdx} through
     * {@link LiveViewInMemoryTier#acquireRead()} and must have established the routing
     * preconditions {@link LiveViewRecordCursor#of} checks: a tier-addressable
     * projection (which is what {@code tierColumns} records), an ascending unfiltered
     * base scan whose {@link PageFrameCursor#size()} is known, and the seqTxn fence
     * (slot and disk reader on the same LV-table version). Routing a read that fails
     * any of them through here would over-return the slot's rows against a disk scan
     * they do not line up with.
     *
     * @param executionContext the read's context; sizes the slot's frames through the row
     *                         bounds {@code changePageFrameSizes} may have narrowed
     * @param base             the LV table's own frame cursor; owned from here on
     * @param tier             the pinned tier; its pin is released by {@link #close()}
     * @param slotIdx          the pinned slot's index, as {@link LiveViewInMemoryTier#acquireRead()} returned it
     * @param slot             the pinned slot, i.e. {@code tier.getSlot(slotIdx)}
     * @param symbolCache      the tier's eager-interning symbol cache, for lead-only SYMBOL ids
     * @param tierColumns      output column -> tier column; copied
     */
    public LiveViewPageFrameCursor of(
            SqlExecutionContext executionContext,
            TablePageFrameCursor base,
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
        // The slot is this scan's last partition, so it splits into frames the same way a
        // disk partition of the same row count does - same helper, same bounds, same
        // trailing-frame rounding. Sharing it is not just tidiness: those bounds carry the
        // engine's hard cap on a frame's row count (Map.BATCH_ROW_INDEX_MASK + 1, the width
        // of the frame-relative row index a batched GROUP BY packs into its entries), which
        // a slot published as one frame would breach for a wide enough IN MEMORY window.
        this.slotRowLimit = FwdTableReaderPageFrameCursor.calculatePageFrameRowLimit(
                0,
                slotRowCount,
                executionContext.getPageFrameMinRows(),
                executionContext.getPageFrameMaxRows(),
                executionContext.getSharedQueryWorkerCount()
        );
        final long diskSize = base.size();
        // The fence admits only a plain entity scan, whose size is always known. A -1
        // would make the seam cut below meaningless (the disk band's row count IS the
        // cut), so it is a precondition, not a case to degrade through.
        assert diskSize >= 0 : "in-mem routing needs a sized disk scan, got " + diskSize;
        final long leadStart = slotRowCount - slot.leadRowCount();
        // The overlap band is a suffix of the disk scan, so it cannot exceed it. Both
        // hold under a passing fence; assert to fail safe rather than cut at a negative
        // row count and serve disk rows twice.
        assert leadStart >= 0 : "leadStart " + leadStart + " is negative";
        assert leadStart <= diskSize : "leadStart " + leadStart + " exceeds disk size " + diskSize;
        this.diskRoutedRows = diskSize - leadStart;
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
        // The disk band below the seam plus the whole slot, i.e. base.size() +
        // leadRowCount: the slot's overlap is already counted in base.size(), and only
        // the un-flushed lead sits on top of it. Same identity LiveViewRecordCursor.size()
        // reports, so a routed read sizes the same through either path.
        return diskRoutedRows + slotRowCount;
    }

    @Override
    public boolean supportsSizeCalculation() {
        return base.supportsSizeCalculation();
    }

    /**
     * Scopes the walk to one of the LV table's disk partitions, which drops the tier out of
     * the read: {@link #next} hands straight to the base until the next {@link #toTop}.
     * <p>
     * The in-mem slot is not a partition of that table and has no index in its space, so
     * there is no partition this cursor could answer with the slot frame. The consumer that
     * asks is {@code ConcurrentTimeFrameState} (a WINDOW / HORIZON JOIN slave), which
     * derives its whole frame model from the table reader's per-partition row counts before
     * walking a partition to patch in the addresses - a model the slot frame is invisible
     * to, and one whose frame-count assert a surprise extra frame would trip. Serving that
     * consumer the applied prefix leaves it exactly where
     * {@link LiveViewRecordCursorFactory#getTimeFrameCursor} already puts an LV: correct,
     * and at most one flush cycle behind the lead. Bridging the lead into a time frame is
     * the deferred enhancement noted there, not something to smuggle in through here.
     * <p>
     * The tier pin stays held for the cursor's life regardless. It buys this walk nothing,
     * but the symbol overlay {@link #of} bound resolves against the slot, and a consumer may
     * still call {@link #getSymbolTable}.
     */
    @Override
    public void toPartition(int partitionIndex) {
        isPartitionScoped = true;
        base.toPartition(partitionIndex);
    }

    @Override
    public void toTop() {
        base.toTop();
        isDiskExhausted = false;
        isPartitionScoped = false;
        diskRowsEmitted = 0;
        slotRowsEmitted = 0;
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
     * A synthetic frame over rows {@code [lo, hi)} of the pinned slot. The slot's rows -
     * overlap band and un-flushed lead together, since the seam cut above already stopped
     * disk where {@code lo == 0} starts - are tiled by {@code slotRowLimit}-row frames, so
     * a parallel filter's workers share the tier the way they share a disk partition.
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
