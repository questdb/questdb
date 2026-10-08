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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewSymbolCache;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.lv.LiveViewPageFrameCursor;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit coverage for {@link LiveViewPageFrameCursor}, the page-frame twin of the
 * live-view seam split. No CREATE LIVE VIEW and no refresh worker: the cursor is
 * driven against a real {@link LiveViewInMemoryTier} slot over a stub disk frame
 * cursor, so the seam cut can be placed exactly - inside a frame, on a frame edge,
 * or with no overlap at all - which a query-level test cannot do.
 * <p>
 * The frames are read back through the real consumer stack
 * ({@link PageFrameAddressCache} + {@link PageFrameMemoryPool} +
 * {@link PageFrameMemoryRecord}) rather than by poking the frame's accessors, so
 * the assertions are on the rows a query would see. A frame whose addresses or
 * extents are wrong reads as wrong values or a boundary exception here, not as a
 * passing test.
 * <p>
 * The tier and the stub's store allocate native memory, so every test runs under
 * {@code assertMemoryLeak}.
 */
public class LiveViewPageFrameCursorTest extends AbstractCairoTest {

    // The live view's full output row as both tiers store it: (ts, rn, s, g SYMBOL).
    private static final int COL_G = 3;
    private static final int COL_RN = 1;
    private static final int COL_S = 2;
    private static final int COL_TS = 0;
    // The only value the LV table's committed symbol table holds, at id 0. A disk row can
    // carry no other; anything past it belongs to the un-flushed lead.
    private static final String COMMITTED_SYMBOL = "committed";
    private static final int COMMITTED_SYMBOL_ID = 0;
    private static final long FRAME_CACHE_BYTES = 1024L * 1024L;
    // The lead's symbol: interned into the tier's cache above the committed band, so only
    // the overlay can resolve it.
    private static final String LEAD_SYMBOL = "lead-only";
    private static final long PAGE_SIZE = 4096L;
    private static final int TIER_COLUMN_COUNT = 4;

    @Test
    public void testBackwardOrderSizesTheLeadBandNotTheWholeSlot() throws Exception {
        // size() and calculateSize() must agree with the rows lead-only actually serves,
        // which is a DIFFERENT split of the same total to the seam's: every disk row plus
        // the lead, where the seam takes the disk band short and the whole slot. The total
        // is the same either way (that is the identity both modes rest on), so a size()
        // left on the seam's split still lands on the right number and only the split
        // itself can catch it - the lesson calculateSize's own arm records.
        assertMemoryLeak(() -> {
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 12)
            ) {
                final int slotIdx = tier.acquireRead();
                // leadStart = 4: the slot holds disk rows [8, 12) as overlap plus a 2-row
                // lead, so lead-only serves 12 + 2 = 14 rows.
                fillSlot(tier, slotIdx, disk, 6, 2);

                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, bwdDiskCursor(disk, identityTierColumns(), 8, 12), true, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());
                    Assert.assertEquals(14, cursor.size());
                    Assert.assertEquals(14, calculateSize(cursor));

                    // The slot band goes out FIRST here, so one frame in leaves the whole
                    // disk scan and nothing else: 12. A mode that counted the slot's
                    // overlap too would report 14.
                    cursor.toTop();
                    Assert.assertNotNull(cursor.next());
                    Assert.assertEquals(12, calculateSize(cursor));
                    Assert.assertEquals(0, calculateSize(cursor));
                }
            }
        });
    }

    @Test
    public void testBackwardOrderTilesTheLeadBandDownFromTheSlotsTop() throws Exception {
        // Lead-only descending, where the whole mode lives. The band is the LEAD alone -
        // starting at leadStart, not at 0 - because the disk scan below it serves every
        // applied row, so a band that reached down into the overlap would serve those rows
        // from both tiers. And it tiles DOWNWARD: the frames go out newest-first, which is
        // the order a descending consumer reads them in.
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(4, 4);
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 12)
            ) {
                final int slotIdx = tier.acquireRead();
                // disk: ts 1..12. slot: 12 rows, the first 4 the overlap (ts 9..12, also
                // disk rows [8, 12)) and the last 8 the lead (ts 13..20). leadStart = 4,
                // so the band is [4, 12) - 8 rows over a 4-row limit, i.e. 2 frames.
                fillSlot(tier, slotIdx, disk, 12, 8);

                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, bwdDiskCursor(disk, identityTierColumns(), 8, 12), true, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());
                    final LongList frameRanges = new LongList();
                    PageFrame frame;
                    while ((frame = cursor.next()) != null) {
                        if (frame.getPartitionIndex() == Rows.MAX_SAFE_PARTITION_INDEX) {
                            frameRanges.add(frame.getPartitionLo());
                            frameRanges.add(frame.getPartitionHi());
                        }
                    }
                    // The lead band, tiled top-down and stopping at leadStart. A band
                    // floored at 0 would add a [0, 4) frame - the overlap, which the disk
                    // scan below already serves.
                    Assert.assertEquals("slot frame ranges", "[8,12,4,8]", frameRanges.toString());

                    // ...and the rows come back strictly descending across both bands: the
                    // reversed lead (ts 20..13) and then every disk row (ts 12..1). The
                    // lead sits at or above the on-disk maximum, so the boundary between
                    // them neither repeats a row nor drops one.
                    cursor.toTop();
                    final LongList timestamps = new LongList();
                    final ObjList<String> strings = new ObjList<>();
                    final ObjList<String> symbols = new ObjList<>();
                    drain(cursor, metadataOf(identityTierColumns()), true, (record) -> {
                        timestamps.add(record.getTimestamp(COL_TS));
                        strings.add(Chars.toString(record.getStrA(COL_S)));
                        symbols.add(Chars.toString(record.getSymA(COL_G)));
                    });
                    assertDescendingTimestamps(timestamps, 20, 1);
                    for (int i = 0; i < 20; i++) {
                        // Row i carries ts (20 - i), i.e. the store's logical row 19 - i.
                        final long logicalRow = 19 - i;
                        Assert.assertEquals("row " + i, stringValue(logicalRow), strings.getQuick(i));
                        // The 8 lead rows lead the output; every row below them is disk's,
                        // and a disk row can only carry the committed symbol.
                        Assert.assertEquals("row " + i, i < 8 ? LEAD_SYMBOL : COMMITTED_SYMBOL, symbols.getQuick(i));
                    }
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testCalculateSizeCountsTheSlotRowsNoFrameHasCovered() throws Exception {
        // A slot that spans several frames can be left half-served, which a single
        // whole-slot frame made unreachable. A boolean "the slot went out" flag answers
        // that state wrongly whichever way it leans - the whole slot for one already
        // half-served, or nothing for one with rows still to come - so count the rows no
        // frame has covered instead.
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(4, 4);
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 4)
            ) {
                final int slotIdx = tier.acquireRead();
                fillSlot(tier, slotIdx, disk, 12, 12);

                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, identityTierColumns(), 4), false, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());
                    Assert.assertEquals(16, cursor.size());
                    Assert.assertEquals(16, calculateSize(cursor));

                    // The disk frame, then the slot's leading 4-row frame: 8 slot rows left.
                    cursor.toTop();
                    Assert.assertNotNull(cursor.next());
                    Assert.assertNotNull(cursor.next());
                    Assert.assertEquals(8, calculateSize(cursor));
                    // A second call must not re-count what the first consumed.
                    Assert.assertEquals(0, calculateSize(cursor));
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testCloseReleasesTheTierPin() throws Exception {
        // The cursor carries the pin for its whole life because its frames publish the
        // slot's native addresses directly. Close must hand the slot back, or the refresh
        // worker can never take it again and emergency-flushes the lead every cycle.
        assertMemoryLeak(() -> {
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 8)
            ) {
                final int slotIdx = tier.acquireRead();
                Assert.assertTrue(slotIdx >= 0);
                fillSlot(tier, slotIdx, disk, 6, 2);

                // Closed twice on the happy path - once here, once by the block. The second
                // must be a no-op; a releaseRead without a matching pin corrupts the count.
                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, identityTierColumns(), 8), false, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());

                    Assert.assertNull("a reader pins the slot, so the writer must trail", tier.tryAcquireWrite(slotIdx));
                    cursor.close();
                    Assert.assertNotNull("close must release the pin", tier.tryAcquireWrite(slotIdx));
                    tier.releaseWriteWithoutPublish(slotIdx);
                }
            }
        });
    }

    @Test
    public void testFrameAboveTheSeamIsNeverPulledFromDisk() throws Exception {
        // Skipping the hot tail is the whole point of the seam: once the disk band is
        // spent the cursor must stop asking the base for frames, not pull them and drop
        // them. The frame boundary here lands exactly on the seam, so the cut costs
        // nothing and the last disk frame must never be requested.
        assertMemoryLeak(() -> {
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 12)
            ) {
                final int slotIdx = tier.acquireRead();
                // Slot holds disk rows [8, 12) as overlap, plus 2 lead rows: leadStart = 4,
                // so the disk band is 12 - 4 = 8 rows and ends on the [0, 8) frame's edge.
                fillSlot(tier, slotIdx, disk, 6, 2);

                final TestDiskPageFrameCursor base = diskCursor(disk, identityTierColumns(), 8, 12);
                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, base, false, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());
                    final LongList served = drainTimestamps(cursor, metadataOf(identityTierColumns()));
                    assertTimestamps(served, 1, 14);
                    Assert.assertEquals("the frame above the seam must never be pulled", 1, base.nextCalls);
                }
            }
        });
    }

    @Test
    public void testIntervalCutsTheLeadBandAndTilesEachSubBand() throws Exception {
        // An interval-filtered scan routes lead-only and cuts the slot's band by the same
        // intervals. Two disjoint intervals over the lead produce two sub-bands, each tiled
        // by the row limit - so this pins the cut, the tiling and the walk across a band
        // boundary at once. Serving the band whole would emit the rows between them.
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(2, 2);
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 4)
            ) {
                final int slotIdx = tier.acquireRead();
                // disk: ts 1..4. slot: 12 rows, no overlap, so leadStart = 0 and the lead is
                // ts 5..16 at slot rows [0, 12).
                fillSlot(tier, slotIdx, disk, 12, 12);

                // Two windows over the lead: ts 5..8 (rows [0,4)) and ts 13..16 (rows
                // [8,12)). The stub's disk frames must match the same filter, so it hands
                // back none of its own rows - the intervals start above the disk ladder.
                final LongList intervals = intervals(5_000, 8_000, 13_000, 16_000);
                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, identityTierColumns()).withIntervals(intervals), false, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());

                    final LongList frameRanges = new LongList();
                    PageFrame frame;
                    while ((frame = cursor.next()) != null) {
                        frameRanges.add(frame.getPartitionLo());
                        frameRanges.add(frame.getPartitionHi());
                    }
                    // Each sub-band tiles independently and the gap between them is never
                    // published: a cut that merged the two would read [0,2,2,4,4,6,...].
                    Assert.assertEquals("slot frame ranges", "[0,2,2,4,8,10,10,12]", frameRanges.toString());

                    // Unsized, because the interval scan under it cannot size itself - and
                    // calculateSize must still land on the 8 rows the bands cover.
                    Assert.assertEquals(-1, cursor.size());
                    cursor.toTop();
                    Assert.assertEquals(8, calculateSize(cursor));

                    // ...and every row still decodes off its own rebased frame.
                    cursor.toTop();
                    final LongList timestamps = drainTimestamps(cursor, metadataOf(identityTierColumns()));
                    Assert.assertEquals("[5000,6000,7000,8000,13000,14000,15000,16000]", timestamps.toString());
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testIntervalOverABackwardOrderReversesTheCutBands() throws Exception {
        // The two mode selectors compose: a descending interval-filtered stream serves the
        // cut lead band newest-first and then the disk scan in full. The bands must go out
        // in REVERSE order and each tile downward, or a descending consumer sees the
        // stream jump back up at a band boundary.
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(2, 2);
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 4)
            ) {
                final int slotIdx = tier.acquireRead();
                fillSlot(tier, slotIdx, disk, 12, 12);

                final LongList intervals = intervals(5_000, 8_000, 13_000, 16_000);
                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, bwdDiskCursor(disk, identityTierColumns()).withIntervals(intervals), true, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());

                    final LongList frameRanges = new LongList();
                    PageFrame frame;
                    while ((frame = cursor.next()) != null) {
                        frameRanges.add(frame.getPartitionLo());
                        frameRanges.add(frame.getPartitionHi());
                    }
                    // The HIGH band first, tiled down from its own top, then the low one.
                    Assert.assertEquals("slot frame ranges", "[10,12,8,10,2,4,0,2]", frameRanges.toString());

                    // The rows come back non-increasing across both band boundaries.
                    cursor.toTop();
                    final LongList timestamps = drainTimestamps(cursor, metadataOf(identityTierColumns()), true);
                    Assert.assertEquals("[16000,15000,14000,13000,8000,7000,6000,5000]", timestamps.toString());
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testIntervalSelectingNoSlotRowStillServesDisk() throws Exception {
        // An interval bounded below the lead leaves the slot's band EMPTY. That is the
        // common dashboard-of-the-past shape, and it must serve the disk scan in full
        // rather than degrade: an empty band list has to read as "slot exhausted" on the
        // first ask, in both directions, not as an index that walks off its own list.
        assertMemoryLeak(() -> {
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 6)
            ) {
                final int slotIdx = tier.acquireRead();
                // slot rows: overlap ts 5, 6 then lead ts 7, 8.
                fillSlot(tier, slotIdx, disk, 4, 2);

                // ts <= 6 keeps every disk row and no lead row.
                final LongList intervals = intervals(Long.MIN_VALUE, 6_000);
                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, identityTierColumns(), 6).withIntervals(intervals), false, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());
                    assertTimestamps(drainTimestamps(cursor, metadataOf(identityTierColumns())), 1, 6);
                    cursor.toTop();
                    Assert.assertEquals(6, calculateSize(cursor));
                }

                // Descending: the empty band must not stop the disk scan from following it.
                // A fresh pin, since the cursor above released the first one on close.
                final int descSlotIdx = tier.acquireRead();
                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, bwdDiskCursor(disk, identityTierColumns(), 6).withIntervals(intervals), true, tier, descSlotIdx, tier.getSlot(descSlotIdx), tier.getSymbolCache(), identityTierColumns());
                    final LongList timestamps = drainTimestamps(cursor, metadataOf(identityTierColumns()), true);
                    Assert.assertEquals(6, timestamps.size());
                    Assert.assertEquals(6_000, timestamps.getQuick(0));
                    Assert.assertEquals(1_000, timestamps.getQuick(5));
                }
            }
        });
    }

    @Test
    public void testIntervalServesTheDiskScanInFullAndNeverSeams() throws Exception {
        // The interval routes lead-only, which means the disk scan is served WHOLE. The
        // seam would cut it at base.size() - leadStart, and there are two independent
        // reasons that is wrong here: the narrowed scan's trailing rows are no longer the
        // slot's overlap, and the scan reports no size to cut against at all. So this pins
        // that every disk frame survives and only the lead sits on top of them.
        assertMemoryLeak(() -> {
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 6)
            ) {
                final int slotIdx = tier.acquireRead();
                // slot: 4 rows - overlap ts 5, 6 (disk rows [4, 6)) then lead ts 7, 8.
                fillSlot(tier, slotIdx, disk, 4, 2);

                // An interval taking everything: the disk scan keeps all 6 rows and the
                // lead's 2 survive the cut, so 8 rows in ts order with no gap or repeat.
                // A seam cut would have dropped disk rows 4 and 5 and served the overlap
                // from the slot instead - the same 8 rows, so only the DISK band's own
                // extent can tell the two apart. Hence the frame ranges below.
                final LongList intervals = intervals(Long.MIN_VALUE, Long.MAX_VALUE);
                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, identityTierColumns(), 3, 6).withIntervals(intervals), false, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());

                    final LongList frameRanges = new LongList();
                    PageFrame frame;
                    while ((frame = cursor.next()) != null) {
                        frameRanges.add(frame.getPartitionLo());
                        frameRanges.add(frame.getPartitionHi());
                    }
                    // Both disk frames whole ([0,3) and [3,6)), then the lead band alone
                    // ([2,4) of the slot). A seam would report [0,3,3,4,0,4].
                    Assert.assertEquals("frame ranges", "[0,3,3,6,2,4]", frameRanges.toString());

                    cursor.toTop();
                    assertTimestamps(drainTimestamps(cursor, metadataOf(identityTierColumns())), 1, 8);
                }
            }
        });
    }

    @Test
    public void testLeadScopedWalkCutsTheLeadByTheInterval() throws Exception {
        // The lead-scoped walk answers to the same interval the unscoped one does, and for
        // the same reason: the time-frame model's disk side is the base's interval-filtered
        // frames, so a lead band served uncut next to them emits rows the query excluded.
        // Two intervals landing INSIDE the lead with a gap between them, so the walk has to
        // step across bands rather than stop at its first - the record path's own band-walk
        // mutant survived exactly this gap.
        assertMemoryLeak(() -> {
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 4)
            ) {
                final int slotIdx = tier.acquireRead();
                // slot: overlap ts 3, 4 (disk rows [2, 4)) then lead ts 5..10.
                fillSlot(tier, slotIdx, disk, 8, 6);

                // Keeps lead ts 5, 6 and 9, 10 - and would keep overlap ts 3, 4 if the walk
                // ever reached below leadStart.
                final LongList intervals = intervals(3_000, 6_000, 9_000, 10_000);
                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, identityTierColumns(), 4).withIntervals(intervals), false, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());
                    Assert.assertTrue(cursor.hasLeadFrames());
                    // [2, 4) is lead ts 5, 6; [6, 8) is lead ts 9, 10. A band floored at 0
                    // would add [0, 2) - the overlap ts 3, 4, which disk already serves.
                    Assert.assertEquals("lead frame ranges", "[2,4,6,8]", leadFrameRanges(cursor));
                    Assert.assertEquals("[5000,6000,9000,10000]", drainLeadTimestamps(cursor).toString());
                }
            }
        });
    }

    @Test
    public void testLeadScopedWalkIsEmptyWithoutALead() throws Exception {
        // A slot that is pure overlap - the steady state right after a flush - has nothing
        // the disk tier lacks, so a time-frame model must add no pseudo-partition and no
        // frames at all. hasLeadFrames() is what tells it so; reporting true here would
        // file an empty partition and leave the model's estimates describing nothing.
        assertMemoryLeak(() -> {
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 6)
            ) {
                final int slotIdx = tier.acquireRead();
                fillSlot(tier, slotIdx, disk, 4, 0); // 4 overlap rows, no lead

                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, identityTierColumns(), 6), false, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());
                    Assert.assertFalse(cursor.hasLeadFrames());
                    Assert.assertEquals("lead frame ranges", "[]", leadFrameRanges(cursor));
                }
            }
        });
    }

    @Test
    public void testLeadScopedWalkServesTheSameBandUnderEitherMode() throws Exception {
        // The claim the whole time-frame bridge rests on: toLeadFrames() hands out the LEAD
        // band whatever mode of() picked. Its caller builds the disk side from the table
        // reader's own partitions rather than from this cursor's stream, so it has already
        // counted the slot's overlap by the time it asks - and the seam's own band is the
        // WHOLE slot, overlap included. A walk that handed out slotBands would serve those
        // rows from both tiers.
        assertMemoryLeak(() -> {
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 12)
            ) {
                // Slot holds disk rows [8, 12) as overlap (ts 9..12) plus lead ts 13, 14.
                // leadStart = 4, so the seam's band is [0, 6) and the lead's is [4, 6):
                // three different numbers, which is what lets this tell them apart.
                final int seamSlotIdx = tier.acquireRead();
                fillSlot(tier, seamSlotIdx, disk, 6, 2);

                // The seam: ascending, unfiltered. Its own stream serves the whole slot.
                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, identityTierColumns(), 8, 12), false, tier, seamSlotIdx, tier.getSlot(seamSlotIdx), tier.getSymbolCache(), identityTierColumns());
                    Assert.assertTrue(cursor.hasLeadFrames());
                    Assert.assertEquals("lead frame ranges", "[4,6]", leadFrameRanges(cursor));
                    Assert.assertEquals("[13000,14000]", drainLeadTimestamps(cursor).toString());

                    // The scoped walk leaves the mode's own stream untouched: the seam still
                    // serves its disk band plus the WHOLE slot, ts 1..8 then 9..14.
                    cursor.toTop();
                    assertTimestamps(drainTimestamps(cursor, metadataOf(identityTierColumns())), 1, 14);
                    cursor.toTop();
                    Assert.assertEquals(14, cursor.size());
                }

                // Lead-only descending over the same slot: a different mode, the same band,
                // and still ascending - a time-frame model only ever walks forward.
                final int descSlotIdx = tier.acquireRead();
                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, bwdDiskCursor(disk, identityTierColumns(), 8, 12), true, tier, descSlotIdx, tier.getSlot(descSlotIdx), tier.getSymbolCache(), identityTierColumns());
                    Assert.assertEquals("lead frame ranges", "[4,6]", leadFrameRanges(cursor));
                    Assert.assertEquals("[13000,14000]", drainLeadTimestamps(cursor).toString());
                }
            }
        });
    }

    @Test
    public void testLeadScopedWalkTilesTheLeadBandByItsOwnRowLimit() throws Exception {
        // The lead band is a partition of its own to the model that walks it, so it is sized
        // by ITS row count - the same rule every disk partition gets. Sharing the mode's
        // slotRowLimit would size it against the seam's whole-slot band and leave a tiny
        // trailing frame, which is the one thing calculatePageFrameRowLimit's rounding
        // exists to prevent.
        assertMemoryLeak(() -> {
            // min 5 / max 7 is what pulls the two limits apart: over the seam's 12-row band
            // the limit rounds to 7 and tiles the 8-row lead as 7 + 1, where the lead's own
            // 8-row band rounds the limit up to 8 and tiles it whole.
            sqlExecutionContext.changePageFrameSizes(5, 7);
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 12)
            ) {
                final int slotIdx = tier.acquireRead();
                // slot: 12 rows, overlap ts 9..12 then lead ts 13..20. leadStart = 4.
                fillSlot(tier, slotIdx, disk, 12, 8);

                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, identityTierColumns(), 8, 12), false, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());
                    Assert.assertEquals("lead frame ranges", "[4,12]", leadFrameRanges(cursor));
                    Assert.assertEquals(8, drainLeadTimestamps(cursor).size());
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testPartitionScopedWalkServesTheDiskTierOnly() throws Exception {
        // toPartition() scopes the walk to one of the LV table's disk partitions, which
        // drops the tier out of the read until the next toTop(). The slot is not a
        // partition of that table, so there is nothing it could contribute to a walk of
        // one - and the caller (ConcurrentTimeFrameState, driving a WINDOW / HORIZON JOIN
        // slave) has already sized its frame model from the reader's per-partition row
        // counts, so a surprise extra frame corrupts it rather than enriching it.
        assertMemoryLeak(() -> {
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 12)
            ) {
                final int slotIdx = tier.acquireRead();
                // Slot holds disk rows [8, 12) as overlap plus 2 lead rows (ts 13, 14).
                fillSlot(tier, slotIdx, disk, 6, 2);

                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, identityTierColumns(), 8, 12), false, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());

                    cursor.toPartition(0);
                    // Every disk row, and not one lead row: 12, not 14. The routed walk
                    // below is the one that reaches the lead.
                    assertTimestamps(drainTimestamps(cursor, metadataOf(identityTierColumns())), 1, 12);

                    // toTop() ends the scoped walk, so the cursor routes again.
                    cursor.toTop();
                    assertTimestamps(drainTimestamps(cursor, metadataOf(identityTierColumns())), 1, 14);
                }
            }
        });
    }

    @Test
    public void testPrunedProjectionReadsTheSlotThroughTierColumns() throws Exception {
        // The tier stores the LV's full output row, so a pruned, reordered projection is
        // a subset of it - reached through the output -> tier mapping, not by indexing the
        // slot at the output position. SELECT g, ts moves the SYMBOL off tier column 3 and
        // the timestamp off tier column 0, so a frame that ignored the mapping would read
        // the wrong column's bytes for both.
        assertMemoryLeak(() -> {
            final IntList tierColumns = new IntList();
            tierColumns.add(COL_G);
            tierColumns.add(COL_TS);
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 8)
            ) {
                final int slotIdx = tier.acquireRead();
                fillSlot(tier, slotIdx, disk, 6, 2);

                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, tierColumns, 4, 8), false, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), tierColumns);
                    // The slot holds disk rows [4, 8) as overlap plus 2 lead rows, so the
                    // disk band is 8 - 4 = 4 rows and the whole 6-row slot follows: ts 1..10.
                    final LongList served = new LongList();
                    final ObjList<String> symbols = new ObjList<>();
                    drain(cursor, metadataOf(tierColumns), (record) -> {
                        // Output column 1 is the timestamp, output column 0 the symbol.
                        served.add(record.getTimestamp(1));
                        symbols.add(Chars.toString(record.getSymA(0)));
                    });
                    assertTimestamps(served, 1, 10);
                    // Every row resolves its symbol; the lead's rows carry the lead-only
                    // value, which only the overlay knows.
                    Assert.assertEquals(COMMITTED_SYMBOL, symbols.getQuick(0));
                    Assert.assertEquals(LEAD_SYMBOL, symbols.getQuick(9));
                }
            }
        });
    }

    @Test
    public void testPureLeadSlotServesEveryDiskRow() throws Exception {
        // leadStart == 0: the slot carries no overlap at all, so disk holds none of the
        // slot's rows and must serve every row it has. Cutting the disk band short here
        // would drop rows served by neither tier - the same hazard
        // LiveViewRecordCursor.hasNext()'s leadStart == 0 branch exists for. Reachable
        // after a restart (a slot restaged as pure lead) and whenever an additive commit
        // whose min ts equals the frontier appends straight into the lead.
        assertMemoryLeak(() -> {
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 8)
            ) {
                final int slotIdx = tier.acquireRead();
                // Every slot row is lead: rows 8..10, none of them on disk.
                fillSlot(tier, slotIdx, disk, 3, 3);

                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, identityTierColumns(), 3, 6, 8), false, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());
                    Assert.assertEquals(11, cursor.size());
                    assertTimestamps(drainTimestamps(cursor, metadataOf(identityTierColumns())), 1, 11);
                }
            }
        });
    }

    @Test
    public void testSeamCutServesDiskBelowSeamThenTheWholeSlot() throws Exception {
        // The core contract: disk below the seam, then the whole slot, every row exactly
        // once and in ascending order. The seam falls INSIDE the [5, 10) frame, so the
        // cursor has to narrow that frame's row range rather than drop or keep it whole -
        // keeping it whole would duplicate the overlap, dropping it would lose rows 5..7.
        assertMemoryLeak(() -> {
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 12)
            ) {
                final int slotIdx = tier.acquireRead();
                // Slot: disk rows [8, 12) as overlap plus 2 un-flushed lead rows (ts 13, 14).
                fillSlot(tier, slotIdx, disk, 6, 2);

                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, identityTierColumns(), 5, 10, 12), false, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());
                    // disk.size() + leadRowCount = 12 + 2. The overlap is already counted in
                    // disk.size(); only the un-flushed lead sits on top.
                    Assert.assertEquals(14, cursor.size());
                    assertTimestamps(drainTimestamps(cursor, metadataOf(identityTierColumns())), 1, 14);
                }
            }
        });
    }

    @Test
    public void testSizeAndCalculateSizeAgreeWithTheRowsServed() throws Exception {
        // calculateSize() splits its answer with getRemainingRowsInInterval() - the
        // consumer adds the latter itself - so the two must sum to the rows still to
        // come, whether the cursor is fresh or mid-scan. Over-counting here would make a
        // count(*) through the view report rows the scan never yields.
        assertMemoryLeak(() -> {
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 12)
            ) {
                final int slotIdx = tier.acquireRead();
                fillSlot(tier, slotIdx, disk, 6, 2);

                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, identityTierColumns(), 5, 10, 12), false, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());
                    Assert.assertEquals(14, cursor.size());
                    Assert.assertEquals(14, calculateSize(cursor));

                    // Mid-scan: one 5-row disk frame consumed, so 3 disk rows below the seam
                    // and the whole 6-row slot are left.
                    cursor.toTop();
                    Assert.assertNotNull(cursor.next());
                    // The base still has 7 rows left in its partition, but only 3 of them
                    // are below the seam - the other 4 are the slot's overlap, which the
                    // slot frame serves from RAM. Reporting the base's 7 claims disk rows
                    // this cursor never yields. The total below hides it (calculateSize nets
                    // off whatever this reports), so assert the split itself.
                    Assert.assertEquals(3, cursor.getRemainingRowsInInterval());
                    Assert.assertEquals(9, calculateSize(cursor));

                    // Exhausted: nothing left, and a second call must not re-count.
                    cursor.toTop();
                    drainTimestamps(cursor, metadataOf(identityTierColumns()));
                    Assert.assertEquals(0, calculateSize(cursor));
                    Assert.assertEquals(0, calculateSize(cursor));
                }
            }
        });
    }

    @Test
    public void testSlotFrameDecodesVarSizeAndSymbolColumns() throws Exception {
        // The slot frame publishes the buffer's column regions as page addresses with no
        // copy and no repacking, which only works because the buffer already writes the
        // drivers' native layout. Decode every column of every slot row through the real
        // record: a mis-sized data page or an aux vector shifted by the leading terminator
        // reads as a wrong value or a boundary exception, not as a silent pass.
        assertMemoryLeak(() -> {
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 4)
            ) {
                final int slotIdx = tier.acquireRead();
                // No overlap: the slot's 5 rows are all lead, so the drain below reads the
                // 4 disk rows and then every slot row.
                fillSlot(tier, slotIdx, disk, 5, 5);

                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, identityTierColumns(), 4), false, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());
                    final LongList timestamps = new LongList();
                    final ObjList<String> strings = new ObjList<>();
                    final ObjList<String> symbols = new ObjList<>();
                    drain(cursor, metadataOf(identityTierColumns()), (record) -> {
                        timestamps.add(record.getTimestamp(COL_TS));
                        strings.add(Chars.toString(record.getStrA(COL_S)));
                        symbols.add(Chars.toString(record.getSymA(COL_G)));
                    });
                    Assert.assertEquals(9, timestamps.size());
                    for (int i = 0; i < 9; i++) {
                        Assert.assertEquals((i + 1) * 1_000L, timestamps.getQuick(i));
                        Assert.assertEquals(stringValue(i), strings.getQuick(i));
                    }
                    // The 4 disk rows carry the committed symbol; every slot row here is
                    // lead, so it carries the value only the overlay can resolve.
                    Assert.assertEquals(COMMITTED_SYMBOL, symbols.getQuick(3));
                    Assert.assertEquals(LEAD_SYMBOL, symbols.getQuick(4));
                    Assert.assertEquals(LEAD_SYMBOL, symbols.getQuick(8));
                }
            }
        });
    }

    @Test
    public void testSlotFrameExtentsRebaseOntoTheFramesOwnRows() throws Exception {
        // Where the rebasing lives, and half of it cannot fail loudly. A consumer reads a
        // column's extent only as a bounds guard, so an extent left describing the WHOLE
        // slot is too LOOSE rather than wrong and every value still decodes - the drain
        // arms pass either way. Assert the addresses and extents directly, against the
        // same arithmetic FwdTableReaderPageFrameCursor.computeNativeFrame publishes.
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(4, 4);
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 4)
            ) {
                final int slotIdx = tier.acquireRead();
                fillSlot(tier, slotIdx, disk, 12, 12);
                final LiveViewInMemoryBuffer slot = tier.getSlot(slotIdx);

                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, identityTierColumns(), 4), false, tier, slotIdx, slot, tier.getSymbolCache(), identityTierColumns());
                    // The disk frame, then the slot's [0, 4). The [4, 8) frame below is the
                    // first whose lo is not 0 - the one a whole-slot frame never had.
                    Assert.assertNotNull(cursor.next());
                    Assert.assertNotNull(cursor.next());
                    final PageFrame frame = cursor.next();
                    Assert.assertNotNull(frame);
                    Assert.assertEquals(4, frame.getPartitionLo());
                    Assert.assertEquals(8, frame.getPartitionHi());

                    // A fixed-width column's page starts at this frame's first row and
                    // covers only its rows. SYMBOL's 4-byte stride is not the timestamp's
                    // 8, so a stride hard-coded to either lands off the rows for the other.
                    Assert.assertEquals(slot.dataAddress(COL_TS) + 4 * 8L, frame.getPageAddress(COL_TS));
                    Assert.assertEquals(4 * 8L, frame.getPageSize(COL_TS));
                    Assert.assertEquals(slot.dataAddress(COL_G) + 4 * 4L, frame.getPageAddress(COL_G));
                    Assert.assertEquals(4 * 4L, frame.getPageSize(COL_G));
                    // ...and it has no aux vector at all.
                    Assert.assertEquals(0, frame.getAuxPageAddress(COL_TS));
                    Assert.assertEquals(0, frame.getAuxPageSize(COL_TS));

                    // A var-size column's aux vector rebases onto this frame's first entry,
                    // and its extent is relative to that base - not the slot's own auxSize,
                    // which counts from entry 0 and carries the trailing terminator on top.
                    final ColumnTypeDriver driver = ColumnType.getDriver(ColumnType.STRING);
                    Assert.assertEquals(slot.auxAddress(COL_S) + driver.getAuxVectorOffset(4), frame.getAuxPageAddress(COL_S));
                    Assert.assertEquals(driver.getAuxVectorOffset(8) - driver.getAuxVectorOffset(4), frame.getAuxPageSize(COL_S));
                    Assert.assertTrue(
                            "a rebased frame must not publish the whole slot's aux extent",
                            frame.getAuxPageSize(COL_S) < slot.auxSize(COL_S)
                    );
                    // Its data page does NOT rebase: an aux entry carries the payload's
                    // offset from the vector's BASE, so the address stays row 0's and the
                    // extent stays absolute - where this frame's LAST row's payload ends,
                    // rather than how many bytes its own rows occupy.
                    Assert.assertEquals(slot.dataAddress(COL_S), frame.getPageAddress(COL_S));
                    Assert.assertEquals(driver.getDataVectorSizeAt(slot.auxAddress(COL_S), 7), frame.getPageSize(COL_S));
                    Assert.assertTrue(
                            "a frame that is not the slot's last must stop at its own last row",
                            frame.getPageSize(COL_S) < slot.dataSize(COL_S)
                    );
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testSlotSplitsIntoFramesBoundedByThePageFrameRowLimit() throws Exception {
        // One frame for the whole slot leaves a wide IN MEMORY window's tail to a single
        // filter worker while the rest idle, and past Map.BATCH_ROW_INDEX_MASK rows it
        // silently truncates the frame-relative row index a batched GROUP BY packs into
        // its entries. The slot tiles into limit-sized frames instead, the way the disk
        // partitions ahead of it already do.
        // min == max pins the limit whatever the shared worker count is:
        // calculatePageFrameRowLimit clamps its per-worker share into that band, and 12
        // rows divide by 4 with no trailing-frame rounding.
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(4, 4);
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 4)
            ) {
                final int slotIdx = tier.acquireRead();
                // No overlap: all 12 slot rows are lead, so the drain reads the 4 disk rows
                // and then the whole slot - ts 1..16, across one disk frame and 3 slot ones.
                fillSlot(tier, slotIdx, disk, 12, 12);

                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, identityTierColumns(), 4), false, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());
                    final LongList frameRanges = new LongList();
                    PageFrame frame;
                    while ((frame = cursor.next()) != null) {
                        if (frame.getPartitionIndex() == Rows.MAX_SAFE_PARTITION_INDEX) {
                            frameRanges.add(frame.getPartitionLo());
                            frameRanges.add(frame.getPartitionHi());
                        }
                    }
                    // Contiguous, limit-sized, and covering the slot exactly once.
                    Assert.assertEquals(
                            "slot frame ranges",
                            "[0,4,4,8,8,12]",
                            frameRanges.toString()
                    );

                    // ...and every row still decodes, which is what the rebasing buys. A
                    // frame's aux vector that did not rebase resolves its first row's string
                    // against the slot's first row instead.
                    cursor.toTop();
                    final LongList timestamps = new LongList();
                    final ObjList<String> strings = new ObjList<>();
                    final ObjList<String> symbols = new ObjList<>();
                    drain(cursor, metadataOf(identityTierColumns()), (record) -> {
                        timestamps.add(record.getTimestamp(COL_TS));
                        strings.add(Chars.toString(record.getStrA(COL_S)));
                        symbols.add(Chars.toString(record.getSymA(COL_G)));
                    });
                    assertTimestamps(timestamps, 1, 16);
                    for (int i = 0; i < 16; i++) {
                        Assert.assertEquals("row " + i, stringValue(i), strings.getQuick(i));
                        // The 4 disk rows carry the committed symbol; every slot row here is
                        // lead, so it carries the value only the overlay can resolve.
                        Assert.assertEquals("row " + i, i < 4 ? COMMITTED_SYMBOL : LEAD_SYMBOL, symbols.getQuick(i));
                    }
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testToTopReplaysTheSameFrameStream() throws Exception {
        // The slot stays pinned and frozen for the cursor's life, so a replay must serve
        // exactly the same rows - the seam does not move under it.
        assertMemoryLeak(() -> {
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(tierSchema(), COL_TS, PAGE_SIZE);
                    LiveViewInMemoryBuffer disk = storeOf(0, 12)
            ) {
                final int slotIdx = tier.acquireRead();
                fillSlot(tier, slotIdx, disk, 6, 2);

                try (LiveViewPageFrameCursor cursor = new LiveViewPageFrameCursor()) {
                    cursor.of(sqlExecutionContext, diskCursor(disk, identityTierColumns(), 5, 10, 12), false, tier, slotIdx, tier.getSlot(slotIdx), tier.getSymbolCache(), identityTierColumns());
                    final RecordMetadata metadata = metadataOf(identityTierColumns());
                    final LongList first = drainTimestamps(cursor, metadata);
                    cursor.toTop();
                    final LongList second = drainTimestamps(cursor, metadata);
                    Assert.assertEquals(first, second);
                    assertTimestamps(second, 1, 14);
                }
            }
        });
    }

    // The descending twin of assertTimestamps: hiTs*1000 down to loTs*1000 inclusive, with
    // no gap and no repeat - i.e. lead-only joined the reversed lead to the disk scan
    // exactly once, and neither band strayed into the other's rows.
    private static void assertDescendingTimestamps(LongList served, int hiTs, int loTs) {
        Assert.assertEquals("row count", hiTs - loTs + 1, served.size());
        for (int i = 0, n = served.size(); i < n; i++) {
            Assert.assertEquals("row " + i, (hiTs - i) * 1_000L, served.getQuick(i));
        }
    }

    // Asserts served holds ts loTs*1000 .. hiTs*1000 inclusive, ascending, with no gap
    // and no repeat - i.e. the seam boundary joined the two tiers exactly once.
    private static void assertTimestamps(LongList served, int loTs, int hiTs) {
        Assert.assertEquals("row count", hiTs - loTs + 1, served.size());
        for (int i = 0, n = served.size(); i < n; i++) {
            Assert.assertEquals("row " + i, (loTs + i) * 1_000L, served.getQuick(i));
        }
    }

    // The backward twin of diskCursor: the same frames, yielded newest-first, as the base
    // hands back for an ORDER_DESC read. frameHis still names them in ascending row order.
    private static TestDiskPageFrameCursor bwdDiskCursor(LiveViewInMemoryBuffer store, IntList tierColumns, int... frameHis) {
        return new TestDiskPageFrameCursor(store, tierColumns, true, frameHis);
    }

    // Flat CLOSED (lo, hi) timestamp pairs, the encoding an interval partition frame cursor
    // hands back.
    private static LongList intervals(long... bounds) {
        final LongList intervals = new LongList();
        for (long bound : bounds) {
            intervals.add(bound);
        }
        return intervals;
    }

    private static long calculateSize(LiveViewPageFrameCursor cursor) {
        // Mirrors PageFrameRecordCursorImpl.calculateSize: the consumer adds the current
        // interval's tail itself, then asks the cursor for the rest.
        final RecordCursor.Counter counter = new RecordCursor.Counter();
        counter.add(cursor.getRemainingRowsInInterval());
        cursor.calculateSize(counter);
        return counter.get();
    }

    private static String columnName(int tierColumn) {
        return switch (tierColumn) {
            case COL_TS -> "ts";
            case COL_RN -> "rn";
            case COL_S -> "s";
            default -> "g";
        };
    }

    // A stub disk scan over store, projecting tierColumns and split at the named row
    // boundaries (each is a frame's exclusive high row, the last one the store's size).
    private static TestDiskPageFrameCursor diskCursor(LiveViewInMemoryBuffer store, IntList tierColumns, int... frameHis) {
        return new TestDiskPageFrameCursor(store, tierColumns, false, frameHis);
    }

    private static void drain(LiveViewPageFrameCursor cursor, RecordMetadata metadata, RowSink sink) {
        drain(cursor, metadata, false, sink);
    }

    // Reads every row of every frame the cursor yields, through the same address cache /
    // memory pool / record stack a real page-frame scan uses.
    //
    // A frame's rows are stored ascending whichever way the stream runs, so isDescending
    // reverses the WITHIN-frame walk and nothing else - the frames' own order is the
    // cursor's to get right. That is the real consumer's contract: an async filter over a
    // descending stream reduces each frame ascending and then reads it back through
    // AsyncFilteredRecordCursor.rowIndex(), which is (frameRowCount - frameRowIndex - 1).
    private static void drain(LiveViewPageFrameCursor cursor, RecordMetadata metadata, boolean isDescending, RowSink sink) {
        try (
                PageFrameAddressCache addressCache = new PageFrameAddressCache();
                PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, FRAME_CACHE_BYTES);
                PageFrameMemoryRecord record = new PageFrameMemoryRecord()
        ) {
            addressCache.of(metadata, cursor.getColumnMapping(), cursor.isExternal());
            pool.of(addressCache);
            record.of(cursor);
            int frameIndex = 0;
            PageFrame frame;
            while ((frame = cursor.next()) != null) {
                addressCache.add(frameIndex, frame);
                final PageFrameMemory frameMemory = pool.navigateTo(frameIndex);
                record.init(frameMemory);
                final long rows = frame.getPartitionHi() - frame.getPartitionLo();
                for (long r = 0; r < rows; r++) {
                    record.setRowIndex(isDescending ? rows - r - 1 : r);
                    sink.accept(record);
                }
                frameIndex++;
            }
        }
    }

    // The timestamps of every row the cursor's LEAD-scoped walk serves, in the order it
    // serves them. Reads them back through the real address cache / memory pool / record
    // stack, so a lead frame whose addresses are not rebased onto its own rows reads as
    // wrong values rather than as a passing test.
    private static LongList drainLeadTimestamps(LiveViewPageFrameCursor cursor) {
        cursor.toLeadFrames();
        return drainTimestamps(cursor, metadataOf(identityTierColumns()));
    }

    private static LongList drainTimestamps(LiveViewPageFrameCursor cursor, RecordMetadata metadata) {
        return drainTimestamps(cursor, metadata, false);
    }

    private static LongList drainTimestamps(LiveViewPageFrameCursor cursor, RecordMetadata metadata, boolean isDescending) {
        final LongList timestamps = new LongList();
        drain(cursor, metadata, isDescending, (record) -> timestamps.add(record.getTimestamp(COL_TS)));
        return timestamps;
    }

    // Fills tier's pinned slot with rowCount rows whose first (rowCount - leadRowCount)
    // rows repeat the disk store's tail - the overlap band - and whose trailing
    // leadRowCount rows are the un-flushed lead, which exists nowhere else. Every lead
    // row carries a symbol the committed table does not hold, eager-interned into the
    // tier's cache the way the refresh worker does, so a read that resolves it can only
    // have gone through the overlay.
    private static void fillSlot(LiveViewInMemoryTier tier, int slotIdx, LiveViewInMemoryBuffer disk, int rowCount, int leadRowCount) {
        final long diskRows = disk.rowCount();
        final LiveViewInMemoryBuffer slot = tier.getSlot(slotIdx);
        final LiveViewSymbolCache cache = tier.getSymbolCache();
        final TestCommittedSymbolTable committed = new TestCommittedSymbolTable();
        // Anchors the lead's id band above the committed count, so "lead-only" lands at
        // id 1 - an id the committed table cannot resolve.
        cache.anchor(COL_G, committed.getSymbolCount());
        final int leadSymbolId = cache.intern(COL_G, LEAD_SYMBOL, committed);
        final TestRow row = new TestRow();
        final int overlap = rowCount - leadRowCount;
        for (int r = 0; r < rowCount; r++) {
            // Slot row r is disk row (diskRows - overlap + r) while in the overlap band,
            // and a brand new row past it - both continue the store's ts ladder.
            final long logicalRow = diskRows - overlap + r;
            row.of((logicalRow + 1) * 1_000L, logicalRow, stringValue(logicalRow), r < overlap ? COMMITTED_SYMBOL_ID : leadSymbolId);
            slot.copyRowFromRecord(row, r);
        }
        slot.setRowCount(rowCount);
        slot.setLeadRowCount(leadRowCount);
        slot.setSeamTs((diskRows - overlap + 1) * 1_000L);
        // Stamp the slot's symbol horizon exactly as the tier does at publish; it is what
        // bounds the overlay's cache scan.
        slot.setNewSymbolMaxId(COL_G, cache.newSymbolMaxIdExclusive(COL_G));
    }

    private static IntList identityTierColumns() {
        final IntList tierColumns = new IntList();
        for (int i = 0; i < TIER_COLUMN_COUNT; i++) {
            tierColumns.add(i);
        }
        return tierColumns;
    }

    // The (lo, hi) row ranges of the frames the cursor's LEAD-scoped walk hands out, as a
    // flat list. Asserting the ranges and not only the rows they decode to is deliberate: a
    // band that reached into the overlap would still decode every value fine, and only the
    // ranges say which rows the walk claimed.
    private static String leadFrameRanges(LiveViewPageFrameCursor cursor) {
        cursor.toLeadFrames();
        final LongList ranges = new LongList();
        PageFrame frame;
        while ((frame = cursor.next()) != null) {
            Assert.assertEquals("a lead frame is not a partition of the table", Rows.MAX_SAFE_PARTITION_INDEX, frame.getPartitionIndex());
            ranges.add(frame.getPartitionLo());
            ranges.add(frame.getPartitionHi());
        }
        return ranges.toString();
    }

    // Query metadata for the projection tierColumns describes.
    private static RecordMetadata metadataOf(IntList tierColumns) {
        final IntList types = tierSchema();
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        for (int i = 0, n = tierColumns.size(); i < n; i++) {
            final int tierColumn = tierColumns.getQuick(i);
            final int type = types.getQuick(tierColumn);
            // SYMBOL needs the symbol-table-parameter constructor; the short one asserts
            // against it.
            metadata.add(ColumnType.isSymbol(type)
                    ? new TableColumnMetadata(columnName(tierColumn), type, IndexType.NONE, 0, true, null)
                    : new TableColumnMetadata(columnName(tierColumn), type));
            if (tierColumn == COL_TS) {
                metadata.setTimestampIndex(i);
            }
        }
        return metadata;
    }

    // A LiveViewInMemoryBuffer standing in for the LV table's on-disk tier. It is used
    // here only because it already lays columns out the way a native page frame wants,
    // which is what lets the stub cursor below publish real addresses; the rows it holds
    // are the test's "disk".
    private static LiveViewInMemoryBuffer storeOf(int firstRow, int rowCount) {
        final LiveViewInMemoryBuffer store = new LiveViewInMemoryBuffer(tierSchema(), COL_TS, PAGE_SIZE);
        try {
            final TestRow row = new TestRow();
            for (int r = 0; r < rowCount; r++) {
                final int logicalRow = firstRow + r;
                // Symbol id 0 - "committed" - is all a disk row can carry: an id the LV
                // table's own symbol table already holds.
                row.of((logicalRow + 1) * 1_000L, logicalRow, stringValue(logicalRow), 0);
                store.copyRowFromRecord(row, r);
            }
            store.setRowCount(rowCount);
        } catch (Throwable t) {
            store.close();
            throw t;
        }
        return store;
    }

    // Per-row STRING value, long enough to push the payload past its aux entry so a
    // shifted aux vector resolves the wrong row's bytes rather than tripping a bound.
    private static String stringValue(long row) {
        return "value-" + row;
    }

    private static IntList tierSchema() {
        final IntList types = new IntList(TIER_COLUMN_COUNT);
        types.add(ColumnType.TIMESTAMP);
        types.add(ColumnType.LONG);
        types.add(ColumnType.STRING);
        types.add(ColumnType.SYMBOL);
        return types;
    }

    @FunctionalInterface
    private interface RowSink {
        void accept(Record record);
    }

    // The LV table's committed symbol table: COMMITTED_SYMBOL at id 0 and nothing else, so
    // a lead-only value can only resolve through the tier's cache. Doubles as the disk
    // cursor's symbol table and as the committed reader the cache anchors its lead band
    // above (a SymbolMapReader IS a StaticSymbolTable), so one stub covers both roles.
    private static class TestCommittedSymbolTable implements SymbolMapReader, QuietCloseable {
        @Override
        public void close() {
        }

        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCapacity() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getSymbolCount() {
            return 1;
        }

        @Override
        public MemoryR getSymbolOffsetsColumn() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MemoryR getSymbolValuesColumn() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCached() {
            return false;
        }

        @Override
        public boolean isDeleted() {
            return false;
        }

        @Override
        public int keyOf(CharSequence value) {
            return Chars.equalsNc(COMMITTED_SYMBOL, value) ? COMMITTED_SYMBOL_ID : SymbolTable.VALUE_NOT_FOUND;
        }

        @Override
        public StaticSymbolTable newSymbolTableView() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateSymbolCount(int count) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            // null for anything else, as the disk table does: that is the miss the overlay
            // falls back to the lead's cache on.
            return key == COMMITTED_SYMBOL_ID ? COMMITTED_SYMBOL : null;
        }
    }

    // Stands in for the LV table's own page-frame scan. Publishes real native frames over
    // a LiveViewInMemoryBuffer store, split at the row boundaries the test names, with
    // per-frame addresses computed exactly the way FwdTableReaderPageFrameCursor computes
    // them - so a frame this hands out is byte-for-byte the shape the real one produces.
    // <p>
    // Everything it exposes is keyed by OUTPUT column and resolved to the store's column
    // through {@code projection}, as the real cursor resolves through its columnIndexes.
    // Keying by storage column instead would quietly model an identity projection and let
    // a pruned-projection test pass without exercising the mapping at all.
    // <p>
    // It models the LV table as a SINGLE partition (every frame reports partition index 0),
    // which is all toPartition() needs to be exercised.
    private static class TestDiskPageFrameCursor implements TablePageFrameCursor {
        private final int columnCount;
        private final ColumnMapping columnMapping = new ColumnMapping();
        private final int[] frameHis;
        private final StoreFrame frame = new StoreFrame();
        // Whether the stub yields its frames newest-first, as a backward table cursor does
        // for an ORDER_DESC read. It reverses the FRAME order only: a frame's own rows stay
        // stored ascending and it is the consumer that walks them back to front (see
        // AsyncFilteredRecordCursor.rowIndex, and drain(..., isDescending) above).
        private final boolean isDescending;
        private final LongList pageAddresses = new LongList();
        private final LongList pageSizes = new LongList();
        // Output column -> store column, the same mapping the cursor under test resolves
        // the slot's columns through.
        private final IntList projection = new IntList();
        private final LiveViewInMemoryBuffer store;
        private final ObjList<TestCommittedSymbolTable> symbolTables = new ObjList<>();
        // The intervals this scan confines its rows to, or null for an unfiltered scan. A
        // real interval cursor hands its frames back ALREADY narrowed, so a test setting
        // these is responsible for handing frameHis that match them - the stub does not
        // filter, exactly as the real one does not re-check.
        private LongList intervals;
        private int nextFrame;
        // Counts the frames the cursor above asked for, so a test can prove it stopped at
        // the seam instead of pulling the hot tail and dropping it.
        private int nextCalls;
        private long remainingRowsInInterval;

        private TestDiskPageFrameCursor(LiveViewInMemoryBuffer store, IntList projection, boolean isDescending, int[] frameHis) {
            this.store = store;
            this.frameHis = frameHis;
            this.isDescending = isDescending;
            this.projection.addAll(projection);
            this.columnCount = projection.size();
            for (int i = 0; i < columnCount; i++) {
                final int storeColumn = projection.getQuick(i);
                // getColumnIndex(i) is the storage column output column i reads - the
                // resolution LiveViewRecordCursor.buildTierColumnMapping reads back.
                columnMapping.addColumn(storeColumn, storeColumn, storeColumn);
                symbolTables.add(ColumnType.isSymbol(store.columnType(storeColumn)) ? new TestCommittedSymbolTable() : null);
            }
            pageAddresses.setPos(2 * columnCount);
            pageSizes.setPos(2 * columnCount);
        }

        @Override
        public void calculateSize(RecordCursor.Counter counter) {
            if (intervals != null) {
                // An unsized interval scan can still count itself, which is the only way a
                // consumer above it ever learns its size. It counts the rows of the frames
                // it has NOT handed out - not the store's, which an interval scan does not
                // serve in full - and one partition means none of them sit behind
                // getRemainingRowsInInterval.
                for (int i = nextFrame; i < frameHis.length; i++) {
                    final int idx = isDescending ? frameHis.length - 1 - i : i;
                    counter.add(frameHis[idx] - (idx == 0 ? 0 : frameHis[idx - 1]));
                }
                nextFrame = frameHis.length;
                return;
            }
            // Unfiltered: a sized cursor counts through size(), and its calculateSize is a
            // no-op - AbstractFullPartitionFrameCursor never overrides the interface's.
        }

        @Override
        public void close() {
            Misc.freeObjListIfCloseable(symbolTables);
        }

        @Override
        public ColumnMapping getColumnMapping() {
            return columnMapping;
        }

        @Override
        public long getRemainingRowsInInterval() {
            return remainingRowsInInterval;
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return symbolTables.getQuick(columnIndex);
        }

        @Override
        public TableReader getTableReader() {
            // No table behind the stub. The cursor under test only delegates this on the
            // partition-scoped path, where nothing reads it; the routing fence, which does,
            // runs in the factory against a real scan.
            return null;
        }

        @Override
        public LongList getIntervals() {
            return intervals;
        }

        @Override
        public boolean hasIntervalFilter() {
            return intervals != null;
        }

        @Override
        public boolean isExternal() {
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            // A fresh instance per call, as a real reader hands out: the overlay that wraps
            // it owns and closes it.
            return symbolTables.getQuick(columnIndex) != null ? new TestCommittedSymbolTable() : null;
        }

        @Override
        public @Nullable PageFrame next(long skipTarget) {
            nextCalls++;
            if (nextFrame >= frameHis.length) {
                return null;
            }
            // frameHis names the frames in ascending row order either way; a descending
            // walk takes them from the far end.
            final int idx = isDescending ? frameHis.length - 1 - nextFrame : nextFrame;
            nextFrame++;
            final long lo = idx == 0 ? 0 : frameHis[idx - 1];
            final long hi = frameHis[idx];
            computeFrame(lo, hi);
            return frame;
        }

        @Override
        public TablePageFrameCursor of(SqlExecutionContext executionContext, PartitionFrameCursor partitionFrameCursor) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            // An interval cursor cannot size itself without walking its partitions, so it
            // hard-reports unknown - the behaviour the cursor above forks its whole disk
            // band accounting on.
            return intervals != null ? -1 : store.rowCount();
        }

        @Override
        public boolean supportsSizeCalculation() {
            return true;
        }

        @Override
        public void toPartition(int partitionIndex) {
            // Single partition, so a scoped walk restarts at the first frame.
            Assert.assertEquals(0, partitionIndex);
            nextFrame = 0;
        }

        @Override
        public void toTop() {
            nextFrame = 0;
            nextCalls = 0;
            remainingRowsInInterval = 0;
        }

        // Declares the intervals this scan confines its rows to. The caller is responsible
        // for handing frameHis that already match them, exactly as a real interval cursor
        // hands back frames it has already narrowed.
        private TestDiskPageFrameCursor withIntervals(LongList intervals) {
            this.intervals = intervals;
            return this;
        }

        private void computeFrame(long lo, long hi) {
            for (int i = 0; i < columnCount; i++) {
                final int storeColumn = projection.getQuick(i);
                final int columnType = store.columnType(storeColumn);
                if (ColumnType.isVarSize(columnType)) {
                    final ColumnTypeDriver driver = ColumnType.getDriver(columnType);
                    final long auxBase = store.auxAddress(storeColumn);
                    final long auxOffsetLo = driver.getAuxVectorOffset(lo);
                    final long auxOffsetHi = driver.getAuxVectorOffset(hi);
                    // The aux entries hold absolute data offsets, so the data address stays
                    // the column's base and the size is measured from the vector's start.
                    final long dataSize = driver.getDataVectorSizeAt(auxBase, hi - 1);
                    pageAddresses.setQuick(2 * i, dataSize > 0 ? store.dataAddress(storeColumn) : 0);
                    pageAddresses.setQuick(2 * i + 1, auxBase + auxOffsetLo);
                    pageSizes.setQuick(2 * i, dataSize);
                    pageSizes.setQuick(2 * i + 1, auxOffsetHi - auxOffsetLo);
                } else {
                    final int shift = ColumnType.pow2SizeOf(columnType);
                    pageAddresses.setQuick(2 * i, store.dataAddress(storeColumn) + (lo << shift));
                    pageAddresses.setQuick(2 * i + 1, 0);
                    pageSizes.setQuick(2 * i, (hi - lo) << shift);
                    pageSizes.setQuick(2 * i + 1, 0);
                }
            }
            frame.partitionLo = lo;
            frame.partitionHi = hi;
            // The rows of this (single) partition the walk has yet to reach, which is what
            // the real cursors report: below the frame going backward, above it going
            // forward.
            remainingRowsInInterval = isDescending ? lo : store.rowCount() - hi;
        }

        private class StoreFrame implements PageFrame {
            private long partitionHi;
            private long partitionLo;

            @Override
            public long getAuxPageAddress(int columnIndex) {
                return pageAddresses.getQuick(2 * columnIndex + 1);
            }

            @Override
            public long getAuxPageSize(int columnIndex) {
                return pageSizes.getQuick(2 * columnIndex + 1);
            }

            @Override
            public int getColumnCount() {
                return columnCount;
            }

            @Override
            public byte getFormat() {
                return PartitionFormat.NATIVE;
            }

            @Override
            public IndexReader getIndexReader(int columnIndex, int direction) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getPageAddress(int columnIndex) {
                return pageAddresses.getQuick(2 * columnIndex);
            }

            @Override
            public long getPageSize(int columnIndex) {
                return pageSizes.getQuick(2 * columnIndex);
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
                return partitionHi;
            }

            @Override
            public int getPartitionIndex() {
                return 0;
            }

            @Override
            public long getPartitionLo() {
                return partitionLo;
            }
        }
    }

    // Single-row Record stub feeding copyRowFromRecord the (ts, rn, s, g) tier row.
    private static final class TestRow implements Record {
        private long rn;
        private CharSequence str;
        private int symbolId;
        private long ts;

        @Override
        public int getInt(int col) {
            return symbolId;
        }

        @Override
        public long getLong(int col) {
            return rn;
        }

        @Override
        public CharSequence getStrA(int col) {
            return str;
        }

        @Override
        public CharSequence getStrB(int col) {
            return str;
        }

        @Override
        public long getTimestamp(int col) {
            return ts;
        }

        void of(long ts, long rn, CharSequence str, int symbolId) {
            this.ts = ts;
            this.rn = rn;
            this.str = str;
            this.symbolId = symbolId;
        }
    }
}
