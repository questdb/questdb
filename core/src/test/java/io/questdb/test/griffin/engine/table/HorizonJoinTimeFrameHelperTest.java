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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.map.Unordered4Map;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.engine.table.HorizonJoinTimeFrameHelper;
import io.questdb.griffin.engine.table.SymbolTranslatingRecord;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Rows;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.IntPredicate;

/**
 * Drives the keyed ASOF lookup of {@link HorizonJoinTimeFrameHelper} over a mock slave and counts
 * the slave rows it reads. Every row a lookup reads costs exactly one read of the slave key
 * column, so the count is the scan cost. The holes of the key map are private to the helper; the
 * count tells whether a lookup scans backward from its position, as backward-only mode does at
 * every position, or reads only the rows that the key map has not read yet. The helper's test
 * counters tell the rows that its compactions of the holes fill and the holes that they merge,
 * a test-only accessor tells whether it keeps the key map, and a test-only constructor lowers the
 * number of holes it keeps. The mock also counts frame opens, which tell how many frames the
 * lookup walks, and can count the reads of every row and track the order of the reads. It tells
 * the frames of a Parquet partition from native frames as the tests choose: native by default.
 * <p>
 * The slave holds one row per timestamp unit, so the ASOF position of a horizon timestamp is the
 * row with the same number.
 */
public class HorizonJoinTimeFrameHelperTest extends AbstractCairoTest {
    // Production defaults of the cairo.sql.horizon.join.bwd.scan.* properties.
    private static final long BWD_SCAN_ABSOLUTE_THRESHOLD = 131_072;
    private static final long BWD_SCAN_MIN_GAP = 1_024;
    private static final long BWD_SCAN_SWITCH_FACTOR = 8;
    private static final Log LOG = LogFactory.getLog(HorizonJoinTimeFrameHelperTest.class);
    private static final RecordSink MASTER_KEY_SINK = new KeySink(MasterRecord.KEY_COLUMN);
    private static final IntPredicate NATIVE_FRAMES = frameIndex -> false;
    private static final IntPredicate PARQUET_FRAMES = frameIndex -> true;
    private static final RecordSink SLAVE_KEY_SINK = new KeySink(SlaveRecord.KEY_COLUMN);
    // Marks a toTop() in a lookup sequence of lookupSequence().
    private static final long TO_TOP = -1;

    @Test
    public void testAbsoluteThresholdSwitchSkipsLargerGaps() throws Exception {
        // The absolute threshold makes the lookup keep the key map after one backward scan of
        // more than 131,072 rows: here 199,996 rows for a key the slave holds only in row 5. The
        // positions that follow sit 250,000 rows apart, more than that scan read, and look up a
        // key that every 1,000th row holds. The lookups must leave the rows of these gaps unread,
        // apart from the at most 1,000 rows down to the key.
        assertMemoryLeak(() -> {
            final int rareKey = 5_000;
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts(2_500_000, 2_500_000),
                    row -> row == 5 ? rareKey : (int) (row % 1_000)
            );
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            add(positions, keys, 150_000, 7);
            add(positions, keys, 200_000, rareKey);
            add(positions, keys, 200_500, 7);
            final int sparseLo = positions.size();
            long position = 200_500;
            for (int i = 0; i < 8; i++) {
                position += 250_013;
                add(positions, keys, position, 7);
            }
            try (Map adaptiveMap = newMap(); Map backwardMap = newMap()) {
                final long[] adaptive = lookup(newAdaptiveHelper(), cursor, adaptiveMap, positions, keys);
                final long[] backward = lookup(newBackwardOnlyHelper(), cursor, backwardMap, positions, keys);
                assertSparseStretchKeepsBackwardScanCost(adaptive, backward, sparseLo);
            }
        });
    }

    @Test
    public void testBurstThenQuotePauseKeepsBackwardScanCost() throws Exception {
        // The burst makes the lookup keep the key map. The looked-up key then has no rows for
        // 60,000 rows, so the first sparse position scans 60,601 rows backward: deeper than a
        // sparse gap of 20,037 rows, below the absolute threshold of 131,072 rows. Once the key is
        // back in every 1,000th row, the lookups must not read every later gap.
        assertMemoryLeak(() -> {
            final long gap = 20_037;
            final long pauseHi = 221_600;
            final long pauseLo = pauseHi - 60_000;
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts(2_000_000, 4_096),
                    row -> row >= pauseLo && row < pauseHi ? 1 + (int) (row % 999) : (int) (row % 1_000)
            );
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            long position = 100_000;
            for (int i = 0; i < 32; i++) {
                add(positions, keys, position, 0);
                position += 50;
            }
            final int sparseLo = positions.size();
            for (position = pauseHi; position < 2_000_000; position += gap) {
                add(positions, keys, position, 0);
            }
            assertDeepScanAfterBurstKeepsBackwardScanCost(cursor, positions, keys, sparseLo, sparseLo, sparseLo + 1, gap);
        });
    }

    @Test
    public void testBurstThenRareKeyLookupKeepsBackwardScanCost() throws Exception {
        // The burst makes the lookup keep the key map. At the second sparse position, one lookup
        // of a key that the slave holds only 50,000 rows below scans deeper than a sparse gap of
        // 20,037 rows, yet below the absolute threshold of 131,072 rows. The lookups that follow
        // read the frequent key and must not read every later gap.
        assertMemoryLeak(() -> {
            final long gap = 20_037;
            final int rareKey = 5_000;
            final long rarePosition = 101_600 + 2 * gap;
            final long rareRow = rarePosition - 50_000;
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts(2_000_000, 1_000_000),
                    row -> row == rareRow ? rareKey : (int) (row % 1_000)
            );
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            long position = 100_000;
            for (int i = 0; i < 32; i++) {
                add(positions, keys, position, 7);
                position += 50;
            }
            final int sparseLo = positions.size();
            int rareLo = -1;
            for (position += gap; position < 2_000_000; position += gap) {
                add(positions, keys, position, 7);
                if (position == rarePosition) {
                    rareLo = positions.size() - 1;
                    add(positions, keys, position, rareKey);
                }
            }
            assertDeepScanAfterBurstKeepsBackwardScanCost(cursor, positions, keys, sparseLo, rareLo, rareLo + 2, gap);
        });
    }

    @Test
    public void testBurstThenSparseKeepsBackwardScanCost() throws Exception {
        // No gap of the burst passes the min gap of 1,024 on its own. Their total does at the 22nd
        // position, and the 21 backward scans up to there read more than 8 rows per row of gap, so
        // the window check makes the lookup keep the key map.
        assertBurstThenSparseKeepsBackwardScanCost(1_000_000, BWD_SCAN_MIN_GAP);
    }

    @Test
    public void testBurstThenSparseKeepsBackwardScanCostAcrossAdjacentFrames() throws Exception {
        // Every sparse position sits 5 rows into the frame after the one of the previous position,
        // so the rows of every gap span a frame boundary, which the lookups must not read either.
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(frameRowCounts(1_000_000, 20_000), row -> (int) (row % 1_000));
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            long position = 100_005;
            for (int i = 0; i < 32; i++) {
                add(positions, keys, position, 7);
                position += 50;
            }
            final int sparseLo = positions.size();
            for (position = 120_005; position < 1_000_000; position += 20_000) {
                add(positions, keys, position, 7);
            }
            assertBurstThenSparseKeepsBackwardScanCost(cursor, positions, keys, sparseLo, BWD_SCAN_MIN_GAP);
        });
    }

    @Test
    public void testBurstThenSparseKeepsBackwardScanCostAcrossFrames() throws Exception {
        // The sparse positions sit about five slave frames apart, so the rows of every gap span
        // several frames.
        assertBurstThenSparseKeepsBackwardScanCost(4_096, BWD_SCAN_MIN_GAP);
    }

    @Test
    public void testBurstThenSparseKeepsBackwardScanCostAcrossSmallFrames() throws Exception {
        // The sparse positions sit 39 slave frames apart, and the backward scans cross frames of
        // 512 rows. A window of small gaps ends at a frame boundary, so the frames of 512 rows need
        // a min gap below their size for the switch.
        assertBurstThenSparseKeepsBackwardScanCost(512, 64);
    }

    @Test
    public void testBurstThenSparseKeepsBackwardScanCostOverManyKeys() throws Exception {
        // Every 1,000th row holds the looked-up key, and the other rows hold 100,000 keys in turn.
        // The burst makes the lookup keep the key map, and its lookups put all of these keys into
        // the map. The sparse positions sit 5,003 rows apart: a backward scan reads at most 1,000
        // rows at each of them, and the gap holds 5,003 rows. The keys that the burst left in the
        // key map must not make the lookup read every gap.
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts(1_300_000, 1_000_000),
                    row -> row % 1_000 == 0 ? 7 : 1_000 + (int) (row % 100_000)
            );
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            long position = 2_007;
            for (int i = 0; i < 4_000; i++) {
                add(positions, keys, position, 7);
                position += 50;
            }
            final int sparseLo = positions.size();
            for (int i = 0; i < 200; i++) {
                position += 5_003;
                add(positions, keys, position, 7);
            }
            assertBurstThenSparseKeepsBackwardScanCost(cursor, positions, keys, sparseLo, BWD_SCAN_MIN_GAP);
        });
    }

    @Test
    public void testCompactedHolesReadFewerThanOneAndAHalfPasses() throws Exception {
        // Positions 20 rows apart look up a hot key that sits s rows below them, with a helper
        // that keeps at most 64 holes and the key map from the second position on: every position
        // leaves a hole of 20 - s rows above s read rows. The first and the last position also
        // look up a key that only row 5 holds, so the last lookup crosses every hole. With s = 9,
        // fewer read rows separate the holes than a hole holds, so the holes merge and the last
        // lookup reads the 9 rows of every separation once more: fewer than 1.5 passes over the
        // slave. With s = 10 and s = 11, a hole costs no more to fill than the read rows below
        // it, so the holes fill and no row is read twice: one pass at most. With frames of 400
        // rows, the read rows at every 20th position span a frame boundary, and the holes next to
        // them fill; the holes around them must still merge across the read rows of a frame, in
        // native frames and in Parquet frames, whose holes the compactions decide from the top down.
        assertCompactedHolesReadCost(9, true, 1_000_000, NATIVE_FRAMES);
        assertCompactedHolesReadCost(9, true, 400, NATIVE_FRAMES);
        assertCompactedHolesReadCost(9, true, 400, PARQUET_FRAMES);
        assertCompactedHolesReadCost(10, false, 1_000_000, NATIVE_FRAMES);
        assertCompactedHolesReadCost(11, false, 1_000_000, NATIVE_FRAMES);
    }

    @Test
    public void testCompactionFillsHoleAcrossFrames() throws Exception {
        // A helper that keeps at most 2 holes keeps the key map from the second position on. The
        // slave has frames of 5, 5, 0, 5, 10, 3, 3, 3, 6, 0, 3 and 6 rows, rows 0 to 48. The first
        // position, row 35 in frame 8, looks up a hot key that rows 22, 41 and 45 hold: its lookup
        // reads rows 35 down to 22, across frames 8 to 4, and leaves rows 0 to 21 in the bottom
        // hole, which spans frames 0 to 4 and the empty frame 2. The second position, row 42 in
        // frame 10, looks up the hot key in row 41 and leaves rows 36 to 40 in a hole that spans
        // frames 8 to 10 and the empty frame 9. At the third position, row 43, the two holes
        // compact. The read rows between them span frames, so the holes don't merge. The bottom
        // hole spans frames, so it counts as more rows than any hole holds and never fills. The
        // upper hole counts fewer rows, so it fills: rows 36 to 39 in frame 8 and row 40 in frame
        // 10. In Parquet frames as well, since the hole starts 2 frames below frame 10 of the
        // position before the compaction. Lookups of the keys that single rows of the filled hole
        // hold follow, and of a key that a row of frame 10 and a row of frame 8 of it hold, whose
        // match must be the later row; then lookups of keys in the bottom hole, down to its lowest
        // frame. lookupWithoutReset() checks every match. The filled keys must need no more reads,
        // and every row from row 3 up must be read once: rows 21 down to 3 after the compaction.
        assertCompactionAcrossFrames(42, NATIVE_FRAMES, 5, 0, 19, 44 - 3);
        assertCompactionAcrossFrames(42, PARQUET_FRAMES, 5, 0, 19, 44 - 3);
        // With the second position at row 47 in frame 11, its lookup leaves rows 36 to 44 in a
        // hole that starts in frame 8, 3 frames below frame 11. In native frames, the hole fills
        // as well: 4 rows in frame 8, 3 in frame 10 and 2 in frame 11.
        assertCompactionAcrossFrames(47, NATIVE_FRAMES, 9, 0, 19, 49 - 3);
        // The time frame cursor of a Parquet slave may no longer keep frame 8 decoded, so in
        // Parquet frames the hole only merges, though it costs less to fill than the read rows
        // below it cost to read again. At the third position, row 48, it merges with the bottom
        // hole across the read rows that span frames, since at a limit of 2 holes the costs select
        // every pair. Nothing fills, and the lookups that follow read the 9 rows of the upper hole,
        // the 14 read rows 35 down to 22 that the merge took in once more, and the 19 rows of the
        // bottom hole. A single Parquet frame among the frames of the hole, frame 10, makes it a
        // Parquet hole; Parquet frames below it, which only the bottom hole spans, don't, since
        // that hole never fills.
        assertCompactionAcrossFrames(47, PARQUET_FRAMES, 0, 1, 9 + 14 + 19, 49 - 3 + 14);
        assertCompactionAcrossFrames(47, frameIndex -> frameIndex == 10, 0, 1, 9 + 14 + 19, 49 - 3 + 14);
        assertCompactionAcrossFrames(47, frameIndex -> frameIndex < 8, 9, 0, 19, 49 - 3);
    }

    @Test
    public void testCompactionFillsHolesFarBelowPositionInNativeFrames() throws Exception {
        // Reading a native frame again decodes nothing, so in native frames the compactions fill
        // the holes that cost less to fill than to merge, however far below the position they
        // lie: with frames of 20 and 50 rows, the fills of most compactions reach frames 3 frames
        // or more below the position.
        assertCompactionFills(20, NATIVE_FRAMES);
        assertCompactionFills(50, NATIVE_FRAMES);
    }

    @Test
    public void testCompactionFillsNativeHoleBelowMergeOnlyParquetHole() throws Exception {
        // A helper that keeps at most 3 holes keeps the key map from the second position on. The
        // slave has frames of 10 rows that alternate between native and Parquet partitions: frames
        // 0, 2 and 4 are native, frames 1 and 3 Parquet. The first position, row 1, looks up a key
        // that only row 0 holds, so its lookup reads rows 1 and 0 and leaves no bottom hole. The
        // positions 12, 25, 38 and 45 then look up a hot key that rows 7, 15, 33 and 42 hold, which
        // leaves hole 1 of rows 2 to 6 in native frame 0, hole 2 of rows 13 and 14 in Parquet frame
        // 1, hole 3 of rows 26 to 32 in frames 2 and 3, and hole 4 of rows 39 to 41 in frames 3 and
        // 4; holes 2 to 4 span a Parquet frame. At position 45, holes 1 to 3 compact from the top
        // down: hole 2 holds fewer rows than hole 3, so it fills, and hole 1 stays, since the read
        // rows between it and hole 3 span frames. At position 47, holes 1, 3 and 4 compact. Hole 3
        // was there at the previous compaction, so it may only merge. Hole 4 holds fewer rows than
        // the 6 read rows below it, so it fills. Hole 1, a native hole, then pairs with hole 3,
        // which counts as more rows than any hole holds, so hole 1 fills. Lookups of the keys that
        // single rows of hole 1 hold follow at position 47, and lookupWithoutReset() checks every
        // match: a compaction that drops hole 1 unread loses the only rows of these keys. Every row
        // up to position 47 must be read once.
        assertMemoryLeak(() -> {
            final int firstKey = 2;
            final int hotKey = 1;
            final int holeKeyLo = 100;
            final long holeRowLo = 2;
            final long holeRowHi = 6;
            final long lastPosition = 47;
            final LongList hotRows = new LongList();
            for (long row : new long[]{7, 15, 33, 42}) {
                hotRows.add(row);
            }
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            add(positions, keys, 1, firstKey);
            for (long position : new long[]{12, 25, 38, 45}) {
                add(positions, keys, position, hotKey);
            }
            final LongList holeKeyPositions = new LongList();
            final IntList holeKeys = new IntList();
            for (long row = holeRowHi; row >= holeRowLo; row--) {
                add(holeKeyPositions, holeKeys, lastPosition, holeKeyLo + (int) row);
            }
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts(50, 10),
                    row -> {
                        if (row == 0) {
                            return firstKey;
                        }
                        if (row >= holeRowLo && row <= holeRowHi) {
                            return holeKeyLo + (int) row;
                        }
                        return hotRows.indexOf(row) >= 0 ? hotKey : 0;
                    }
            );
            cursor.parquetFrames = frameIndex -> (frameIndex & 1) == 1;
            cursor.rowReadCounts = new int[50];

            try (Map map = newMap()) {
                final HorizonJoinTimeFrameHelper helper = newKeptKeyMapHelper(3);
                helper.of(cursor);
                map.clear();
                lookupWithoutReset(helper, cursor, map, positions, keys, null);
                // The compaction at position 45 fills the 2 rows of hole 2.
                Assert.assertEquals("filled rows up to position 45", 2, helper.getFilledRowCount());
                lookupWithoutReset(helper, cursor, map, holeKeyPositions, holeKeys, null);
                // The compaction at position 47 fills the 3 rows of hole 4 and the 5 rows of hole 1.
                Assert.assertEquals("filled rows", 2 + 3 + 5, helper.getFilledRowCount());
                Assert.assertEquals("merged holes", 0, helper.getMergedHoleCount());
                for (int row = 0; row < cursor.rowReadCounts.length; row++) {
                    Assert.assertEquals("reads of row " + row, row <= lastPosition ? 1 : 0, cursor.rowReadCounts[row]);
                }
            }
        });
    }

    @Test
    public void testCompactionFillsOnlyHolesAbovePreviousCompaction() throws Exception {
        // The time frame cursor of a Parquet slave decodes a row group again when a read returns to
        // one that it no longer caches, so the fills of an epoch must read its rows once, in order,
        // and each compaction from the top down: the frames it reads first are the ones that the
        // lookups have just read. With frames of 50 rows, the frame of the previous compaction
        // lies 3 frames or more below the position at the compactions, and its rows stay unfilled,
        // as do the rows of every frame that far below.
        assertCompactionFills(Long.MAX_VALUE, PARQUET_FRAMES);
        assertCompactionFills(1_000, PARQUET_FRAMES);
        assertCompactionFills(50, PARQUET_FRAMES);
    }

    @Test
    public void testCompactionFillsOnlyHolesInFramesNearPosition() throws Exception {
        // The time frame cursor of a Parquet slave keeps at most four decoded row groups. With
        // frames of 20 rows and a position in every frame, the holes that a compaction finds span
        // 8 frames, and the holes added since the previous compaction span 7 or 8. A compaction
        // must fill no hole that starts 3 frames or more below the frame of the position, since
        // the cursor would decode those frames again; such holes only merge.
        assertCompactionFills(20, PARQUET_FRAMES);
    }

    @Test
    public void testCompactionFillsParquetHolesNearPositionAndNativeHolesAnywhere() throws Exception {
        // A slave whose frames of 20 rows alternate between Parquet and native partitions, with a
        // position in every frame, so every hole lies in a single frame of either kind. The holes
        // in Parquet frames must keep to the rules of a Parquet slave: no fill at or below the
        // position of the previous compaction, nor 3 frames or more below the position. The holes
        // in native frames fill however far below the position they lie.
        assertCompactionFills(20, frameIndex -> (frameIndex & 1) == 0);
    }

    @Test
    public void testCompactionFillsSmallHoles() throws Exception {
        // A helper that keeps at most 8 holes keeps the key map from the second position on. The
        // first position looks up a key that only row 5 holds, which leaves rows 0 to 4 in the
        // bottom hole; then a hot key sits right below every position, so the lookup there reads
        // down to it and leaves the other rows of the gap in a hole. The gaps leave holes 1 to 8
        // of 30, 4, 6, 30, 30, 30, 30 and 30 rows; the lookups read 40, 4, 4, 40, 4, 40, 4 and 4
        // rows. At the 9th position, the 8 holes compact: the bottom hole is smaller than the hole
        // and the 196 read rows above it, so it fills; hole 2 is smaller than the 40 read rows
        // below it, so it fills; hole 3 has lost its pair with hole 2 and stays; holes 4 and 6
        // merge into holes 3 and 5 across 4 read rows; holes 5 and 7 stay. Lookups of keys in
        // every hole follow, from the top down. The filled rows must be in the key map: the key of
        // hole 2 also sits in a read row below it, and its match must be the later row. Each
        // remaining hole row must be read once, and the 8 rows of the merged separations once more.
        assertMemoryLeak(() -> {
            final int hotKey = 1;
            final int firstKey = 2;
            final int[] holeSizes = {30, 4, 6, 30, 30, 30, 30, 30};
            final int[] depths = {40, 4, 4, 40, 4, 40, 4, 4};
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            final LongList hotRows = new LongList();
            hotRows.add(199);
            long position = 200;
            add(positions, keys, position, firstKey);
            add(positions, keys, position, hotKey);
            final LongList holeLos = new LongList();
            for (int i = 0; i < holeSizes.length; i++) {
                holeLos.add(position);
                position += holeSizes[i] + depths[i];
                hotRows.add(position - depths[i] + 1);
                add(positions, keys, position, hotKey);
            }
            final int lookupLo = positions.size();
            // Keys that a single row holds: rows 0 and 3 of the bottom hole, the lowest row of hole
            // 2 and a row of every other hole. A shared key sits in a row of hole 2 and in a read
            // row below hole 2.
            final LongList keyRows = new LongList();
            keyRows.add(0);
            keyRows.add(3);
            for (int i = 0; i < holeSizes.length; i++) {
                keyRows.add(holeLos.getQuick(i) + (i == 1 ? 1 : 3));
            }
            final long sharedRowBelow = holeLos.getQuick(1) - 20;
            final long sharedRow = holeLos.getQuick(1) + 2;
            final int sharedKey = 500;
            for (int k = keyRows.size() - 1; k >= 0; k--) {
                add(positions, keys, position, 100 + k);
            }
            add(positions, keys, position, sharedKey);
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts(1_000, 1_000),
                    row -> {
                        if (row == 5) {
                            return firstKey;
                        }
                        if (row == sharedRow || row == sharedRowBelow) {
                            return sharedKey;
                        }
                        final int keyIndex = keyRows.indexOf(row);
                        if (keyIndex >= 0) {
                            return 100 + keyIndex;
                        }
                        return hotRows.indexOf(row) >= 0 ? hotKey : 0;
                    }
            );

            try (Map map = newMap()) {
                final HorizonJoinTimeFrameHelper helper = newKeptKeyMapHelper(8);
                helper.of(cursor);
                map.clear();
                final long[] scannedRows = lookupWithoutReset(helper, cursor, map, positions, keys, null);
                // The 5 rows of the bottom hole and the 4 rows of hole 2.
                Assert.assertEquals("filled rows", 5 + 4, helper.getFilledRowCount());
                Assert.assertEquals("merged holes", 2, helper.getMergedHoleCount());
                // The rows of holes 1 and 3 to 8, and the 8 rows of the merged separations once more.
                Assert.assertEquals(6 + 6 * 30 + 8, sum(scannedRows, lookupLo, scannedRows.length));
            }
        });
    }

    @Test
    public void testCompactionKeepsUnreadRowsOfKeptHoles() throws Exception {
        // A helper that keeps at most 8 holes keeps the key map from the second position on. A
        // hot key sits right below every position, so the lookup there reads down to it and
        // leaves the other 30 rows of the gap in a hole; the first lookup leaves rows 0 to 198 in
        // the bottom hole. The lookups of the positions 2, 3 and 4 read 40 rows each, the others 2
        // rows each. At the 9th position, 8 holes compact:
        // holes 0 to 2 and holes 5 to 7 merge across the 4 separations of 2 rows, while holes 3
        // and 4 stay as they are, behind a merged hole. Lookups of keys whose only rows lie in
        // each of the 8 holes follow, from the top down, so they need the unread rows of the
        // bottom hole, of both kept holes and of both merged holes. lookupWithoutReset() checks
        // every match. The lookups must read every unread row from the 9th position down to row
        // 50 once, and the 8 rows of the merged separations once more.
        assertMemoryLeak(() -> {
            final int hotKey = 1;
            final int holeKeyLo = 100;
            final long holeSize = 30;
            // Rows that the lookup at every position reads, down to the hot key.
            final int[] depths = {2, 2, 40, 40, 40, 2, 2, 2, 2};
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            final LongList hotRows = new LongList();
            final LongList holeRows = new LongList();
            // The only row of the key of the bottom hole.
            holeRows.add(50);
            long position = 200;
            for (int i = 0; i < depths.length; i++) {
                if (i > 0) {
                    // A row of the hole between this position and the previous one.
                    holeRows.add(position + 10);
                    position += holeSize + depths[i];
                }
                hotRows.add(position - depths[i] + 1);
                add(positions, keys, position, hotKey);
            }
            // holeRows also holds a row of the hole that the last position adds, which no
            // compaction has seen; its key is looked up first.
            for (int k = holeRows.size() - 1; k >= 0; k--) {
                add(positions, keys, position, holeKeyLo + k);
            }
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts(1_000, 1_000),
                    row -> {
                        final int holeIndex = holeRows.indexOf(row);
                        if (holeIndex >= 0) {
                            return holeKeyLo + holeIndex;
                        }
                        return hotRows.indexOf(row) >= 0 ? hotKey : 0;
                    }
            );

            try (Map map = newMap()) {
                final HorizonJoinTimeFrameHelper helper = newKeptKeyMapHelper(8);
                helper.of(cursor);
                map.clear();
                final long[] scannedRows = lookupWithoutReset(helper, cursor, map, positions, keys, null);
                Assert.assertEquals("merged holes", 4, helper.getMergedHoleCount());
                // The unread rows from row 50 up: 149 of the bottom hole, 30 of each of the
                // other 8 holes, and the 8 rows of the merged separations once more.
                Assert.assertEquals(149 + 8 * holeSize + 8, sum(scannedRows, depths.length, scannedRows.length));
            }
        });
    }

    @Test
    public void testCompactionLeavesRowsBelowEpochUnread() throws Exception {
        // The bottom hole of the epoch spans the frames below the epoch, 2 or 100 of them, and
        // holds only 2 rows in its top frame. A fill of it would read every row of these frames,
        // which neither forward scans nor backward-only mode read, so the compactions must never
        // fill it.
        assertCompactionLeavesRowsBelowEpochUnread(2);
        assertCompactionLeavesRowsBelowEpochUnread(100);
    }

    @Test
    public void testCompactionOverNativeHolesIgnoresParquetFramesOfBottomHole() throws Exception {
        // Frames of 20 rows, Parquet below the first position of the epoch, as old partitions
        // converted to Parquet are, and native from there on. The first position looks up a key
        // that only row 25 holds, in frame 1, so the bottom hole spans the Parquet frames 0 and 1,
        // and every hole that a later position adds lies in a native frame. The bottom hole never
        // fills once it spans frames, so its Parquet frames must not change how the compactions
        // treat the native holes: the lookups and fills read, fill and merge exactly as over a
        // native slave. Over native frames, the compactions decide the pairs from the bottom up,
        // as they did before Parquet frames were set apart, and a fill of an upper hole ends the
        // next pair: the lookups and fills read 40,959 rows, 15,872 of them filled, with no merge.
        // Deciding the pairs from the top down would fill 15,960 rows.
        assertMemoryLeak(() -> {
            final long[] nativeCounts = countCompactionReads(NATIVE_FRAMES);
            Assert.assertArrayEquals(new long[]{40_959, 15_872, 0}, nativeCounts);
            Assert.assertArrayEquals(nativeCounts, countCompactionReads(frameIndex -> frameIndex < 50));
        });
    }

    @Test
    public void testDeepMatchesKeepFrameWalksShort() throws Exception {
        // 1,000 positions 10 rows apart look up 100 keys whose only rows sit 200,000 rows below the
        // positions, in frames of 1,000 rows. The first position scans backward to its match and
        // makes the lookup keep the key map, so the lookups at every later position read only the
        // 10 rows of its gap and find their keys in the key map. They must not walk the 200 frames
        // down to the matches at every position.
        assertMemoryLeak(() -> {
            final long positionLo = 1_000_000;
            final long rareRowLo = positionLo - 200_000;
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts(1_200_000, 1_000),
                    row -> row >= rareRowLo && row < rareRowLo + 100 ? 5_000 + (int) (row - rareRowLo) : (int) (row % 1_000)
            );
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            for (int i = 0; i < 1_000; i++) {
                add(positions, keys, positionLo + 10L * i, 5_000 + i % 100);
            }

            try (Map map = newMap()) {
                final long openCount = cursor.getOpenCount();
                final long adaptiveRows = sum(lookup(newAdaptiveHelper(), cursor, map, positions, keys));
                final long opens = cursor.getOpenCount() - openCount;
                // One backward scan of 200,001 rows, then the gaps.
                Assert.assertEquals(200_001 + 999 * 10, adaptiveRows);
                Assert.assertTrue("frame opens [opens=" + opens + ", lookups=" + positions.size() + ']', opens < 50L * positions.size());
            }
        });
    }

    @Test
    public void testDeepScanBeforeBurstKeepsBackwardScanCost() throws Exception {
        // The slave holds the key in every 1,000th row, except for a stretch of 100,000 rows right
        // before the burst: the first position of the burst scans 100,991 rows backward, the
        // other ones at most 1,000. The backward-only positions clear the key map, so that one
        // deep scan is no longer in the map when the burst makes the lookup keep it, and the
        // lookups of the sparse stretch must not read its gaps.
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts(2_000_000, 2_000_000),
                    row -> row >= 900_000 && row < 1_000_000 ? 1 + (int) (row % 999) : (int) (row % 1_000)
            );
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            long position = 999_990;
            for (int i = 0; i < 32; i++) {
                add(positions, keys, position, 0);
                position += 50;
            }
            final int sparseLo = positions.size();
            for (int i = 0; i < 40; i++) {
                position += 20_037;
                add(positions, keys, position, 0);
            }

            try (Map adaptiveMap = newMap(); Map backwardMap = newMap()) {
                final long[] adaptive = lookup(newAdaptiveHelper(), cursor, adaptiveMap, positions, keys);
                final long[] backward = lookup(newBackwardOnlyHelper(), cursor, backwardMap, positions, keys);
                Assert.assertEquals(100_991, backward[0]);
                // The burst makes the lookup keep the key map.
                Assert.assertTrue(sum(adaptive, 0, sparseLo) < sum(backward, 0, sparseLo));
                assertSparseStretchKeepsBackwardScanCost(adaptive, backward, sparseLo);
            }
        });
    }

    @Test
    public void testDeepSparseLookupsAfterManyKeysKeepKeyMap() throws Exception {
        // The lookups of testSparseLookupsAfterManyKeysKeepKeyMap() with gaps of 7,003 rows, where
        // every 32nd sparse position also looks up the key 20,000 rows back, which reads the
        // rows of about three gaps and puts about 20,000 keys into the key map.
        assertSparseLookupsAfterManyKeys(7_003, 32);
    }

    @Test
    public void testDenseRunAfterDeepScanKeepsSparseStretchCost() throws Exception {
        // A rare key 100,000 rows deep comes after the burst, among sparse positions. A dense run
        // of 500 positions 10 rows apart follows; each of its positions costs up to 1,000 rows in
        // backward-only mode, and the run must read far fewer rows. The sparse stretch after the
        // run must not read its gaps, whatever the deep scan and the run have read before.
        assertMemoryLeak(() -> {
            final long gap = 20_037;
            final int rareKey = 5_000;
            final long rarePosition = 101_600 + gap;
            final long rareRow = rarePosition - 100_000;
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts(2_000_000, 1_000_000),
                    row -> row == rareRow ? rareKey : (int) (row % 1_000)
            );
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            long position = 100_000;
            for (int i = 0; i < 32; i++) {
                add(positions, keys, position, 7);
                position += 50;
            }
            for (int i = 0; i < 10; i++) {
                position += gap;
                add(positions, keys, position, 7);
                if (i == 0) {
                    add(positions, keys, position, rareKey);
                }
            }
            final int denseLo = positions.size();
            for (int i = 0; i < 500; i++) {
                position += 10;
                add(positions, keys, position, (i * 37) % 1_000);
            }
            final int sparseLo = positions.size();
            for (position += gap; position < 2_000_000; position += gap) {
                add(positions, keys, position, 7);
            }

            try (Map adaptiveMap = newMap(); Map backwardMap = newMap()) {
                final long[] adaptive = lookup(newAdaptiveHelper(), cursor, adaptiveMap, positions, keys);
                final long[] backward = lookup(newBackwardOnlyHelper(), cursor, backwardMap, positions, keys);
                final long adaptiveDenseRows = sum(adaptive, denseLo, sparseLo);
                final long backwardDenseRows = sum(backward, denseLo, sparseLo);
                Assert.assertTrue(
                        "dense run [adaptive=" + adaptiveDenseRows + ", backwardOnly=" + backwardDenseRows + ']',
                        adaptiveDenseRows * 4 < backwardDenseRows
                );
                assertSparseStretchKeepsBackwardScanCost(adaptive, backward, sparseLo);
            }
        });
    }

    @Test
    public void testDenseRunAfterSparseStretchReadsGapsOnce() throws Exception {
        // A second burst follows the sparse stretch. The kept key map must serve it at once:
        // backward scans would cost up to 1,000 rows per position of the burst again, while the
        // burst holds 32 gaps of 50 rows, which the lookups read at most once.
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(frameRowCounts(1_000_000, 4_096), row -> (int) (row % 1_000));
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            long position = 100_000;
            for (int i = 0; i < 32; i++) {
                add(positions, keys, position, 7);
                position += 50;
            }
            for (int i = 0; i < 5; i++) {
                position += 20_037;
                add(positions, keys, position, 7);
            }
            final int secondBurstLo = positions.size();
            for (int i = 0; i < 32; i++) {
                position += 50;
                add(positions, keys, position, 7);
            }

            try (Map adaptiveMap = newMap(); Map backwardMap = newMap()) {
                final long[] adaptive = lookup(newAdaptiveHelper(), cursor, adaptiveMap, positions, keys);
                final long[] backward = lookup(newBackwardOnlyHelper(), cursor, backwardMap, positions, keys);
                final long adaptiveRows = sum(adaptive, secondBurstLo, adaptive.length);
                final long backwardRows = sum(backward, secondBurstLo, backward.length);
                Assert.assertTrue(
                        "second burst [adaptive=" + adaptiveRows + ", backwardOnly=" + backwardRows + ']',
                        adaptiveRows <= 32 * 50 && adaptiveRows * 4 < backwardRows
                );
            }
        });
    }

    @Test
    public void testDenseRunScansForward() throws Exception {
        // 20,000 positions 10 rows apart that look up all 1,000 keys in turn: the shape the window
        // check exists for. Backward-only mode reads 500 rows per position on average; with the
        // key map kept, the lookups read every row of the run at most once.
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(frameRowCounts(400_000, 4_096), row -> (int) (row % 1_000));
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            for (int i = 0; i < 20_000; i++) {
                add(positions, keys, 100_000 + 10L * i, (i * 37) % 1_000);
            }

            try (Map adaptiveMap = newMap(); Map backwardMap = newMap()) {
                final long adaptiveRows = sum(lookup(newAdaptiveHelper(), cursor, adaptiveMap, positions, keys));
                final long backwardRows = sum(lookup(newBackwardOnlyHelper(), cursor, backwardMap, positions, keys));
                final long span = 10L * 20_000;
                // The backward scans up to the switch come on top of the span.
                Assert.assertTrue(
                        "dense run [adaptive=" + adaptiveRows + ", span=" + span + ']',
                        adaptiveRows < 2 * span
                );
                Assert.assertTrue(
                        "dense run [adaptive=" + adaptiveRows + ", backwardOnly=" + backwardRows + ']',
                        adaptiveRows * 8 < backwardRows
                );
            }
        });
    }

    @Test
    public void testFuzzMatchesAcrossScanModeSwitches() throws Exception {
        // Random slaves with empty frames and NULL keys, random switch thresholds, and random
        // lookup sequences: dense runs with sparse steps, several lookups per position, positions
        // that go back, toTop() as at a new master page frame, keys that no row holds, missing
        // symbols, and NULL keys. lookupSequence() checks every match against a brute-force scan
        // and moves the time frame cursor between the lookups, as the matched records do. At
        // every position, the lookups must read no more rows than backward-only mode does.
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            try (
                    Map map = newMap();
                    Map backwardMap = newMap();
                    MissingSymbolRecord symbolRecord = new MissingSymbolRecord(Integer.MAX_VALUE)
            ) {
                HorizonJoinTimeFrameHelper helper = null;
                for (int iteration = 0; iteration < 1_000; iteration++) {
                    final int frameCount = 1 + rnd.nextInt(12);
                    final long[] frameRowCounts = new long[frameCount];
                    long rowCount = 0;
                    for (int f = 0; f < frameCount; f++) {
                        // Every fourth frame is empty on average.
                        frameRowCounts[f] = rnd.nextInt(4) == 0 ? 0 : 1 + rnd.nextInt(400);
                        rowCount += frameRowCounts[f];
                    }
                    if (rowCount == 0) {
                        frameRowCounts[frameCount - 1] = 1 + rnd.nextInt(400);
                        rowCount = frameRowCounts[frameCount - 1];
                    }
                    final int keyCount = 1 + rnd.nextInt(40);
                    final int[] slaveKeys = new int[(int) rowCount];
                    for (int r = 0; r < rowCount; r++) {
                        // Skewed keys: a low key is frequent, a high key is rare.
                        slaveKeys[r] = rnd.nextInt(20) == 0 ? Numbers.INT_NULL : rnd.nextInt(1 + rnd.nextInt(keyCount));
                    }
                    final SlaveCursor cursor = new SlaveCursor(frameRowCounts, row -> slaveKeys[(int) row]);

                    final LongList positions = new LongList();
                    final IntList keys = new IntList();
                    long position = rnd.nextLong(rowCount);
                    while (position < rowCount) {
                        // One to three lookups per ASOF position. keyCount misses the slave, and
                        // the symbol record tells that keyCount + 1 is a missing symbol.
                        for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                            add(positions, keys, position, rnd.nextInt(20) == 0 ? Numbers.INT_NULL : rnd.nextInt(keyCount + 2));
                        }
                        final int step = rnd.nextInt(100);
                        if (step == 0) {
                            add(positions, keys, TO_TOP, 0);
                        }
                        if (step < 2) {
                            // A position below the previous one.
                            position = rnd.nextLong(position + 1);
                        } else {
                            // Dense runs with a sparse step now and then.
                            position += step < 12 ? 1 + rnd.nextInt(300) : 1 + rnd.nextInt(4);
                        }
                    }

                    if (helper == null || rnd.nextBoolean()) {
                        helper = new HorizonJoinTimeFrameHelper(
                                1 + rnd.nextInt(8),
                                1,
                                rnd.nextInt(3) == 0 ? Long.MAX_VALUE : rnd.nextInt(64) - 1,
                                rnd.nextInt(16),
                                rnd.nextInt(4)
                        );
                    }
                    symbolRecord.missingKeyLo = keyCount + 1;
                    // Two passes: the second one runs after of() and toTop() on a helper that has
                    // kept its key map; the helper may come from the previous slave.
                    final long[] adaptive = lookupSequence(helper, cursor, map, positions, keys, symbolRecord, rnd);
                    Assert.assertArrayEquals(adaptive, lookupSequence(helper, cursor, map, positions, keys, symbolRecord, rnd));
                    final long[] backward = lookupSequence(newBackwardOnlyHelper(), cursor, backwardMap, positions, keys, symbolRecord, rnd);
                    assertNoMoreRowsThanBackwardOnlyAtEveryPosition(positions, adaptive, backward);
                }
            }
        });
    }

    @Test
    public void testFuzzMatchesAtSmallHoleLimits() throws Exception {
        // Hole limits of 2 to 16 make the helper compact its holes every few positions. Most
        // lookups look up a few hot keys that most rows hold, so they read few rows and leave
        // the other rows of every gap in holes, which pile up and compact. A few lookups look up
        // keys that a single row holds, keys that no row holds, cold keys, NULL keys and missing
        // symbols, and read the compacted holes. Most helpers keep the key map from the second
        // position on; the others switch as their random thresholds tell. Positions repeat, go
        // back, and toTop() comes now and then. lookupSequence() checks every match against a
        // brute-force scan, a second pass must read the same rows, and at every position the
        // lookups must read no more rows than backward-only mode does; the rows that compactions
        // fill come on top. A third of the slaves have 20 to 300 frames of up to 8 rows, three in
        // ten of them empty, so that most holes, the read rows between them and the holes that
        // compactions fill span frames and empty frames. A third of the slaves are native, a third
        // Parquet, and in a third every frame is either at random. The holes must merge and fill,
        // also over tiny frames and over slaves that mix Parquet and native frames.
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final int coldKeyLo = 100;
            final int absentKey = 500;
            final int singleRowKeyLo = 1_000;
            final int missingSymbolKey = 2_000;
            try (
                    Map map = newMap();
                    Map backwardMap = newMap();
                    MissingSymbolRecord symbolRecord = new MissingSymbolRecord(missingSymbolKey)
            ) {
                long filledRowCount = 0;
                long tinyFrameFilledRowCount = 0;
                long mixedFilledRowCount = 0;
                long mergedHoleCount = 0;
                HorizonJoinTimeFrameHelper helper = null;
                for (int iteration = 0; iteration < 1_000; iteration++) {
                    final boolean isTinyFrames = rnd.nextInt(3) == 0;
                    final int frameCount = isTinyFrames ? 20 + rnd.nextInt(281) : 1 + rnd.nextInt(12);
                    final int maxFrameRowCount = isTinyFrames ? 8 : 600;
                    final long[] frameRowCounts = new long[frameCount];
                    long rowCount = 0;
                    for (int f = 0; f < frameCount; f++) {
                        // Every fourth frame is empty on average, or three in ten tiny frames.
                        final boolean isEmpty = isTinyFrames ? rnd.nextInt(10) < 3 : rnd.nextInt(4) == 0;
                        frameRowCounts[f] = isEmpty ? 0 : 1 + rnd.nextInt(maxFrameRowCount);
                        rowCount += frameRowCounts[f];
                    }
                    if (rowCount == 0) {
                        frameRowCounts[frameCount - 1] = 1 + rnd.nextInt(maxFrameRowCount);
                        rowCount = frameRowCounts[frameCount - 1];
                    }
                    final int hotKeyCount = 1 + rnd.nextInt(4);
                    final int coldKeyCount = 1 + rnd.nextInt(40);
                    final int hotPercent = 40 + rnd.nextInt(55);
                    final int[] slaveKeys = new int[(int) rowCount];
                    for (int r = 0; r < rowCount; r++) {
                        final int choice = rnd.nextInt(100);
                        if (choice < hotPercent) {
                            slaveKeys[r] = rnd.nextInt(hotKeyCount);
                        } else if (choice < hotPercent + 3) {
                            slaveKeys[r] = Numbers.INT_NULL;
                        } else {
                            slaveKeys[r] = coldKeyLo + rnd.nextInt(coldKeyCount);
                        }
                    }
                    for (int k = 0, n = rnd.nextInt(4); k < n; k++) {
                        slaveKeys[rnd.nextInt((int) rowCount)] = singleRowKeyLo + k;
                    }
                    final SlaveCursor cursor = new SlaveCursor(frameRowCounts, row -> slaveKeys[(int) row]);
                    final int formatChoice = rnd.nextInt(3);
                    final boolean isMixed = formatChoice == 2;
                    final boolean[] parquetFrameFlags = new boolean[frameCount];
                    for (int f = 0; f < frameCount; f++) {
                        parquetFrameFlags[f] = formatChoice == 1 || (isMixed && rnd.nextBoolean());
                    }
                    cursor.parquetFrames = frameIndex -> parquetFrameFlags[frameIndex];

                    final int rareLookupPermille = rnd.nextInt(80);
                    final LongList positions = new LongList();
                    final IntList keys = new IntList();
                    long position = rnd.nextLong(1 + rowCount / 4);
                    while (position < rowCount) {
                        for (int i = 0, n = 1 + rnd.nextInt(2); i < n; i++) {
                            final int key;
                            if (rnd.nextInt(1_000) < rareLookupPermille) {
                                key = switch (rnd.nextInt(6)) {
                                    case 0 -> absentKey;
                                    case 1 -> Numbers.INT_NULL;
                                    case 2 -> missingSymbolKey;
                                    case 3 -> coldKeyLo + rnd.nextInt(coldKeyCount);
                                    default -> singleRowKeyLo + rnd.nextInt(4);
                                };
                            } else {
                                key = rnd.nextInt(hotKeyCount);
                            }
                            add(positions, keys, position, key);
                        }
                        final int step = rnd.nextInt(200);
                        if (step == 0) {
                            add(positions, keys, TO_TOP, 0);
                        }
                        if (step < 2) {
                            // A position below the previous one.
                            position = rnd.nextLong(position + 1);
                        } else if (step >= 8) {
                            // Dense runs with a sparse step now and then; the other steps repeat
                            // the position.
                            position += step < 30 ? 1 + rnd.nextInt(200) : 1 + rnd.nextInt(8);
                        }
                    }

                    if (helper == null || rnd.nextBoolean()) {
                        final int maxHoleCount = 2 + rnd.nextInt(15);
                        helper = rnd.nextInt(4) > 0
                                ? newKeptKeyMapHelper(maxHoleCount)
                                : new HorizonJoinTimeFrameHelper(
                                1 + rnd.nextInt(8),
                                1,
                                rnd.nextInt(3) == 0 ? Long.MAX_VALUE : rnd.nextInt(64) - 1,
                                rnd.nextInt(16),
                                rnd.nextInt(4),
                                maxHoleCount
                        );
                    }
                    final long filledRowCountBefore = helper.getFilledRowCount();
                    final long mergedHoleCountBefore = helper.getMergedHoleCount();
                    final long[] adaptive = lookupSequence(helper, cursor, map, positions, keys, symbolRecord, rnd);
                    filledRowCount += helper.getFilledRowCount() - filledRowCountBefore;
                    if (isTinyFrames) {
                        tinyFrameFilledRowCount += helper.getFilledRowCount() - filledRowCountBefore;
                    }
                    if (isMixed) {
                        mixedFilledRowCount += helper.getFilledRowCount() - filledRowCountBefore;
                    }
                    mergedHoleCount += helper.getMergedHoleCount() - mergedHoleCountBefore;
                    Assert.assertArrayEquals(adaptive, lookupSequence(helper, cursor, map, positions, keys, symbolRecord, rnd));
                    final long[] backward = lookupSequence(newBackwardOnlyHelper(), cursor, backwardMap, positions, keys, symbolRecord, rnd);
                    assertNoMoreRowsThanBackwardOnlyAtEveryPosition(positions, adaptive, backward);
                }
                // The holes must merge and fill, or the test proves nothing about compaction.
                Assert.assertTrue("merged holes " + mergedHoleCount, mergedHoleCount > 0);
                Assert.assertTrue("filled rows " + filledRowCount, filledRowCount > 0);
                Assert.assertTrue("filled rows over tiny frames " + tinyFrameFilledRowCount, tinyFrameFilledRowCount > 0);
                Assert.assertTrue("filled rows over mixed frames " + mixedFilledRowCount, mixedFilledRowCount > 0);
            }
        });
    }

    @Test
    public void testFuzzSwitchesWhereBackwardScansDo() throws Exception {
        // Random slaves with empty frames and NULL keys, random switch thresholds, and random lookup
        // sequences with up to 8 lookups per position: dense runs with sparse steps, positions that
        // repeat and go back, toTop() as at a new master page frame, keys that no row holds and
        // missing symbols. SwitchModel counts the rows that backward-only mode reads as its backward
        // scans counted them, and the helper must keep the key map from exactly the lookup where the
        // switch rules pass on that count. Some of the switches must depend on the rows that the
        // backward scans read again, or the test proves nothing.
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            try (
                    Map map = newMap();
                    MissingSymbolRecord symbolRecord = new MissingSymbolRecord(Integer.MAX_VALUE)
            ) {
                int switchCount = 0;
                int rereadSwitchCount = 0;
                for (int iteration = 0; iteration < 1_000; iteration++) {
                    final int frameCount = 1 + rnd.nextInt(12);
                    final long[] frameRowCounts = new long[frameCount];
                    long rowCount = 0;
                    for (int f = 0; f < frameCount; f++) {
                        // Every fourth frame is empty on average.
                        frameRowCounts[f] = rnd.nextInt(4) == 0 ? 0 : 1 + rnd.nextInt(400);
                        rowCount += frameRowCounts[f];
                    }
                    if (rowCount == 0) {
                        frameRowCounts[frameCount - 1] = 1 + rnd.nextInt(400);
                        rowCount = frameRowCounts[frameCount - 1];
                    }
                    final int keyCount = 1 + rnd.nextInt(60);
                    final int[] slaveKeys = new int[(int) rowCount];
                    for (int r = 0; r < rowCount; r++) {
                        slaveKeys[r] = rnd.nextInt(20) == 0 ? Numbers.INT_NULL : rnd.nextInt(1 + rnd.nextInt(keyCount));
                    }
                    final SlaveCursor cursor = new SlaveCursor(frameRowCounts, row -> slaveKeys[(int) row]);

                    final LongList positions = new LongList();
                    final IntList keys = new IntList();
                    long position = rnd.nextLong(rowCount);
                    while (position < rowCount) {
                        // keyCount misses the slave, and keyCount + 1 is a missing symbol.
                        for (int i = 0, n = 1 + rnd.nextInt(8); i < n; i++) {
                            add(positions, keys, position, rnd.nextInt(20) == 0 ? Numbers.INT_NULL : rnd.nextInt(keyCount + 2));
                        }
                        final int step = rnd.nextInt(100);
                        if (step == 0) {
                            add(positions, keys, TO_TOP, 0);
                        }
                        if (step < 2) {
                            position = rnd.nextLong(position + 1);
                        } else {
                            position += step < 12 ? 1 + rnd.nextInt(300) : 1 + rnd.nextInt(4);
                        }
                    }

                    symbolRecord.missingKeyLo = keyCount + 1;
                    // Thresholds of a few rows make the rows read again decide many switches, and so
                    // does a threshold that the rows that backward-only mode reads at the first
                    // position pass only with the rows read again.
                    final long absoluteThreshold = switch (rnd.nextInt(5)) {
                        case 0 -> Long.MAX_VALUE;
                        case 1 -> rnd.nextInt(64);
                        case 2 -> rnd.nextInt(512);
                        case 3 -> rnd.nextInt(4_000);
                        default -> {
                            final long rows = SwitchModel.countFirstPositionRows(cursor, positions, keys, symbolRecord.missingKeyLo, false);
                            final long rowsWithRereads = SwitchModel.countFirstPositionRows(cursor, positions, keys, symbolRecord.missingKeyLo, true);
                            yield rows + rnd.nextLong(Math.max(1, rowsWithRereads - rows));
                        }
                    };
                    final long minGap = rnd.nextInt(64);
                    final long switchFactor = rnd.nextInt(16);
                    final boolean[] isKeyMapKept = new SwitchModel(absoluteThreshold, minGap, switchFactor, true)
                            .replay(cursor, positions, keys, symbolRecord.missingKeyLo);
                    final boolean[] isKeyMapKeptWithoutRereads = new SwitchModel(absoluteThreshold, minGap, switchFactor, false)
                            .replay(cursor, positions, keys, symbolRecord.missingKeyLo);
                    final HorizonJoinTimeFrameHelper helper = new HorizonJoinTimeFrameHelper(
                            1 + rnd.nextInt(8),
                            1,
                            absoluteThreshold,
                            minGap,
                            switchFactor
                    );
                    assertSwitchesWhereBackwardScansDo(helper, cursor, map, positions, keys, symbolRecord, isKeyMapKept);
                    for (int i = 0, n = isKeyMapKept.length; i < n; i++) {
                        if (isKeyMapKept[i]) {
                            switchCount++;
                            if (!isKeyMapKeptWithoutRereads[i]) {
                                rereadSwitchCount++;
                            }
                            break;
                        }
                    }
                }
                Assert.assertTrue("switches " + switchCount, switchCount > 100);
                Assert.assertTrue("switches that the rows read again decide " + rereadSwitchCount, rereadSwitchCount > 20);
            }
        });
    }

    @Test
    public void testHoleLimitBelowTwoFails() {
        // A compaction keeps at least one hole and needs another one to remove.
        boolean isRejected = false;
        try {
            newKeptKeyMapHelper(1);
        } catch (AssertionError e) {
            isRejected = true;
        }
        Assert.assertTrue("hole limit of 1 accepted", isRejected);
    }

    @Test
    public void testHolesAcrossFramesCountRowsOfTopAndBottomFrame() throws Exception {
        // One position at the same row of every frame of 20 rows looks up a hot key that sits a
        // few rows below it in that frame, so every hole spans a frame boundary: it holds the rows
        // of the frame below above the previous position, and the rows of its own frame below the
        // hot key, above the read rows of that frame. The row ids of such a hole differ by more
        // than 2^44, and the compaction counts its rows in these two frames. With the positions at
        // row 19 and the hot key 18 rows below, a hole of 1 row costs less to fill than the 19
        // read rows above it cost to read again, so in native frames the holes fill, and the deep
        // lookup at the last position reads every row at most once: one pass at most. With the hot
        // key 5 rows below, the 6 read rows cost less to read again than a hole of 14 rows costs
        // to fill, so the holes merge, and the deep lookup reads the read rows once more: fewer
        // than 1.5 passes; only the bottom hole may fill, in native frames, within the first
        // frame. With the positions at row 6 and the hot key 4 rows below, a hole holds 13 rows of
        // the frame below and 2 rows of its own frame, more than the 5 read rows, so the holes
        // merge as well. A compaction finds 64 holes in as many frames, and in Parquet frames only
        // those that start fewer than 3 frames below the frame of the position may fill: the time
        // frame cursor of a Parquet slave may no longer keep the frames further down decoded. The
        // holes further down only merge, across the read rows of their frame, and the deep lookup
        // reads those once more: with the hot key 18 rows below, fewer than 2 passes. An empty
        // frame between every two frames of rows changes none of it.
        for (boolean isEmptyFrameBetween : new boolean[]{false, true}) {
            for (boolean isParquet : new boolean[]{false, true}) {
                assertHolesAcrossFramesReadCost(19, 18, isEmptyFrameBetween, isParquet, false);
                assertHolesAcrossFramesReadCost(19, 5, isEmptyFrameBetween, isParquet, true);
                assertHolesAcrossFramesReadCost(6, 4, isEmptyFrameBetween, isParquet, true);
            }
        }
    }

    @Test
    public void testKeptKeyMapReadsEveryRowAtMostOnce() throws Exception {
        // A helper that keeps the key map from the second position on, over random slaves with
        // empty frames and random lookups at increasing positions, fewer than the holes the helper
        // keeps. Every slave row must be read at most once.
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            try (Map map = newMap()) {
                for (int iteration = 0; iteration < 200; iteration++) {
                    final int frameCount = 1 + rnd.nextInt(20);
                    final long[] frameRowCounts = new long[frameCount];
                    long rowCount = 0;
                    for (int f = 0; f < frameCount; f++) {
                        frameRowCounts[f] = rnd.nextInt(4) == 0 ? 0 : 1 + rnd.nextInt(2_000);
                        rowCount += frameRowCounts[f];
                    }
                    if (rowCount == 0) {
                        frameRowCounts[frameCount - 1] = 1 + rnd.nextInt(2_000);
                        rowCount = frameRowCounts[frameCount - 1];
                    }
                    final int keyCount = 1 + rnd.nextInt(200);
                    final int[] slaveKeys = new int[(int) rowCount];
                    for (int r = 0; r < rowCount; r++) {
                        slaveKeys[r] = rnd.nextInt(1 + rnd.nextInt(keyCount));
                    }
                    final SlaveCursor cursor = new SlaveCursor(frameRowCounts, row -> slaveKeys[(int) row]);
                    final LongList positions = new LongList();
                    final IntList keys = new IntList();
                    for (long position = rnd.nextLong(rowCount); position < rowCount; position += 1 + rnd.nextInt(50)) {
                        for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                            add(positions, keys, position, rnd.nextInt(keyCount + 1));
                        }
                    }
                    cursor.rowReadCounts = new int[(int) rowCount];
                    lookup(newKeptKeyMapHelper(), cursor, map, positions, keys);
                    for (int r = 0; r < rowCount; r++) {
                        Assert.assertTrue("reads of row " + r, cursor.rowReadCounts[r] <= 1);
                    }
                }
            }
        });
    }

    @Test
    public void testManyKeysMatchWithOrderedKeyMap() throws Exception {
        // The lookups of testRecurringColdKeyKeepsKeyMapWithoutClears() over the map of a join key
        // of several columns, whose keys have their own hash and commit; lookupWithoutReset()
        // checks every match. The helper must not clear the map either.
        assertMemoryLeak(() -> {
            try (ClearCountingOrderedMap map = new ClearCountingOrderedMap()) {
                final HorizonJoinTimeFrameHelper helper = newAdaptiveHelper();
                final LongList positions = new LongList();
                final IntList keys = new IntList();
                final SlaveCursor cursor = recurringColdKeyLookups(positions, keys);
                helper.of(cursor);
                map.clear();
                map.clearCount = 0;
                lookupWithoutReset(helper, cursor, map, positions, keys, null);
                Assert.assertEquals("clears", 0, map.clearCount);
            }
        });
    }

    @Test
    public void testMergedHolesMatchAndKeepBackwardScanCost() throws Exception {
        // Every sixth row holds a hot key, row 5 alone holds a deep key, every 100,000th row holds
        // a rare key, the row 2 rows below the 16,000th position holds a marker key, and the other
        // rows hold 997 cold keys in turn. A helper that keeps the key map from the second
        // position on serves 60,000 positions 6 rows apart that look up the hot key in the row
        // right below them, which leaves the 4 rows below that row and above the previous position
        // unread at every position: more holes than the helper keeps. The 2 read rows between two
        // holes cost less to read again than a hole of 4 rows costs to fill, so the compactions
        // merge the holes and the hot rows between them. Every 777th position also looks up a cold
        // key. The 16,000th position looks up the marker, and the 17,000th looks it up again once
        // the holes around its row have merged: that lookup must not read the rows of the merged
        // hole below the marker. The last position looks up the rare key, whose last row lies in a
        // merged hole, and the first and the last position look up the deep key, which reads every
        // hole. lookupSequence() checks every match, at every position the lookups must read no
        // more rows than backward-only mode does, and some row must be read twice.
        assertMemoryLeak(() -> {
            final int hotKey = 1;
            final int deepKey = 99_999;
            final int markerKey = 100_000;
            final int rareKey = 50_000;
            final int positionCount = 60_000;
            final long firstPosition = 1_000;
            final long markerRow = firstPosition + 6L * 16_000 - 2;
            final long rowCount = firstPosition + 6L * positionCount + 10;
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts(rowCount, 100_000),
                    row -> {
                        if (row == 5) {
                            return deepKey;
                        }
                        if (row == markerRow) {
                            return markerKey;
                        }
                        if (row % 6 == 3) {
                            return hotKey;
                        }
                        if (row % 100_000 == 2) {
                            return rareKey;
                        }
                        return 1_000 + (int) (row % 997);
                    }
            );
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            for (int i = 0; i < positionCount; i++) {
                final long position = firstPosition + 6L * i;
                if (i == positionCount - 1) {
                    add(positions, keys, position, rareKey);
                }
                if (i == 0 || i == positionCount - 1) {
                    add(positions, keys, position, deepKey);
                }
                add(positions, keys, position, hotKey);
                if (i % 777 == 776) {
                    add(positions, keys, position, 1_000 + (int) ((position - 2_001) % 997));
                }
                if (i == 16_000 || i == 17_000) {
                    add(positions, keys, position, markerKey);
                }
            }

            try (
                    Map map = newMap();
                    Map backwardMap = newMap();
                    MissingSymbolRecord symbolRecord = new MissingSymbolRecord(Integer.MAX_VALUE)
            ) {
                final Rnd rnd = TestUtils.generateRandom(LOG);
                final HorizonJoinTimeFrameHelper helper = newKeptKeyMapHelper();
                cursor.rowReadCounts = new int[(int) rowCount];
                final long[] adaptive = lookupSequence(helper, cursor, map, positions, keys, symbolRecord, rnd);
                int maxRowReadCount = 0;
                for (int r = 0; r < rowCount; r++) {
                    maxRowReadCount = Math.max(maxRowReadCount, cursor.rowReadCounts[r]);
                }
                cursor.rowReadCounts = null;
                final long[] backward = lookupSequence(newBackwardOnlyHelper(), cursor, backwardMap, positions, keys, symbolRecord, rnd);
                assertNoMoreRowsThanBackwardOnlyAtEveryPosition(positions, adaptive, backward);
                // The holes must merge, or the test proves nothing.
                Assert.assertTrue("merged holes " + helper.getMergedHoleCount(), helper.getMergedHoleCount() > 0);
                Assert.assertTrue("max reads of a row " + maxRowReadCount, maxRowReadCount > 1);
            }
        });
    }

    @Test
    public void testMissingSymbolKeepsBackwardScanCost() throws Exception {
        // Every sparse position also looks up a symbol that the slave's symbol table lacks. The
        // lookup answers it without a scan, so it costs nothing in backward-only mode either. At
        // the first sparse position, a key that no slave row holds scans the slave down to its
        // first row. The lookups must not read the gaps after that, and every lookup must ask
        // the symbol record whether the slave lacks the symbol at most once.
        assertMemoryLeak(() -> {
            final long gap = 20_037;
            final int absentKey = 5_000;
            final int missingSymbolKey = 6_000;
            final SlaveCursor cursor = new SlaveCursor(frameRowCounts(4_000_000, 1_000_000), row -> (int) (row % 1_000));
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            long position = 100_000;
            for (int i = 0; i < 32; i++) {
                add(positions, keys, position, 7);
                position += 50;
            }
            final int sparseLo = positions.size();
            final long absentKeyPosition = position + gap;
            for (position = absentKeyPosition; position < 4_000_000; position += gap) {
                add(positions, keys, position, 7);
                add(positions, keys, position, missingSymbolKey);
                if (position == absentKeyPosition) {
                    add(positions, keys, position, absentKey);
                }
            }

            try (
                    Map adaptiveMap = newMap();
                    Map backwardMap = newMap();
                    MissingSymbolRecord symbolRecord = new MissingSymbolRecord(missingSymbolKey)
            ) {
                final long[] adaptive = lookup(newAdaptiveHelper(), cursor, adaptiveMap, positions, keys, symbolRecord);
                Assert.assertEquals("missing symbol checks", positions.size(), symbolRecord.nonExistentKeyCheckCount);
                final long[] backward = lookup(newBackwardOnlyHelper(), cursor, backwardMap, positions, keys, symbolRecord);
                assertBurstKeepsKeyMap(adaptive, backward, sparseLo);
                assertSparseStretchKeepsBackwardScanCost(adaptive, backward, sparseLo);
            }
        });
    }

    @Test
    public void testRecurringAbsentKeyKeepsKeyMap() throws Exception {
        // Every 10th sparse position also looks up a key that no slave row holds. Backward-only
        // mode reads the slave down to its first row for it every time. The kept key map reads it
        // once, after which the key map tells that the key is absent from every row read so far,
        // and only the gaps since the last lookup of the key remain to read.
        assertMemoryLeak(() -> {
            final long gap = 20_037;
            final int absentKey = 9_999;
            final SlaveCursor cursor = new SlaveCursor(frameRowCounts(2_000_000, 4_096), row -> (int) (row % 1_000));
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            long position = 100_000;
            for (int i = 0; i < 32; i++) {
                add(positions, keys, position, 7);
                position += 50;
            }
            int sparseIndex = 0;
            for (position += gap; position < 2_000_000; position += gap) {
                if (sparseIndex++ % 10 == 5) {
                    add(positions, keys, position, absentKey);
                }
                add(positions, keys, position, 7);
            }

            try (Map adaptiveMap = newMap(); Map backwardMap = newMap()) {
                final long adaptiveRows = sum(lookup(newAdaptiveHelper(), cursor, adaptiveMap, positions, keys));
                final long backwardRows = sum(lookup(newBackwardOnlyHelper(), cursor, backwardMap, positions, keys));
                Assert.assertTrue(
                        "recurring absent key [adaptive=" + adaptiveRows + ", backwardOnly=" + backwardRows + ']',
                        adaptiveRows * 3 < backwardRows
                );
                // One pass over the slave up to the last position.
                Assert.assertTrue("recurring absent key [adaptive=" + adaptiveRows + ']', adaptiveRows <= positions.getLast() + 1);
            }
        });
    }

    @Test
    public void testRecurringColdKeyKeepsKeyMapWithoutClears() throws Exception {
        // Even rows hold a hot key and odd rows hold 70,000 cold keys in turn. The first lookup
        // scans 140,000 rows backward and puts all 70,001 keys into the key map, which grows to
        // 131,072 slots, and the lookup keeps the key map from the next position on. Hot lookups
        // 100 rows apart follow, and every 300th position also looks up the cold key whose last
        // row lies 19,999 rows back. clear() zeroes the whole capacity of the map, so a lookup
        // that drops the map between two cold lookups zeroes 131,072 slots at a time, or shrinks
        // the map and grows it back, while the cold lookup then reads its 20,000 rows again. The
        // lookup must keep the map without a clear, a shrink or a regrow, and read every row at
        // most once; lookupWithoutReset() checks every match.
        assertMemoryLeak(() -> {
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            final SlaveCursor cursor = recurringColdKeyLookups(positions, keys);

            try (ClearCountingMap map = new ClearCountingMap()) {
                final HorizonJoinTimeFrameHelper helper = newAdaptiveHelper();
                helper.of(cursor);
                map.clear();
                map.clearCount = 0;
                final long[] scannedRows = lookupWithoutReset(helper, cursor, map, positions, keys, null);
                // The first lookup reads down to row 11, which holds the cold key whose last row
                // before the first position lies furthest back.
                Assert.assertEquals(positions.getQuick(0) - 11 + 1, scannedRows[0]);
                Assert.assertEquals("clears", 0, map.clearCount);
                Assert.assertEquals("shrinks", 0, map.shrinkCount);
                Assert.assertEquals("regrows", 0, map.regrowCount);
                // The map must grow, or the test proves nothing about its capacity.
                Assert.assertTrue("capacity " + map.getKeyCapacity(), map.getKeyCapacity() >= 131_072);
                // One pass over the slave up to the last position.
                final long passRows = positions.getLast() + 1;
                final long rows = sum(scannedRows);
                Assert.assertTrue("recurring cold key [rows=" + rows + ", pass=" + passRows + ']', rows <= passRows);
            }
        });
    }

    @Test
    public void testRecurringDeepKeyAtGrowingIntervalsReadsOnePass() throws Exception {
        // Positions 100 rows apart look up a key that every row holds, and four of them also look
        // up a key that only row 5 holds. The first lookup scans 199,999 rows backward, more than
        // the absolute threshold, so the lookup keeps the key map from the next position on. Every
        // later interval is 1.02 times the depth of the deep lookup before it, which is where a
        // rule that drops the key map once the gaps cost more than the deep scans re-scans down to
        // row 5 at every deep lookup, up to three passes over the slave as the lookups go on. The
        // key map remembers row 5, and every row between two positions stays unread until a
        // lookup needs it. The 16,000 or so positions leave fewer holes than the helper keeps, so
        // the lookups read every row at most once: one pass at most.
        assertMemoryLeak(() -> {
            final int deepKey = 5_000;
            final long deepKeyRow = 5;
            final long gap = 100;
            final LongList deepKeyPositions = new LongList();
            long deepKeyPosition = 200_003;
            deepKeyPositions.add(deepKeyPosition);
            for (int i = 1; i < 4; i++) {
                // 1.02 times the depth, rounded up to whole gaps.
                final long interval = (102 * (deepKeyPosition - deepKeyRow) + 99) / 100;
                deepKeyPosition += (interval + gap - 1) / gap * gap;
                deepKeyPositions.add(deepKeyPosition);
            }
            final long positionHi = deepKeyPosition + gap;
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts(positionHi + 1_000, 1_000_000),
                    row -> row == deepKeyRow ? deepKey : 7
            );
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            int deepKeyIndex = 0;
            for (long position = deepKeyPositions.getQuick(0); position <= positionHi; position += gap) {
                if (deepKeyIndex < deepKeyPositions.size() && position == deepKeyPositions.getQuick(deepKeyIndex)) {
                    add(positions, keys, position, deepKey);
                    deepKeyIndex++;
                }
                add(positions, keys, position, 7);
            }

            try (Map map = newMap()) {
                final long adaptiveRows = sum(lookup(newAdaptiveHelper(), cursor, map, positions, keys));
                // One pass over the slave up to the last position.
                final long passRows = positions.getLast() + 1;
                Assert.assertTrue(
                        "recurring deep key [adaptive=" + adaptiveRows + ", pass=" + passRows + ']',
                        adaptiveRows <= passRows
                );
            }
        });
    }

    @Test
    public void testRecurringDeepKeyStaysInKeyMapOverSmallGaps() throws Exception {
        // Positions 100 rows apart look up a key that every 10th row holds, which costs at most
        // 10 rows in backward-only mode, less than the gap. Every 3,000th position also looks up
        // a deep key, so 300,000 rows of small gaps lie between two of its lookups. The frames
        // hold 100,000 rows, so the backward-only cost of a deep lookup spans many frames. The
        // first position scans the slave backward and makes the lookup keep the key map. The key
        // must stay in the key map instead of being scanned for again at every lookup. The deep
        // key is one that only row 5 holds, one that no row holds, and one that only row 400,000
        // holds after the first position has looked up a key that no row holds.
        assertRecurringDeepKeyStaysInKeyMap(5, false);
        assertRecurringDeepKeyStaysInKeyMap(-1, false);
        assertRecurringDeepKeyStaysInKeyMap(400_000, true);
    }

    @Test
    public void testSparseLookupsAfterManyKeysKeepKeyMap() throws Exception {
        // The lookups of testBurstThenSparseKeepsBackwardScanCostOverManyKeys(), which leave about
        // 100,000 keys in the key map after the burst, followed by 200 sparse positions. Every
        // sparse lookup reads at most 1,000 rows, while clear() would zero the 262,144 slots that
        // the burst left in the map.
        assertSparseLookupsAfterManyKeys(5_003, 0);
        assertSparseLookupsAfterManyKeys(7_003, 0);
    }

    @Test
    public void testSwitchCountsRowsThatBackwardScansReadAgain() throws Exception {
        // Row r holds key r % 50, and a position every 8 rows looks up all 50 keys, from key 49 down
        // to key 0. The lookups at a position read the 50 rows below it once. Every lookup whose key
        // the rows read at its position so far lack goes on below them, and a backward scan read the
        // lowest of them once more for it, which the switch rules count. No gap passes the min gap
        // of 1,024 rows, so the window rule checks the gaps of 129 positions: with the rows read
        // again, backward-only mode reads more than 8 rows per row of that window, without them 50
        // rows per position, fewer. The lookup must keep the key map from the position where
        // backward-only mode passes, and read at most the 8 rows of every gap from there on.
        assertMemoryLeak(() -> {
            final int keyCount = 50;
            final long gap = 8;
            final long rowCount = 24_001;
            final SlaveCursor cursor = new SlaveCursor(frameRowCounts(rowCount, rowCount), row -> (int) (row % keyCount));
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            for (long position = 71; position < rowCount; position += gap) {
                for (int key = keyCount - 1; key >= 0; key--) {
                    add(positions, keys, position, key);
                }
            }

            try (Map map = newMap(); MissingSymbolRecord symbolRecord = new MissingSymbolRecord(Integer.MAX_VALUE)) {
                final boolean[] isKeyMapKept = new SwitchModel(BWD_SCAN_ABSOLUTE_THRESHOLD, BWD_SCAN_MIN_GAP, BWD_SCAN_SWITCH_FACTOR, true)
                        .replay(cursor, positions, keys, Integer.MAX_VALUE);
                final boolean[] isKeyMapKeptWithoutRereads = new SwitchModel(BWD_SCAN_ABSOLUTE_THRESHOLD, BWD_SCAN_MIN_GAP, BWD_SCAN_SWITCH_FACTOR, false)
                        .replay(cursor, positions, keys, Integer.MAX_VALUE);
                int switchLookup = 0;
                while (switchLookup < isKeyMapKept.length && !isKeyMapKept[switchLookup]) {
                    switchLookup++;
                }
                // Backward-only mode switches at the 130th position, and only with the rows read again.
                Assert.assertEquals(129 * keyCount, switchLookup);
                Assert.assertFalse(isKeyMapKeptWithoutRereads[isKeyMapKeptWithoutRereads.length - 1]);

                final long[] scannedRows = assertSwitchesWhereBackwardScansDo(
                        newAdaptiveHelper(),
                        cursor,
                        map,
                        positions,
                        keys,
                        symbolRecord,
                        isKeyMapKept
                );
                Assert.assertEquals(129 * keyCount, sum(scannedRows, 0, switchLookup));
                for (int lo = switchLookup, n = scannedRows.length; lo < n; lo += keyCount) {
                    final long rows = sum(scannedRows, lo, lo + keyCount);
                    Assert.assertTrue("rows at lookup " + lo + " [rows=" + rows + ']', rows <= gap);
                }
            }
        });
    }

    @Test
    public void testToTopResetsKeyMapState() throws Exception {
        // lookup() calls toTop() through of(). The first pass ends with the key map kept after a
        // burst and a rare key 100,000 rows deep. A second pass over the same positions must read
        // exactly the rows of the first pass.
        assertMemoryLeak(() -> {
            final long gap = 20_037;
            final int rareKey = 5_000;
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts(1_000_000, 1_000_000),
                    row -> row == 200_000 ? rareKey : (int) (row % 1_000)
            );
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            long position = 100_000;
            for (int i = 0; i < 128; i++) {
                add(positions, keys, position, 7);
                position += 50;
            }
            position += gap;
            add(positions, keys, position, 7);
            position = 300_000;
            add(positions, keys, position, 7);
            add(positions, keys, position, rareKey);
            add(positions, keys, position + gap, 7);

            try (Map map = newMap()) {
                final HorizonJoinTimeFrameHelper helper = newAdaptiveHelper();
                final long[] firstPass = lookup(helper, cursor, map, positions, keys);
                final long[] secondPass = lookup(helper, cursor, map, positions, keys);
                Assert.assertArrayEquals(firstPass, secondPass);
            }
        });
    }

    private static void add(LongList positions, IntList keys, long position, int key) {
        positions.add(position);
        keys.add(key);
    }

    private static void assertBurstKeepsKeyMap(long[] adaptive, long[] backward, int sparseLo) {
        // The burst must make the helper keep the key map, or the test proves nothing: the
        // positions after the switch read only the rows their lookups need.
        final long adaptiveBurstRows = sum(adaptive, 0, sparseLo);
        final long backwardBurstRows = sum(backward, 0, sparseLo);
        Assert.assertTrue(
                "burst [adaptive=" + adaptiveBurstRows + ", backwardOnly=" + backwardBurstRows + ']',
                adaptiveBurstRows < backwardBurstRows
        );
    }

    private static void assertBurstThenSparseKeepsBackwardScanCost(long frameRowCount, long minGap) throws Exception {
        // 1,000 keys in round-robin order, so a backward scan for a key reads up to 1,000 rows.
        // A burst of 32 positions 50 rows apart looks up one key and makes the lookup keep the
        // key map. 40 positions about 20,000 rows apart follow. A backward scan reads at most
        // 1,000 rows at each of them; the gap holds 20,000 rows.
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(frameRowCounts(1_000_000, frameRowCount), row -> (int) (row % 1_000));
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            long position = 100_000;
            for (int i = 0; i < 32; i++) {
                add(positions, keys, position, 7);
                position += 50;
            }
            final int sparseLo = positions.size();
            for (int i = 0; i < 40; i++) {
                position += 20_037;
                add(positions, keys, position, 7);
            }
            assertBurstThenSparseKeepsBackwardScanCost(cursor, positions, keys, sparseLo, minGap);
        });
    }

    private static void assertBurstThenSparseKeepsBackwardScanCost(
            SlaveCursor cursor,
            LongList positions,
            IntList keys,
            int sparseLo,
            long minGap
    ) {
        try (Map adaptiveMap = newMap(); Map backwardMap = newMap()) {
            final long[] adaptive = lookup(newAdaptiveHelper(minGap), cursor, adaptiveMap, positions, keys);
            final long[] backward = lookup(newBackwardOnlyHelper(), cursor, backwardMap, positions, keys);
            assertBurstKeepsKeyMap(adaptive, backward, sparseLo);
            assertSparseStretchKeepsBackwardScanCost(adaptive, backward, sparseLo);
        }
    }

    private static void assertCompactedHolesReadCost(
            int readRows,
            boolean isMerge,
            long frameRowCount,
            IntPredicate parquetFrames
    ) throws Exception {
        assertMemoryLeak(() -> {
            final int gap = 20;
            final int deepKey = 5_000;
            final int hotKey = 7;
            final long firstPosition = 1_003;
            final long hotResidue = Math.floorMod(firstPosition - readRows + 1, gap);
            final int positionCount = 5_000;
            final long lastPosition = firstPosition + (long) gap * (positionCount - 1);
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts(lastPosition + 1_000, frameRowCount),
                    row -> row == 5 ? deepKey : (row % gap == hotResidue ? hotKey : 1_000 + (int) (row % 997))
            );
            cursor.parquetFrames = parquetFrames;
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            for (int i = 0; i < positionCount; i++) {
                final long position = firstPosition + (long) gap * i;
                if (i == 0 || i == positionCount - 1) {
                    add(positions, keys, position, deepKey);
                }
                add(positions, keys, position, hotKey);
            }

            try (Map map = newMap()) {
                final HorizonJoinTimeFrameHelper helper = newKeptKeyMapHelper(64);
                final long rows = sum(lookup(helper, cursor, map, positions, keys));
                final long passRows = lastPosition + 1;
                final String message = "[readRows=" + readRows + ", frameRowCount=" + frameRowCount + ", isParquet=" + parquetFrames.test(0)
                        + ", rows=" + rows + ", pass=" + passRows
                        + ", filledRows=" + helper.getFilledRowCount() + ", mergedHoles=" + helper.getMergedHoleCount() + ']';
                if (isMerge) {
                    Assert.assertTrue("merged holes " + message, helper.getMergedHoleCount() > positionCount / 2);
                    Assert.assertTrue("rows " + message, 2 * rows < 3 * passRows);
                } else {
                    Assert.assertTrue("filled rows " + message, helper.getFilledRowCount() > 0);
                    Assert.assertTrue("rows " + message, rows <= passRows);
                }
            }
        });
    }

    /**
     * Runs the lookups of {@link #testCompactionFillsHoleAcrossFrames}: the hot key at row 35, at
     * the given second position and at the position right above it, where the holes compact, then
     * the keys that single rows up to that position hold, from the top down, with a key that rows
     * 40 and 37 hold after the key of row 39, over a slave whose Parquet frames the predicate
     * tells. Asserts the rows that the compaction filled, the holes that it merged, the rows that
     * the lookups after the compaction read and the rows that all lookups and fills read.
     */
    private static void assertCompactionAcrossFrames(
            long secondPosition,
            IntPredicate parquetFrames,
            long filledRows,
            long mergedHoles,
            long lookupRows,
            long rows
    ) throws Exception {
        assertMemoryLeak(() -> {
            final int hotKey = 1;
            final int sharedKey = 500;
            final int singleRowKeyLo = 100;
            final long[] frameRowCounts = {5, 5, 0, 5, 10, 3, 3, 3, 6, 0, 3, 6};
            // Rows 44 and 43 in frame 11, 42 in frame 10, 39, 38 and 36 in frame 8, then 20, 12, 8
            // and 3 in frames 4, 3, 1 and 0, all in the bottom hole.
            final LongList singleKeyRows = new LongList();
            for (long row : new long[]{44, 43, 42, 39, 38, 36, 20, 12, 8, 3}) {
                singleKeyRows.add(row);
            }
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts,
                    row -> {
                        if (row == 22 || row == 41 || row == 45) {
                            return hotKey;
                        }
                        if (row == 40 || row == 37) {
                            return sharedKey;
                        }
                        final int keyIndex = singleKeyRows.indexOf(row);
                        return keyIndex >= 0 ? singleRowKeyLo + keyIndex : 0;
                    }
            );
            cursor.parquetFrames = parquetFrames;
            final long lastPosition = secondPosition + 1;
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            add(positions, keys, 35, hotKey);
            add(positions, keys, secondPosition, hotKey);
            add(positions, keys, lastPosition, hotKey);
            final int lookupLo = positions.size();
            for (int k = 0; k < singleKeyRows.size(); k++) {
                final long row = singleKeyRows.getQuick(k);
                if (row <= lastPosition) {
                    add(positions, keys, lastPosition, singleRowKeyLo + k);
                }
                if (row == 39) {
                    add(positions, keys, lastPosition, sharedKey);
                }
            }

            try (Map map = newMap()) {
                final HorizonJoinTimeFrameHelper helper = newKeptKeyMapHelper(2);
                helper.of(cursor);
                map.clear();
                final long[] scannedRows = lookupWithoutReset(helper, cursor, map, positions, keys, null);
                final String message = "[secondPosition=" + secondPosition + ", parquetFrames=" + parquetFrameList(cursor) + ']';
                Assert.assertEquals("filled rows " + message, filledRows, helper.getFilledRowCount());
                Assert.assertEquals("merged holes " + message, mergedHoles, helper.getMergedHoleCount());
                Assert.assertEquals("rows after the compaction " + message, lookupRows, sum(scannedRows, lookupLo, scannedRows.length));
                Assert.assertEquals("rows " + message, rows, sum(scannedRows));
            }
        });
    }

    /**
     * A helper that keeps at most 16 holes keeps the key map from the second position on. The first
     * position looks up a key that only row 5 holds; then 2,000 positions 20 rows apart look up a hot
     * key 11 rows below them, so every position leaves a hole of 8 rows above the 12 rows that its
     * lookup reads. A hole costs less to fill than the read rows next to it, so the holes fill at
     * every compaction, every few positions. The last position looks up the key of row 5 again,
     * which crosses every hole left. The same lookups then run again from the first position, below
     * the last one, which starts a new epoch, whose compactions must fill the same rows as the first
     * epoch's did. Every match is checked.
     * <p>
     * The predicate tells the frames of a Parquet partition. In those, a compaction must read no row
     * at or below the position of the previous compaction, nor in a frame 3 frames or more below the
     * frame of the position before the compaction, and when the slave has Parquet frames, it must
     * read the rows it fills from the top down: the reads of the lookup that runs it then go down
     * from the top twice at most, once for the fills and once from its position. With a Parquet
     * slave in a single frame, the lookups and fills of the first epoch must read every row at most
     * once. In native frames, with frames of 50 rows or fewer, the fills of most compactions must
     * reach frames 3 frames or more below the position.
     */
    private static void assertCompactionFills(long frameRowCount, IntPredicate parquetFrames) throws Exception {
        assertMemoryLeak(() -> {
            final int deepKey = 5_000;
            final int hotKey = 7;
            final long gap = 20;
            final long firstPosition = 2_003;
            final int positionCount = 2_000;
            final long lastPosition = firstPosition + gap * (positionCount - 1);
            final long rowCount = lastPosition + 1_000;
            final long hotResidue = Math.floorMod(firstPosition - 11, gap);
            final long[] frameRowCounts = frameRowCounts(rowCount, Math.min(frameRowCount, rowCount));
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts,
                    row -> row == 5 ? deepKey : (row % gap == hotResidue ? hotKey : 1_000 + (int) (row % 997))
            );
            cursor.parquetFrames = parquetFrames;
            boolean hasParquetFrames = false;
            boolean hasNativeFrames = false;
            for (int f = 0; f < frameRowCounts.length; f++) {
                if (parquetFrames.test(f)) {
                    hasParquetFrames = true;
                } else {
                    hasNativeFrames = true;
                }
            }
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            for (int epoch = 0; epoch < 2; epoch++) {
                add(positions, keys, firstPosition, deepKey);
                for (int i = 0; i < positionCount; i++) {
                    add(positions, keys, firstPosition + gap * i, hotKey);
                }
                add(positions, keys, lastPosition, deepKey);
            }
            final int secondEpochLo = positions.size() / 2;

            try (Map map = newMap()) {
                final HorizonJoinTimeFrameHelper helper = newKeptKeyMapHelper(16);
                helper.of(cursor);
                map.clear();
                cursor.rowReadCounts = new int[(int) rowCount];
                final MasterRecord masterRecord = new MasterRecord();
                // The position before the one of the previous compaction of the epoch, below which
                // the holes were there at that compaction; -1 before the first compaction.
                long compactedRow = -1;
                long prevPosition = -1;
                // findAsOfRow() expects positions that don't decrease until toTop().
                long asOfPosition = -1;
                final int[] fillingCompactionCounts = new int[2];
                final long[] filledRowCounts = new long[2];
                int farFrameCompactionCount = 0;
                int farNativeFillCompactionCount = 0;
                for (int i = 0, n = positions.size(); i < n; i++) {
                    final long position = positions.getQuick(i);
                    if (i == secondEpochLo) {
                        if (frameRowCount >= rowCount && hasParquetFrames) {
                            for (int r = 0; r < rowCount; r++) {
                                Assert.assertTrue("reads of row " + r, cursor.rowReadCounts[r] <= 1);
                            }
                        }
                        cursor.rowReadCounts = null;
                        compactedRow = -1;
                    }
                    final long asOfRowId;
                    if (position >= asOfPosition) {
                        asOfRowId = helper.findAsOfRow(position);
                        asOfPosition = position;
                    } else {
                        asOfRowId = cursor.rowIdOf(position);
                    }
                    masterRecord.key = keys.getQuick(i);
                    final long filledRowCount = helper.getFilledRowCount();
                    final long mergedHoleCount = helper.getMergedHoleCount();
                    cursor.resetReadOrder();
                    final long matchRowId = helper.findKeyedAsOfMatch(asOfRowId, masterRecord, MASTER_KEY_SINK, SLAVE_KEY_SINK, map, null);
                    Assert.assertEquals("match of lookup " + i, cursor.findMatch(position, masterRecord.key), matchRowId);
                    // The last row of the frame 3 frames below the frame of the position before the
                    // compaction, or -1 when there is no such frame.
                    final long prevFrameIndex = prevPosition < 0 ? 0 : prevPosition / frameRowCount;
                    final long farFrameRowHi = prevFrameIndex > 2 ? (prevFrameIndex - 2) * frameRowCount - 1 : -1;
                    final String message = "[frameRowCount=" + frameRowCount + ", lookup=" + i + ", position=" + position
                            + ", minParquetReadRow=" + cursor.minParquetReadRow + ", minNativeReadRow=" + cursor.minNativeReadRow
                            + ", previousCompactionRow=" + compactedRow + ", farFrameRowHi=" + farFrameRowHi
                            + ", readAscents=" + cursor.readAscentCount + ']';
                    if (helper.getFilledRowCount() > filledRowCount) {
                        fillingCompactionCounts[i < secondEpochLo ? 0 : 1]++;
                        filledRowCounts[i < secondEpochLo ? 0 : 1] += helper.getFilledRowCount() - filledRowCount;
                        if (farFrameRowHi > compactedRow) {
                            farFrameCompactionCount++;
                        }
                        if (cursor.minNativeReadRow <= farFrameRowHi) {
                            farNativeFillCompactionCount++;
                        }
                        Assert.assertTrue("compaction fills a Parquet hole below the previous compaction " + message, cursor.minParquetReadRow > compactedRow);
                        Assert.assertTrue("compaction fills a Parquet hole 3 frames or more below the position " + message, cursor.minParquetReadRow > farFrameRowHi);
                        if (hasParquetFrames) {
                            Assert.assertTrue("compaction fills from the bottom up " + message, cursor.readAscentCount <= 1);
                        }
                    }
                    if (helper.getFilledRowCount() > filledRowCount || helper.getMergedHoleCount() > mergedHoleCount) {
                        compactedRow = prevPosition;
                    }
                    prevPosition = position;
                }
                cursor.isReadOrderTracked = false;
                final String message = "[frameRowCount=" + frameRowCount + ", fillingCompactions=" + fillingCompactionCounts[0]
                        + ", farFrameCompactions=" + farFrameCompactionCount + ", farNativeFillCompactions=" + farNativeFillCompactionCount + ']';
                for (int epoch = 0; epoch < 2; epoch++) {
                    Assert.assertTrue("compactions that fill in epoch " + epoch + ' ' + message, fillingCompactionCounts[epoch] > 100);
                }
                Assert.assertEquals("filled rows of the second epoch", filledRowCounts[0], filledRowCounts[1]);
                if (frameRowCount <= 50) {
                    Assert.assertTrue("compactions 3 frames above the previous one " + message, farFrameCompactionCount > 100);
                    if (hasNativeFrames) {
                        Assert.assertTrue("compactions that fill native holes 3 frames below " + message, farNativeFillCompactionCount > 100);
                    }
                }
            }
        });
    }

    /**
     * Frames 0 to historyFrameCount - 1 hold 100 rows each, and the frame of the epoch after them
     * holds 1,000 rows. The first position, row 200 of the epoch's frame, looks up a deep key that
     * only row 2 of the frame below holds: its lookup reads down to that row and leaves every row
     * below it in the bottom hole, which spans all frames below the epoch and holds only 2 rows in
     * its top frame. A helper that keeps at most 2, 4, 8 or 16 holes keeps the key map from the
     * second position on. 24 positions 20 rows apart follow and look up a hot key 5 rows below
     * each of them, so every position leaves a hole of 14 rows above 6 read rows, and the holes
     * compact. The read rows between the bottom hole and the hole above it span frames, so the two
     * never merge. Up to the last position, the lookups and fills must read no more rows than
     * forward scans from the row of the deep key up. A last lookup of the deep key crosses every
     * hole above that row. No row below it may be read.
     */
    private static void assertCompactionLeavesRowsBelowEpochUnread(int historyFrameCount) throws Exception {
        assertMemoryLeak(() -> {
            final int deepKey = 2;
            final int hotKey = 1;
            final long historyFrameRowCount = 100;
            final long[] frameRowCounts = new long[historyFrameCount + 1];
            for (int f = 0; f < historyFrameCount; f++) {
                frameRowCounts[f] = historyFrameRowCount;
            }
            frameRowCounts[historyFrameCount] = 1_000;
            final long epochFrameRowLo = historyFrameCount * historyFrameRowCount;
            final long rowCount = epochFrameRowLo + frameRowCounts[historyFrameCount];
            final long deepKeyRow = epochFrameRowLo - historyFrameRowCount + 2;
            final long firstPosition = epochFrameRowLo + 200;
            final long gap = 20;
            final long hotKeyDepth = 5;
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts,
                    row -> {
                        if (row == deepKeyRow) {
                            return deepKey;
                        }
                        final long hotRowLo = firstPosition - hotKeyDepth;
                        return row >= hotRowLo && (row - hotRowLo) % gap == 0 ? hotKey : 0;
                    }
            );
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            add(positions, keys, firstPosition, deepKey);
            add(positions, keys, firstPosition, hotKey);
            for (int i = 1; i <= 24; i++) {
                add(positions, keys, firstPosition + gap * i, hotKey);
            }
            final long lastPosition = positions.getLast();
            add(positions, keys, lastPosition, deepKey);

            try (Map map = newMap()) {
                for (int maxHoleCount : new int[]{2, 4, 8, 16}) {
                    cursor.rowReadCounts = new int[(int) rowCount];
                    final HorizonJoinTimeFrameHelper helper = newKeptKeyMapHelper(maxHoleCount);
                    final long[] scannedRows = lookup(helper, cursor, map, positions, keys);
                    final long rows = sum(scannedRows, 0, scannedRows.length - 1);
                    final long forwardScanRows = lastPosition - deepKeyRow + 1;
                    long rowsReadBelowEpoch = 0;
                    for (int row = 0; row < deepKeyRow; row++) {
                        rowsReadBelowEpoch += cursor.rowReadCounts[row];
                    }
                    final String message = "[historyFrameCount=" + historyFrameCount + ", maxHoleCount=" + maxHoleCount
                            + ", rows=" + rows + ", forwardScanRows=" + forwardScanRows + ", filledRows=" + helper.getFilledRowCount()
                            + ", mergedHoles=" + helper.getMergedHoleCount() + ']';
                    Assert.assertEquals("rows read below the epoch " + message, 0, rowsReadBelowEpoch);
                    Assert.assertTrue("rows " + message, rows <= forwardScanRows);
                }
            }
        });
    }

    /**
     * Runs a burst, then a sparse stretch whose lookups in {@code [deepLo, deepHi)}, all at one
     * ASOF position, scan backward deeper than a sparse gap but not deeper than the absolute
     * threshold, so the merge-base rule keeps backward-only mode there.
     */
    private static void assertDeepScanAfterBurstKeepsBackwardScanCost(
            SlaveCursor cursor,
            LongList positions,
            IntList keys,
            int sparseLo,
            int deepLo,
            int deepHi,
            long gap
    ) {
        try (Map adaptiveMap = newMap(); Map backwardMap = newMap()) {
            final long[] adaptive = lookup(newAdaptiveHelper(), cursor, adaptiveMap, positions, keys);
            final long[] backward = lookup(newBackwardOnlyHelper(), cursor, backwardMap, positions, keys);
            assertBurstKeepsKeyMap(adaptive, backward, sparseLo);
            final long deepRows = sum(backward, deepLo, deepHi);
            Assert.assertTrue(
                    "deep scan [rows=" + deepRows + ", gap=" + gap + ']',
                    deepRows > gap && deepRows <= BWD_SCAN_ABSOLUTE_THRESHOLD
            );
            assertSparseStretchKeepsBackwardScanCost(adaptive, backward, sparseLo);
        }
    }

    /**
     * Frame 0 holds rows 0 to 999, and 5,000 frames of 20 rows follow, with an empty frame between
     * every two of them if asked. A helper that keeps at most 64 holes keeps the key map from the
     * second position on. The first position, row 999, looks up a key that only row 5 holds and
     * a hot key; one position at row positionRowIndex of every frame of 20 rows follows and looks
     * up the hot key, which sits hotKeyDepth rows below it; the last position looks up the deep
     * key again, which crosses every hole. When isMerge is set, the holes must merge, and in
     * Parquet frames none may fill; in native frames, the bottom hole may still fill within the
     * first frame. Otherwise, in native frames the holes must fill and none may merge, and in
     * Parquet frames the holes that start 3 frames or more below the position at a compaction
     * must merge and the others fill.
     */
    private static void assertHolesAcrossFramesReadCost(
            int positionRowIndex,
            int hotKeyDepth,
            boolean isEmptyFrameBetween,
            boolean isParquet,
            boolean isMerge
    ) throws Exception {
        assertMemoryLeak(() -> {
            final int deepKey = 5_000;
            final int hotKey = 7;
            final int rowFrameCount = 5_000;
            final long frameRowCount = 20;
            final long firstPosition = 999;
            final long[] frameRowCounts = new long[1 + (isEmptyFrameBetween ? 2 * rowFrameCount - 1 : rowFrameCount)];
            frameRowCounts[0] = firstPosition + 1;
            for (int f = 1; f < frameRowCounts.length; f++) {
                frameRowCounts[f] = isEmptyFrameBetween && (f & 1) == 0 ? 0 : frameRowCount;
            }
            final long hotRowIndex = positionRowIndex - hotKeyDepth;
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts,
                    row -> {
                        if (row == 5) {
                            return deepKey;
                        }
                        if (row == firstPosition || (row > firstPosition && (row - firstPosition - 1) % frameRowCount == hotRowIndex)) {
                            return hotKey;
                        }
                        return 1_000 + (int) (row % 997);
                    }
            );
            cursor.parquetFrames = isParquet ? PARQUET_FRAMES : NATIVE_FRAMES;
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            add(positions, keys, firstPosition, deepKey);
            add(positions, keys, firstPosition, hotKey);
            for (int i = 0; i < rowFrameCount; i++) {
                add(positions, keys, firstPosition + 1 + frameRowCount * i + positionRowIndex, hotKey);
            }
            add(positions, keys, positions.getLast(), deepKey);

            try (Map map = newMap()) {
                final HorizonJoinTimeFrameHelper helper = newKeptKeyMapHelper(64);
                final long rows = sum(lookup(helper, cursor, map, positions, keys));
                final long passRows = positions.getLast() + 1;
                final String message = "[positionRowIndex=" + positionRowIndex + ", hotKeyDepth=" + hotKeyDepth
                        + ", isEmptyFrameBetween=" + isEmptyFrameBetween + ", isParquet=" + isParquet + ", rows=" + rows
                        + ", pass=" + passRows + ", filledRows=" + helper.getFilledRowCount() + ", mergedHoles=" + helper.getMergedHoleCount() + ']';
                if (isMerge) {
                    Assert.assertTrue("merged holes " + message, helper.getMergedHoleCount() > rowFrameCount / 2);
                    if (isParquet) {
                        Assert.assertEquals("filled rows " + message, 0, helper.getFilledRowCount());
                    }
                    Assert.assertTrue("rows " + message, 2 * rows < 3 * passRows);
                } else if (isParquet) {
                    Assert.assertTrue("merged holes " + message, helper.getMergedHoleCount() > rowFrameCount / 2);
                    Assert.assertTrue("filled rows " + message, helper.getFilledRowCount() > 0);
                    Assert.assertTrue("rows " + message, rows < 2 * passRows);
                } else {
                    Assert.assertEquals("merged holes " + message, 0, helper.getMergedHoleCount());
                    Assert.assertTrue("filled rows " + message, helper.getFilledRowCount() > 0);
                    Assert.assertTrue("rows " + message, rows <= passRows);
                }
            }
        });
    }

    /**
     * Compares the rows that two runs of the same lookup sequence read at every ASOF position,
     * a run of lookups at the same position between two toTop() calls.
     */
    private static void assertNoMoreRowsThanBackwardOnlyAtEveryPosition(LongList positions, long[] adaptive, long[] backward) {
        for (int lo = 0, n = positions.size(); lo < n; ) {
            final long position = positions.getQuick(lo);
            long adaptiveRows = 0;
            long backwardRows = 0;
            int hi = lo;
            while (hi < n && positions.getQuick(hi) == position) {
                adaptiveRows += adaptive[hi];
                backwardRows += backward[hi];
                hi++;
            }
            if (adaptiveRows > backwardRows) {
                Assert.fail("rows at position " + position + " [lookup=" + lo + ", adaptive=" + adaptiveRows + ", backwardOnly=" + backwardRows + ']');
            }
            lo = hi;
        }
    }

    private static void assertRecurringDeepKeyStaysInKeyMap(long deepKeyRow, boolean isAbsentKeyFirst) throws Exception {
        assertMemoryLeak(() -> {
            final int deepKey = 5_000;
            final int absentKey = 9_999;
            final long rowCount = 3_000_000;
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts(rowCount, 100_000),
                    row -> row == deepKeyRow ? deepKey : (int) (row % 10)
            );
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            int positionIndex = 0;
            for (long position = 200_003; position < rowCount; position += 100, positionIndex++) {
                if (positionIndex == 0 && isAbsentKeyFirst) {
                    add(positions, keys, position, absentKey);
                } else if (positionIndex % 3_000 == 0) {
                    add(positions, keys, position, deepKey);
                }
                add(positions, keys, position, 7);
            }

            try (Map map = newMap()) {
                final long adaptiveRows = sum(lookup(newAdaptiveHelper(), cursor, map, positions, keys));
                // One pass over the slave up to the last position.
                final long passRows = positions.getLast() + 1;
                Assert.assertTrue(
                        "recurring deep key [deepKeyRow=" + deepKeyRow + ", isAbsentKeyFirst=" + isAbsentKeyFirst
                                + ", adaptive=" + adaptiveRows + ", pass=" + passRows + ']',
                        2 * adaptiveRows <= 3 * passRows
                );
            }
        });
    }

    /**
     * Runs the burst of testBurstThenSparseKeepsBackwardScanCostOverManyKeys() and then 200
     * sparse positions the given gap apart that look up the key every 1,000th row holds. Every
     * deepLookupPeriod-th of them also looks up the key 20,000 rows back, unless the period is 0.
     * Asserts that the helper never clears, shrinks or regrows the key map in the sparse stretch,
     * and that the sparse lookups read no more rows than backward-only mode does.
     */
    private static void assertSparseLookupsAfterManyKeys(long gap, int deepLookupPeriod) throws Exception {
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(
                    frameRowCounts(1_700_000, 1_000_000),
                    row -> row % 1_000 == 0 ? 7 : 1_000 + (int) (row % 100_000)
            );
            final LongList burstPositions = new LongList();
            final IntList burstKeys = new IntList();
            long position = 2_007;
            for (int i = 0; i < 4_000; i++) {
                add(burstPositions, burstKeys, position, 7);
                position += 50;
            }
            final LongList sparsePositions = new LongList();
            final IntList sparseKeys = new IntList();
            for (int i = 1; i <= 200; i++) {
                position += gap;
                add(sparsePositions, sparseKeys, position, 7);
                if (deepLookupPeriod > 0 && i % deepLookupPeriod == 0) {
                    add(sparsePositions, sparseKeys, position, 1_000 + (int) ((position - 20_000) % 100_000));
                }
            }

            try (ClearCountingMap map = new ClearCountingMap(); Map backwardMap = newMap()) {
                final HorizonJoinTimeFrameHelper helper = newAdaptiveHelper();
                lookup(helper, cursor, map, burstPositions, burstKeys);
                // The burst must leave the capacity of many keys, or the test proves nothing.
                Assert.assertTrue("capacity after the burst " + map.getKeyCapacity(), map.getKeyCapacity() >= 131_072);
                map.clearCount = 0;
                final long[] sparseRows = lookupWithoutReset(helper, cursor, map, sparsePositions, sparseKeys, null);
                final String message = "[gap=" + gap + ", deepLookupPeriod=" + deepLookupPeriod + ']';
                Assert.assertEquals("clears in the sparse stretch " + message, 0, map.clearCount);
                Assert.assertEquals("shrinks in the sparse stretch " + message, 0, map.shrinkCount);
                Assert.assertEquals("regrows in the sparse stretch " + message, 0, map.regrowCount);

                final HorizonJoinTimeFrameHelper backwardHelper = newBackwardOnlyHelper();
                lookup(backwardHelper, cursor, backwardMap, burstPositions, burstKeys);
                final long[] backwardSparseRows = lookupWithoutReset(backwardHelper, cursor, backwardMap, sparsePositions, sparseKeys, null);
                assertNoMoreRowsThanBackwardOnlyAtEveryPosition(sparsePositions, sparseRows, backwardSparseRows);
            }
        });
    }

    private static void assertSparseStretchKeepsBackwardScanCost(long[] adaptive, long[] backward, int sparseLo) {
        final long adaptiveRows = sum(adaptive, sparseLo, adaptive.length);
        final long backwardRows = sum(backward, sparseLo, backward.length);
        Assert.assertTrue(
                "sparse stretch [adaptive=" + adaptiveRows + ", backwardOnly=" + backwardRows + ']',
                adaptiveRows <= 2 * backwardRows
        );
    }

    /**
     * Runs a lookup sequence as {@link #lookupSequence} does, without the moves of the time frame
     * cursor, checks every match against a brute-force scan, and checks after every lookup that the
     * helper keeps the key map exactly where the given flags tell. Returns the rows that each
     * lookup read.
     */
    private static long[] assertSwitchesWhereBackwardScansDo(
            HorizonJoinTimeFrameHelper helper,
            SlaveCursor cursor,
            Map map,
            LongList positions,
            IntList keys,
            MissingSymbolRecord symbolRecord,
            boolean[] isKeyMapKept
    ) {
        helper.of(cursor);
        map.clear();
        final MasterRecord masterRecord = new MasterRecord();
        symbolRecord.of(masterRecord);
        final long[] scannedRows = new long[positions.size()];
        long prevPosition = -1;
        for (int i = 0, n = positions.size(); i < n; i++) {
            final long position = positions.getQuick(i);
            if (position == TO_TOP) {
                helper.toTop();
                map.clear();
                prevPosition = -1;
                continue;
            }
            final long asOfRowId;
            if (position >= prevPosition) {
                asOfRowId = helper.findAsOfRow(position);
                Assert.assertEquals("ASOF position of lookup " + i, cursor.rowIdOf(position), asOfRowId);
                prevPosition = position;
            } else {
                asOfRowId = cursor.rowIdOf(position);
            }
            masterRecord.key = keys.getQuick(i);
            final long keyReadCount = cursor.getKeyReadCount();
            final long matchRowId = helper.findKeyedAsOfMatch(asOfRowId, masterRecord, MASTER_KEY_SINK, SLAVE_KEY_SINK, map, symbolRecord);
            scannedRows[i] = cursor.getKeyReadCount() - keyReadCount;
            final long expectedRowId = masterRecord.key >= symbolRecord.missingKeyLo ? Long.MIN_VALUE : cursor.findMatch(position, masterRecord.key);
            if (expectedRowId != matchRowId) {
                Assert.fail("match of lookup " + i + " [position=" + position + ", key=" + masterRecord.key
                        + ", expected=" + expectedRowId + ", actual=" + matchRowId + ']');
            }
            if (isKeyMapKept[i] != helper.isKeyMapKept()) {
                Assert.fail("key map kept at lookup " + i + " [position=" + position + ", key=" + masterRecord.key
                        + ", expected=" + isKeyMapKept[i] + ", actual=" + helper.isKeyMapKept() + ']');
            }
        }
        return scannedRows;
    }

    /**
     * A helper that keeps at most 16 holes keeps the key map from the second position on. The
     * first position, row 1,003 in frame 50 of frames of 20 rows, looks up a key that only row 25
     * holds; then 2,000 positions 20 rows apart look up a hot key 11 rows below them, and the last
     * position looks up the key of row 25 again. Returns the rows that the lookups and the fills
     * read, the rows that the compactions filled and the holes that they merged.
     */
    private static long[] countCompactionReads(IntPredicate parquetFrames) {
        final int deepKey = 5_000;
        final int hotKey = 7;
        final long gap = 20;
        final long firstPosition = 1_003;
        final int positionCount = 2_000;
        final long lastPosition = firstPosition + gap * (positionCount - 1);
        final long hotResidue = Math.floorMod(firstPosition - 11, gap);
        final SlaveCursor cursor = new SlaveCursor(
                frameRowCounts(lastPosition + 1_000, 20),
                row -> row == 25 ? deepKey : (row % gap == hotResidue ? hotKey : 1_000 + (int) (row % 997))
        );
        cursor.parquetFrames = parquetFrames;
        final LongList positions = new LongList();
        final IntList keys = new IntList();
        add(positions, keys, firstPosition, deepKey);
        for (int i = 0; i < positionCount; i++) {
            add(positions, keys, firstPosition + gap * i, hotKey);
        }
        add(positions, keys, lastPosition, deepKey);
        try (Map map = newMap()) {
            final HorizonJoinTimeFrameHelper helper = newKeptKeyMapHelper(16);
            final long rows = sum(lookup(helper, cursor, map, positions, keys));
            return new long[]{rows, helper.getFilledRowCount(), helper.getMergedHoleCount()};
        }
    }

    private static long[] frameRowCounts(long rowCount, long frameRowCount) {
        final int frameCount = (int) ((rowCount + frameRowCount - 1) / frameRowCount);
        final long[] frameRowCounts = new long[frameCount];
        for (int f = 0; f < frameCount; f++) {
            frameRowCounts[f] = Math.min(frameRowCount, rowCount - f * frameRowCount);
        }
        return frameRowCounts;
    }

    /**
     * Runs the lookups through the helper in order, checks every match against a brute-force scan
     * and returns the number of slave rows the keyed lookup read at each one.
     */
    private static long[] lookup(
            HorizonJoinTimeFrameHelper helper,
            SlaveCursor cursor,
            Map map,
            LongList positions,
            IntList keys
    ) {
        return lookup(helper, cursor, map, positions, keys, null);
    }

    private static long[] lookup(
            HorizonJoinTimeFrameHelper helper,
            SlaveCursor cursor,
            Map map,
            LongList positions,
            IntList keys,
            @Nullable SymbolTranslatingRecord symbolTranslatingRecord
    ) {
        helper.of(cursor);
        map.clear();
        return lookupWithoutReset(helper, cursor, map, positions, keys, symbolTranslatingRecord);
    }

    /**
     * Runs a lookup sequence as the factories do: of(), and toTop() with a clear of the key map
     * where the sequence holds {@link #TO_TOP}. It moves the time frame cursor to a random frame
     * and positions the record at the match between the lookups, and checks every match against a
     * brute-force scan; a missing symbol has no match. findAsOfRow() expects timestamps that do
     * not decrease until toTop(), so a position below the previous one takes its ASOF row id from
     * the mock. Returns the rows that each lookup read, without the rows of the holes that a
     * compaction of the holes filled before it.
     */
    private static long[] lookupSequence(
            HorizonJoinTimeFrameHelper helper,
            SlaveCursor cursor,
            Map map,
            LongList positions,
            IntList keys,
            MissingSymbolRecord symbolRecord,
            Rnd rnd
    ) {
        helper.of(cursor);
        map.clear();
        final MasterRecord masterRecord = new MasterRecord();
        symbolRecord.of(masterRecord);
        final long[] scannedRows = new long[positions.size()];
        long prevPosition = -1;
        for (int i = 0, n = positions.size(); i < n; i++) {
            final long position = positions.getQuick(i);
            if (position == TO_TOP) {
                helper.toTop();
                map.clear();
                prevPosition = -1;
                continue;
            }
            final long asOfRowId;
            if (position >= prevPosition) {
                asOfRowId = helper.findAsOfRow(position);
                Assert.assertEquals("ASOF position of lookup " + i, cursor.rowIdOf(position), asOfRowId);
                prevPosition = position;
            } else {
                asOfRowId = cursor.rowIdOf(position);
            }
            masterRecord.key = keys.getQuick(i);
            final long keyReadCount = cursor.getKeyReadCount();
            final long filledRowCount = helper.getFilledRowCount();
            final long matchRowId = helper.findKeyedAsOfMatch(asOfRowId, masterRecord, MASTER_KEY_SINK, SLAVE_KEY_SINK, map, symbolRecord);
            scannedRows[i] = cursor.getKeyReadCount() - keyReadCount - (helper.getFilledRowCount() - filledRowCount);
            final long expectedRowId = masterRecord.key >= symbolRecord.missingKeyLo ? Long.MIN_VALUE : cursor.findMatch(position, masterRecord.key);
            if (expectedRowId != matchRowId) {
                Assert.fail("match of lookup " + i + " [position=" + position + ", key=" + masterRecord.key
                        + ", expected=" + expectedRowId + ", actual=" + matchRowId + ']');
            }
            if (matchRowId != Long.MIN_VALUE) {
                helper.recordAt(matchRowId);
            }
            if (rnd.nextBoolean()) {
                cursor.jumpTo(rnd.nextInt(cursor.frameRowCounts.length));
            }
        }
        return scannedRows;
    }

    /**
     * Runs the lookups as {@link #lookup} does, but goes on from the state that the helper and the
     * map are in, as the lookups of a master page frame after toTop() do.
     */
    private static long[] lookupWithoutReset(
            HorizonJoinTimeFrameHelper helper,
            SlaveCursor cursor,
            Map map,
            LongList positions,
            IntList keys,
            @Nullable SymbolTranslatingRecord symbolTranslatingRecord
    ) {
        final MasterRecord masterRecord = new MasterRecord();
        if (symbolTranslatingRecord != null) {
            symbolTranslatingRecord.of(masterRecord);
        }
        final long[] scannedRows = new long[positions.size()];
        for (int i = 0, n = positions.size(); i < n; i++) {
            final long position = positions.getQuick(i);
            final long asOfRowId = helper.findAsOfRow(position);
            Assert.assertEquals("ASOF position of lookup " + i, cursor.rowIdOf(position), asOfRowId);

            masterRecord.key = keys.getQuick(i);
            final long keyReadCount = cursor.getKeyReadCount();
            final long matchRowId = helper.findKeyedAsOfMatch(
                    asOfRowId,
                    masterRecord,
                    MASTER_KEY_SINK,
                    SLAVE_KEY_SINK,
                    map,
                    symbolTranslatingRecord
            );
            scannedRows[i] = cursor.getKeyReadCount() - keyReadCount;
            Assert.assertEquals(
                    "match of lookup " + i + " [position=" + position + ", key=" + masterRecord.key + ']',
                    cursor.findMatch(position, masterRecord.key),
                    matchRowId
            );
        }
        return scannedRows;
    }

    private static HorizonJoinTimeFrameHelper newAdaptiveHelper() {
        return newAdaptiveHelper(BWD_SCAN_MIN_GAP);
    }

    private static HorizonJoinTimeFrameHelper newAdaptiveHelper(long minGap) {
        return new HorizonJoinTimeFrameHelper(
                configuration.getSqlAsOfJoinLookAhead(),
                1,
                BWD_SCAN_ABSOLUTE_THRESHOLD,
                minGap,
                BWD_SCAN_SWITCH_FACTOR
        );
    }

    /**
     * Neither the gap checks nor the absolute threshold can pass, so the helper scans backward at
     * every position.
     */
    private static HorizonJoinTimeFrameHelper newBackwardOnlyHelper() {
        return new HorizonJoinTimeFrameHelper(
                configuration.getSqlAsOfJoinLookAhead(),
                1,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                BWD_SCAN_SWITCH_FACTOR
        );
    }

    /**
     * Every backward scan cost passes an absolute threshold of -1, so the helper keeps the key map
     * from the second position on.
     */
    private static HorizonJoinTimeFrameHelper newKeptKeyMapHelper() {
        return new HorizonJoinTimeFrameHelper(
                configuration.getSqlAsOfJoinLookAhead(),
                1,
                -1,
                BWD_SCAN_MIN_GAP,
                BWD_SCAN_SWITCH_FACTOR
        );
    }

    /**
     * {@link #newKeptKeyMapHelper()} with at most the given number of holes.
     */
    private static HorizonJoinTimeFrameHelper newKeptKeyMapHelper(int maxHoleCount) {
        return new HorizonJoinTimeFrameHelper(
                configuration.getSqlAsOfJoinLookAhead(),
                1,
                -1,
                BWD_SCAN_MIN_GAP,
                BWD_SCAN_SWITCH_FACTOR,
                maxHoleCount
        );
    }

    private static Map newMap() {
        // The INT key selects the same map implementation as a SYMBOL join key.
        return MapFactory.createUnorderedMap(
                configuration,
                new SingleColumnType(ColumnType.INT),
                new SingleColumnType(ColumnType.LONG)
        );
    }

    /**
     * Returns the indexes of the frames that the cursor tells are Parquet, for messages.
     */
    private static IntList parquetFrameList(SlaveCursor cursor) {
        final IntList frameIndexes = new IntList();
        for (int f = 0, n = cursor.frameRowCounts.length; f < n; f++) {
            if (cursor.isParquetFrame(f)) {
                frameIndexes.add(f);
            }
        }
        return frameIndexes;
    }

    /**
     * Fills the lookups of the recurring cold key: even rows hold a hot key and odd rows hold
     * 70,000 cold keys in turn. The first lookup looks up the cold key whose last row before the
     * first position lies furthest back, in row 11. Hot lookups 100 rows apart follow, and every
     * 300th position also looks up the cold key whose last row lies 19,999 rows back.
     */
    private static SlaveCursor recurringColdKeyLookups(LongList positions, IntList keys) {
        final int coldKeyCount = 70_000;
        final int hotKey = 1;
        final long gap = 100;
        final int hotLookupCount = 9_000;
        final long firstPosition = 2L * coldKeyCount + 10;
        add(positions, keys, firstPosition, 2 + (11 >> 1));
        for (int i = 1; i <= hotLookupCount; i++) {
            final long position = firstPosition + i * gap;
            add(positions, keys, position, hotKey);
            if (i % 300 == 0) {
                // Positions are even, so the odd row 19,999 rows back holds the cold key.
                add(positions, keys, position, 2 + (int) (((position - 19_999) >> 1) % coldKeyCount));
            }
        }
        return new SlaveCursor(
                frameRowCounts(firstPosition + hotLookupCount * gap + 1, 1_000_000),
                row -> (row & 1) == 0 ? hotKey : 2 + (int) ((row >> 1) % coldKeyCount)
        );
    }

    private static long sum(long[] values) {
        return sum(values, 0, values.length);
    }

    private static long sum(long[] values, int lo, int hi) {
        long sum = 0;
        for (int i = lo; i < hi; i++) {
            sum += values[i];
        }
        return sum;
    }

    @FunctionalInterface
    private interface KeyFunction {
        int keyAt(long row);
    }

    /**
     * The map of a SYMBOL or INT join key. It counts its clears, its shrinks to the initial
     * capacity and its regrows to a given capacity.
     */
    private static final class ClearCountingMap extends Unordered4Map {
        private int clearCount;
        private int regrowCount;
        private int shrinkCount;

        private ClearCountingMap() {
            super(
                    ColumnType.INT,
                    new SingleColumnType(ColumnType.LONG),
                    configuration.getSqlSmallMapKeyCapacity(),
                    configuration.getSqlFastMapLoadFactor(),
                    Integer.MAX_VALUE
            );
        }

        @Override
        public void clear() {
            clearCount++;
            super.clear();
        }

        @Override
        public void restoreInitialCapacity() {
            shrinkCount++;
            super.restoreInitialCapacity();
        }

        @Override
        public void setKeyCapacity(int keyCapacity) {
            regrowCount++;
            super.setKeyCapacity(keyCapacity);
        }
    }

    /**
     * The map of a join key of several columns. It counts its clears.
     */
    private static final class ClearCountingOrderedMap extends OrderedMap {
        private int clearCount;

        private ClearCountingOrderedMap() {
            super(
                    configuration.getSqlSmallMapPageSize(),
                    new SingleColumnType(ColumnType.INT),
                    new SingleColumnType(ColumnType.LONG),
                    configuration.getSqlSmallMapKeyCapacity(),
                    configuration.getSqlFastMapLoadFactor(),
                    Integer.MAX_VALUE
            );
        }

        @Override
        public void clear() {
            clearCount++;
            super.clear();
        }
    }

    private static final class KeySink implements RecordSink {
        private final int columnIndex;

        private KeySink(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void copy(Record r, RecordSinkSPI w) {
            w.putInt(r.getInt(columnIndex));
        }

        @Override
        public void setFunctions(ObjList<Function> keyFunctions) {
        }
    }

    private static final class MasterRecord implements Record {
        private static final int KEY_COLUMN = 0;
        private int key;

        @Override
        public int getInt(int col) {
            Assert.assertEquals(KEY_COLUMN, col);
            return key;
        }
    }

    /**
     * Tells that the slave's symbol table lacks every master key from {@code missingKeyLo} up, so
     * the helper answers their lookups without a scan, as it does for a real missing symbol. It
     * counts how often the helper asks.
     */
    private static final class MissingSymbolRecord extends SymbolTranslatingRecord {
        private int missingKeyLo;
        private int nonExistentKeyCheckCount;

        private MissingSymbolRecord(int missingKeyLo) {
            super(configuration, 1, 1);
            this.missingKeyLo = missingKeyLo;
        }

        @Override
        public boolean hasNonExistentKey() {
            nonExistentKeyCheckCount++;
            return base.getInt(MasterRecord.KEY_COLUMN) >= missingKeyLo;
        }
    }

    /**
     * A slave of consecutive frames, one row per timestamp unit: the row with number {@code n}
     * across all frames carries timestamp {@code n} and the key {@code keyFunction.keyAt(n)}.
     */
    private static final class SlaveCursor implements TimeFrameCursor {
        private final long[] frameRowCounts;
        // Number of the first row of every frame; the last element holds the total row count.
        private final long[] frameRowOffsets;
        private final KeyFunction keyFunction;
        private final SlaveRecord record = new SlaveRecord(this);
        private final TimeFrame timeFrame = new TimeFrame();
        // Whether the reads of the keys track their order, from the last resetReadOrder() on.
        private boolean isReadOrderTracked;
        private long keyReadCount;
        private long lastReadRow;
        // The lowest rows read in native frames, in Parquet frames and in all frames.
        private long minNativeReadRow;
        private long minParquetReadRow;
        private long minReadRow;
        private long openCount;
        // The frames that isParquetFrame() tells are Parquet.
        private IntPredicate parquetFrames = NATIVE_FRAMES;
        // Reads of a key of a row above the row read before it.
        private int readAscentCount;
        // Reads of the key of every row when not null.
        private int[] rowReadCounts;

        private SlaveCursor(long[] frameRowCounts, KeyFunction keyFunction) {
            this.frameRowCounts = frameRowCounts;
            this.frameRowOffsets = new long[frameRowCounts.length + 1];
            for (int f = 0; f < frameRowCounts.length; f++) {
                frameRowOffsets[f + 1] = frameRowOffsets[f] + frameRowCounts[f];
            }
            this.keyFunction = keyFunction;
            toTop();
        }

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TimeFrame getTimeFrame() {
            return timeFrame;
        }

        @Override
        public int getTimestampIndex() {
            return SlaveRecord.TIMESTAMP_COLUMN;
        }

        @Override
        public boolean isParquetFrame(int frameIndex) {
            Assert.assertTrue("frame index out of bounds: " + frameIndex, frameIndex >= 0 && frameIndex < frameRowCounts.length);
            return parquetFrames.test(frameIndex);
        }

        @Override
        public void jumpTo(int frameIndex) {
            Assert.assertTrue("frame index out of bounds: " + frameIndex, frameIndex >= 0 && frameIndex < frameRowCounts.length);
            timeFrame.ofEstimate(frameIndex, frameRowOffsets[frameIndex], frameRowOffsets[frameIndex + 1]);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean next() {
            final int frameIndex = timeFrame.getFrameIndex() + 1;
            if (frameIndex < frameRowCounts.length) {
                timeFrame.ofEstimate(frameIndex, frameRowOffsets[frameIndex], frameRowOffsets[frameIndex + 1]);
                return true;
            }
            timeFrame.ofEstimate(frameRowCounts.length, Long.MIN_VALUE, Long.MIN_VALUE);
            return false;
        }

        @Override
        public long open() {
            openCount++;
            final int frameIndex = timeFrame.getFrameIndex();
            Assert.assertTrue("open call on uninitialized time frame", frameIndex >= 0 && frameIndex < frameRowCounts.length);
            final long rowCount = frameRowCounts[frameIndex];
            if (rowCount > 0) {
                timeFrame.ofOpen(frameRowOffsets[frameIndex], frameRowOffsets[frameIndex + 1], 0, rowCount);
                return rowCount;
            }
            timeFrame.ofOpen(timeFrame.getTimestampEstimateLo(), timeFrame.getTimestampEstimateHi(), 0, 0);
            return 0;
        }

        @Override
        public boolean prev() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void recordAt(Record record, long rowId) {
            recordAt(record, Rows.toPartitionIndex(rowId), Rows.toLocalRowID(rowId));
        }

        @Override
        public void recordAt(Record record, int frameIndex, long rowIndex) {
            final SlaveRecord slaveRecord = (SlaveRecord) record;
            slaveRecord.frameIndex = frameIndex;
            slaveRecord.rowIndex = rowIndex;
        }

        @Override
        public void recordAtRowIndex(Record record, long rowIndex) {
            ((SlaveRecord) record).rowIndex = rowIndex;
        }

        @Override
        public void seekEstimate(long timestamp) {
            // Last frame whose ceiling is at or before the timestamp, as in the table-backed cursors.
            int result = -1;
            for (int f = 0; f < frameRowCounts.length; f++) {
                if (frameRowOffsets[f + 1] <= timestamp) {
                    result = f;
                }
            }
            if (result >= 0) {
                timeFrame.ofEstimate(result, frameRowOffsets[result], frameRowOffsets[result + 1]);
            } else {
                timeFrame.ofEstimate(-1, Long.MIN_VALUE, Long.MIN_VALUE);
            }
        }

        @Override
        public void toTop() {
            timeFrame.clear();
        }

        /**
         * Brute-force keyed ASOF match: the last row at or before the position that holds the key.
         */
        private long findMatch(long position, int key) {
            for (long row = position; row >= 0; row--) {
                if (keyFunction.keyAt(row) == key) {
                    return rowIdOf(row);
                }
            }
            return Long.MIN_VALUE;
        }

        private long getKeyReadCount() {
            return keyReadCount;
        }

        private long getOpenCount() {
            return openCount;
        }

        private void resetReadOrder() {
            isReadOrderTracked = true;
            lastReadRow = Long.MAX_VALUE;
            minNativeReadRow = Long.MAX_VALUE;
            minParquetReadRow = Long.MAX_VALUE;
            minReadRow = Long.MAX_VALUE;
            readAscentCount = 0;
        }

        private long rowIdOf(long row) {
            int frameIndex = 0;
            while (row >= frameRowOffsets[frameIndex + 1]) {
                frameIndex++;
            }
            return Rows.toRowID(frameIndex, row - frameRowOffsets[frameIndex]);
        }
    }

    private static final class SlaveRecord implements Record {
        private static final int KEY_COLUMN = 1;
        private static final int TIMESTAMP_COLUMN = 0;
        private final SlaveCursor cursor;
        private int frameIndex;
        private long rowIndex;

        private SlaveRecord(SlaveCursor cursor) {
            this.cursor = cursor;
        }

        @Override
        public int getInt(int col) {
            Assert.assertEquals(KEY_COLUMN, col);
            cursor.keyReadCount++;
            final long row = row();
            if (cursor.rowReadCounts != null) {
                cursor.rowReadCounts[(int) row]++;
            }
            if (cursor.isReadOrderTracked) {
                if (row > cursor.lastReadRow) {
                    cursor.readAscentCount++;
                }
                cursor.lastReadRow = row;
                cursor.minReadRow = Math.min(cursor.minReadRow, row);
                if (cursor.parquetFrames.test(frameIndex)) {
                    cursor.minParquetReadRow = Math.min(cursor.minParquetReadRow, row);
                } else {
                    cursor.minNativeReadRow = Math.min(cursor.minNativeReadRow, row);
                }
            }
            return cursor.keyFunction.keyAt(row);
        }

        @Override
        public long getTimestamp(int col) {
            Assert.assertEquals(TIMESTAMP_COLUMN, col);
            return row();
        }

        private long row() {
            Assert.assertTrue(
                    "row out of frame [frameIndex=" + frameIndex + ", rowIndex=" + rowIndex + ']',
                    rowIndex >= 0 && rowIndex < cursor.frameRowCounts[frameIndex]
            );
            return cursor.frameRowOffsets[frameIndex] + rowIndex;
        }
    }

    /**
     * Replays a lookup sequence as backward-only mode reads it, counts the rows as its backward
     * scans count them, and applies the switch rules of {@link HorizonJoinTimeFrameHelper} to that
     * count. A backward scan reads from the position down to the key and puts every key it reads
     * into the key map. A later lookup at the same position whose key the map lacks goes on from
     * the lowest row read there, which it reads once more, unless the scans have read the first
     * row of the slave. With rereads off, the count leaves out the rows read once more.
     */
    private static final class SwitchModel {
        private final long absoluteThreshold;
        private final boolean isRereadCounted;
        private final long minGap;
        private final long switchFactor;
        private long rows;
        // The rows counted up to every lookup of the last replay.
        private long[] rowsAfterLookups;
        private long rowsAtPositionStart;
        private long rowsAtWindowStart;
        private long windowGap;

        private SwitchModel(long absoluteThreshold, long minGap, long switchFactor, boolean isRereadCounted) {
            this.absoluteThreshold = absoluteThreshold;
            this.minGap = minGap;
            this.switchFactor = switchFactor;
            this.isRereadCounted = isRereadCounted;
        }

        /**
         * Returns the rows that backward-only mode reads at the first position of a lookup sequence.
         */
        private static long countFirstPositionRows(
                SlaveCursor cursor,
                LongList positions,
                IntList keys,
                int missingKeyLo,
                boolean isRereadCounted
        ) {
            final SwitchModel model = new SwitchModel(Long.MAX_VALUE, Long.MAX_VALUE, 1, isRereadCounted);
            model.replay(cursor, positions, keys, missingKeyLo);
            int last = 0;
            while (last + 1 < positions.size() && positions.getQuick(last + 1) == positions.getQuick(0)) {
                last++;
            }
            return model.rowsAfterLookups[last];
        }

        private boolean isWindowSwitch(long gap) {
            boolean isSwitch = false;
            boolean isWindowOpen = gap > 0 && gap <= minGap;
            if (isWindowOpen) {
                windowGap += gap;
                if (windowGap > minGap) {
                    isSwitch = HorizonJoinTimeFrameHelper.shouldSwitchToForwardScan(
                            rows - rowsAtWindowStart,
                            windowGap,
                            minGap,
                            switchFactor,
                            Long.MAX_VALUE
                    );
                    isWindowOpen = false;
                }
            }
            if (!isWindowOpen) {
                windowGap = 0;
                rowsAtWindowStart = rows;
            }
            return isSwitch;
        }

        /**
         * Returns, for every lookup, whether the helper keeps the key map at it.
         */
        private boolean[] replay(SlaveCursor cursor, LongList positions, IntList keys, int missingKeyLo) {
            final boolean[] isKeyMapKept = new boolean[positions.size()];
            rowsAfterLookups = new long[positions.size()];
            final IntHashSet keysRead = new IntHashSet();
            boolean isKept = false;
            long prevAsOfRowId = Long.MIN_VALUE;
            // The lowest row read at the position, -1 before the first read, and whether the reads
            // there have reached the first row of the slave.
            long lowestRow = -1;
            boolean isFirstRowRead = false;
            rows = 0;
            rowsAtPositionStart = 0;
            rowsAtWindowStart = 0;
            windowGap = 0;
            for (int i = 0, n = positions.size(); i < n; i++) {
                final long position = positions.getQuick(i);
                if (position == TO_TOP) {
                    isKept = false;
                    prevAsOfRowId = Long.MIN_VALUE;
                    rows = 0;
                    rowsAtPositionStart = 0;
                    rowsAtWindowStart = 0;
                    windowGap = 0;
                    continue;
                }
                final long asOfRowId = cursor.rowIdOf(position);
                if (asOfRowId != prevAsOfRowId) {
                    if (!isKept && prevAsOfRowId != Long.MIN_VALUE) {
                        final long gap = asOfRowId - prevAsOfRowId;
                        isKept = HorizonJoinTimeFrameHelper.shouldSwitchToForwardScan(
                                rows - rowsAtPositionStart,
                                gap,
                                minGap,
                                switchFactor,
                                absoluteThreshold
                        ) || isWindowSwitch(gap);
                    }
                    if (!isKept) {
                        keysRead.clear();
                        lowestRow = -1;
                        isFirstRowRead = false;
                        rowsAtPositionStart = rows;
                    }
                    prevAsOfRowId = asOfRowId;
                }
                isKeyMapKept[i] = isKept;
                rowsAfterLookups[i] = rows;
                final int key = keys.getQuick(i);
                if (isKept || key >= missingKeyLo) {
                    continue;
                }
                long row = position;
                if (lowestRow >= 0) {
                    if (keysRead.contains(key) || isFirstRowRead) {
                        continue;
                    }
                    row = lowestRow;
                    if (!isRereadCounted) {
                        row--;
                    }
                }
                for (; row >= 0; row--) {
                    rows++;
                    lowestRow = row;
                    final int rowKey = cursor.keyFunction.keyAt(row);
                    keysRead.add(rowKey);
                    if (rowKey == key) {
                        break;
                    }
                }
                isFirstRowRead = lowestRow == 0 && (row < 0 || cursor.rowIdOf(0) == 0);
                rowsAfterLookups[i] = rows;
            }
            return isKeyMapKept;
        }
    }
}
