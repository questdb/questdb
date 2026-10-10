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
import io.questdb.griffin.engine.table.HorizonJoinMatcher;
import io.questdb.griffin.engine.table.HorizonJoinSlaveState;
import io.questdb.griffin.engine.table.HorizonJoinTimeFrameHelper;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Rows;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Drives the keyed ASOF lookup of {@link HorizonJoinTimeFrameHelper} over a mock slave and counts
 * the slave rows it reads: every row a lookup reads, backward or forward, costs exactly one read of
 * the slave key column. A test-only accessor tells whether the lookup has left backward-only mode.
 * <p>
 * The slave holds one row per timestamp unit, so the ASOF position of a horizon timestamp is the
 * row with the same number.
 */
public class HorizonJoinTimeFrameHelperTest extends AbstractCairoTest {
    // Production defaults of the cairo.sql.horizon.join.bwd.scan.* properties.
    private static final long BWD_SCAN_ABSOLUTE_THRESHOLD = 131_072;
    private static final long BWD_SCAN_MIN_GAP = 1_024;
    private static final long BWD_SCAN_SWITCH_FACTOR = 8;
    private static final int COMMON_KEY = 2;
    private static final Log LOG = LogFactory.getLog(HorizonJoinTimeFrameHelperTest.class);
    private static final RecordSink MASTER_KEY_SINK = new KeySink(MasterRecord.KEY_COLUMN);
    private static final int RARE_KEY = 1;
    private static final RecordSink SLAVE_KEY_SINK = new KeySink(SlaveRecord.KEY_COLUMN);

    @Test
    public void testAggregatingConstructorKeepsWindowSwitchOff() throws Exception {
        // The aggregating HORIZON JOIN factories build their helpers with the constructor that
        // takes no window switch flag. Over the fixture of
        // testBurstThenSparseRestartsForwardScanWithWindowSwitch(), only the window check can
        // switch, so such a helper stays in backward-only mode after every lookup and reads the
        // rows that a helper with the window switch off reads.
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(new long[]{2_000_000}, row -> (int) (row % 1_000));
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            addBurstThenSparse(positions, keys);
            try (Map map = newMap()) {
                final HorizonJoinTimeFrameHelper aggregating = new HorizonJoinTimeFrameHelper(
                        configuration.getSqlAsOfJoinLookAhead(),
                        1,
                        BWD_SCAN_ABSOLUTE_THRESHOLD,
                        BWD_SCAN_MIN_GAP,
                        BWD_SCAN_SWITCH_FACTOR
                );
                Assert.assertEquals(-1, firstForwardScanLookup(aggregating, cursor, map, positions, keys));
                Assert.assertArrayEquals(
                        lookup(newHelper(false), cursor, map, positions, keys),
                        lookup(aggregating, cursor, map, positions, keys)
                );
            }
        });
    }

    @Test
    public void testBurstThenSparseRestartsForwardScanWithWindowSwitch() throws Exception {
        // A burst of 60 lookups 20 rows apart, then 18 lookups 100,000 rows apart, all for key 7,
        // which every 1,000th slave row holds. Each lookup scans back fewer than 1,000 rows. The
        // burst's gaps sit below the min gap, so only the window check can switch: with it, the
        // lookup switches in the burst. A gap of the sparse stretch is then worth far more rows
        // than the key cache, so the lookup restarts the forward scan at every sparse position
        // and reads the short backward scan that it reads without the window check, as in the
        // aggregating factories, instead of every row of the stretch.
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(new long[]{2_000_000}, row -> (int) (row % 1_000));
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            final int burstCount = addBurstThenSparse(positions, keys);
            try (Map map = newMap()) {
                final HorizonJoinTimeFrameHelper aggregating = newHelper(false);
                final long[] aggregatingRows = lookup(aggregating, cursor, map, positions, keys);
                Assert.assertFalse(aggregating.isForwardScanMode());
                for (int i = 0, n = positions.size(); i < n; i++) {
                    Assert.assertTrue("rows of lookup " + i + ": " + aggregatingRows[i], aggregatingRows[i] <= 1_000);
                }

                final HorizonJoinTimeFrameHelper projection = newHelper(true);
                final long[] projectionRows = lookup(projection, cursor, map, positions, keys);
                Assert.assertTrue(projection.isForwardScanMode());
                for (int i = burstCount, n = positions.size(); i < n; i++) {
                    Assert.assertEquals("rows of lookup " + i, aggregatingRows[i], projectionRows[i]);
                }
            }
        });
    }

    @Test
    public void testForwardScanBoundOverflowScansGapForward() throws Exception {
        // An operator may set the switch factor to Long.MAX_VALUE to turn the relative checks off.
        // The rare lookup at row 150,000 reads 150,001 rows, above the absolute threshold, so the
        // lookup switches at the next position, 2,000 rows on. The key cache is then worth the
        // factor times those rows in rows of forward scan, a product that overflows a long: no gap
        // is worth more, so the lookup scans every gap forward and never restarts.
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(new long[]{300_000}, row -> row == 0 ? RARE_KEY : COMMON_KEY);
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            positions.add(150_000);
            keys.add(RARE_KEY);
            positions.add(152_000);
            keys.add(COMMON_KEY);
            positions.add(154_000);
            keys.add(COMMON_KEY);
            try (Map map = newMap()) {
                final HorizonJoinTimeFrameHelper projection = new HorizonJoinTimeFrameHelper(
                        configuration.getSqlAsOfJoinLookAhead(),
                        1,
                        BWD_SCAN_ABSOLUTE_THRESHOLD,
                        BWD_SCAN_MIN_GAP,
                        Long.MAX_VALUE,
                        true
                );
                final long[] projectionRows = lookup(projection, cursor, map, positions, keys);
                Assert.assertTrue(projection.isForwardScanMode());
                Assert.assertArrayEquals(new long[]{150_001, 2_000, 2_000}, projectionRows);
            }
        });
    }

    @Test
    public void testForwardScanGapCountsRowsAcrossFrames() throws Exception {
        // The slave cycles through 30 keys over frames of 101,100, 0, 600, 300, 0, 700, 1,000, 300,
        // 0 and 60,000 rows. 600 lookups 2 rows apart from row 100,000 on each find their key 20
        // rows back, so the window check switches at lookup 513 and every lookup from there reads
        // its 2-row gap forward, lookup 550 from the first frame over the empty one into the third.
        // Row ids of different frames lie 2^44 apart, so only a count of the rows in between tells
        // such a gap from a long one. Lookup 600 lies 1,024 rows on, past a whole frame and an
        // empty one: the min gap allows that many rows, so the lookup scans them forward. Lookup
        // 601 lies 1,025 rows on, in the next frame, and restarts the forward scan with its 20-row
        // backward scan. Lookup 602 lies 1,025 rows on too, past a whole frame of 300 rows and an
        // empty one, and restarts again: it counts every row of the frame in between.
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(
                    new long[]{101_100, 0, 600, 300, 0, 700, 1_000, 300, 0, 60_000},
                    row -> (int) (row % 30)
            );
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            addDenseRun(positions, keys, 100_000, 600);
            addDenseRun(positions, keys, positions.getLast() + 1_024, 1);
            addDenseRun(positions, keys, positions.getLast() + 1_025, 1);
            addDenseRun(positions, keys, positions.getLast() + 1_025, 1);
            try (Map map = newMap()) {
                final HorizonJoinTimeFrameHelper projection = newHelper(true);
                final long[] projectionRows = lookup(projection, cursor, map, positions, keys);
                Assert.assertTrue(projection.isForwardScanMode());
                for (int i = 0; i < 600; i++) {
                    Assert.assertEquals("rows of lookup " + i, i < 513 ? 20 : 2, projectionRows[i]);
                }
                Assert.assertEquals(1_024, projectionRows[600]);
                Assert.assertEquals(20, projectionRows[601]);
                Assert.assertEquals(20, projectionRows[602]);
            }
        });
    }

    @Test
    public void testForwardScanRestartTrustsKeptKeyCacheEntriesOnceConfirmed() throws Exception {
        // The slave cycles through 10 keys. The first lookup asks for a key that the slave never
        // holds and reads its 140,001 rows into the key cache, above the absolute threshold, so
        // the next position, 1,200,000 rows on, switches. The cache is worth 8 times those rows,
        // fewer than the gap, so the forward scan restarts there. A cache of 16 slots is full
        // enough that the restart clears it. A cache of 2,048 slots is not, and the restart keeps
        // its 10 entries, each with its key's last row up to the first position. Either way the
        // two lookups at the restart position find their key's latest row and read the same
        // rows. The first reads the row at the position and overwrites the entry of that row's
        // key. The second asks for the key 5 rows back: a kept entry lies below the backward
        // watermark, so the lookup scans on from there to the key's latest row, as it does over
        // the cleared cache. The large caches run on two of the maps that the matcher can pick
        // for a key cache: Unordered4Map, for an INT or SYMBOL key, and OrderedMap, for a key of
        // several columns.
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(new long[]{1_500_000}, row -> (int) (row % 10));
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            positions.add(140_000);
            keys.add(-1);
            positions.add(1_340_000);
            keys.add(0);
            positions.add(1_340_000);
            keys.add(5);
            final SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);
            final double loadFactor = configuration.getSqlFastMapLoadFactor();
            final int maxResizes = configuration.getSqlMapMaxResizes();
            try (
                    Map smallMap = new Unordered4Map(ColumnType.INT, valueTypes, 8, loadFactor, maxResizes);
                    Map largeMap = new Unordered4Map(ColumnType.INT, valueTypes, 1_024, loadFactor, maxResizes);
                    Map largeOrderedMap = new OrderedMap(
                            configuration.getSqlSmallMapPageSize(),
                            new SingleColumnType(ColumnType.INT),
                            valueTypes,
                            1_024,
                            loadFactor,
                            maxResizes
                    )
            ) {
                final ObjList<Map> maps = new ObjList<>();
                maps.add(smallMap);
                maps.add(largeMap);
                maps.add(largeOrderedMap);
                for (int i = 0, n = maps.size(); i < n; i++) {
                    final Map map = maps.getQuick(i);
                    final HorizonJoinTimeFrameHelper projection = newHelper(true);
                    final long[] projectionRows = lookup(projection, cursor, map, positions, keys);
                    Assert.assertTrue(projection.isForwardScanMode());
                    Assert.assertArrayEquals(new long[]{140_001, 1, 6}, projectionRows);
                    // The lookups after the restart put 6 keys into the cache; a kept cache still
                    // holds the other 4 from before it.
                    Assert.assertEquals("map " + i, map == smallMap ? 6 : 10, map.size());
                }
            }
        });
    }

    @Test
    public void testForwardScanRestartsAboveKeyCacheWorth() throws Exception {
        // The first nine lookups of testRelativeSwitchWithoutWindowSwitch() switch both helpers at
        // lookup 8, after the rare lookup at row 12,000 read 12,001 rows into the key cache. With
        // the window switch, the cache is worth 8 times those rows of forward scan: lookup 9 lies
        // 96,008 rows on and scans them forward, lookup 10 lies 96,009 rows on and restarts the
        // forward scan with a 1-row backward scan for the common key. That scan is all the new
        // cache holds, so the min gap bounds the next forward scan: lookup 11 reads its 1,024-row
        // gap forward and lookup 12, 1,025 rows on, restarts again. Without the window switch, as
        // in the aggregating factories, the lookup scans every gap forward.
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(new long[]{300_000}, row -> row == 0 ? RARE_KEY : COMMON_KEY);
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            addRareKeyRun(positions, keys, 1_500, 9);
            for (long gap : new long[]{96_008, 96_009, 1_024, 1_025}) {
                positions.add(positions.getLast() + gap);
                keys.add(COMMON_KEY);
            }
            try (Map map = newMap()) {
                final HorizonJoinTimeFrameHelper aggregating = newHelper(false);
                Assert.assertEquals(8, firstForwardScanLookup(aggregating, cursor, map, positions, keys));
                final long[] aggregatingRows = lookup(aggregating, cursor, map, positions, keys);
                Assert.assertArrayEquals(
                        new long[]{1_500, 96_008, 96_009, 1_024, 1_025},
                        Arrays.copyOfRange(aggregatingRows, 8, 13)
                );

                final HorizonJoinTimeFrameHelper projection = newHelper(true);
                Assert.assertEquals(8, firstForwardScanLookup(projection, cursor, map, positions, keys));
                final long[] projectionRows = lookup(projection, cursor, map, positions, keys);
                Assert.assertArrayEquals(
                        new long[]{1_500, 96_008, 1, 1_024, 1},
                        Arrays.copyOfRange(projectionRows, 8, 13)
                );
                Assert.assertArrayEquals(Arrays.copyOf(aggregatingRows, 8), Arrays.copyOf(projectionRows, 8));
            }
        });
    }

    @Test
    public void testForwardScanRestartsDoNotClearGrownKeyCache() throws Exception {
        // Rows below 90,000 hold a key each, and the slave cycles through 1,000 keys from there
        // on. A first batch looks up a key that the slave never holds: the backward scan puts the
        // 90,000 keys into the key cache, which grows to fit them. toTop() and a clear keep that
        // capacity, and a clear writes every slot of it. The next batch is the burst and sparse
        // stretch of testBurstThenSparseRestartsForwardScanWithWindowSwitch(): the burst switches,
        // and every sparse lookup restarts the forward scan and reads the rows that it reads over
        // a cache that never grew. Those restarts must not pay for the grown cache: all of them
        // together clear fewer slots than one clear of it.
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(
                    new long[]{2_000_000},
                    row -> row < 90_000 ? 1_000 + (int) row : (int) (row % 1_000)
            );
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            final int burstCount = addBurstThenSparse(positions, keys);
            final LongList burstPositions = new LongList();
            final IntList burstKeys = new IntList();
            final LongList sparsePositions = new LongList();
            final IntList sparseKeys = new IntList();
            for (int i = 0, n = positions.size(); i < n; i++) {
                (i < burstCount ? burstPositions : sparsePositions).add(positions.getQuick(i));
                (i < burstCount ? burstKeys : sparseKeys).add(keys.getQuick(i));
            }
            final long[] smallCacheRows;
            try (Map map = newMap()) {
                smallCacheRows = lookup(newHelper(true), cursor, map, positions, keys);
            }

            try (ClearCountingMap map = new ClearCountingMap()) {
                final HorizonJoinTimeFrameHelper helper = newHelper(true);
                final LongList absentKeyPositions = new LongList();
                absentKeyPositions.add(89_999);
                final IntList absentKeys = new IntList();
                absentKeys.add(-1);
                lookup(helper, cursor, map, absentKeyPositions, absentKeys);
                final int grownCapacity = map.getKeyCapacity();
                Assert.assertTrue("capacity " + grownCapacity, grownCapacity > 90_000);

                helper.toTop();
                map.clear();
                lookupWithoutReset(helper, cursor, map, burstPositions, burstKeys);
                Assert.assertTrue(helper.isForwardScanMode());
                map.clearedSlots = 0;
                final long[] sparseRows = lookupWithoutReset(helper, cursor, map, sparsePositions, sparseKeys);
                Assert.assertArrayEquals(Arrays.copyOfRange(smallCacheRows, burstCount, positions.size()), sparseRows);
                Assert.assertTrue(
                        "slots that the restarts cleared: " + map.clearedSlots + ", capacity " + grownCapacity,
                        map.clearedSlots < grownCapacity
                );
            }
        });
    }

    @Test
    public void testFuzzMatchesWithAndWithoutWindowSwitch() throws Exception {
        // Random frames, some of them empty, random keys, random runs of small and large gaps with
        // several lookups at some positions, random thresholds and a toTop() between some runs.
        // Every match must equal a brute-force keyed ASOF lookup, with the window switch on and off.
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (int iteration = 0; iteration < 200; iteration++) {
                final int frameCount = 1 + rnd.nextInt(6);
                final long[] frameRowCounts = new long[frameCount];
                long rowCount = 0;
                for (int f = 0; f < frameCount; f++) {
                    frameRowCounts[f] = rnd.nextInt(8) == 0 ? 0 : 1 + rnd.nextInt(3_000);
                    rowCount += frameRowCounts[f];
                }
                if (rowCount == 0) {
                    continue;
                }
                final int keyCount = 1 + rnd.nextInt(50);
                final int[] rowKeys = new int[(int) rowCount];
                for (int r = 0; r < rowCount; r++) {
                    // Low keys are frequent, high keys rare; key keyCount never appears.
                    rowKeys[r] = rnd.nextInt(4) == 0 ? rnd.nextInt(keyCount) : rnd.nextInt(1 + keyCount / 8);
                }
                final SlaveCursor cursor = new SlaveCursor(frameRowCounts, row -> rowKeys[(int) row]);
                final long[] absoluteThresholds = {0, 100, BWD_SCAN_ABSOLUTE_THRESHOLD, Long.MAX_VALUE};
                final long[] minGaps = {0, 16, BWD_SCAN_MIN_GAP, Long.MAX_VALUE};
                final long absoluteThreshold = absoluteThresholds[rnd.nextInt(absoluteThresholds.length)];
                final long minGap = minGaps[rnd.nextInt(minGaps.length)];
                final long switchFactor = rnd.nextInt(9);
                for (boolean isWindowSwitchEnabled : new boolean[]{false, true}) {
                    final HorizonJoinTimeFrameHelper helper = new HorizonJoinTimeFrameHelper(
                            configuration.getSqlAsOfJoinLookAhead(),
                            1,
                            absoluteThreshold,
                            minGap,
                            switchFactor,
                            isWindowSwitchEnabled
                    );
                    try (Map map = newMap()) {
                        helper.of(cursor);
                        final Rnd runRnd = new Rnd(rnd.getSeed0() + iteration, rnd.getSeed1());
                        final int runCount = 1 + runRnd.nextInt(3);
                        for (int run = 0; run < runCount; run++) {
                            if (run > 0) {
                                helper.toTop();
                                map.clear();
                            }
                            final LongList positions = new LongList();
                            final IntList keys = new IntList();
                            long position = runRnd.nextLong(rowCount);
                            while (position < rowCount && positions.size() < 400) {
                                final int lookupCount = 1 + (runRnd.nextInt(4) == 0 ? runRnd.nextInt(5) : 0);
                                for (int l = 0; l < lookupCount; l++) {
                                    positions.add(position);
                                    keys.add(runRnd.nextInt(keyCount + 1));
                                }
                                position += runRnd.nextInt(3) == 0 ? runRnd.nextInt(2_000) : runRnd.nextInt(40);
                            }
                            lookupWithoutReset(helper, cursor, map, positions, keys);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testMatcherEnablesWindowSwitch() throws Exception {
        // The row-preserving HORIZON JOIN looks up through HorizonJoinMatcher, whose helpers run the
        // window check: over the run of small gaps of testWindowSwitchOverSmallGapsOnlyWhenEnabled()
        // they leave backward-only mode where a helper with the window switch does.
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(new long[]{20_000}, row -> row == 0 ? RARE_KEY : COMMON_KEY);
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            addRareKeyRun(positions, keys, 100, 150);
            final ObjList<HorizonJoinSlaveState> slaveStates = new ObjList<>();
            slaveStates.add(new HorizonJoinSlaveState(null, 1, 1, null, 1, null, null));
            @SuppressWarnings("unchecked") final Class<RecordSink>[] sinkClasses = new Class[1];
            try (
                    HorizonJoinMatcher matcher = new HorizonJoinMatcher(configuration, slaveStates, sinkClasses, sinkClasses);
                    Map map = newMap()
            ) {
                Assert.assertEquals(22, firstForwardScanLookup(matcher.getHelper(0), cursor, map, positions, keys));
                Assert.assertEquals(22, firstForwardScanLookup(newHelper(true), cursor, map, positions, keys));
                Assert.assertEquals(-1, firstForwardScanLookup(newHelper(false), cursor, map, positions, keys));
            }
        });
    }

    @Test
    public void testOffsetSpanIsNotScannedForwardInEveryBatch() throws Exception {
        // Three batches of a row-preserving HORIZON JOIN with two offsets 1,000,000 slave rows
        // apart, over a slave that cycles through 30 keys in frames of 250,000 rows. The horizon
        // timestamps of a batch walk 600 positions 2 rows apart at the small offset, then 600 at
        // the large one, and toTop() precedes every batch, as in the projection cursors. Every
        // lookup finds its key 20 rows back. The window check switches at lookup 513 of a batch,
        // after lookups 0-512 cost 10,260 rows, above 8 x 1,026, and every lookup from there reads
        // its 2-row gap forward. The first lookup at the large offset must not read the rows of
        // the offset span, which toTop() does not bound: it restarts the forward scan with its own
        // 20-row backward scan, and the lookups after it read their 2-row gap forward again.
        assertMemoryLeak(() -> {
            final long[] frameRowCounts = new long[8];
            Arrays.fill(frameRowCounts, 250_000);
            final SlaveCursor cursor = new SlaveCursor(frameRowCounts, row -> (int) (row % 30));
            final ObjList<HorizonJoinSlaveState> slaveStates = new ObjList<>();
            slaveStates.add(new HorizonJoinSlaveState(null, 1, 1, null, 1, null, null));
            @SuppressWarnings("unchecked") final Class<RecordSink>[] sinkClasses = new Class[1];
            try (
                    HorizonJoinMatcher matcher = new HorizonJoinMatcher(configuration, slaveStates, sinkClasses, sinkClasses);
                    Map map = newMap()
            ) {
                // The helpers of HorizonJoinMatcher are the ones that the projection cursors use.
                final ObjList<HorizonJoinTimeFrameHelper> helpers = new ObjList<>();
                helpers.add(newHelper(true));
                helpers.add(matcher.getHelper(0));
                for (int h = 0, helperCount = helpers.size(); h < helperCount; h++) {
                    final HorizonJoinTimeFrameHelper helper = helpers.getQuick(h);
                    helper.of(cursor);
                    for (int batch = 0; batch < 3; batch++) {
                        final LongList positions = new LongList();
                        final IntList keys = new IntList();
                        final long batchPosition = 100_000 + 1_200L * batch;
                        addDenseRun(positions, keys, batchPosition, 600);
                        addDenseRun(positions, keys, batchPosition + 1_000_000, 600);
                        helper.toTop();
                        map.clear();
                        final long[] rows = lookupWithoutReset(helper, cursor, map, positions, keys);
                        Assert.assertTrue(helper.isForwardScanMode());
                        for (int i = 0, n = rows.length; i < n; i++) {
                            Assert.assertEquals(
                                    "rows of lookup " + i + " of batch " + batch,
                                    i < 513 || i == 600 ? 20 : 2,
                                    rows[i]
                            );
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testRelativeSwitchWithoutWindowSwitch() throws Exception {
        // Lookups 1,500 rows apart, above the min gap, alternate between a common key and a key
        // that only row 0 holds. The relative check compares a position's backward scan cost with
        // 8 times its gap: the rare lookup at row 12,000 reads 12,001 rows, so both helpers switch
        // at the next position, with or without the window switch, and read the same rows.
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(new long[]{20_000}, row -> row == 0 ? RARE_KEY : COMMON_KEY);
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            addRareKeyRun(positions, keys, 1_500, 13);
            try (Map map = newMap()) {
                final HorizonJoinTimeFrameHelper aggregating = newHelper(false);
                Assert.assertEquals(8, firstForwardScanLookup(aggregating, cursor, map, positions, keys));
                final HorizonJoinTimeFrameHelper projection = newHelper(true);
                Assert.assertEquals(8, firstForwardScanLookup(projection, cursor, map, positions, keys));
                Assert.assertArrayEquals(
                        lookup(newHelper(false), cursor, map, positions, keys),
                        lookup(newHelper(true), cursor, map, positions, keys)
                );
            }
        });
    }

    @Test
    public void testToTopEndsForwardScanMode() throws Exception {
        // The row-preserving HORIZON JOIN calls toTop() before every batch of horizon timestamps.
        // After the burst of testBurstThenSparseRestartsForwardScanWithWindowSwitch() switches the
        // lookup, toTop() takes it back to backward-only mode, so the sparse stretch of the next
        // batch keeps its short backward scans.
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(new long[]{2_000_000}, row -> (int) (row % 1_000));
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            final int burstCount = addBurstThenSparse(positions, keys);
            final LongList burstPositions = new LongList();
            final IntList burstKeys = new IntList();
            final LongList sparsePositions = new LongList();
            final IntList sparseKeys = new IntList();
            for (int i = 0, n = positions.size(); i < n; i++) {
                (i < burstCount ? burstPositions : sparsePositions).add(positions.getQuick(i));
                (i < burstCount ? burstKeys : sparseKeys).add(keys.getQuick(i));
            }
            try (Map map = newMap()) {
                final HorizonJoinTimeFrameHelper helper = newHelper(true);
                lookup(helper, cursor, map, burstPositions, burstKeys);
                Assert.assertTrue(helper.isForwardScanMode());

                helper.toTop();
                map.clear();
                Assert.assertFalse(helper.isForwardScanMode());
                final long[] sparseRows = lookupWithoutReset(helper, cursor, map, sparsePositions, sparseKeys);
                Assert.assertFalse(helper.isForwardScanMode());
                for (int i = 0, n = sparseRows.length; i < n; i++) {
                    Assert.assertTrue("rows of lookup " + i + ": " + sparseRows[i], sparseRows[i] <= 1_000);
                }
            }
        });
    }

    @Test
    public void testWindowSwitchOverSmallGapsOnlyWhenEnabled() throws Exception {
        // 150 lookups 100 rows apart alternate between a common key, held by every row but row 0,
        // and a rare key that only row 0 holds. No gap reaches the min gap (1,024), so only the
        // window check can switch. Without it, every rare lookup scans back to row 0 and every
        // common lookup reads its own row. With it, the first window (lookups 1-11) costs
        // 3,011 rows, below 8 x 1,100, and the second window (lookups 12-22) costs 10,211 rows,
        // above it: lookup 22 switches, and every lookup from there scans its 100-row gap forward.
        assertMemoryLeak(() -> {
            final SlaveCursor cursor = new SlaveCursor(new long[]{20_000}, row -> row == 0 ? RARE_KEY : COMMON_KEY);
            final LongList positions = new LongList();
            final IntList keys = new IntList();
            addRareKeyRun(positions, keys, 100, 150);
            try (Map map = newMap()) {
                final HorizonJoinTimeFrameHelper aggregating = newHelper(false);
                Assert.assertEquals(-1, firstForwardScanLookup(aggregating, cursor, map, positions, keys));
                final long[] aggregatingRows = lookup(newHelper(false), cursor, map, positions, keys);
                for (int i = 0, n = positions.size(); i < n; i++) {
                    Assert.assertEquals(
                            "rows of lookup " + i,
                            keys.getQuick(i) == RARE_KEY ? positions.getQuick(i) + 1 : 1,
                            aggregatingRows[i]
                    );
                }

                final HorizonJoinTimeFrameHelper projection = newHelper(true);
                Assert.assertEquals(22, firstForwardScanLookup(projection, cursor, map, positions, keys));
                final long[] projectionRows = lookup(newHelper(true), cursor, map, positions, keys);
                for (int i = 0, n = positions.size(); i < n; i++) {
                    Assert.assertEquals("rows of lookup " + i, i < 22 ? aggregatingRows[i] : 100, projectionRows[i]);
                }
            }
        });
    }

    /**
     * Adds 60 lookups 20 rows apart from row 100,000 on, then 18 lookups 100,000 rows apart from
     * row 200,000 on, all for key 7. Returns the number of burst lookups.
     */
    private static int addBurstThenSparse(LongList positions, IntList keys) {
        final int burstCount = 60;
        for (int i = 0; i < burstCount; i++) {
            positions.add(100_000 + 20L * i);
            keys.add(7);
        }
        for (int i = 0; i < 18; i++) {
            positions.add(200_000 + 100_000L * i);
            keys.add(7);
        }
        return burstCount;
    }

    /**
     * Adds {@code count} lookups 2 rows apart from row {@code position} on, for a slave that
     * cycles through 30 keys: every lookup asks for the key that the slave holds 19 rows before
     * the lookup's position, so a backward scan for it from scratch reads 20 rows.
     */
    private static void addDenseRun(LongList positions, IntList keys, long position, int count) {
        for (int i = 0; i < count; i++) {
            positions.add(position + 2L * i);
            keys.add((int) ((position + 2L * i - 19) % 30));
        }
    }

    /**
     * Adds lookups at {@code gap}, 2 x {@code gap} and so on, for the common key at odd multiples
     * and for the rare key at even ones.
     */
    private static void addRareKeyRun(LongList positions, IntList keys, long gap, int count) {
        for (int x = 1; x <= count; x++) {
            positions.add(gap * x);
            keys.add(x % 2 == 0 ? RARE_KEY : COMMON_KEY);
        }
    }

    /**
     * Runs the lookups and returns the index of the first one after which the helper is in forward
     * scan mode, or -1 if it stays in backward-only mode.
     */
    private static int firstForwardScanLookup(
            HorizonJoinTimeFrameHelper helper,
            SlaveCursor cursor,
            Map map,
            LongList positions,
            IntList keys
    ) {
        helper.of(cursor);
        map.clear();
        final MasterRecord masterRecord = new MasterRecord();
        int result = -1;
        for (int i = 0, n = positions.size(); i < n; i++) {
            final long asOfRowId = helper.findAsOfRow(positions.getQuick(i));
            masterRecord.key = keys.getQuick(i);
            helper.findKeyedAsOfMatch(asOfRowId, masterRecord, MASTER_KEY_SINK, SLAVE_KEY_SINK, map, null);
            if (helper.isForwardScanMode()) {
                if (result == -1) {
                    result = i;
                }
            } else {
                Assert.assertEquals("forward scan mode is sticky until toTop()", -1, result);
            }
        }
        return result;
    }

    /**
     * Runs the lookups through the helper from of() on, checks every match against a brute-force
     * scan and returns the number of slave rows that each lookup read.
     */
    private static long[] lookup(
            HorizonJoinTimeFrameHelper helper,
            SlaveCursor cursor,
            Map map,
            LongList positions,
            IntList keys
    ) {
        helper.of(cursor);
        map.clear();
        return lookupWithoutReset(helper, cursor, map, positions, keys);
    }

    /**
     * Runs the lookups as {@link #lookup} does, but goes on from the state that the helper and the
     * map are in.
     */
    private static long[] lookupWithoutReset(
            HorizonJoinTimeFrameHelper helper,
            SlaveCursor cursor,
            Map map,
            LongList positions,
            IntList keys
    ) {
        final MasterRecord masterRecord = new MasterRecord();
        final long[] scannedRows = new long[positions.size()];
        for (int i = 0, n = positions.size(); i < n; i++) {
            final long position = positions.getQuick(i);
            final long asOfRowId = helper.findAsOfRow(position);
            Assert.assertEquals("ASOF position of lookup " + i, cursor.rowIdOf(position), asOfRowId);

            masterRecord.key = keys.getQuick(i);
            final long keyReadCount = cursor.keyReadCount;
            final long matchRowId = helper.findKeyedAsOfMatch(asOfRowId, masterRecord, MASTER_KEY_SINK, SLAVE_KEY_SINK, map, null);
            scannedRows[i] = cursor.keyReadCount - keyReadCount;
            Assert.assertEquals(
                    "match of lookup " + i + " [position=" + position + ", key=" + masterRecord.key + ']',
                    cursor.findMatch(position, masterRecord.key),
                    matchRowId
            );
        }
        return scannedRows;
    }

    private static HorizonJoinTimeFrameHelper newHelper(boolean isWindowSwitchEnabled) {
        return new HorizonJoinTimeFrameHelper(
                configuration.getSqlAsOfJoinLookAhead(),
                1,
                BWD_SCAN_ABSOLUTE_THRESHOLD,
                BWD_SCAN_MIN_GAP,
                BWD_SCAN_SWITCH_FACTOR,
                isWindowSwitchEnabled
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

    private interface KeyFunction {
        int keyAt(long row);
    }

    /**
     * The key cache of an INT or SYMBOL join key, counting the slots that its clears write.
     */
    private static final class ClearCountingMap extends Unordered4Map {
        private long clearedSlots;

        private ClearCountingMap() {
            super(
                    ColumnType.INT,
                    new SingleColumnType(ColumnType.LONG),
                    configuration.getSqlSmallMapKeyCapacity(),
                    configuration.getSqlFastMapLoadFactor(),
                    configuration.getSqlMapMaxResizes()
            );
        }

        @Override
        public void clear() {
            clearedSlots += getKeyCapacity();
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
        private long keyReadCount;

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
            return cursor.keyFunction.keyAt(row());
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
}
