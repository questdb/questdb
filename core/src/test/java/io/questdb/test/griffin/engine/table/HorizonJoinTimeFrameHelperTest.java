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
    public void testBurstThenSparseScansForwardOnlyWithWindowSwitch() throws Exception {
        // A burst of 60 lookups 20 rows apart, then 18 lookups 100,000 rows apart, all for key 7,
        // which every 1,000th slave row holds. Each lookup scans back fewer than 1,000 rows. The
        // burst's gaps sit below the min gap, so only the window check can switch: with it, the
        // lookup switches in the burst and then scans every row of the sparse stretch forward.
        // Without it, as in the aggregating factories, every lookup keeps its short backward scan.
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
                // The sparse stretch reads every row between the last burst position and the last position.
                Assert.assertEquals(
                        positions.getLast() - positions.getQuick(burstCount - 1),
                        sum(projectionRows, burstCount, positions.size())
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
        // After the burst of testBurstThenSparseScansForwardOnlyWithWindowSwitch() switches the
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

    private static long sum(long[] values, int lo, int hi) {
        long sum = 0;
        for (int i = lo; i < hi; i++) {
            sum += values[i];
        }
        return sum;
    }

    private interface KeyFunction {
        int keyAt(long row);
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
