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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionCompactionPolicy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TxReader;
import io.questdb.std.LongList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@code cairo.o3.partition.max.splits} is the squash target, not a split gate. A split that pays - MOVE-TAIL or
 * the O3 prefix split - happens even when its day already holds the cap, up to
 * {@link PartitionCompactionPolicy#getSplitCeiling}; housekeeping then squashes the smallest cold adjacent pairs
 * back to the cap, and a day stays over the cap only while its folders are hot.
 */
public class CompactionSplitOverflowTest extends AbstractCairoTest {
    private static final int CAP = 2;
    private static final long DAY = MicrosTimestampDriver.floor("2024-01-01T00:00:00.000000Z");

    @Test
    public void testHotFoldersHoldOverflowUntilTheyCool() throws Exception {
        assertMemoryLeak(() -> {
            final int hotCommits = 3;
            createClassicSplitTable(hotCommits);
            final int ceiling = PartitionCompactionPolicy.getSplitCeiling(configuration);
            Assert.assertEquals(CAP + CAP, ceiling);
            for (int minute : new int[]{600, 540, 480}) {
                insertClassicRows(minute);
            }
            Assert.assertEquals("three splits in a row leave every folder of the day hot", ceiling, dayFolderCount());
            // Commits that do not touch the day only age its folders. The splits went in one commit apart, so
            // they cool one commit apart: each commit frees exactly one more cold pair for the squash, and the
            // last hot folder cools on the hotCommits-th commit.
            final int[] expected = {4, 3, 2};
            Assert.assertEquals(hotCommits, expected.length);
            for (int commit = 0; commit < hotCommits; commit++) {
                insertNextDayRow(commit);
                Assert.assertEquals("folders after commit " + commit, expected[commit], dayFolderCount());
            }
            for (int commit = 0; commit < hotCommits; commit++) {
                insertNextDayRow(100 + commit);
                Assert.assertEquals(CAP, dayFolderCount());
            }
            assertClassicRows(3, 2 * hotCommits);
        });
    }

    @Test
    public void testMoveTailSplitsPastCapAndSquashesBackWhenCold() throws Exception {
        assertMemoryLeak(() -> {
            final int hotCommits = 2;
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_PRESSURE_DEAD_RATIO, "0.005");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "0");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, "1.0");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "100000h");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD_PERCENT, "99");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, hotCommits);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_TIME, 0);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_MOVE_TAIL_MIN_GAIN, 1);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, CAP);
            final int ceiling = PartitionCompactionPolicy.getSplitCeiling(configuration);
            Assert.assertEquals(CAP + hotCommits, ceiling);
            execute("CREATE TABLE x (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE oracle (v LONG, ts TIMESTAMP)");
            // A later day, so 2024-01-01 is never the active partition and every write to it is O3.
            insertBoth("SELECT 0, '2024-01-03'::TIMESTAMP FROM long_sequence(1)");

            final int iterations = 8;
            int movesAtCap = 0;
            int maxCount = 0;
            for (int iter = 0, base = 0; iter < iterations; iter++, base += 440) {
                // Grow the day's last folder past the split size with rows above everything it holds...
                node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 16);
                node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1G");
                insertBoth(rowsAt(base, 440));
                maxCount = Math.max(maxCount, assertWithinCeiling(ceiling));
                // ...leave dead rows over its top 40 rows, which the pre-split cuts into a tail piece...
                insertBoth(rowsAt(base + 400, 40));
                maxCount = Math.max(maxCount, assertWithinCeiling(ceiling));
                // ...and merge into that tail piece, which the forecast moves to a fresh folder first.
                node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, Long.MAX_VALUE / 8);
                node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 20);
                node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1_919);
                final int countBefore = dayFolderCount();
                final long lastFolderBefore = lastDayFolderTimestamp();
                insertBoth(rowsAt(base + 410, 10));
                maxCount = Math.max(maxCount, assertWithinCeiling(ceiling));
                Assert.assertTrue("MOVE-TAIL must cut a fresh folder on iteration " + iter, lastDayFolderTimestamp() > lastFolderBefore);
                if (countBefore >= CAP) {
                    movesAtCap++;
                }
            }
            Assert.assertTrue("splits must not stop at the cap", movesAtCap >= iterations - 2);
            Assert.assertTrue("the day must overflow the cap while its new folders are hot", maxCount > CAP);

            for (int commit = 0; commit <= hotCommits; commit++) {
                insertNextDayRow(1_000 + commit);
            }
            Assert.assertEquals("cold folders must be squashed back to the cap", CAP, dayFolderCount());

            assertSqlCursors("SELECT * FROM oracle ORDER BY ts, v", "SELECT * FROM x ORDER BY ts, v");
            assertQuery("SELECT count() c, sum(v) s FROM x WHERE ts IN '2024-01-01'")
                    .noRandomAccess().expectSize().returns("c\ts\n3920\t783160\n");
            assertQuery("SELECT count() c FROM (SELECT ts FROM x ORDER BY ts)")
                    .noRandomAccess().expectSize().returns("c\n3924\n");
        });
    }

    @Test
    public void testO3SplitPastCapAndSquashesBackWhenCold() throws Exception {
        assertMemoryLeak(() -> {
            final int hotCommits = 2;
            createClassicSplitTable(hotCommits);
            final int ceiling = PartitionCompactionPolicy.getSplitCeiling(configuration);
            Assert.assertEquals(CAP + hotCommits, ceiling);
            final int[] minutes = {600, 540, 480, 420, 360, 300, 240, 180};
            int splitsAtCap = 0;
            int maxCount = 0;
            for (int minute : minutes) {
                final int countBefore = dayFolderCount();
                final LongList foldersBefore = dayFolderTimestamps();
                insertClassicRows(minute);
                maxCount = Math.max(maxCount, assertWithinCeiling(ceiling));
                if (countBefore >= CAP && hasNewFolder(foldersBefore, dayFolderTimestamps())) {
                    splitsAtCap++;
                }
            }
            Assert.assertTrue("O3 splits must not stop at the cap", splitsAtCap >= 3);
            Assert.assertEquals("the day must reach, and not pass, the ceiling", ceiling, maxCount);
            for (int commit = 0; commit <= hotCommits; commit++) {
                insertNextDayRow(commit);
            }
            Assert.assertEquals("cold folders must be squashed back to the cap", CAP, dayFolderCount());
            assertClassicRows(minutes.length, hotCommits + 1);
        });
    }

    private static int assertWithinCeiling(int ceiling) {
        final int count = dayFolderCount();
        Assert.assertTrue("the day holds " + count + " folders, over the ceiling of " + ceiling, count <= ceiling);
        return count;
    }

    private static void createClassicSplitTable(int hotCommits) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, CAP);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, hotCommits);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "100000h");
        execute("CREATE TABLE x AS (SELECT x v, timestamp_sequence('2024-01-01', 1_000_000L) ts FROM long_sequence(40_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        // A later day, so 2024-01-01 is never the active partition and every write to it is O3.
        execute("INSERT INTO x VALUES (0, '2024-01-03')");
        drainWalQueue();
        execute("CREATE TABLE oracle (v LONG, ts TIMESTAMP)");
        execute("INSERT INTO oracle SELECT v, ts FROM x");
    }

    private static int dayFolderCount() {
        return dayFolderTimestamps().size();
    }

    private static LongList dayFolderTimestamps() {
        final LongList timestamps = new LongList();
        try (TableReader reader = engine.getReader("x")) {
            final TxReader tx = reader.getTxFile();
            for (int i = 0, n = tx.getPartitionCount(); i < n; i++) {
                final long ts = tx.getPartitionTimestampByIndex(i);
                if (tx.getLogicalPartitionTimestamp(ts) == DAY) {
                    timestamps.add(ts);
                }
            }
        }
        return timestamps;
    }

    private static boolean hasNewFolder(LongList before, LongList after) {
        for (int i = 0, n = after.size(); i < n; i++) {
            if (before.indexOf(after.getQuick(i)) < 0) {
                return true;
            }
        }
        return false;
    }

    private static void insertBoth(String select) throws Exception {
        execute("INSERT INTO x " + select);
        execute("INSERT INTO oracle " + select);
        drainWalQueue();
    }

    private static void insertClassicRows(int minute) throws Exception {
        insertBoth("SELECT x + 100_000, timestamp_sequence('2024-01-01'::TIMESTAMP + "
                + minute + " * 60_000_000L, 1_000L) FROM long_sequence(20)");
    }

    private static void insertNextDayRow(int v) throws Exception {
        insertBoth("SELECT " + v + ", '2024-01-03T01'::TIMESTAMP FROM long_sequence(1)");
    }

    private static long lastDayFolderTimestamp() {
        final LongList timestamps = dayFolderTimestamps();
        return timestamps.getQuick(timestamps.size() - 1);
    }

    private static String rowsAt(long secondLo, int count) {
        return "SELECT x, '2024-01-01'::TIMESTAMP + (" + secondLo + " + x - 1) * 1_000_000L FROM long_sequence(" + count + ")";
    }

    private void assertClassicRows(int batches, int nextDayRows) throws Exception {
        final long dayRows = 40_000L + 20L * batches;
        final long daySum = 40_000L * 40_001 / 2 + batches * (20L * 100_000 + 210);
        assertQuery("SELECT count() c, sum(v) s FROM x WHERE ts IN '2024-01-01'")
                .noRandomAccess().expectSize().returns("c\ts\n" + dayRows + '\t' + daySum + '\n');
        assertQuery("SELECT count() c FROM x WHERE ts IN '2024-01-03'")
                .noRandomAccess().expectSize().returns("c\n" + (1 + nextDayRows) + '\n');
        assertSqlCursors("SELECT * FROM oracle ORDER BY ts, v", "SELECT * FROM x ORDER BY ts, v");
    }
}
