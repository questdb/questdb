/*******************************************************************************
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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxReader;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MOVE-TAIL decided and executed inside the partition task ({@code O3PartitionJob.moveTailToFreshPartition}): the
 * prefix of a composite folder stays where it is under a shorter geometry, and the tail pieces plus the commit's rows
 * are written once into a fresh sibling, which the writer inserts when it consumes the partition-update sink. The
 * shapes here are the ones the writer-side pre-pass never had to handle: the last (active) partition, two siblings of
 * one day moving in one commit, a covering posting index, and a failure part-way through the move.
 */
public class CompositeMoveTailInTaskTest extends AbstractCairoTest {
    private static final long DAY = MicrosTimestampDriver.floor("2024-01-01T00:00:00.000000Z");

    @Test
    public void testFailedMoveLeavesNoSiblingDirectoryAndResumes() throws Exception {
        final AtomicBoolean armed = new AtomicBoolean();
        final AtomicInteger refused = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                // The sibling's directory is the only one of the day named with a time of day.
                if (armed.get() && Utf8s.containsAscii(name, Files.SEPARATOR + "2024-01-01T0")
                        && Utf8s.endsWithAscii(name, Files.SEPARATOR + "v.d")
                        && refused.compareAndSet(0, 1)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            // Pooled frame columns capture the FilesFacade they were built with; start from a fresh pool.
            engine.resetFrameFactory();
            configureMoveTail();
            createDayWithABigPrefixAndASmallTailPiece(true);
            final TableToken token = engine.verifyTableName("x");
            final long nameTxnBefore = nameTxnOfDay();
            final int piecesBefore = pieceCountOfDay();

            armed.set(true);
            insertBoth("SELECT x + 480, timestamp_sequence('2024-01-01T00:06:50', 1_000_000L) FROM long_sequence(10)");
            armed.set(false);
            Assert.assertEquals("fixture: the injected failure never fired", 1, refused.get());
            Assert.assertTrue("the failed move must suspend the table", engine.getTableSequencerAPI().isSuspended(token));
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            Assert.assertEquals("a failed move must leave the day's folders as they were", 1, dayFolderCount());
            Assert.assertEquals("a failed move must leave the prefix's directory alone", nameTxnBefore, nameTxnOfDay());
            Assert.assertEquals("a failed move must leave the prefix's geometry alone", piecesBefore, pieceCountOfDay());
            assertNoSiblingDirectoryOfDay(ff);

            execute("ALTER TABLE x RESUME WAL");
            drainWalQueue();
            Assert.assertFalse("the resumed apply suspended the table again", engine.getTableSequencerAPI().isSuspended(token));
            Assert.assertEquals("the resumed apply must move the tail", 2, dayFolderCount());
            Assert.assertEquals(nameTxnBefore, nameTxnOfDay());
            assertDayMatchesOracle();
        });
    }

    @Test
    public void testMoveTailOnTheLastPartition() throws Exception {
        assertMemoryLeak(() -> {
            configureMoveTail();
            // No later day: 2024-01-01 is the active partition throughout.
            createDayWithABigPrefixAndASmallTailPiece(false);
            final long nameTxnBefore = nameTxnOfDay();
            final long writtenBefore = physicallyWrittenRows();
            insertBoth("SELECT x + 480, timestamp_sequence('2024-01-01T00:06:50', 1_000_000L) FROM long_sequence(10)");
            Assert.assertEquals(2, dayFolderCount());
            // The 80-row tail and the ten incoming rows, written once into the sibling - plus the ten rows' copy
            // into the active partition's lag, which every WAL transaction on the last partition pays first.
            Assert.assertEquals(100, physicallyWrittenRows() - writtenBefore);
            Assert.assertEquals("the prefix must keep its directory", nameTxnBefore, nameTxnOfDay());
            try (TableReader reader = engine.getReader("x")) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals(400, tx.getPartitionSize(0));
                Assert.assertEquals(90, tx.getPartitionSize(1));
                Assert.assertEquals("the sibling is the new active partition", 90, tx.getTransientRowCount());
                Assert.assertEquals(400, tx.getFixedRowCount());
                Assert.assertEquals(MicrosTimestampDriver.floor("2024-01-01T00:06:40.000000Z"), tx.getPartitionTimestampByIndex(1));
            }
            assertDayMatchesOracle();

            // The sibling is the active partition now: a plain append lands on it, and backfill below its
            // floor still finds the prefix.
            insertBoth("SELECT x + 600, timestamp_sequence('2024-01-01T00:08:00', 1_000_000L) FROM long_sequence(20)");
            insertBoth("SELECT x + 700, timestamp_sequence('2024-01-01T00:03:00', 1_000_000L) FROM long_sequence(5)");
            Assert.assertEquals(2, dayFolderCount());
            assertDayMatchesOracle();
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertDayMatchesOracle();
        });
    }

    @Test
    public void testMoveTailWithCoveringPostingIndex() throws Exception {
        assertMemoryLeak(() -> {
            configureMoveTail();
            execute("CREATE TABLE x (v LONG, sym SYMBOL INDEX TYPE POSTING INCLUDE (v, s), p SYMBOL INDEX TYPE POSTING," +
                    " s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE oracle (v LONG, sym SYMBOL, p SYMBOL, s VARCHAR, ts TIMESTAMP)");
            insertBoth(coveredRows(0, 440, "2024-01-01T00:00:00"));
            insertBoth("SELECT 0, 'none', 'none', '', '2024-01-03'::TIMESTAMP FROM long_sequence(1)");
            insertBoth(coveredRows(440, 40, "2024-01-01T00:06:40"));
            final long nameTxnBefore = nameTxnOfDay();
            insertBoth(coveredRows(480, 10, "2024-01-01T00:06:50"));
            Assert.assertEquals(2, dayFolderCount());
            Assert.assertEquals(nameTxnBefore, nameTxnOfDay());
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertDayMatchesOracle();
            // Through the indexes: both the covering one, which serves v and s out of its sidecars, and the plain one.
            for (int k = 0; k < 7; k++) {
                assertSqlCursors(
                        "SELECT v, s FROM oracle WHERE sym = 'sym" + k + "' AND ts IN '2024-01-01' ORDER BY ts, v",
                        "SELECT v, s FROM x WHERE sym = 'sym" + k + "' AND ts IN '2024-01-01' ORDER BY ts, v"
                );
            }
            for (int k = 0; k < 5; k++) {
                assertSqlCursors(
                        "SELECT v, s FROM oracle WHERE p = 'p" + k + "' AND ts IN '2024-01-01' ORDER BY ts, v",
                        "SELECT v, s FROM x WHERE p = 'p" + k + "' AND ts IN '2024-01-01' ORDER BY ts, v"
                );
            }
        });
    }

    @Test
    public void testTwoSiblingsOfOneDayMoveTheirTailsInOneCommit() throws Exception {
        assertMemoryLeak(() -> {
            configureMoveTail();
            // First sibling: the day's folder, 440 rows with a 40-row backfill over its top 40, moves its tail.
            createDayWithABigPrefixAndASmallTailPiece(true);
            insertBoth("SELECT x + 480, timestamp_sequence('2024-01-01T00:06:50', 1_000_000L) FROM long_sequence(10)");
            Assert.assertEquals(2, dayFolderCount());
            // Grow the sibling at 00:06:40 the same way: 440 rows above its range, then a 40-row backfill over the
            // top 40 of those, which the pre-split cuts into an untouched prefix and a small tail.
            insertBoth("SELECT x + 1000, timestamp_sequence('2024-01-01T00:10:00', 1_000_000L) FROM long_sequence(440)");
            insertBoth("SELECT x + 1440, timestamp_sequence('2024-01-01T00:16:40', 1_000_000L) FROM long_sequence(40)");
            // Give the first folder a fresh small tail too: a 20-row backfill over rows 300..319 of its 400-row
            // prefix, which the pre-split cuts at 300, leaving a 120-row tail a 300-row prefix dominates.
            insertBoth("SELECT x + 2000, timestamp_sequence('2024-01-01T00:05:00', 1_000_000L) FROM long_sequence(20)");
            Assert.assertEquals(2, dayFolderCount());
            final LongList nameTxnsBefore = dayFolderNameTxns();

            // One block, two transactions, one per sibling, each merging into its small tail.
            execute("INSERT INTO x SELECT x + 3000, timestamp_sequence('2024-01-01T00:05:10', 1_000_000L) FROM long_sequence(10)");
            execute("INSERT INTO x SELECT x + 3100, timestamp_sequence('2024-01-01T00:16:50', 1_000_000L) FROM long_sequence(10)");
            execute("INSERT INTO oracle SELECT x + 3000, timestamp_sequence('2024-01-01T00:05:10', 1_000_000L) FROM long_sequence(10)");
            execute("INSERT INTO oracle SELECT x + 3100, timestamp_sequence('2024-01-01T00:16:50', 1_000_000L) FROM long_sequence(10)");
            drainWalQueue();
            Assert.assertEquals("both siblings must move their tails", 4, dayFolderCount());
            final LongList nameTxnsAfter = dayFolderNameTxns();
            Assert.assertEquals("the first prefix must keep its directory", nameTxnsBefore.getQuick(0), nameTxnsAfter.getQuick(0));
            Assert.assertEquals("the second prefix must keep its directory", nameTxnsBefore.getQuick(1), nameTxnsAfter.getQuick(2));
            Assert.assertEquals("both tails are named by the one commit", nameTxnsAfter.getQuick(1), nameTxnsAfter.getQuick(3));
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertDayMatchesOracle();
        });
    }

    private static void assertDayMatchesOracle() throws Exception {
        assertSqlCursors("SELECT * FROM oracle ORDER BY ts, v", "SELECT * FROM x ORDER BY ts, v");
        assertSqlCursors(
                "SELECT count() c, sum(v) s FROM oracle WHERE ts IN '2024-01-01'",
                "SELECT count() c, sum(v) s FROM x WHERE ts IN '2024-01-01'"
        );
    }

    private static void assertNoSiblingDirectoryOfDay(FilesFacade ff) {
        final TableToken token = engine.verifyTableName("x");
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token);
            final long findPtr = ff.findFirst(path.$());
            Assert.assertTrue(findPtr != 0);
            try {
                final StringSink name = new StringSink();
                do {
                    name.clear();
                    Utf8s.utf8ToUtf16Z(ff.findName(findPtr), name);
                    if (Chars.startsWith(name, "2024-01-01T0")) {
                        Assert.fail("the failed move left its sibling's directory behind: " + name);
                    }
                } while (ff.findNext(findPtr) > 0);
            } finally {
                ff.findClose(findPtr);
            }
        }
    }

    /**
     * The thresholds {@code CompositeAppendCompactionForecastTest} uses for its MOVE-TAIL cases: a dead-space floor
     * a hundred-row fixture reaches, the piece-count rule at twenty, and background compaction off.
     */
    private static void configureMoveTail() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_PRESSURE_DEAD_RATIO, "0.005");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "100000h");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD_PERCENT, "99");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 16);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 50);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 20);
    }

    private static String coveredRows(int base, int count, String start) {
        return "SELECT x + " + base + ", 'sym' || (x % 7), 'p' || (x % 5), 's' || (x + " + base + "), timestamp_sequence('"
                + start + "', 1_000_000L) FROM long_sequence(" + count + ")";
    }

    /**
     * 2024-01-01 holding two pieces: 440 rows one second apart, then 40 backdated rows over the top 40 of them. The
     * pre-split cuts the untouched 400-row prefix off into a piece of its own, and merge-append re-writes the last 40
     * together with the incoming 40 at the shared files' tail, leaving an 80-row tail piece over 40 dead rows.
     */
    private static void createDayWithABigPrefixAndASmallTailPiece(boolean hasLaterDay) throws Exception {
        execute("CREATE TABLE x (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE oracle (v LONG, ts TIMESTAMP)");
        insertBoth("SELECT x, timestamp_sequence('2024-01-01T00:00:00', 1_000_000L) FROM long_sequence(440)");
        if (hasLaterDay) {
            // A later day, so 2024-01-01 is never the active partition and every further write to it is O3.
            insertBoth("SELECT 0, '2024-01-03'::TIMESTAMP FROM long_sequence(1)");
        }
        insertBoth("SELECT x + 440, timestamp_sequence('2024-01-01T00:06:40', 1_000_000L) FROM long_sequence(40)");
    }

    private static int dayFolderCount() {
        return dayFolderNameTxns().size();
    }

    private static LongList dayFolderNameTxns() {
        final LongList nameTxns = new LongList();
        try (TableReader reader = engine.getReader("x")) {
            final TxReader tx = reader.getTxFile();
            for (int i = 0, n = tx.getPartitionCount(); i < n; i++) {
                if (tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(i)) == DAY) {
                    nameTxns.add(tx.getPartitionNameTxn(i));
                }
            }
        }
        return nameTxns;
    }

    private static void insertBoth(String select) throws Exception {
        execute("INSERT INTO x " + select);
        execute("INSERT INTO oracle " + select);
        drainWalQueue();
    }

    private static long nameTxnOfDay() {
        return dayFolderNameTxns().getQuick(0);
    }

    private static long physicallyWrittenRows() {
        return node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
    }

    private static int pieceCountOfDay() {
        try (TableReader reader = engine.getReader("x")) {
            return reader.getGeometry().getPieceCount(reader.getTxFile().getPartitionIndex(DAY));
        }
    }
}
