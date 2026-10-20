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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.PartitionCompactionScanJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An O3 commit that updates a parquet partition in place keeps the partition directory and
 * its index files, but it changes the row layout. The symbol indexes therefore store index
 * rows rather than partition rows: the update keeps the index rows of the row groups it kept,
 * gives the row groups it wrote fresh index rows, and appends only those to the indexes.
 * Every test opens index-driven cursors before the commit and checks that they keep returning
 * their snapshot, then checks that new queries see the merged rows, and pins how many index
 * rows the partition covers after the commit.
 * <p>
 * The parquet partition holds 12 rows in 3 row groups of 4 rows:
 * RG0 01:00-04:00, RG1 05:00-08:00, RG2 09:00-12:00.
 */
@RunWith(Parameterized.class)
public class O3ParquetIndexIsolationTest extends AbstractCairoTest {
    private static final String[] KEY_FILTERS = {"s = 'a'", "s = 'b'", "s = 'c'", "s IS NULL"};
    private static final String[] ORDERS = {"", " ORDER BY ts DESC"};
    private static final String PARTITION = "2024-01-01";
    private static final long PARTITION_TIMESTAMP = 1704067200000000L;
    // the same keys through a cast, so the filter does not use the index
    private static final String[] TRUTH_FILTERS = {"s::STRING = 'a'", "s::STRING = 'b'", "s::STRING = 'c'", "s::STRING IS NULL"};
    private final String indexClause;
    // a covering index knows the result size, and its forward scan has no random access
    private final boolean isCovering;

    public O3ParquetIndexIsolationTest(String indexClause, @SuppressWarnings("unused") byte indexType, boolean isCovering) {
        this.indexClause = indexClause;
        this.isCovering = isCovering;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"INDEX TYPE BITMAP", IndexType.BITMAP, false},
                {"INDEX TYPE POSTING", IndexType.POSTING, false},
                {"INDEX TYPE POSTING INCLUDE (v)", IndexType.POSTING, true},
        });
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // small row groups and permissive rewrite thresholds keep the O3 merge in place
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
    }

    @Test
    public void testAddIndexAfterInPlaceUpdate() throws Exception {
        // ADD INDEX builds the index of a partition whose row groups an in-place update
        // moved to fresh index rows, so it must build in index rows too
        assertMemoryLeak(() -> {
            createParquetPartition("", false);
            execute("INSERT INTO pt VALUES ('b', 100, '2024-01-01T00:30:00.000000Z'), ('a', 101, '2024-01-01T06:30:00.000000Z')");
            drainWalQueue();
            Assert.assertTrue(hasIndexRowMap());
            execute("ALTER TABLE pt ALTER COLUMN s ADD " + indexClause);
            drainWalQueue();
            assertIndexQueriesMatchTruth();
            // and the next in-place update appends to the index ADD INDEX built
            assertIsolatedAcrossCommit("INSERT INTO pt VALUES ('c', 102, '2024-01-01T10:30:00.000000Z')", true);
        });
    }

    @Test
    public void testAppendAfterLastRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            createParquetPartition("", true);
            assertIsolatedAcrossCommit(
                    """
                            INSERT INTO pt VALUES
                            ('a', 100, '2024-01-01T12:30:00.000000Z'),
                            (NULL, 101, '2024-01-01T13:00:00.000000Z'),
                            ('b', 102, '2024-01-01T13:30:00.000000Z')
                            """,
                    true
            );
            // a row group added after the last one keeps the identity map
            Assert.assertFalse(hasIndexRowMap());
        });
    }

    @Test
    public void testBackgroundCompactionAfterInPlaceUpdate() throws Exception {
        // The idle sweep copies the live row groups into a fresh file that uses the identity
        // index row map, so it must index the copy afresh rather than carry over index files
        // that an in-place update grew over another map
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, "0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "60m");
        assertMemoryLeak(() -> {
            createParquetPartition("", true);
            assertIsolatedAcrossCommit("INSERT INTO pt VALUES ('b', 100, '2024-01-01T00:30:00.000000Z'), ('a', 101, '2024-01-01T06:30:00.000000Z')", true);
            Assert.assertTrue(hasIndexRowMap());
            final long nameTxn = getPartitionNameTxn();
            final ObjList<PinnedQuery> pinned = openPinnedQueries();
            try {
                engine.releaseInactive();
                final Clock twoHoursAhead = () -> configuration.getMicrosecondClock().getTicks() + 2 * Micros.HOUR_MICROS;
                try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, configuration.getFilesFacade(), twoHoursAhead)) {
                    job.run();
                }
                Assert.assertNotEquals("the sweep must compact the partition", nameTxn, getPartitionNameTxn());
                Assert.assertFalse(hasIndexRowMap());
                assertPinnedSnapshots(pinned);
            } finally {
                Misc.freeObjList(pinned);
            }
            assertIndexQueriesMatchTruth();
            // the compacted partition keeps taking in-place updates
            assertIsolatedAcrossCommit("INSERT INTO pt VALUES ('c', 102, '2024-01-01T02:30:00.000000Z')", true);
        });
    }

    @Test
    public void testConvertToNativeAfterInPlaceUpdate() throws Exception {
        assertMemoryLeak(() -> {
            createParquetPartition("", true);
            assertIsolatedAcrossCommit("INSERT INTO pt VALUES ('b', 100, '2024-01-01T00:30:00.000000Z')", true);
            Assert.assertTrue(hasIndexRowMap());
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '" + PARTITION + "'");
            drainWalQueue();
            assertIndexQueriesMatchTruth();
        });
    }

    @Test
    public void testDedupUpsertInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createParquetPartition(" DEDUP UPSERT KEYS(ts, s)", true);
            assertIsolatedAcrossCommit(
                    """
                            INSERT INTO pt VALUES
                            ('b', 500, '2024-01-01T05:00:00.000000Z'),
                            ('a', 501, '2024-01-01T05:00:00.000000Z'),
                            ('c', 502, '2024-01-01T07:30:00.000000Z')
                            """,
                    true
            );
        });
    }

    @Test
    public void testFailedInPlaceUpdateRollsBackIndexTail() throws Exception {
        // s2 is indexed after s, so failing to open its index files leaves the failed
        // in-place update with rows appended to the index of s. The retry must drop them
        // before it appends the same rows again.
        final AtomicBoolean isArmed = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isArmed.get() && Utf8s.containsAscii(name, Files.SEPARATOR + "s2.")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE pt (s SYMBOL " + indexClause + ", v INT, s2 SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO pt VALUES
                            ('a', 1, 'x', '2024-01-01T01:00:00.000000Z'), ('b', 2, 'y', '2024-01-01T02:00:00.000000Z'),
                            ('c', 3, 'x', '2024-01-01T03:00:00.000000Z'), ('a', 4, 'y', '2024-01-01T04:00:00.000000Z'),
                            ('b', 5, 'x', '2024-01-01T05:00:00.000000Z'), (NULL, 6, 'y', '2024-01-01T06:00:00.000000Z'),
                            ('a', 7, 'x', '2024-01-01T07:00:00.000000Z'), ('b', 8, 'y', '2024-01-01T08:00:00.000000Z'),
                            ('c', 9, 'x', '2024-01-01T09:00:00.000000Z'), ('a', 10, 'y', '2024-01-01T10:00:00.000000Z'),
                            ('b', 11, 'x', '2024-01-01T11:00:00.000000Z'), ('c', 12, 'y', '2024-01-01T12:00:00.000000Z'),
                            ('a', 99, 'x', '2024-01-02T00:00:00.000000Z')
                            """
            );
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '" + PARTITION + "'");
            drainWalQueue();

            final long nameTxn = getPartitionNameTxn();
            final long indexRowCount = getIndexRowCount();
            final ObjList<PinnedQuery> pinned = openPinnedQueries();
            try {
                isArmed.set(true);
                execute("INSERT INTO pt VALUES ('a', 100, 'x', '2024-01-01T06:30:00.000000Z')");
                drainWalQueue();
                isArmed.set(false);
                final TableToken tableToken = engine.verifyTableName("pt");
                Assert.assertTrue("failed update must suspend the table", engine.getTableSequencerAPI().isSuspended(tableToken));

                // the committed snapshot is unchanged, and the rows the failed update
                // appended to the index of s are out of every reader's reach
                Assert.assertEquals(indexRowCount, getIndexRowCount());
                assertPinnedSnapshots(pinned);
                assertIndexQueriesMatchTruth();

                execute("ALTER TABLE pt RESUME WAL");
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
                Assert.assertEquals("O3 merge must update the parquet partition in place", nameTxn, getPartitionNameTxn());
                assertPinnedSnapshots(pinned);
            } finally {
                isArmed.set(false);
                Misc.freeObjList(pinned);
            }
            assertIndexQueriesMatchTruth();
            assertQuery("SELECT count() FROM pt WHERE s = 'a' AND ts IN '" + PARTITION + "'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n5\n");
        });
    }

    @Test
    public void testGapBetweenRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            createParquetPartition("", true);
            assertIsolatedAcrossCommit(
                    """
                            INSERT INTO pt VALUES
                            ('c', 100, '2024-01-01T04:20:00.000000Z'),
                            ('a', 101, '2024-01-01T04:40:00.000000Z')
                            """,
                    true
            );
        });
    }

    @Test
    public void testLatestOnAfterInPlaceUpdate() throws Exception {
        assertMemoryLeak(() -> {
            createParquetPartition("", true);
            assertIsolatedAcrossCommit("INSERT INTO pt VALUES ('b', 100, '2024-01-01T00:30:00.000000Z'), ('a', 101, '2024-01-01T11:30:00.000000Z')", true);
            Assert.assertTrue(hasIndexRowMap());
            assertQuery("SELECT s, v, ts FROM pt WHERE ts IN '" + PARTITION + "' LATEST ON ts PARTITION BY s ORDER BY s")
                    .noLeakCheck()
                    .expectSize()
                    .returns(queryToString("""
                            SELECT s, last(v) v, last(ts) ts FROM (SELECT * FROM pt WHERE ts IN '2024-01-01' ORDER BY ts)
                            GROUP BY s ORDER BY s
                            """));
        });
    }

    @Test
    public void testMergeAllRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            createParquetPartition("", true);
            assertIsolatedAcrossCommit(
                    """
                            INSERT INTO pt VALUES
                            ('b', 100, '2024-01-01T01:30:00.000000Z'),
                            (NULL, 101, '2024-01-01T05:30:00.000000Z'),
                            ('a', 102, '2024-01-01T09:30:00.000000Z')
                            """,
                    true
            );
        });
    }

    @Test
    public void testMergeFirstAndLastRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            createParquetPartition("", true);
            assertIsolatedAcrossCommit(
                    """
                            INSERT INTO pt VALUES
                            ('a', 100, '2024-01-01T02:30:00.000000Z'),
                            ('c', 101, '2024-01-01T10:30:00.000000Z')
                            """,
                    true
            );
        });
    }

    @Test
    public void testMergeFirstRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            createParquetPartition("", true);
            assertIsolatedAcrossCommit("INSERT INTO pt VALUES ('a', 100, '2024-01-01T02:30:00.000000Z')", true);
        });
    }

    @Test
    public void testMergeLastRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            createParquetPartition("", true);
            assertIsolatedAcrossCommit("INSERT INTO pt VALUES (NULL, 100, '2024-01-01T10:30:00.000000Z')", true);
        });
    }

    @Test
    public void testMergeMiddleRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            createParquetPartition("", true);
            assertIsolatedAcrossCommit("INSERT INTO pt VALUES ('b', 100, '2024-01-01T06:30:00.000000Z')", true);
        });
    }

    @Test
    public void testMergeSplitsMiddleRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            createParquetPartition("", true);
            assertIsolatedAcrossCommit(
                    """
                            INSERT INTO pt VALUES
                            ('a', 100, '2024-01-01T05:15:00.000000Z'),
                            ('c', 101, '2024-01-01T05:30:00.000000Z'),
                            ('a', 102, '2024-01-01T05:45:00.000000Z'),
                            ('b', 103, '2024-01-01T06:15:00.000000Z'),
                            (NULL, 104, '2024-01-01T06:30:00.000000Z')
                            """,
                    true
            );
        });
    }

    @Test
    public void testPrependBeforeFirstRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            createParquetPartition("", true);
            assertIsolatedAcrossCommit(
                    """
                            INSERT INTO pt VALUES
                            ('b', 100, '2024-01-01T00:10:00.000000Z'),
                            ('a', 101, '2024-01-01T00:20:00.000000Z')
                            """,
                    true
            );
        });
    }

    @Test
    public void testRandomInPlaceUpdates() throws Exception {
        // Random O3 batches into the parquet partition, with the readers of the last few
        // versions pinned across every commit. The batches land anywhere in the day, so the
        // updates mix appends, prepends, merges and row group splits, and the index row map
        // keeps growing.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            createParquetPartition("", true);
            final String[] keys = {"'a'", "'b'", "'c'", "'d'", "NULL"};
            final ObjList<ObjList<PinnedQuery>> pinnedPerVersion = new ObjList<>();
            final StringSink sql = new StringSink();
            try {
                for (int iteration = 0; iteration < 12; iteration++) {
                    if (pinnedPerVersion.size() == 3) {
                        Misc.freeObjList(pinnedPerVersion.getQuick(0));
                        pinnedPerVersion.remove(0);
                    }
                    pinnedPerVersion.add(openPinnedQueries());

                    sql.clear();
                    sql.put("INSERT INTO pt VALUES ");
                    for (int i = 0, n = 1 + rnd.nextInt(6); i < n; i++) {
                        if (i > 0) {
                            sql.put(", ");
                        }
                        // minutes in [0, 14 * 60): around and past the committed rows. Every
                        // generated row has its own non-zero microseconds, so no two rows share
                        // a timestamp: a covering DESC plan sorts, and ties would come out in an
                        // order the backward table scan does not reproduce.
                        final int minute = rnd.nextInt(14 * 60);
                        final int micros = 1 + iteration * 6 + i;
                        sql.put('(').put(keys[rnd.nextInt(keys.length)])
                                .put(", ").put(1000 * (iteration + 1) + i)
                                .put(", '2024-01-01T").put(minute / 60 < 10 ? "0" : "").put(minute / 60)
                                .put(':').put(minute % 60 < 10 ? "0" : "").put(minute % 60)
                                .put(":00.0000").put(micros < 10 ? "0" : "").put(micros)
                                .put("Z')");
                    }
                    execute(sql);
                    drainWalQueue();
                    Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
                    for (int v = 0, n = pinnedPerVersion.size(); v < n; v++) {
                        assertPinnedSnapshots(pinnedPerVersion.getQuick(v));
                    }
                    assertIndexQueriesMatchTruth();
                }
            } finally {
                for (int v = 0, n = pinnedPerVersion.size(); v < n; v++) {
                    Misc.freeObjList(pinnedPerVersion.getQuick(v));
                }
            }
        });
    }

    @Test
    public void testRequestedCompactionAfterInPlaceUpdateAndAlterColumnType() throws Exception {
        assertRequestedCompactionAfterInPlaceUpdateAndAlterColumnType(true);
    }

    @Test
    public void testRequestedCompactionAfterInPlaceUpdateAndAlterColumnTypeNonWal() throws Exception {
        assertRequestedCompactionAfterInPlaceUpdateAndAlterColumnType(false);
    }

    @Test
    public void testRewriteDropsIndexRowMap() throws Exception {
        // with a zero ratio, the first commit updates in place (no unused bytes yet),
        // and its unused bytes make the second commit rewrite the file, which rebuilds
        // the index in partition rows
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "0.0");
        assertMemoryLeak(() -> {
            createParquetPartition("", true);
            assertIsolatedAcrossCommit("INSERT INTO pt VALUES ('a', 100, '2024-01-01T00:30:00.000000Z')", true);
            Assert.assertTrue(hasIndexRowMap());
            assertIsolatedAcrossCommit("INSERT INTO pt VALUES ('b', 101, '2024-01-01T02:30:00.000000Z')", false);
            Assert.assertFalse(hasIndexRowMap());
            assertIsolatedAcrossCommit("INSERT INTO pt VALUES ('c', 102, '2024-01-01T10:30:00.000000Z')", true);
        });
    }

    @Test
    public void testSingleRowGroupRewrite() throws Exception {
        // a single row group always takes the rewrite path
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createParquetPartition("", true);
            assertIsolatedAcrossCommit("INSERT INTO pt VALUES ('a', 100, '2024-01-01T06:30:00.000000Z')", false);
        });
    }

    @Test
    public void testSkippedFailedCommitLeavesNoStaleIndexRows() throws Exception {
        // The O3 commit updates two parquet partitions in place and fails on the second
        // one. The first partition's index keeps the rows the failed update appended.
        // After skipping the failed transaction, the next in-place update of the first
        // partition must drop them before it appends its own rows at the same index rows.
        final AtomicBoolean isArmed = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isArmed.get()
                        && Utf8s.containsAscii(name, Files.SEPARATOR + "2024-01-02")
                        && Utf8s.containsAscii(name, Files.SEPARATOR + "s.")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE pt (s SYMBOL " + indexClause + ", v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO pt VALUES
                            ('a', 1, '2024-01-01T01:00:00.000000Z'), ('b', 2, '2024-01-01T02:00:00.000000Z'),
                            ('c', 3, '2024-01-01T03:00:00.000000Z'), ('a', 4, '2024-01-01T04:00:00.000000Z'),
                            ('b', 5, '2024-01-01T05:00:00.000000Z'), (NULL, 6, '2024-01-01T06:00:00.000000Z'),
                            ('a', 7, '2024-01-01T07:00:00.000000Z'), ('b', 8, '2024-01-01T08:00:00.000000Z'),
                            ('c', 9, '2024-01-01T09:00:00.000000Z'), ('a', 10, '2024-01-01T10:00:00.000000Z'),
                            ('b', 11, '2024-01-01T11:00:00.000000Z'), ('c', 12, '2024-01-01T12:00:00.000000Z'),
                            ('a', 21, '2024-01-02T01:00:00.000000Z'), ('b', 22, '2024-01-02T02:00:00.000000Z'),
                            ('c', 23, '2024-01-02T03:00:00.000000Z'), ('a', 24, '2024-01-02T04:00:00.000000Z'),
                            ('b', 25, '2024-01-02T05:00:00.000000Z'), ('c', 26, '2024-01-02T06:00:00.000000Z'),
                            ('a', 27, '2024-01-02T07:00:00.000000Z'), ('b', 28, '2024-01-02T08:00:00.000000Z'),
                            ('a', 99, '2024-01-03T00:00:00.000000Z')
                            """
            );
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("pt");

            // the first partition takes the update, 'a' goes to fresh index rows
            isArmed.set(true);
            execute(
                    """
                            INSERT INTO pt VALUES
                            ('a', 100, '2024-01-01T06:30:00.000000Z'),
                            ('b', 101, '2024-01-02T06:30:00.000000Z')
                            """
            );
            drainWalQueue();
            isArmed.set(false);
            Assert.assertTrue("failed update must suspend the table", engine.getTableSequencerAPI().isSuspended(tableToken));

            // skip the failed transaction, then update the first partition again: the same
            // row group gets the same fresh index rows, now holding a different key
            final long failedSeqTxn = engine.getTableSequencerAPI().lastTxn(tableToken);
            execute("ALTER TABLE pt RESUME WAL FROM TXN " + (failedSeqTxn + 1));
            assertIsolatedAcrossCommit("INSERT INTO pt VALUES ('c', 200, '2024-01-01T06:30:00.000000Z')", true);
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertQuery("SELECT count() FROM pt WHERE s = 'a' AND ts IN '" + PARTITION + "'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n4\n");
        });
    }

    @Test
    public void testSuccessiveInPlaceUpdatesWithPinnedReaders() throws Exception {
        assertMemoryLeak(() -> {
            createParquetPartition("", true);
            final long nameTxn = getPartitionNameTxn();
            final ObjList<ObjList<PinnedQuery>> pinnedPerVersion = new ObjList<>();
            final String[] commits = {
                    "INSERT INTO pt VALUES ('a', 100, '2024-01-01T06:30:00.000000Z')",
                    "INSERT INTO pt VALUES ('b', 101, '2024-01-01T00:30:00.000000Z')",
                    "INSERT INTO pt VALUES (NULL, 102, '2024-01-01T10:30:00.000000Z'), ('c', 103, '2024-01-01T12:30:00.000000Z')",
                    "INSERT INTO pt VALUES ('a', 104, '2024-01-01T06:45:00.000000Z')",
            };
            try {
                for (String commit : commits) {
                    pinnedPerVersion.add(openPinnedQueries());
                    final long indexRowCount = getIndexRowCount();
                    execute(commit);
                    drainWalQueue();
                    Assert.assertEquals("O3 merge must update the parquet partition in place", nameTxn, getPartitionNameTxn());
                    Assert.assertTrue("index rows only grow", getIndexRowCount() > indexRowCount);
                    // every reader keeps its own version, the older ones included
                    for (int v = 0, n = pinnedPerVersion.size(); v < n; v++) {
                        assertPinnedSnapshots(pinnedPerVersion.getQuick(v));
                    }
                    assertIndexQueriesMatchTruth();
                }
            } finally {
                for (int v = 0, n = pinnedPerVersion.size(); v < n; v++) {
                    Misc.freeObjList(pinnedPerVersion.getQuick(v));
                }
            }
            assertIndexQueriesMatchTruth();
        });
    }

    private static void assertPinnedSnapshots(ObjList<PinnedQuery> pinned) {
        for (int i = 0, n = pinned.size(); i < n; i++) {
            pinned.getQuick(i).assertSnapshot();
        }
    }

    private static long getIndexRowCount() {
        try (TableReader reader = getReader("pt")) {
            reader.openPartition(0);
            return reader.getAndInitParquetPartitionDecoder(0).metadata().getIndexRowCount();
        }
    }

    private static long getPartitionNameTxn() {
        try (TableReader reader = getReader("pt")) {
            Assert.assertTrue(reader.getTxFile().isPartitionParquet(0));
            return reader.getTxFile().getPartitionNameTxn(0);
        }
    }

    private static boolean hasIndexRowMap() {
        try (TableReader reader = getReader("pt")) {
            reader.openPartition(0);
            return reader.getAndInitParquetPartitionDecoder(0).metadata().hasIndexRowMap();
        }
    }

    private static String indexQuery(int key, int order) {
        return "SELECT s, v, ts FROM pt WHERE " + KEY_FILTERS[key] + " AND ts IN '" + PARTITION + "'" + ORDERS[order];
    }

    private static String queryToString(String sql) throws Exception {
        final StringSink sink = new StringSink();
        printSql(sql, sink);
        return sink.toString();
    }

    private static boolean isPartitionCompact() {
        try (TableReader reader = getReader("pt")) {
            reader.openPartition(0);
            return PartitionCompactionScanJob.isParquetPartitionCompact(
                    reader.getTxFile(),
                    0,
                    reader.getMetadata(),
                    reader.getAndInitParquetPartitionDecoder(0).metadata()
            );
        }
    }

    private static String sortedLines(String text) {
        // -1 keeps the empty lines a NULL key prints; the one past the final new line is dropped first
        final String[] lines = (text.endsWith("\n") ? text.substring(0, text.length() - 1) : text).split("\n", -1);
        Arrays.sort(lines, 1, lines.length);
        return String.join("\n", lines);
    }

    private static String truth(int key, int order) throws Exception {
        return queryToString("SELECT s, v, ts FROM pt WHERE " + TRUTH_FILTERS[key] + " AND ts IN '" + PARTITION + "'" + ORDERS[order]);
    }

    // The factory-property battery of assertIndexQueriesMatchTruth does not apply once an ALTER changes the type
    // of a column a covering index includes: the index no longer covers it.
    private void assertKeyQueriesMatchTruth() throws Exception {
        for (int key = 0; key < KEY_FILTERS.length; key++) {
            for (int order = 0; order < ORDERS.length; order++) {
                final String sql = indexQuery(key, order);
                assertQuery(sql).noLeakCheck().assertsPlanContaining("on: s");
                TestUtils.assertEquals(truth(key, order), queryToString(sql));
            }
        }
    }

    private void assertMultiKeyQueriesMatchTruth() throws Exception {
        // multi-key lookups and DISTINCT read the index of every key at once
        final String partitionFilter = " FROM pt WHERE ts IN '" + PARTITION + "' AND ";
        TestUtils.assertEquals(
                sortedLines(queryToString("SELECT s, v, ts" + partitionFilter + "s::STRING IN ('a', 'c')")),
                sortedLines(queryToString("SELECT s, v, ts" + partitionFilter + "s IN ('a', 'c')"))
        );
        TestUtils.assertEquals(
                sortedLines(queryToString("SELECT s, v, ts" + partitionFilter + "(s::STRING IN ('b') OR s IS NULL)")),
                sortedLines(queryToString("SELECT s, v, ts" + partitionFilter + "s IN ('b', NULL)"))
        );
        TestUtils.assertEquals("s\n\na\nb\nc", sortedLines(queryToString("SELECT DISTINCT s FROM pt")));
    }

    private void assertRequestedCompactionAfterInPlaceUpdateAndAlterColumnType(boolean isWal) throws Exception {
        // An ALTER COLUMN TYPE over a parquet partition whose in-place update gave it an index row map: the
        // requested rebuild re-encodes the file under the identity map and indexes it afresh. This is how
        // Enterprise cold storage brings such a partition to the current schema before it uploads it.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, "-1");
        assertMemoryLeak(() -> {
            createParquetPartition("", true, isWal);
            execute("INSERT INTO pt VALUES ('b', 100, '2024-01-01T00:30:00.000000Z'), ('a', 101, '2024-01-01T06:30:00.000000Z')");
            drainWalQueue();
            Assert.assertTrue(hasIndexRowMap());
            Assert.assertFalse("a partition with an index row map is not compact", isPartitionCompact());
            execute("ALTER TABLE pt ALTER COLUMN v TYPE LONG");
            drainWalQueue();
            assertKeyQueriesMatchTruth();

            final long nameTxn = getPartitionNameTxn();
            engine.releaseInactive();
            final TableToken tableToken = engine.verifyTableName("pt");
            Assert.assertEquals(isWal, tableToken.isWal());
            final ObjList<PinnedQuery> pinned = openPinnedQueries();
            try {
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(tableToken, PARTITION_TIMESTAMP));
                try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                    Assert.assertTrue("the request must dispatch the rebuild", job.run());
                }
                Assert.assertNotEquals("the request must rebuild the partition", nameTxn, getPartitionNameTxn());
                Assert.assertFalse(hasIndexRowMap());
                Assert.assertTrue(isPartitionCompact());
                assertPinnedSnapshots(pinned);
            } finally {
                Misc.freeObjList(pinned);
            }
            assertKeyQueriesMatchTruth();
            assertMultiKeyQueriesMatchTruth();
        });
    }

    private void assertIndexQueriesMatchTruth() throws Exception {
        for (int key = 0; key < KEY_FILTERS.length; key++) {
            for (int order = 0; order < ORDERS.length; order++) {
                final String sql = indexQuery(key, order);
                assertQuery(sql).noLeakCheck().assertsPlanContaining("on: s");
                if (order == 0) {
                    assertQuery(sql)
                            .noLeakCheck()
                            .supportsRandomAccess(!isCovering)
                            .expectSize(isCovering)
                            .timestamp("ts")
                            .returns(truth(key, order));
                } else {
                    assertQuery(sql)
                            .noLeakCheck()
                            .expectSize(isCovering)
                            .timestampDesc("ts")
                            .returns(truth(key, order));
                }
            }
        }
    }

    private void assertIsolatedAcrossCommit(String o3Insert, boolean isInPlaceExpected) throws Exception {
        final long nameTxn = getPartitionNameTxn();
        final long indexRowCount = getIndexRowCount();
        final ObjList<PinnedQuery> pinned = openPinnedQueries();
        try {
            execute(o3Insert);
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
            if (isInPlaceExpected) {
                Assert.assertEquals("O3 merge must update the parquet partition in place", nameTxn, getPartitionNameTxn());
                // the indexes only grow: kept row groups keep their index rows
                Assert.assertTrue("index rows only grow", getIndexRowCount() > indexRowCount);
            } else {
                Assert.assertNotEquals("O3 merge must rewrite the parquet partition", nameTxn, getPartitionNameTxn());
            }
            assertPinnedSnapshots(pinned);
        } finally {
            Misc.freeObjList(pinned);
        }
        assertIndexQueriesMatchTruth();
    }

    private void createParquetPartition(String dedupClause, boolean isIndexed) throws Exception {
        createParquetPartition(dedupClause, isIndexed, true);
    }

    private void createParquetPartition(String dedupClause, boolean isIndexed, boolean isWal) throws Exception {
        execute("CREATE TABLE pt (s SYMBOL " + (isIndexed ? indexClause : "") + ", v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY "
                + (isWal ? "WAL" : "BYPASS WAL") + dedupClause);
        execute(
                """
                        INSERT INTO pt VALUES
                        ('a', 1, '2024-01-01T01:00:00.000000Z'), ('b', 2, '2024-01-01T02:00:00.000000Z'),
                        ('c', 3, '2024-01-01T03:00:00.000000Z'), ('a', 4, '2024-01-01T04:00:00.000000Z'),
                        ('b', 5, '2024-01-01T05:00:00.000000Z'), (NULL, 6, '2024-01-01T06:00:00.000000Z'),
                        ('a', 7, '2024-01-01T07:00:00.000000Z'), ('b', 8, '2024-01-01T08:00:00.000000Z'),
                        ('c', 9, '2024-01-01T09:00:00.000000Z'), ('a', 10, '2024-01-01T10:00:00.000000Z'),
                        ('b', 11, '2024-01-01T11:00:00.000000Z'), ('c', 12, '2024-01-01T12:00:00.000000Z'),
                        ('a', 99, '2024-01-02T00:00:00.000000Z')
                        """
        );
        drainWalQueue();
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '" + PARTITION + "'");
        drainWalQueue();
        if (isIndexed) {
            assertIndexQueriesMatchTruth();
        }
    }

    private ObjList<PinnedQuery> openPinnedQueries() throws Exception {
        final ObjList<PinnedQuery> pinned = new ObjList<>();
        try {
            for (int key = 0; key < KEY_FILTERS.length; key++) {
                for (int order = 0; order < ORDERS.length; order++) {
                    pinned.add(new PinnedQuery(indexQuery(key, order), truth(key, order)));
                }
            }
        } catch (Throwable th) {
            Misc.freeObjList(pinned);
            throw th;
        }
        return pinned;
    }

    /**
     * An index-driven cursor opened before an O3 commit. The cursor reads its first row
     * before the commit, so its index reader maps the index files of the snapshot.
     */
    private static class PinnedQuery implements Closeable {
        private final RecordCursor cursor;
        private final String expected;
        private final RecordCursorFactory factory;
        private final String head;
        private final StringSink sink = new StringSink();
        private final String sql;
        private boolean isFirstPassDone;

        PinnedQuery(String sql, String expected) throws Exception {
            this.sql = sql;
            this.expected = expected;
            RecordCursorFactory factory = null;
            RecordCursor cursor = null;
            try {
                factory = select(sql);
                cursor = factory.getCursor(sqlExecutionContext);
                sink.clear();
                CursorPrinter.println(factory.getMetadata(), sink);
                if (cursor.hasNext()) {
                    CursorPrinter.println(cursor.getRecord(), factory.getMetadata(), sink);
                }
                this.head = sink.toString();
            } catch (Throwable th) {
                Misc.free(cursor);
                Misc.free(factory);
                throw th;
            }
            this.factory = factory;
            this.cursor = cursor;
        }

        @Override
        public void close() {
            Misc.free(cursor);
            Misc.free(factory);
        }

        void assertSnapshot() {
            if (!isFirstPassDone) {
                // finish the pass that started before the commit
                sink.clear();
                sink.put(head);
                while (cursor.hasNext()) {
                    CursorPrinter.println(cursor.getRecord(), factory.getMetadata(), sink);
                }
                TestUtils.assertEquals(sql, expected, sink);
                isFirstPassDone = true;
            }
            // another pass over the same snapshot
            cursor.toTop();
            TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
        }
    }
}
