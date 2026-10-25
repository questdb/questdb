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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A WAL dedup commit that sorts against rows held in LAG maps the LAG rows of every dedup key
 * (TableWriter.deduplicateSortedIndex()). These tests check that such a commit dedups the same rows
 * as a commit without LAG, and fail the mapping, or its release.
 * <p>
 * Unless a test says otherwise, it applies one WAL transaction at a time. The first commit creates
 * the partition, and its partition task takes a dedup column block that stays allocated. The second
 * commit appends, and its dedup call clears the block list. Append commits take no block, so every
 * later dedup call gets the same block. In the failure tests, each later pair of transactions parks
 * rows in LAG and commits them: the second transaction of a pair starts half a millisecond after the
 * first, and rows are a millisecond apart, so the first stays in LAG and no row of the pair
 * duplicates another.
 */
public class WalDedupLagMapTest extends AbstractCairoTest {
    // Rows of an 8-byte column per append page; the tests make append pages as small as the OS allows.
    private static final long ROWS_PER_PAGE = Files.PAGE_SIZE / Long.BYTES;
    private static final String[] LAG_ROUND_FROM = {"2024-01-01T02:00:00", "2024-01-01T05:00:00"};
    private static final long[] LAG_ROUND_ROWS = {3 * ROWS_PER_PAGE, 10};

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // LAG rows that span append pages need a fresh mapping, and that mapping can fail.
        node1.setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        // Merge-append tables take no LAG.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
        // Apply one WAL transaction per run, so a transaction followed by an earlier one stays in LAG.
        node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 0);
    }

    @Test
    public void testDedupOfLagRowsMatchesDedupWithoutLag() throws Exception {
        assertMemoryLeak(() -> {
            final String columns = " (k LONG, v VARCHAR, s STRING, val INT, ts TIMESTAMP)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k, v, s)";
            execute("CREATE TABLE t_lag" + columns);
            execute("CREATE TABLE t_ref" + columns);
            final TableToken lagToken = engine.verifyTableName("t_lag");

            // t_lag applies one transaction at a time, so each round parks its first two transactions in LAG
            // and its third commits them. The first round's LAG rows span pages, the second round's fit in one.
            insertKeyRows("t_lag", "2024-01-01T00:00:00", 1_000, 10, 0, 1);
            applyOneWalTxn(lagToken);
            insertKeyRows("t_lag", "2024-01-01T01:00:00", 1_000, ROWS_PER_PAGE, 0, 1);
            applyOneWalTxn(lagToken);
            for (int round = 0; round < LAG_ROUND_ROWS.length; round++) {
                insertLagRound("t_lag", round);
                applyOneWalTxn(lagToken);
                Assert.assertEquals(LAG_ROUND_ROWS[round], lagRowCount(lagToken));
                applyOneWalTxn(lagToken);
                applyOneWalTxn(lagToken);
                Assert.assertEquals(0, lagRowCount(lagToken));
            }

            // t_ref applies the same transactions together
            insertKeyRows("t_ref", "2024-01-01T00:00:00", 1_000, 10, 0, 1);
            insertKeyRows("t_ref", "2024-01-01T01:00:00", 1_000, ROWS_PER_PAGE, 0, 1);
            for (int round = 0; round < LAG_ROUND_ROWS.length; round++) {
                insertLagRound("t_ref", round);
            }
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(lagToken));
            assertSqlCursors("t_ref", "t_lag");
            long replacedRows = 0;
            for (long lagRows : LAG_ROUND_ROWS) {
                replacedRows += Math.max(1, lagRows / 100);
            }
            assertQuery("SELECT val, count() FROM t_lag ORDER BY val")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            val\tcount
                            1\t%d
                            2\t%d
                            3\t%d
                            """.formatted(
                            10 + ROWS_PER_PAGE + 3 * ROWS_PER_PAGE + 10 - replacedRows,
                            replacedRows,
                            10 * LAG_ROUND_ROWS.length
                    ));
        });
    }

    @Test
    public void testFailedLagMapKeepsItsErrorWhenAReleaseFails() throws Exception {
        final LagMapFacade facade = new LagMapFacade("k1.d", "k2.d");
        ff = facade;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k1 LONG, k2 LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k1, k2)");
            final TableToken token = engine.verifyTableName("t");
            insertAppendedRows(token, "x, x");

            insert("x, x", "2024-01-01T02:00:00", 3 * ROWS_PER_PAGE);
            insert("x, x", "2024-01-01T02:00:00.000500", 10);
            applyOneWalTxn(token);
            Assert.assertEquals(3 * ROWS_PER_PAGE, lagRowCount(token));
            // The commit maps the LAG rows of k1, fails to map those of k2, and fails to release those of k1.
            facade.failNextMap("k2.d");
            facade.failNextUnmap("k1.d");
            applyOneWalTxn(token);
            Assert.assertEquals(1, facade.failedMaps.get());
            Assert.assertEquals(1, facade.failedUnmaps.get());

            TestUtils.assertContains(engine.getTableSequencerAPI().getTxnTracker(token).getErrorMessage(), "could not mmap");
            Assert.assertTrue("the failed commit leaked LAG mappings [address=len]: " + facade.liveMaps, facade.liveMaps.isEmpty());
        });
    }

    @Test
    public void testFailedLagMapOfFixedKeyUnmapsOnlyItsOwnMappings() throws Exception {
        final LagMapFacade facade = new LagMapFacade("k.d");
        ff = facade;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k)");
            final TableToken token = engine.verifyTableName("t");
            insertAppendedRows(token, "x");

            insert("x", "2024-01-01T02:00:00", 3 * ROWS_PER_PAGE);
            insert("x", "2024-01-01T02:00:00.000500", 10);
            applyOneWalTxn(token);
            Assert.assertEquals(3 * ROWS_PER_PAGE, lagRowCount(token));
            applyOneWalTxn(token);
            Assert.assertTrue("the commit must map the LAG rows of the key", facade.maps.get() > 0);

            insert("x", "2024-01-01T05:00:00", 3 * ROWS_PER_PAGE);
            insert("x", "2024-01-01T05:00:00.000500", 10);
            applyOneWalTxn(token);
            facade.failNextMap("k.d");
            applyOneWalTxn(token);
            Assert.assertEquals(1, facade.failedMaps.get());

            Assert.assertEquals("the failed commit unmapped LAG mappings of an earlier commit", 0, facade.staleUnmaps.get());
        });
    }

    @Test
    public void testFailedLagMapOfVarKeyReleasesAuxMapping() throws Exception {
        final LagMapFacade facade = new LagMapFacade("v.i", "v.d");
        ff = facade;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, v)");
            final TableToken token = engine.verifyTableName("t");
            // Longer than 9 bytes, so VARCHAR stores it in the data vector.
            final String value = "'value_in_the_data_vector'::VARCHAR";
            insert(value, "2024-01-01T00:00:00", 10);
            applyOneWalTxn(token);
            insert(value, "2024-01-01T01:00:00", 10);
            applyOneWalTxn(token);

            // The LAG rows of this commit sit in the mapped pages, so the dedup column block keeps
            // positive addresses, and the failed commit below has nothing stale to unmap.
            insert(value, "2024-01-01T02:00:00", 10);
            // Also puts the next LAG past the first aux page, so its aux mapping stays out of the mmap cache.
            insert(value, "2024-01-01T02:00:00.000500", ROWS_PER_PAGE / 2);
            applyOneWalTxn(token);
            Assert.assertEquals(10, lagRowCount(token));
            applyOneWalTxn(token);
            Assert.assertEquals("the LAG rows of the commit must sit in the mapped pages", 0, facade.maps.get());

            insert(value, "2024-01-01T05:00:00", 3 * ROWS_PER_PAGE);
            insert(value, "2024-01-01T05:00:00.000500", 10);
            applyOneWalTxn(token);
            facade.failNextMap("v.d");
            applyOneWalTxn(token);
            Assert.assertEquals(1, facade.failedMaps.get());
            Assert.assertEquals("the failed commit must map the aux vector of the LAG rows", 1, facade.maps.get());

            Assert.assertTrue("the failed commit leaked LAG mappings [address=len]: " + facade.liveMaps, facade.liveMaps.isEmpty());
        });
    }

    @Test
    public void testFailedReleaseOfLagMappingReleasesTheNextKey() throws Exception {
        final LagMapFacade facade = new LagMapFacade("k1.d", "k2.d");
        ff = facade;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k1 LONG, k2 LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k1, k2)");
            final TableToken token = engine.verifyTableName("t");
            insertAppendedRows(token, "x, x");

            insert("x, x", "2024-01-01T02:00:00", 3 * ROWS_PER_PAGE);
            insert("x, x", "2024-01-01T02:00:00.000500", 10);
            applyOneWalTxn(token);
            Assert.assertEquals(3 * ROWS_PER_PAGE, lagRowCount(token));
            // The dedup succeeds, and the release of the LAG mapping of k1 fails.
            facade.failNextUnmap("k1.d");
            applyOneWalTxn(token);
            Assert.assertEquals(1, facade.failedUnmaps.get());

            TestUtils.assertContains(engine.getTableSequencerAPI().getTxnTracker(token).getErrorMessage(), "injected munmap failure");
            Assert.assertTrue("the commit leaked LAG mappings [address=len]: " + facade.liveMaps, facade.liveMaps.isEmpty());
        });
    }

    private static void applyOneWalTxn(TableToken token) {
        engine.getTableSequencerAPI().getTxnTracker(token).getMemPressureControl().setMaxBlockRowCount(1);
        try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
            walApplyJob.run();
        }
    }

    private static void insert(String values, String fromTimestamp, long rowCount) throws Exception {
        execute("INSERT INTO t SELECT " + values + ", timestamp_sequence('" + fromTimestamp + "', 1_000) FROM long_sequence(" + rowCount + ")");
    }

    // Commits a partition-creating transaction, then an appending one that puts the LAG of the next commits past
    // the first page of an 8-byte column, so their mappings stay out of the mmap cache.
    private static void insertAppendedRows(TableToken token, String values) throws Exception {
        insert(values, "2024-01-01T00:00:00", 10);
        applyOneWalTxn(token);
        insert(values, "2024-01-01T01:00:00", ROWS_PER_PAGE);
        applyOneWalTxn(token);
        Assert.assertEquals(0, lagRowCount(token));
    }

    // Row x gets key keyShift + (x - 1) * stepMicros / 1000 + 1, so at the same timestamps, rows a step of 100ms
    // apart repeat the keys of every 100th row of rows 1ms apart.
    private static void insertKeyRows(String table, String fromTimestamp, long stepMicros, long rowCount, long keyShift, int val) throws Exception {
        final String key = "(" + keyShift + " + (x - 1) * " + stepMicros / 1_000 + " + 1)";
        execute("INSERT INTO " + table + " SELECT " + key + ", ('v_' || " + key + " || '_in_the_data_vector')::VARCHAR, 's_' || "
                + key + ", " + val + ", timestamp_sequence('" + fromTimestamp + "', " + stepMicros + ") FROM long_sequence(" + rowCount + ")");
    }

    // Three transactions: rows 1ms apart, rows that repeat the keys and timestamps of every 100th of them with
    // another val, and rows of their own half a millisecond off the first.
    private static void insertLagRound(String table, int round) throws Exception {
        final String from = LAG_ROUND_FROM[round];
        insertKeyRows(table, from, 1_000, LAG_ROUND_ROWS[round], 0, 1);
        insertKeyRows(table, from, 100_000, Math.max(1, LAG_ROUND_ROWS[round] / 100), 0, 2);
        insertKeyRows(table, from + ".000500", 1_000, 10, 1_000_000, 3);
    }

    private static long lagRowCount(TableToken token) {
        try (TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade())) {
            txReader.ofRO(Path.getThreadLocal(root).concat(token).concat(TableUtils.TXN_FILE_NAME).$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
            txReader.unsafeLoadAll();
            return txReader.getLagRowCount();
        }
    }

    /**
     * Tracks the read-only MMAP_TABLE_WRITER mappings of the given column files, which is how
     * TableWriter.mapAppendColumnBuffer() maps LAG rows, and fails the next such mapping, or the
     * release of the next such mapping, of one file on request. A failed release unmaps first.
     */
    private static class LagMapFacade extends TestFilesFacadeImpl {
        final AtomicInteger failedMaps = new AtomicInteger();
        final AtomicInteger failedUnmaps = new AtomicInteger();
        // address -> length
        final ConcurrentHashMap<Long, Long> liveMaps = new ConcurrentHashMap<>();
        final AtomicInteger maps = new AtomicInteger();
        final AtomicInteger staleUnmaps = new AtomicInteger();
        private final AtomicBoolean failNextMap = new AtomicBoolean();
        private final AtomicBoolean failNextUnmap = new AtomicBoolean();
        // fd -> file name
        private final ConcurrentHashMap<Long, String> fdFileNames = new ConcurrentHashMap<>();
        private final String[] fileNames;
        // address -> file name
        private final ConcurrentHashMap<Long, String> liveMapFileNames = new ConcurrentHashMap<>();
        // address -> length
        private final ConcurrentHashMap<Long, Long> releasedMaps = new ConcurrentHashMap<>();
        private volatile String failMapFileName;
        private volatile String failUnmapFileName;

        LagMapFacade(String... fileNames) {
            this.fileNames = fileNames;
        }

        @Override
        public boolean close(long fd) {
            fdFileNames.remove(fd);
            return super.close(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            final String fileName = flags == Files.MAP_RO && memoryTag == MemoryTag.MMAP_TABLE_WRITER ? fdFileNames.get(fd) : null;
            if (fileName == null) {
                return super.mmap(fd, len, offset, flags, memoryTag);
            }
            if (fileName.equals(failMapFileName) && failNextMap.compareAndSet(true, false)) {
                failedMaps.incrementAndGet();
                return FilesFacade.MAP_FAILED;
            }
            final long address = super.mmap(fd, len, offset, flags, memoryTag);
            if (address != FilesFacade.MAP_FAILED) {
                maps.incrementAndGet();
                liveMaps.put(address, len);
                liveMapFileNames.put(address, fileName);
                releasedMaps.remove(address);
            }
            return address;
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            if (memoryTag == MemoryTag.MMAP_TABLE_WRITER) {
                if (liveMaps.remove(address, size)) {
                    final String fileName = liveMapFileNames.remove(address);
                    releasedMaps.put(address, size);
                    super.munmap(address, size, memoryTag);
                    if (fileName.equals(failUnmapFileName) && failNextUnmap.compareAndSet(true, false)) {
                        failedUnmaps.incrementAndGet();
                        throw CairoException.critical(0).put("injected munmap failure");
                    }
                    return;
                }
                if (failedMaps.get() > 0 && releasedMaps.remove(address, size)) {
                    // The range may belong to another mapping by now, leave it mapped.
                    staleUnmaps.incrementAndGet();
                    return;
                }
            }
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (fd > -1) {
                for (String fileName : fileNames) {
                    if (Utf8s.endsWithAscii(name, Files.SEPARATOR + fileName)) {
                        fdFileNames.put(fd, fileName);
                    }
                }
            }
            return fd;
        }

        void failNextMap(String fileName) {
            failMapFileName = fileName;
            failNextMap.set(true);
        }

        void failNextUnmap(String fileName) {
            failUnmapFileName = fileName;
            failNextUnmap.set(true);
        }
    }
}
