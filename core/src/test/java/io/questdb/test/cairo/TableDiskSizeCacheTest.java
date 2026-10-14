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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableDiskSizeCache;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TableDiskSizeCacheTest extends AbstractCairoTest {
    // partitions of the tables createDailyTable() creates
    private static final String[] PARTITIONS = {"2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"};

    @Test
    public void testCachesSealedPartitions() throws Exception {
        final WalkRecordingFilesFacade ff = new WalkRecordingFilesFacade();
        assertMemoryLeak(ff, () -> {
            createDailyTable("x", false);
            setClockPastRacyWindow();

            assertDiskSize("x");
            for (String partition : PARTITIONS) {
                Assert.assertEquals(partition, 1, ff.countWalks(partition));
            }

            ff.walkedDirs.clear();
            assertDiskSize("x");
            for (int i = 0; i < PARTITIONS.length - 1; i++) {
                Assert.assertEquals(PARTITIONS[i], 0, ff.countWalks(PARTITIONS[i]));
            }
            // appends land in the last partition, so it is measured on every call
            Assert.assertEquals(1, ff.countWalks("2024-01-05"));
            Assert.assertEquals(PARTITIONS.length, engine.getTableDiskSizeCache().getPartitionCount(token("x")));
        });
    }

    @Test
    public void testConcurrentMeasurementsAndDdlLeaveNoDroppedTables() throws Exception {
        assertMemoryLeak(() -> {
            createSystemTables();
            createDailyTable("stable", false);
            final TableDiskSizeCache cache = engine.getTableDiskSizeCache();
            final int pollerCount = 3;
            final AtomicBoolean isStopped = new AtomicBoolean();
            final AtomicLong completedPolls = new AtomicLong();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final ObjList<Thread> pollers = new ObjList<>();
            for (int t = 0; t < pollerCount; t++) {
                // pollers measure every table, or one table by name, while tables come and go
                final String sql = t == 0
                        ? "SELECT diskSize FROM table_storage()"
                        : "SELECT diskSize FROM table_storage() WHERE tableName = 'churn" + t + "'";
                final Thread poller = new Thread(() -> {
                    try (
                            SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine);
                            RecordCursorFactory factory = engine.select(sql, executionContext)
                    ) {
                        while (!isStopped.get()) {
                            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                                while (cursor.hasNext()) {
                                    cursor.getRecord().getLong(0);
                                }
                                completedPolls.incrementAndGet();
                            } catch (CairoException e) {
                                // Non-WAL DDL locks the table, so table_storage() fails the poll while the
                                // DDL holds the lock and the table files still exist, with or without the
                                // cache. The test checks the cache state.
                                if (!Chars.contains(e.getFlyweightMessage(), "table busy")) {
                                    throw e;
                                }
                            }
                        }
                    } catch (Throwable th) {
                        error.compareAndSet(null, th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
                pollers.add(poller);
                poller.start();
            }

            try {
                for (int i = 0; i < 60; i++) {
                    final String name = "churn" + (1 + i % (pollerCount - 1));
                    final boolean isWal = i % 2 == 0;
                    execute("CREATE TABLE " + name + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
                    execute("INSERT INTO " + name + " SELECT timestamp_sequence('2024-01-01', 8_640_000_000L), x FROM long_sequence(30)");
                    if (isWal) {
                        drainWalQueue();
                    }
                    if (i % 3 == 0) {
                        executeRetryingBusy("RENAME TABLE " + name + " TO " + name + "_renamed");
                        executeRetryingBusy("DROP TABLE " + name + "_renamed");
                    } else {
                        executeRetryingBusy("DROP TABLE " + name);
                    }
                }
            } finally {
                isStopped.set(true);
                for (int i = 0, n = pollers.size(); i < n; i++) {
                    pollers.getQuick(i).join();
                }
            }
            if (error.get() != null) {
                throw new AssertionError(error.get());
            }
            Assert.assertTrue(completedPolls.get() > 0);

            drainWalQueue();
            drainPurgeJob();
            // no query runs anymore: stable is the only table the cache may still hold
            Assert.assertEquals(cache.getPartitionCount(token("stable")) > 0 ? 1 : 0, cache.getTableCount());
            execute("DROP TABLE stable");
            Assert.assertEquals(0, cache.getTableCount());
        });
    }

    @Test
    public void testConcurrentMeasurementsDuringWrites() throws Exception {
        assertMemoryLeak(() -> {
            createDailyTable("x", false);
            createDailyTable("y", true);
            setClockPastRacyWindow();

            final int threadCount = 4;
            final int iterations = 30;
            final CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final ObjList<Thread> threads = new ObjList<>();
            for (int t = 0; t < threadCount; t++) {
                final Thread thread = new Thread(() -> {
                    try {
                        barrier.await();
                    } catch (Throwable th) {
                        error.compareAndSet(null, th);
                        return;
                    }
                    try (
                            SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine);
                            RecordCursorFactory factory = engine.select("SELECT * FROM table_storage()", executionContext)
                    ) {
                        for (int i = 0; i < iterations; i++) {
                            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                                final Record record = cursor.getRecord();
                                while (cursor.hasNext()) {
                                    Assert.assertNotNull(record.getStrA(2));
                                    Assert.assertTrue(record.getLong(4) > 0);
                                    Assert.assertTrue(record.getLong(5) > 0);
                                }
                            }
                        }
                    } catch (Throwable th) {
                        error.compareAndSet(null, th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
                threads.add(thread);
                thread.start();
            }

            barrier.await();
            // appends and out-of-order inserts into sealed partitions race with the measurements
            for (int i = 0; i < 20; i++) {
                execute("INSERT INTO x VALUES ('" + PARTITIONS[i % PARTITIONS.length] + "T12:00:00.000000Z', 'z', " + i + ", 'v')");
            }
            for (int i = 0, n = threads.size(); i < n; i++) {
                threads.getQuick(i).join();
            }
            if (error.get() != null) {
                throw new AssertionError(error.get());
            }

            engine.releaseAllWriters();
            assertDiskSize("x");
            assertDiskSize("y");
        });
    }

    @Test
    public void testDetectsAddIndexMeasuredMidway() throws Exception {
        // ADD INDEX creates the index files of a partition and then fills them in place: the
        // directory keeps its name, row count and modification time. A call that measures the
        // partition in between caches a partial size.
        final InterleavingFilesFacade ff = new InterleavingFilesFacade();
        assertMemoryLeak(ff, () -> {
            createDailyTable("x", false);
            setClockPastRacyWindow();

            // the indexer reads the symbol column of 2024-01-02 after it created its index files
            ff.onRead(Files.SEPARATOR + "2024-01-02" + Files.SEPARATOR + "sym.d", () -> measureDiskSizeOnThisThread("x"));
            execute("ALTER TABLE x ALTER COLUMN sym ADD INDEX");
            ff.assertFired();
            engine.releaseAllWriters();
            assertDiskSize("x");
        });
    }

    @Test
    public void testDetectsDetachedPartition() throws Exception {
        assertMemoryLeak(() -> {
            createDailyTable("x", false);
            setClockPastRacyWindow();
            assertDiskSize("x");

            execute("ALTER TABLE x DETACH PARTITION LIST '2024-01-02'");
            // the detached directory stays in the table directory and keeps counting
            assertDiskSize("x");
            Assert.assertEquals(PARTITIONS.length - 1, engine.getTableDiskSizeCache().getPartitionCount(token("x")));
        });
    }

    @Test
    public void testDetectsDropAndRecreateOfLastPartitionMeasuredMidway() throws Exception {
        // like testDetectsDropOfLastPartitionMeasuredMidway, but a partition with the timestamp of
        // the dropped one replaces it
        assertDetectsLastPartitionRemovalMeasuredMidway(
                false,
                () -> executeOnThisThread("ALTER TABLE x DROP PARTITION LIST '2024-01-05'"),
                "2024-01-05"
        );
    }

    @Test
    public void testDetectsDropOfLastPartitionMeasuredMidway() throws Exception {
        // The drop of the last partition reopens the partition before it for appends and grows its
        // column files to an append page, then commits. A call that read _txn before the commit
        // measures the grown partition as sealed.
        assertDetectsLastPartitionRemovalMeasuredMidway(
                false,
                () -> executeOnThisThread("ALTER TABLE x DROP PARTITION LIST '2024-01-05'"),
                "2024-01-06"
        );
    }

    @Test
    public void testDetectsDroppedPartition() throws Exception {
        assertMemoryLeak(() -> {
            createDailyTable("x", false);
            setClockPastRacyWindow();
            assertDiskSize("x");

            execute("ALTER TABLE x DROP PARTITION LIST '2024-01-02'");
            // the directory of the dropped partition counts until the purge job removes it
            assertDiskSize("x");
            Assert.assertEquals(PARTITIONS.length - 1, engine.getTableDiskSizeCache().getPartitionCount(token("x")));
        });
    }

    @Test
    public void testDetectsInPlaceAppendToSealedPartition() throws Exception {
        assertMemoryLeak(() -> {
            createDailyTable("x", false);
            setClockPastRacyWindow();
            assertDiskSize("x");

            // Rows after the last row of a sealed partition extend its files in place: the
            // directory keeps its name and modification time, only the row count changes.
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-02T22:00:00.000000Z', 1_000_000L), 'z', x, 'abc' FROM long_sequence(3_000)");
            engine.releaseAllWriters();
            try (TableReader reader = engine.getReader("x")) {
                Assert.assertEquals(-1, reader.getTxFile().getPartitionNameTxn(1));
                Assert.assertEquals(3_010, reader.getTxFile().getPartitionSize(1));
            }
            assertDiskSize("x");
        });
    }

    @Test
    public void testDetectsInPlaceParquetUpdate() throws Exception {
        // O3PartitionJob updates a Parquet file with more than one row group in place
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, k SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k)");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-01', 8_000_000L), 'a', x FROM long_sequence(10_000)");
            // the cache measures the last partition on every call, keep 2024-01-01 sealed
            execute("INSERT INTO x VALUES ('2024-01-03T00:00:00.000000Z', 'a', 1)");
            drainWalQueue();
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            engine.releaseAllWriters();
            setClockPastRacyWindow();
            assertDiskSize("x");

            final long nameTxn;
            final long parquetFileSize;
            try (TableReader reader = engine.getReader("x")) {
                Assert.assertTrue(reader.getTxFile().isPartitionParquet(0));
                nameTxn = reader.getTxFile().getPartitionNameTxn(0);
                parquetFileSize = reader.getTxFile().getPartitionParquetFileSize(0);
            }

            // An upsert of existing keys merges a row group into the Parquet file in place: the
            // directory keeps its name, row count and modification time, only the Parquet file
            // size changes.
            execute("INSERT INTO x SELECT '2024-01-01T10:00:00.000000Z'::TIMESTAMP + (x - 1) * 8_000_000L, 'a', -x FROM long_sequence(20)");
            drainWalQueue();
            engine.releaseAllWriters();
            try (TableReader reader = engine.getReader("x")) {
                Assert.assertEquals(nameTxn, reader.getTxFile().getPartitionNameTxn(0));
                Assert.assertEquals(10_000, reader.getTxFile().getPartitionSize(0));
                Assert.assertTrue(reader.getTxFile().getPartitionParquetFileSize(0) > parquetFileSize);
            }
            assertDiskSize("x");
        });
    }

    @Test
    public void testDetectsNewFilesInSealedPartition() throws Exception {
        assertMemoryLeak(() -> {
            createDailyTable("x", false);
            setClockPastRacyWindow();
            assertDiskSize("x");

            // keep the directory modification time of the change apart from the cached one:
            // file systems take timestamps from a clock that ticks every few milliseconds
            Os.sleep(50);
            // the index files land next to the column files of every partition: the directories
            // keep their names and row counts, only their modification times change
            execute("ALTER TABLE x ALTER COLUMN sym ADD INDEX");
            engine.releaseAllWriters();
            assertDiskSize("x");
        });
    }

    @Test
    public void testDetectsOutOfOrderWriteIntoSealedPartition() throws Exception {
        assertMemoryLeak(() -> {
            createDailyTable("x", false);
            setClockPastRacyWindow();
            assertDiskSize("x");

            // a row inside the time range of a sealed partition rewrites it as a new partition version
            execute("INSERT INTO x VALUES ('2024-01-02T12:30:00.000000Z', 'z', 1, 'abc')");
            engine.releaseAllWriters();
            assertDiskSize("x");
        });
    }

    @Test
    public void testDetectsOutOfOrderWritesIntoSealedWalPartition() throws Exception {
        assertMemoryLeak(() -> {
            createDailyTable("x", true);
            setClockPastRacyWindow();
            assertDiskSize("x");

            execute("INSERT INTO x VALUES ('2024-01-02T12:30:00.000000Z', 'z', 1, 'abc')");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-03T22:00:00.000000Z', 1_000_000L), 'z', x, 'abc' FROM long_sequence(3_000)");
            drainWalQueue();
            engine.releaseAllWriters();
            assertDiskSize("x");
        });
    }

    @Test
    public void testDetectsParquetConversion() throws Exception {
        assertMemoryLeak(() -> {
            createDailyTable("x", false);
            setClockPastRacyWindow();
            assertDiskSize("x");

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
            engine.releaseAllWriters();
            assertDiskSize("x");
        });
    }

    @Test
    public void testDetectsReindexMeasuredMidway() throws Exception {
        // REINDEX rebuilds the index files in place and commits nothing to _txn
        final InterleavingFilesFacade ff = new InterleavingFilesFacade();
        assertMemoryLeak(ff, () -> {
            createDailyTable("x", false);
            execute("ALTER TABLE x ALTER COLUMN sym ADD INDEX");
            engine.releaseAllWriters();
            setClockPastRacyWindow();
            assertDiskSize("x");

            ff.onRead(Files.SEPARATOR + "2024-01-02" + Files.SEPARATOR + "sym.d", () -> measureDiskSizeOnThisThread("x"));
            execute("REINDEX TABLE x COLUMN sym LOCK EXCLUSIVE");
            ff.assertFired();
            assertDiskSize("x");
        });
    }

    @Test
    public void testDetectsReplaceCommitRemovingLastPartitionMeasuredMidway() throws Exception {
        // a replace commit that leaves the last partition empty removes it like a drop, but
        // without bumping the truncate version
        assertDetectsLastPartitionRemovalMeasuredMidway(
                true,
                () -> {
                    try (WalWriter walWriter = engine.getWalWriter(token("x"))) {
                        walWriter.commitWithParams(
                                MicrosTimestampDriver.floor("2024-01-05"),
                                MicrosTimestampDriver.floor("2024-01-06"),
                                WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                        );
                    }
                    drainWalQueue();
                },
                "2024-01-06"
        );
    }

    @Test
    public void testDetectsTruncate() throws Exception {
        assertMemoryLeak(() -> {
            createDailyTable("x", false);
            setClockPastRacyWindow();
            assertDiskSize("x");

            execute("TRUNCATE TABLE x");
            assertDiskSize("x");
            Assert.assertEquals(0, engine.getTableDiskSizeCache().getPartitionCount(token("x")));
        });
    }

    @Test
    public void testDoesNotCacheRecentlyModifiedPartitions() throws Exception {
        final WalkRecordingFilesFacade ff = new WalkRecordingFilesFacade();
        assertMemoryLeak(ff, () -> {
            createDailyTable("x", false);
            long oldestMtime = Long.MAX_VALUE;
            long youngestMtime = Long.MIN_VALUE;
            try (Path path = new Path()) {
                for (String partition : PARTITIONS) {
                    final long mtime = Files.getLastModified(tableDir(path, "x").concat(partition).$());
                    oldestMtime = Math.min(oldestMtime, mtime);
                    youngestMtime = Math.max(youngestMtime, mtime);
                }
            }

            // every partition directory changed within the racy window
            setCurrentMicros(oldestMtime * 1000L);
            assertDiskSize("x");
            ff.walkedDirs.clear();
            assertDiskSize("x");
            for (String partition : PARTITIONS) {
                Assert.assertEquals(partition, 1, ff.countWalks(partition));
            }

            // the window has passed for every partition directory
            setCurrentMicros((youngestMtime + TableDiskSizeCache.RACY_WINDOW_MILLIS) * 1000L);
            assertDiskSize("x");
            ff.walkedDirs.clear();
            assertDiskSize("x");
            Assert.assertEquals(0, ff.countWalks("2024-01-01"));
            Assert.assertEquals(1, ff.countWalks("2024-01-05"));
        });
    }

    @Test
    public void testDoesNotCacheTableDroppedWhileMeasured() throws Exception {
        final DropOnTxnOpenFilesFacade ff = new DropOnTxnOpenFilesFacade();
        assertMemoryLeak(ff, () -> {
            createDailyTable("unmeasured", false);
            createDailyTable("w", true);
            final TableDiskSizeCache cache = engine.getTableDiskSizeCache();

            // the query lists w, then the files facade drops w while the query opens its _txn:
            // the drop finds no entry to evict, and the query creates the entry after the drop
            ff.arm(token("w").getDirName(), "DROP TABLE w");
            try (
                    RecordCursorFactory factory = select("SELECT diskSize FROM table_storage() WHERE tableName = 'w'");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertTrue(cursor.hasNext());
                // the WAL table files stay until the purge, the query measures them
                Assert.assertTrue(cursor.getRecord().getLong(0) > 0);
            }
            Assert.assertTrue(ff.hasFired);
            if (ff.error != null) {
                throw new AssertionError(ff.error);
            }
            Assert.assertNull(engine.getTableTokenIfExists("w"));
            Assert.assertEquals(0, cache.getTableCount());
        });
    }

    @Test
    public void testDropAndRecreateUnderSameDirectoryName() throws Exception {
        configOverrideMangleTableDirNames(false);
        assertMemoryLeak(() -> {
            createDailyTable("x", false);
            setClockPastRacyWindow();
            assertDiskSize("x");

            execute("DROP TABLE x");
            execute("CREATE TABLE x (ts TIMESTAMP, sym SYMBOL, v LONG, s VARCHAR, extra DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 'a', 1, 'b', 1.5), ('2024-01-03T00:00:00.000000Z', 'a', 1, 'b', 2.5)");
            engine.releaseAllWriters();
            assertDiskSize("x");
            Assert.assertEquals(2, engine.getTableDiskSizeCache().getPartitionCount(token("x")));
        });
    }

    @Test
    public void testEvictsDirectoryRetiredByRename() throws Exception {
        assertMemoryLeak(() -> {
            createDailyTable("x", false);
            createDailyTable("w", true);
            final TableDiskSizeCache cache = engine.getTableDiskSizeCache();
            measureDiskSize("x");
            measureDiskSize("w");
            Assert.assertEquals(2, cache.getTableCount());

            // a WAL table keeps its directory, and so its cached partitions, across a rename
            final TableToken walToken = token("w");
            execute("RENAME TABLE w TO w2");
            Assert.assertEquals(walToken.getDirName(), token("w2").getDirName());
            Assert.assertEquals(PARTITIONS.length, cache.getPartitionCount(token("w2")));

            // a non-WAL table moves to a new directory, the old one must not stay cached
            final TableToken oldToken = token("x");
            execute("RENAME TABLE x TO x2");
            Assert.assertNotEquals(oldToken.getDirName(), token("x2").getDirName());
            Assert.assertEquals(1, cache.getTableCount());
            Assert.assertEquals(0, cache.getPartitionCount(oldToken));
            assertDiskSize("x2");
            Assert.assertEquals(2, cache.getTableCount());
        });
    }

    @Test
    public void testEvictsDroppedTables() throws Exception {
        assertMemoryLeak(() -> {
            // system tables and tables no query measures leave the cache with fewer tables than
            // the database, they must not keep dropped tables cached
            createSystemTables();
            createDailyTable("unmeasured", false);
            createDailyTable("x", false);
            createDailyTable("y", true);
            final TableDiskSizeCache cache = engine.getTableDiskSizeCache();

            measureDiskSize("x");
            measureDiskSize("y");
            Assert.assertEquals(2, cache.getTableCount());

            // the drop evicts the table, no query has to run first
            execute("DROP TABLE x");
            Assert.assertEquals(1, cache.getTableCount());
            execute("DROP TABLE y");
            Assert.assertEquals(0, cache.getTableCount());
            drainWalQueue();
            drainPurgeJob();
            // a full poll measures the one table left
            assertQuery("SELECT count() FROM table_storage() WHERE diskSize > 0")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n1\n");
            Assert.assertEquals(1, cache.getTableCount());
        });
    }

    @Test
    public void testExpiredEntriesPickUpInPlaceChanges() throws Exception {
        // The test needs a file to grow without changing the modification time of its
        // directory. POSIX file systems guarantee that; NTFS updates the copy of the file
        // size kept in the directory index on close, which may touch the directory.
        Assume.assumeFalse(Os.isWindows());
        setProperty(PropertyKey.CAIRO_TABLE_STORAGE_CACHE_TTL, 60_000);
        assertMemoryLeak(() -> {
            createDailyTable("x", false);
            final long now = System.currentTimeMillis() + 2 * TableDiskSizeCache.RACY_WINDOW_MILLIS;
            setCurrentMicros(now * 1000L);
            final long before = assertDiskSize("x");

            // Grow a column file of a sealed partition in place, like an external tool could.
            // Neither the directory modification time nor _txn changes.
            try (Path path = new Path()) {
                tableDir(path, "x").concat("2024-01-02").concat("v.d");
                Assert.assertTrue(Files.exists(path.$()));
                final long fd = Files.openRW(path.$());
                Assert.assertTrue(fd > -1);
                try {
                    Assert.assertTrue(Files.truncate(fd, Files.length(fd) + 4096));
                } finally {
                    Files.close(fd);
                }
            }
            Assert.assertEquals(before, measureDiskSize("x"));

            setCurrentMicros((now + 60_001) * 1000L);
            Assert.assertEquals(before + 4096, assertDiskSize("x"));
        });
    }

    @Test
    public void testKeepsSealedPartitionsAfterOutOfOrderCommits() throws Exception {
        // out-of-order commits rewrite partitions, which the cache sees through their directory
        // names, and must not make it re-walk the partitions they leave alone
        final WalkRecordingFilesFacade ff = new WalkRecordingFilesFacade();
        assertMemoryLeak(ff, () -> {
            createDailyTable("x", true);
            setClockPastRacyWindow();
            assertDiskSize("x");

            execute("INSERT INTO x VALUES ('2024-01-05T01:00:00.000000Z', 'z', 1, 'v')");
            drainWalQueue();
            ff.walkedDirs.clear();
            assertDiskSize("x");
            for (int i = 0; i < PARTITIONS.length - 1; i++) {
                Assert.assertEquals(PARTITIONS[i], 0, ff.countWalks(PARTITIONS[i]));
            }

            execute("INSERT INTO x VALUES ('2024-01-02T01:00:00.000000Z', 'z', 1, 'v')");
            drainWalQueue();
            ff.walkedDirs.clear();
            assertDiskSize("x");
            Assert.assertEquals(0, ff.countWalks("2024-01-01"));
            Assert.assertEquals(0, ff.countWalks("2024-01-03"));
            Assert.assertEquals(0, ff.countWalks("2024-01-04"));
        });
    }

    @Test
    public void testKeepsSealedPartitionsAfterRemovalOfOlderPartitions() throws Exception {
        // the drop of a partition that is not the last one, and TTL eviction, reopen nothing
        final WalkRecordingFilesFacade ff = new WalkRecordingFilesFacade();
        assertMemoryLeak(ff, () -> {
            createDailyTable("x", false);
            setClockPastRacyWindow();
            assertDiskSize("x");

            execute("ALTER TABLE x DROP PARTITION LIST '2024-01-02'");
            ff.walkedDirs.clear();
            assertDiskSize("x");
            Assert.assertEquals(0, ff.countWalks("2024-01-01"));
            Assert.assertEquals(0, ff.countWalks("2024-01-03"));
            Assert.assertEquals(0, ff.countWalks("2024-01-04"));

            execute("ALTER TABLE x SET TTL 2 DAYS");
            // the TTL change bumps the metadata version, which re-walks every partition once
            assertDiskSize("x");
            // the new partition evicts 2024-01-01 and 2024-01-03
            execute("INSERT INTO x VALUES ('2024-01-06T01:00:00.000000Z', 'z', 1, 'v')");
            engine.releaseAllWriters();
            ff.walkedDirs.clear();
            assertDiskSize("x");
            Assert.assertEquals(0, ff.countWalks("2024-01-04"));
        });
    }

    @Test
    public void testMeasuresNativePartitionHeldByWriterBeforeParquetLastPartition() throws Exception {
        // When the last partition is in Parquet format, the writer keeps the newest native
        // partition open, its column files grown to an append page, and truncates them when it
        // closes. Nothing in _txn or the partition directory reflects the truncation.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG, s VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-01', 8_640_000_000L), x, x::VARCHAR FROM long_sequence(20)");
            drainWalQueue();
            execute("ALTER TABLE x SET FORMAT PARQUET");
            execute("INSERT INTO x VALUES ('2024-01-03T01:00:00.000000Z', 1, 'a')");
            drainWalQueue();
            try (TableReader reader = engine.getReader("x")) {
                Assert.assertEquals(3, reader.getTxFile().getPartitionCount());
                Assert.assertFalse(reader.getTxFile().isPartitionParquet(1));
                Assert.assertTrue(reader.getTxFile().isPartitionParquet(2));
            }
            setClockPastRacyWindow();
            final long sizeWithOpenWriter = assertDiskSize("x");

            engine.releaseAllWriters();
            // the test relies on the writer holding 2024-01-02 open until now
            Assert.assertTrue(assertDiskSize("x") < sizeWithOpenWriter);
        });
    }

    @Test
    public void testNonPartitionedTable() throws Exception {
        final WalkRecordingFilesFacade ff = new WalkRecordingFilesFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts)");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-01', 1_000_000L), x FROM long_sequence(1_000)");
            engine.releaseAllWriters();
            setClockPastRacyWindow();

            assertDiskSize("x");
            ff.walkedDirs.clear();
            assertDiskSize("x");
            // the only partition is the last one
            Assert.assertEquals(1, ff.countWalks("default"));
        });
    }

    @Test
    public void testReadOnlyInstanceEvictsTablesDroppedByPrimary() throws Exception {
        assertMemoryLeak(() -> {
            createDailyTable("unmeasured", true);
            createDailyTable("x", true);
            final CairoConfiguration roConfiguration = new DefaultTestCairoConfiguration(root) {
                @Override
                public boolean getAllowTableRegistrySharedWrite() {
                    return false;
                }

                @Override
                public boolean isReadOnlyInstance() {
                    return true;
                }
            };
            try (
                    CairoEngine roEngine = new CairoEngine(roConfiguration);
                    SqlExecutionContext roContext = TestUtils.createSqlExecutionCtx(roEngine)
            ) {
                roEngine.reloadTableNames();
                try (
                        RecordCursorFactory factory = roEngine.select("SELECT diskSize FROM table_storage() WHERE tableName = 'x'", roContext);
                        RecordCursor cursor = factory.getCursor(roContext)
                ) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(cursor.getRecord().getLong(0) > 0);
                }
                final TableDiskSizeCache roCache = roEngine.getTableDiskSizeCache();
                Assert.assertEquals(1, roCache.getTableCount());

                // a read-only instance learns about the drop when it reloads the table registry
                execute("DROP TABLE x");
                Assert.assertEquals(1, roCache.getTableCount());
                roEngine.reloadTableNames();
                Assert.assertEquals(0, roCache.getTableCount());
            }
        });
    }

    @Test
    public void testTtlChangeAppliesToCachedSizes() throws Exception {
        // a configuration reload changes the TTL of the sizes the cache already holds
        final WalkRecordingFilesFacade ff = new WalkRecordingFilesFacade();
        assertMemoryLeak(ff, () -> {
            createDailyTable("x", false);
            final long now = System.currentTimeMillis() + 2 * TableDiskSizeCache.RACY_WINDOW_MILLIS;
            setCurrentMicros(now * 1000L);
            // the default TTL of 10 minutes keeps the sizes for at least 5 minutes
            assertDiskSize("x");

            // a shorter TTL expires the sizes measured under the longer one
            setProperty(PropertyKey.CAIRO_TABLE_STORAGE_CACHE_TTL, 60_000);
            setCurrentMicros((now + 60_000) * 1000L);
            ff.walkedDirs.clear();
            assertDiskSize("x");
            for (String partition : PARTITIONS) {
                Assert.assertEquals(partition, 1, ff.countWalks(partition));
            }

            // a longer TTL keeps the sizes measured under the shorter one
            setProperty(PropertyKey.CAIRO_TABLE_STORAGE_CACHE_TTL, 600_000);
            setCurrentMicros((now + 180_000) * 1000L);
            ff.walkedDirs.clear();
            assertDiskSize("x");
            for (int i = 0; i < PARTITIONS.length - 1; i++) {
                Assert.assertEquals(PARTITIONS[i], 0, ff.countWalks(PARTITIONS[i]));
            }
        });
    }

    @Test
    public void testZeroTtlDisablesCache() throws Exception {
        setProperty(PropertyKey.CAIRO_TABLE_STORAGE_CACHE_TTL, 0);
        final WalkRecordingFilesFacade ff = new WalkRecordingFilesFacade();
        assertMemoryLeak(ff, () -> {
            createDailyTable("x", false);
            setClockPastRacyWindow();
            final String tableDirName = token("x").getDirName();

            for (int i = 0; i < 2; i++) {
                ff.walkedDirs.clear();
                assertDiskSize("x");
                // one walk of the whole table directory per measurement
                Assert.assertEquals(1, ff.walkedDirs.size());
                Assert.assertEquals(1, ff.countWalks(tableDirName));
            }
            Assert.assertEquals(0, engine.getTableDiskSizeCache().getTableCount());
        });
    }

    @Test
    public void testZeroTtlReleasesCachedSizes() throws Exception {
        final WalkRecordingFilesFacade ff = new WalkRecordingFilesFacade();
        assertMemoryLeak(ff, () -> {
            createDailyTable("x", false);
            setClockPastRacyWindow();
            final TableDiskSizeCache cache = engine.getTableDiskSizeCache();
            assertDiskSize("x");
            Assert.assertEquals(PARTITIONS.length, cache.getPartitionCount(token("x")));

            // a configuration reload disables the cache after queries filled it
            setProperty(PropertyKey.CAIRO_TABLE_STORAGE_CACHE_TTL, 0);
            ff.walkedDirs.clear();
            assertDiskSize("x");
            // one walk of the whole table directory
            Assert.assertEquals(1, ff.walkedDirs.size());
            Assert.assertEquals(1, ff.countWalks(token("x").getDirName()));
            Assert.assertEquals(0, cache.getTableCount());

            // a reload that re-enables the cache lets it fill again
            setProperty(PropertyKey.CAIRO_TABLE_STORAGE_CACHE_TTL, 60_000);
            assertDiskSize("x");
            ff.walkedDirs.clear();
            assertDiskSize("x");
            for (int i = 0; i < PARTITIONS.length - 1; i++) {
                Assert.assertEquals(PARTITIONS[i], 0, ff.countWalks(PARTITIONS[i]));
            }
            Assert.assertEquals(PARTITIONS.length, cache.getPartitionCount(token("x")));
        });
    }

    // Measures the table while the writer removes its last partition, with a hook between the
    // read of _txn and the walk of 2024-01-04, then adds a partition, which seals 2024-01-04 again
    // before any call sees it as the last partition.
    private static void assertDetectsLastPartitionRemovalMeasuredMidway(
            boolean isWal,
            TestUtils.LeakProneCode removeLastPartition,
            String newPartition
    ) throws Exception {
        final InterleavingFilesFacade ff = new InterleavingFilesFacade();
        assertMemoryLeak(ff, () -> {
            createDailyTable("x", isWal);
            setClockPastRacyWindow();

            ff.onDirSize(Files.SEPARATOR + "2024-01-04", removeLastPartition);
            measureDiskSize("x");
            ff.assertFired();

            execute("INSERT INTO x VALUES ('" + newPartition + "T01:00:00.000000Z', 'z', 1, 'v')");
            if (isWal) {
                drainWalQueue();
            }
            engine.releaseAllWriters();
            assertDiskSize("x");
        });
    }

    // Asserts that table_storage() reports the size of a fresh walk of the table directory.
    private static long assertDiskSize(String tableName) throws SqlException {
        final long expected;
        try (Path path = new Path()) {
            expected = Files.getDirSize(tableDir(path, tableName));
        }
        Assert.assertEquals(expected, measureDiskSize(tableName));
        return expected;
    }

    private static void createDailyTable(String tableName, boolean isWal) throws SqlException {
        execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym SYMBOL, v LONG, s VARCHAR) TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
        // 50 rows 2.4 hours apart fill the five partitions in PARTITIONS
        execute("INSERT INTO " + tableName + " SELECT timestamp_sequence('2024-01-01', 8_640_000_000L), (x % 3)::SYMBOL, x, x::VARCHAR FROM long_sequence(50)");
        if (isWal) {
            drainWalQueue();
        }
        engine.releaseAllWriters();
    }

    // Creates two system tables: the engine counts them as tables, but table_storage() never lists
    // them, so the cache never holds them.
    private static void createSystemTables() throws SqlException {
        for (int i = 0; i < 2; i++) {
            final String tableName = configuration.getSystemTableNamePrefix() + "t" + i;
            execute("CREATE TABLE '" + tableName + "' (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            Assert.assertTrue(token(tableName).isSystem());
        }
    }

    // Executes a statement with an execution context of the calling thread.
    private static void executeOnThisThread(String sql) throws SqlException {
        try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
            engine.execute(sql, executionContext);
        }
    }

    // Runs a statement until no concurrent query holds the metadata of the table: until then,
    // non-WAL DDL fails to lock the table.
    private static void executeRetryingBusy(String sql) throws SqlException {
        for (int attempt = 0; ; attempt++) {
            try {
                execute(sql);
                return;
            } catch (CairoException e) {
                final CharSequence message = e.getFlyweightMessage();
                if (attempt > 100_000 || !(Chars.contains(message, "could not lock") || Chars.contains(message, "busy"))) {
                    throw e;
                }
                Os.pause();
            }
        }
    }

    private static long measureDiskSize(String tableName) throws SqlException {
        return measureDiskSize(tableName, sqlExecutionContext);
    }

    private static long measureDiskSize(String tableName, SqlExecutionContext executionContext) throws SqlException {
        try (
                RecordCursorFactory factory = engine.select("SELECT diskSize FROM table_storage() WHERE tableName = '" + tableName + "'", executionContext);
                RecordCursor cursor = factory.getCursor(executionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
            final long diskSize = cursor.getRecord().getLong(0);
            Assert.assertFalse(cursor.hasNext());
            return diskSize;
        }
    }

    // Measures the table with an execution context of the calling thread.
    private static void measureDiskSizeOnThisThread(String tableName) throws SqlException {
        try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
            measureDiskSize(tableName, executionContext);
        }
    }

    // Moves the test clock far enough ahead of every directory written so far to let the cache keep them.
    private static void setClockPastRacyWindow() {
        setCurrentMicros((System.currentTimeMillis() + 2 * TableDiskSizeCache.RACY_WINDOW_MILLIS) * 1000L);
    }

    private static Path tableDir(Path path, String tableName) {
        return path.of(configuration.getDbRoot()).concat(token(tableName).getDirName());
    }

    private static TableToken token(String tableName) {
        return engine.verifyTableName(tableName);
    }

    // Drops a table from another thread the first time a query opens the table's _txn file.
    private static class DropOnTxnOpenFilesFacade extends TestFilesFacadeImpl {
        private volatile String dirName;
        private volatile String dropSql;
        private volatile Throwable error;
        private volatile boolean hasFired;

        @Override
        public long openRO(LPSZ name) {
            final String dirName = this.dirName;
            if (dirName != null && Utf8s.endsWithAscii(name, Files.SEPARATOR + TableUtils.TXN_FILE_NAME)
                    && Utf8s.containsAscii(name, Files.SEPARATOR + dirName + Files.SEPARATOR)) {
                this.dirName = null;
                final Thread dropper = new Thread(() -> {
                    try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                        engine.execute(dropSql, executionContext);
                    } catch (Throwable th) {
                        error = th;
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
                dropper.start();
                try {
                    dropper.join(TimeUnit.MINUTES.toMillis(1));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (dropper.isAlive()) {
                    error = new AssertionError("the drop did not complete while the query waited for it");
                }
                hasFired = true;
            }
            return super.openRO(name);
        }

        private void arm(String dirName, String dropSql) {
            this.dropSql = dropSql;
            this.dirName = dirName;
        }
    }

    // Runs an action on another thread at a chosen file operation of the measured table and waits
    // for it: on the walk of a directory, or on the first read of a file.
    private static class InterleavingFilesFacade extends TestFilesFacadeImpl {
        private volatile TestUtils.LeakProneCode action;
        private volatile String dirSizeSuffix;
        private volatile Throwable error;
        private volatile boolean hasFired;
        private volatile long readFd = -1;
        private volatile String readSuffix;

        @Override
        public long getDirSize(Path path) {
            final String suffix = dirSizeSuffix;
            if (suffix != null && Utf8s.endsWithAscii(path, suffix)) {
                dirSizeSuffix = null;
                fire();
            }
            return super.getDirSize(path);
        }

        @Override
        public long openRO(LPSZ name) {
            final long fd = super.openRO(name);
            final String suffix = readSuffix;
            if (suffix != null && fd > -1 && Utf8s.endsWithAscii(name, suffix)) {
                readSuffix = null;
                readFd = fd;
            }
            return fd;
        }

        @Override
        public long read(long fd, long buf, long len, long offset) {
            if (fd == readFd) {
                readFd = -1;
                fire();
            }
            return super.read(fd, buf, len, offset);
        }

        private void assertFired() {
            Assert.assertTrue(hasFired);
            if (error != null) {
                throw new AssertionError(error);
            }
        }

        private void fire() {
            final Thread thread = new Thread(() -> {
                try {
                    action.run();
                } catch (Throwable th) {
                    error = th;
                } finally {
                    Path.clearThreadLocals();
                }
            });
            thread.start();
            try {
                thread.join(TimeUnit.MINUTES.toMillis(1));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (thread.isAlive()) {
                error = new AssertionError("the action did not complete while the hook waited for it");
            }
            hasFired = true;
        }

        private void onDirSize(String dirSuffix, TestUtils.LeakProneCode action) {
            this.action = action;
            this.dirSizeSuffix = dirSuffix;
        }

        private void onRead(String fileSuffix, TestUtils.LeakProneCode action) {
            this.action = action;
            this.readSuffix = fileSuffix;
        }
    }

    private static class WalkRecordingFilesFacade extends TestFilesFacadeImpl {
        private final ObjList<String> walkedDirs = new ObjList<>();

        @Override
        public long getDirSize(Path path) {
            walkedDirs.add(path.toString());
            return super.getDirSize(path);
        }

        // counts the walks of directories whose name starts with the given prefix
        private int countWalks(String dirNamePrefix) {
            int count = 0;
            for (int i = 0, n = walkedDirs.size(); i < n; i++) {
                final String dir = walkedDirs.getQuick(i);
                if (dir.substring(dir.lastIndexOf(Files.SEPARATOR) + 1).startsWith(dirNamePrefix)) {
                    count++;
                }
            }
            return count;
        }
    }
}
