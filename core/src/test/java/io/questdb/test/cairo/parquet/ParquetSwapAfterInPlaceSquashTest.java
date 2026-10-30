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
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionCompactionScanJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A parquet conversion or parquet copy of a native day is built off a reader snapshot and swapped in later. WAL
 * apply can land in between: a DEDUP UPSERT into the end of the day splits it, and a squash appends the split back
 * into the day in place. The day keeps its name txn and row count while the rows the build read are overwritten,
 * so the swap has to refuse it on the day's squash tracker, or it installs the values the upsert replaced.
 */
public class ParquetSwapAfterInPlaceSquashTest extends AbstractCairoTest {
    private static final long DAY = parseTimestamp("2020-01-01T00:00:00.000000Z");
    // The upsert's 200 rows, from 22:00 at the table's 15 s step: every one replaces an existing row.
    private static final String UPSERTED_ROWS = """
            c\tmn\tmx\ts
            200\t70000\t70199\t14019900
            """;
    private static final String UPSERTED_ROWS_QUERY = "SELECT count() c, min(i) mn, max(i) mx, sum(i) s FROM t WHERE ts >= '2020-01-01T22:00' AND ts < '2020-01-01T22:50'";

    @Override
    @Before
    public void setUp() {
        // ADAPTIVE pins the squash range with its durable epoch, which sends the squash down the copy route.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        // The split squashes in the commit that made it.
        setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 1);
        setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
        super.setUp();
    }

    @Test
    public void testConversionRefusesADaySquashedInPlaceAfterItsBuild() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTableWithPendingUpsert();
            final String dayAfterApply = buildThenApplyWal(token, true);
            Assert.assertEquals("the squash must have gone in place", "nameTxn=-1, rows=5760, squashCount=1", dayAfterApply);
            Assert.assertFalse("the conversion must be refused", isDayParquet(token));
            assertQuery(UPSERTED_ROWS_QUERY).noLeakCheck().expectSize().noRandomAccess().returns(UPSERTED_ROWS);

            // A conversion built off the squashed day lands.
            Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetConversion(token, DAY));
            runCompactionJob();
            Assert.assertTrue("the next conversion must land", isDayParquet(token));
            assertQuery(UPSERTED_ROWS_QUERY).noLeakCheck().expectSize().noRandomAccess().returns(UPSERTED_ROWS);
        });
    }

    @Test
    public void testParquetCopyInstallRefusesADaySquashedInPlaceAfterItsBuild() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTableWithPendingUpsert();
            final String dayAfterApply = buildThenApplyWal(token, false);
            Assert.assertEquals("the squash must have gone in place", "nameTxn=-1, rows=5760, squashCount=1", dayAfterApply);
            Assert.assertFalse("the install must be refused", isDayParquetGenerated(token));

            // A copy built off the squashed day is installed, and holds the upserted values.
            Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetGeneration(token, DAY));
            runCompactionJob();
            Assert.assertTrue("the next copy must be installed", isDayParquetGenerated(token));
            Assert.assertEquals("the copy must hold the upserted values", 70_199, getGeneratedCopyMaxI(token));
            assertQuery(UPSERTED_ROWS_QUERY).noLeakCheck().expectSize().noRandomAccess().returns(UPSERTED_ROWS);
        });
    }

    @Test
    public void testSquashTrackerReadsTheSquashTimestampOnceTheCounterSaturates() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path dir = new Path().of(root).concat("squash_tracker")) {
                Assert.assertEquals(0, ff.mkdirs(dir.slash(), configuration.getMkDirMode()));
                dir.trimTo(dir.size() - 1);
                final int dirLen = dir.size();
                Assert.assertEquals("an unsaturated count is the tracker", 17, TableUtils.readPartitionSquashTracker(ff, dir, 17));
                Assert.assertEquals(
                        "a saturated count without a squash timestamp stays saturated",
                        TxReader.PARTITION_SQUASH_COUNTER_MAX,
                        TableUtils.readPartitionSquashTracker(ff, dir, TxReader.PARTITION_SQUASH_COUNTER_MAX)
                );
                Assert.assertEquals(dirLen, dir.size());

                final long squashTimestamp = 1_700_000_000_000_000L;
                final long fd = ff.openRW(dir.concat(TableUtils.PARTITION_LAST_SQUASH_TIMESTAMP_FILE).$(), configuration.getWriterFileOpenOpts());
                dir.trimTo(dirLen);
                Assert.assertTrue(fd > -1);
                final long mem = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.putLong(mem, squashTimestamp);
                    Assert.assertEquals(Long.BYTES, ff.write(fd, mem, Long.BYTES, 0));
                } finally {
                    Unsafe.free(mem, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                    ff.close(fd);
                }
                Assert.assertEquals(
                        "a saturated count reads the squash timestamp",
                        squashTimestamp,
                        TableUtils.readPartitionSquashTracker(ff, dir, TxReader.PARTITION_SQUASH_COUNTER_MAX)
                );
                Assert.assertEquals(dirLen, dir.size());
            }
        });
    }

    private static String dayState(TableToken token) {
        try (TableReader reader = engine.getReader(token)) {
            final TxReader txFile = reader.getTxFile();
            final int partitionIndex = txFile.getPartitionIndex(DAY);
            return "nameTxn=" + txFile.getPartitionNameTxn(partitionIndex)
                    + ", rows=" + txFile.getPartitionSize(partitionIndex)
                    + ", squashCount=" + txFile.getPartitionSquashCount(partitionIndex);
        }
    }

    private static long getGeneratedCopyMaxI(TableToken token) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (TableReader reader = engine.getReader(token); Path path = new Path()) {
            final TxReader txFile = reader.getTxFile();
            final int partitionIndex = txFile.getPartitionIndex(DAY);
            path.of(configuration.getDbRoot()).concat(token.getDirName());
            TableUtils.setPathForNativePartition(path, reader.getMetadata().getTimestampType(), reader.getPartitionedBy(), DAY, txFile.getPartitionNameTxn(partitionIndex));
            final int partitionDirLen = path.size();
            final long parquetFileSize = ff.length(path.concat(TableUtils.PARQUET_PARTITION_NAME).$());
            path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME);
            final ParquetMetaFileReader parquetMeta = new ParquetMetaFileReader();
            final long addr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), parquetMeta);
            Assert.assertNotEquals(0, addr);
            final long mappedSize = parquetMeta.getFileSize();
            try {
                Assert.assertTrue(parquetMeta.resolveFooter(parquetFileSize));
                final int columnIndex = parquetMeta.getColumnIndexById(reader.getMetadata().getColumnMetadata(0).getOriginalWriterIndex());
                long max = Long.MIN_VALUE;
                for (int rowGroup = 0, n = parquetMeta.getRowGroupCount(); rowGroup < n; rowGroup++) {
                    max = Math.max(max, parquetMeta.getChunkMaxStat(rowGroup, columnIndex));
                }
                return max;
            } finally {
                parquetMeta.clear();
                ff.munmap(addr, mappedSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
            }
        }
    }

    private static boolean isDayParquet(TableToken token) {
        try (TableReader reader = engine.getReader(token)) {
            return reader.getTxFile().isPartitionParquet(reader.getTxFile().getPartitionIndex(DAY));
        }
    }

    private static boolean isDayParquetGenerated(TableToken token) {
        try (TableReader reader = engine.getReader(token)) {
            return reader.getTxFile().isPartitionParquetGenerated(reader.getTxFile().getPartitionIndex(DAY));
        }
    }

    private static long parseTimestamp(String timestamp) {
        try {
            return MicrosFormatUtils.parseTimestamp(timestamp);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static void runCompactionJob() throws InterruptedException {
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread compactor = new Thread(() -> {
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            } catch (Throwable th) {
                failure.set(th);
            } finally {
                Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                Path.clearThreadLocals();
            }
        });
        compactor.start();
        compactor.join();
        if (failure.get() != null) {
            throw new AssertionError("the compaction job failed", failure.get());
        }
    }

    /**
     * Runs the compaction job on a thread of its own and applies the WAL the moment its build reader goes back to
     * the pool: the build is done, the swap is not published yet.
     *
     * @return the day's state right after the apply
     */
    private String buildThenApplyWal(TableToken token, boolean isConversion) throws InterruptedException {
        if (isConversion) {
            Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetConversion(token, DAY));
        } else {
            Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetGeneration(token, DAY));
        }
        final AtomicReference<String> dayAfterApply = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread compactor = new Thread(() -> {
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            } catch (Throwable th) {
                failure.set(th);
            } finally {
                Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                Path.clearThreadLocals();
            }
        });
        final long compactorThreadId = compactor.threadId();
        engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
            if (factoryType == PoolListener.SRC_READER
                    && event == PoolListener.EV_RETURN
                    && thread == compactorThreadId
                    && token.equals(tableToken)
                    && dayAfterApply.get() == null) {
                final Thread applier = new Thread(() -> {
                    try {
                        drainWalQueue();
                        dayAfterApply.set(dayState(token));
                    } catch (Throwable th) {
                        failure.set(th);
                        dayAfterApply.set("apply failed");
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
                applier.start();
                try {
                    applier.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        try {
            compactor.start();
            compactor.join();
        } finally {
            engine.setPoolListener(null);
        }
        if (failure.get() != null) {
            throw new AssertionError("the build or the apply failed", failure.get());
        }
        Assert.assertNotNull("the build must have read the day", dayAfterApply.get());
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        return dayAfterApply.get();
    }

    /**
     * 2020-01-01 as one plain folder, with an upsert of its last 200 rows waiting in the WAL. 2020-01-03 keeps the
     * day inactive.
     */
    private TableToken createTableWithPendingUpsert() throws Exception {
        setCurrentMicros(DAY);
        execute("CREATE TABLE t (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
        execute("INSERT INTO t SELECT x::INT i, timestamp_sequence('2020-01-01', 15 * 1_000_000L) ts FROM long_sequence(5_760)");
        execute("INSERT INTO t SELECT x::INT + 90_000 i, timestamp_sequence('2020-01-03', 60 * 1_000_000L) ts FROM long_sequence(50)");
        drainWalQueue();
        execute("INSERT INTO t SELECT (70_000 + x - 1)::INT i, timestamp_sequence('2020-01-01T22:00:00', 15 * 1_000_000L) ts FROM long_sequence(200)");
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        return engine.verifyTableName("t");
    }
}
