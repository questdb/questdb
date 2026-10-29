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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionCompactionScanJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxReader;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Durability of the partition rebuilds the compaction job runs off a reader and the writer swaps in: the parquet
 * conversion of a logical partition ({@code swapMergedLogicalPartition}) and the parquet copy beside a native
 * partition ({@code installGeneratedParquet}). They hold the barriers {@code CONVERT PARTITION TO PARQUET} holds:
 * under SYNC each name they create is durable before the {@code _txn} that names it, and a failed flush under SYNC
 * or ADAPTIVE poisons the engine. Under ADAPTIVE a crash before the next save point rewinds to a save point that
 * still names the native partition, which must still read back.
 */
public class CompactionJobDurabilityCrashTest extends AbstractAdaptiveCrashTest {
    private static final long DAY = parse("2024-01-01T00:00:00.000000Z");
    private static final String ROWS = """
            ts\ts\tv
            2024-01-01T00:00:00.000000Z\ta\t1.0
            2024-01-02T00:00:00.000000Z\tb\t2.0
            2024-01-03T00:00:00.000000Z\ta\t3.0
            """;

    @Test
    public void testAdaptiveJobParquetConversionSurvivesCrashBeforeSavePoint() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 0);
        runWithCrashFacade(() -> {
            createTable(true);
            final TableToken token = engine.verifyTableName("x");
            markDurableBaseline();
            convertFirstDayInJob(token);
            assertFirstDayParquet(true);
            recoverAfterCrash(new TableToken[]{token});
            Assert.assertFalse("the table must not be suspended", anyTableSuspended(token));
            assertRowsAndIndex();
            releaseEngineHandles();
        });
    }

    @Test
    public void testSyncJobParquetConversionFailedTxnMsyncIsFailStop() throws Exception {
        assertFailStop(true, false, new SyncFailureFacade("swapMergedLogicalPartition", true));
    }

    @Test
    public void testSyncJobParquetConversionNonWalFailedTxnMsyncIsFailStop() throws Exception {
        assertFailStop(false, false, new SyncFailureFacade("swapMergedLogicalPartition", true));
    }

    @Test
    public void testSyncJobParquetConversionNonWalSurvivesCrash() throws Exception {
        assertSyncJobParquetConversionSurvivesCrash(false);
    }

    @Test
    public void testSyncJobParquetConversionWalSurvivesCrash() throws Exception {
        assertSyncJobParquetConversionSurvivesCrash(true);
    }

    @Test
    public void testSyncJobParquetGenerationFailedDirFsyncIsFailStop() throws Exception {
        Assume.assumeFalse("a directory takes no fsync on Windows", Os.isWindows());
        assertFailStop(true, true, new SyncFailureFacade("installGeneratedParquet", false));
    }

    private static void assertFirstDayParquet(boolean expected) {
        try (TableReader reader = engine.getReader("x")) {
            final TxReader tx = reader.getTxFile();
            Assert.assertEquals("parquet", expected, tx.isPartitionParquet(tx.getPartitionIndex(DAY)));
        }
    }

    private static void convertFirstDayInJob(TableToken token) {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
            Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetConversion(token, DAY));
            Assert.assertTrue("the request must dispatch the conversion", job.run());
        }
        engine.releaseAllReaders();
        engine.releaseAllWriters();
    }

    private static void generateFirstDayParquetInJob(TableToken token) {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
            Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetGeneration(token, DAY));
            job.run();
        }
        engine.releaseAllReaders();
        engine.releaseAllWriters();
    }

    private static long parse(String timestamp) {
        try {
            return MicrosFormatUtils.parseTimestamp(timestamp);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Under SYNC, the job's swap or install meets a failed flush. Like every other failed data sync under SYNC, it
     * must poison the engine; the swap runs as a writer command, so nothing reaches a caller that could report it.
     */
    private void assertFailStop(boolean wal, boolean isGeneration, SyncFailureFacade ff) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        assertMemoryLeak(ff, () -> {
            createTable(wal);
            final TableToken token = engine.verifyTableName("x");
            ff.isArmed = true;
            try {
                if (isGeneration) {
                    generateFirstDayParquetInJob(token);
                } else {
                    convertFirstDayInJob(token);
                }
            } catch (Throwable ignore) {
                // The verdict is the engine's state, not how the job reports the failure.
            } finally {
                ff.isArmed = false;
            }
            final boolean isPoisoned = engine.isDurabilityFailed();
            engine.resetDurabilityFailure();
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            Assert.assertEquals("the injected failure must have been reached", 1, ff.failureCount);
            Assert.assertTrue("a failed data sync under SYNC must poison the engine", isPoisoned);
        });
    }

    private void assertRowsAndIndex() throws Exception {
        assertQuery("SELECT ts, s, v FROM x").timestamp("ts").expectSize().returns(ROWS);
        assertQuery("SELECT count() c FROM x WHERE s = 'a'").expectSize().noRandomAccess().returns("c\n2\n");
    }

    /**
     * The job converts the first day to parquet under SYNC and the power fails. The conversion committed a durable
     * {@code _txn} naming the new partition directory, so the directory and every file in it must survive.
     */
    private void assertSyncJobParquetConversionSurvivesCrash(boolean wal) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        runWithCrashFacade(() -> {
            createTable(wal);
            final TableToken token = engine.verifyTableName("x");
            markDurableBaseline();
            convertFirstDayInJob(token);
            assertFirstDayParquet(true);
            if (wal) {
                recoverAfterCrash(new TableToken[]{token});
                Assert.assertFalse("the table must not be suspended", anyTableSuspended(token));
            } else {
                crashAndReopen();
            }
            // The superseded native directory may come back too, as its removal was not flushed; the purge takes it.
            assertFirstDayParquet(true);
            assertRowsAndIndex();
            releaseEngineHandles();
        });
    }

    private void createTable(boolean wal) throws Exception {
        execute("CREATE TABLE x (ts TIMESTAMP, s SYMBOL INDEX, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY "
                + (wal ? "WAL" : "BYPASS WAL"));
        execute("""
                INSERT INTO x VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 1.0),
                    ('2024-01-02T00:00:00.000000Z', 'b', 2.0),
                    ('2024-01-03T00:00:00.000000Z', 'a', 3.0)
                """);
        drainWalQueue();
    }

    /**
     * Once armed, fails the first {@code fsync}/{@code fsyncAndClose} (or synchronous {@code msync} when
     * {@code isMsync}) called from {@code caller} with a genuine data-sync failure, then behaves normally.
     */
    private static final class SyncFailureFacade extends TestFilesFacadeImpl {
        private final String caller;
        private final boolean isMsync;
        int failureCount;
        volatile boolean isArmed;

        SyncFailureFacade(String caller, boolean isMsync) {
            this.caller = caller;
            this.isMsync = isMsync;
        }

        @Override
        public void fsync(long fd) {
            if (!isMsync && isFailureDue()) {
                throw CairoException.dataSyncFailure(this, 5, "fsync", "injected fsync failure");
            }
            super.fsync(fd);
        }

        @Override
        public void fsyncAndClose(long fd) {
            if (!isMsync && isFailureDue()) {
                super.close(fd);
                throw CairoException.dataSyncFailure(this, 5, "fsyncAndClose", "injected fsync failure");
            }
            super.fsyncAndClose(fd);
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (isMsync && !async && isFailureDue()) {
                throw CairoException.dataSyncFailure(this, 5, "msync", "injected msync failure");
            }
            super.msync(addr, len, async);
        }

        private boolean isFailureDue() {
            if (!isArmed) {
                return false;
            }
            for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
                if (caller.equals(frame.getMethodName())) {
                    isArmed = false;
                    failureCount++;
                    return true;
                }
            }
            return false;
        }
    }
}
