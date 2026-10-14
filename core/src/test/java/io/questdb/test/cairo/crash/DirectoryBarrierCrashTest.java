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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTaskJob;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.tasks.ColumnTask;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.function.BooleanSupplier;

/**
 * Explicit durability operations keep their promises with only the directory barriers they take.
 * <p>
 * The crash model is strict POSIX ({@link CrashFaultFilesFacade}): a directory entry survives a power loss
 * only if its parent directory was fsynced after the entry appeared, or a {@code syncfs} ran. Nothing is
 * written back by the kernel unless a test says so.
 * <ul>
 *   <li>Ordinary SYNC partition opens retain the existing durability contract: they do not flush new
 *   directory entries, which this strict model may lose.</li>
 *   <li>SYNC fsyncs each partition directory in which ALTER COLUMN TYPE creates converted column files, and the
 *   partition directory a parquet conversion or the switch of a native partition to its parquet file creates and
 *   then the table directory, before the commit that publishes them. A failed one poisons the engine, as every
 *   failed barrier does.</li>
 *   <li>SYNC and ADAPTIVE fsync each column file ALTER COLUMN TYPE converts, data before aux, before the commit
 *   that publishes it, and each file a parquet conversion writes. A failed one poisons the engine too; under ASYNC
 *   it only fails the statement.</li>
 *   <li>ADAPTIVE takes no directory barrier in {@code TableWriter.openPartition}: the durable epoch makes
 *   directory entries durable, and recovery re-creates the later ones when it replays the WAL. That holds
 *   also when {@code syncfs} is not filesystem-wide (macOS, Windows), where the epoch fsyncs the dirty
 *   partition directories itself, and then the table directory that holds their entries.</li>
 *   <li>The WAL writer, under SYNC and ADAPTIVE, fsyncs each new segment's entry in {@code wal<N>} and, once
 *   per writer, the table directory that holds {@code wal<N>}.</li>
 * </ul>
 */
public class DirectoryBarrierCrashTest extends AbstractAdaptiveCrashTest {

    @Test
    public void testAdaptiveAlterColumnTypeNonWalFailedFixedFsyncIsFailStop() throws Exception {
        assertColumnConversionFsyncFailureIsFailStop("adaptive", false, "LONG", 0);
    }

    @Test
    public void testAdaptiveAlterColumnTypeWalFailedFixedFsyncIsFailStop() throws Exception {
        assertColumnConversionFsyncFailureIsFailStop("adaptive", true, "LONG", 0);
    }

    @Test
    public void testAdaptiveAlterColumnTypeWalFailedSymbolFsyncIsFailStop() throws Exception {
        assertColumnConversionFsyncFailureIsFailStop("adaptive", true, "SYMBOL", 0);
    }

    @Test
    public void testAdaptiveAlterColumnTypeWalFailedVarAuxFsyncIsFailStop() throws Exception {
        assertColumnConversionFsyncFailureIsFailStop("adaptive", true, "VARCHAR", 1);
    }

    @Test
    public void testAdaptiveAlterColumnTypeWalFailedVarDataFsyncIsFailStop() throws Exception {
        assertColumnConversionFsyncFailureIsFailStop("adaptive", true, "VARCHAR", 0);
    }

    @Test
    public void testAdaptiveAttachParquetFailedDirFsyncIsFailStop() throws Exception {
        Assume.assumeFalse("a directory takes no fsync on Windows", Os.isWindows());
        assertAttachParquetSyncFailure("adaptive", 1);
    }

    @Test
    public void testAdaptiveConvertToParquetWalFailedDataFsyncIsFailStop() throws Exception {
        assertConvertToParquetSyncFailure("adaptive", true, new FsyncFailureFacade("produceParquetFromNative", 0, false));
    }

    @Test
    public void testAdaptiveEpochCoversPartitionDirs() throws Exception {
        assertAdaptiveWalPartitionsSurviveCrash(true, false, new CrashFaultFilesFacade());
    }

    @Test
    public void testAdaptiveEpochCoversPartitionDirsWithoutFsWideSyncfs() throws Exception {
        assertAdaptiveWalPartitionsSurviveCrash(true, false, new SingleFileSyncfsFacade());
    }

    @Test
    public void testAdaptiveEpochCoversRewrittenPartitionDirs() throws Exception {
        assertAdaptiveWalPartitionsSurviveCrash(true, true, new CrashFaultFilesFacade());
    }

    @Test
    public void testAdaptiveEpochCoversRewrittenPartitionDirsWithoutFsWideSyncfs() throws Exception {
        assertAdaptiveWalPartitionsSurviveCrash(true, true, new SingleFileSyncfsFacade());
    }

    @Test
    public void testAdaptiveLazyPartitionsSurviveCrash() throws Exception {
        assertAdaptiveWalPartitionsSurviveCrash(false, false, new CrashFaultFilesFacade());
    }

    @Test
    public void testAdaptiveNewSequencerPartSurvivesCrash() throws Exception {
        assertNewSequencerPartSurvivesCrash("adaptive", new CrashFaultFilesFacade());
    }

    @Test
    public void testAdaptiveNewSequencerPartSurvivesCrashWithoutFsWideSyncfs() throws Exception {
        assertNewSequencerPartSurvivesCrash("adaptive", new SingleFileSyncfsFacade());
    }

    @Test
    public void testAdaptiveNewTableSequencerSurvivesCrash() throws Exception {
        assertNewTableSequencerSurvivesCrash("adaptive", new CrashFaultFilesFacade());
    }

    @Test
    public void testAdaptiveNewTableSequencerSurvivesCrashWithoutFsWideSyncfs() throws Exception {
        assertNewTableSequencerSurvivesCrash("adaptive", new SingleFileSyncfsFacade());
    }

    @Test
    public void testAdaptiveWalSegmentsSurviveCrash() throws Exception {
        assertWalSegmentsSurviveCrash("adaptive");
    }

    @Test
    public void testAsyncAddColumnFailedDirFsyncIsNotFailStop() throws Exception {
        assertAddColumnDirFsyncFailureIsNotFailStop("async");
    }

    @Test
    public void testAsyncAttachParquetFailedDirFsyncIsNotFailStop() throws Exception {
        Assume.assumeFalse("a directory takes no fsync on Windows", Os.isWindows());
        assertAttachParquetSyncFailure("async", 1);
    }

    @Test
    public void testAsyncAttachParquetFailedPmFsyncIsNotFailStop() throws Exception {
        assertAttachParquetSyncFailure("async", 0);
    }

    @Test
    public void testAsyncConvertToParquetNonWalFailedPmFsyncIsNotFailStop() throws Exception {
        // ASYNC fsyncs _pm and the partition directory but not data.parquet, so the first fsync is that of _pm
        assertConvertToParquetSyncFailure("async", false, new FsyncFailureFacade("produceParquetFromNative", 0, false));
    }

    @Test
    public void testAsyncConvertToParquetWalFailedSquashFsyncIsNotFailStop() throws Exception {
        // The fd-copy path fsyncs under ASYNC. Never force mixed I/O on a filesystem that cannot support it.
        Assume.assumeTrue(configuration.getFilesFacade().allowMixedIO(root));
        node1.setProperty(PropertyKey.DEBUG_CAIRO_ALLOW_MIXED_IO, true);
        assertConvertToParquetSquashSyncFailure("async", new FsyncFailureFacade("squashPartitionForce", 0, false));
    }

    @Test
    public void testAsyncConvertToParquetWalFailedSquashMsyncIsNotFailStop() throws Exception {
        // ZFS and other non-mixed-I/O filesystems copy through mappings and use MS_ASYNC, not fsync.
        node1.setProperty(PropertyKey.DEBUG_CAIRO_ALLOW_MIXED_IO, false);
        assertConvertToParquetSquashSyncFailure("async", new FsyncFailureFacade("squashPartitionForce", 0, true, true));
    }

    @Test
    public void testNoSyncAddColumnFailedDirFsyncIsNotFailStop() throws Exception {
        assertAddColumnDirFsyncFailureIsNotFailStop("nosync");
    }

    @Test
    public void testNoSyncAttachParquetTakesNoFsync() throws Exception {
        // NOSYNC regenerates _pm without an fsync, as the base does, so no failed sync can stop the engine there
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        final FsyncFailureFacade ff = new FsyncFailureFacade("attachPartition", 0, false);
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, s SYMBOL INDEX, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO x VALUES
                        ('2024-01-01T00:00:00.000000Z', 'a', 1.0),
                        ('2024-01-02T00:00:00.000000Z', 'b', 2.0),
                        ('2024-01-03T00:00:00.000000Z', 'a', 3.0)
                    """);
            stageParquetForAttach();
            ff.isArmed = true;
            try {
                execute("ALTER TABLE x ATTACH PARTITION LIST '2024-01-01'");
            } finally {
                ff.isArmed = false;
            }
            Assert.assertEquals("NOSYNC must take no fsync in ATTACH", 0, ff.failureCount);
            Assert.assertFalse(engine.isDurabilityFailed());
            assertAllColumns();
            releaseEngineHandles();
        });
    }

    @Test
    public void testSyncAddColumnFailedDirFsyncIsFailStop() throws Exception {
        Assume.assumeFalse("a directory takes no fsync on Windows", Os.isWindows());
        // the _meta swap takes four fsyncs first
        assertFsyncFailureIsFailStop("sync", false, null, () -> execute("ALTER TABLE x ADD COLUMN w INT"), "addColumn", 4);
    }

    @Test
    public void testSyncAlterColumnTypeNonWalFailedBarrierIsFailStop() throws Exception {
        assertSyncSchemaChangeBarrierFailureIsFailStop(false, false, "ALTER TABLE x ALTER COLUMN v TYPE LONG", "convertColumn0");
    }

    @Test
    public void testSyncAlterColumnTypeNonWalFailedFixedFsyncIsFailStop() throws Exception {
        assertColumnConversionFsyncFailureIsFailStop("sync", false, "LONG", 0);
    }

    /**
     * A pool worker runs the conversion task of the first partition and fails its fsync, while the converting
     * thread, already past the error check of the second partition, fails to open that partition's new column
     * file with a plain error. The worker's data-sync failure must still poison the engine.
     */
    @Test
    public void testSyncAlterColumnTypeNonWalFailedWorkerFsyncIsFailStop() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        final WorkerFsyncMaskingFacade ff = new WorkerFsyncMaskingFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, s SYMBOL INDEX, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO x VALUES
                        ('2024-01-01T00:00:00.000000Z', 'a', 1.0),
                        ('2024-01-02T00:00:00.000000Z', 'b', 2.0),
                        ('2024-01-03T00:00:00.000000Z', 'a', 3.0)
                    """);
            ff.mainThread = Thread.currentThread();
            final Sequence subSeq = engine.getMessageBus().getColumnTaskSubSeq();
            final RingQueue<ColumnTask> queue = engine.getMessageBus().getColumnTaskQueue();
            // runs the column tasks as the shared write pool's ColumnTaskJob does
            final Thread worker = new Thread(() -> {
                while (!ff.isWorkerStopped) {
                    long cursor = subSeq.next();
                    if (cursor > -1) {
                        ColumnTaskJob.processColumnTask(queue.get(cursor), cursor, subSeq);
                    } else {
                        Os.pause();
                    }
                }
            }, "column-task-worker");
            worker.start();
            Throwable failure = null;
            ff.isArmed = true;
            try {
                execute("ALTER TABLE x ALTER COLUMN v TYPE LONG");
            } catch (Throwable th) {
                failure = th;
            } finally {
                ff.isArmed = false;
                ff.isWorkerStopped = true;
                worker.join();
            }
            final boolean isPoisoned = engine.isDurabilityFailed();
            engine.resetDurabilityFailure();
            releaseEngineHandles();
            Assert.assertTrue("the worker's fsync must have failed", ff.isWorkerFsyncFailed);
            Assert.assertTrue("the converting thread's open must have failed", ff.isMainOpenFailed);
            Assert.assertTrue("a worker's failed fsync must poison the engine, failure: " + failure, isPoisoned);
            Assert.assertTrue(
                    "the statement must fail with a data-sync CairoError, failure: " + failure,
                    failure instanceof CairoError && CairoException.isDataSyncFailure(failure)
            );
        });
    }

    @Test
    public void testSyncAlterColumnTypeNonWalSurvivesCrash() throws Exception {
        assertSyncSchemaChangeSurvivesCrash(false, false, "ALTER TABLE x ALTER COLUMN v TYPE LONG", this::assertConvertedColumnNames);
    }

    @Test
    public void testSyncAlterColumnTypeOverParquetFailedBarrierIsFailStop() throws Exception {
        assertSyncSchemaChangeBarrierFailureIsFailStop(false, true, "ALTER TABLE x ALTER COLUMN v TYPE DECIMAL(10,2)", "fsyncConvertedPartitionDir");
    }

    @Test
    public void testSyncAlterColumnTypeOverParquetSurvivesCrash() throws Exception {
        // The parquet decoder cannot read DOUBLE as DECIMAL, so the change first converts the parquet partition
        // back to native, in a new partition directory.
        assertSyncSchemaChangeSurvivesCrash(false, true, "ALTER TABLE x ALTER COLUMN v TYPE DECIMAL(10,2)", this::assertConvertedColumnNames);
    }

    @Test
    public void testSyncAlterColumnTypeWalFailedBarrierIsFailStop() throws Exception {
        assertSyncSchemaChangeBarrierFailureIsFailStop(true, false, "ALTER TABLE x ALTER COLUMN v TYPE LONG", "convertColumn0");
    }

    @Test
    public void testSyncAlterColumnTypeWalFailedFixedFsyncIsFailStop() throws Exception {
        assertColumnConversionFsyncFailureIsFailStop("sync", true, "LONG", 0);
    }

    @Test
    public void testSyncAlterColumnTypeWalFailedSymbolFsyncIsFailStop() throws Exception {
        assertColumnConversionFsyncFailureIsFailStop("sync", true, "SYMBOL", 0);
    }

    @Test
    public void testSyncAlterColumnTypeWalFailedVarAuxFsyncIsFailStop() throws Exception {
        assertColumnConversionFsyncFailureIsFailStop("sync", true, "VARCHAR", 1);
    }

    @Test
    public void testSyncAlterColumnTypeWalFailedVarDataFsyncIsFailStop() throws Exception {
        assertColumnConversionFsyncFailureIsFailStop("sync", true, "VARCHAR", 0);
    }

    @Test
    public void testSyncAlterColumnTypeWalSurvivesCrash() throws Exception {
        assertSyncSchemaChangeSurvivesCrash(true, false, "ALTER TABLE x ALTER COLUMN v TYPE LONG", this::assertConvertedColumnNames);
    }

    @Test
    public void testSyncConvertToNativeNonWalFailedBarrierIsFailStop() throws Exception {
        assertSyncSchemaChangeBarrierFailureIsFailStop(false, true, "ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2024-01-01'", "fsyncConvertedPartitionDir");
    }

    @Test
    public void testSyncConvertToNativeNonWalFailedFileFsyncIsFailStop() throws Exception {
        assertSyncConvertToNativeFileFsyncFailureIsFailStop(false);
    }

    @Test
    public void testSyncConvertToNativeNonWalFailedTxnMsyncThenCrashReadsTable() throws Exception {
        assertSyncConvertTxnMsyncFailureKeepsTable(true, true, false, false);
    }

    @Test
    public void testSyncConvertToNativeNonWalFailedTxnMsyncThenRestartReadsTable() throws Exception {
        assertSyncConvertTxnMsyncFailureKeepsTable(true, false, false, false);
    }

    @Test
    public void testSyncConvertToNativeNonWalFailedTxnMsyncThenVacuumThenCrashReadsTable() throws Exception {
        assertSyncConvertTxnMsyncFailureKeepsTable(true, true, false, true);
    }

    @Test
    public void testSyncConvertToNativeNonWalFailedTxnMsyncThenWriterOpenThenCrashReadsTable() throws Exception {
        assertSyncConvertTxnMsyncFailureKeepsTable(true, true, true, false);
    }

    @Test
    public void testSyncConvertToNativeNonWalSurvivesCrash() throws Exception {
        assertSyncSchemaChangeSurvivesCrash(false, true, "ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2024-01-01'", this::assertAllColumns);
    }

    @Test
    public void testSyncConvertToNativeWalFailedBarrierIsFailStop() throws Exception {
        assertSyncSchemaChangeBarrierFailureIsFailStop(true, true, "ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2024-01-01'", "fsyncConvertedPartitionDir");
    }

    @Test
    public void testSyncConvertToNativeWalFailedFileFsyncIsFailStop() throws Exception {
        assertSyncConvertToNativeFileFsyncFailureIsFailStop(true);
    }

    @Test
    public void testSyncConvertToNativeWalSurvivesCrash() throws Exception {
        assertSyncSchemaChangeSurvivesCrash(true, true, "ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2024-01-01'", this::assertAllColumns);
    }

    @Test
    public void testSyncConvertToParquetNonWalFailedBarrierIsFailStop() throws Exception {
        assertSyncSchemaChangeBarrierFailureIsFailStop(false, false, "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'", "fsyncConvertedPartitionDir");
    }

    @Test
    public void testSyncConvertToParquetNonWalFailedDataFsyncIsFailStop() throws Exception {
        assertConvertToParquetSyncFailure("sync", false, new FsyncFailureFacade("produceParquetFromNative", 0, false));
    }

    @Test
    public void testSyncConvertToParquetNonWalFailedDirFsyncIsFailStop() throws Exception {
        Assume.assumeFalse("a directory takes no fsync on Windows", Os.isWindows());
        assertConvertToParquetSyncFailure("sync", false, new FsyncFailureFacade("produceParquetFromNative", 2, false));
    }

    @Test
    public void testSyncConvertToParquetNonWalFailedPmFsyncIsFailStop() throws Exception {
        assertConvertToParquetSyncFailure("sync", false, new FsyncFailureFacade("produceParquetFromNative", 1, false));
    }

    @Test
    public void testSyncConvertToParquetNonWalFailedTxnMsyncIsFailStop() throws Exception {
        assertConvertToParquetSyncFailure("sync", false, new FsyncFailureFacade("commitTxWriter", 0, true));
    }

    @Test
    public void testSyncConvertToParquetNonWalFailedTxnMsyncThenCrashReadsTable() throws Exception {
        assertSyncConvertTxnMsyncFailureKeepsTable(false, true, false, false);
    }

    @Test
    public void testSyncConvertToParquetNonWalFailedTxnMsyncThenRestartReadsTable() throws Exception {
        assertSyncConvertTxnMsyncFailureKeepsTable(false, false, false, false);
    }

    @Test
    public void testSyncConvertToParquetNonWalFailedTxnMsyncThenVacuumThenCrashReadsTable() throws Exception {
        assertSyncConvertTxnMsyncFailureKeepsTable(false, true, false, true);
    }

    @Test
    public void testSyncConvertToParquetNonWalFailedTxnMsyncThenWriterOpenThenCrashReadsTable() throws Exception {
        assertSyncConvertTxnMsyncFailureKeepsTable(false, true, true, false);
    }

    @Test
    public void testSyncConvertToParquetNonWalSurvivesCrash() throws Exception {
        assertSyncSchemaChangeSurvivesCrash(false, false, "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'", this::assertParquetPartitionNames);
    }

    @Test
    public void testSyncConvertToParquetWalFailedBarrierIsFailStop() throws Exception {
        assertSyncSchemaChangeBarrierFailureIsFailStop(true, false, "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'", "fsyncConvertedPartitionDir");
    }

    @Test
    public void testSyncConvertToParquetWalFailedDataFsyncIsFailStop() throws Exception {
        assertConvertToParquetSyncFailure("sync", true, new FsyncFailureFacade("produceParquetFromNative", 0, false));
    }

    @Test
    public void testSyncConvertToParquetWalFailedSquashMsyncIsFailStop() throws Exception {
        assertConvertToParquetSquashSyncFailure("sync", new FsyncFailureFacade("squashPartitionForce", 0, true));
    }

    @Test
    public void testSyncConvertToParquetWalSurvivesCrash() throws Exception {
        assertSyncSchemaChangeSurvivesCrash(true, false, "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'", this::assertParquetPartitionNames);
    }

    @Test
    public void testSyncNewSequencerPartSurvivesCrash() throws Exception {
        assertNewSequencerPartSurvivesCrash("sync", new CrashFaultFilesFacade());
    }

    @Test
    public void testSyncNewTableSequencerSurvivesCrash() throws Exception {
        assertNewTableSequencerSurvivesCrash("sync", new CrashFaultFilesFacade());
    }

    @Test
    public void testSyncSwitchToParquetNonWalFailedBarrierIsFailStop() throws Exception {
        assertSyncSchemaChangeBarrierFailureIsFailStop(false, this::stageParquetForSwitch, this::switchFirstPartitionToParquet, "fsyncConvertedPartitionDir");
    }

    @Test
    public void testSyncSwitchToParquetNonWalFailedTxnMsyncThenCrashReadsTable() throws Exception {
        assertSyncConvertTxnMsyncFailureKeepsTable(this::stageConvertedParquetForSwitch, this::switchFirstPartitionToParquet, "switchNativePartitionWithParquet", true, true, false, false);
    }

    @Test
    public void testSyncSwitchToParquetNonWalFailedTxnMsyncThenRestartReadsTable() throws Exception {
        assertSyncConvertTxnMsyncFailureKeepsTable(this::stageConvertedParquetForSwitch, this::switchFirstPartitionToParquet, "switchNativePartitionWithParquet", true, false, false, false);
    }

    @Test
    public void testSyncSwitchToParquetNonWalFailedTxnMsyncThenVacuumThenCrashReadsTable() throws Exception {
        assertSyncConvertTxnMsyncFailureKeepsTable(this::stageConvertedParquetForSwitch, this::switchFirstPartitionToParquet, "switchNativePartitionWithParquet", true, true, false, true);
    }

    @Test
    public void testSyncSwitchToParquetNonWalFailedTxnMsyncThenWriterOpenThenCrashReadsTable() throws Exception {
        assertSyncConvertTxnMsyncFailureKeepsTable(this::stageConvertedParquetForSwitch, this::switchFirstPartitionToParquet, "switchNativePartitionWithParquet", true, true, true, false);
    }

    @Test
    public void testSyncSwitchToParquetNonWalSurvivesCrash() throws Exception {
        assertSyncSchemaChangeSurvivesCrash(false, this::stageParquetForSwitch, this::switchFirstPartitionToParquet, this::assertParquetPartitionNames);
    }

    @Test
    public void testSyncSwitchToParquetWalFailedBarrierIsFailStop() throws Exception {
        assertSyncSchemaChangeBarrierFailureIsFailStop(true, this::stageParquetForSwitch, this::switchFirstPartitionToParquet, "fsyncConvertedPartitionDir");
    }

    @Test
    public void testSyncSwitchToParquetWalSurvivesCrash() throws Exception {
        assertSyncSchemaChangeSurvivesCrash(true, this::stageParquetForSwitch, this::switchFirstPartitionToParquet, this::assertParquetPartitionNames);
    }

    @Test
    public void testSyncWalSegmentsSurviveCrash() throws Exception {
        assertWalSegmentsSurviveCrash("sync");
    }

    private static void appendRow(WalWriter writer, long ts, long v) {
        final TableWriter.Row row = writer.newRow(ts);
        row.putLong(1, v);
        row.append();
    }

    private static boolean isCalledFrom(String methodName) {
        for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
            if (methodName.equals(frame.getMethodName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * An ADAPTIVE WAL table gets two new partition directories after the baseline. The kernel wrote {@code _txn}
     * and {@code _cv} back before the power loss, so they name both directories, while no directory barrier made
     * either directory's entries durable. Recovery must rewind the pointers to the durable epoch and replay the
     * WAL.
     *
     * @param isEpochAfterFirstPartition when true, a durable epoch covers the first new directory; the second is
     *                                   applied lazily in both variants
     * @param isPartitionRewritten       when true, the first new directory is a new version of the first
     *                                   partition, written by an out-of-order insert into its middle; when false,
     *                                   it is a new partition
     */
    private void assertAdaptiveWalPartitionsSurviveCrash(
            boolean isEpochAfterFirstPartition,
            boolean isPartitionRewritten,
            CrashFaultFilesFacade facade
    ) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 0);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, isEpochAfterFirstPartition ? 0 : -1);
        runWithFacade(facade, () -> {
            crashFf.modelSharedJournal = false;
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            if (isPartitionRewritten) {
                execute("INSERT INTO x VALUES ('2024-01-01T12:00:00.000000Z', 1), ('2024-01-02T00:00:00.000000Z', 2)");
            } else {
                execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            }
            drainWalQueue();
            final TableToken token = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            final String tableDir = root + File.separator + token.getDirName();
            markDurableBaseline();

            if (isPartitionRewritten) {
                execute("INSERT INTO x VALUES ('2024-01-01T06:00:00.000000Z', 4)");
                drainWalQueue();
                final String[] versions = new File(tableDir).list((dir, name) -> name.startsWith("2024-01-01."));
                Assert.assertNotNull(versions);
                Assert.assertEquals("the insert must write a new version of the first partition", 1, versions.length);
            } else {
                execute("INSERT INTO x VALUES ('2024-01-02T00:00:00.000000Z', 2)");
                drainWalQueue();
            }
            if (isEpochAfterFirstPartition) {
                Assert.assertEquals("an epoch must cover the first new directory", 2, tracker.getDurableEpochSeqTxn());
            }
            node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            execute("INSERT INTO x VALUES ('2024-01-03T00:00:00.000000Z', 3)");
            drainWalQueue();
            Assert.assertTrue("the second new partition must be applied lazily", tracker.getDurableEpochSeqTxn() < 3);

            crashFf.markFileDurable(tableDir + File.separator + TableUtils.TXN_FILE_NAME);
            crashFf.markFileDurable(tableDir + File.separator + TableUtils.COLUMN_VERSION_FILE_NAME);

            recoverAfterCrash(new TableToken[]{token});
            assertNotSuspended(token);
            if (isPartitionRewritten) {
                assertRows("""
                        ts\tv
                        2024-01-01T06:00:00.000000Z\t4
                        2024-01-01T12:00:00.000000Z\t1
                        2024-01-02T00:00:00.000000Z\t2
                        2024-01-03T00:00:00.000000Z\t3
                        """);
            } else {
                assertRows("""
                        ts\tv
                        2024-01-01T00:00:00.000000Z\t1
                        2024-01-02T00:00:00.000000Z\t2
                        2024-01-03T00:00:00.000000Z\t3
                        """);
            }
            releaseEngineHandles();
        });
    }

    /**
     * ADD COLUMN fsyncs the table directory once the column is added, in every mode. Under NOSYNC and ASYNC, which
     * promise nothing on a power loss, a data-sync failure of that fsync is logged as non-critical: the column is
     * added and the engine keeps taking writes. These modes take no other fsync in the statement.
     */
    private void assertAddColumnDirFsyncFailureIsNotFailStop(String commitMode) throws Exception {
        Assume.assumeFalse("a directory takes no fsync on Windows", Os.isWindows());
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        final FsyncFailureFacade ff = new FsyncFailureFacade("addColumn", 0, false);
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            Throwable failure = null;
            ff.isArmed = true;
            try {
                execute("ALTER TABLE x ADD COLUMN w INT");
            } catch (Throwable th) {
                failure = th;
            } finally {
                ff.isArmed = false;
            }
            final boolean isPoisoned = engine.isDurabilityFailed();
            engine.resetDurabilityFailure();
            releaseEngineHandles();
            Assert.assertEquals("the injected failure must have been reached", 1, ff.failureCount);
            Assert.assertFalse("the engine must keep taking writes, failure: " + failure, isPoisoned);
            Assert.assertNull("the column must be added", failure);
            execute("INSERT INTO x VALUES ('2024-01-02T00:00:00.000000Z', 2, 2)");
            assertQuery("SELECT ts, v, w FROM x").timestamp("ts").expectSize().returns("""
                    ts\tv\tw
                    2024-01-01T00:00:00.000000Z\t1\tnull
                    2024-01-02T00:00:00.000000Z\t2\t2
                    """);
            releaseEngineHandles();
        });
    }

    private void assertAllColumns() throws Exception {
        assertQuery("SELECT ts, s, v FROM x").timestamp("ts").expectSize().returns("""
                ts\ts\tv
                2024-01-01T00:00:00.000000Z\ta\t1.0
                2024-01-02T00:00:00.000000Z\tb\t2.0
                2024-01-03T00:00:00.000000Z\ta\t3.0
                """);
        assertIndexedSymbolFilter();
    }

    /**
     * ATTACH of a parquet partition without {@code _pm} regenerates {@code _pm}, then fsyncs it and the partition
     * directory, in every mode but NOSYNC. One of those fsyncs fails with a data-sync error. Under SYNC and
     * ADAPTIVE the engine must be poisoned; under ASYNC, which promises nothing on a power loss, the statement
     * fails and the engine keeps taking writes, as in the base.
     *
     * @param skipCount 0 fails the {@code _pm} fsync, 1 the partition directory fsync
     */
    private void assertAttachParquetSyncFailure(String commitMode, int skipCount) throws Exception {
        assertSyncFailure(
                commitMode,
                false,
                this::stageParquetForAttach,
                () -> execute("ALTER TABLE x ATTACH PARTITION LIST '2024-01-01'"),
                new FsyncFailureFacade("attachPartition", skipCount, false),
                !"async".equals(commitMode)
        );
    }

    /**
     * ALTER COLUMN TYPE converts the column of each native partition in a conversion task, which fsyncs the new
     * column files, data before aux, under SYNC and ADAPTIVE. One of those fsyncs fails with a data-sync error.
     * The engine must be poisoned, as for a failed directory barrier around the same statement.
     *
     * @param commitMode the commit mode the row pins
     * @param newType    the column's new type: fixed-size, var-size (data and aux files) or symbol
     * @param skipCount  how many of the conversion task's fsyncs succeed before the one that fails
     */
    private void assertColumnConversionFsyncFailureIsFailStop(
            String commitMode,
            boolean wal,
            String newType,
            int skipCount
    ) throws Exception {
        assertFsyncFailureIsFailStop(
                commitMode,
                wal,
                null,
                () -> execute("ALTER TABLE x ALTER COLUMN v TYPE " + newType),
                "cthConvertPartitionHandler",
                skipCount
        );
    }

    /**
     * CONVERT TO PARQUET of a WAL table's active partition first squashes the parts an out-of-order insert split it
     * into. A data sync of that squash fails. Under SYNC and ADAPTIVE the engine must be poisoned, as for a failed
     * data sync of the conversion itself; otherwise the apply fails and the engine keeps taking writes.
     */
    private void assertConvertToParquetSquashSyncFailure(String commitMode, FsyncFailureFacade ff) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 << 10);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);
        assertSyncFailure(
                commitMode,
                true,
                () -> {
                    execute("INSERT INTO x SELECT timestamp_sequence('2024-01-03T00:01', 60_000_000L), 'a', x FROM long_sequence(1320)");
                    drainWalQueue();
                    execute("INSERT INTO x SELECT timestamp_sequence('2024-01-03T20:01', 1_000_000L), 'b', x FROM long_sequence(200)");
                    drainWalQueue();
                    try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                        Assert.assertEquals("the active partition must be split", 4, reader.getTxFile().getPartitionCount());
                    }
                },
                () -> execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-03'"),
                ff,
                "sync".equals(commitMode) || "adaptive".equals(commitMode)
        );
    }

    /**
     * CONVERT TO PARQUET fsyncs {@code data.parquet} (SYNC and ADAPTIVE), then {@code _pm} and the new partition
     * directory (every mode but NOSYNC), and its commit msyncs {@code _txn} (SYNC). One of those fails with a
     * data-sync error. Under SYNC and ADAPTIVE the engine must be poisoned; under ASYNC, which promises nothing on
     * a power loss, the statement fails and the engine keeps taking writes.
     */
    private void assertConvertToParquetSyncFailure(String commitMode, boolean wal, FsyncFailureFacade ff) throws Exception {
        assertSyncFailure(
                commitMode,
                wal,
                null,
                () -> execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'"),
                ff,
                !"async".equals(commitMode)
        );
    }

    /**
     * The column converter writes the new column through a mapping that it unmaps without {@code msync}, and the
     * crash model credits the later {@code fsync} only with bytes it saw written, so it cannot check the
     * converted values. It checks that each partition's file of the converted column survives under the name
     * the durable {@code _txn} and {@code _cv} give it, and reads the other columns and the index.
     */
    private void assertConvertedColumnNames() throws Exception {
        try (TableReader reader = engine.getReader(engine.verifyTableName("x")); Path path = new Path()) {
            final int columnIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("v"));
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                final long partitionTimestamp = reader.getPartitionTimestampByIndex(i);
                path.of(root).concat(reader.getTableToken().getDirName());
                TableUtils.setPathForNativePartition(
                        path,
                        reader.getMetadata().getTimestampType(),
                        reader.getPartitionedBy(),
                        partitionTimestamp,
                        reader.getPartitionNameTxnByIndex(i)
                );
                final long columnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(partitionTimestamp, columnIndex);
                final boolean exists = Files.exists(TableUtils.dFile(path, "v", columnNameTxn));
                Assert.assertTrue("converted column file lost: " + path, exists);
            }
        }
        assertQuery("SELECT ts, s FROM x").timestamp("ts").expectSize().returns("""
                ts\ts
                2024-01-01T00:00:00.000000Z\ta
                2024-01-02T00:00:00.000000Z\tb
                2024-01-03T00:00:00.000000Z\ta
                """);
        assertIndexedSymbolFilter();
    }

    /**
     * Fails one fsync of a change, in the given commit mode: the engine must be poisoned.
     *
     * @param prepare   runs once the rows are inserted, before they are applied to a WAL table; null for none
     * @param change    the change whose fsync fails
     * @param caller    the method whose fsync fails
     * @param skipCount how many of the caller's fsyncs succeed before the one that fails
     */
    private void assertFsyncFailureIsFailStop(
            String commitMode,
            boolean wal,
            TestUtils.LeakProneCode prepare,
            TestUtils.LeakProneCode change,
            String caller,
            int skipCount
    ) throws Exception {
        assertSyncFailure(commitMode, wal, prepare, change, new FsyncFailureFacade(caller, skipCount, false), true);
    }

    /**
     * Reads the index files of every partition.
     */
    private void assertIndexedSymbolFilter() throws Exception {
        assertQuery("SELECT ts FROM x WHERE s = 'a'").timestamp("ts").returns("""
                ts
                2024-01-01T00:00:00.000000Z
                2024-01-03T00:00:00.000000Z
                """);
    }

    /**
     * A V2 sequencer log with two records per part: three commits after the baseline open a new part file, and the
     * power fails before any of them is applied. Every commit was acknowledged (ADAPTIVE) or returned (SYNC), so all
     * of them must survive, and the log must hold their records, not zeros: the new part's entry in
     * {@code _txn_parts} has to be durable by the time a record in it is.
     */
    private void assertNewSequencerPartSurvivesCrash(String commitMode, CrashFaultFilesFacade facade) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 0);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 2);
        runWithFacade(facade, () -> {
            crashFf.modelSharedJournal = false;
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("x");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            markDurableBaseline();

            execute("INSERT INTO x VALUES ('2024-01-02T00:00:00.000000Z', 2)");
            execute("INSERT INTO x VALUES ('2024-01-03T00:00:00.000000Z', 3)");
            execute("INSERT INTO x VALUES ('2024-01-04T00:00:00.000000Z', 4)");
            Assert.assertEquals(4, engine.getTableSequencerAPI().getTxnTracker(token).getSeqTxn());
            final File secondPart = new File(
                    root + File.separator + token.getDirName() + File.separator + WalUtils.SEQ_DIR
                            + File.separator + WalUtils.TXNLOG_PARTS_DIR, "1"
            );
            Assert.assertTrue("the commits must open a second log part", secondPart.exists());

            recoverAfterCrash(new TableToken[]{token});
            assertNotSuspended(token);
            assertQuery("SELECT sequencerTxn, walId, segmentTxn FROM wal_transactions('x')").noRandomAccess().returns("""
                    sequencerTxn\twalId\tsegmentTxn
                    1\t1\t0
                    2\t1\t1
                    3\t1\t2
                    4\t1\t3
                    """);
            assertQuery("SELECT count(), sum(v) FROM x").noRandomAccess().expectSize().returns("""
                    count\tsum
                    4\t10
                    """);
            releaseEngineHandles();
        });
    }

    /**
     * A WAL table created after the baseline takes one commit, and the power fails before any save point. The commit
     * was acknowledged (ADAPTIVE) or returned (SYNC), so it must survive. Flushing the log's content does not make its
     * files' entries durable: the CREATE has to make {@code txn_seq}, its files and the table's entry in the database
     * root durable itself.
     */
    private void assertNewTableSequencerSurvivesCrash(String commitMode, CrashFaultFilesFacade facade) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 0);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        runWithFacade(facade, () -> {
            crashFf.modelSharedJournal = false;
            markDurableBaseline();
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("x");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            Assert.assertEquals(1, tracker.getSeqTxn());
            Assert.assertTrue("no save point may cover the commit", tracker.getDurableEpochSeqTxn() < 1);

            recoverAfterCrash(new TableToken[]{token});
            assertNotSuspended(token);
            assertQuery("SELECT count(), sum(v) FROM x").noRandomAccess().expectSize().returns("""
                    count\tsum
                    1\t1
                    """);
            releaseEngineHandles();
        });
    }

    private void assertNotSuspended(TableToken token) {
        Assert.assertFalse(
                "table must not be suspended: " + engine.getTableSequencerAPI().getTxnTracker(token).getErrorMessage(),
                engine.getTableSequencerAPI().isSuspended(token)
        );
    }

    /**
     * The parquet encoder writes {@code data.parquet} and {@code _pm} through JNI, which the crash model does not
     * see, so it cannot check their content; it checks that the new partition directory and the files in it,
     * {@code data.parquet}, {@code _pm} and the index files of the symbol, survive under their names.
     */
    private void assertParquetPartitionNames() {
        final File[] dirs = new File(root, engine.verifyTableName("x").getDirName())
                .listFiles((dir, name) -> name.startsWith("2024-01-01."));
        Assert.assertNotNull(dirs);
        Assert.assertEquals("the converted partition directory must survive", 1, dirs.length);
        final String[] names = dirs[0].list();
        Assert.assertNotNull(names);
        Arrays.sort(names);
        Assert.assertEquals("[_pm, data.parquet, s.k, s.v]", Arrays.toString(names));
    }

    private void assertRows(String expected) throws Exception {
        assertQuery("SELECT ts, v FROM x").timestamp("ts").expectSize().returns(expected);
    }

    /**
     * Under SYNC, CONVERT TO NATIVE fsyncs each column file it decodes from the parquet file. The first of those
     * fsyncs fails with a data-sync error: the engine must be poisoned, as for a failed directory barrier around
     * the same statement.
     */
    private void assertSyncConvertToNativeFileFsyncFailureIsFailStop(boolean wal) throws Exception {
        assertFsyncFailureIsFailStop(
                "sync",
                wal,
                this::convertFirstPartitionToParquet,
                () -> execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2024-01-01'"),
                "produceNativeFromParquet",
                0
        );
    }

    /**
     * Under SYNC, the commit of a partition conversion fails its {@code _txn} msync after the new {@code _txn} is
     * written through the mapping, and the engine must be poisoned. A restart without a power loss then reads the
     * {@code _txn} the page cache holds, which names the converted partition; after a power loss the durable, old
     * {@code _txn} comes back and names the old version of the partition. Either way that directory must be there
     * and the table must read every row.
     *
     * @param toNative            converts the first partition from parquet back to native; otherwise to parquet
     * @param isPowerLoss         the power fails after the stop; otherwise the process restarts without a power loss
     * @param isWriterOpenedFirst with a power loss: the process restarts first and a writer opens and closes the table
     * @param isVacuumedFirst     with a power loss: the process restarts first and VACUUM TABLE runs the partition purge
     */
    private void assertSyncConvertTxnMsyncFailureKeepsTable(boolean toNative, boolean isPowerLoss, boolean isWriterOpenedFirst, boolean isVacuumedFirst) throws Exception {
        assertSyncConvertTxnMsyncFailureKeepsTable(
                toNative ? this::convertFirstPartitionToParquet : null,
                () -> execute("ALTER TABLE x CONVERT PARTITION TO " + (toNative ? "NATIVE" : "PARQUET") + " LIST '2024-01-01'"),
                toNative ? "convertPartitionParquetToNative" : "convertPartitionNativeToParquet",
                !toNative,
                isPowerLoss,
                isWriterOpenedFirst,
                isVacuumedFirst
        );
    }

    /**
     * A change of the first partition's format fails the msync of its {@code _txn} under SYNC, and nothing of the
     * new {@code _txn} reaches the device. The engine must stop and both versions of the partition must stay: a
     * restart without a power loss reads it in the new format, a restart after a power loss in the old one.
     * <p>
     * A restart without a power loss, a writer open, then a power loss: the open removes the partition version
     * the page-cache {@code _txn} does not name, which the device's {@code _txn} may still name, so the open must
     * make the page-cache {@code _txn} durable first and the table must read in the new format. With VACUUM TABLE in
     * place of the writer open, the partition purge must leave both versions until a writer open has made
     * {@code _txn} durable, and the table must read in the old format.
     *
     * @param prepare             runs once the rows are inserted, before the durable baseline; null for none
     * @param change              the change whose {@code _txn} msync fails
     * @param operation           the {@code TableWriter} method of the change that commits the {@code _txn}
     * @param isParquetAfter      whether the change leaves the partition in parquet format
     * @param isPowerLoss         the power fails after the stop; otherwise the process restarts without a power loss
     * @param isWriterOpenedFirst with a power loss: the process restarts first and a writer opens and closes the table
     * @param isVacuumedFirst     with a power loss: the process restarts first and VACUUM TABLE runs the partition purge
     */
    private void assertSyncConvertTxnMsyncFailureKeepsTable(
            TestUtils.LeakProneCode prepare,
            TestUtils.LeakProneCode change,
            String operation,
            boolean isParquetAfter,
            boolean isPowerLoss,
            boolean isWriterOpenedFirst,
            boolean isVacuumedFirst
    ) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        final var ff = new CrashFaultFilesFacade() {
            int failureCount;
            boolean isArmed;

            @Override
            public void msync(long addr, long len, boolean async) {
                if (isArmed && !async && isCalledFrom("commitTxWriter") && isCalledFrom(operation)) {
                    isArmed = false;
                    failureCount++;
                    // nothing of the new _txn reaches the device
                    throw CairoException.dataSyncFailure(this, 5, "msync", "injected msync failure");
                }
                super.msync(addr, len, async);
            }
        };
        runWithFacade(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, s SYMBOL INDEX, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO x VALUES
                        ('2024-01-01T00:00:00.000000Z', 'a', 1.0),
                        ('2024-01-02T00:00:00.000000Z', 'b', 2.0),
                        ('2024-01-03T00:00:00.000000Z', 'a', 3.0)
                    """);
            if (prepare != null) {
                prepare.run();
            }
            markDurableBaseline();
            Throwable failure = null;
            ff.isArmed = true;
            try {
                change.run();
            } catch (Throwable th) {
                failure = th;
            } finally {
                ff.isArmed = false;
            }
            final boolean isPoisoned = engine.isDurabilityFailed();
            engine.resetDurabilityFailure();
            if (isPowerLoss) {
                if (isWriterOpenedFirst || isVacuumedFirst) {
                    releaseEngineHandles();
                    if (isWriterOpenedFirst) {
                        try (TableWriter writer = getWriter("x")) {
                            Assert.assertNotNull(writer);
                        }
                    } else {
                        execute("VACUUM TABLE x");
                        try (O3PartitionPurgeJob job = new O3PartitionPurgeJob(engine)) {
                            //noinspection StatementWithEmptyBody
                            while (job.run()) {
                            }
                        }
                    }
                    releaseEngineHandles();
                    // the filesystem journals the writer's directory changes on its own, as ext4 does within seconds
                    try (Path path = new Path()) {
                        path.of(root).concat(engine.verifyTableName("x").getDirName());
                        ff.fsyncAndClose(ff.openRO(path.$()));
                    }
                }
                crashAndReopen();
                // a power loss ends the process, and the transaction scoreboard with it
                engine.getTxnScoreboardPool().remove(engine.verifyTableName("x"));
            } else {
                releaseEngineHandles();
            }
            Assert.assertEquals("the injected failure must have been reached", 1, ff.failureCount);
            Assert.assertTrue("a failed _txn msync must poison the engine, failure: " + failure, isPoisoned);
            Throwable readFailure = null;
            try {
                assertAllColumns();
                try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                    Assert.assertEquals(
                            "the partition must be in the format the _txn names",
                            isParquetAfter != (isPowerLoss && !isWriterOpenedFirst),
                            reader.getTxFile().isPartitionParquet(0)
                    );
                }
            } catch (Throwable th) {
                readFailure = th;
            }
            releaseEngineHandles();
            if (readFailure != null) {
                throw new AssertionError("the table must read back", readFailure);
            }
        });
    }

    /**
     * Fails one data sync of a change, in the given commit mode. When it is fail-stop, the engine must be poisoned
     * and refuse the next write; a non-WAL statement must also fail with a data-sync {@link CairoError}, and a WAL
     * apply must not merely suspend the table while the engine keeps taking writes. Otherwise the statement fails
     * and the engine keeps taking writes.
     *
     * @param prepare    runs once the rows are inserted, before they are applied to a WAL table; null for none
     * @param change     the change whose data sync fails
     * @param ff         fails the data sync once armed
     * @param isFailStop whether the failure must poison the engine
     */
    private void assertSyncFailure(
            String commitMode,
            boolean wal,
            TestUtils.LeakProneCode prepare,
            TestUtils.LeakProneCode change,
            FsyncFailureFacade ff,
            boolean isFailStop
    ) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, s SYMBOL INDEX, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY "
                    + (wal ? "WAL" : "BYPASS WAL"));
            execute("""
                    INSERT INTO x VALUES
                        ('2024-01-01T00:00:00.000000Z', 'a', 1.0),
                        ('2024-01-02T00:00:00.000000Z', 'b', 2.0),
                        ('2024-01-03T00:00:00.000000Z', 'a', 3.0)
                    """);
            if (prepare != null) {
                prepare.run();
            }
            drainWalQueue();
            Throwable failure = null;
            ff.isArmed = true;
            try {
                change.run();
                drainWalQueue();
            } catch (Throwable th) {
                failure = th;
            } finally {
                ff.isArmed = false;
            }
            final boolean isPoisoned = engine.isDurabilityFailed();
            Throwable nextWriteFailure = null;
            try {
                execute("INSERT INTO x (ts, s) VALUES ('2024-01-04T00:00:00.000000Z', 'c')");
            } catch (Throwable th) {
                nextWriteFailure = th;
            }
            engine.resetDurabilityFailure();
            releaseEngineHandles();
            Assert.assertEquals("the injected failure must have been reached", 1, ff.failureCount);
            if (isFailStop) {
                Assert.assertTrue("a failed data sync must poison the engine, failure: " + failure, isPoisoned);
                Assert.assertTrue(
                        "a poisoned engine must refuse the next write, failure: " + nextWriteFailure,
                        nextWriteFailure instanceof CairoError
                                && nextWriteFailure.getMessage().contains("engine is poisoned by a failed durability barrier")
                );
                if (!wal) {
                    Assert.assertTrue(
                            "the statement must fail with a data-sync CairoError, failure: " + failure,
                            failure instanceof CairoError && CairoException.isDataSyncFailure(failure)
                    );
                }
            } else {
                Assert.assertFalse("the engine must keep taking writes, failure: " + failure, isPoisoned);
                Assert.assertNull("the next write must succeed, failure: " + nextWriteFailure, nextWriteFailure);
                if (!wal) {
                    Assert.assertTrue(
                            "the statement must fail with a CairoException, failure: " + failure,
                            failure instanceof CairoException
                    );
                }
            }
        });
    }

    /**
     * Under SYNC, a directory barrier of a schema change fails. The engine must be poisoned, as for every other
     * failed barrier: a non-WAL statement fails with a {@link CairoError}, and a WAL apply does not merely
     * suspend the table while the engine keeps taking writes.
     *
     * @param barrierCaller the method whose directory fsync fails
     */
    private void assertSyncSchemaChangeBarrierFailureIsFailStop(
            boolean wal,
            boolean isFirstPartitionParquet,
            String ddl,
            String barrierCaller
    ) throws Exception {
        assertSyncSchemaChangeBarrierFailureIsFailStop(wal, isFirstPartitionParquet ? this::convertFirstPartitionToParquet : null, () -> execute(ddl), barrierCaller);
    }

    /**
     * @param prepare runs once the rows are inserted, before they are applied to a WAL table; null for none
     * @param change  the change whose directory barrier fails
     */
    private void assertSyncSchemaChangeBarrierFailureIsFailStop(
            boolean wal,
            TestUtils.LeakProneCode prepare,
            TestUtils.LeakProneCode change,
            String barrierCaller
    ) throws Exception {
        Assume.assumeFalse("a directory takes no fsync on Windows", Os.isWindows());
        assertFsyncFailureIsFailStop("sync", wal, prepare, change, barrierCaller, 0);
    }

    /**
     * A schema change that rewrites data returns under SYNC, then the power fails. Each name it created must
     * survive: the converted column files in every partition directory, the partition directory a parquet
     * conversion or the switch of a native partition to its parquet file creates, and the index files it copies,
     * restores or links there. Otherwise the durable {@code _meta} and {@code _txn} name files that are gone and
     * the table does not open.
     */
    private void assertSyncSchemaChangeSurvivesCrash(
            boolean wal,
            boolean isFirstPartitionParquet,
            String ddl,
            TestUtils.LeakProneCode assertAfterCrash
    ) throws Exception {
        assertSyncSchemaChangeSurvivesCrash(wal, isFirstPartitionParquet ? this::convertFirstPartitionToParquet : null, () -> execute(ddl), assertAfterCrash);
    }

    /**
     * @param prepare runs once the rows are inserted, before they are applied to a WAL table and before the
     *                durable baseline; null for none
     * @param change  the change the power loss follows
     */
    private void assertSyncSchemaChangeSurvivesCrash(
            boolean wal,
            TestUtils.LeakProneCode prepare,
            TestUtils.LeakProneCode change,
            TestUtils.LeakProneCode assertAfterCrash
    ) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        runWithCrashFacade(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, s SYMBOL INDEX, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY "
                    + (wal ? "WAL" : "BYPASS WAL"));
            execute("""
                    INSERT INTO x VALUES
                        ('2024-01-01T00:00:00.000000Z', 'a', 1.0),
                        ('2024-01-02T00:00:00.000000Z', 'b', 2.0),
                        ('2024-01-03T00:00:00.000000Z', 'a', 3.0)
                    """);
            if (prepare != null) {
                prepare.run();
            }
            drainWalQueue();
            final TableToken token = engine.verifyTableName("x");
            markDurableBaseline();
            change.run();
            drainWalQueue();
            if (wal) {
                recoverAfterCrash(new TableToken[]{token});
                assertNotSuspended(token);
            } else {
                crashAndReopen();
            }
            assertAfterCrash.run();
            releaseEngineHandles();
        });
    }

    /**
     * A WAL writer created after the baseline writes three commits into three segments, and the power fails
     * before any of them is applied. Every commit was acknowledged (ADAPTIVE) or returned (SYNC), so all of
     * them must survive: {@code wal1} in the table directory, and each segment in {@code wal1}, have to be
     * durable by the time a commit that names them is sequenced.
     */
    private void assertWalSegmentsSurviveCrash(String commitMode) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 0);
        node1.setProperty(PropertyKey.CAIRO_WAL_COMMIT_WRITEBACK_DRAIN, false);
        node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 3);
        runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false;
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("x");
            markDurableBaseline();
            try (WalWriter writer = getWalWriter("x")) {
                long v = 0;
                for (int commit = 0; commit < 3; commit++) {
                    for (int row = 0; row < 3; row++) {
                        appendRow(writer, v * 1_000_000L, v);
                        v++;
                    }
                    writer.commit();
                }
                Assert.assertEquals("the commits must span three segments", 2, writer.getSegmentId());
            }
            Assert.assertEquals(3, engine.getTableSequencerAPI().getTxnTracker(token).getSeqTxn());

            recoverAfterCrash(new TableToken[]{token});
            assertNotSuspended(token);
            assertQuery("SELECT count(), sum(v) FROM x").noRandomAccess().expectSize().returns("""
                    count\tsum
                    9\t36
                    """);
            releaseEngineHandles();
        });
    }

    private void convertFirstPartitionToParquet() throws Exception {
        execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
    }

    private File firstPartitionDir() {
        try (TableReader reader = engine.getReader(engine.verifyTableName("x")); Path path = new Path()) {
            path.of(root).concat(reader.getTableToken().getDirName());
            TableUtils.setPathForNativePartition(
                    path,
                    reader.getMetadata().getTimestampType(),
                    reader.getPartitionedBy(),
                    reader.getPartitionTimestampByIndex(0),
                    reader.getPartitionNameTxnByIndex(0)
            );
            return new File(path.toString());
        }
    }

    private void runWithFacade(CrashFaultFilesFacade facade, TestUtils.LeakProneCode body) throws Exception {
        assumeCrashHarnessSupported();
        crashFf = facade;
        crashFf.setDbRoot(root);
        assertMemoryLeak(crashFf, body);
    }

    /**
     * Leaves the real {@code data.parquet} and {@code _pm} of the first partition in its native partition
     * directory, so that the switched partition reads back: converts the partition to parquet, keeps both files
     * and converts it back.
     */
    private void stageConvertedParquetForSwitch() throws Exception {
        convertFirstPartitionToParquet();
        final File stageDir = temp.newFolder();
        final String[] names = {TableUtils.PARQUET_PARTITION_NAME, TableUtils.PARQUET_METADATA_FILE_NAME};
        for (String name : names) {
            java.nio.file.Files.copy(new File(firstPartitionDir(), name).toPath(), new File(stageDir, name).toPath());
        }
        execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
        for (String name : names) {
            java.nio.file.Files.copy(new File(stageDir, name).toPath(), new File(firstPartitionDir(), name).toPath());
        }
    }

    /**
     * Converts the first partition to parquet, detaches it and leaves it ready to attach without its {@code _pm},
     * which ATTACH then regenerates.
     */
    private void stageParquetForAttach() throws Exception {
        convertFirstPartitionToParquet();
        execute("ALTER TABLE x DETACH PARTITION LIST '2024-01-01'");
        try (Path detached = new Path(); Path attachable = new Path()) {
            final String tableDir = engine.verifyTableName("x").getDirName();
            detached.of(root).concat(tableDir).concat("2024-01-01").put(TableUtils.DETACHED_DIR_MARKER).$();
            attachable.of(root).concat(tableDir).concat("2024-01-01").put(configuration.getAttachPartitionSuffix()).$();
            Assert.assertEquals(Files.FILES_RENAME_OK, Files.rename(detached.$(), attachable.$()));
            Assert.assertTrue(Files.remove(attachable.concat(TableUtils.PARQUET_METADATA_FILE_NAME).$()));
        }
    }

    /**
     * Leaves {@code data.parquet} and {@code _pm} in the first, native partition, as the cold storage job does
     * before it switches the partition to them. The switch only links them, so empty files do.
     */
    private void stageParquetForSwitch() throws Exception {
        drainWalQueue();
        try (TableReader reader = engine.getReader(engine.verifyTableName("x")); Path path = new Path()) {
            path.of(root).concat(reader.getTableToken().getDirName());
            TableUtils.setPathForNativePartition(
                    path,
                    reader.getMetadata().getTimestampType(),
                    reader.getPartitionedBy(),
                    reader.getPartitionTimestampByIndex(0),
                    reader.getPartitionNameTxnByIndex(0)
            );
            final int partitionDirLen = path.size();
            Assert.assertTrue(Files.touch(path.concat(TableUtils.PARQUET_PARTITION_NAME).$()));
            Assert.assertTrue(Files.touch(path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$()));
        }
    }

    /**
     * Switches the first partition to its staged parquet file the way the cold storage job does: marks the file
     * ready, then switches. The switch creates a new partition directory and links the parquet and index files
     * into it.
     */
    private void switchFirstPartitionToParquet() {
        try (TableWriter writer = getWriter("x")) {
            final long partitionTimestamp = writer.getTxWriter().getPartitionTimestampByIndex(0);
            Assert.assertTrue(writer.markPartitionParquetReady(partitionTimestamp));
            final long parquetFileSize = new File(firstPartitionDir(), TableUtils.PARQUET_PARTITION_NAME).length();
            Assert.assertEquals(TableWriter.SWITCH_OK, writer.switchNativePartitionWithParquet(partitionTimestamp, parquetFileSize));
        }
    }

    /**
     * Once armed, fails one {@code fsync} or {@code fsyncAndClose} called from {@code caller}, or one synchronous
     * or asynchronous {@code msync} when {@code isMsync}, selected by {@code isAsyncMsync}. Fails the first after
     * {@code skipCount} such calls with a genuine data-sync failure, then behaves normally.
     */
    private static final class FsyncFailureFacade extends TestFilesFacadeImpl {
        private final String caller;
        private final boolean isAsyncMsync;
        private final boolean isMsync;
        int failureCount;
        boolean isArmed;
        private int skipCount;

        FsyncFailureFacade(String caller, int skipCount, boolean isMsync) {
            this(caller, skipCount, isMsync, false);
        }

        FsyncFailureFacade(String caller, int skipCount, boolean isMsync, boolean isAsyncMsync) {
            this.caller = caller;
            this.skipCount = skipCount;
            this.isMsync = isMsync;
            this.isAsyncMsync = isAsyncMsync;
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
                // FilesFacadeImpl closes the fd before it throws. So must this fake, or the leak is its own.
                super.close(fd);
                throw CairoException.dataSyncFailure(this, 5, "fsyncAndClose", "injected fsync failure");
            }
            super.fsyncAndClose(fd);
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (isMsync && async == isAsyncMsync && isFailureDue()) {
                throw CairoException.dataSyncFailure(this, 5, "msync", "injected msync failure");
            }
            super.msync(addr, len, async);
        }

        private boolean isFailureDue() {
            if (!isArmed || !isCalledFrom(caller)) {
                return false;
            }
            if (skipCount > 0) {
                skipCount--;
                return false;
            }
            isArmed = false;
            failureCount++;
            return true;
        }
    }

    /**
     * {@code syncfs(fd)} as macOS and Windows have it: a flush of that one file, not of the filesystem. The
     * durable epoch then takes its fallback path, which fsyncs every file and directory it has to publish.
     */
    private static class SingleFileSyncfsFacade extends CrashFaultFilesFacade {
        @Override
        public boolean isSyncfsFileSystemWide() {
            return false;
        }

        @Override
        public void syncfs(long fd) {
            fsync(fd);
        }
    }

    /**
     * Holds a pool worker's fsync of a conversion task until the converting thread has opened the second
     * partition's source column, then fails it with a data-sync error, and fails the converting thread's open of
     * the second partition's new column file with a plain error once the worker's fsync has failed.
     */
    private static final class WorkerFsyncMaskingFacade extends TestFilesFacadeImpl {
        volatile boolean isArmed;
        volatile boolean isMainAtSecondPartition;
        volatile boolean isMainOpenFailed;
        volatile boolean isWorkerFsyncFailed;
        volatile boolean isWorkerStopped;
        volatile Thread mainThread;
        private int mainOpenRoCount;
        private int mainOpenRwCount;

        @Override
        public void fsync(long fd) {
            if (isArmed && !isWorkerFsyncFailed && Thread.currentThread() != mainThread && isCalledFrom("cthConvertPartitionHandler")) {
                waitFor(() -> isMainAtSecondPartition);
                isWorkerFsyncFailed = true;
                throw CairoException.dataSyncFailure(this, 5, "fsync", "injected worker fsync failure");
            }
            super.fsync(fd);
        }

        @Override
        public long openRO(LPSZ name) {
            if (isArmed && Thread.currentThread() == mainThread && isCalledFrom("openColumnsRO") && ++mainOpenRoCount == 2) {
                isMainAtSecondPartition = true;
            }
            return super.openRO(name);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            if (isArmed && !isMainOpenFailed && Thread.currentThread() == mainThread && isCalledFrom("openColumnsRW") && ++mainOpenRwCount == 2) {
                waitFor(() -> isWorkerFsyncFailed);
                isMainOpenFailed = true;
                return -1;
            }
            return super.openRW(name, opts);
        }

        private static void waitFor(BooleanSupplier condition) {
            final long deadline = System.currentTimeMillis() + 10_000;
            while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
                Os.pause();
            }
        }
    }
}
