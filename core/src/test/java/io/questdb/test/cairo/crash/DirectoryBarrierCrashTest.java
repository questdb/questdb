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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

/**
 * Every commit mode that promises durability keeps its promise with only the directory barriers it takes.
 * <p>
 * The crash model is strict POSIX ({@link CrashFaultFilesFacade}): a directory entry survives a power loss
 * only if its parent directory was fsynced after the entry appeared, or a {@code syncfs} ran. Nothing is
 * written back by the kernel unless a test says so.
 * <ul>
 *   <li>SYNC fsyncs a partition directory after its column files exist, and the table directory after it,
 *   before the commit that publishes the partition. A barrier issued before the files exist persists an
 *   empty directory.</li>
 *   <li>SYNC fsyncs each partition directory in which ALTER COLUMN TYPE creates converted column files, and the
 *   partition directory a parquet conversion or the switch of a native partition to its parquet file creates and
 *   then the table directory, before the commit that publishes them. A failed one poisons the engine, as every
 *   failed barrier does.</li>
 *   <li>SYNC and ADAPTIVE fsync each column file ALTER COLUMN TYPE converts, data before aux, before the commit
 *   that publishes it. A failed one poisons the engine too.</li>
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
    public void testAdaptiveWalSegmentsSurviveCrash() throws Exception {
        assertWalSegmentsSurviveCrash("adaptive");
    }

    @Test
    public void testSyncAlterColumnTypeNonWalFailedBarrierIsFailStop() throws Exception {
        assertSyncSchemaChangeBarrierFailureIsFailStop(false, false, "ALTER TABLE x ALTER COLUMN v TYPE LONG", "convertColumn0");
    }

    @Test
    public void testSyncAlterColumnTypeNonWalFailedFixedFsyncIsFailStop() throws Exception {
        assertColumnConversionFsyncFailureIsFailStop("sync", false, "LONG", 0);
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
    public void testSyncConvertToParquetNonWalSurvivesCrash() throws Exception {
        assertSyncSchemaChangeSurvivesCrash(false, false, "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'", this::assertParquetPartitionNames);
    }

    @Test
    public void testSyncConvertToParquetWalFailedBarrierIsFailStop() throws Exception {
        assertSyncSchemaChangeBarrierFailureIsFailStop(true, false, "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'", "fsyncConvertedPartitionDir");
    }

    @Test
    public void testSyncConvertToParquetWalSurvivesCrash() throws Exception {
        assertSyncSchemaChangeSurvivesCrash(true, false, "ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'", this::assertParquetPartitionNames);
    }

    @Test
    public void testSyncMultiPartitionCommitSurvivesCrash() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        runWithCrashFacade(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            markDurableBaseline();
            execute("""
                    INSERT INTO x VALUES
                        ('2024-01-02T00:00:00.000000Z', 2),
                        ('2024-01-03T00:00:00.000000Z', 3),
                        ('2024-01-04T00:00:00.000000Z', 4)
                    """);
            crashAndReopen();
            assertRows("""
                    ts\tv
                    2024-01-01T00:00:00.000000Z\t1
                    2024-01-02T00:00:00.000000Z\t2
                    2024-01-03T00:00:00.000000Z\t3
                    2024-01-04T00:00:00.000000Z\t4
                    """);
            releaseEngineHandles();
        });
    }

    @Test
    public void testSyncO3IntoLastPartitionSurvivesCrash() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        runWithCrashFacade(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO x VALUES
                        ('2024-01-01T00:00:00.000000Z', 1),
                        ('2024-01-02T00:00:00.000000Z', 2),
                        ('2024-01-02T02:00:00.000000Z', 3)
                    """);
            markDurableBaseline();
            // O3 into the last partition writes a new version of it, which the writer then reopens.
            execute("INSERT INTO x VALUES ('2024-01-02T01:00:00.000000Z', 4)");
            crashAndReopen();
            assertRows("""
                    ts\tv
                    2024-01-01T00:00:00.000000Z\t1
                    2024-01-02T00:00:00.000000Z\t2
                    2024-01-02T01:00:00.000000Z\t4
                    2024-01-02T02:00:00.000000Z\t3
                    """);
            releaseEngineHandles();
        });
    }

    @Test
    public void testSyncPartitionSwitchSurvivesCrash() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        runWithCrashFacade(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            markDurableBaseline();
            execute("INSERT INTO x VALUES ('2024-01-02T00:00:00.000000Z', 2)");
            crashAndReopen();
            assertRows("""
                    ts\tv
                    2024-01-01T00:00:00.000000Z\t1
                    2024-01-02T00:00:00.000000Z\t2
                    """);
            releaseEngineHandles();
        });
    }

    @Test
    public void testSyncSwitchToParquetNonWalFailedBarrierIsFailStop() throws Exception {
        assertSyncSchemaChangeBarrierFailureIsFailStop(false, this::stageParquetForSwitch, this::switchFirstPartitionToParquet, "fsyncConvertedPartitionDir");
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
    public void testSyncWalApplyNewPartitionSurvivesCrash() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        runWithCrashFacade(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("x");
            markDurableBaseline();
            execute("INSERT INTO x VALUES ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();
            recoverAfterCrash(new TableToken[]{token});
            assertNotSuspended(token);
            assertRows("""
                    ts\tv
                    2024-01-01T00:00:00.000000Z\t1
                    2024-01-02T00:00:00.000000Z\t2
                    """);
            releaseEngineHandles();
        });
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
     * Reads the index files of every partition.
     */
    /**
     * Fails one fsync of a change, in the given commit mode. The engine must be poisoned and refuse the next
     * write; a non-WAL statement must also fail with a data-sync {@link CairoError}, and a WAL apply must not
     * merely suspend the table while the engine keeps taking writes.
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
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        final FsyncFailureFacade ff = new FsyncFailureFacade(caller, skipCount);
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
            Assert.assertEquals("the injected fsync failure must have been reached", 1, ff.failureCount);
            Assert.assertTrue("a failed fsync must poison the engine, failure: " + failure, isPoisoned);
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
        });
    }

    private void assertIndexedSymbolFilter() throws Exception {
        assertQuery("SELECT ts FROM x WHERE s = 'a'").timestamp("ts").returns("""
                ts
                2024-01-01T00:00:00.000000Z
                2024-01-03T00:00:00.000000Z
                """);
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

    private void runWithFacade(CrashFaultFilesFacade facade, TestUtils.LeakProneCode body) throws Exception {
        assumeCrashHarnessSupported();
        crashFf = facade;
        crashFf.setDbRoot(root);
        assertMemoryLeak(crashFf, body);
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
            Assert.assertEquals(TableWriter.SWITCH_OK, writer.switchNativePartitionWithParquet(partitionTimestamp, 0));
        }
    }

    /**
     * Once armed, fails one {@code fsync} or {@code fsyncAndClose} called from {@code caller}, the first after
     * {@code skipCount} such calls, with a genuine data-sync failure, then behaves normally.
     */
    private static final class FsyncFailureFacade extends TestFilesFacadeImpl {
        private final String caller;
        int failureCount;
        boolean isArmed;
        private int skipCount;

        FsyncFailureFacade(String caller, int skipCount) {
            this.caller = caller;
            this.skipCount = skipCount;
        }

        @Override
        public void fsync(long fd) {
            if (isFailureDue()) {
                throw CairoException.dataSyncFailure(5, "fsync").put("injected fsync failure");
            }
            super.fsync(fd);
        }

        @Override
        public void fsyncAndClose(long fd) {
            if (isFailureDue()) {
                // FilesFacadeImpl closes the fd before it throws. So must this fake, or the leak is its own.
                super.close(fd);
                throw CairoException.dataSyncFailure(5, "fsyncAndClose").put("injected fsync failure");
            }
            super.fsyncAndClose(fd);
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
}
