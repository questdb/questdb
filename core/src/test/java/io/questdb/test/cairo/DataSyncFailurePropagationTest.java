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
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.CopyWalSegmentUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.fuzz.FailureFileFacade;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Each workload records its flush paths, then fails each distinct path in all four modes. The engine fence alone
 * is not a handler oracle: durable-mode failures must retain their data-sync cause, and WAL apply and guarded
 * engine DDL must throw a stop rather than silently suspend or succeed. Held writer workloads observe distress
 * before close; SQL-owned writers are not directly observable through the facade, so their distress is not
 * claimed as covered. NOSYNC/ASYNC must not fence the engine; existing housekeeping distress remains unchanged.
 * A SYNC bystander on the same facade checks path-based ownership on Linux and the conservative broadcast on
 * platforms that cannot resolve an fd's path.
 */
@RunWith(Parameterized.class)
public class DataSyncFailurePropagationTest extends AbstractCairoTest {
    private static final int EIO = 5;
    private final String commitMode;
    private FlushFailureFacade ff;
    private Boolean writerDistressedAtFailure;

    public DataSyncFailurePropagationTest(String commitMode) {
        this.commitMode = commitMode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{"nosync"}, {"async"}, {"sync"}, {"adaptive"}});
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // Windows takes no directory flushes and fails some of the others differently; the paths are the same.
        Assume.assumeFalse(Os.isWindows());
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        // A save point after every apply batch, so its flushes are among the ones failed.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        ff = new FlushFailureFacade();
    }

    @Test
    public void testCheckpointCreateAndRelease() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    try {
                        execute("CHECKPOINT CREATE");
                    } finally {
                        execute("CHECKPOINT RELEASE");
                    }
                }
        );
    }

    @Test
    public void testCheckpointCreateIsRefusedAfterAFailedFlush() throws Exception {
        Assume.assumeTrue(CommitMode.isPowerLossDurable(CommitMode.fromString(commitMode)));
        // A checkpoint, and the Enterprise incremental backup that takes one without the final sync(), copies the
        // tables' state. Once a flush of that state has failed the copy is refused: where nothing halts the process
        // (an embedded engine, a test JVM) the fence is the stop.
        assertMemoryLeak(ff, () -> {
            createWalTable("t");
            failFirstFlush("t", table -> {
                insertDays(table, "2024-01-04", 1);
                drainWalQueue();
            });
            Assert.assertTrue("the failed flush stops the engine", engine.isDurabilityFailed());
            boolean created = false;
            try {
                execute("CHECKPOINT CREATE");
                created = true;
            } catch (CairoError e) {
                TestUtils.assertContains(e.getMessage(), "engine is poisoned");
            } finally {
                engine.resetDurabilityFailure();
                releasePools();
            }
            if (created) {
                execute("CHECKPOINT RELEASE");
                Assert.fail("CHECKPOINT CREATE must be refused on a stopped engine");
            }
        });
    }

    @Test
    public void testCreateAndDropLiveView() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("CREATE LIVE VIEW lv_" + table + " FLUSH EVERY 100ms START FROM NOW AS SELECT ts, s, sum(i) OVER (PARTITION BY s ORDER BY ts RANGE BETWEEN '1' SECOND PRECEDING AND CURRENT ROW) AS c FROM " + table);
                    execute("DROP LIVE VIEW lv_" + table);
                }
        );
    }

    @Test
    public void testCreateAndDropView() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("CREATE VIEW v_" + table + " AS (SELECT ts, s FROM " + table + " WHERE i > 2)");
                    drainWalAndViewQueues();
                    execute("ALTER VIEW v_" + table + " AS (SELECT ts, i FROM " + table + ")");
                    drainWalAndViewQueues();
                    execute("DROP VIEW v_" + table);
                    drainWalAndViewQueues();
                }
        );
    }

    @Test
    public void testCreateAndRefreshMatView() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("CREATE MATERIALIZED VIEW mv_" + table + " AS (SELECT ts, count() c FROM " + table + " SAMPLE BY 1h) PARTITION BY DAY");
                    drainWalAndMatViewQueues();
                    insertDays(table, "2024-01-04", 1);
                    drainWalAndMatViewQueues();
                }
        );
    }

    @Test
    public void testCreateNonWalTable() throws Exception {
        assertEveryFlushFailureFollowsMode(
                table -> {
                },
                table -> execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL")
        );
    }

    @Test
    public void testCreateWalTable() throws Exception {
        assertEveryFlushFailureFollowsMode(
                table -> {
                },
                table -> execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL")
        );
    }

    @Test
    public void testFlushFailureStopsEngineWhicheverFacadeRaisesIt() throws Exception {
        // A failed flush is raised by a facade's own method, by an interface default the facade does not override,
        // or by the facade a wrapper forwards to. Whichever raises it, the engine configured with the facade stops.
        assertMemoryLeak(() -> {
            final StringBuilder failures = new StringBuilder();
            final String[] everyFlush = {"barrierFsync", "fdatasync", "fsync", "fsyncAndClose", "fsyncDurable", "msync", "syncfs"};
            assertFlushFailuresFollowMode("FilesFacadeImpl", new FilesFacadeImpl(), everyFlush, failures);
            assertFlushFailuresFollowMode("FailureFileFacade", new FailureFileFacade(new FilesFacadeImpl()), everyFlush, failures);
            // syncfs is the one interface default that flushes on its own; barrierFsync and fsyncDurable default to
            // the facade's fdatasync and fsync.
            assertFlushFailuresFollowMode("interface defaults", facadeWithInterfaceDefaults(new FilesFacadeImpl()), new String[]{"syncfs"}, failures);
            if (!failures.isEmpty()) {
                Assert.fail("[mode=" + commitMode + "]\n" + failures);
            }
        });
    }

    @Test
    public void testInVolumeTableFailureStopsOnlyTheEngineThatOwnsIt() throws Exception {
        Assume.assumeTrue(CommitMode.isPowerLossDurable(CommitMode.fromString(commitMode)));
        // A table created IN VOLUME lives behind a soft link, so the OS names its files by the volume's path and not
        // by the database root's. A failed flush of one of them must still stop only the engine whose volume it is,
        // not a second engine on the same facade object.
        Assume.assumeTrue("the owner is found by the failed file's path, which only Linux tells", Os.isLinux());
        assertMemoryLeak(ff, () -> {
            final File volume = new File(temp.newFolder(), "volume");
            Assert.assertTrue(volume.mkdir());
            try (Path p = new Path()) {
                configuration.getVolumeDefinitions().of("vol->" + volume.getAbsolutePath(), p, root);
            }
            final File bystanderRoot = new File(temp.newFolder(), "db");
            Assert.assertTrue(bystanderRoot.mkdir());
            try (CairoEngine bystander = new CairoEngine(bystanderConfiguration(bystanderRoot.getAbsolutePath(), ff))) {
                execute("CREATE TABLE t (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL IN VOLUME 'vol'");
                failFirstFlush("t", table -> {
                    insertDays(table, "2024-01-01", 1);
                    drainWalQueue();
                });
                Assert.assertTrue("the failed flush stops the engine that owns the table", engine.isDurabilityFailed());
                Assert.assertFalse(
                        "the failed flush stopped an engine that does not own the file [failure=" + bystander.getDurabilityFailure() + ']',
                        bystander.isDurabilityFailed()
                );
            } finally {
                configuration.getVolumeDefinitions().clear();
                engine.resetDurabilityFailure();
                releasePools();
            }
        });
    }

    @Test
    public void testHeldTableWriterAddColumn() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> {
                    try (SqlCompiler compiler = engine.getSqlCompiler();
                         AlterOperation operation = compiler.compile("ALTER TABLE " + table + " ADD COLUMN c LONG", sqlExecutionContext).getAlterOperation();
                         TableWriter writer = getWriter(table)) {
                        try {
                            writer.applyOutsideWal(operation, true);
                        } finally {
                            // Observe before close/rollback can distress or discard the owner for another reason.
                            writerDistressedAtFailure = ff.getInjection() != null ? writer.isDistressed() : null;
                        }
                    }
                }
        );
    }

    @Test
    public void testHeldWalWriterCommit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 0);
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    try (WalWriter writer = getWalWriter(table)) {
                        try {
                            final TableWriter.Row row = writer.newRow(MicrosTimestampDriver.floor("2024-01-04"));
                            row.putInt(1, 42);
                            row.append();
                            writer.commit();
                        } finally {
                            writerDistressedAtFailure = ff.getInjection() != null ? writer.isDistressed() : null;
                        }
                    }
                }
        );
    }

    @Test
    public void testNonWalAddColumn() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> execute("ALTER TABLE " + table + " ADD COLUMN c LONG")
        );
    }

    @Test
    public void testNonWalAlterColumnType() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> execute("ALTER TABLE " + table + " ALTER COLUMN i TYPE LONG")
        );
    }

    @Test
    public void testNonWalColumnPurge() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> {
                    execute("UPDATE " + table + " SET i = i + 1");
                    engine.releaseAllReaders();
                    drainPurgeJob();
                }
        );
    }

    @Test
    public void testNonWalConvertPartitionToNative() throws Exception {
        assertEveryFlushFailureFollowsMode(
                table -> {
                    createNonWalTable(table);
                    execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
                },
                table -> execute("ALTER TABLE " + table + " CONVERT PARTITION TO NATIVE WHERE ts < '2024-01-03'")
        );
    }

    @Test
    public void testNonWalConvertPartitionToParquet() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'")
        );
    }

    @Test
    public void testNonWalDetachAttachPartition() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> {
                    execute("ALTER TABLE " + table + " DETACH PARTITION LIST '2024-01-01'");
                    renameDetachedToAttachable(table, "2024-01-01");
                    execute("ALTER TABLE " + table + " ATTACH PARTITION LIST '2024-01-01'");
                }
        );
    }

    @Test
    public void testNonWalDropAndRenameColumnAndAddIndex() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> {
                    execute("ALTER TABLE " + table + " RENAME COLUMN v TO v2");
                    execute("ALTER TABLE " + table + " DROP COLUMN v2");
                    execute("ALTER TABLE " + table + " ALTER COLUMN s ADD INDEX");
                }
        );
    }

    @Test
    public void testNonWalDropPartition() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> execute("ALTER TABLE " + table + " DROP PARTITION LIST '2024-01-01'")
        );
    }

    @Test
    public void testNonWalInsertAcrossPages() throws Exception {
        // Small pages, so the row API releases (and in sync flushes) pages as it appends.
        node1.setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, 4096);
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> insertDays(table, "2024-01-04", 200)
        );
    }

    @Test
    public void testNonWalO3MergeIntoNativePartitions() throws Exception {
        assertEveryFlushFailureFollowsMode(
                table -> {
                    execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                    insertDays(table, "2024-01-01", 3);
                },
                table -> insertOutOfOrder(table)
        );
    }

    @Test
    public void testNonWalO3MergeIntoParquetPartition() throws Exception {
        assertEveryFlushFailureFollowsMode(
                table -> {
                    execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                    insertDays(table, "2024-01-01", 3);
                    execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
                },
                table -> insertOutOfOrder(table)
        );
    }

    @Test
    public void testNonWalTtlDropsPartitions() throws Exception {
        assertEveryFlushFailureFollowsMode(
                table -> {
                    execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY TTL 1 DAY BYPASS WAL");
                    insertDays(table, "2024-01-01", 3);
                },
                table -> execute("INSERT INTO " + table + " VALUES ('2024-01-06T00:00:00.000000Z', 1, 'a', 'ttl')")
        );
    }

    @Test
    public void testNonWalTruncate() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> execute("TRUNCATE TABLE " + table)
        );
    }

    @Test
    public void testNonWalUpdate() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> execute("UPDATE " + table + " SET i = i + 1 WHERE ts < '2024-01-02'")
        );
    }

    @Test
    public void testWalAddColumn() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("ALTER TABLE " + table + " ADD COLUMN c LONG");
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalAddColumnRollsUncommittedRows() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    // A column added while the writer holds uncommitted rows moves them into a new segment.
                    try (WalWriter writer = engine.getWalWriter(engine.verifyTableName(table))) {
                        TableWriter.Row row = writer.newRow(MicrosTimestampDriver.floor("2024-01-04"));
                        row.putInt(1, 1);
                        row.append();
                        writer.commit();
                        row = writer.newRow(MicrosTimestampDriver.floor("2024-01-04T01"));
                        row.putInt(1, 2);
                        row.append();
                        writer.addColumn("c", ColumnType.LONG, AllowAllSecurityContext.INSTANCE);
                        writer.commit();
                    }
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalAlterColumnType() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("ALTER TABLE " + table + " ALTER COLUMN i TYPE LONG");
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalApplyBlockOfTransactions() throws Exception {
        assertEveryFlushFailureFollowsMode(
                table -> {
                    execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    insertDays(table, "2024-01-01", 3);
                    drainWalQueue();
                },
                table -> {
                    execute("INSERT INTO " + table + " VALUES ('2024-01-01T10:30:00.000000Z', 101, 'b', 'w1')");
                    execute("INSERT INTO " + table + " VALUES ('2024-01-02T10:30:00.000000Z', 102, 'c', 'w2')");
                    execute("INSERT INTO " + table + " VALUES ('2024-01-03T23:30:00.000000Z', 103, 'd', 'w3')");
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalApplyO3MergeIntoNativePartitions() throws Exception {
        assertEveryFlushFailureFollowsMode(
                table -> {
                    execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    insertDays(table, "2024-01-01", 3);
                    drainWalQueue();
                },
                table -> {
                    insertOutOfOrder(table);
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalApplyO3MergeIntoParquetPartition() throws Exception {
        assertEveryFlushFailureFollowsMode(
                table -> {
                    execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    insertDays(table, "2024-01-01", 3);
                    drainWalQueue();
                    execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
                    drainWalQueue();
                },
                table -> {
                    insertOutOfOrder(table);
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalConvertPartitionToParquet() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalDedupUpsert() throws Exception {
        assertEveryFlushFailureFollowsMode(
                table -> {
                    execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
                    insertDays(table, "2024-01-01", 3);
                    drainWalQueue();
                },
                table -> {
                    insertDays(table, "2024-01-01", 2);
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalDropAndRenameTable() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    // The dropped table's writer is closed by the next apply, outside the workload: a failed flush
                    // inside a writer close leaves that writer's files open in a test, where nothing halts.
                    execute("RENAME TABLE " + table + " TO " + table + "_r");
                    drainWalQueue();
                    execute("DROP TABLE " + table + "_r");
                }
        );
    }

    @Test
    public void testWalDropPartition() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("ALTER TABLE " + table + " DROP PARTITION LIST '2024-01-01'");
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalFirstInsert() throws Exception {
        assertEveryFlushFailureFollowsMode(
                table -> execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL"),
                table -> {
                    insertDays(table, "2024-01-01", 1);
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalInsertAcrossPages() throws Exception {
        // Small pages, so the WAL writer's row API flips (and in sync flushes) pages as it appends.
        // Fixed-size columns only: a rollback after a failed page flip of a VARCHAR column crashes the JVM in a test
        // (production halts at the failed flush first).
        node1.setProperty(PropertyKey.CAIRO_WAL_WRITER_DATA_APPEND_PAGE_SIZE, 4096);
        assertEveryFlushFailureFollowsMode(
                table -> execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, l LONG) TIMESTAMP(ts) PARTITION BY DAY WAL"),
                table -> {
                    execute("INSERT INTO " + table + " SELECT timestamp_sequence('2024-01-01', 60_000_000L), x::INT, x FROM long_sequence(3000)");
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalSegmentCopyFlushFailureKeepsItsCause() throws Exception {
        Assume.assumeTrue(CommitMode.isPowerLossDurable(CommitMode.fromString(commitMode)));
        // Uncommitted rows roll into a new segment when a column's type changes under them. A flush failure inside
        // that copy is wrapped in "failed to copy column file" and must stay its cause: the writer's handler then
        // sees the flag, drops the pending durable txn and raises the stop, with or without a halt installed.
        assertMemoryLeak(ff, () -> {
            final int failAt = segmentCopyFlushOf("t_dry");
            Assert.assertTrue("the segment copy flushes in " + commitMode, failAt > 0);
            releasePools();
            createWalTable("t");
            try (WalWriter writer = getWalWriter("t")) {
                appendRow(writer);
                execute("ALTER TABLE t ALTER COLUMN i TYPE LONG");
                ff.arm(failAt);
                try {
                    writer.commit();
                    Assert.fail("the commit must raise the stop");
                } catch (CairoError e) {
                    // the handler's stop carries the flush failure; the fence's refusal does not
                    Assert.assertTrue("the stop carries the flush failure [error=" + e + ']', CairoException.isDataSyncFailure(e));
                } finally {
                    ff.disarm();
                }
                Assert.assertNotNull("the segment copy's flush was failed", ff.getInjection());
            }
            Assert.assertTrue(engine.isDurabilityFailed());
            engine.resetDurabilityFailure();
            releasePools();
        });
    }

    @Test
    public void testWalTruncate() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("TRUNCATE TABLE " + table);
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalUpdate() throws Exception {
        assertEveryFlushFailureFollowsMode(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("UPDATE " + table + " SET i = i + 1 WHERE ts < '2024-01-02'");
                    drainWalQueue();
                }
        );
    }

    private static CairoConfiguration bystanderConfiguration(String root, FilesFacade ff) {
        return engineConfiguration(root, ff, CommitMode.SYNC);
    }

    private static void appendRow(WalWriter writer) {
        final TableWriter.Row row = writer.newRow(1_704_326_400_000_000L); // 2024-01-04
        row.putInt(1, 1);
        row.putSym(2, "a");
        row.putVarchar(3, new io.questdb.std.str.Utf8String("a varchar too long to inline"));
        row.append();
    }

    private static void callFlush(FilesFacade ff, String operation) {
        switch (operation) {
            case "barrierFsync" -> ff.barrierFsync(-1);
            case "fdatasync" -> ff.fdatasync(-1);
            case "fsync" -> ff.fsync(-1);
            case "fsyncAndClose" -> ff.fsyncAndClose(-1);
            case "fsyncDurable" -> ff.fsyncDurable(-1);
            case "msync" -> ff.msync(1, 1, false);
            case "syncfs" -> ff.syncfs(-1);
            default -> throw new IllegalArgumentException(operation);
        }
    }

    private static void createNonWalTable(String table) throws Exception {
        execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        insertDays(table, "2024-01-01", 3);
    }

    private static void createWalTable(String table) throws Exception {
        execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
        insertDays(table, "2024-01-01", 3);
        drainWalQueue();
    }

    private static CairoConfiguration engineConfiguration(String root, FilesFacade ff, int commitMode) {
        return new DefaultTestCairoConfiguration(root) {
            @Override
            public int getCommitMode() {
                return commitMode;
            }

            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }
        };
    }

    // Fails the workload's first flush; the workload's own error is not the check.
    private void failFirstFlush(String table, Workload action) throws Exception {
        ff.arm(1);
        try {
            action.run(table);
        } catch (Throwable ignore) {
            // the statement or the apply fails; the check is on the engine
        } finally {
            ff.disarm();
        }
        Assert.assertNotNull("the workload takes a flush in " + commitMode, ff.getInjection());
    }

    private static FilesFacade facadeWithInterfaceDefaults(FilesFacade delegate) {
        // implements the interface and overrides none of its defaults
        return (FilesFacade) Proxy.newProxyInstance(
                FilesFacade.class.getClassLoader(),
                new Class<?>[]{FilesFacade.class},
                (proxy, method, args) -> {
                    if (method.isDefault()) {
                        return InvocationHandler.invokeDefault(proxy, method, args);
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                }
        );
    }

    private static void insertDays(String table, String firstDay, int days) throws Exception {
        execute("INSERT INTO " + table + " SELECT timestamp_sequence('" + firstDay + "', 7_200_000_000L), x::INT, " +
                "rnd_symbol('a', 'b', 'c'), rnd_varchar(1, 40, 0) FROM long_sequence(" + days * 12 + ")");
    }

    private static void insertOutOfOrder(String table) throws Exception {
        execute("INSERT INTO " + table + " SELECT timestamp_sequence('2024-01-01T01:00:00.000000Z', 10_800_000_000L), " +
                "(1_000 + x)::INT, rnd_symbol('a', 'b', 'z'), rnd_varchar(1, 40, 0) FROM long_sequence(12)");
    }

    private void assertEveryFlushFailureFollowsMode(Workload setup, Workload action) throws Exception {
        assertMemoryLeak(ff, () -> {
            // its own install root, so it shares no checkpoint or COPY directory with the engine under test
            final File bystanderRoot = new File(temp.newFolder(), "db");
            Assert.assertTrue(bystanderRoot.mkdir());
            try (CairoEngine bystander = new CairoEngine(bystanderConfiguration(bystanderRoot.getAbsolutePath(), ff))) {
                assertEveryFlushFailureFollowsMode(setup, action, bystander);
            }
        });
    }

    private void assertEveryFlushFailureFollowsMode(Workload setup, Workload action, CairoEngine bystander) throws Exception {
        setup.run("dry_run");
        ff.arm(-1);
        action.run("dry_run");
        ff.disarm();
        // A workload may take no flush in a mode (a non-WAL O3 commit in adaptive), which leaves nothing to fail.
        // Flushes reached through the same stack take the same path to a handler, so only the first is failed.
        final int flushCount = ff.getFlushCount();
        final IntList firstOfEachPath = new IntList();
        firstOfEachPath.addAll(ff.getFirstFlushOfEachPath());

        final StringBuilder failures = new StringBuilder();
        int injectedCount = 0;
        releasePools();
        for (int i = 0, n = firstOfEachPath.size(); i < n; i++) {
            final int failAt = firstOfEachPath.getQuick(i);
            final String table = "t_" + failAt;
            final HashSet<String> openFilesBefore = openFiles();
            final long openOsFilesBefore = io.questdb.std.Files.getOpenFileCount();
            setup.run(table);
            writerDistressedAtFailure = null;
            ff.arm(failAt);
            Throwable outcome = null;
            try {
                action.run(table);
            } catch (Throwable failure) {
                outcome = failure;
            } finally {
                ff.disarm();
            }
            final Throwable injection = ff.getInjection();
            if (injection != null) {
                injectedCount++;
            }
            if (injection != null && engine.isDurabilityFailed() != isDurableMode()) {
                failures.append("flush ").append(failAt).append(" of ").append(flushCount)
                        .append(" has the wrong engine fence for the mode, at:\n").append(stackOf(injection));
            }
            if (injection != null && isDurableMode()) {
                // A factory-set fence alone proves nothing about propagation. Apply and the engine's guarded DDL
                // must stop, not merely suspend. Other paths must at least retain the data-sync cause; the held
                // writer workloads additionally observe distress before the pool can discard their owner.
                if (!CairoException.isDataSyncFailure(outcome) || requiresStop(injection) && !(outcome instanceof CairoError)) {
                    final TableToken token = engine.getTableTokenIfExists(table);
                    failures.append("flush ").append(failAt).append(" of ").append(flushCount)
                            .append(" lost its required data-sync outcome [requiresStop=").append(requiresStop(injection))
                            .append(", outcome=").append(outcome)
                            .append(", suspended=").append(token != null && engine.getTableSequencerAPI().isSuspended(token))
                            .append("], at:\n").append(stackOf(injection));
                }
            }
            // Base already distresses a TableWriter on a failed _todo_ housekeeping flush in every mode:
            // clearTodoLog -> handleHousekeepingException. Do not demand that NOSYNC/ASYNC undo that behaviour.
            final boolean expectedDistress = isDurableMode()
                    || hasFrame(injection, "io.questdb.cairo.TableWriter", "clearTodoLog");
            if (injection != null && writerDistressedAtFailure != null
                    && writerDistressedAtFailure != expectedDistress) {
                failures.append("flush ").append(failAt).append(" has wrong held-writer distress: ")
                        .append(writerDistressedAtFailure).append(", at:\n").append(stackOf(injection));
            }
            // Linux locates the owner by path. Elsewhere the path is unknown and every durable engine sharing the
            // facade must be fenced; the bystander is SYNC even when the tested engine is NOSYNC/ASYNC.
            if (injection != null && bystander.isDurabilityFailed() != !Os.isLinux()) {
                failures.append("flush ").append(failAt).append(" of ").append(flushCount)
                        .append(" has the wrong bystander fence [failure=")
                        .append(bystander.getDurabilityFailure()).append("], at:\n").append(stackOf(injection));
            }
            bystander.resetDurabilityFailure();
            engine.resetDurabilityFailure();
            releasePools();
            // Whatever the failed flush interrupted, its files are closed by the time the pools let go of the table,
            // not by the next statement that reuses the object holding them.
            if (injection != null) {
                final StringBuilder leaked = new StringBuilder();
                for (String fd : openFiles()) {
                    if (!openFilesBefore.contains(fd)) {
                        leaked.append(' ').append(fd.indexOf('=') > 0 ? fd : fd + '=' + io.questdb.std.Files.findPathOfFd(Long.parseLong(fd)));
                    }
                }
                // A no-cache fd is counted, not listed.
                final long leakedOsFiles = io.questdb.std.Files.getOpenFileCount() - openOsFilesBefore;
                if (!leaked.isEmpty() || leakedOsFiles > 0) {
                    failures.append("flush ").append(failAt).append(" of ").append(flushCount)
                            .append(" left ").append(leakedOsFiles).append(" file(s) open:").append(leaked)
                            .append(", at:\n").append(stackOf(injection));
                }
            }
        }
        if (!failures.isEmpty()) {
            Assert.fail("[mode=" + commitMode + "]\n" + failures);
        }
        Assert.assertTrue("most flushes of the dry run were not reached again", injectedCount * 2 >= firstOfEachPath.size());
    }

    private void assertFlushFailuresFollowMode(String shape, FilesFacade facade, String[] operations, StringBuilder failures) throws Exception {
        final File root = new File(temp.newFolder(), "db");
        Assert.assertTrue(root.mkdir());
        try (CairoEngine owner = new CairoEngine(engineConfiguration(root.getAbsolutePath(), facade, CommitMode.fromString(commitMode)))) {
            for (String operation : operations) {
                try {
                    callFlush(facade, operation);
                    failures.append(shape).append(' ').append(operation).append(": the flush did not fail\n");
                } catch (CairoException e) {
                    if (!e.isDataSyncFailure()) {
                        failures.append(shape).append(' ').append(operation).append(": not raised as a failed flush [")
                                .append(e.getFlyweightMessage()).append("]\n");
                    }
                }
                if (owner.isDurabilityFailed() != isDurableMode()) {
                    failures.append(shape).append(' ').append(operation).append(": wrong fence for mode\n");
                }
                owner.resetDurabilityFailure();
            }
        }
    }

    private static boolean hasFrame(Throwable injection, String className, String methodName) {
        if (injection != null) {
            for (StackTraceElement frame : injection.getStackTrace()) {
                if (className.equals(frame.getClassName()) && (methodName == null || methodName.equals(frame.getMethodName()))) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isDurableMode() {
        return "sync".equals(commitMode) || "adaptive".equals(commitMode);
    }

    private static HashSet<String> openFiles() {
        final HashSet<String> fds = new HashSet<>();
        for (String fd : io.questdb.std.Files.getOpenFdDebugInfo().split(",")) {
            if (!fd.isBlank()) {
                fds.add(fd.trim());
            }
        }
        return fds;
    }

    // The first flush of the rolled segment's column copy, found by a dry run on its own table.
    private int segmentCopyFlushOf(String table) throws Exception {
        createWalTable(table);
        try (WalWriter writer = getWalWriter(table)) {
            appendRow(writer);
            execute("ALTER TABLE " + table + " ALTER COLUMN i TYPE LONG");
            ff.arm(-1);
            try {
                writer.commit();
            } finally {
                ff.disarm();
            }
        }
        // the conversion's own flushes, not those of the copy helpers of the other columns
        return ff.firstFlushFrom(CopyWalSegmentUtils.class.getName() + ".rollColumnToSegment");
    }

    private static void releasePools() {
        engine.releaseInactive();
        // A table or view the failed workload left behind keeps its pooled WAL writer until the pool's TTL.
        engine.releaseAllWalWriters();
        engine.releaseAllViewWalWriters();
    }

    private static boolean requiresStop(Throwable injection) {
        return hasFrame(injection, "io.questdb.cairo.wal.ApplyWal2TableJob", null)
                || hasFrame(injection, "io.questdb.cairo.CairoEngine", "checkpointRecover")
                || hasFrame(injection, "io.questdb.cairo.CairoEngine", "completeInit")
                || hasFrame(injection, "io.questdb.cairo.CairoEngine", "createTableOrViewOrMatViewUnsecure");
    }

    private static void renameDetachedToAttachable(String table, String partition) throws Exception {
        final java.nio.file.Path tableDir = Paths.get(root, engine.verifyTableName(table).getDirName());
        Files.move(tableDir.resolve(partition + ".detached"), tableDir.resolve(partition + ".attachable"));
    }

    private static String stackOf(Throwable injection) {
        final StringBuilder sink = new StringBuilder();
        final StackTraceElement[] frames = injection.getStackTrace();
        int printed = 0;
        for (int i = 0; i < frames.length && printed < 14; i++) {
            final String className = frames[i].getClassName();
            if (className.startsWith("io.questdb.") && !className.startsWith("io.questdb.test.")) {
                sink.append("    ").append(frames[i]).append('\n');
                printed++;
            }
        }
        return sink.toString();
    }

    @FunctionalInterface
    private interface Workload {
        void run(String table) throws Exception;
    }

    private static final class FlushFailureFacade extends TestFilesFacadeImpl {
        private final AtomicInteger flushCount = new AtomicInteger();
        private final IntList firstFlushOfEachPath = new IntList();
        private final ObjList<String> firstFlushStacks = new ObjList<>();
        private final HashSet<String> paths = new HashSet<>();
        private volatile long errnoThreadId = -1;
        private volatile int failAt = -1;
        private volatile Throwable injection;
        private volatile boolean isCounting;

        @Override
        public int errno() {
            // An injected failure returned -1 without a syscall, so the thread's errno is stale: report EIO, never
            // a transient code a caller would retry or skip.
            return Thread.currentThread().threadId() == errnoThreadId ? EIO : super.errno();
        }

        @Override
        public void fdatasync(long fd) {
            if (shouldFail()) {
                throw CairoException.dataSyncFailureOfFd(this, fd, EIO, "fdatasync", "injected fdatasync failure");
            }
            super.fdatasync(fd);
        }

        @Override
        public void fsync(long fd) {
            if (shouldFail()) {
                throw CairoException.dataSyncFailureOfFd(this, fd, EIO, "fsync", "injected fsync failure");
            }
            super.fsync(fd);
        }

        @Override
        public void fsyncAndClose(long fd) {
            if (shouldFail()) {
                final String path = io.questdb.std.Files.findPathOfFd(fd);
                super.close(fd);
                throw CairoException.dataSyncFailure(this, path, EIO, "fsyncAndClose", "injected fsync failure");
            }
            super.fsyncAndClose(fd);
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (!async && shouldFail()) {
                throw CairoException.dataSyncFailureOfMapping(this, addr, EIO, "msync", "injected msync failure");
            }
            super.msync(addr, len, async);
        }

        @Override
        public int sync() {
            if (shouldFail()) {
                errnoThreadId = Thread.currentThread().threadId();
                return -1;
            }
            return super.sync();
        }

        @Override
        public int syncFileRange(long fd, long offset, long nbytes, int flags) {
            if (shouldFail()) {
                errnoThreadId = Thread.currentThread().threadId();
                return -1;
            }
            return super.syncFileRange(fd, offset, nbytes, flags);
        }

        @Override
        public void syncfs(long fd) {
            if (shouldFail()) {
                throw CairoException.dataSyncFailureOfFd(this, fd, EIO, "syncfs", "injected syncfs failure");
            }
            super.syncfs(fd);
        }

        void arm(int failAt) {
            errnoThreadId = -1;
            flushCount.set(0);
            firstFlushOfEachPath.clear();
            firstFlushStacks.clear();
            paths.clear();
            injection = null;
            this.failAt = failAt;
            isCounting = true;
        }

        void disarm() {
            errnoThreadId = -1;
            isCounting = false;
        }

        // The first flush of the dry run that the method ran itself (its frame is the innermost), or -1.
        int firstFlushFrom(String method) {
            for (int i = 0, n = firstFlushStacks.size(); i < n; i++) {
                if (firstFlushStacks.getQuick(i).startsWith("    " + method + '(')) {
                    return firstFlushOfEachPath.getQuick(i);
                }
            }
            return -1;
        }

        IntList getFirstFlushOfEachPath() {
            return firstFlushOfEachPath;
        }

        int getFlushCount() {
            return flushCount.get();
        }

        Throwable getInjection() {
            return injection;
        }

        private boolean shouldFail() {
            if (!isCounting) {
                return false;
            }
            final int flush = flushCount.incrementAndGet();
            if (failAt < 0) {
                synchronized (paths) {
                    final String stack = stackOf(new Throwable());
                    if (paths.add(stack)) {
                        firstFlushOfEachPath.add(flush);
                        firstFlushStacks.add(stack);
                    }
                }
                return false;
            }
            if (flush == failAt) {
                injection = new Throwable("injected flush failure " + failAt);
                return true;
            }
            return false;
        }
    }
}
