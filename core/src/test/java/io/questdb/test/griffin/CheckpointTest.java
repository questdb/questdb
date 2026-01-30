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

package io.questdb.test.griffin;

import io.questdb.Bootstrap;
import io.questdb.FactoryProvider;
import io.questdb.FreeOnExit;
import io.questdb.MemoryConfiguration;
import io.questdb.Metrics;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropertyKey;
import io.questdb.PublicPassthroughConfiguration;
import io.questdb.ServerConfiguration;
import io.questdb.cairo.BitmapIndexUtils;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CheckpointListener;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.metrics.MetricsConfiguration;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.SimpleWaitingLock;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.preferences.SettingsStore;
import io.questdb.std.CharSequenceLongHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;

import static io.questdb.PropertyKey.*;

public class CheckpointTest extends AbstractCairoTest {
    private static final TestFilesFacade testFilesFacade = new TestFilesFacade();
    private static Path path;
    private static Rnd rnd;
    private static Path triggerFilePath;
    private int rootLen;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        path = new Path();
        triggerFilePath = new Path();
        ff = testFilesFacade;
        AbstractCairoTest.setUpStatic();
    }

    @AfterClass
    public static void tearDownStatic() {
        path = Misc.free(path);
        triggerFilePath = Misc.free(triggerFilePath);
        AbstractCairoTest.tearDownStatic();
    }

    @Before
    public void setUp() {
        // sync() system call is not available on Windows, so we skip the whole test suite there.
        Assume.assumeTrue(Os.type != Os.WINDOWS);

        super.setUp();
        ff = testFilesFacade;
        path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory()).slash();
        triggerFilePath.of(configuration.getDbRoot()).parent().concat(TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME).$();
        rootLen = path.size();
        testFilesFacade.reset();

        circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public int getCircuitBreakerThrottle() {
                return 0;
            }

            @Override
            public long getQueryTimeout() {
                return 100;
            }
        };
        circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, circuitBreakerConfiguration, MemoryTag.NATIVE_CB5) {
            @Override
            protected boolean testConnection(long fd) {
                return false;
            }
        };
        circuitBreaker.setTimeout(Long.MAX_VALUE);
        ((SqlExecutionContextImpl) sqlExecutionContext).with(circuitBreaker);

        rnd = TestUtils.generateRandom(LOG);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        path.trimTo(rootLen);
        configuration.getFilesFacade().rmdir(path.slash());
        // reset inProgress for all tests
        execute("checkpoint release");
    }

    @Test
    public void testCheckpointCompleteDeletesCheckpointDir() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, name symbol, val int)");
            execute("checkpoint create");
            execute("checkpoint release");

            path.trimTo(rootLen).slash$();
            Assert.assertFalse(configuration.getFilesFacade().exists(path.$()));
        });
    }

    @Test
    public void testCheckpointCompleteWithoutPrepareIsIgnored() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, name symbol, val int)");
            // Verify that checkpoint release doesn't return errors.
            execute("checkpoint release");
        });
    }

    @Test
    public void testCheckpointCreate() throws Exception {
        assertMemoryLeak(() -> {
            for (char i = 'a'; i < 'f'; i++) {
                execute("create table " + i + " (ts timestamp, name symbol, val int)");
            }

            execute("checkpoint create");
            execute("checkpoint release");
        });
    }

    @Test
    public void testCheckpointCreateEmptyFolder() throws Exception {
        final String tableName = "test";
        path.of(configuration.getDbRoot()).concat("empty_folder").slash$();
        TestFilesFacadeImpl.INSTANCE.mkdirs(path, configuration.getMkDirMode());

        assertMemoryLeak(() -> {
            testCheckpointCreateCheckTableMetadataFiles(
                    "create table " + tableName + " (a symbol index capacity 128, b double, c long)",
                    null,
                    tableName
            );

            // Assert snapshot folder exists
            Assert.assertTrue(TestFilesFacadeImpl.INSTANCE.exists(
                    path.of(configuration.getCheckpointRoot()).slash$()
            ));
            // But snapshot/db folder does not
            Assert.assertFalse(TestFilesFacadeImpl.INSTANCE.exists(
                    path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory()).slash$()
            ));
        });
    }

    @Test
    public void testCheckpointDbWithWalTable() throws Exception {
        assertMemoryLeak(() -> {
            for (char i = 'a'; i < 'd'; i++) {
                execute("create table " + i + " (ts timestamp, name symbol, val int)");
            }

            for (char i = 'd'; i < 'f'; i++) {
                execute("create table " + i + " (ts timestamp, name symbol, val int) timestamp(ts) partition by DAY WAL");
            }

            execute("checkpoint create");
            execute("checkpoint release");
        });
    }

    @Test
    public void testCheckpointListenerOnReleased() throws Exception {
        File dir = temp.newFolder("listener_release_test");

        // Track listener callbacks - use ObjList to verify exactly one call
        final ObjList<CheckpointReleaseEvent> events = new ObjList<>();

        CheckpointListener listener = new CheckpointListener() {
            @Override
            public void onCheckpointReleased(long timestampMicros, CharSequenceLongHashMap tableDirNamesToSeqTxn) {
                // Copy the map since it's owned by caller
                CharSequenceLongHashMap copy = new CharSequenceLongHashMap();
                for (int i = 0, n = tableDirNamesToSeqTxn.size(); i < n; i++) {
                    CharSequence key = tableDirNamesToSeqTxn.keys().get(i);
                    copy.put(key.toString(), tableDirNamesToSeqTxn.get(key));
                }
                events.add(new CheckpointReleaseEvent(timestampMicros, copy));
            }

            @Override
            public void onCheckpointRestoreComplete() {
            }
        };

        try (TestServerMain server = startServerMainWithListener(dir.getAbsolutePath(), listener)) {
            // Create WAL tables
            server.execute("CREATE TABLE wal_table1 (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            server.execute("CREATE TABLE wal_table2 (y LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // Create non-WAL table (should NOT appear in callback)
            server.execute("CREATE TABLE non_wal_table (z INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            // Create a view on a WAL table (should appear in callback)
            server.execute("CREATE VIEW test_view AS SELECT * FROM wal_table1 WHERE x > 0");

            // Create a materialized view on a WAL table (should appear in callback)
            server.execute("CREATE MATERIALIZED VIEW mat_view AS SELECT ts, count() cnt FROM wal_table1 SAMPLE BY 1d");

            // Insert data to generate transactions
            server.execute("INSERT INTO wal_table1 VALUES (1, '2024-01-01T00:00:00.000000Z')");
            server.execute("INSERT INTO wal_table2 VALUES (100, '2024-01-01T00:00:00.000000Z')");
            server.execute("INSERT INTO non_wal_table VALUES (999, '2024-01-01T00:00:00.000000Z')");

            // Wait for WAL transactions to be fully sequenced before checkpointing.
            // The checkpoint reads seqTxn from the sequencer (lastTxn), so we wait for the
            // sequencer to have the expected transaction counts.
            // Note: TestServerMain runs its own background workers, so we use assertEventually
            // rather than drainWalAndMatViewQueues (which creates separate job instances).
            TableToken walTable1Token = server.getEngine().getTableTokenIfExists("wal_table1");
            TableToken walTable2Token = server.getEngine().getTableTokenIfExists("wal_table2");
            TableToken matViewToken = server.getEngine().getTableTokenIfExists("mat_view");

            // wal_table1: seqTxn=1 (one insert)
            // wal_table2: seqTxn=1 (one insert)
            // mat_view: seqTxn=3 (creation + empty refresh + actual refresh with data)
            TestUtils.assertEventually(() -> {
                long seqTxn = server.getEngine().getTableSequencerAPI().getTxnTracker(walTable1Token).getSeqTxn();
                Assert.assertEquals("wal_table1 seqTxn should be 1", 1L, seqTxn);
            });
            TestUtils.assertEventually(() -> {
                long seqTxn = server.getEngine().getTableSequencerAPI().getTxnTracker(walTable2Token).getSeqTxn();
                Assert.assertEquals("wal_table2 seqTxn should be 1", 1L, seqTxn);
            });
            TestUtils.assertEventually(() -> {
                long seqTxn = server.getEngine().getTableSequencerAPI().getTxnTracker(matViewToken).getSeqTxn();
                Assert.assertEquals("mat_view seqTxn should be 3", 3L, seqTxn);
            });

            // Verify listener not called yet
            Assert.assertEquals("Listener should not be called before checkpoint", 0, events.size());

            // Create checkpoint
            server.execute("CHECKPOINT CREATE");

            // Capture the checkpoint's timestamp from the status
            long checkpointTimestamp = server.getEngine().getCheckpointStatus().startedAtTimestamp();
            Assert.assertTrue("Checkpoint should be active after CREATE", checkpointTimestamp > 0);

            // Listener still not called (only at release)
            Assert.assertEquals("Listener should not be called at checkpoint create", 0, events.size());

            server.execute("CHECKPOINT RELEASE");

            // Verify listener was called exactly once
            Assert.assertEquals("Listener should be called exactly once", 1, events.size());

            CheckpointReleaseEvent event = events.get(0);
            // Verify callback receives the checkpoint's creation timestamp, not the release time
            Assert.assertEquals("Callback timestamp should be the checkpoint's creation timestamp",
                    checkpointTimestamp, event.timestampMicros);

            CharSequenceLongHashMap actual = event.tableDirNamesToSeqTxn;

            // Verify exact seqTxn values
            Assert.assertEquals("wal_table1 seqTxn mismatch", 1L, getSeqTxnForTable(actual, "wal_table1"));
            Assert.assertEquals("wal_table2 seqTxn mismatch", 1L, getSeqTxnForTable(actual, "wal_table2"));
            Assert.assertEquals("test_view seqTxn mismatch", 0L, getSeqTxnForTable(actual, "test_view"));
            Assert.assertEquals("mat_view seqTxn mismatch", 3L, getSeqTxnForTable(actual, "mat_view"));

            // Verify non-WAL table is NOT present
            for (int i = 0, n = actual.size(); i < n; i++) {
                CharSequence tableDirName = actual.keys().get(i);
                Assert.assertFalse("Should NOT contain non_wal_table",
                        tableDirName.toString().startsWith("non_wal_table"));
            }

            // Define allowed table name prefixes (user tables only)
            ObjHashSet<String> allowedPrefixes = new ObjHashSet<>();
            allowedPrefixes.add("wal_table1");
            allowedPrefixes.add("wal_table2");
            allowedPrefixes.add("test_view");
            allowedPrefixes.add("mat_view");
            // Note: If system tables appear in callback, add their prefixes here

            // Verify no unexpected tables appear
            for (int i = 0, n = actual.size(); i < n; i++) {
                String dirName = actual.keys().get(i).toString();
                boolean isAllowed = false;
                for (int j = 0, m = allowedPrefixes.size(); j < m; j++) {
                    if (dirName.startsWith(allowedPrefixes.get(j))) {
                        isAllowed = true;
                        break;
                    }
                }
                Assert.assertTrue("Unexpected table in callback: " + dirName, isAllowed);
            }

            // Verify exactly 4 tables (2 WAL tables + 1 view + 1 mat view)
            Assert.assertEquals("Should have exactly 4 entries", 4, actual.size());
        }
    }

    @Test
    public void testCheckpointListenerOnRestoreComplete() throws Exception {
        File dir1 = temp.newFolder("listener_restore_source");
        File dir2 = temp.newFolder("listener_restore_target");

        // Track listener callbacks - use counter to verify called exactly once
        final int[] restoreCompleteCallCount = new int[1];

        CheckpointListener listener = new CheckpointListener() {
            @Override
            public void onCheckpointReleased(long timestampMicros, CharSequenceLongHashMap tableDirNamesToSeqTxn) {
            }

            @Override
            public void onCheckpointRestoreComplete() {
                restoreCompleteCallCount[0]++;
            }
        };

        // Server 1: Create checkpoint (without custom listener - not needed here)
        try (TestServerMain server1 = startServerMain(dir1.getAbsolutePath())) {
            server1.execute("CREATE TABLE test_table (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            server1.execute("INSERT INTO test_table VALUES (1, '2024-01-01T00:00:00.000000Z')");
            TestUtils.drainWalQueue(server1.getEngine());

            server1.execute("CHECKPOINT CREATE");
            copyDirectory(dir1, dir2);
            server1.execute("CHECKPOINT RELEASE");
        }

        // Create trigger file to force checkpoint recovery in dir2
        try (Path triggerPath = new Path().of(dir2.getAbsolutePath()).concat(TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME)) {
            Files.touch(triggerPath.$());
        }

        // Verify listener not called yet
        Assert.assertEquals("Listener should not be called before restore", 0, restoreCompleteCallCount[0]);

        // Server 2: Start with trigger file and custom listener - should trigger restore
        try (@SuppressWarnings("unused") TestServerMain _server2 = startServerMainWithListener(dir2.getAbsolutePath(), listener)) {
            // Restore happens during startup, so listener should have been called exactly once
            Assert.assertEquals("Listener onCheckpointRestoreComplete should be called exactly once", 1, restoreCompleteCallCount[0]);
        }
    }

    @Test
    public void testCheckpointPrepareCheckMatViewMetaFiles() throws Exception {
        assertMemoryLeak(() -> {
            testCheckpointCreateCheckTableMetadataFiles(
                    "create table base_price (sym varchar, price double, ts timestamp) timestamp(ts) partition by DAY WAL",
                    null,
                    "base_price"
            );
            String viewSql = "select sym, last(price) as price, ts from base_price sample by 1h";
            String sql = "create materialized view price_1h as (" + viewSql + ") partition by DAY";
            testCheckpointCreateCheckTableMetadataFiles(
                    sql,
                    null,
                    "price_1h"
            );
        });
    }

    @Test
    public void testCheckpointPrepareCheckMetadataFileForDefaultInstanceId() throws Exception {
        testCheckpointPrepareCheckMetadataFile(null);
    }

    @Test
    public void testCheckpointPrepareCheckMetadataFileForNonDefaultInstanceId() throws Exception {
        testCheckpointPrepareCheckMetadataFile("foobar");
    }

    @Test
    public void testCheckpointPrepareCheckTableMetadata() throws Exception {
        testCheckpointPrepareCheckTableMetadata(false, false);
    }

    @Test
    public void testCheckpointPrepareCheckTableMetadataFilesForNonPartitionedTable() throws Exception {
        final String tableName = "test";
        assertMemoryLeak(() -> testCheckpointCreateCheckTableMetadataFiles(
                "create table " + tableName + " (a symbol, b double, c long)",
                null,
                tableName
        ));
    }

    @Test
    public void testCheckpointPrepareCheckTableMetadataFilesForNonWalSystemTable() throws Exception {
        final String sysTableName = configuration.getSystemTableNamePrefix() + "test_non_wal";
        assertMemoryLeak(() -> testCheckpointCreateCheckTableMetadataFiles(
                "create table '" + sysTableName + "' (a symbol, b double, c long);",
                null,
                sysTableName
        ));
    }

    @Test
    public void testCheckpointPrepareCheckTableMetadataFilesForPartitionedTable() throws Exception {
        final String tableName = "test";
        assertMemoryLeak(() -> testCheckpointCreateCheckTableMetadataFiles(
                "create table " + tableName + " as " +
                        " (select x, timestamp_sequence(0, 100000000000) ts from long_sequence(20)) timestamp(ts) partition by day",
                null,
                tableName
        ));
    }

    @Test
    public void testCheckpointPrepareCheckTableMetadataFilesForTableWithDroppedColumns() throws Exception {
        final String tableName = "test";
        assertMemoryLeak(() -> testCheckpointCreateCheckTableMetadataFiles(
                "create table " + tableName + " (a symbol index capacity 128, b double, c long)",
                "alter table " + tableName + " drop column c",
                tableName
        ));
    }

    @Test
    public void testCheckpointPrepareCheckTableMetadataFilesForTableWithIndex() throws Exception {
        final String tableName = "test";
        assertMemoryLeak(() -> testCheckpointCreateCheckTableMetadataFiles(
                "create table " + tableName + " (a symbol index capacity 128, b double, c long)",
                null,
                tableName
        ));
    }

    @Test
    public void testCheckpointPrepareCheckTableMetadataFilesForWalSystemTable() throws Exception {
        final String sysTableName = configuration.getSystemTableNamePrefix() + "test_wal";
        assertMemoryLeak(() -> testCheckpointCreateCheckTableMetadataFiles(
                "create table '" + sysTableName + "' (ts timestamp, a symbol, b double, c long) timestamp(ts) partition by day wal;",
                null,
                sysTableName
        ));
    }

    @Test
    public void testCheckpointPrepareCheckTableMetadataFilesForWithParameters() throws Exception {
        final String tableName = "test";
        assertMemoryLeak(() -> testCheckpointCreateCheckTableMetadataFiles(
                "create table " + tableName +
                        " (a symbol, b double, c long, ts timestamp) timestamp(ts) partition by hour with maxUncommittedRows=250000, o3MaxLag = 240s",
                null,
                tableName
        ));
    }

    @Test
    public void testCheckpointPrepareCheckTableMetadataWithColTops() throws Exception {
        testCheckpointPrepareCheckTableMetadata(true, false);
    }

    @Test
    public void testCheckpointPrepareCheckTableMetadataWithColTopsAndDroppedColumns() throws Exception {
        testCheckpointPrepareCheckTableMetadata(true, true);
    }

    @Test
    public void testCheckpointPrepareCheckTableMetadataWithDroppedColumns() throws Exception {
        testCheckpointPrepareCheckTableMetadata(true, true);
    }

    @Test
    public void testCheckpointPrepareCleansUpCheckpointDir() throws Exception {
        assertMemoryLeak(() -> {
            path.trimTo(rootLen);
            FilesFacade ff = configuration.getFilesFacade();
            int rc = ff.mkdirs(path.slash(), configuration.getMkDirMode());
            Assert.assertEquals(0, rc);

            // Create a test file.
            path.trimTo(rootLen).concat("test.txt").$();
            Assert.assertTrue(Files.touch(path.$()));

            execute("create table test (ts timestamp, name symbol, val int)");
            execute("checkpoint create", sqlExecutionContext);

            // The test file should be deleted by checkpoint-create.
            Assert.assertFalse(ff.exists(path.$()));

            execute("checkpoint release");
        });
    }

    @Test
    public void testCheckpointPrepareFailsOnCorruptedTable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "t";
            execute("create table " + tableName + " (ts timestamp, name symbol, val int)");

            // Corrupt the table by removing _txn file.
            FilesFacade ff = configuration.getFilesFacade();
            TableToken tableToken = engine.verifyTableName(tableName);

            engine.releaseInactive();
            Assert.assertTrue(ff.removeQuiet(path.of(root).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$()));

            assertException("checkpoint create", 0, "could not open txn file");
        });
    }

    @Test
    public void testCheckpointPrepareFailsOnLockedTableReader() throws Exception {
        configureCircuitBreakerTimeoutOnFirstCheck(); // trigger timeout on first check
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, name symbol, val int)");

            TableToken tableToken = engine.getTableTokenIfExists("test");
            engine.lockReadersByTableToken(tableToken);

            try {
                execute("checkpoint create");
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "timeout, query aborted");
            } finally {
                engine.unlockReaders(tableToken);
            }
        });
    }

    @Test
    public void testCheckpointPrepareFailsOnSyncError() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, name symbol, val int)");

            testFilesFacade.errorOnSync = true;
            assertException("checkpoint create", 0, "Could not sync");

            // Once the error is gone, subsequent PREPARE/COMPLETE statements should execute successfully.
            testFilesFacade.errorOnSync = false;
            execute("checkpoint create");
            execute("checkpoint release");
        });
    }

    @Test
    public void testCheckpointPrepareOnEmptyDatabase() throws Exception {
        assertMemoryLeak(() -> {
            execute("checkpoint create");
            execute("checkpoint release");
        });
    }

    @Test
    public void testCheckpointPrepareOnEmptyDatabaseWithLock() throws Exception {
        assertMemoryLeak(() -> {
            SimpleWaitingLock lock = new SimpleWaitingLock();

            circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
                @Override
                public long getQueryTimeout() {
                    return 10;
                }
            };

            try {
                engine.setWalPurgeJobRunLock(lock);
                Assert.assertFalse(lock.isLocked());
                execute("checkpoint create");
                Assert.assertTrue(lock.isLocked());
                try {
                    assertExceptionNoLeakCheck("checkpoint create");
                } catch (SqlException ex) {
                    Assert.assertTrue(lock.isLocked());
                    Assert.assertTrue(ex.getMessage().startsWith("[0] Waiting for CHECKPOINT RELEASE to be called"));
                }
                execute("checkpoint release");
                Assert.assertFalse(lock.isLocked());

                //DB is empty
                execute("checkpoint release");
                Assert.assertFalse(lock.isLocked());

                execute("checkpoint release");
                Assert.assertFalse(lock.isLocked());
            } finally {
                engine.setWalPurgeJobRunLock(null);
            }
        });
    }

    @Test
    public void testCheckpointPrepareSubsequentCallFails() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, name symbol, val int)");

            SimpleWaitingLock lock = new SimpleWaitingLock();

            circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
                @Override
                public long getQueryTimeout() {
                    return 10;
                }
            };

            engine.setWalPurgeJobRunLock(lock);
            try {
                Assert.assertFalse(lock.isLocked());
                execute("checkpoint create");
                Assert.assertTrue(lock.isLocked());
                execute("checkpoint create");
                Assert.assertTrue(lock.isLocked());
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertTrue(ex.getMessage().startsWith("[0] Waiting for CHECKPOINT RELEASE to be called"));
            } finally {
                Assert.assertTrue(lock.isLocked());
                execute("checkpoint release");
                Assert.assertFalse(lock.isLocked());

                engine.setWalPurgeJobRunLock(null);
            }
        });
    }

    @Test
    public void testCheckpointPrepareSubsequentCallFailsWithLock() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, name symbol, val int)");
            execute("checkpoint create");
            assertException(
                    "checkpoint create",
                    0,
                    "Waiting for CHECKPOINT RELEASE to be called"
            );
            execute("checkpoint release");
        });
    }

    @Test
    public void testCheckpointPreventsNonWalTableDeletion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, name symbol, val int) timestamp(ts) partition by day bypass wal;");
            execute("insert into test values ('2023-09-20T12:39:01.933062Z', 'foobar', 42);");
            execute("checkpoint create;");

            try {
                execute("drop table test;");
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not lock 'test~' [reason='checkpointInProgress']");
            }
            engine.checkpointRelease();
        });
    }

    @Test
    public void testCheckpointPreventsNonWalTableRenaming() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, name symbol, val int) timestamp(ts) partition by day bypass wal;");
            execute("insert into test values ('2023-09-20T12:39:01.933062Z', 'foobar', 42);");
            execute("checkpoint create;");

            try {
                execute("rename table test to test2;");
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table busy [reason=checkpointInProgress]");
            }
            engine.checkpointRelease();
        });
    }

    @Test
    public void testCheckpointPreventsNonWalTableTruncation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, name symbol, val int) timestamp(ts) partition by day bypass wal;");
            execute("insert into test values ('2023-09-20T12:39:01.933062Z', 'foobar', 42);");
            execute("checkpoint create;");

            try {
                execute("truncate table test;");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "there is an active query against 'test'");
            }
            engine.checkpointRelease();
        });
    }

    @Test
    public void testCheckpointRecoveryTornKeyEntry_rebuildFalse() throws Exception {
        testCheckpointRecoveryTornKeyEntry(false);
    }

    @Test
    public void testCheckpointRecoveryTornKeyEntry_rebuildTrue() throws Exception {
        testCheckpointRecoveryTornKeyEntry(true);
    }

    @Test
    public void testCheckpointRecoveryWithStaleIndex_rebuildFalse() throws Exception {
        testCheckpointRecoveryWithStaleIndex(false);
    }

    @Test
    public void testCheckpointRecoveryWithStaleIndex_rebuildTrue() throws Exception {
        testCheckpointRecoveryWithStaleIndex(true);
    }

    @Test
    public void testCheckpointRestoreIndexNonPartitioned() throws Exception {
        final String snapshotId = "00000000-0000-0000-0000-000000000000";
        final String restartedId = "123e4567-e89b-12d3-a456-426614174000";
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

            final String tableName = getTestTableName();
            execute(
                    "create table " + tableName + " as (" +
                            "select rnd_symbol('A','B','C') sym1, " +
                            "rnd_symbol('X','Y','Z') sym2, " +
                            "x " +
                            "from long_sequence(1000)" +
                            "), index(sym1), index(sym2)"
            );

            // Query using indexes before checkpoint to get expected counts
            sink.clear();
            printSql("select count() from " + tableName + " where sym1 = 'A'");
            final String sym1ACountBefore = sink.toString();

            sink.clear();
            printSql("select count() from " + tableName + " where sym2 = 'X'");
            final String sym2XCountBefore = sink.toString();

            execute("checkpoint create");

            // Insert more data after checkpoint
            execute(
                    "insert into " + tableName +
                            " select rnd_symbol('A','B','C') sym1, " +
                            "rnd_symbol('X','Y','Z') sym2, " +
                            "x + 1000 " +
                            "from long_sequence(500)"
            );

            // Release all readers and writers, but keep the snapshot dir around.
            engine.clear();
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
            engine.checkpointRecover();

            // Verify index queries return correct results (pre-checkpoint data only)
            assertSql(sym1ACountBefore, "select count() from " + tableName + " where sym1 = 'A'");
            assertSql(sym2XCountBefore, "select count() from " + tableName + " where sym2 = 'X'");

            // Verify new inserts work correctly with rebuilt indexes
            execute("insert into " + tableName + " values('A', 'X', 9999)");

            final long expectedSym1A = Long.parseLong(sym1ACountBefore.split("\n")[1].trim()) + 1;
            final long expectedSym2X = Long.parseLong(sym2XCountBefore.split("\n")[1].trim()) + 1;
            assertSql("count\n" + expectedSym1A + "\n", "select count() from " + tableName + " where sym1 = 'A'");
            assertSql("count\n" + expectedSym2X + "\n", "select count() from " + tableName + " where sym2 = 'X'");

            engine.checkpointRelease();
        });
    }

    @Test
    public void testCheckpointRestoreIndexWithColumnTop() throws Exception {
        final String snapshotId = "00000000-0000-0000-0000-000000000000";
        final String restartedId = "123e4567-e89b-12d3-a456-426614174000";
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

            final String tableName = getTestTableName();
            // Create table without indexed columns initially
            execute(
                    "create table " + tableName + " as (" +
                            "select x, " +
                            "timestamp_sequence(0, 100000000000) ts " +
                            "from long_sequence(500)" +
                            ") timestamp(ts) PARTITION BY DAY"
            );

            // Add indexed symbol columns (creates column top)
            execute("alter table " + tableName + " add column sym1 symbol index");
            execute("alter table " + tableName + " add column sym2 symbol index");

            // Insert data with the new columns
            execute(
                    "insert into " + tableName +
                            " select x + 500, " +
                            "timestamp_sequence(50000000000000, 100000000000) ts, " +
                            "rnd_symbol('A','B','C') sym1, " +
                            "rnd_symbol('X','Y','Z') sym2 " +
                            "from long_sequence(500)"
            );

            // Query using indexes before checkpoint to get expected counts
            sink.clear();
            printSql("select count() from " + tableName + " where sym1 = 'A'");
            final String sym1ACountBefore = sink.toString();

            sink.clear();
            printSql("select count() from " + tableName + " where sym2 = 'X'");
            final String sym2XCountBefore = sink.toString();

            execute("checkpoint create");

            // Release all readers and writers, but keep the snapshot dir around.
            engine.clear();
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
            engine.checkpointRecover();

            // Verify index queries return correct results
            assertSql(sym1ACountBefore, "select count() from " + tableName + " where sym1 = 'A'");
            assertSql(sym2XCountBefore, "select count() from " + tableName + " where sym2 = 'X'");

            // Verify new inserts work correctly with rebuilt indexes
            execute("insert into " + tableName + " values(9999, now(), 'A', 'X')");

            final long expectedSym1A = Long.parseLong(sym1ACountBefore.split("\n")[1].trim()) + 1;
            final long expectedSym2X = Long.parseLong(sym2XCountBefore.split("\n")[1].trim()) + 1;
            assertSql("count\n" + expectedSym1A + "\n", "select count() from " + tableName + " where sym1 = 'A'");
            assertSql("count\n" + expectedSym2X + "\n", "select count() from " + tableName + " where sym2 = 'X'");

            engine.checkpointRelease();
        });
    }

    @Test
    public void testCheckpointRestoreOrphanDirRemovalFailure() throws Exception {
        final String snapshotId = "id1";
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean rmdir(Path name, boolean lazy) {
                if (Utf8s.containsAscii(name, "tiesto~")) {
                    return false;
                }
                return super.rmdir(name, lazy);
            }
        };

        assertMemoryLeak(ff, () -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

            // 1. Create base table with data
            execute("create table test (ts timestamp, name symbol, val int) timestamp(ts) partition by day wal;");
            execute("insert into test values ('2023-09-20T12:00:00.000000Z', 'a', 10);");
            drainWalQueue();

            // 2. Checkpoint
            execute("checkpoint create");

            // 3. Drop table and create a new one after checkpoint
            execute("drop table test");
            drainWalQueue();
            drainPurgeJob();

            execute("create table tiesto (ts timestamp, name symbol, val int) timestamp(ts) partition by day wal;");


            // 4. Restore from the checkpoint - rmdir for tiesto will fail but restore should complete
            engine.clear();
            engine.closeNameRegistry();
            createTriggerFile();
            try {
                engine.checkpointRecover();
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Aborting QuestDB startup; could not remove orphan table directory, remove it manually to continue");
                TestUtils.assertContains(e.getFlyweightMessage(), "dbRoot/tiesto~");
            } finally {
                execute("checkpoint release");
            }
        });
    }

    @Test
    public void testCheckpointRestoreRebuildsBitmapIndexes() throws Exception {
        final String snapshotId = "00000000-0000-0000-0000-000000000000";
        final String restartedId = "123e4567-e89b-12d3-a456-426614174000";
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

            final String tableName = getTestTableName();
            execute(
                    "create table " + tableName + " as (" +
                            "select rnd_symbol('A','B','C') sym1, " +
                            "rnd_symbol('X','Y','Z') sym2, " +
                            "x, " +
                            "timestamp_sequence(0, 100000000000) ts " +
                            "from long_sequence(1000)" +
                            "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY"
            );

            // Query using indexes before checkpoint to get expected counts
            sink.clear();
            printSql("select count() from " + tableName + " where sym1 = 'A'");
            final String sym1ACountBefore = sink.toString();

            sink.clear();
            printSql("select count() from " + tableName + " where sym2 = 'X'");
            final String sym2XCountBefore = sink.toString();

            execute("checkpoint create");

            // Insert more data after checkpoint
            execute(
                    "insert into " + tableName +
                            " select rnd_symbol('A','B','C') sym1, " +
                            "rnd_symbol('X','Y','Z') sym2, " +
                            "x + 1000, " +
                            "timestamp_sequence(100000000000000, 100000000000) ts " +
                            "from long_sequence(500)"
            );

            // Release all readers and writers, but keep the snapshot dir around.
            engine.clear();
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
            engine.checkpointRecover();

            // Verify index queries return correct results (pre-checkpoint data only)
            assertSql(sym1ACountBefore, "select count() from " + tableName + " where sym1 = 'A'");
            assertSql(sym2XCountBefore, "select count() from " + tableName + " where sym2 = 'X'");

            // Verify new inserts work correctly with rebuilt indexes
            execute("insert into " + tableName + " values('A', 'X', 9999, now())");

            // Count should increase by 1 for both
            final long expectedSym1A = Long.parseLong(sym1ACountBefore.split("\n")[1].trim()) + 1;
            final long expectedSym2X = Long.parseLong(sym2XCountBefore.split("\n")[1].trim()) + 1;
            assertSql("count\n" + expectedSym1A + "\n", "select count() from " + tableName + " where sym1 = 'A'");
            assertSql("count\n" + expectedSym2X + "\n", "select count() from " + tableName + " where sym2 = 'X'");
            engine.checkpointRelease();
        });
    }

    @Test
    public void testCheckpointRestoresDroppedView() throws Exception {
        final String snapshotId = "id1";
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

            execute("create table test (ts timestamp, name symbol, val int) timestamp(ts) partition by day wal;");
            execute("insert into test values ('2023-09-20T12:39:01.933062Z', 'foobar', 42);");
            drainWalQueue();

            execute("create view v as select * from test where val > 0;");

            drainViewQueue();

            // sanity check: the view exists and works
            assertSql("count\n1\n", "select count() from views() where view_name = 'v';");
            assertSql("count\n1\n", "select count() from v;");

            execute("checkpoint create;");

            // drop the view after checkpoint
            execute("drop view v;");
            drainWalAndViewQueues();

            assertSql("count\n0\n", "select count() from views() where view_name = 'v';");

            engine.clear();
            engine.closeNameRegistry();
            createTriggerFile();
            engine.checkpointRecover();
            engine.reloadTableNames();
            engine.getMetadataCache().onStartupAsyncHydrator();
            engine.buildViewGraphs();

            // the dropped view should be restored
            assertSql("count\n1\n", "select count() from views() where view_name = 'v';");
            assertSql(
                    """
                            ts\tname\tval
                            2023-09-20T12:39:01.933062Z\tfoobar\t42
                            """,
                    "v;"
            );
            engine.checkpointRelease();
        });
    }

    @Test
    public void testCheckpointRestoresDroppedWalTable() throws Exception {
        final String snapshotId = "id1";
        final String restartedId = "id2";
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

            execute("create table test (ts timestamp, name symbol, val int) timestamp(ts) partition by day wal;");
            execute("insert into test values ('2023-09-20T12:39:01.933062Z', 'foobar', 42);");
            drainWalQueue();

            execute("checkpoint create;");

            execute("drop table test;");
            drainWalQueue();

            assertSql("count\n0\n", "select count() from tables() where table_name = 'test';");

            // Release readers, writers and table name registry files, but keep the snapshot dir around.
            engine.clear();
            engine.closeNameRegistry();
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
            engine.checkpointRecover();
            engine.reloadTableNames();
            engine.getMetadataCache().onStartupAsyncHydrator();

            drainWalQueue();

            // Dropped table should be there.
            assertSql("count\n1\n", "select count() from tables() where table_name = 'test';");
            assertSql(
                    """
                            ts\tname\tval
                            2023-09-20T12:39:01.933062Z\tfoobar\t42
                            """,
                    "test;"
            );
            engine.checkpointRelease();
        });
    }

    @Test
    public void testCheckpointRestoresMatViewMetaFiles() throws Exception {
        assertMemoryLeak(() -> {
            testCheckpointCreateCheckTableMetadataFiles(
                    "create table base_price (sym varchar, price double, ts timestamp) timestamp(ts) partition by DAY WAL",
                    null,
                    "base_price"
            );
            String viewSql = "select sym, last(price) as price, ts from base_price sample by 1h";
            String sql = "create materialized view price_1h as (" + viewSql + ") partition by DAY";

            execute(sql);

            assertSql(
                    """
                            view_name\trefresh_type\tbase_table_name
                            price_1h\timmediate\tbase_price
                            """,
                    "select view_name,refresh_type,base_table_name from materialized_views();"
            );

            execute("checkpoint create");

            execute("alter materialized view price_1h SET REFRESH MANUAL");
            drainWalQueue();

            assertSql(
                    """
                            view_name\trefresh_type\tbase_table_name
                            price_1h\tmanual\tbase_price
                            """,
                    "select view_name,refresh_type,base_table_name from materialized_views();"
            );


            // Release readers, writers and table name registry files, but keep the snapshot dir around.
            engine.clear();
            engine.closeNameRegistry();
            createTriggerFile();
            engine.checkpointRecover();
            engine.reloadTableNames();
            engine.getMetadataCache().onStartupAsyncHydrator();
            engine.buildViewGraphs();

            assertSql(
                    """
                            view_name\trefresh_type\tbase_table_name
                            price_1h\timmediate\tbase_price
                            """,
                    "select view_name,refresh_type,base_table_name from materialized_views();"
            );


            execute("checkpoint release");
        });
    }

    @Test
    public void testCheckpointRestoresNestedViews() throws Exception {
        final String snapshotId = "id1";
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

            // 1. Create base table with data
            execute("create table test (ts timestamp, name symbol, val int) timestamp(ts) partition by day wal;");
            execute("insert into test values ('2023-09-20T12:00:00.000000Z', 'a', 10);");
            execute("insert into test values ('2023-09-20T13:00:00.000000Z', 'b', 20);");
            execute("insert into test values ('2023-09-20T14:00:00.000000Z', 'c', 30);");
            drainWalQueue();

            // 2. Create view_a: selects rows with val > 5 (returns a, b, c)
            execute("create view view_a as select name, val from test where val > 5;");
            drainViewQueue();

            // 3. Create view_b: selects from view_a with val > 15 (returns b, c)
            execute("create view view_b as select name, val from view_a where val > 15;");
            drainViewQueue();

            // 4. Verify both views work correctly
            assertSql(
                    """
                            name\tval
                            a\t10
                            b\t20
                            c\t30
                            """,
                    "view_a;"
            );
            assertSql(
                    """
                            name\tval
                            b\t20
                            c\t30
                            """,
                    "view_b;"
            );

            // 5. Checkpoint
            execute("checkpoint create;");

            // 6. Drop both views
            execute("drop view view_b;");
            execute("drop view view_a;");
            drainViewQueue();

            // 7. Verify neither view exists
            assertSql("count\n0\n", "select count() from views() where view_name = 'view_a';");
            assertSql("count\n0\n", "select count() from views() where view_name = 'view_b';");

            // 8. Restore from checkpoint
            engine.clear();
            engine.closeNameRegistry();
            createTriggerFile();
            engine.checkpointRecover();
            engine.reloadTableNames();
            engine.getMetadataCache().onStartupAsyncHydrator();
            engine.buildViewGraphs();

            // 9. Verify both views are restored
            assertSql("count\n1\n", "select count() from views() where view_name = 'view_a';");
            assertSql("count\n1\n", "select count() from views() where view_name = 'view_b';");

            // 10. Verify view_a returns correct data (a, b, c)
            assertSql(
                    """
                            name\tval
                            a\t10
                            b\t20
                            c\t30
                            """,
                    "view_a;"
            );

            // 11. Verify view_b returns correct data (b, c) - the nested view chain works
            assertSql(
                    """
                            name\tval
                            b\t20
                            c\t30
                            """,
                    "view_b;"
            );

            engine.checkpointRelease();
        });
    }

    @Test
    public void testCheckpointRestoresPreferencesStore() throws Exception {
        File dir1 = temp.newFolder("server1_prefs");
        File dir2 = temp.newFolder("server2_prefs");

        // Server 1: Set preferences and create checkpoint
        try (TestServerMain server1 = startServerMain(dir1.getAbsolutePath())) {
            SettingsStore settingsStore = server1.getEngine().getSettingsStore();

            // Set preferences
            try (DirectUtf8Sink preferencesJson = new DirectUtf8Sink(128)) {
                preferencesJson.put("{\"instance_name\":\"My Test Instance\",\"instance_type\":\"production\"}");
                settingsStore.save(preferencesJson, SettingsStore.Mode.OVERWRITE, 0L);
            }

            // Verify preferences were saved (using observe() to avoid registering persistent listener)
            Assert.assertEquals(1, settingsStore.getVersion());
            settingsStore.observe(prefs -> {
                Assert.assertEquals("My Test Instance", prefs.get("instance_name").toString());
                Assert.assertEquals("production", prefs.get("instance_type").toString());
            });

            server1.execute("CHECKPOINT CREATE");
            copyDirectory(dir1, dir2);
            server1.execute("CHECKPOINT RELEASE");

            // Modify preferences after checkpoint (in source dir - won't affect dir2)
            try (DirectUtf8Sink preferencesJson = new DirectUtf8Sink(128)) {
                preferencesJson.put("{\"instance_name\":\"Modified Instance\",\"instance_type\":\"development\"}");
                settingsStore.save(preferencesJson, SettingsStore.Mode.OVERWRITE, 1L);
            }
            Assert.assertEquals(2, settingsStore.getVersion());
        }

        // Manually modify the preferences file in dir2 to simulate post-checkpoint changes
        try (SettingsStore tempStore = new SettingsStore(
                new DefaultCairoConfiguration(dir2.getAbsolutePath() + Files.SEPARATOR + "db"))) {
            tempStore.init();
            try (DirectUtf8Sink preferencesJson = new DirectUtf8Sink(128)) {
                preferencesJson.put("{\"instance_name\":\"Modified Instance\",\"instance_type\":\"development\"}");
                tempStore.save(preferencesJson, SettingsStore.Mode.OVERWRITE, 1L);
            }
            Assert.assertEquals(2, tempStore.getVersion());
        }

        // Create trigger file to force checkpoint recovery in dir2
        try (Path triggerPath = new Path().of(dir2.getAbsolutePath()).concat(TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME)) {
            Files.touch(triggerPath.$());
        }

        // Server 2: Start with trigger file - should restore preferences from checkpoint
        try (TestServerMain server2 = startServerMain(dir2.getAbsolutePath())) {
            SettingsStore settingsStore = server2.getEngine().getSettingsStore();

            // Verify preferences were restored to checkpoint state
            Assert.assertEquals("Preferences version should be restored", 1, settingsStore.getVersion());
            settingsStore.registerListener(prefs -> {
                Assert.assertEquals("My Test Instance", prefs.get("instance_name").toString());
                Assert.assertEquals("production", prefs.get("instance_type").toString());
            });
        }
    }

    @Test
    public void testCheckpointRestoresRenamedWalTableName() throws Exception {
        final String snapshotId = "id1";
        final String restartedId = "id2";
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

            execute("create table test (ts timestamp, name symbol, val int) timestamp(ts) partition by day wal;");
            execute("insert into test values ('2023-09-20T12:39:01.933062Z', 'foobar', 42);");
            drainWalQueue();

            execute("checkpoint create;");

            execute("rename table test to test2;");

            drainWalQueue();

            assertSql("count\n0\n", "select count() from tables() where table_name = 'test';");
            assertSql("count\n1\n", "select count() from tables() where table_name = 'test2';");

            // Release readers, writers and table name registry files, but keep the snapshot dir around.
            engine.clear();
            engine.closeNameRegistry();
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
            engine.checkpointRecover();
            engine.reloadTableNames();
            engine.getMetadataCache().onStartupAsyncHydrator();

            drainWalQueue();

            // Renamed table should be there under the original name.
            assertSql("count\n1\n", "select count() from tables() where table_name = 'test';");
            assertSql("count\n0\n", "select count() from tables() where table_name = 'test2';");

            engine.checkpointRelease();
        });
    }

    @Test
    public void testCheckpointRestoresTruncatedWalTable() throws Exception {
        final String snapshotId = "id1";
        final String restartedId = "id2";
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

            execute("create table test (ts timestamp, name symbol, val int) timestamp(ts) partition by day wal;");
            execute("insert into test values (now(), 'foobar', 42);");
            drainWalQueue();

            execute("checkpoint create;");

            execute("truncate table test;");
            drainWalQueue();

            assertSql("count\n0\n", "select count() from test;");

            // Release all readers and writers, but keep the snapshot dir around.
            engine.clear();
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
            engine.checkpointRecover();

            drainWalQueue();

            // Dropped rows should be there.
            assertSql("count\n1\n", "select count() from test;");
            engine.checkpointRelease();
        });
    }

    @Test
    public void testCheckpointRestoresViewDefinition() throws Exception {
        final String snapshotId = "id1";
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

            // 1. Create base table with data
            execute("create table test (ts timestamp, name symbol, val int) timestamp(ts) partition by day wal;");
            execute("insert into test values ('2023-09-20T12:00:00.000000Z', 'a', 10);");
            execute("insert into test values ('2023-09-20T13:00:00.000000Z', 'b', 20);");
            execute("insert into test values ('2023-09-20T14:00:00.000000Z', 'c', 30);");
            drainWalQueue();

            // 2. Create view with predicate (val > 15) - should return 'b' and 'c'
            execute("create view v as select name, val from test where val > 15;");
            drainViewQueue();

            // Validate the view returns expected data
            assertSql(
                    """
                            name\tval
                            b\t20
                            c\t30
                            """,
                    "v;"
            );

            // 3. Checkpoint
            execute("checkpoint create;");

            // 4. Drop the view and create a new one with the same name but different predicate (val > 25)
            execute("drop view v;");
            drainViewQueue();

            execute("create view v as select name, val from test where val > 25;");
            drainViewQueue();

            // 5. Validate the new view returns different data (only 'c')
            assertSql(
                    """
                            name\tval
                            c\t30
                            """,
                    "v;"
            );

            // 6. Restore from the checkpoint
            engine.clear();
            engine.closeNameRegistry();
            createTriggerFile();
            engine.checkpointRecover();
            engine.reloadTableNames();
            engine.getMetadataCache().onStartupAsyncHydrator();
            engine.buildViewGraphs();

            // 7. Validate the view uses the predicate from before the checkpoint (val > 15)
            assertSql(
                    """
                            name\tval
                            b\t20
                            c\t30
                            """,
                    "v;"
            );
            engine.checkpointRelease();
        });
    }

    @Test
    public void testCheckpointRestoresViewWithBaseTableData() throws Exception {
        final String snapshotId = "id1";
        final String restartedId = "id2";
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

            execute("create table test (ts timestamp, name symbol, val int) timestamp(ts) partition by day wal;");
            execute("insert into test values ('2023-09-20T12:00:00.000000Z', 'a', 10);");
            execute("insert into test values ('2023-09-20T13:00:00.000000Z', 'b', 20);");
            drainWalQueue();

            execute("create view test_view as select name, val from test where val > 15;");

            assertSql(
                    """
                            name\tval
                            b\t20
                            """,
                    "test_view;"
            );

            execute("checkpoint create;");

            // insert more data after checkpoint
            execute("insert into test values ('2023-09-20T14:00:00.000000Z', 'c', 30);");
            drainWalQueue();

            // view should now show 2 rows
            assertSql("count\n2\n", "select count() from test_view;");

            // Recover from checkpoint
            engine.clear();
            engine.closeNameRegistry();
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
            engine.checkpointRecover();
            engine.reloadTableNames();
            engine.getMetadataCache().onStartupAsyncHydrator();
            engine.buildViewGraphs();

            // After recovery, view should only show data from checkpoint time (1 row)
            assertSql(
                    """
                            name\tval
                            b\t20
                            """,
                    "test_view;"
            );
            engine.checkpointRelease();
        });
    }

    @Test
    public void testCheckpointStatus() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            assertSql(
                    """
                            in_progress\tstarted_at
                            false\t
                            """,
                    "select * from checkpoint_status();"
            );

            execute("checkpoint create");

            assertSql(
                    """
                            in_progress\tstarted_at
                            true\t1970-01-01T00:00:00.000000Z
                            """,
                    "select * from checkpoint_status();"
            );

            execute("checkpoint release");

            assertSql(
                    """
                            in_progress\tstarted_at
                            false\t
                            """,
                    "select * from checkpoint_status();"
            );
        });
    }

    @Test
    public void testCheckpointUnknownSubOptionFails() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, name symbol, val int)");
            assertException(
                    "checkpoint commit",
                    11,
                    "'create' or 'release' expected"
            );
        });
    }

    @Test
    public void testCheckpointViewMetadataFiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, name symbol, val int) timestamp(ts) partition by day wal;");
            execute("create view test_view as select * from test;");

            execute("checkpoint create;");

            // view directory exists in checkpoint
            TableToken viewToken = engine.getTableTokenIfExists("test_view");
            Assert.assertNotNull(viewToken);

            path.trimTo(rootLen).concat(viewToken.getDirName()).slash$();
            Assert.assertTrue("View directory should exist in checkpoint", TestFilesFacadeImpl.INSTANCE.exists(path.$()));

            // _view file exists
            path.trimTo(rootLen).concat(viewToken.getDirName()).concat("_view").$();
            Assert.assertTrue("_view file should exist in checkpoint", TestFilesFacadeImpl.INSTANCE.exists(path.$()));

            // check txn_seq directory exists (views have sequencers)
            path.trimTo(rootLen).concat(viewToken.getDirName()).concat(WalUtils.SEQ_DIR).slash$();
            Assert.assertTrue("txn_seq directory should exist in checkpoint", TestFilesFacadeImpl.INSTANCE.exists(path.$()));

            // check txn_seq/_meta file exists
            path.trimTo(rootLen).concat(viewToken.getDirName()).concat(WalUtils.SEQ_DIR).concat(TableUtils.META_FILE_NAME).$();
            Assert.assertTrue("txn_seq/_meta file should exist in checkpoint", TestFilesFacadeImpl.INSTANCE.exists(path.$()));

            execute("checkpoint release;");
        });
    }

    @Test
    public void testCheckpointWithTableDropAndTableCreate() throws Exception {
        final String snapshotId = "id1";
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

            // 1. Create base table with data
            execute("create table test (ts timestamp, name symbol, val int) timestamp(ts) partition by day wal;");
            execute("insert into test values ('2023-09-20T12:00:00.000000Z', 'a', 10);");
            execute("insert into test values ('2023-09-20T13:00:00.000000Z', 'b', 20);");
            execute("insert into test values ('2023-09-20T14:00:00.000000Z', 'c', 30);");
            drainWalQueue();

            // 2. Checkpoint
            execute("checkpoint create;");

            // 3. Drop table and create a new one after checkpoint
            execute("drop table test;");
            drainWalQueue();
            drainPurgeJob();

            execute("create table tiesto (ts timestamp, name symbol, val int) timestamp(ts) partition by day wal;");

            // 4. Restore from the checkpoint
            engine.clear();
            engine.closeNameRegistry();
            createTriggerFile();
            engine.checkpointRecover();
            engine.reloadTableNames();
            engine.getMetadataCache().onStartupAsyncHydrator();
            engine.buildViewGraphs();

            execute("CHECKPOINT RELEASE;");

            // 5. Validate the restored table exists and new table does not
            assertSql(
                    """
                            ts	name	val
                            2023-09-20T12:00:00.000000Z	a	10
                            2023-09-20T13:00:00.000000Z	b	20
                            2023-09-20T14:00:00.000000Z	c	30
                            """,
                    "test;"
            );

            assertException("tiesto;", 0, "table does not exist [table=tiesto]");
        });
    }

    @Test
    public void testCheckpointWithViewAlters() throws Exception {
        final String snapshotId = "id1";
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

            execute("create table test (ts timestamp, name symbol, val int) timestamp(ts) partition by day wal;");
            execute("insert into test values ('2023-09-20T12:39:01.933062Z', 'foobar', 42);");
            execute("create view v as select * from test where val > 0;");
            drainWalAndViewQueues();

            execute("alter view v as select * from test where val > 18;");
            drainWalAndViewQueues();

            // sanity check: the view exists and works
            assertSql("count\n1\n", "select count() from views() where view_name = 'v';");
            assertSql("count\n1\n", "select count() from v;");

            execute("checkpoint create;");

            execute("alter view v as select * from test where val > 100;");
            drainWalAndViewQueues();

            assertSql("count\n0\n", "select count() from v;");

            engine.clear();
            engine.closeNameRegistry();
            createTriggerFile();
            engine.checkpointRecover();
            engine.reloadTableNames();
            engine.getMetadataCache().onStartupAsyncHydrator();
            engine.buildViewGraphs();

            // the dropped view should be restored
            assertSql("count\n1\n", "select count() from v;");

            final TableToken viewToken = engine.getTableTokenIfExists("v");
            final ViewDefinition viewDefinition = engine.getViewGraph().getViewDefinition(viewToken);
            Assert.assertNotNull(viewDefinition);
            Assert.assertEquals("select * from test where val > 18;", viewDefinition.getViewSql());
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(viewToken));

            engine.checkpointRelease();
            assertSql("count\n1\n", "select count() from v;");
        });
    }

    @Test
    public void testFailFastWhenCannotCopyRegistryFile() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "t";
            execute(
                    "create table " + tableName + " as " +
                            "(select rnd_str(2,3,0) a, rnd_symbol('A','B','C') b, x c from long_sequence(3))"
            );

            execute("checkpoint create");

            engine.clear();
            createTriggerFile();
            testFilesFacade.errorOnRegistryFileCopy = true;
            try {
                engine.checkpointRecover();
                Assert.fail("Exception expected");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getMessage(), "Could not copy registry file");
            } finally {
                testFilesFacade.errorOnRegistryFileCopy = false;
            }
            engine.checkpointRelease();
        });
    }

    @Test
    public void testFailFastWhenCannotRemoveRegistryFile() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "t";
            execute(
                    "create table " + tableName + " as " +
                            "(select rnd_str(2,3,0) a, rnd_symbol('A','B','C') b, x c from long_sequence(3))"
            );

            execute("checkpoint create");

            engine.clear();
            createTriggerFile();
            testFilesFacade.errorOnRegistryFileRemoval = true;
            try {
                engine.checkpointRecover();
                Assert.fail("Exception expected");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getMessage(), "Could not remove registry file");
            } finally {
                testFilesFacade.errorOnRegistryFileRemoval = false;
            }
            engine.checkpointRelease();
        });
    }

    @Test
    public void testFailFastWhenCannotRemoveTriggerFile() throws Exception {
        assertMemoryLeak(() -> {
            testFilesFacade.errorOnTriggerFileRemoval = true;

            final String tableName = "t";
            execute(
                    "create table " + tableName + " as " +
                            "(select rnd_str(2,3,0) a, rnd_symbol('A','B','C') b, x c from long_sequence(3))"
            );

            execute("checkpoint create");

            engine.clear();
            createTriggerFile();
            try {
                engine.checkpointRecover();
                Assert.fail("Exception expected");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getMessage(), "could not remove restore trigger file");
            }
            engine.checkpointRelease();
        });
    }

    @Test
    public void testFailFastWhenTriggerFailExistsButThereIsNoCheckpointDirectory() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "t";
            execute(
                    "create table " + tableName + " as " +
                            "(select rnd_str(2,3,0) a, rnd_symbol('A','B','C') b, x c from long_sequence(3))"
            );

            engine.clear();
            createTriggerFile();
            try {
                engine.checkpointRecover();
                Assert.fail("Exception expected");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getMessage(), "checkpoint directory does not exist");
            }
        });
    }

    @Test
    public void testFailFastWhenTriggerFailExistsButThereIsNoCheckpointMetadataFile() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "t";
            execute(
                    "create table " + tableName + " as " +
                            "(select rnd_str(2,3,0) a, rnd_symbol('A','B','C') b, x c from long_sequence(3))"
            );

            SimpleWaitingLock lock = new SimpleWaitingLock();
            try {
                engine.setWalPurgeJobRunLock(lock);
                execute("checkpoint create");

                Assert.assertTrue(lock.isLocked());

                path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory());
                FilesFacade ff = configuration.getFilesFacade();
                ff.removeQuiet(path.concat(TableUtils.CHECKPOINT_META_FILE_NAME).$());

                engine.clear();
                createTriggerFile();
                try {
                    engine.checkpointRecover();
                    Assert.fail("Exception expected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getMessage(), "checkpoint metadata file does not exist");
                }
                engine.checkpointRelease();

                Assert.assertFalse(lock.isLocked());
            } finally {
                engine.setWalPurgeJobRunLock(null);
            }
        });
    }

    @Test
    public void testIncrementalCheckpointPrepareRefreshesMatViewIntervals() throws Exception {
        assertMemoryLeak(() -> {

            // Create base table and mat view
            execute("create table base_price (sym varchar, price double, ts timestamp) timestamp(ts) partition by DAY WAL");
            String viewSql = "select sym, last(price) as price, ts from base_price sample by 1h";
            execute("create materialized view price_1h as (" + viewSql + ") partition by DAY");
            drainWalQueue();

            // Insert data into base table and refresh mat view
            execute("insert into base_price values ('BTC', 100.0, '2024-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();

            TableToken baseTableToken = engine.verifyTableName("base_price");
            TableToken matViewToken = engine.verifyTableName("price_1h");

            // Insert more data without refreshing the mat view
            execute("insert into base_price values ('BTC', 200.0, '2024-01-01T01:00:00.000000Z')");
            execute("insert into base_price values ('BTC', 300.0, '2024-01-01T02:00:00.000000Z')");
            drainWalQueue();

            // Get the base table's seqTxn after additional inserts
            long seqTxnAfterMoreInserts = engine.getTableSequencerAPI().getTxnTracker(baseTableToken).getSeqTxn();

            SimpleWaitingLock lock = new SimpleWaitingLock();
            try {
                engine.setWalPurgeJobRunLock(lock);

                // Create incremental checkpoint
                engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), true);

                // Check that WAL purge job lock is released after checkpoint is done, no need to keep it until checkpiont release
                Assert.assertFalse(lock.isLocked());

                // Read the mat view state from the checkpoint
                try (
                        Path checkpointPath = new Path();
                        io.questdb.cairo.file.BlockFileReader reader = new io.questdb.cairo.file.BlockFileReader(configuration)
                ) {
                    checkpointPath.of(configuration.getCheckpointRoot())
                            .concat(configuration.getDbDirectory())
                            .concat(matViewToken)
                            .concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$();

                    reader.of(checkpointPath.$());
                    io.questdb.cairo.mv.MatViewStateReader stateReader = new io.questdb.cairo.mv.MatViewStateReader();
                    stateReader.of(reader, matViewToken);

                    // Verify that the checkpoint mat view state has updated refreshIntervalsBaseTxn
                    // It should match the base table's seqTxn at checkpoint time
                    Assert.assertEquals("Checkpoint mat view state should have refreshIntervalsBaseTxn updated to checkpoint base table txn",
                            seqTxnAfterMoreInserts, stateReader.getRefreshIntervalsBaseTxn());

                    // Verify that refresh intervals were loaded (should not be empty since we have unrefreshed data)
                    Assert.assertTrue("Checkpoint mat view state should have refresh intervals",
                            stateReader.getRefreshIntervals().size() > 0);
                }

                execute("checkpoint release");
            } finally {
                engine.setWalPurgeJobRunLock(null);
            }
        });
    }

    @Test
    public void testRecoverCheckpointForDefaultCheckpointId() throws Exception {
        testRecoverCheckpoint("", "id1", false, false);
    }

    @Test
    public void testRecoverCheckpointForDefaultCheckpointIdAndTriggerFile() throws Exception {
        testRecoverCheckpoint("", "id1", true, true);
    }

    @Test
    public void testRecoverCheckpointForDefaultInstanceIds() throws Exception {
        testRecoverCheckpoint("", "", false, false);
    }

    @Test
    public void testRecoverCheckpointForDefaultInstanceIdsAndTriggerFile() throws Exception {
        testRecoverCheckpoint("", "", true, true);
    }

    @Test
    public void testRecoverCheckpointForDefaultRestartedId() throws Exception {
        testRecoverCheckpoint("id1", "", false, false);
    }

    @Test
    public void testRecoverCheckpointForDefaultRestartedIdAndTriggerFile() throws Exception {
        testRecoverCheckpoint("id1", "", true, true);
    }

    @Test
    public void testRecoverCheckpointForDifferentInstanceIds() throws Exception {
        testRecoverCheckpoint("id1", "id2", false, true);
    }

    @Test
    public void testRecoverCheckpointForDifferentInstanceIdsAndTriggerFile() throws Exception {
        testRecoverCheckpoint("id1", "id2", true, true);
    }

    @Test
    public void testRecoverCheckpointForDifferentInstanceIdsAndTriggerFileWhenRecoveryIsDisabled() throws Exception {
        node1.setProperty(CAIRO_CHECKPOINT_RECOVERY_ENABLED, "false");
        testRecoverCheckpoint("id1", "id2", true, false);
    }

    @Test
    public void testRecoverCheckpointForDifferentInstanceIdsAndTriggerFileWhenRecoveryIsDisabledViaLegacy() throws Exception {
        node1.setProperty(CAIRO_LEGACY_SNAPSHOT_RECOVERY_ENABLED, "false");
        testRecoverCheckpoint("id1", "id2", true, false);
    }

    @Test
    public void testRecoverCheckpointForDifferentInstanceIdsWhenRecoveryIsDisabled() throws Exception {
        node1.setProperty(CAIRO_CHECKPOINT_RECOVERY_ENABLED, "false");
        testRecoverCheckpoint("id1", "id2", false, false);
    }

    @Test
    public void testRecoverCheckpointForDifferentInstanceIdsWhenRecoveryIsDisabledViaLegacy() throws Exception {
        node1.setProperty(CAIRO_LEGACY_SNAPSHOT_RECOVERY_ENABLED, "false");
        testRecoverCheckpoint("id1", "id2", false, false);
    }

    @Test
    public void testRecoverCheckpointForEqualInstanceIds() throws Exception {
        testRecoverCheckpoint("id1", "id1", false, false);
    }

    @Test
    public void testRecoverCheckpointForEqualInstanceIdsAndTriggerFile() throws Exception {
        testRecoverCheckpoint("id1", "id1", true, true);
    }

    @Test
    public void testRecoverCheckpointLargePartitionCount() throws Exception {
        final int partitionCount = 2000;
        final String snapshotId = "id1";
        final String restartedId = "id2";
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);
            final String tableName = "t";
            execute(
                    "create table " + tableName + " as " +
                            "(select x, timestamp_sequence(0, 100000000000) ts from long_sequence(" + partitionCount + ")) timestamp(ts) partition by day"
            );

            execute("checkpoint create");

            execute(
                    "insert into " + tableName +
                            " select x+20 x, timestamp_sequence(100000000000, 100000000000) ts from long_sequence(3)"
            );

            // Release all readers and writers, but keep the snapshot dir around.
            engine.clear();
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
            engine.checkpointRecover();

            // Data inserted after PREPARE SNAPSHOT should be discarded.
            assertSql(
                    "count\n" +
                            partitionCount + "\n",
                    "select count() from " + tableName
            );
            engine.checkpointRelease();
        });
    }

    @Test
    public void testRecoverCheckpointRestoresDroppedColumns() throws Exception {
        final String snapshotId = "00000000-0000-0000-0000-000000000000";
        final String restartedId = "123e4567-e89b-12d3-a456-426614174000";
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

            final String tableName = "t";
            execute(
                    "create table " + tableName + " as " +
                            "(select rnd_str(2,3,0) a, rnd_symbol('A','B','C') b, x c from long_sequence(3))"
            );

            execute("checkpoint create");

            final String expectedAllColumns = """
                    a\tb\tc
                    JW\tC\t1
                    WH\tB\t2
                    PE\tB\t3
                    """;
            assertSql(expectedAllColumns, "select * from " + tableName);

            execute("alter table " + tableName + " drop column b");
            assertSql(
                    """
                            a\tc
                            JW\t1
                            WH\t2
                            PE\t3
                            """,
                    "select * from " + tableName
            );

            // Release all readers and writers, but keep the snapshot dir around.
            engine.clear();
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
            engine.checkpointRecover();

            // Dropped column should be there.
            assertSql(expectedAllColumns, "select * from " + tableName);
            engine.checkpointRelease();
        });
    }

    @Test
    public void testRecoverCheckpointSupportsCheckpointTxtFile() throws Exception {
        final int partitionCount = 10;
        final String snapshotId = "id1";
        final String restartedId = "id2";
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, "");
            final String tableName = "t";
            execute(
                    "create table " + tableName + " as " +
                            "(select x, timestamp_sequence(0, 100000000000) ts from long_sequence(" + partitionCount + ")) timestamp(ts) partition by day"
            );

            execute("checkpoint create");

            execute(
                    "insert into " + tableName +
                            " select x+20 x, timestamp_sequence(100000000000, 100000000000) ts from long_sequence(3)"
            );

            // Release all readers and writers, but keep the snapshot dir around.
            engine.clear();

            // create snapshot.txt file
            FilesFacade ff = configuration.getFilesFacade();
            path.trimTo(rootLen).concat(TableUtils.CHECKPOINT_LEGACY_META_FILE_NAME_TXT);
            long fd = ff.openRW(path.$(), configuration.getWriterFileOpenOpts());
            Assert.assertTrue(fd > 0);

            try {
                try (DirectUtf8Sink utf8 = new DirectUtf8Sink(3)) {
                    utf8.put(snapshotId);
                    ff.write(fd, utf8.ptr(), utf8.size(), 0);
                    ff.truncate(fd, utf8.size());
                }
            } finally {
                ff.close(fd);
            }
            Assert.assertEquals(ff.length(path.$()), restartedId.length());

            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
            engine.checkpointRecover();

            // Data inserted after PREPARE SNAPSHOT should be discarded.
            assertSql(
                    "count\n" +
                            partitionCount + "\n",
                    "select count() from " + tableName
            );
            engine.checkpointRelease();
        });
    }

    @Test
    public void testRunWalPurgeJobLockTimeout() throws Exception {
        configureCircuitBreakerTimeoutOnFirstCheck(); // trigger timeout on first check
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, name symbol, val int)");
            SimpleWaitingLock lock = new SimpleWaitingLock();
            SOCountDownLatch latch1 = new SOCountDownLatch(1);
            SOCountDownLatch latch2 = new SOCountDownLatch(1);

            engine.setWalPurgeJobRunLock(lock);

            Thread t = new Thread(() -> {
                lock.lock(); //emulate WalPurgeJob running with lock
                latch2.countDown();
                try {
                    latch1.await();
                } finally {
                    lock.unlock();
                }
            });

            try {
                t.start();
                latch2.await();
                assertExceptionNoLeakCheck("checkpoint create");
            } catch (CairoException ex) {
                latch1.countDown();
                t.join();
                Assert.assertFalse(lock.isLocked());
                TestUtils.assertContains(ex.getFlyweightMessage(), "timeout, query aborted [fd=-1");
            } finally {
                execute("checkpoint release");
                Assert.assertFalse(lock.isLocked());
                engine.setWalPurgeJobRunLock(null);
            }
        });
    }

    @Test
    public void testSnapshotPrepareSavesToSnapshotFolder() throws Exception {
        final String tableName = "test";
        path.of(configuration.getDbRoot()).concat("empty_folder").slash$();
        TestFilesFacadeImpl.INSTANCE.mkdirs(path, configuration.getMkDirMode());

        assertMemoryLeak(() -> {
            try (Path path = new Path(); Path copyPath = new Path()) {
                if (rnd.nextBoolean()) {
                    // Create .checkpoint folder sometimes
                    path.of(configuration.getCheckpointRoot()).slash$();
                    TestFilesFacadeImpl.INSTANCE.mkdirs(path, configuration.getMkDirMode());
                    Assert.assertTrue(TestFilesFacadeImpl.INSTANCE.exists(
                            path.of(configuration.getCheckpointRoot()).slash$()
                    ));
                }


                path.of(configuration.getDbRoot());
                copyPath.of(configuration.getLegacyCheckpointRoot()).concat(configuration.getDbDirectory());

                execute("create table " + tableName + " (a symbol index capacity 128, b double, c long)");

                execute("snapshot prepare");

                TableToken tableToken = engine.verifyTableName(tableName);
                path.concat(tableToken);
                int tableNameLen = path.size();
                copyPath.concat(tableToken);
                int copyTableNameLen = copyPath.size();

                // _meta
                path.concat(TableUtils.META_FILE_NAME).$();
                copyPath.concat(TableUtils.META_FILE_NAME).$();
                TestUtils.assertFileContentsEquals(path, copyPath);
                // _txn
                path.trimTo(tableNameLen).concat(TableUtils.TXN_FILE_NAME).$();
                copyPath.trimTo(copyTableNameLen).concat(TableUtils.TXN_FILE_NAME).$();
                TestUtils.assertFileContentsEquals(path, copyPath);
                // _cv
                path.trimTo(tableNameLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
                copyPath.trimTo(copyTableNameLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
                TestUtils.assertFileContentsEquals(path, copyPath);

                // Assert snapshot folder exists
                Assert.assertTrue(TestFilesFacadeImpl.INSTANCE.exists(
                        path.of(configuration.getLegacyCheckpointRoot()).slash$()
                ));
                // Assert snapshot folder is not empty
                Assert.assertTrue(TestFilesFacadeImpl.INSTANCE.exists(
                        path.of(configuration.getLegacyCheckpointRoot()).concat(configuration.getDbDirectory()).slash$()
                ));
                // Assert .checkpoint folder DOES NOT exist
                Assert.assertFalse(TestFilesFacadeImpl.INSTANCE.exists(
                        path.of(configuration.getCheckpointRoot()).slash$()
                ));

                execute("checkpoint release");

                // Assert snapshot folder exists
                Assert.assertTrue(TestFilesFacadeImpl.INSTANCE.exists(
                        path.of(configuration.getLegacyCheckpointRoot()).slash$()
                ));
                // Assert snapshot folder is EMPTY!
                Assert.assertFalse(TestFilesFacadeImpl.INSTANCE.exists(
                        path.of(configuration.getLegacyCheckpointRoot()).concat(configuration.getDbDirectory()).slash$()
                ));
            }
        });
    }

    @Test
    public void testSuspendResumeWalPurgeJob() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            String tableName = getTestTableName();
            execute(
                    "create table " + tableName + " as (" +
                            "select x, " +
                            " timestamp_sequence('2022-02-24', 1000000L) ts " +
                            " from long_sequence(5)" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);

            drainWalQueue();

            assertWalExistence(true, tableName, 1);

            assertSql(
                    """
                            x\tts
                            1\t2022-02-24T00:00:00.000000Z
                            2\t2022-02-24T00:00:01.000000Z
                            3\t2022-02-24T00:00:02.000000Z
                            4\t2022-02-24T00:00:03.000000Z
                            5\t2022-02-24T00:00:04.000000Z
                            """,
                    tableName
            );

            final long interval = engine.getConfiguration().getWalPurgeInterval() * 1000;
            final WalPurgeJob job = new WalPurgeJob(engine);
            engine.setWalPurgeJobRunLock(job.getRunLock());

            execute("checkpoint create");
            Thread controlThread1 = new Thread(() -> {
                setCurrentMicros(interval);
                job.drain(0);
                Path.clearThreadLocals();
            });

            controlThread1.start();
            controlThread1.join();

            assertSegmentExistence(true, tableName, 1, 0);
            assertWalExistence(true, tableName, 1);

            engine.releaseInactive();

            execute("checkpoint release");
            Thread controlThread2 = new Thread(() -> {
                setCurrentMicros(2 * interval);
                job.drain(0);
                Path.clearThreadLocals();
            });

            controlThread2.start();
            controlThread2.join();

            job.close();
            engine.setWalPurgeJobRunLock(null);

            assertSegmentExistence(false, tableName, 1, 0);
            assertWalExistence(false, tableName, 1);
        });
    }

    @Test
    public void testViewDoesNotObstructCheckpointCreation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, name symbol, val int)");
            execute("create view test_view as select * from test");
            execute("checkpoint create");
            execute("checkpoint release");
        });
    }

    @Test
    public void testWalMetadataRecovery() throws Exception {
        final String snapshotId = "id1";
        final String restartedId = "id2";
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);
            String tableName = getTestTableName() + "_abc";
            execute(
                    "create table " + tableName + " as (" +
                            "select x, " +
                            " rnd_symbol('AB', 'BC', 'CD') sym, " +
                            " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                            " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                            " from long_sequence(5)" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            execute("alter table " + tableName + " add column iii int");
            execute("insert into " + tableName + " values (101, 'dfd', '2022-02-24T01', 'asd', 41)");
            execute("alter table " + tableName + " add column jjj int");
            execute("insert into " + tableName + " values (102, 'dfd', '2022-02-24T02', 'asd', 41, 42)");
            update("UPDATE " + tableName + " SET iii = 0 where iii = null");
            update("UPDATE " + tableName + " SET jjj = 0 where iii = null");

            drainWalQueue();

            // all updates above should be applied to table
            assertSql(
                    """
                            x\tsym\tts\tsym2\tiii\tjjj
                            1\tAB\t2022-02-24T00:00:00.000000Z\tEF\t0\tnull
                            2\tBC\t2022-02-24T00:00:01.000000Z\tFG\t0\tnull
                            3\tCD\t2022-02-24T00:00:02.000000Z\tFG\t0\tnull
                            4\tCD\t2022-02-24T00:00:03.000000Z\tFG\t0\tnull
                            5\tAB\t2022-02-24T00:00:04.000000Z\tDE\t0\tnull
                            101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\t41\tnull
                            102\tdfd\t2022-02-24T02:00:00.000000Z\tasd\t41\t42
                            """,
                    tableName
            );


            execute("alter table " + tableName + " add column kkk int");
            execute("insert into " + tableName + " values (103, 'dfd', '2022-02-24T03', 'xyz', 41, 42, 43)");

            // the updates above should apply to WAL, not table
            execute("checkpoint create");

            // these updates are lost during the snapshotting
            execute("alter table " + tableName + " add column lll int");
            execute("insert into " + tableName + " values (104, 'dfd', '2022-02-24T04', 'asdf', 1, 2, 3, 4)");
            execute("insert into " + tableName + " values (105, 'dfd', '2022-02-24T05', 'asdf', 5, 6, 7, 8)");

            // Release all readers and writers, but keep the snapshot dir around.
            engine.clear();
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
            engine.checkpointRecover();

            // apply updates from WAL
            drainWalQueue();

            assertSql(
                    """
                            x\tsym\tts\tsym2\tiii\tjjj\tkkk
                            1\tAB\t2022-02-24T00:00:00.000000Z\tEF\t0\tnull\tnull
                            2\tBC\t2022-02-24T00:00:01.000000Z\tFG\t0\tnull\tnull
                            3\tCD\t2022-02-24T00:00:02.000000Z\tFG\t0\tnull\tnull
                            4\tCD\t2022-02-24T00:00:03.000000Z\tFG\t0\tnull\tnull
                            5\tAB\t2022-02-24T00:00:04.000000Z\tDE\t0\tnull\tnull
                            101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\t41\tnull\tnull
                            102\tdfd\t2022-02-24T02:00:00.000000Z\tasd\t41\t42\tnull
                            103\tdfd\t2022-02-24T03:00:00.000000Z\txyz\t41\t42\t43
                            """,
                    tableName
            );

            // check for updates to the restored table
            execute("alter table " + tableName + " add column lll int");
            execute("insert into " + tableName + " values (104, 'dfd', '2022-02-24T04', 'asdf', 1, 2, 3, 4)");
            execute("insert into " + tableName + " values (105, 'dfd', '2022-02-24T05', 'asdf', 5, 6, 7, 8)");
            update("UPDATE " + tableName + " SET jjj = 0 where iii = 0");

            drainWalQueue();

            assertSql(
                    """
                            x\tsym\tts\tsym2\tiii\tjjj\tkkk\tlll
                            1\tAB\t2022-02-24T00:00:00.000000Z\tEF\t0\t0\tnull\tnull
                            2\tBC\t2022-02-24T00:00:01.000000Z\tFG\t0\t0\tnull\tnull
                            3\tCD\t2022-02-24T00:00:02.000000Z\tFG\t0\t0\tnull\tnull
                            4\tCD\t2022-02-24T00:00:03.000000Z\tFG\t0\t0\tnull\tnull
                            5\tAB\t2022-02-24T00:00:04.000000Z\tDE\t0\t0\tnull\tnull
                            101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\t41\tnull\tnull\tnull
                            102\tdfd\t2022-02-24T02:00:00.000000Z\tasd\t41\t42\tnull\tnull
                            103\tdfd\t2022-02-24T03:00:00.000000Z\txyz\t41\t42\t43\tnull
                            104\tdfd\t2022-02-24T04:00:00.000000Z\tasdf\t1\t2\t3\t4
                            105\tdfd\t2022-02-24T05:00:00.000000Z\tasdf\t5\t6\t7\t8
                            """,
                    tableName
            );

            // WalWriter.applyMetadataChangeLog should be triggered
            try (WalWriter walWriter1 = getWalWriter(tableName)) {
                try (WalWriter walWriter2 = getWalWriter(tableName)) {
                    walWriter1.addColumn("C", ColumnType.INT);
                    walWriter1.commit();

                    TableWriter.Row row = walWriter1.newRow(MicrosFormatUtils.parseTimestamp("2022-02-24T06:00:00.000000Z"));

                    row.putLong(0, 777L);
                    row.putSym(1, "XXX");
                    row.putSym(3, "YYY");
                    row.putInt(4, 0);
                    row.putInt(5, 1);
                    row.putInt(6, 2);
                    row.putInt(7, 3);
                    row.putInt(8, 42);
                    row.append();
                    walWriter1.commit();

                    TableWriter.Row row2 = walWriter2.newRow(MicrosFormatUtils.parseTimestamp("2022-02-24T06:01:00.000000Z"));
                    row2.putLong(0, 999L);
                    row2.putSym(1, "AAA");
                    row2.putSym(3, "BBB");
                    row2.putInt(4, 10);
                    row2.putInt(5, 11);
                    row2.putInt(6, 12);
                    row2.putInt(7, 13);
                    row2.append();
                    walWriter2.commit();
                }
            }
            drainWalQueue();
            assertSql(
                    """
                            x\tsym\tts\tsym2\tiii\tjjj\tkkk\tlll\tC
                            1\tAB\t2022-02-24T00:00:00.000000Z\tEF\t0\t0\tnull\tnull\tnull
                            2\tBC\t2022-02-24T00:00:01.000000Z\tFG\t0\t0\tnull\tnull\tnull
                            3\tCD\t2022-02-24T00:00:02.000000Z\tFG\t0\t0\tnull\tnull\tnull
                            4\tCD\t2022-02-24T00:00:03.000000Z\tFG\t0\t0\tnull\tnull\tnull
                            5\tAB\t2022-02-24T00:00:04.000000Z\tDE\t0\t0\tnull\tnull\tnull
                            101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\t41\tnull\tnull\tnull\tnull
                            102\tdfd\t2022-02-24T02:00:00.000000Z\tasd\t41\t42\tnull\tnull\tnull
                            103\tdfd\t2022-02-24T03:00:00.000000Z\txyz\t41\t42\t43\tnull\tnull
                            104\tdfd\t2022-02-24T04:00:00.000000Z\tasdf\t1\t2\t3\t4\tnull
                            105\tdfd\t2022-02-24T05:00:00.000000Z\tasdf\t5\t6\t7\t8\tnull
                            777\tXXX\t2022-02-24T06:00:00.000000Z\tYYY\t0\t1\t2\t3\t42
                            999\tAAA\t2022-02-24T06:01:00.000000Z\tBBB\t10\t11\t12\t13\tnull
                            """,
                    tableName
            );


            engine.checkpointRelease();
        });
    }

    private static void configureCircuitBreakerTimeoutOnFirstCheck() {
        circuitBreaker.setTimeout(-100);
        circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public long getQueryTimeout() {
                return 1;
            }
        };
    }

    private static void copyDirectory(File source, File target) throws IOException {
        java.nio.file.Files.walkFileTree(source.toPath(), new SimpleFileVisitor<>() {
            @Override
            public @NotNull FileVisitResult preVisitDirectory(java.nio.file.@NotNull Path dir, @NotNull BasicFileAttributes attrs) throws IOException {
                java.nio.file.Path targetDir = target.toPath().resolve(source.toPath().relativize(dir));
                java.nio.file.Files.createDirectories(targetDir);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public @NotNull FileVisitResult visitFile(java.nio.file.@NotNull Path file, @NotNull BasicFileAttributes attrs) throws IOException {
                java.nio.file.Files.copy(file, target.toPath().resolve(source.toPath().relativize(file)),
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static void corruptIndexKeyEntry(String dbRoot, String tableDirName) {
        File tableDir = new File(dbRoot, "db/" + tableDirName);
        File partDir = new File(tableDir, "1970");
        File[] keyFiles = partDir.listFiles((dir, name) -> name.startsWith("sym" + ".k"));
        if (keyFiles == null || keyFiles.length == 0) {
            throw new IllegalStateException("No key file found for column: " + "sym");
        }
        File keyFile = keyFiles[0];

        FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        try (
                Path keyPath = new Path().of(keyFile.getAbsolutePath());
                MemoryCMARW mem = Vm.getCMARWInstance()
        ) {
            mem.of(ff, keyPath.$(), ff.getMapPageSize(), keyFile.length(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE, -1);

            // Key 1 entry starts at offset 64 (header) + 32 (key 0) = 96
            long keyEntryOffset = BitmapIndexUtils.KEY_FILE_RESERVED + BitmapIndexUtils.KEY_ENTRY_SIZE;

            // Read current valueCount (at offset 0 within key entry)
            long valueCount = mem.getLong(keyEntryOffset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);

            // Increment valueCount but NOT countCheck - creates inconsistency
            mem.putLong(keyEntryOffset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, valueCount + 1);

            // countCheck at offset +24 remains unchanged
            // This creates: valueCount=11, countCheck=10 (torn write simulation)
        }
    }

    private static void createTriggerFile() {
        Files.touch(triggerFilePath.$());
    }

    private static long getSeqTxnForTable(CharSequenceLongHashMap map, String tableNamePrefix) {
        for (int i = 0, n = map.size(); i < n; i++) {
            CharSequence dirName = map.keys().get(i);
            if (dirName.toString().startsWith(tableNamePrefix)) {
                return map.get(dirName);
            }
        }
        Assert.fail("Table not found in callback map: " + tableNamePrefix);
        return -1; // unreachable
    }

    private static LongList longList(long... values) {
        LongList list = new LongList(values.length);
        for (long v : values) {
            list.add(v);
        }
        return list;
    }

    private static TestServerMain startServerMain(String root, String... envKeyValues) {
        Map<String, String> env = new HashMap<>(System.getenv());
        for (int i = 0; i < envKeyValues.length; i += 2) {
            env.put(envKeyValues[i], envKeyValues[i + 1]);
        }
        env.put(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED.getEnvVarName(), "false");
        // Disable HTTP and PG servers to avoid port conflicts
        env.put(PropertyKey.HTTP_ENABLED.getEnvVarName(), "false");
        env.put(PropertyKey.HTTP_MIN_ENABLED.getEnvVarName(), "false");
        env.put(PropertyKey.PG_ENABLED.getEnvVarName(), "false");
        env.put(PropertyKey.LINE_TCP_ENABLED.getEnvVarName(), "false");

        TestServerMain serverMain = new TestServerMain(new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public Map<String, String> getEnv() {
                        return env;
                    }
                },
                Bootstrap.getServerMainArgs(root)
        ));
        try {
            serverMain.start();
            return serverMain;
        } catch (Throwable th) {
            serverMain.close();
            throw th;
        }
    }

    private static TestServerMain startServerMainWithListener(String root, CheckpointListener listener, String... envKeyValues) {
        Map<String, String> env = new HashMap<>(System.getenv());
        for (int i = 0; i < envKeyValues.length; i += 2) {
            env.put(envKeyValues[i], envKeyValues[i + 1]);
        }
        env.put(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED.getEnvVarName(), "false");
        // Disable HTTP and PG servers to avoid port conflicts
        env.put(PropertyKey.HTTP_ENABLED.getEnvVarName(), "false");
        env.put(PropertyKey.HTTP_MIN_ENABLED.getEnvVarName(), "false");
        env.put(PropertyKey.PG_ENABLED.getEnvVarName(), "false");
        env.put(PropertyKey.LINE_TCP_ENABLED.getEnvVarName(), "false");

        TestServerMain serverMain = new TestServerMain(new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public Map<String, String> getEnv() {
                        return env;
                    }

                    @Override
                    public ServerConfiguration getServerConfiguration(Bootstrap bootstrap) throws Exception {
                        ServerConfiguration baseConfig = super.getServerConfiguration(bootstrap);
                        // Wrap the CairoConfiguration to inject our listener
                        CairoConfiguration wrappedCairoConfig = new CairoConfigurationWrapper(baseConfig.getCairoConfiguration()) {
                            @Override
                            public @NotNull CheckpointListener getCheckpointListener() {
                                return listener;
                            }
                        };
                        return new ServerConfigurationWrapper(baseConfig, wrappedCairoConfig);
                    }
                },
                Bootstrap.getServerMainArgs(root)
        ));
        try {
            serverMain.start();
            return serverMain;
        } catch (Throwable th) {
            serverMain.close();
            throw th;
        }
    }

    private String getTestTableName() {
        return testName.getMethodName().replace('[', '_').replace(']', '_');
    }

    private void testCheckpointCreateCheckTableMetadataFiles(String ddl, String ddl2, String tableName) throws Exception {
        try (Path path = new Path(); Path copyPath = new Path()) {
            path.of(configuration.getDbRoot());
            copyPath.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory());

            execute(ddl);
            if (ddl2 != null) {
                execute(ddl2);
            }

            execute("checkpoint create");

            TableToken tableToken = engine.verifyTableName(tableName);
            path.concat(tableToken);
            int tableNameLen = path.size();
            copyPath.concat(tableToken);
            int copyTableNameLen = copyPath.size();

            // _meta
            path.concat(TableUtils.META_FILE_NAME).$();
            copyPath.concat(TableUtils.META_FILE_NAME).$();
            TestUtils.assertFileContentsEquals(path, copyPath);
            // _txn
            path.trimTo(tableNameLen).concat(TableUtils.TXN_FILE_NAME).$();
            copyPath.trimTo(copyTableNameLen).concat(TableUtils.TXN_FILE_NAME).$();
            TestUtils.assertFileContentsEquals(path, copyPath);
            // _cv
            path.trimTo(tableNameLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
            copyPath.trimTo(copyTableNameLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
            TestUtils.assertFileContentsEquals(path, copyPath);

            if (tableToken.isMatView()) {
                path.trimTo(tableNameLen).concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME).$();
                copyPath.trimTo(copyTableNameLen).concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME).$();
                TestUtils.assertFileContentsEquals(path, copyPath);
                path.trimTo(tableNameLen).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$();
                if (configuration.getFilesFacade().exists(path.$())) {
                    copyPath.trimTo(copyTableNameLen).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$();
                    TestUtils.assertFileContentsEquals(path, copyPath);
                }
            }
            execute("checkpoint release");
        }
    }

    private void testCheckpointPrepareCheckMetadataFile(String snapshotId) throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

            try (Path path = new Path()) {
                execute("create table x as (select * from (select rnd_str(5,10,2) a, x b from long_sequence(20)))");
                execute("checkpoint create");

                path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory());
                FilesFacade ff = configuration.getFilesFacade();
                try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                    mem.smallFile(ff, path.concat(TableUtils.CHECKPOINT_META_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);

                    CharSequence expectedId = configuration.getSnapshotInstanceId();
                    CharSequence actualId = mem.getStrA(0);
                    Assert.assertTrue(Chars.equals(actualId, expectedId));
                }

                execute("checkpoint release");
            }
        });
    }

    private void testCheckpointPrepareCheckTableMetadata(boolean generateColTops, boolean dropColumns) throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory());

                String tableName = "t";
                execute("create table " + tableName + " (a STRING, b LONG)");

                // Bump truncate version by truncating non-empty table
                execute("insert into " + tableName + " VALUES('abasd', 1L)");
                execute("truncate table " + tableName);

                execute(
                        "insert into " + tableName +
                                " select * from (select rnd_str(5,10,2) a, x b from long_sequence(20))"
                );
                if (generateColTops) {
                    execute("alter table " + tableName + " add column c int");
                }
                if (dropColumns) {
                    execute("alter table " + tableName + " drop column a");
                }

                execute("checkpoint create");

                TableToken tableToken = engine.verifyTableName(tableName);
                path.concat(tableToken);
                int tableNameLen = path.size();
                FilesFacade ff = configuration.getFilesFacade();
                try (TableReader tableReader = newOffPoolReader(configuration, "t")) {
                    try (TableReaderMetadata metadata0 = tableReader.getMetadata()) {
                        path.concat(TableUtils.META_FILE_NAME).$();
                        try (TableReaderMetadata metadata = new TableReaderMetadata(configuration)) {
                            metadata.loadMetadata(path.$());
                            // Assert _meta contents.

                            Assert.assertEquals(metadata0.getColumnCount(), metadata.getColumnCount());
                            Assert.assertEquals(metadata0.getPartitionBy(), metadata.getPartitionBy());
                            Assert.assertEquals(metadata0.getTimestampIndex(), metadata.getTimestampIndex());
                            Assert.assertEquals(metadata0.getTableId(), metadata.getTableId());
                            Assert.assertEquals(metadata0.getMaxUncommittedRows(), metadata.getMaxUncommittedRows());
                            Assert.assertEquals(metadata0.getO3MaxLag(), metadata.getO3MaxLag());
                            Assert.assertEquals(metadata0.getMetadataVersion(), metadata.getMetadataVersion());

                            for (int i = 0, n = metadata0.getColumnCount(); i < n; i++) {
                                TableColumnMetadata columnMetadata0 = metadata0.getColumnMetadata(i);
                                TableColumnMetadata columnMetadata1 = metadata0.getColumnMetadata(i);
                                Assert.assertEquals(columnMetadata0.getColumnName(), columnMetadata1.getColumnName());
                                Assert.assertEquals(columnMetadata0.getColumnType(), columnMetadata1.getColumnType());
                                Assert.assertEquals(columnMetadata0.getIndexValueBlockCapacity(), columnMetadata1.getIndexValueBlockCapacity());
                                Assert.assertEquals(columnMetadata0.isSymbolIndexFlag(), columnMetadata1.isSymbolIndexFlag());
                                Assert.assertEquals(columnMetadata0.isSymbolTableStatic(), columnMetadata1.isSymbolTableStatic());
                            }

                            // Assert _txn contents.
                            path.trimTo(tableNameLen).concat(TableUtils.TXN_FILE_NAME).$();
                            try (TxReader txReader0 = tableReader.getTxFile()) {
                                try (TxReader txReader1 = new TxReader(ff).ofRO(path.$(), metadata.getTimestampType(), metadata.getPartitionBy())) {
                                    TableUtils.safeReadTxn(txReader1, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());

                                    Assert.assertEquals(txReader0.getTxn(), txReader1.getTxn());
                                    Assert.assertEquals(txReader0.getTransientRowCount(), txReader1.getTransientRowCount());
                                    Assert.assertEquals(txReader0.getFixedRowCount(), txReader1.getFixedRowCount());
                                    Assert.assertEquals(txReader0.getMinTimestamp(), txReader1.getMinTimestamp());
                                    Assert.assertEquals(txReader0.getMaxTimestamp(), txReader1.getMaxTimestamp());
                                    Assert.assertEquals(txReader0.getMetadataVersion(), txReader1.getMetadataVersion());
                                    Assert.assertEquals(txReader0.getDataVersion(), txReader1.getDataVersion());
                                    Assert.assertEquals(txReader0.getPartitionTableVersion(), txReader1.getPartitionTableVersion());
                                    Assert.assertEquals(1, txReader0.getTruncateVersion());
                                    Assert.assertEquals(txReader0.getTruncateVersion(), txReader1.getTruncateVersion());
                                    for (int i = 0; i < txReader0.getSymbolColumnCount(); i++) {
                                        Assert.assertEquals(txReader0.getSymbolValueCount(i), txReader1.getSymbolValueCount(i));
                                    }
                                    for (int i = 0; i < txReader0.getPartitionCount(); i++) {
                                        Assert.assertEquals(txReader0.getPartitionNameTxn(i), txReader1.getPartitionNameTxn(i));
                                        Assert.assertEquals(txReader0.getPartitionSize(i), txReader1.getPartitionSize(i));
                                        Assert.assertEquals(txReader0.getPartitionTimestampByIndex(i), txReader1.getPartitionTimestampByIndex(i));
                                    }
                                }
                            }

                            // Assert _cv contents.
                            path.trimTo(tableNameLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
                            try (ColumnVersionReader cvReader0 = tableReader.getColumnVersionReader()) {
                                try (ColumnVersionReader cvReader1 = new ColumnVersionReader().ofRO(ff, path.$())) {
                                    cvReader1.readSafe(configuration.getMillisecondClock(), configuration.getSpinLockTimeout());

                                    Assert.assertEquals(cvReader0.getVersion(), cvReader1.getVersion());
                                    TestUtils.assertEquals(cvReader0.getCachedColumnVersionList(), cvReader1.getCachedColumnVersionList());
                                }
                            }
                        }
                    }
                }

                execute("checkpoint release");
            }
        });
    }

    private void testCheckpointRecoveryTornKeyEntry(boolean rebuildColumnIndexes) throws Exception {
        assertMemoryLeak(() -> {
            // Test that checkpoint recovery correctly handles corrupted (torn) index key entries.
            //
            // Scenario: Simulate a torn write by corrupting the .k index file such that
            // valueCount != countCheck for a key entry. This represents an incomplete write
            // that could occur during a crash.
            //
            // - When rebuildColumnIndexes=TRUE: Index is rebuilt from scratch, corruption fixed
            // - When rebuildColumnIndexes=FALSE: Corrupted index is preserved

            Assume.assumeTrue(Os.type != Os.WINDOWS);

            File dir1 = temp.newFolder("server1_torn_" + rebuildColumnIndexes);
            File dir2 = temp.newFolder("server2_torn_" + rebuildColumnIndexes);
            String tableDirName;

            // Server 1: Create data + checkpoint
            try (TestServerMain server1 = startServerMain(dir1.getAbsolutePath())) {
                server1.execute("CREATE TABLE t (sym SYMBOL INDEX, x LONG, ts TIMESTAMP) " +
                        "TIMESTAMP(ts) PARTITION BY YEAR WAL");
                server1.execute("INSERT INTO t SELECT 'SYM', x, timestamp_sequence(0, 100000000000) " +
                        "FROM long_sequence(10)");

                // Wait for WAL to flush
                TestUtils.assertEventually(() -> server1.assertSql(
                        "SELECT sym, x FROM t ORDER BY x",
                        """
                                sym\tx
                                SYM\t1
                                SYM\t2
                                SYM\t3
                                SYM\t4
                                SYM\t5
                                SYM\t6
                                SYM\t7
                                SYM\t8
                                SYM\t9
                                SYM\t10
                                """
                ));

                server1.execute("CHECKPOINT CREATE");
                tableDirName = server1.getEngine().verifyTableName("t").getDirName();
                copyDirectory(dir1, dir2);
                server1.execute("CHECKPOINT RELEASE");
            }

            // Verify index is valid before corruption
            IndexSnapshot beforeCorruption = IndexSnapshot.read(dir2.getAbsolutePath(), tableDirName);
            Assert.assertEquals("Index should have 10 entries", 10, beforeCorruption.getRowIds().size());
            TestUtils.assertEquals(longList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), beforeCorruption.getRowIds());

            // Corrupt the index in dir2: make valueCount != countCheck
            corruptIndexKeyEntry(dir2.getAbsolutePath(), tableDirName);

            // Verify corruption was applied - IndexSnapshot reads valueCount, which we incremented to 11
            IndexSnapshot afterCorruption = IndexSnapshot.read(dir2.getAbsolutePath(), tableDirName);
            Assert.assertEquals("Corrupted index should show 11 entries (valueCount)", 11,
                    afterCorruption.getRowIds().size());

            // Create trigger file to force checkpoint recovery
            try (Path triggerPath = new Path().of(dir2.getAbsolutePath()).concat(TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME)) {
                Files.touch(triggerPath.$());
            }

            // Server 2: Start with corrupted index
            try (TestServerMain server2 = startServerMain(
                    dir2.getAbsolutePath(),
                    CAIRO_CHECKPOINT_RECOVERY_REBUILD_COLUMN_INDEXES.getEnvVarName(), String.valueOf(rebuildColumnIndexes)
            )) {
                IndexSnapshot afterRecovery = IndexSnapshot.read(dir2.getAbsolutePath(), tableDirName);

                if (rebuildColumnIndexes) {
                    // With rebuild=true, index is rebuilt from data files, corruption fixed
                    Assert.assertEquals("Rebuilt index should have 10 entries", 10, afterRecovery.getRowIds().size());
                    TestUtils.assertEquals(longList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), afterRecovery.getRowIds());

                    // Queries should work correctly
                    server2.assertSql(
                            "SELECT count() FROM t WHERE sym = 'SYM'",
                            "count\n10\n"
                    );
                } else {
                    // With rebuild=false, corrupted index is preserved
                    // valueCount=11 but countCheck=10 - the index file still has the corruption
                    Assert.assertEquals("Corrupted index should still show 11 entries", 11,
                            afterRecovery.getRowIds().size());

                    // Query using the corrupted index throws CairoException due to consistency check
                    try {
                        server2.assertSql(
                                "SELECT count() FROM t WHERE sym = 'SYM'",
                                "count\n10\n"
                        );
                        Assert.fail("Expected CairoException due to corrupted index");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getMessage(), "cursor could not consistently read index header [corrupt?]");
                    }
                }
            }
        });
    }

    private void testCheckpointRecoveryWithStaleIndex(boolean rebuildColumnIndexes) throws Exception {
        // Test that checkpoint recovery correctly handles stale index entries.
        //
        // Scenario: User creates checkpoint, then more data is written to the db.
        // User copies the ENTIRE db directory (with stale index files that contain
        // entries beyond the checkpoint's row_count). On restore, the index state
        // depends on the CAIRO_CHECKPOINT_RECOVERY_REBUILD_COLUMN_INDEXES setting:
        //
        // - When rebuildColumnIndexes=TRUE: Checkpoint recovery rebuilds the index
        //   from scratch, so the index immediately has only 10 entries (correct state).
        //
        // - When rebuildColumnIndexes=FALSE: The index file on disk retains the stale
        //   15 entries. However, TableWriter.rollbackIndexes() will clean them up when
        //   the table is first opened for writing (triggered by the lock file presence).
        //
        // This test verifies the index state BEFORE any writes to show the difference.

        Assume.assumeTrue(Os.type != Os.WINDOWS);

        File dir1 = temp.newFolder("server1_" + rebuildColumnIndexes);
        File dir2 = temp.newFolder("server2_" + rebuildColumnIndexes);
        String tableDirName;

        // Server 1: Create data + checkpoint + more data
        try (TestServerMain server1 = startServerMain(dir1.getAbsolutePath())) {
            server1.execute("CREATE TABLE t (sym SYMBOL INDEX, x LONG, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY YEAR WAL");
            server1.execute("INSERT INTO t SELECT 'OLD_SYM', x, timestamp_sequence(0, 100000000000) " +
                    "FROM long_sequence(10)");

            // Wait for WAL to flush and assert full data
            TestUtils.assertEventually(() -> server1.assertSql(
                    "SELECT sym, x FROM t ORDER BY x",
                    """
                            sym\tx
                            OLD_SYM\t1
                            OLD_SYM\t2
                            OLD_SYM\t3
                            OLD_SYM\t4
                            OLD_SYM\t5
                            OLD_SYM\t6
                            OLD_SYM\t7
                            OLD_SYM\t8
                            OLD_SYM\t9
                            OLD_SYM\t10
                            """
            ));

            server1.execute("CHECKPOINT CREATE");

            // Insert MORE of the SAME symbol - this updates the live index
            // The index now has entries for OLD_SYM pointing to row IDs 0-14
            server1.execute("INSERT INTO t SELECT 'OLD_SYM', x + 10, timestamp_sequence(1000000000000, 100000000000) " +
                    "FROM long_sequence(5)");

            // Wait for WAL to flush the new data
            TestUtils.assertEventually(() -> server1.assertSql(
                    "SELECT sym, x FROM t ORDER BY x",
                    """
                            sym\tx
                            OLD_SYM\t1
                            OLD_SYM\t2
                            OLD_SYM\t3
                            OLD_SYM\t4
                            OLD_SYM\t5
                            OLD_SYM\t6
                            OLD_SYM\t7
                            OLD_SYM\t8
                            OLD_SYM\t9
                            OLD_SYM\t10
                            OLD_SYM\t11
                            OLD_SYM\t12
                            OLD_SYM\t13
                            OLD_SYM\t14
                            OLD_SYM\t15
                            """
            ));

            // Get the table directory name for later index reading
            tableDirName = server1.getEngine().verifyTableName("t").getDirName();

            // Copy entire dir1 to dir2 - this simulates a backup AFTER more writes
            // The copied db has:
            // - _txn metadata that says row_count=10 (from checkpoint)
            // - Index files that have entries for OLD_SYM pointing to row IDs 0-14
            copyDirectory(dir1, dir2);

            server1.execute("CHECKPOINT RELEASE");
        }

        // Verify the STALE index state before server2 starts
        // Key 0 is null symbol, Key 1 is OLD_SYM
        IndexSnapshot staleIndex = IndexSnapshot.read(dir2.getAbsolutePath(), tableDirName);
        Assert.assertEquals("Stale index should have maxValue=14 (row IDs 0-14)", 14, staleIndex.maxValue);
        Assert.assertEquals("Stale index should have 2 keys", 2, staleIndex.keyCount);
        TestUtils.assertEquals(longList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14), staleIndex.getRowIds());

        // Create the trigger file in dir2 to force checkpoint recovery on startup
        try (Path triggerPath = new Path().of(dir2.getAbsolutePath()).concat(TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME)) {
            Files.touch(triggerPath.$());
        }

        // Server 2: Start with the specified rebuild setting
        try (TestServerMain ignored = startServerMain(
                dir2.getAbsolutePath(),
                CAIRO_CHECKPOINT_RECOVERY_REBUILD_COLUMN_INDEXES.getEnvVarName(), String.valueOf(rebuildColumnIndexes)
        )) {
            // Read the index AFTER checkpoint recovery but BEFORE any writes.
            // This shows the difference between rebuild=true and rebuild=false.
            IndexSnapshot afterRecoveryIndex = IndexSnapshot.read(dir2.getAbsolutePath(), tableDirName);

            if (rebuildColumnIndexes) {
                // With rebuild=true, checkpoint recovery rebuilt the index from scratch.
                // It should have exactly 10 entries (matching the checkpoint row count) and maxValue=9.
                Assert.assertEquals("With rebuild=true, maxValue should be 9", 9, afterRecoveryIndex.maxValue);
                TestUtils.assertEquals(longList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), afterRecoveryIndex.getRowIds());
            } else {
                // With rebuild=false, the index file on disk STILL has the stale 15 entries.
                // They will be cleaned up by TableWriter.rollbackIndexes() on first write.
                Assert.assertEquals("With rebuild=false, maxValue still stale at 14", 14, afterRecoveryIndex.maxValue);
                TestUtils.assertEquals(longList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14), afterRecoveryIndex.getRowIds());
            }
        }
    }

    private void testRecoverCheckpoint(
            String snapshotId,
            String restartedId,
            boolean createTriggerFile,
            boolean expectRecovery
    ) throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);
            Assert.assertEquals(engine.getConfiguration().getSnapshotInstanceId(), snapshotId);

            final String nonPartitionedTable = "npt";
            execute(
                    "create table " + nonPartitionedTable + " as " +
                            "(select rnd_str(5,10,2) a, x b from long_sequence(20))",
                    sqlExecutionContext
            );
            final String partitionedTable = "pt";
            execute(
                    "create table " + partitionedTable + " as " +
                            "(select x, timestamp_sequence(0, 100000000000) ts from long_sequence(20)) timestamp(ts) partition by hour"
            );

            if (rnd.nextBoolean()) {
                execute("checkpoint create");
                // also rename ".checkpoint" dir to the legacy "snapshot"
                try (
                        Path p1 = new Path();
                        Path p2 = new Path()
                ) {
                    p1.of(configuration.getDbRoot()).concat(TableUtils.LEGACY_CHECKPOINT_DIRECTORY);
                    p2.of(configuration.getDbRoot()).concat(TableUtils.CHECKPOINT_DIRECTORY);
                    Assert.assertEquals(0, ff.rename(p2.$(), p1.$()));
                    path.of(p1).concat(configuration.getDbDirectory());
                    rootLen = path.size();
                }
            } else {
                execute("snapshot prepare");
                path.of(configuration.getDbRoot()).concat(TableUtils.LEGACY_CHECKPOINT_DIRECTORY).concat(configuration.getDbDirectory());
                rootLen = path.size();
            }

            path.trimTo(rootLen).slash$();
            Assert.assertTrue(Utf8s.toString(path), configuration.getFilesFacade().exists(path.$()));

            execute(
                    "insert into " + nonPartitionedTable +
                            " select rnd_str(3,6,2) a, x+20 b from long_sequence(20)"
            );
            execute(
                    "insert into " + partitionedTable +
                            " select x+20 x, timestamp_sequence(100000000000, 100000000000) ts from long_sequence(20)"
            );

            // Release all readers and writers, but keep the snapshot dir around.
            engine.clear();
            node1.setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
            Assert.assertEquals(engine.getConfiguration().getSnapshotInstanceId(), restartedId);

            if (createTriggerFile) {
                createTriggerFile();
            }

            engine.checkpointRecover();

            // In case of recovery, data inserted after PREPARE SNAPSHOT should be discarded.
            int expectedCount = expectRecovery ? 20 : 40;
            assertSql(
                    "count\n" +
                            expectedCount + "\n",
                    "select count() from " + nonPartitionedTable
            );

            assertSql(
                    "count\n" +
                            expectedCount + "\n",
                    "select count() from " + partitionedTable
            );

            // Recovery should delete the snapshot dir. Otherwise, the dir should be kept as is.
            path.trimTo(rootLen).slash$();
            if (expectRecovery == configuration.getFilesFacade().exists(path.$())) {
                if (expectRecovery) {
                    Assert.fail("Recovery should happen but the snapshot path still exists:" + Utf8s.toString(path));
                } else {
                    Assert.fail("Recovery shouldn't happen but the snapshot path does not exist:" + Utf8s.toString(path));
                }
            }

            engine.checkpointRelease();
        });
    }

    private record CheckpointReleaseEvent(long timestampMicros, CharSequenceLongHashMap tableDirNamesToSeqTxn) {
    }

    /**
     * Snapshot of a bitmap index file contents for testing purposes.
     * Reads the .k and .v files and extracts all key entries with their row IDs.
     */
    private static class IndexSnapshot {
        private static final long MAX_VALUE_OFFSET = 37L;

        final IntObjHashMap<LongList> entries;
        final int keyCount;
        final long maxValue;
        final long sequence;

        private IndexSnapshot(IntObjHashMap<LongList> entries, int keyCount, long sequence, long maxValue) {
            this.entries = entries;
            this.keyCount = keyCount;
            this.sequence = sequence;
            this.maxValue = maxValue;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("IndexSnapshot{keyCount=").append(keyCount)
                    .append(", sequence=").append(sequence)
                    .append(", maxValue=").append(maxValue)
                    .append(", entries={\n");
            for (int i = 0; i < keyCount; i++) {
                LongList rowIds = entries.get(i);
                sb.append("  key ").append(i).append(": ").append(rowIds != null ? rowIds.size() : 0).append(" entries");
                if (rowIds != null && rowIds.size() > 0) {
                    sb.append(" [");
                    for (int j = 0; j < Math.min(rowIds.size(), 20); j++) {
                        if (j > 0) sb.append(", ");
                        sb.append(rowIds.get(j));
                    }
                    if (rowIds.size() > 20) sb.append(", ...");
                    sb.append("]");
                }
                sb.append("\n");
            }
            sb.append("}}");
            return sb.toString();
        }

        /**
         * Reads index from the standard location in a QuestDB data directory.
         */
        static IndexSnapshot read(String dbRoot, String tableDirName) {
            File tableDir = new File(dbRoot, "db/" + tableDirName);
            File partDir = new File(tableDir, "1970");

            // Find .k file (may have txn suffix)
            File[] keyFiles = partDir.listFiles((dir, name) -> name.startsWith("sym" + ".k"));
            if (keyFiles == null || keyFiles.length == 0) {
                throw new IllegalStateException("No key file found for column: " + "sym" + " in " + partDir);
            }
            File keyFile = keyFiles[0];
            String vFileName = keyFile.getName().replace(".k", ".v");
            File valueFile = new File(partDir, vFileName);

            try (
                    MemoryMR keyMem = Vm.getCMRInstance();
                    MemoryMR valueMem = Vm.getCMRInstance()
            ) {
                FilesFacade ff = TestFilesFacadeImpl.INSTANCE;

                try (Path keyPath = new Path().of(keyFile.getAbsolutePath())) {
                    keyMem.of(ff, keyPath.$(), ff.getMapPageSize(), keyFile.length(),
                            MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE, -1);
                }

                long sequence = keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
                int blockValueCount = keyMem.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_BLOCK_VALUE_COUNT);
                int keyCount = keyMem.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
                long maxValue = keyMem.getLong(MAX_VALUE_OFFSET);

                try (Path valuePath = new Path().of(valueFile.getAbsolutePath())) {
                    valueMem.of(ff, valuePath.$(), valueFile.length(), valueFile.length(), MemoryTag.MMAP_DEFAULT);
                }

                IntObjHashMap<LongList> entries = new IntObjHashMap<>();

                // Read each key entry
                for (int key = 0; key < keyCount; key++) {
                    long keyEntryOffset = BitmapIndexUtils.getKeyEntryOffset(key);
                    long valueCount = keyMem.getLong(keyEntryOffset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);
                    long firstBlockOffset = keyMem.getLong(keyEntryOffset + BitmapIndexUtils.KEY_ENTRY_OFFSET_FIRST_VALUE_BLOCK_OFFSET);

                    LongList rowIds = new LongList();
                    if (valueCount > 0 && firstBlockOffset < valueMem.size()) {
                        long remaining = valueCount;
                        long blockOffset = firstBlockOffset;
                        int blockValueCountMod = blockValueCount - 1;

                        while (remaining > 0 && blockOffset < valueMem.size()) {
                            long cellCount = Math.min(remaining, blockValueCount);
                            for (int i = 0; i < cellCount; i++) {
                                long rowId = valueMem.getLong(blockOffset + i * 8L);
                                rowIds.add(rowId);
                            }
                            remaining -= cellCount;
                            if (remaining > 0) {
                                // Next block pointer is after the values
                                long nextBlockPtrOffset = (blockValueCountMod + 1) * 8L + 8;
                                blockOffset = valueMem.getLong(blockOffset + nextBlockPtrOffset);
                            }
                        }
                    }
                    entries.put(key, rowIds);
                }

                return new IndexSnapshot(entries, keyCount, sequence, maxValue);
            }
        }

        LongList getRowIds() {
            return entries.get(1);
        }
    }

    /**
     * Wrapper for ServerConfiguration that allows overriding the CairoConfiguration.
     */
    private static class ServerConfigurationWrapper implements ServerConfiguration {
        private final CairoConfiguration cairoConfig;
        private final ServerConfiguration delegate;

        ServerConfigurationWrapper(ServerConfiguration delegate, CairoConfiguration cairoConfig) {
            this.delegate = delegate;
            this.cairoConfig = cairoConfig;
        }

        @Override
        public CairoConfiguration getCairoConfiguration() {
            return cairoConfig;
        }

        @Override
        public WorkerPoolConfiguration getExportPoolConfiguration() {
            return delegate.getExportPoolConfiguration();
        }

        @Override
        public FactoryProvider getFactoryProvider() {
            return delegate.getFactoryProvider();
        }

        @Override
        public HttpServerConfiguration getHttpMinServerConfiguration() {
            return delegate.getHttpMinServerConfiguration();
        }

        @Override
        public HttpFullFatServerConfiguration getHttpServerConfiguration() {
            return delegate.getHttpServerConfiguration();
        }

        @Override
        public LineTcpReceiverConfiguration getLineTcpReceiverConfiguration() {
            return delegate.getLineTcpReceiverConfiguration();
        }

        @Override
        public LineUdpReceiverConfiguration getLineUdpReceiverConfiguration() {
            return delegate.getLineUdpReceiverConfiguration();
        }

        @Override
        public WorkerPoolConfiguration getMatViewRefreshPoolConfiguration() {
            return delegate.getMatViewRefreshPoolConfiguration();
        }

        @Override
        public MemoryConfiguration getMemoryConfiguration() {
            return delegate.getMemoryConfiguration();
        }

        @Override
        public Metrics getMetrics() {
            return delegate.getMetrics();
        }

        @Override
        public MetricsConfiguration getMetricsConfiguration() {
            return delegate.getMetricsConfiguration();
        }

        @Override
        public PGConfiguration getPGWireConfiguration() {
            return delegate.getPGWireConfiguration();
        }

        @Override
        public PublicPassthroughConfiguration getPublicPassthroughConfiguration() {
            return delegate.getPublicPassthroughConfiguration();
        }

        @Override
        public WorkerPoolConfiguration getSharedWorkerPoolNetworkConfiguration() {
            return delegate.getSharedWorkerPoolNetworkConfiguration();
        }

        @Override
        public WorkerPoolConfiguration getSharedWorkerPoolQueryConfiguration() {
            return delegate.getSharedWorkerPoolQueryConfiguration();
        }

        @Override
        public WorkerPoolConfiguration getSharedWorkerPoolWriteConfiguration() {
            return delegate.getSharedWorkerPoolWriteConfiguration();
        }

        @Override
        public WorkerPoolConfiguration getViewCompilerPoolConfiguration() {
            return delegate.getViewCompilerPoolConfiguration();
        }

        @Override
        public WorkerPoolConfiguration getWalApplyPoolConfiguration() {
            return delegate.getWalApplyPoolConfiguration();
        }

        @Override
        public void init(io.questdb.cairo.CairoEngine engine, FreeOnExit freeOnExit) {
            delegate.init(engine, freeOnExit);
        }
    }

    private static class TestFilesFacade extends TestFilesFacadeImpl {

        boolean errorOnRegistryFileCopy = false;
        boolean errorOnRegistryFileRemoval = false;
        boolean errorOnSync = false;
        boolean errorOnTriggerFileRemoval = false;

        @Override
        public int copy(LPSZ from, LPSZ to) {
            if (errorOnRegistryFileCopy
                    && Utf8s.endsWithAscii(from, WalUtils.TABLE_REGISTRY_NAME_FILE + ".0")
                    && Utf8s.endsWithAscii(to, WalUtils.TABLE_REGISTRY_NAME_FILE + ".0")
            ) {
                return -1;
            }
            return super.copy(from, to);
        }

        @Override
        public boolean removeQuiet(LPSZ name) {
            if (errorOnTriggerFileRemoval && Utf8s.endsWithAscii(name, TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME)) {
                return false;
            }
            if (errorOnRegistryFileRemoval && Utf8s.endsWithAscii(name, WalUtils.TABLE_REGISTRY_NAME_FILE + ".0")) { // version 0
                return false;
            }
            return super.removeQuiet(name);
        }

        @Override
        public int sync() {
            if (!errorOnSync) {
                return super.sync();
            }
            return -1;
        }

        void reset() {
            errorOnSync = false;
            errorOnTriggerFileRemoval = false;
            errorOnRegistryFileRemoval = false;
            errorOnRegistryFileCopy = false;
        }
    }
}
