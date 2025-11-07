/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
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
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.SimpleWaitingLock;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.questdb.PropertyKey.CAIRO_CHECKPOINT_RECOVERY_ENABLED;
import static io.questdb.PropertyKey.CAIRO_LEGACY_SNAPSHOT_RECOVERY_ENABLED;

public class CheckpointTest extends AbstractCairoTest {
    private static final TestFilesFacade testFilesFacade = new TestFilesFacade();
    static int SCOREBOARD_FORMAT = 1;
    private static Path path;
    private static Rnd rnd;
    private static Path triggerFilePath;
    private int rootLen;

    public CheckpointTest() throws Exception {
        int scoreboardFormat = TestUtils.generateRandom(LOG).nextBoolean() ? 1 : 2;
        if (scoreboardFormat != SCOREBOARD_FORMAT) {
            SCOREBOARD_FORMAT = scoreboardFormat;
            tearDownStatic();
            setUpStatic();
        }
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_TXN_SCOREBOARD_FORMAT, SCOREBOARD_FORMAT);
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
            lock.lock();
            execute("checkpoint release");
            Assert.assertFalse(lock.isLocked());

            engine.setWalPurgeJobRunLock(null);
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
                TestUtils.assertContains(e.getMessage(), "could not remove registry file");
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

            execute("checkpoint create");

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

    private static void createTriggerFile() {
        Files.touch(triggerFilePath.$());
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
