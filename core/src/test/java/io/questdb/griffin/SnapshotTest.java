/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.griffin.wal.WalPurgeJobTest;
import io.questdb.mp.SimpleWaitingLock;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import java.util.concurrent.CountDownLatch;

public class SnapshotTest extends AbstractGriffinTest {

    private static final TestFilesFacadeImpl testFilesFacade = new TestFilesFacadeImpl();
    private static Path path;
    private int rootLen;

    @BeforeClass
    public static void setUpStatic() {
        path = new Path();
        ff = testFilesFacade;
        AbstractGriffinTest.setUpStatic();
    }

    @AfterClass
    public static void tearDownStatic() {
        path = Misc.free(path);
        AbstractGriffinTest.tearDownStatic();
    }

    @Before
    public void setUp() {
        // sync() system call is not available on Windows, so we skip the whole test suite there.
        Assume.assumeTrue(Os.type != Os.WINDOWS);

        super.setUp();
        path.of(configuration.getSnapshotRoot()).concat(configuration.getDbDirectory()).slash();
        rootLen = path.length();
        testFilesFacade.errorOnSync = false;
    }

    @After
    public void tearDown() {
        super.tearDown();
        path.trimTo(rootLen);
        configuration.getFilesFacade().rmdir(path.slash$());
    }

    @Test
    public void testRecoverSnapshotForDefaultInstanceIds() throws Exception {
        testRecoverSnapshot(null, null, false);
    }

    @Test
    public void testRecoverSnapshotForDefaultRestartedId() throws Exception {
        testRecoverSnapshot("id1", null, false);
    }

    @Test
    public void testRecoverSnapshotForDefaultSnapshotId() throws Exception {
        testRecoverSnapshot(null, "id1", false);
    }

    @Test
    public void testRecoverSnapshotForDifferentInstanceIds() throws Exception {
        testRecoverSnapshot("id1", "id2", true);
    }

    @Test
    public void testRecoverSnapshotForDifferentInstanceIdsWhenRecoveryIsDisabled() throws Exception {
        snapshotRecoveryEnabled = false;
        testRecoverSnapshot("id1", "id2", false);
    }

    @Test
    public void testRecoverSnapshotForEqualInstanceIds() throws Exception {
        testRecoverSnapshot("id1", "id1", false);
    }

    @Test
    public void testRecoverSnapshotLargePartitionCount() throws Exception {
        final int partitionCount = 2000;
        final String snapshotId = "id1";
        final String restartedId = "id2";
        assertMemoryLeak(() -> {
            snapshotInstanceId = snapshotId;

            final String tableName = "t";
            compile("create table " + tableName + " as " +
                            "(select x, timestamp_sequence(0, 100000000000) ts from long_sequence(" + partitionCount + ")) timestamp(ts) partition by day",
                    sqlExecutionContext);

            compiler.compile("snapshot prepare", sqlExecutionContext);

            compile("insert into " + tableName +
                    " select x+20 x, timestamp_sequence(100000000000, 100000000000) ts from long_sequence(3)", sqlExecutionContext);

            // Release all readers and writers, but keep the snapshot dir around.
            snapshotAgent.clear();
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            snapshotInstanceId = restartedId;

            DatabaseSnapshotAgent.recoverSnapshot(engine);

            // Data inserted after PREPARE SNAPSHOT should be discarded.
            assertSql("select count() from " + tableName, "count\n" +
                    partitionCount + "\n");
        });
    }

    @Ignore("Enable when table readers start preventing from column file deletion. This could be done along with column versioning.")
    @Test
    public void testRecoverSnapshotRestoresDroppedColumns() throws Exception {
        final String snapshotId = "00000000-0000-0000-0000-000000000000";
        final String restartedId = "123e4567-e89b-12d3-a456-426614174000";
        assertMemoryLeak(() -> {
            snapshotInstanceId = snapshotId;

            final String tableName = "t";
            compile("create table " + tableName + " as " +
                            "(select rnd_str(2,3,0) a, rnd_symbol('A','B','C') b, x c from long_sequence(3))",
                    sqlExecutionContext);

            compiler.compile("snapshot prepare", sqlExecutionContext);

            final String expectedAllColumns = "a\tb\tc\n" +
                    "JW\tC\t1\n" +
                    "WH\tB\t2\n" +
                    "PE\tB\t3\n";
            assertSql("select * from " + tableName, expectedAllColumns);

            compile("alter table " + tableName + " drop column b", sqlExecutionContext);
            assertSql("select * from " + tableName, "a\tc\n" +
                    "JW\t1\n" +
                    "WH\t2\n" +
                    "PE\t3\n");

            // Release all readers and writers, but keep the snapshot dir around.
            snapshotAgent.clear();
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            snapshotInstanceId = restartedId;

            DatabaseSnapshotAgent.recoverSnapshot(engine);

            // Dropped column should be there.
            assertSql("select * from " + tableName, expectedAllColumns);
        });
    }

    @Test
    public void testRunWalPurgeJobLockTimeout() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table test (ts timestamp, name symbol, val int)", sqlExecutionContext);
            SimpleWaitingLock lock = new SimpleWaitingLock();
            CountDownLatch latch1 = new CountDownLatch(1);
            CountDownLatch latch2 = new CountDownLatch(1);

            circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
                @Override
                public long getTimeout() {
                    return 10000;
                }
            };

            snapshotAgent.setWalPurgeJobRunLock(lock);

            Thread t = new Thread(() -> {
                lock.lock(); //emulate WalPurgeJob running with lock
                latch2.countDown();
                try {
                    latch1.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    lock.unlock();
                }
            });

            try {
                t.start();
                latch2.await();
                compiler.compile("snapshot prepare", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException ex) {
                latch1.countDown();
                t.join();
                Assert.assertFalse(lock.isLocked());
                Assert.assertTrue(ex.getMessage().startsWith("[0] Snapshot command cancelled due to timeout"));
            } finally {
                compiler.compile("snapshot complete", sqlExecutionContext);
                Assert.assertFalse(lock.isLocked());
                circuitBreakerConfiguration = null;
                snapshotAgent.setWalPurgeJobRunLock(null);
            }
        });
    }

    @Test
    public void testSnapshotCompleteDeletesSnapshotDir() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table test (ts timestamp, name symbol, val int)", sqlExecutionContext);
            compiler.compile("snapshot prepare", sqlExecutionContext);
            compiler.compile("snapshot complete", sqlExecutionContext);

            path.trimTo(rootLen).slash$();
            Assert.assertFalse(configuration.getFilesFacade().exists(path));
        });
    }

    @Test
    public void testSnapshotCompleteWithoutPrepareIsIgnored() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table test (ts timestamp, name symbol, val int)", sqlExecutionContext);
            // Verify that SNAPSHOT COMPLETE doesn't return errors.
            compiler.compile("snapshot complete", sqlExecutionContext);
        });
    }

    @Test
    public void testSnapshotDbWithWalTable() throws Exception {
        assertMemoryLeak(() -> {
            for (char i = 'a'; i < 'd'; i++) {
                compile("create table " + i + " (ts timestamp, name symbol, val int)", sqlExecutionContext);
            }

            for (char i = 'd'; i < 'f'; i++) {
                compile("create table " + i + " (ts timestamp, name symbol, val int) timestamp(ts) partition by DAY WAL", sqlExecutionContext);
            }

            compiler.compile("snapshot prepare", sqlExecutionContext);
            compiler.compile("snapshot complete", sqlExecutionContext);
        });
    }

    @Test
    public void testSnapshotPrepare() throws Exception {
        assertMemoryLeak(() -> {
            for (char i = 'a'; i < 'f'; i++) {
                compile("create table " + i + " (ts timestamp, name symbol, val int)", sqlExecutionContext);
            }

            compiler.compile("snapshot prepare", sqlExecutionContext);
            compiler.compile("snapshot complete", sqlExecutionContext);
        });
    }

    @Test
    public void testSnapshotPrepareCheckMetadataFileForDefaultInstanceId() throws Exception {
        testSnapshotPrepareCheckMetadataFile(null);
    }

    @Test
    public void testSnapshotPrepareCheckMetadataFileForNonDefaultInstanceId() throws Exception {
        testSnapshotPrepareCheckMetadataFile("foobar");
    }

    @Test
    public void testSnapshotPrepareCheckTableMetadata() throws Exception {
        testSnapshotPrepareCheckTableMetadata(false, false);
    }

    @Test
    public void testSnapshotPrepareCheckTableMetadataFilesForNonPartitionedTable() throws Exception {
        final String tableName = "test";
        testSnapshotPrepareCheckTableMetadataFiles(
                "create table " + tableName + " (a symbol, b double, c long)",
                null,
                tableName
        );
    }

    @Test
    public void testSnapshotPrepareCheckTableMetadataFilesForPartitionedTable() throws Exception {
        final String tableName = "test";
        testSnapshotPrepareCheckTableMetadataFiles(
                "create table " + tableName + " as " +
                        " (select x, timestamp_sequence(0, 100000000000) ts from long_sequence(20)) timestamp(ts) partition by day",
                null,
                tableName
        );
    }

    @Test
    public void testSnapshotPrepareCheckTableMetadataFilesForTableWithDroppedColumns() throws Exception {
        final String tableName = "test";
        testSnapshotPrepareCheckTableMetadataFiles(
                "create table " + tableName + " (a symbol index capacity 128, b double, c long)",
                "alter table " + tableName + " drop column c",
                tableName
        );
    }

    @Test
    public void testSnapshotPrepareCheckTableMetadataFilesForTableWithIndex() throws Exception {
        final String tableName = "test";
        testSnapshotPrepareCheckTableMetadataFiles(
                "create table " + tableName + " (a symbol index capacity 128, b double, c long)",
                null,
                tableName
        );
    }

    @Test
    public void testSnapshotPrepareCheckTableMetadataFilesForWithParameters() throws Exception {
        final String tableName = "test";
        testSnapshotPrepareCheckTableMetadataFiles(
                "create table " + tableName +
                        " (a symbol, b double, c long, ts timestamp) timestamp(ts) partition by hour with maxUncommittedRows=250000, commitLag = 240s",
                null,
                tableName
        );
    }

    @Test
    public void testSnapshotPrepareCheckTableMetadataWithColTops() throws Exception {
        testSnapshotPrepareCheckTableMetadata(true, false);
    }

    @Test
    public void testSnapshotPrepareCheckTableMetadataWithColTopsAndDroppedColumns() throws Exception {
        testSnapshotPrepareCheckTableMetadata(true, true);
    }

    @Test
    public void testSnapshotPrepareCheckTableMetadataWithDroppedColumns() throws Exception {
        testSnapshotPrepareCheckTableMetadata(true, true);
    }

    @Test
    public void testSnapshotPrepareCleansUpSnapshotDir() throws Exception {
        assertMemoryLeak(() -> {
            path.trimTo(rootLen);
            FilesFacade ff = configuration.getFilesFacade();
            int rc = ff.mkdirs(path.slash$(), configuration.getMkDirMode());
            Assert.assertEquals(0, rc);

            // Create a test file.
            path.trimTo(rootLen).concat("test.txt").$();
            Assert.assertTrue(Files.touch(path));

            compile("create table test (ts timestamp, name symbol, val int)", sqlExecutionContext);
            compiler.compile("snapshot prepare", sqlExecutionContext);

            // The test file should be deleted by SNAPSHOT PREPARE.
            Assert.assertFalse(ff.exists(path));

            compiler.compile("snapshot complete", sqlExecutionContext);
        });
    }

    @Test
    public void testSnapshotPrepareEmptyFolder() throws Exception {
        final String tableName = "test";
        path.of(configuration.getRoot()).concat("empty_folder").slash$();
        FilesFacadeImpl.INSTANCE.mkdirs(path, configuration.getMkDirMode());

        testSnapshotPrepareCheckTableMetadataFiles(
                "create table " + tableName + " (a symbol index capacity 128, b double, c long)",
                null,
                tableName
        );

        // Assert snapshot folder exists
        Assert.assertTrue(FilesFacadeImpl.INSTANCE.exists(
                path.of(configuration.getSnapshotRoot()).slash$())
        );
        // But snapshot/db folder does not
        Assert.assertFalse(FilesFacadeImpl.INSTANCE.exists(
                path.of(configuration.getSnapshotRoot()).concat(configuration.getDbDirectory()).slash$())
        );
    }

    @Test
    public void testSnapshotPrepareFailsOnCorruptedTable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "t";
            compile("create table " + tableName + " (ts timestamp, name symbol, val int)", sqlExecutionContext);

            // Corrupt the table by removing _txn file.
            FilesFacade ff = configuration.getFilesFacade();
            Assert.assertTrue(ff.remove(path.of(root).concat(tableName).concat(TableUtils.TXN_FILE_NAME).$()));

            try {
                compiler.compile("snapshot prepare", sqlExecutionContext);
                Assert.fail();
            } catch (CairoException ex) {
                Assert.assertTrue(ex.getMessage().contains("Cannot append. File does not exist"));
            }
        });
    }

    @Test
    public void testSnapshotPrepareFailsOnSyncError() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table test (ts timestamp, name symbol, val int)", sqlExecutionContext);

            testFilesFacade.errorOnSync = true;
            try {
                compiler.compile("snapshot prepare", sqlExecutionContext);
                Assert.fail();
            } catch (CairoException ex) {
                Assert.assertTrue(ex.getMessage().contains("Could not sync"));
            }

            // Once the error is gone, subsequent PREPARE/COMPLETE statements should execute successfully.
            testFilesFacade.errorOnSync = false;
            compiler.compile("snapshot prepare", sqlExecutionContext);
            compiler.compile("snapshot complete", sqlExecutionContext);
        });
    }

    @Test
    public void testSnapshotPrepareOnEmptyDatabase() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("snapshot prepare", sqlExecutionContext);
            compiler.compile("snapshot complete", sqlExecutionContext);
        });
    }

    @Test
    public void testSnapshotPrepareOnEmptyDatabaseWithLock() throws Exception {
        assertMemoryLeak(() -> {
            SimpleWaitingLock lock = new SimpleWaitingLock();

            circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
                @Override
                public long getTimeout() {
                    return 10;
                }
            };

            snapshotAgent.setWalPurgeJobRunLock(lock);
            Assert.assertFalse(lock.isLocked());
            compiler.compile("snapshot prepare", sqlExecutionContext);
            Assert.assertTrue(lock.isLocked());
            try {
                compiler.compile("snapshot prepare", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertTrue(lock.isLocked());
                Assert.assertTrue(ex.getMessage().startsWith("[0] Snapshot command cancelled due to timeout"));
            }
            compiler.compile("snapshot complete", sqlExecutionContext);
            Assert.assertFalse(lock.isLocked());


            //DB is empty
            compiler.compile("snapshot complete", sqlExecutionContext);
            Assert.assertFalse(lock.isLocked());
            lock.lock();
            compiler.compile("snapshot complete", sqlExecutionContext);
            Assert.assertFalse(lock.isLocked());

            circuitBreakerConfiguration = null;
            snapshotAgent.setWalPurgeJobRunLock(null);
        });
    }

    @Test
    public void testSnapshotPrepareSubsequentCallFails() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table test (ts timestamp, name symbol, val int)", sqlExecutionContext);

            SimpleWaitingLock lock = new SimpleWaitingLock();

            circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
                @Override
                public long getTimeout() {
                    return 10;
                }
            };

            snapshotAgent.setWalPurgeJobRunLock(lock);
            try {

                Assert.assertFalse(lock.isLocked());
                compiler.compile("snapshot prepare", sqlExecutionContext);
                Assert.assertTrue(lock.isLocked());
                compiler.compile("snapshot prepare", sqlExecutionContext);
                Assert.assertTrue(lock.isLocked());
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertTrue(ex.getMessage().startsWith("[0] Waiting for SNAPSHOT COMPLETE to be called"));
            } finally {
                Assert.assertTrue(lock.isLocked());
                compiler.compile("snapshot complete", sqlExecutionContext);
                Assert.assertFalse(lock.isLocked());

                circuitBreakerConfiguration = null;
                snapshotAgent.setWalPurgeJobRunLock(null);
            }
        });
    }

    @Test
    public void testSnapshotPrepareSubsequentCallFailsWithLock() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table test (ts timestamp, name symbol, val int)", sqlExecutionContext);
            try {
                compiler.compile("snapshot prepare", sqlExecutionContext);
                compiler.compile("snapshot prepare", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertTrue(ex.getMessage().startsWith("[0] Waiting for SNAPSHOT COMPLETE to be called"));
            } finally {
                compiler.compile("snapshot complete", sqlExecutionContext);
            }
        });
    }

    @Test
    public void testSnapshotUnknownSubOptionFails() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table test (ts timestamp, name symbol, val int)", sqlExecutionContext);
            try {
                compiler.compile("snapshot commit", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertTrue(ex.getMessage().startsWith("[9] 'prepare' or 'complete' expected"));
            }
        });
    }

    @Test
    public void testSuspendResumeWalPurgeJob() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            assertWalExistence(true, tableName, 1);
            assertSegmentExistence(true, tableName, 1, 0);

            drainWalQueue();

            assertWalExistence(true, tableName, 1);

            assertSql(tableName, "x\tts\n" +
                    "1\t2022-02-24T00:00:00.000000Z\n" +
                    "2\t2022-02-24T00:00:01.000000Z\n" +
                    "3\t2022-02-24T00:00:02.000000Z\n" +
                    "4\t2022-02-24T00:00:03.000000Z\n" +
                    "5\t2022-02-24T00:00:04.000000Z\n");

            WalPurgeJobTest.MicrosecondClockMock clock = new WalPurgeJobTest.MicrosecondClockMock();
            final long interval = engine.getConfiguration().getWalPurgeInterval() * 1000;
            FilesFacade ff = configuration.getFilesFacade();
            WalPurgeJob job = new WalPurgeJob(engine, ff, clock);
            snapshotAgent.setWalPurgeJobRunLock(job.getRunLock());

            compiler.compile("snapshot prepare", sqlExecutionContext);
            Thread controlThread1 = new Thread(() -> {
                clock.timestamp = interval;
                while (job.run(0)) {
                    // run until empty
                }
                Path.clearThreadLocals();
            });

            controlThread1.start();
            controlThread1.join();

            assertSegmentExistence(true, tableName, 1, 0);
            assertWalExistence(true, tableName, 1);

            engine.releaseInactive();

            compiler.compile("snapshot complete", sqlExecutionContext);
            Thread controlThread2 = new Thread(() -> {
                clock.timestamp = 2 * interval;
                while (job.run(0)) {
                    // run until empty
                }
                Path.clearThreadLocals();
            });

            controlThread2.start();
            controlThread2.join();

            job.close();
            snapshotAgent.setWalPurgeJobRunLock(null);

            assertSegmentExistence(false, tableName, 1, 0);
            assertWalExistence(false, tableName, 1);
        });
    }

    @Test
    public void testWalMetadataRecovery() throws Exception {
        final String snapshotId = "id1";
        final String restartedId = "id2";
        assertMemoryLeak(() -> {
            snapshotInstanceId = snapshotId;
            String tableName = testName.getMethodName() + "_abc";
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            drainWalQueue();

            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\n" +
                    "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T00:00:03.000000Z\tFG\n" +
                    "5\tAB\t2022-02-24T00:00:04.000000Z\tDE\n");

            compile("alter table " + tableName + " add column jjj int");
            executeInsert("insert into " + tableName + " values (101, 'dfd', '2022-02-24T01', 'asd', 42)");


            compiler.compile("snapshot prepare", sqlExecutionContext);

            // ignored updates
            compile("alter table " + tableName + " add column kkk int");
            executeInsert("insert into " + tableName + " values (102, 'dfd', '2022-02-24T02', 'asdf', 1, 11)");
            executeInsert("insert into " + tableName + " values (103, 'dfd', '2022-02-24T03', 'asdfe', 2, 22)");


            // Release all readers and writers, but keep the snapshot dir around.
            snapshotAgent.clear();
            engine.releaseInactive();

            snapshotInstanceId = restartedId;
            DatabaseSnapshotAgent.recoverSnapshot(engine);

            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\n" +
                    "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T00:00:03.000000Z\tFG\n" +
                    "5\tAB\t2022-02-24T00:00:04.000000Z\tDE\n");

            drainWalQueue();

            assertSql(tableName, "x\tsym\tts\tsym2\tjjj\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\tNaN\n" +
                    "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\tNaN\n" +
                    "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\tNaN\n" +
                    "4\tCD\t2022-02-24T00:00:03.000000Z\tFG\tNaN\n" +
                    "5\tAB\t2022-02-24T00:00:04.000000Z\tDE\tNaN\n" +
                    "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\t42\n");
        });
    }

    private void testRecoverSnapshot(String snapshotId, String restartedId, boolean expectRecovery) throws Exception {
        assertMemoryLeak(() -> {
            snapshotInstanceId = snapshotId;

            final String nonPartitionedTable = "npt";
            compile("create table " + nonPartitionedTable + " as " +
                            "(select rnd_str(5,10,2) a, x b from long_sequence(20))",
                    sqlExecutionContext);
            final String partitionedTable = "pt";
            compile("create table " + partitionedTable + " as " +
                            "(select x, timestamp_sequence(0, 100000000000) ts from long_sequence(20)) timestamp(ts) partition by hour",
                    sqlExecutionContext);

            compiler.compile("snapshot prepare", sqlExecutionContext);

            compile("insert into " + nonPartitionedTable +
                    " select rnd_str(3,6,2) a, x+20 b from long_sequence(20)", sqlExecutionContext);
            compile("insert into " + partitionedTable +
                    " select x+20 x, timestamp_sequence(100000000000, 100000000000) ts from long_sequence(20)", sqlExecutionContext);

            // Release all readers and writers, but keep the snapshot dir around.
            snapshotAgent.clear();
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            snapshotInstanceId = restartedId;

            DatabaseSnapshotAgent.recoverSnapshot(engine);

            // In case of recovery, data inserted after PREPARE SNAPSHOT should be discarded.
            int expectedCount = expectRecovery ? 20 : 40;
            assertSql("select count() from " + nonPartitionedTable, "count\n" +
                    expectedCount + "\n");
            assertSql("select count() from " + partitionedTable, "count\n" +
                    expectedCount + "\n");

            // Recovery should delete the snapshot dir. Otherwise, the dir should be kept as is.
            path.trimTo(rootLen).slash$();
            Assert.assertEquals(!expectRecovery, configuration.getFilesFacade().exists(path));
        });
    }

    private void testSnapshotPrepareCheckMetadataFile(String snapshotId) throws Exception {
        assertMemoryLeak(() -> {
            snapshotInstanceId = snapshotId;

            try (Path path = new Path()) {
                compile("create table x as (select * from (select rnd_str(5,10,2) a, x b from long_sequence(20)))", sqlExecutionContext);
                compiler.compile("snapshot prepare", sqlExecutionContext);

                path.of(configuration.getSnapshotRoot()).concat(configuration.getDbDirectory());
                FilesFacade ff = configuration.getFilesFacade();
                try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                    mem.smallFile(ff, path.concat(TableUtils.SNAPSHOT_META_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);

                    CharSequence expectedId = configuration.getSnapshotInstanceId();
                    CharSequence actualId = mem.getStr(0);
                    Assert.assertTrue(Chars.equals(actualId, expectedId));
                }

                compiler.compile("snapshot complete", sqlExecutionContext);
            }
        });
    }

    private void testSnapshotPrepareCheckTableMetadata(boolean generateColTops, boolean dropColumns) throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                path.of(configuration.getSnapshotRoot()).concat(configuration.getDbDirectory());

                String tableName = "t";
                compile("create table " + tableName + " (a STRING, b LONG)");

                // Bump truncate version by truncating non-empty table
                compile("insert into " + tableName + " VALUES('abasd', 1L)");
                compile("truncate table " + tableName);

                compile("insert into " + tableName +
                        " select * from (select rnd_str(5,10,2) a, x b from long_sequence(20))", sqlExecutionContext);
                if (generateColTops) {
                    compile("alter table " + tableName + " add column c int", sqlExecutionContext);
                }
                if (dropColumns) {
                    compile("alter table " + tableName + " drop column a", sqlExecutionContext);
                }

                compiler.compile("snapshot prepare", sqlExecutionContext);

                path.concat(tableName);
                int tableNameLen = path.length();
                FilesFacade ff = configuration.getFilesFacade();
                try (TableReader tableReader = new TableReader(configuration, "t")) {
                    try (TableReaderMetadata metadata0 = tableReader.getMetadata()) {
                        path.concat(TableUtils.META_FILE_NAME).$();
                        try (TableReaderMetadata metadata = new TableReaderMetadata(configuration)) {
                            metadata.load(path);
                            // Assert _meta contents.

                            Assert.assertEquals(metadata0.getColumnCount(), metadata.getColumnCount());
                            Assert.assertEquals(metadata0.getPartitionBy(), metadata.getPartitionBy());
                            Assert.assertEquals(metadata0.getTimestampIndex(), metadata.getTimestampIndex());
                            Assert.assertEquals(metadata0.getTableId(), metadata.getTableId());
                            Assert.assertEquals(metadata0.getMaxUncommittedRows(), metadata.getMaxUncommittedRows());
                            Assert.assertEquals(metadata0.getCommitLag(), metadata.getCommitLag());
                            Assert.assertEquals(metadata0.getStructureVersion(), metadata.getStructureVersion());

                            for (int i = 0, n = metadata0.getColumnCount(); i < n; i++) {
                                TableColumnMetadata columnMetadata0 = metadata0.getColumnMetadata(i);
                                TableColumnMetadata columnMetadata1 = metadata0.getColumnMetadata(i);
                                Assert.assertEquals(columnMetadata0.getName(), columnMetadata1.getName());
                                Assert.assertEquals(columnMetadata0.getType(), columnMetadata1.getType());
                                Assert.assertEquals(columnMetadata0.getIndexValueBlockCapacity(), columnMetadata1.getIndexValueBlockCapacity());
                                Assert.assertEquals(columnMetadata0.isIndexed(), columnMetadata1.isIndexed());
                                Assert.assertEquals(columnMetadata0.isSymbolTableStatic(), columnMetadata1.isSymbolTableStatic());
                            }

                            // Assert _txn contents.
                            path.trimTo(tableNameLen).concat(TableUtils.TXN_FILE_NAME).$();
                            try (TxReader txReader0 = tableReader.getTxFile()) {
                                try (TxReader txReader1 = new TxReader(ff).ofRO(path, metadata.getPartitionBy())) {
                                    TableUtils.safeReadTxn(txReader1, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());

                                    Assert.assertEquals(txReader0.getTxn(), txReader1.getTxn());
                                    Assert.assertEquals(txReader0.getTransientRowCount(), txReader1.getTransientRowCount());
                                    Assert.assertEquals(txReader0.getFixedRowCount(), txReader1.getFixedRowCount());
                                    Assert.assertEquals(txReader0.getMinTimestamp(), txReader1.getMinTimestamp());
                                    Assert.assertEquals(txReader0.getMaxTimestamp(), txReader1.getMaxTimestamp());
                                    Assert.assertEquals(txReader0.getStructureVersion(), txReader1.getStructureVersion());
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
                                        Assert.assertEquals(txReader0.getPartitionTimestamp(i), txReader1.getPartitionTimestamp(i));
                                        Assert.assertEquals(txReader0.getPartitionColumnVersion(i), txReader1.getPartitionColumnVersion(i));
                                    }
                                }
                            }

                            // Assert _cv contents.
                            path.trimTo(tableNameLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
                            try (ColumnVersionReader cvReader0 = tableReader.getColumnVersionReader()) {
                                try (ColumnVersionReader cvReader1 = new ColumnVersionReader().ofRO(ff, path)) {
                                    cvReader1.readSafe(configuration.getMillisecondClock(), configuration.getSpinLockTimeout());

                                    Assert.assertEquals(cvReader0.getVersion(), cvReader1.getVersion());
                                    TestUtils.assertEquals(cvReader0.getCachedList(), cvReader1.getCachedList());
                                }
                            }
                        }
                    }
                }

                compiler.compile("snapshot complete", sqlExecutionContext);
            }
        });
    }

    private void testSnapshotPrepareCheckTableMetadataFiles(String ddl, String ddl2, String tableName) throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path(); Path copyPath = new Path()) {
                path.of(configuration.getRoot());
                copyPath.of(configuration.getSnapshotRoot()).concat(configuration.getDbDirectory());

                compile(ddl, sqlExecutionContext);
                if (ddl2 != null) {
                    compile(ddl2, sqlExecutionContext);
                }

                compiler.compile("snapshot prepare", sqlExecutionContext);

                path.concat(tableName);
                int tableNameLen = path.length();
                copyPath.concat(tableName);
                int copyTableNameLen = copyPath.length();

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

                compiler.compile("snapshot complete", sqlExecutionContext);
            }
        });
    }

    private static class TestFilesFacadeImpl extends FilesFacadeImpl {

        boolean errorOnSync = false;

        @Override
        public int sync() {
            if (!errorOnSync) {
                return super.sync();
            }
            return -1;
        }
    }
}
