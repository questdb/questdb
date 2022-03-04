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
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

public class SnapshotTest extends AbstractGriffinTest {

    private static final TestFilesFacadeImpl testFilesFacade = new TestFilesFacadeImpl();
    private final Path path = new Path();
    private int rootLen;

    @BeforeClass
    public static void setUpStatic() {
        ff = testFilesFacade;
        AbstractGriffinTest.setUpStatic();
    }

    @Before
    public void setUp() {
        // sync() system call is not available on Windows, so we skip the whole test suite there.
        Assume.assumeTrue(Os.type != Os.WINDOWS);

        super.setUp();
        path.of(configuration.getSnapshotRoot()).slash();
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
    public void testSnapshotPrepare() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 'a'; i < 'f'; i++) {
                compile("create table " + i + " (ts timestamp, name symbol, val int)", sqlExecutionContext);
            }

            compiler.compile("snapshot prepare", sqlExecutionContext);
            compiler.compile("snapshot complete", sqlExecutionContext);
        });
    }

    @Test
    public void testSnapshotPrepareCheckTableMetadata() throws Exception {
        testSnapshotPrepareCheckTableMetadata(false, false);
    }

    @Test
    public void testSnapshotPrepareCheckTableMetadataWithColTops() throws Exception {
        testSnapshotPrepareCheckTableMetadata(true, false);
    }

    @Test
    public void testSnapshotPrepareCheckTableMetadataWithDroppedColumns() throws Exception {
        testSnapshotPrepareCheckTableMetadata(true, true);
    }

    @Test
    public void testSnapshotPrepareCheckTableMetadataWithColTopsAndDroppedColumns() throws Exception {
        testSnapshotPrepareCheckTableMetadata(true, true);
    }

    private void testSnapshotPrepareCheckTableMetadata(boolean generateColTops, boolean dropColumns) throws Exception {
        assertMemoryLeak(() -> {
            snapshotInstanceId = "foobar";

            try (Path path = new Path()) {
                path.of(configuration.getSnapshotRoot()).concat(configuration.getDbDirectory());

                String tableName = "t";
                compile("create table " + tableName + " as " +
                        "(select * from (select rnd_str(5,10,2) a, x b from long_sequence(20)))", sqlExecutionContext);
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

                        try (TableReaderMetadata metadata1 = new TableReaderMetadata(ff)) {
                            // Assert _meta contents.
                            path.concat(TableUtils.META_FILE_NAME).$();
                            metadata1.of(path, ColumnType.VERSION);

                            Assert.assertEquals(metadata0.getColumnCount(), metadata1.getColumnCount());
                            Assert.assertEquals(metadata0.getPartitionBy(), metadata1.getPartitionBy());
                            Assert.assertEquals(metadata0.getTimestampIndex(), metadata1.getTimestampIndex());
                            Assert.assertEquals(metadata0.getVersion(), metadata1.getVersion());
                            Assert.assertEquals(metadata0.getId(), metadata1.getId());
                            Assert.assertEquals(metadata0.getMaxUncommittedRows(), metadata1.getMaxUncommittedRows());
                            Assert.assertEquals(metadata0.getCommitLag(), metadata1.getCommitLag());
                            Assert.assertEquals(metadata0.getStructureVersion(), metadata1.getStructureVersion());

                            for (int i = 0, n = metadata0.getColumnCount(); i < n; i++) {
                                TableColumnMetadata columnMetadata0 = metadata0.getColumnQuick(i);
                                TableColumnMetadata columnMetadata1 = metadata0.getColumnQuick(i);
                                Assert.assertEquals(columnMetadata0.getName(), columnMetadata1.getName());
                                Assert.assertEquals(columnMetadata0.getType(), columnMetadata1.getType());
                                Assert.assertEquals(columnMetadata0.getHash(), columnMetadata1.getHash());
                                Assert.assertEquals(columnMetadata0.getIndexValueBlockCapacity(), columnMetadata1.getIndexValueBlockCapacity());
                                Assert.assertEquals(columnMetadata0.isIndexed(), columnMetadata1.isIndexed());
                                Assert.assertEquals(columnMetadata0.isSymbolTableStatic(), columnMetadata1.isSymbolTableStatic());
                            }

                            // Assert _txn contents.
                            path.trimTo(tableNameLen).concat(TableUtils.TXN_FILE_NAME).$();
                            try (TxReader txReader0 = tableReader.getTxFile()) {
                                try (TxReader txReader1 = new TxReader(ff).ofRO(path, metadata1.getPartitionBy())) {
                                    TableUtils.safeReadTxn(txReader1, configuration.getMicrosecondClock(), configuration.getSpinLockTimeoutUs());

                                    Assert.assertEquals(txReader0.getTxn(), txReader1.getTxn());
                                    Assert.assertEquals(txReader0.getTransientRowCount(), txReader1.getTransientRowCount());
                                    Assert.assertEquals(txReader0.getFixedRowCount(), txReader1.getFixedRowCount());
                                    Assert.assertEquals(txReader0.getMinTimestamp(), txReader1.getMinTimestamp());
                                    Assert.assertEquals(txReader0.getMaxTimestamp(), txReader1.getMaxTimestamp());
                                    Assert.assertEquals(txReader0.getStructureVersion(), txReader1.getStructureVersion());
                                    Assert.assertEquals(txReader0.getDataVersion(), txReader1.getDataVersion());
                                    Assert.assertEquals(txReader0.getPartitionTableVersion(), txReader1.getPartitionTableVersion());
                                    for (int i = 0; i < txReader0.getSymbolColumnCount(); i++) {
                                        Assert.assertEquals(txReader0.getSymbolValueCount(i), txReader1.getSymbolValueCount(i));
                                    }
                                    for (int i = 0; i < txReader0.getPartitionCount(); i++) {
                                        Assert.assertEquals(txReader0.getPartitionNameTxn(i), txReader1.getPartitionNameTxn(i));
                                        Assert.assertEquals(txReader0.getPartitionSize(i), txReader1.getPartitionSize(i));
                                        Assert.assertEquals(txReader0.getPartitionTimestamp(i), txReader1.getPartitionTimestamp(i));
                                        Assert.assertEquals(txReader0.getPartitionDataTxn(i), txReader1.getPartitionDataTxn(i));
                                    }
                                }
                            }

                            // Assert _cv contents.
                            path.trimTo(tableNameLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
                            try (ColumnVersionReader cvReader0 = tableReader.getColumnVersionReader()) {
                                try (ColumnVersionReader cvReader1 = new ColumnVersionReader().ofRO(ff, path)) {
                                    cvReader1.readSafe(configuration.getMicrosecondClock(), configuration.getSpinLockTimeoutUs());

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

    @Test
    public void testSnapshotPrepareCheckMetadataFileForDefaultInstanceId() throws Exception {
        testSnapshotPrepareCheckMetadataFile(null);
    }

    @Test
    public void testSnapshotPrepareCheckMetadataFileForNonDefaultInstanceId() throws Exception {
        testSnapshotPrepareCheckMetadataFile("foobar");
    }

    private void testSnapshotPrepareCheckMetadataFile(String snapshotId) throws Exception {
        assertMemoryLeak(() -> {
            snapshotInstanceId = snapshotId;

            try (Path path = new Path()) {
                compile("create table x as (select * from (select rnd_str(5,10,2) a, x b from long_sequence(20)))", sqlExecutionContext);
                compiler.compile("snapshot prepare", sqlExecutionContext);

                path.of(configuration.getSnapshotRoot());
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

    @Test
    public void testSnapshotPrepareCleansUpSnapshotDir() throws Exception {
        assertMemoryLeak(() -> {
            path.trimTo(rootLen);
            int rc = configuration.getFilesFacade().mkdirs(path.slash$(), configuration.getMkDirMode());
            Assert.assertEquals(0, rc);

            // Create a test file.
            path.trimTo(rootLen).concat("test.txt").$();
            Assert.assertTrue(Files.touch(path));

            compile("create table test (ts timestamp, name symbol, val int)", sqlExecutionContext);
            compiler.compile("snapshot prepare", sqlExecutionContext);

            // The test file should be deleted by SNAPSHOT PREPARE.
            Assert.assertFalse(configuration.getFilesFacade().exists(path));

            compiler.compile("snapshot complete", sqlExecutionContext);
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
    public void testSnapshotCompleteWithoutPrepareFails() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table test (ts timestamp, name symbol, val int)", sqlExecutionContext);
            try {
                compiler.compile("snapshot complete", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertTrue(ex.getMessage().startsWith("[0] SNAPSHOT PREPARE must be called before SNAPSHOT COMPLETE"));
            }
        });
    }

    @Test
    public void testSnapshotPrepareSubsequentCallFails() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table test (ts timestamp, name symbol, val int)", sqlExecutionContext);
            try {
                compiler.compile("snapshot prepare", sqlExecutionContext);
                compiler.compile("snapshot prepare", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertTrue(ex.getMessage().startsWith("[0] Another snapshot command in progress"));
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
    public void testSnapshotPrepareFailsOnCorruptedTable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "t";
            compile("create table " + tableName + " (ts timestamp, name symbol, val int)", sqlExecutionContext);

            // Corrupt the table by removing _txn file.
            Assert.assertTrue(configuration.getFilesFacade().remove(path.of(root).concat(tableName).concat(TableUtils.TXN_FILE_NAME).$()));

            try {
                compiler.compile("snapshot prepare", sqlExecutionContext);
                Assert.fail();
            } catch (CairoException ex) {
                Assert.assertTrue(ex.getMessage().contains("Cannot append. File does not exist"));
            }
        });
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
    public void testRecoverSnapshotForDefaultInstanceIds() throws Exception {
        testRecoverSnapshot(null, null, false);
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
            snapshotAgent.releaseReaders();
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
            snapshotAgent.releaseReaders();
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            snapshotInstanceId = restartedId;

            DatabaseSnapshotAgent.recoverSnapshot(engine);

            // Dropped column should be there.
            assertSql("select * from " + tableName, expectedAllColumns);
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
