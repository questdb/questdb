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
import org.junit.*;

public class SnapshotTest extends AbstractGriffinTest {

    private final Path path = new Path();
    private int rootLen;

    @Before
    public void setUp() {
        // sync() system call is not available on Windows, so we skip the whole test suite there.
        Assume.assumeTrue(Os.type != Os.WINDOWS);

        super.setUp();
        path.of(configuration.getSnapshotRoot()).slash();
        rootLen = path.length();
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
    public void testSnapshotCheckTableMetadata() throws Exception {
        assertMemoryLeak(() -> {
            snapshotInstanceId = "foobar";

            try (Path path = new Path()) {
                path.of(configuration.getSnapshotRoot()).slash();

                path.concat(configuration.getDbDirectory());

                String tableName = "t";
                String sql = "create table " + tableName + " as (select * from (select rnd_str(5,10,2) a, x b from long_sequence(20)))";
                compile(sql, sqlExecutionContext);

                compiler.compile("snapshot prepare", sqlExecutionContext);

                path.concat(tableName);
                int tableNameLen = path.length();
                FilesFacade ff = configuration.getFilesFacade();
                try (TableReader tableReader = new TableReader(configuration, "t")) {
                    try (TableReaderMetadata metadata0 = tableReader.getMetadata()) {

                        try (TableReaderMetadata metadata1 = new TableReaderMetadata(ff)) {
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

                            path.trimTo(tableNameLen);
                            path.concat(TableUtils.TXN_FILE_NAME).$();

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
                        }
                    }
                }

                compiler.compile("snapshot complete", sqlExecutionContext);
            }
        });
    }

    @Test
    public void testSnapshotCheckMetadataForDefaultInstanceId() throws Exception {
        testSnapshotCheckMetadata(null);
    }

    @Test
    public void testSnapshotCheckMetadataForNonDefaultInstanceId() throws Exception {
        testSnapshotCheckMetadata("foobar");
    }

    private void testSnapshotCheckMetadata(String expectedId) throws Exception {
        assertMemoryLeak(() -> {
            snapshotInstanceId = expectedId;

            try (Path path = new Path()) {
                String sql = "create table x as (select * from (select rnd_str(5,10,2) a, x b from long_sequence(20)))";
                compile(sql, sqlExecutionContext);
                compiler.compile("snapshot prepare", sqlExecutionContext);

                path.of(configuration.getSnapshotRoot());
                FilesFacade ff = configuration.getFilesFacade();
                try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                    mem.smallFile(ff, path.concat(TableUtils.SNAPSHOT_META_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);

                    CharSequence actualId = mem.getStr(0);
                    if (expectedId != null) {
                        Assert.assertTrue(Chars.equals(actualId, expectedId));
                    } else {
                        Assert.assertEquals(0, actualId.length());
                    }
                }

                compiler.compile("snapshot complete", sqlExecutionContext);
            }
        });
    }

    @Test
    public void testSnapshotDirExists() throws Exception {
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

            path.trimTo(rootLen);
            Assert.assertFalse(configuration.getFilesFacade().exists(path));
        });
    }

    @Test
    public void testSnapshotCompleteWithoutPrepare() throws Exception {
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
    public void testPrepareInFlight() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table test (ts timestamp, name symbol, val int)", sqlExecutionContext);
            try {
                compiler.compile("snapshot prepare", sqlExecutionContext);
                compiler.compile("snapshot prepare", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException ex) {
                Assert.assertTrue(ex.getMessage().startsWith("[0] Another snapshot command in progress"));
            } finally {
                // release locked readers
                compiler.compile("snapshot complete", sqlExecutionContext);
            }
        });
    }

    @Test
    public void testUnknownSubOption() throws Exception {
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
}