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
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;


public class AlterTableAttachPartitionFromSoftLinkTest extends AbstractAlterTableAttachPartitionTest {

    // some tests begin with: Assume.assumeTrue(Os.type != Os.WINDOWS);
    // in WINDOWS the user performing the tests needs to have the 'Create Symbolic Links' privilege. 
    // this privilege is not granted by default. in addition, if User Account Control (UAC) is on, and 
    // the user has administrator privileges, tests must 'Run as administrator'.

    private static final String expectedMaxTimestamp = "2022-10-18T23:59:59.000000Z";
    private static final String expectedMinTimestamp = "2022-10-17T00:00:17.279900Z";
    private static final long partitionTimestamp;
    private static final String readOnlyPartitionName = "2022-10-17";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        Assert.assertEquals(TableUtils.ATTACHABLE_DIR_MARKER, configuration.getAttachPartitionSuffix());
        Assert.assertFalse(configuration.attachPartitionCopy());
    }

    @Test
    public void testAddColumn() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, () -> {
                        try {
                            executeOperation("ALTER TABLE " + tableName + " ADD COLUMN ss SYMBOL", CompiledQuery.ALTER);
                            assertInsertFailsBecausePartitionIsReadOnly(
                                    "INSERT INTO " + tableName + " VALUES(666, 666, 'queso', '" + readOnlyPartitionName + "T23:59:59.999999Z', '¶')",
                                    tableName,
                                    readOnlyPartitionName);
                            executeOperation("ALTER TABLE " + tableName + " ALTER COLUMN ss ADD INDEX CAPACITY 32", CompiledQuery.ALTER);
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT -5",
                                    "l\ti\ts\tts\tss\n" +
                                            "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\t\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\t\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\t\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\t\n" +
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\t\n"
                            );
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                    }
            );
        });
    }

    @Test
    public void testDetachPartitionAttachedFromSoftLink() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            attachPartitionFromSoftLink(tableName, "SNOW", () -> {
                        try {
                            compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + readOnlyPartitionName + "'", sqlExecutionContext);
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-18T00:00:16.779900Z\t2022-10-18T23:59:59.000000Z\t5000\n");

                            // verify cold storage folder exists
                            Assert.assertTrue(Files.exists(other));
                            AtomicInteger fileCount = new AtomicInteger();
                            ff.walk(other, (file, type) -> fileCount.incrementAndGet());
                            Assert.assertTrue(fileCount.get() > 0);

                            // verify the link was removed
                            other.of(configuration.getRoot())
                                    .concat(tableName)
                                    .concat(readOnlyPartitionName)
                                    .put(configuration.getAttachPartitionSuffix())
                                    .$();
                            Assert.assertFalse(ff.exists(other));

                            // insert a row at the end of the partition, the only row, which will create the partition
                            // at this point there is no longer information as to weather it was read-only in the past
                            executeInsert("INSERT INTO " + tableName + " (l, i, ts) VALUES(0, 0, '" + readOnlyPartitionName + "T23:59:59.500001Z')");
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-17T23:59:59.500001Z\t2022-10-18T23:59:59.000000Z\t5001\n");

                            // drop the partition
                            compile("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + readOnlyPartitionName + "'", sqlExecutionContext);
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-18T00:00:16.779900Z\t2022-10-18T23:59:59.000000Z\t5000\n");
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                    }
            );
        });
    }

    @Test
    public void testDropIndex() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, () -> {
                        try {
                            executeOperation("ALTER TABLE " + tableName + " ALTER COLUMN s DROP INDEX", CompiledQuery.ALTER);
                            executeOperation("ALTER TABLE " + tableName + " ALTER COLUMN s ADD INDEX", CompiledQuery.ALTER);
                            assertInsertFailsBecausePartitionIsReadOnly(
                                    "INSERT INTO " + tableName + " VALUES(1492, 10, 'howdy', '" + readOnlyPartitionName + "T23:59:59.999999Z')",
                                    tableName,
                                    readOnlyPartitionName);
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT 5",
                                    "l\ti\ts\tts\n" +
                                            "1\t1\tCPSW\t2022-10-17T00:00:17.279900Z\n" +
                                            "2\t2\tHYRX\t2022-10-17T00:00:34.559800Z\n" +
                                            "3\t3\t\t2022-10-17T00:00:51.839700Z\n" +
                                            "4\t4\tVTJW\t2022-10-17T00:01:09.119600Z\n" +
                                            "5\t5\tPEHN\t2022-10-17T00:01:26.399500Z\n");
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                    }
            );
        });
    }

    @Test
    public void testDropPartition() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            attachPartitionFromSoftLink(tableName, "IGLOO", () -> {
                        try {
                            compile("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + readOnlyPartitionName + "'", sqlExecutionContext);
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-18T00:00:16.779900Z\t2022-10-18T23:59:59.000000Z\t5000\n");

                            // verify cold storage folder exists
                            Assert.assertTrue(Files.exists(other));
                            AtomicInteger fileCount = new AtomicInteger();
                            ff.walk(other, (file, type) -> fileCount.incrementAndGet());
                            Assert.assertTrue(fileCount.get() > 0);
                            path.of(configuration.getRoot())
                                    .concat(tableName)
                                    .concat(readOnlyPartitionName)
                                    .put(".2")
                                    .$();
                            Assert.assertFalse(ff.exists(path));
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                    }
            );
        });
    }

    @Test
    public void testDropPartitionWhileThereIsAReader() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            attachPartitionFromSoftLink(tableName, "FINLAND", () -> {
                        try {
                            try (TableReader ignore = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                                // drop the partition which was attached via soft link
                                compile("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + readOnlyPartitionName + "'", sqlExecutionContext);
                                // there is a reader, cannot unlink, thus the link will still exist
                                path.of(configuration.getRoot()) // <-- soft link path
                                        .concat(tableName)
                                        .concat(readOnlyPartitionName)
                                        .put(".2")
                                        .$();
                                Assert.assertTrue(Files.exists(path));
                            }
                            engine.releaseAllReaders();
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-18T00:00:16.779900Z\t2022-10-18T23:59:59.000000Z\t5000\n");

                            runO3PartitionPurgeJob();

                            // verify cold storage folder still exists
                            Assert.assertTrue(Files.exists(other));
                            AtomicInteger fileCount = new AtomicInteger();
                            ff.walk(other, (file, type) -> fileCount.incrementAndGet());
                            Assert.assertTrue(fileCount.get() > 0);
                            Assert.assertFalse(Files.exists(path));
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                    }
            );
        });
    }

    @Test
    public void testDropPartitionWhileThereIsAReaderWindows() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, () -> {
                        try {
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT -5",
                                    "l\ti\ts\tts\n" +
                                            "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n"
                            );

                            try (TableReader ignore = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                                compile("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + readOnlyPartitionName + "'", sqlExecutionContext);
                            }

                            runO3PartitionPurgeJob();

                            path.of(configuration.getRoot()).concat(tableName);
                            int plen = path.length();
                            // in windows if this was a real soft link to a folder, the link would be deleted
                            Assert.assertFalse(ff.exists(path.concat(readOnlyPartitionName).$()));
                            Assert.assertTrue(ff.exists(path.trimTo(plen).concat("2022-10-18").$()));
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                    }
            );
        });
    }

    @Test
    public void testDropPartitionWindows() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, () -> {
                        try {
                            compile("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + readOnlyPartitionName + "'", sqlExecutionContext);
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-18T00:00:16.779900Z\t2022-10-18T23:59:59.000000Z\t5000\n");
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT 5",
                                    "l\ti\ts\tts\n");
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                    }
            );
        });
    }

    @Test
    public void testInsertUpdate() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, () -> {
                        try {
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT -5",
                                    "l\ti\ts\tts\n" +
                                            "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n"
                            );

                            assertInsertFailsBecausePartitionIsReadOnly(
                                    "INSERT INTO " + tableName + " (l, i, s, ts) VALUES(0, 0, 'ø','" + readOnlyPartitionName + "T23:59:59.500001Z')",
                                    tableName,
                                    readOnlyPartitionName
                            );

                            assertUpdateFailsBecausePartitionIsReadOnly(
                                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '" + readOnlyPartitionName + "T23:59:42.220100Z'",
                                    tableName,
                                    readOnlyPartitionName
                            );

                            assertInsertFailsBecausePartitionIsReadOnly(
                                    "INSERT INTO " + tableName + " (l, i, s, ts) VALUES(-1, -1, 'µ','" + readOnlyPartitionName + "T00:00:00.100005Z')",
                                    tableName,
                                    readOnlyPartitionName
                            );

                            assertUpdateFailsBecausePartitionIsReadOnly(
                                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '2022-10-17T00:00:34.559800Z'",
                                    tableName,
                                    readOnlyPartitionName
                            );

                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT -5",
                                    "l\ti\ts\tts\n" +
                                            "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" + // <-- update was skipped, l would have been 13
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n"  // <-- no new row at the end
                            );
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT 5",
                                    "l\ti\ts\tts\n" +
                                            "1\t1\tCPSW\t2022-10-17T00:00:17.279900Z\n" +
                                            "2\t2\tHYRX\t2022-10-17T00:00:34.559800Z\n" + // <-- update was skipped, l would have been 13
                                            "3\t3\t\t2022-10-17T00:00:51.839700Z\n" +
                                            "4\t4\tVTJW\t2022-10-17T00:01:09.119600Z\n" +
                                            "5\t5\tPEHN\t2022-10-17T00:01:26.399500Z\n"
                            );
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                    }
            );
        });
    }

    @Test
    public void testPurgePartitions() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            String tableName = testName.getMethodName();
            String[] partitionName = {
                    "2022-10-17",
                    "2022-10-18",
                    "2022-10-19",
                    "2022-10-20",
                    "2022-10-21",
                    "2022-10-22",
            };
            int partitionCount = partitionName.length;
            String expectedMinTimestamp = "2022-10-17T00:00:51.839900Z";
            String expectedMaxTimestamp = "2022-10-22T23:59:59.000000Z";
            String otherLocation = "CON-CHIN-CHINA";
            int txn = 0;
            try (TableModel src = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(
                        1,
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 32)
                                .timestamp("ts"),
                        10000,
                        partitionName[0],
                        partitionCount
                );
            }
            txn++;
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            expectedMinTimestamp + "\t" + expectedMaxTimestamp + "\t10000\n");

            // detach all partitions but last two and them from soft link
            path.of(configuration.getRoot()).concat(tableName);
            int pathLen = path.length();
            for (int i = 0; i < partitionCount - 2; i++) {
                compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + partitionName[i] + "'", sqlExecutionContext);
                txn++;
                copyToDifferentLocationAndMakeAttachableViaSoftLink(tableName, partitionName[i], otherLocation);
                compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + partitionName[i] + "'", sqlExecutionContext);
                txn++;

                // verify that the link has been renamed to what we expect
                path.trimTo(pathLen).concat(partitionName[i]);
                TableUtils.txnPartitionConditionally(path, txn - 1);
                Assert.assertTrue(Files.exists(path.$()));
            }

            // verify read-only flag
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                TxReader txFile = reader.getTxFile();
                for (int i = 0; i < partitionCount - 2; i++) {
                    Assert.assertTrue(txFile.isPartitionReadOnly(i));
                }
                Assert.assertFalse(txFile.isPartitionReadOnly(partitionCount - 2));
                Assert.assertFalse(txFile.isPartitionReadOnly(partitionCount - 1));
            }

            // verify content
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            expectedMinTimestamp + "\t" + expectedMaxTimestamp + "\t10000\n");

            // create a reader, which will prevent partitions from being immediately purged
            try (TableReader ignore = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                // drop all partitions but the most recent
                for (int i = 0, expectedTxn = 2; i < partitionCount - 2; i++, expectedTxn += 2) {
                    compile("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + partitionName[i] + "'", sqlExecutionContext);
                    path.trimTo(pathLen).concat(partitionName[i]);
                    TableUtils.txnPartitionConditionally(path, expectedTxn);
                    Assert.assertTrue(Files.exists(path.$()));
                }
                compile("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + partitionName[partitionCount - 2] + "'", sqlExecutionContext);
                path.trimTo(pathLen).concat(partitionName[partitionCount - 2]);
                Assert.assertTrue(Files.exists(path.$()));
            }
            engine.releaseAllReaders();
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            "2022-10-22T00:00:33.726600Z\t2022-10-22T23:59:59.000000Z\t1667\n");

            runO3PartitionPurgeJob();

            // verify cold storage still exists
            other.of(new File(temp.getRoot(), otherLocation).getAbsolutePath()).concat(tableName);
            int otherLen = other.length();
            AtomicInteger fileCount = new AtomicInteger();
            for (int i = 0; i < partitionCount - 2; i++) {
                other.trimTo(otherLen).concat(partitionName[i]).put(TableUtils.DETACHED_DIR_MARKER).$();
                Assert.assertTrue(Files.exists(other));
                fileCount.set(0);
                ff.walk(other, (file, type) -> fileCount.incrementAndGet());
                Assert.assertTrue(fileCount.get() > 0);
            }

            // verify all partitions but last one are gone
            for (int i = 0; i < partitionCount - 1; i++) {
                path.trimTo(pathLen).concat(partitionName[i]).$();
                Assert.assertFalse(Files.exists(path));
            }
        });
    }

    @Test
    public void testRemoveColumn() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            attachPartitionFromSoftLink(tableName, "REFRIGERATOR", () -> {
                        try {
                            executeOperation("ALTER TABLE " + tableName + " DROP COLUMN s", CompiledQuery.ALTER);
                            assertInsertFailsBecausePartitionIsReadOnly(
                                    "INSERT INTO " + tableName + " VALUES(666, 666, '" + readOnlyPartitionName + "T23:59:59.999999Z')",
                                    tableName,
                                    readOnlyPartitionName
                            );
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT -5",
                                    "l\ti\tts\n" +
                                            "4996\t4996\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\t2022-10-17T23:59:59.500000Z\n"
                            );
                        } catch (SqlException e) {
                            throw new RuntimeException(e);
                        }

                        // check that the column files still exist within the partition folder (attached from soft link)
                        final int pathLen = path.length();
                        Assert.assertTrue(ff.exists(path.trimTo(pathLen).concat("s.d").$()));
                        Assert.assertTrue(ff.exists(path.trimTo(pathLen).concat("s.k").$()));
                        Assert.assertTrue(ff.exists(path.trimTo(pathLen).concat("s.v").$()));

                        engine.releaseAllReaders();
                        engine.releaseAllWriters();
                        try (
                                ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine, null);
                                TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)
                        ) {
                            TxReader txReader = reader.getTxFile();
                            Assert.assertTrue(txReader.unsafeLoadAll());
                            Assert.assertTrue(txReader.isPartitionReadOnlyByPartitionTimestamp(txReader.getPartitionTimestamp(0)));
                            Assert.assertFalse(txReader.isPartitionReadOnlyByPartitionTimestamp(txReader.getPartitionTimestamp(1)));
                            if (Os.isWindows()) {
                                engine.releaseInactive();
                            }
                            purgeJob.run(0);
                        } catch (SqlException unexpected) {
                            Assert.fail(unexpected.getMessage());
                        }

                        // check that the column files still exist within the partition folder (attached from soft link)
                        Assert.assertTrue(ff.exists(path.trimTo(pathLen).concat("s.d").$()));
                        Assert.assertTrue(ff.exists(path.trimTo(pathLen).concat("s.k").$()));
                        Assert.assertTrue(ff.exists(path.trimTo(pathLen).concat("s.v").$()));
                    }
            );
        });
    }

    @Test
    public void testRemoveColumnWindows() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, () -> {
                        try {
                            executeOperation("ALTER TABLE " + tableName + " DROP COLUMN s", CompiledQuery.ALTER);
                            assertInsertFailsBecausePartitionIsReadOnly(
                                    "INSERT INTO " + tableName + " VALUES(666, 666, '" + readOnlyPartitionName + "T23:59:59.999999Z')",
                                    tableName,
                                    readOnlyPartitionName
                            );
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT -5",
                                    "l\ti\tts\n" +
                                            "4996\t4996\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\t2022-10-17T23:59:59.500000Z\n"
                            );
                        } catch (SqlException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );
        });
    }

    @Test
    public void testRenameColumn() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, () -> {
                        try {
                            executeOperation("ALTER TABLE " + tableName + " RENAME COLUMN s TO ss", CompiledQuery.ALTER);
                            executeOperation("ALTER TABLE " + tableName + " ALTER COLUMN ss DROP INDEX", CompiledQuery.ALTER);
                            assertInsertFailsBecausePartitionIsReadOnly(
                                    "INSERT INTO " + tableName + " VALUES(666, 666, 'queso', '" + readOnlyPartitionName + "T23:59:59.999999Z')",
                                    tableName,
                                    readOnlyPartitionName
                            );
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT -5",
                                    "l\ti\tss\tts\n" +
                                            "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n"
                            );
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                    }
            );
        });
    }

    @Test
    public void testTruncateTable() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            attachPartitionFromSoftLink(tableName, "FRIO_DEL_15", () -> {
                        try {
                            compile("TRUNCATE TABLE " + tableName, sqlExecutionContext);
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "\t\t0\n");

                            // verify cold storage folder exists
                            Assert.assertTrue(Files.exists(other));
                            AtomicInteger fileCount = new AtomicInteger();
                            ff.walk(other, (file, type) -> fileCount.incrementAndGet());
                            Assert.assertTrue(fileCount.get() > 0);
                            path.of(configuration.getRoot())
                                    .concat(tableName)
                                    .concat(readOnlyPartitionName)
                                    .put(".2")
                                    .$();
                            Assert.assertFalse(ff.exists(path));
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                    }
            );
        });
    }

    @Test
    public void testTruncateTableWindows() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, () -> {
                        try {
                            compile("TRUNCATE TABLE " + tableName, sqlExecutionContext);
                            path.of(configuration.getRoot()).concat(tableName);
                            int plen = path.length();
                            Assert.assertFalse(ff.exists(path.concat(readOnlyPartitionName).$()));
                            Assert.assertFalse(ff.exists(path.trimTo(plen).concat("2022-10-18").$()));
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                    }
            );
        });
    }

    @Test
    public void testUpdate() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            attachPartitionFromSoftLink(tableName, "LEGEND", () -> {
                        try {
                            assertUpdateFailsBecausePartitionIsReadOnly(
                                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '" + readOnlyPartitionName + "T00:00:17.279900Z'",
                                    tableName,
                                    readOnlyPartitionName
                            );
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT -5",
                                    "l\ti\ts\tts\n" +
                                            "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n"
                            );
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT 5",
                                    "l\ti\ts\tts\n" +
                                            "1\t1\tCPSW\t2022-10-17T00:00:17.279900Z\n" +
                                            "2\t2\tHYRX\t2022-10-17T00:00:34.559800Z\n" +
                                            "3\t3\t\t2022-10-17T00:00:51.839700Z\n" +
                                            "4\t4\tVTJW\t2022-10-17T00:01:09.119600Z\n" +
                                            "5\t5\tPEHN\t2022-10-17T00:01:26.399500Z\n"
                            );
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                    }
            );
        });
    }

    @Test
    public void testUpdateWindows() throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            createTableWithReadOnlyPartition(tableName, () -> {
                        try {
                            assertUpdateFailsBecausePartitionIsReadOnly(
                                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '" + readOnlyPartitionName + "T00:00:17.279900Z'",
                                    tableName,
                                    readOnlyPartitionName
                            );
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + readOnlyPartitionName + "' LIMIT 5",
                                    "l\ti\ts\tts\n" +
                                            "1\t1\tCPSW\t2022-10-17T00:00:17.279900Z\n" + // update is skipped, l would have been 13
                                            "2\t2\tHYRX\t2022-10-17T00:00:34.559800Z\n" +
                                            "3\t3\t\t2022-10-17T00:00:51.839700Z\n" +
                                            "4\t4\tVTJW\t2022-10-17T00:01:09.119600Z\n" +
                                            "5\t5\tPEHN\t2022-10-17T00:01:26.399500Z\n"
                            );
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                    }
            );
        });
    }

    private static void runO3PartitionPurgeJob() {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        try (O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine.getMessageBus(), 1)) {
            while (purgeJob.run(0)) {
                Os.pause();
            }
        }
    }

    private void assertInsertFailsBecausePartitionIsReadOnly(String insertStmt, String tableName, String partitionName) {
        try {
            executeInsert(insertStmt);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(
                    "cannot insert into read-only partition [table=" + tableName + ", partitionTs=" + partitionName + "T00:00:00.000Z]",
                    e.getFlyweightMessage());
        } catch (SqlException e) {
            Assert.fail("not expecting any SqlExceptions: " + e.getFlyweightMessage());
        }
    }

    private void assertUpdateFailsBecausePartitionIsReadOnly(String updateStmt, String tableName, String partitionName) {
        try {
            executeOperation(updateStmt, CompiledQuery.UPDATE);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(
                    "cannot update read-only partition [table=" + tableName + ", partitionIndex=0, partitionTs=" + partitionName + "T00:00:00.000Z]",
                    e.getFlyweightMessage());
        } catch (SqlException e) {
            Assert.fail("not expecting any SqlExceptions: " + e.getFlyweightMessage());
        }
    }

    private void attachPartitionFromSoftLink(String tableName, String otherLocation, Runnable test) throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            createTable(tableName);
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            expectedMinTimestamp + "\t" + expectedMaxTimestamp + "\t10000\n");

            // detach partition and attach it from soft link
            compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + readOnlyPartitionName + "'", sqlExecutionContext);
            copyToDifferentLocationAndMakeAttachableViaSoftLink(tableName, readOnlyPartitionName, otherLocation);
            compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + readOnlyPartitionName + "'", sqlExecutionContext);

            // verify that the link has been renamed to what we expect
            path.of(configuration.getRoot()).concat(tableName).concat(readOnlyPartitionName);
            TableUtils.txnPartitionConditionally(path, 2);
            Assert.assertTrue(Files.exists(path.$()));

            // verify RO flag
            engine.releaseAllReaders();
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                TxReader txFile = reader.getTxFile();
                Assert.assertNotNull(txFile);
                Assert.assertTrue(txFile.isPartitionReadOnly(0));
                Assert.assertFalse(txFile.isPartitionReadOnly(1));
            }
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            expectedMinTimestamp + "\t" + expectedMaxTimestamp + "\t10000\n");
            test.run();
        });
    }

    private void copyToDifferentLocationAndMakeAttachableViaSoftLink(
            String tableName,
            CharSequence partitionName,
            String otherLocation
    ) {
        engine.releaseAllReaders();
        engine.releaseAllWriters();

        // copy .detached folder to the different location
        CharSequence tmp;
        try {
            tmp = temp.newFolder(otherLocation).getAbsolutePath();
        } catch (IOException e) {
            tmp = new File(temp.getRoot(), otherLocation).getAbsolutePath();
        }
        final CharSequence s3Buckets = tmp;
        final String detachedPartitionName = partitionName + TableUtils.DETACHED_DIR_MARKER;
        copyPartitionAndMetadata( // this creates s3Buckets
                configuration.getRoot(),
                tableName,
                detachedPartitionName,
                s3Buckets,
                tableName,
                detachedPartitionName,
                null
        );

        // create the .attachable link in the table's data folder
        // with target the .detached folder in the different location
        other.of(s3Buckets)
                .concat(tableName)
                .concat(detachedPartitionName)
                .$();
        path.of(configuration.getRoot()) // <-- soft link path
                .concat(tableName)
                .concat(partitionName)
                .put(configuration.getAttachPartitionSuffix())
                .$();
        Assert.assertEquals(0, ff.softLink(other, path));
    }

    private void createTable(String tableName) throws NumericException, SqlException {
        try (TableModel src = new TableModel(configuration, tableName, PartitionBy.DAY)) {
            createPopulateTable(
                    1,
                    src.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .col("s", ColumnType.SYMBOL).indexed(true, 32)
                            .timestamp("ts"),
                    10000,
                    "2022-10-17",
                    2
            );
        }
    }

    private void createTableWithReadOnlyPartition(String tableName, Runnable test) throws Exception {
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            createTable(tableName);
            // the read-only flag is only set when a partition is attached from soft link
            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "read-only-flag")) {
                TxWriter txWriter = writer.getTxWriter();
                txWriter.setPartitionReadOnlyByTimestamp(partitionTimestamp, true);
                txWriter.bumpTruncateVersion();
                txWriter.commit(configuration.getCommitMode(), writer.getDenseSymbolMapWriters());
            }
            engine.releaseAllReaders();
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                TxReader txFile = reader.getTxFile();
                Assert.assertTrue(txFile.isPartitionReadOnlyByPartitionTimestamp(partitionTimestamp));
                Assert.assertTrue(txFile.isPartitionReadOnly(0));
                Assert.assertFalse(txFile.isPartitionReadOnly(1));
            }
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            expectedMinTimestamp + "\t" + expectedMaxTimestamp + "\t10000\n");
            test.run();
        });
    }

    static {
        try {
            partitionTimestamp = TimestampFormatUtils.parseTimestamp(readOnlyPartitionName + "T00:00:00.000Z");
        } catch (NumericException impossible) {
            throw new RuntimeException(impossible);
        }
    }
}

