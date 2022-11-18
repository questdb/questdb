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
import io.questdb.tasks.ColumnPurgeTask;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;


public class AlterTableAttachPartitionFromSoftLinkTest extends AlterTableAttachPartitionBase {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        Assert.assertEquals(TableUtils.ATTACHABLE_DIR_MARKER, configuration.getAttachPartitionSuffix());
        Assert.assertFalse(configuration.attachPartitionCopy());
    }

    @Test
    public void testAddColumn() throws Exception {

        // TODO: this test, as all others that assume the same, need revisiting in WINDOWS
        //  as in WINDOWS CI needs to run in administrator mode, and then possibly a few
        //  release readers/writers might be needed to release file handles
        Assume.assumeTrue(Os.type != Os.WINDOWS);

        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            final String partitionName = "2022-10-17";
            attachPartitionFromSoftLink(
                    tableName,
                    partitionName,
                    2,
                    "2022-10-17T00:00:17.279900Z",
                    "2022-10-18T23:59:59.000000Z",
                    "TUNGSTEN",
                    txn -> {
                        try {
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT -5",
                                    "l\ti\ts\tts\n" +
                                            "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n"
                            );

                            // add column ss of type symbol
                            executeOperation("ALTER TABLE " + tableName + " ADD COLUMN ss SYMBOL", CompiledQuery.ALTER);

                            // insert a row at the end of the partition
                            executeInsert("INSERT INTO " + tableName + " VALUES(666, 666, 'queso', '" + partitionName + "T23:59:59.999999Z', '¶')");

                            // add index on symbol column ss
                            executeOperation("ALTER TABLE " + tableName + " ALTER COLUMN ss ADD INDEX CAPACITY 32", CompiledQuery.ALTER);

                            // check content
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10001\n");
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT -5",
                                    "l\ti\ts\tts\tss\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\t\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\t\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\t\n" +
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\t\n" +
                                            "666\t666\tqueso\t2022-10-17T23:59:59.999999Z\t¶\n"
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
            final String partitionName = "2022-10-17";
            attachPartitionFromSoftLink(
                    tableName,
                    partitionName,
                    2,
                    "2022-10-17T00:00:17.279900Z",
                    "2022-10-18T23:59:59.000000Z",
                    "SNOW",
                    txn -> {
                        try {
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT -5",
                                    "l\ti\ts\tts\n" +
                                            "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n"
                            );

                            // detach the partition which was attached from soft link,
                            // will result in the link being removed
                            compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
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
                                    .concat(partitionName)
                                    .put(configuration.getAttachPartitionSuffix())
                                    .$();
                            Assert.assertFalse(ff.exists(other));

                            // verify no copy was produced to hot space
                            Assert.assertFalse(configuration.attachPartitionCopy());
                            path.of(configuration.getRoot())
                                    .concat(tableName)
                                    .concat(partitionName)
                                    .put(TableUtils.DETACHED_DIR_MARKER)
                                    .$();
                            Assert.assertFalse(ff.exists(path));

                            // insert a row at the end of the partition, the only row, which will create the partition
                            executeInsert("INSERT INTO " + tableName + " (l, i, ts) VALUES(0, 0, '" + partitionName + "T23:59:59.500001Z')");
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-17T23:59:59.500001Z\t2022-10-18T23:59:59.000000Z\t5001\n");

                            // drop the partition
                            compile("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
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
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            final String partitionName = "2022-10-17";
            attachPartitionFromSoftLink(
                    tableName,
                    partitionName,
                    2,
                    "2022-10-17T00:00:17.279900Z",
                    "2022-10-18T23:59:59.000000Z",
                    "BILBAO",
                    txn -> {
                        try {
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT -5",
                                    "l\ti\ts\tts\n" +
                                            "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n"
                            );

                            // drop the index on symbol column s
                            executeOperation(
                                    "ALTER TABLE " + tableName + " ALTER COLUMN s DROP INDEX",
                                    CompiledQuery.ALTER
                            );

                            // add index on symbol column s
                            executeOperation(
                                    "ALTER TABLE " + tableName + " ALTER COLUMN s ADD INDEX",
                                    CompiledQuery.ALTER
                            );
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT 5",
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
    public void testDropPartition() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            final String partitionName = "2022-10-17";
            attachPartitionFromSoftLink(
                    tableName,
                    partitionName,
                    2,
                    "2022-10-17T00:00:17.279900Z",
                    "2022-10-18T23:59:59.000000Z",
                    "IGLOO",
                    txn -> {
                        try {
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT -5",
                                    "l\ti\ts\tts\n" +
                                            "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n"
                            );

                            // drop the partition which was attached from soft link
                            compile("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
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
                                    .concat(partitionName)
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
            final String partitionName = "2022-10-17";
            attachPartitionFromSoftLink(
                    tableName,
                    partitionName,
                    2,
                    "2022-10-17T00:00:17.279900Z",
                    "2022-10-18T23:59:59.000000Z",
                    "FINLAND",
                    txn -> {
                        try {
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT -5",
                                    "l\ti\ts\tts\n" +
                                            "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n"
                            );

                            try (TableReader ignore = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                                // drop the partition which was attached via soft link
                                compile("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
                                // there is a reader, cannot unlink, thus the link will still exist
                                path.of(configuration.getRoot()) // <-- soft link path
                                        .concat(tableName)
                                        .concat(partitionName)
                                        .put(".2")
                                        .$();
                                Assert.assertTrue(Files.exists(path));
                            }
                            engine.releaseAllReaders();
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-18T00:00:16.779900Z\t2022-10-18T23:59:59.000000Z\t5000\n");

                            // purge old partitions
                            engine.releaseAllReaders();
                            engine.releaseAllWriters();
                            engine.releaseInactive();
                            try (O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine.getMessageBus(), 1)) {
                                while (purgeJob.run(0)) {
                                    Os.pause();
                                }
                            }

                            // verify cold storage folder still exists
                            Assert.assertTrue(Files.exists(other));
                            AtomicInteger fileCount = new AtomicInteger();
                            ff.walk(other, (file, type) -> fileCount.incrementAndGet());
                            Assert.assertTrue(fileCount.get() > 0);
                            Assert.assertFalse(Files.exists(path));
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        } catch (IOException ex) {
                            Assert.fail(ex.getMessage());
                        }
                    }
            );
        });
    }

    @Test
    public void testInsertUpdate() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            final String partitionName = "2022-10-17";
            attachPartitionFromSoftLink(
                    tableName,
                    partitionName,
                    2,
                    "2022-10-17T00:00:17.279900Z",
                    "2022-10-18T23:59:59.000000Z",
                    "S3",
                    txn -> {
                        try {
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT -5",
                                    "l\ti\ts\tts\n" +
                                            "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n"
                            );

                            // insert a row at the end of the partition
                            executeInsert("INSERT INTO " + tableName + " (l, i, s, ts) VALUES(0, 0, 'ø','" + partitionName + "T23:59:59.500001Z')");
                            txn++;

                            // update a row toward the end of the partition
                            // takes no effect, no commit
                            executeOperation(
                                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '" + partitionName + "T23:59:42.220100Z'",
                                    CompiledQuery.UPDATE
                            );

                            // insert a row at the beginning of the partition, this will result in the original folder being
                            // copied across to table data space (hot), with a new txn, the original folder's content are NOT
                            // removed, the link is.
                            executeInsert("INSERT INTO " + tableName + " (l, i, s, ts) VALUES(-1, -1, 'µ','" + partitionName + "T00:00:00.100005Z')");
                            txn++;

                            // verify that the link does not exist
                            // in windows the handle is held and cannot be deleted
                            Assert.assertFalse(Files.exists(path));

                            // verify that a new partition folder has been created in the table data space (hot)
                            path.of(configuration.getRoot()).concat(tableName).concat(partitionName);
                            TableUtils.txnPartitionConditionally(path, txn - 1);
                            Assert.assertTrue(Files.exists(path.$()));

                            // verify cold storage folder exists
                            Assert.assertTrue(Files.exists(other));
                            AtomicInteger fileCount = new AtomicInteger();
                            ff.walk(other, (file, type) -> fileCount.incrementAndGet());
                            Assert.assertTrue(fileCount.get() > 0);

                            // update a row toward the beginning of the partition
                            executeOperation(
                                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '2022-10-17T00:00:34.559800Z'",
                                    CompiledQuery.UPDATE
                            );

                            // verify content
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT -5",
                                    "l\ti\ts\tts\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" + // <-- update was skipped, l would have been 13
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n" +
                                            "0\t0\tø\t2022-10-17T23:59:59.500001Z\n" // <-- the new row at the end
                            );
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT 5",
                                    "l\ti\ts\tts\n" +
                                            "-1\t-1\tµ\t2022-10-17T00:00:00.100005Z\n" +
                                            "1\t1\tCPSW\t2022-10-17T00:00:17.279900Z\n" +
                                            "2\t2\tHYRX\t2022-10-17T00:00:34.559800Z\n" + // <-- update was skipped, l would have been 13
                                            "3\t3\t\t2022-10-17T00:00:51.839700Z\n" +
                                            "4\t4\tVTJW\t2022-10-17T00:01:09.119600Z\n"
                            );
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-17T00:00:00.100005Z\t2022-10-18T23:59:59.000000Z\t10002\n");
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
                // note that the default value for server.conf
                path.trimTo(pathLen).concat(partitionName[i]);
                TableUtils.txnPartitionConditionally(path, txn - 1);
                Assert.assertTrue(Files.exists(path.$()));
            }

            // verify RO flag
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                TxReader txFile = reader.getTxFile();
                for (int i = 0; i < partitionCount - 2; i++) {
                    Assert.assertTrue(txFile.getPartitionIsRO(i));
                }
                Assert.assertFalse(txFile.getPartitionIsRO(partitionCount - 2));
                Assert.assertFalse(txFile.getPartitionIsRO(partitionCount - 1));
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

            // purge partitions
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            engine.releaseInactive();
            try (O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine.getMessageBus(), 1)) {
                while (purgeJob.run(0)) {
                    Os.pause();
                }
            }

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
            final String partitionName = "2022-10-17";
            attachPartitionFromSoftLink(
                    tableName,
                    partitionName,
                    2,
                    "2022-10-17T00:00:17.279900Z",
                    "2022-10-18T23:59:59.000000Z",
                    "REFRIGERATOR",
                    txn -> {
                        try {
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT -5",
                                    "l\ti\ts\tts\n" +
                                            "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n"
                            );

                            // remove column s of type symbol
                            executeOperation(
                                    "ALTER TABLE " + tableName + " DROP COLUMN s",
                                    CompiledQuery.ALTER
                            );

                            // insert a row at the end of the partition
                            executeInsert("INSERT INTO " + tableName + " VALUES(666, 666, '" + partitionName + "T23:59:59.999999Z')");

                            // verify content
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10001\n");
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT -5",
                                    "l\ti\tts\n" +
                                            "4997\t4997\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\t2022-10-17T23:59:59.500000Z\n" +
                                            "666\t666\t2022-10-17T23:59:59.999999Z\n"
                            );
                        } catch (SqlException e) {
                            throw new RuntimeException(e);
                        }

                        // check that the column files still exist within the partition folder (attached from soft link)
                        final int pathLen = path.length();
                        Assert.assertTrue(ff.exists(path.trimTo(pathLen).concat("s.d").$()));
                        Assert.assertTrue(ff.exists(path.trimTo(pathLen).concat("s.k").$()));
                        Assert.assertTrue(ff.exists(path.trimTo(pathLen).concat("s.v").$()));

                        // invoke colum purge task
                        try (
                                ColumnPurgeOperator purgeOperator = new ColumnPurgeOperator(configuration);
                                TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)
                        ) {
                            TxReader txReader = reader.getTxFile();
                            Assert.assertTrue(txReader.getPartitionIsROByPartitionTimestamp(txReader.getPartitionTimestamp(0)));
                            Assert.assertFalse(txReader.getPartitionIsROByPartitionTimestamp(txReader.getPartitionTimestamp(1)));

                            ColumnPurgeTask purgeTask = new ColumnPurgeTask();
                            LongList updatedColumnInfo = new LongList(2 * TableUtils.LONGS_PER_TX_ATTACHED_PARTITION);
                            updatedColumnInfo.setPos(2 * TableUtils.LONGS_PER_TX_ATTACHED_PARTITION);
                            updatedColumnInfo.setQuick(ColumnPurgeTask.OFFSET_COLUMN_VERSION, txReader.getPartitionColumnVersion(0));
                            updatedColumnInfo.setQuick(ColumnPurgeTask.OFFSET_PARTITION_TIMESTAMP, txReader.getPartitionTimestamp(0));
                            updatedColumnInfo.setQuick(ColumnPurgeTask.OFFSET_PARTITION_NAME_TXN, txReader.getPartitionNameTxn(0));
                            updatedColumnInfo.setQuick(ColumnPurgeTask.OFFSET_UPDATE_ROW_ID, 314159L);
                            updatedColumnInfo.setQuick(ColumnPurgeTask.BLOCK_SIZE + ColumnPurgeTask.OFFSET_COLUMN_VERSION, txReader.getPartitionColumnVersion(1));
                            updatedColumnInfo.setQuick(ColumnPurgeTask.BLOCK_SIZE + ColumnPurgeTask.OFFSET_PARTITION_TIMESTAMP, txReader.getPartitionTimestamp(1));
                            updatedColumnInfo.setQuick(ColumnPurgeTask.BLOCK_SIZE + ColumnPurgeTask.OFFSET_PARTITION_NAME_TXN, txReader.getPartitionNameTxn(1));
                            updatedColumnInfo.setQuick(ColumnPurgeTask.BLOCK_SIZE + ColumnPurgeTask.OFFSET_UPDATE_ROW_ID, 628218L);
                            purgeTask.of(tableName, "s", 1, 0, ColumnType.SYMBOL, PartitionBy.DAY, 1, updatedColumnInfo);
                            purgeOperator.purgeExternal(purgeTask, txReader);
                            LongList purgedRowIds = purgeOperator.getCompletedRowIds();
                            Assert.assertEquals(0, purgedRowIds.binarySearch(314159L, BinarySearch.SCAN_UP));
                            Assert.assertEquals(1, purgedRowIds.binarySearch(628218L, BinarySearch.SCAN_UP));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
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
    public void testRenameColumn() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {
            final String tableName = testName.getMethodName();
            final String partitionName = "2022-10-17";
            attachPartitionFromSoftLink(
                    tableName,
                    partitionName,
                    2,
                    "2022-10-17T00:00:17.279900Z",
                    "2022-10-18T23:59:59.000000Z",
                    "ESPAÑA",
                    txn -> {
                        try {
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT -5",
                                    "l\ti\ts\tts\n" +
                                            "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n"
                            );

                            // add column ss of type symbol
                            executeOperation(
                                    "ALTER TABLE " + tableName + " RENAME COLUMN s TO ss",
                                    CompiledQuery.ALTER
                            );

                            // drop index on symbol column ss
                            executeOperation(
                                    "ALTER TABLE " + tableName + " ALTER COLUMN ss DROP INDEX",
                                    CompiledQuery.ALTER
                            );

                            // insert a row at the end of the partition
                            executeInsert("INSERT INTO " + tableName + " VALUES(666, 666, 'queso', '" + partitionName + "T23:59:59.999999Z')");

                            // verify content
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10001\n");
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT -5",
                                    "l\ti\tss\tts\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n" +
                                            "666\t666\tqueso\t2022-10-17T23:59:59.999999Z\n"
                            );
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
            final String partitionName = "2022-10-17";
            attachPartitionFromSoftLink(
                    tableName,
                    partitionName,
                    2,
                    "2022-10-17T00:00:17.279900Z",
                    "2022-10-18T23:59:59.000000Z",
                    "LEGEND",
                    txn -> {
                        try {
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT -5",
                                    "l\ti\ts\tts\n" +
                                            "4996\t4996\tVTJW\t2022-10-17T23:58:50.380400Z\n" +
                                            "4997\t4997\tCPSW\t2022-10-17T23:59:07.660300Z\n" +
                                            "4998\t4998\tHYRX\t2022-10-17T23:59:24.940200Z\n" +
                                            "4999\t4999\tHYRX\t2022-10-17T23:59:42.220100Z\n" +
                                            "5000\t5000\tCPSW\t2022-10-17T23:59:59.500000Z\n"
                            );

                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT 5",
                                    "l\ti\ts\tts\n" +
                                            "1\t1\tCPSW\t2022-10-17T00:00:17.279900Z\n" +
                                            "2\t2\tHYRX\t2022-10-17T00:00:34.559800Z\n" +
                                            "3\t3\t\t2022-10-17T00:00:51.839700Z\n" +
                                            "4\t4\tVTJW\t2022-10-17T00:01:09.119600Z\n" +
                                            "5\t5\tPEHN\t2022-10-17T00:01:26.399500Z\n"
                            );

                            // collect last modified timestamps for files in cold storage
                            final Map<String, Long> lastModified = new HashMap<>();
                            path.of(other);
                            final int len = path.length();
                            ff.walk(other, (file, type) -> {
                                path.trimTo(len).concat(file).$();
                                lastModified.put(path.toString(), ff.getLastModified(path));
                            });

                            // execute an update directly on the cold storage partition
                            // it will not fail, but it will not take effect either, and
                            // a line will be logged:
                            // skipping RO partition [partitionIndex=0, rowPartitionTs=2022-10-17T00:00:00.000000Z]
                            executeOperation(
                                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '" + partitionName + "T00:00:17.279900Z'",
                                    CompiledQuery.UPDATE
                            );
                            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                                    "min\tmax\tcount\n" +
                                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");
                            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT 5",
                                    "l\ti\ts\tts\n" +
                                            "1\t1\tCPSW\t2022-10-17T00:00:17.279900Z\n" + // update is skipped, l would have been 13
                                            "2\t2\tHYRX\t2022-10-17T00:00:34.559800Z\n" +
                                            "3\t3\t\t2022-10-17T00:00:51.839700Z\n" +
                                            "4\t4\tVTJW\t2022-10-17T00:01:09.119600Z\n" +
                                            "5\t5\tPEHN\t2022-10-17T00:01:26.399500Z\n"
                            );

                            // verify that no new partition folder has been created in the table data space (hot)
                            // but rather the files in cold storage have been modified
                            path.of(configuration.getRoot())
                                    .concat(tableName)
                                    .concat(partitionName);
                            TableUtils.txnPartitionConditionally(path, txn - 1);
                            Assert.assertTrue(Files.exists(path.$()));
                            Assert.assertTrue(Files.isSoftLink(path));
                            // in windows, detecting a soft link is tricky, and unnecessary. Removing
                            // a soft link does not remove the target's content, so we do not need to
                            // call unlink, thus we do not need isSoftLink. It has been implemented however
                            // and it does not seem to work.
                            path.of(other);
                            ff.walk(other, (file, type) -> {
                                path.trimTo(len).concat(file).$();
                                Long lm = lastModified.get(path.toString());
                                Assert.assertTrue(lm == null || lm == ff.getLastModified(path));
                            });
                        } catch (SqlException ex) {
                            Assert.fail(ex.getMessage());
                        }
                    }
            );
        });
    }

    private void attachPartitionFromSoftLink(
            String tableName,
            String partitionName,
            int partitionCount,
            String expectedMinTimestamp,
            String expectedMaxTimestamp,
            String otherLocation,
            Consumer<Integer> test
    ) throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {

            int txn = 0;
            try (TableModel src = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(
                        1,
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 32)
                                .timestamp("ts"),
                        10000,
                        partitionName,
                        partitionCount
                );
            }
            txn++;
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            expectedMinTimestamp + "\t" + expectedMaxTimestamp + "\t10000\n");

            // detach partition and attach it from soft link
            compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
            txn++;
            copyToDifferentLocationAndMakeAttachableViaSoftLink(tableName, partitionName, otherLocation);
            compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
            txn++;

            // verify that the link has been renamed to what we expect
            // note that the default value for server.conf
            path.of(configuration.getRoot()).concat(tableName).concat(partitionName);
            TableUtils.txnPartitionConditionally(path, txn - 1);
            Assert.assertTrue(Files.exists(path.$()));

            // verify RO flag
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                TxReader txFile = reader.getTxFile();
                Assert.assertTrue(txFile.getPartitionIsRO(0));
                Assert.assertFalse(txFile.getPartitionIsRO(1));
            }

            // verify content
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            expectedMinTimestamp + "\t" + expectedMaxTimestamp + "\t10000\n");
            // handover to actual test
            test.accept(txn);
        });
    }

    private void copyToDifferentLocationAndMakeAttachableViaSoftLink(String tableName, CharSequence partitionName, String otherLocation) throws IOException {
        engine.releaseAllReaders();
        engine.releaseAllWriters();

        // copy .detached folder to the different location
        // then remove the original .detached folder

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
        Files.rmdir(path.of(configuration.getRoot())
                .concat(tableName)
                .concat(detachedPartitionName)
                .$());
        Assert.assertFalse(ff.exists(path));

        // create the .attachable link in the table's data folder
        // with target the .detached folder in the different location
        other.of(s3Buckets) // <-- the copy of the now lost .detached folder
                .concat(tableName)
                .concat(detachedPartitionName)
                .$();
        path.of(configuration.getRoot()) // <-- soft link path
                .concat(tableName)
                .concat(partitionName)
                .put(configuration.getAttachPartitionSuffix())
                .$();
        Assert.assertEquals(0, ff.softLink(other, path));
        Assert.assertFalse(ff.isSoftLink(other));
        Assert.assertTrue(Os.isWindows() || ff.isSoftLink(path)); // TODO: isSoftLink does not work for windows
    }
}
