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
import io.questdb.cairo.sql.DataFrame;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.AttachDetachStatus.ATTACH_ERR_RENAME;


public class AlterTableAttachPartitionTest extends AbstractGriffinTest {
    private final static StringSink partitions = new StringSink();
    @Rule
    public TestName testName = new TestName();
    private Path otherPath;
    private Path path;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        otherPath = new Path();
        path = new Path();
    }

    @Override
    @After
    public void tearDown() {
        super.tearDown();
        path = Misc.free(path);
        otherPath = Misc.free(otherPath);
    }

    @Test
    public void testAlterTableAttachPartitionFromSoftLink() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {

            final String tableName = testName.getMethodName();
            final String partitionName = "2022-10-17";
            int txn = 0; // keep track of the transaction number

            // create table
            try (TableModel src = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(
                        1,
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        partitionName,
                        2
                ); // -> creates partitions 2022-10-17 and 2022-10-18 with 5K rows each
            }
            txn++;
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");

            // detach partition
            compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
            txn++;
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            "2022-10-18T00:00:16.779900Z\t2022-10-18T23:59:59.000000Z\t5000\n"); // 2022-10-17 is gone

            // attach partition via soft link
            copyToDifferentLocationAndMakeAttachableViaSoftLink(tableName, partitionName, "S3");
            compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
            txn++;

            // verify that the link has been renamed to what we expect
            path.of(configuration.getRoot()).concat(tableName).concat(partitionName);
            TableUtils.txnPartitionConditionally(path, txn - 1);
            Assert.assertTrue(Files.exists(path.$()));

            // verify content
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");
            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT -5",
                    "l\ti\tts\n" +
                            "4996\t4996\t2022-10-17T23:58:50.380400Z\n" +
                            "4997\t4997\t2022-10-17T23:59:07.660300Z\n" +
                            "4998\t4998\t2022-10-17T23:59:24.940200Z\n" +
                            "4999\t4999\t2022-10-17T23:59:42.220100Z\n" +
                            "5000\t5000\t2022-10-17T23:59:59.500000Z\n"
            );

            // insert a row at the end of the partition
            executeInsert("INSERT INTO " + tableName + " (l, i, ts) VALUES(0, 0, '" + partitionName + "T23:59:59.500001Z')");
            txn++;

            // update a row toward the end of the partition
            executeOperation(
                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '" + partitionName + "T23:59:42.220100Z'",
                    CompiledQuery.UPDATE
            );
            txn++;

            // insert a row at the beginning of the partition, this will result in the original folder being
            // copied across to table data space (hot), with a new txn, the original folder's content are NOT
            // removed, the link is.
            executeInsert("INSERT INTO " + tableName + " (l, i, ts) VALUES(-1, -1, '" + partitionName + "T00:00:00.100005Z')");
            txn++;
            // verify that the link does not exist
            // in windows the handle is held and cannot be deleted
            Assert.assertFalse(Files.exists(path));
            // verify that a new partition folder has been created in the table data space (hot)
            path.of(configuration.getRoot()).concat(tableName).concat(partitionName);
            TableUtils.txnPartitionConditionally(path, txn - 1);
            Assert.assertTrue(Files.exists(path.$()));
            // verify cold storage folder exists
            Assert.assertTrue(Files.exists(otherPath));
            AtomicInteger fileCount = new AtomicInteger();
            ff.walk(otherPath, (file, type) -> fileCount.incrementAndGet());
            Assert.assertTrue(fileCount.get() > 0);

            // update a row toward the beginning of the partition
            executeOperation(
                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '2022-10-17T00:00:34.559800Z'",
                    CompiledQuery.UPDATE
            );

            // verify content
            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT -5",
                    "l\ti\tts\n" +
                            "4997\t4997\t2022-10-17T23:59:07.660300Z\n" +
                            "4998\t4998\t2022-10-17T23:59:24.940200Z\n" +
                            "13\t4999\t2022-10-17T23:59:42.220100Z\n" +
                            "5000\t5000\t2022-10-17T23:59:59.500000Z\n" +
                            "0\t0\t2022-10-17T23:59:59.500001Z\n" // <-- the new row at the end
            );
            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT 5",
                    "l\ti\tts\n" +
                            "-1\t-1\t2022-10-17T00:00:00.100005Z\n" + // <-- the new row at the beginning
                            "1\t1\t2022-10-17T00:00:17.279900Z\n" +
                            "13\t2\t2022-10-17T00:00:34.559800Z\n" +
                            "3\t3\t2022-10-17T00:00:51.839700Z\n" +
                            "4\t4\t2022-10-17T00:01:09.119600Z\n"
            );
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            "2022-10-17T00:00:00.100005Z\t2022-10-18T23:59:59.000000Z\t10002\n");
        });
    }

    @Test
    public void testAlterTableAttachPartitionFromSoftLinkThenDetachIt() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);

        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {

            final String tableName = testName.getMethodName();
            final String partitionName = "2022-10-17";

            try (TableModel src = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(
                        1,
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        partitionName,
                        2
                );
            }

            compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
            copyToDifferentLocationAndMakeAttachableViaSoftLink(tableName, partitionName, "SNOW");
            compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");

            // detach the partition which was attached via soft link will result in the link being removed
            compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            "2022-10-18T00:00:16.779900Z\t2022-10-18T23:59:59.000000Z\t5000\n");
            // verify cold storage folder exists
            Assert.assertTrue(Files.exists(otherPath));
            AtomicInteger fileCount = new AtomicInteger();
            ff.walk(otherPath, (file, type) -> fileCount.incrementAndGet());
            Assert.assertTrue(fileCount.get() > 0);
            // verify the link was removed
            otherPath.of(configuration.getRoot())
                    .concat(tableName)
                    .concat(partitionName)
                    .put(configuration.getAttachPartitionSuffix())
                    .$();
            Assert.assertFalse(ff.exists(otherPath));
            // verify no copy was produced to hot space
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
        });
    }

    @Test
    public void testAlterTableAttachPartitionFromSoftLinkThenDropIt() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);

        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {

            final String tableName = testName.getMethodName();
            final String partitionName = "2022-10-17";

            try (TableModel src = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(
                        1,
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        partitionName,
                        2
                );
            }

            compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
            copyToDifferentLocationAndMakeAttachableViaSoftLink(tableName, partitionName, "IGLU");
            compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");

            // drop the partition which was attached via soft link
            compile("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            "2022-10-18T00:00:16.779900Z\t2022-10-18T23:59:59.000000Z\t5000\n");
            // verify cold storage folder exists
            Assert.assertTrue(Files.exists(otherPath));
            AtomicInteger fileCount = new AtomicInteger();
            ff.walk(otherPath, (file, type) -> fileCount.incrementAndGet());
            Assert.assertTrue(fileCount.get() > 0);

            path.of(configuration.getRoot())
                    .concat(tableName)
                    .concat(partitionName)
                    .put(".2")
                    .$();
            Assert.assertFalse(ff.exists(path));
        });
    }

    @Test
    public void testAlterTableAttachPartitionFromSoftLinkThenDropItWhileThereIsAReader() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);

        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {

            final String tableName = "table101";
            final String partitionName = "2022-11-04";

            try (TableModel src = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(
                        1,
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        partitionName,
                        2
                );
            }

            compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
            copyToDifferentLocationAndMakeAttachableViaSoftLink(tableName, partitionName, "FINLAND");
            compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            "2022-11-04T00:00:17.279900Z\t2022-11-05T23:59:59.000000Z\t10000\n");

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
                            "2022-11-05T00:00:16.779900Z\t2022-11-05T23:59:59.000000Z\t5000\n");

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
            Assert.assertTrue(Files.exists(otherPath));
            AtomicInteger fileCount = new AtomicInteger();
            ff.walk(otherPath, (file, type) -> fileCount.incrementAndGet());
            Assert.assertTrue(fileCount.get() > 0);
            Assert.assertFalse(Files.exists(path));
        });
    }

    @Test
    public void testAlterTableAttachPartitionFromSoftLinkThenUpdate() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(FilesFacadeImpl.INSTANCE, () -> {

            final String tableName = testName.getMethodName();
            final String partitionName = "2022-10-17";
            int txn = 0;

            try (TableModel src = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                createPopulateTable(
                        1,
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        partitionName,
                        2
                );
            }
            txn++;
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");

            compile("ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
            txn++;
            copyToDifferentLocationAndMakeAttachableViaSoftLink(tableName, partitionName, "LEGEND");
            compile("ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + partitionName + "'", sqlExecutionContext);
            txn++;

            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");
            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT 5",
                    "l\ti\tts\n" +
                            "1\t1\t2022-10-17T00:00:17.279900Z\n" +
                            "2\t2\t2022-10-17T00:00:34.559800Z\n" +
                            "3\t3\t2022-10-17T00:00:51.839700Z\n" +
                            "4\t4\t2022-10-17T00:01:09.119600Z\n" +
                            "5\t5\t2022-10-17T00:01:26.399500Z\n"
            );

            // collect last modified timestamps for files in cold storage
            final Map<String, Long> lastModified = new HashMap<>();
            path.of(otherPath);
            final int len = path.length();
            ff.walk(otherPath, (file, type) -> {
                path.trimTo(len).concat(file).$();
                lastModified.put(path.toString(), ff.getLastModified(path));
            });

            // execute an update directly on the cold storage partition
            executeOperation(
                    "UPDATE " + tableName + " SET l = 13 WHERE ts = '" + partitionName + "T00:00:17.279900Z'",
                    CompiledQuery.UPDATE
            );
            assertSql("SELECT min(ts), max(ts), count() FROM " + tableName,
                    "min\tmax\tcount\n" +
                            "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");
            assertSql("SELECT * FROM " + tableName + " WHERE ts in '" + partitionName + "' LIMIT 5",
                    "l\ti\tts\n" +
                            "13\t1\t2022-10-17T00:00:17.279900Z\n" +
                            "2\t2\t2022-10-17T00:00:34.559800Z\n" +
                            "3\t3\t2022-10-17T00:00:51.839700Z\n" +
                            "4\t4\t2022-10-17T00:01:09.119600Z\n" +
                            "5\t5\t2022-10-17T00:01:26.399500Z\n"
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
            path.of(otherPath);
            ff.walk(otherPath, (file, type) -> {
                // TODO: Update does not follow the usual path, like insert. To be able to
                //  prevent modifications on cold storage we must be able to detect whether
                //  the column files being versioned belong in a folder that is soft linked,
                //  and then move first the data to hot storage and take it from there.
                //  OR accept that UPDATE does modify cold storage, OR simply provide the
                //  mechanism to flag a partition as RO/RW and fail updates on RO partitions.
                //  The later will be achieved in a later PR.
                path.trimTo(len).concat(file).$();
                Long lm = lastModified.get(path.toString());
                Assert.assertTrue(lm == null || lm == ff.getLastModified(path));
            });
        });
    }

    @Test
    public void testAttach2Partitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src1", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst1", PartitionBy.DAY)) {
                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10000,
                        "2020-01-01",
                        12);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                attachFromSrcIntoDst(src, dst, "2020-01-09", "2020-01-10");
            }
        });
    }

    @Test
    public void testAttachActive2PartitionsOneByOneInDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src2", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst2", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10000,
                        "2020-01-01",
                        12);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                attachFromSrcIntoDst(src, dst, "2020-01-10");
                attachFromSrcIntoDst(src, dst, "2020-01-09");
            }
        });
    }

    @Test
    public void testAttachActive3Partitions() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src3", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst3", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10000,
                        "2020-01-01",
                        12);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                // 3 partitions unordered
                attachFromSrcIntoDst(src, dst, "2020-01-09", "2020-01-10", "2020-01-01");
            }
        });
    }

    @Test
    public void testAttachActiveWrittenPartition() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src4", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst4", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10000,
                        "2020-01-01",
                        11);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                attachFromSrcIntoDst(src, dst, "2020-01-10");
            }
        });
    }

    @Test
    public void testAttachFailsInvalidFormat() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst5", PartitionBy.MONTH)) {

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '202A-01'";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[39] 'YYYY-MM' expected[errno=0]", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachFailsInvalidFormatPartitionsAnnually() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst6", PartitionBy.YEAR)) {

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020-01-01'";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[39] 'YYYY' expected[errno=0]", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachFailsInvalidFormatPartitionsMonthly() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst7", PartitionBy.MONTH)) {

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020-01-01'";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[39] 'YYYY-MM' expected[errno=0]", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachFailsInvalidSeparatorFormat() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst8", PartitionBy.MONTH)) {

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020-01'.'2020-02'";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals("[48] ',' expected", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachMissingPartition() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst9", PartitionBy.DAY)) {
                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020-01-01'";
                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not attach partition");
                }
            }
        });
    }

    @Test
    public void testAttachNonExisting() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel dst = new TableModel(configuration, "dst10", PartitionBy.DAY)) {
                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020-01-01'";

                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not attach partition");
                }
            }
        });
    }

    @Test
    public void testAttachPartitionInWrongDirectoryName() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    TableModel src = new TableModel(configuration, "src11", PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, "dst11", PartitionBy.DAY)
            ) {

                createPopulateTable(
                        1,
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        10000,
                        "2020-01-01",
                        1);

                CairoTestUtils.create(
                        dst.col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .timestamp("ts"));

                copyPartitionToAttachable(src.getName(), "2020-01-01", dst.getName(), "COCONUTS");

                try {
                    compile("ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2020-01-02'", sqlExecutionContext);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not attach partition");
                }
            }
        });
    }

    @Test
    public void testAttachPartitionStringColIndexMessedNotInOrder() throws Exception {
        assertMemoryLeak(() -> {
            AddColumn src = s -> s.col("l", ColumnType.LONG)
                    .col("i", ColumnType.INT)
                    .timestamp("ts")
                    .col("str", ColumnType.STRING);

            assertSchemaMismatch(
                    "src26",
                    src,
                    "dst26",
                    dst -> {
                    },
                    s -> writeToStrIndexFile(s, "2022-08-01", "str.i", 0L, 16L),
                    "Variable size column has invalid data address value"
            );
        });
    }

    @Test
    public void testAttachPartitionStringColIndexMessedOffsetOutsideFileBounds() throws Exception {
        assertMemoryLeak(() -> {
            AddColumn src = s -> s.col("i", ColumnType.INT)
                    .col("l", ColumnType.LONG)
                    .timestamp("ts")
                    .col("str", ColumnType.STRING);

            assertSchemaMismatch(
                    "src27",
                    src,
                    "dst27",
                    dst -> {
                    },
                    s -> writeToStrIndexFile(s, "2022-08-01", "str.i", Long.MAX_VALUE, 256L),
                    "dataAddress=" + Long.MAX_VALUE
            );

            assertSchemaMismatch(
                    "src28",
                    src,
                    "dst28",
                    dst -> {
                    },
                    s -> writeToStrIndexFile(s, "2022-08-01", "str.i", -1L, 256L),
                    "dataAddress=" + -1L
            );
        });
    }

    @Test
    public void testAttachPartitionStringIndexFileTooSmall() throws Exception {
        assertMemoryLeak(() -> {
            AddColumn src = s -> s.col("i", ColumnType.INT)
                    .col("l", ColumnType.LONG)
                    .timestamp("ts")
                    .col("sh", ColumnType.STRING);

            assertSchemaMismatch(
                    "src30",
                    src,
                    "dst30",
                    dst -> {
                    },
                    s -> {
                        engine.clear();
                        path.of(configuration.getRoot()).concat(s.getName()).concat("2022-08-01").concat("sh.i").$();
                        int fd = Files.openRW(path);
                        Files.truncate(fd, Files.length(fd) / 4);
                        Files.close(fd);
                    },
                    "Column file is too small"
            );
        });
    }

    @Test
    public void testAttachPartitionSymbolFileNegativeValue() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src31", PartitionBy.DAY)) {
                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("l", ColumnType.LONG)
                                .col("sym", ColumnType.SYMBOL)
                                .col("i", ColumnType.INT),
                        10000,
                        "2022-08-01",
                        3);

                writeToStrIndexFile(src, "2022-08-02", "sym.d", -1L, 4L);

                try (TableModel dst = new TableModel(configuration, "dst31", PartitionBy.DAY)) {
                    createPopulateTable(
                            1,
                            dst.timestamp("ts")
                                    .col("l", ColumnType.LONG)
                                    .col("sym", ColumnType.SYMBOL)
                                    .col("i", ColumnType.INT),
                            10000,
                            "2022-08-01",
                            1);

                    try {
                        attachFromSrcIntoDst(src, dst, "2022-08-02");
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Symbol file does not match symbol column, invalid key");
                    }
                }
            }
        });
    }

    @Test
    public void testAttachPartitionSymbolFileTooSmall() throws Exception {
        assertMemoryLeak(() -> {

            AddColumn src = s -> s.col("i", ColumnType.INT)
                    .col("l", ColumnType.LONG)
                    .timestamp("ts")
                    .col("sh", ColumnType.SYMBOL);

            assertSchemaMismatch(
                    "src32",
                    src,
                    "dst32",
                    dst -> {
                    },
                    s -> {
                        // .v file
                        engine.clear();
                        path.of(configuration.getRoot()).concat(s.getName()).concat("2022-08-01").concat("sh.v").$();
                        int fd = Files.openRW(path);
                        Files.truncate(fd, Files.length(fd) / 2);
                        Files.close(fd);
                    },
                    "Symbol file does not match symbol column"
            );
        });
    }

    @Test
    public void testAttachPartitionWhereTimestampColumnNameIsOtherThanTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src33", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst33", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10000,
                        "2022-08-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts1")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                try {
                    attachFromSrcIntoDst(src, dst, "2022-08-01");
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(),
                            "could not open read-only"
                    );
                    TestUtils.assertContains(e.getFlyweightMessage(),
                            "ts1.d"
                    );
                }
            }
        });
    }

    @Test
    public void testAttachPartitionWithColumnTypes() throws Exception {
        int idx = 0;
        assertSchemaMatch(dst -> dst.col("str", ColumnType.STRING), idx++);
        assertSchemaMatch(dst -> dst.col("l1", ColumnType.LONG), idx++);
        assertSchemaMatch(dst -> dst.col("i1", ColumnType.INT), idx++);
        assertSchemaMatch(dst -> dst.col("b", ColumnType.BOOLEAN), idx++);
        assertSchemaMatch(dst -> dst.col("db", ColumnType.DOUBLE), idx++);
        assertSchemaMatch(dst -> dst.col("fl", ColumnType.FLOAT), idx++);
        assertSchemaMatch(dst -> dst.col("dt", ColumnType.DATE), idx++);
        assertSchemaMatch(dst -> dst.col("ts1", ColumnType.TIMESTAMP), idx++);
        assertSchemaMatch(dst -> dst.col("l256", ColumnType.LONG256), idx++);
        assertSchemaMatch(dst -> dst.col("byt", ColumnType.BYTE), idx++);
        assertSchemaMatch(dst -> dst.col("ch", ColumnType.CHAR), idx++);
        assertSchemaMatch(dst -> dst.col("sh", ColumnType.SHORT), idx);
    }

    @Test
    public void testAttachPartitionWrongFixedColumn() throws Exception {
        assertMemoryLeak(() -> {
            AddColumn src = s -> s.col("l", ColumnType.LONG)
                    .col("i", ColumnType.INT)
                    .timestamp("ts")
                    .col("sh", ColumnType.SHORT);

            assertSchemaMismatch(
                    "src34",
                    src,
                    "dst34",
                    dst -> {
                    },
                    s -> {
                        // .d file
                        engine.clear();
                        path.of(configuration.getRoot()).concat(s.getName()).concat("2022-08-01").concat("sh.d").$();
                        int fd = Files.openRW(path);
                        Files.truncate(fd, Files.length(fd) / 10);
                        Files.close(fd);
                    },
                    "Column file is too small"
            );
        });
    }

    @Test
    public void testAttachPartitionsDeletedColumnFromSrc() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, testName.getMethodName() + "_src", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, testName.getMethodName() + "_dst", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s", ColumnType.SYMBOL).indexed(true, 128)
                                .col("str", ColumnType.STRING),
                        8,
                        "2022-08-01",
                        4);
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, src.getName(), "testing")) {
                    writer.removeColumn("s");
                    writer.removeColumn("str");
                    writer.removeColumn("i");
                }

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG)
                        .col("s", ColumnType.SYMBOL).indexed(true, 128)
                        .col("str", ColumnType.STRING)
                );

                copyPartitionToAttachable(src.getName(), "2022-08-02", dst.getName(), "2022-08-02");
                compile("ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2022-08-02'", sqlExecutionContext);

                engine.clear();
                assertQuery(
                        "ts\ti\tl\ts\tstr\n" +
                                "2022-08-02T11:59:59.625000Z\tNaN\t3\t\t\n" +
                                "2022-08-02T23:59:59.500000Z\tNaN\t4\t\t\n",
                        dst.getName(),
                        null,
                        "ts",
                        true,
                        false,
                        true
                );
            }
        });
    }

    @Test
    public void testAttachPartitionsDetachedHasExtraColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src48", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst48", PartitionBy.DAY)) {

                int partitionRowCount = 11;
                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s", ColumnType.SYMBOL).indexed(true, 128),
                        partitionRowCount,
                        "2022-08-01",
                        4);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                copyPartitionToAttachable(src.getName(), "2022-08-01", dst.getName(), "2022-08-01");

                long timestamp = TimestampFormatUtils.parseTimestamp("2022-08-01T00:00:00.000z");
                long txn;
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, dst.getName(), "testing")) {
                    txn = writer.getTxn();
                    writer.attachPartition(timestamp);
                }
                path.of(configuration.getRoot()).concat(dst.getName()).concat("2022-08-01");
                TableUtils.txnPartitionConditionally(path, txn);
                int pathLen = path.length();

                // Extra columns not deleted
                Assert.assertTrue(Files.exists(path.concat("s.d").$()));
                Assert.assertTrue(Files.exists(path.trimTo(pathLen).concat("s.k").$()));
                Assert.assertTrue(Files.exists(path.trimTo(pathLen).concat("s.v").$()));
                Assert.assertTrue(Files.exists(path.trimTo(pathLen).concat("l.d").$()));

                engine.clear();
                assertQuery(
                        "ts\ti\tl\n" +
                                "2022-08-01T08:43:38.090909Z\t1\t1\n" +
                                "2022-08-01T17:27:16.181818Z\t2\t2\n",
                        dst.getName(),
                        null,
                        "ts",
                        true,
                        false,
                        true
                );
            }
        });
    }

    @Test
    public void testAttachPartitionsNonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src35", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst35", PartitionBy.NONE)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10000,
                        "2022-08-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                try {
                    attachFromSrcIntoDst(src, dst, "2022-08-01");
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertEquals("[25] table is not partitioned", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAttachPartitionsTableInTransaction() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src36", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst36", PartitionBy.DAY)) {

                int partitionRowCount = 111;
                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        partitionRowCount,
                        "2022-08-01",
                        1);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                copyPartitionToAttachable(src.getName(), "2022-08-01", dst.getName(), "2022-08-01");

                // Add 1 row without commit
                long timestamp = TimestampFormatUtils.parseTimestamp("2022-08-01T00:00:00.000z");
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, dst.getName(), "testing")) {
                    long insertTs = TimestampFormatUtils.parseTimestamp("2022-08-01T23:59:59.999z");
                    TableWriter.Row row = writer.newRow(insertTs + 1000L);
                    row.putLong(0, 1L);
                    row.putInt(1, 1);
                    row.append();

                    Assert.assertTrue(writer.inTransaction());

                    // This commits the append before attaching
                    writer.attachPartition(timestamp);
                    Assert.assertEquals(partitionRowCount + 1, writer.size());

                    Assert.assertFalse(writer.inTransaction());
                }
            }
        });
    }

    @Test
    public void testAttachPartitionsWithIndexedSymbolsValueMatch() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src37", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst37", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s", ColumnType.SYMBOL).indexed(false, 4096),
                        10000,
                        "2022-08-01",
                        10);

                // Make sure nulls are included in the partition to be attached
                assertSql("select count() from " + src.getName() + " where ts in '2022-08-09' and s = null", "count\n302\n");

                createPopulateTable(
                        1,
                        dst.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG)
                                .col("s", ColumnType.SYMBOL).indexed(false, 4096),
                        10000,
                        "2022-08-01",
                        10);

                compile("alter table " + dst.getName() + " drop partition list '2022-08-09'");

                attachFromSrcIntoDst(src, dst, "2022-08-09");
            }
        });
    }

    @Test
    public void testAttachPartitionsWithSymbolsValueDoesNotMatch() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src38", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst38", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("s2", ColumnType.SYMBOL)
                                .col("l", ColumnType.LONG),
                        20,
                        "2022-08-01",
                        3);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("s", ColumnType.SYMBOL)
                        .col("l", ColumnType.LONG));

                attachFromSrcIntoDst(src, dst, "2022-08-01", "2022-08-02");

                // s2 column files from the attached partitions should be ignored
                // and coltops for s column should be created instead.
                assertSql("select count() from " + dst.getName() + " where s is not null", "count\n0\n");
            }
        });
    }

    @Test
    public void testAttachPartitionsWithSymbolsValueMatch() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src39", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst39", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL)
                                .timestamp("ts"),
                        10000,
                        "2022-08-01",
                        10);

                // Make sure nulls are included in the partition to be attached
                assertSql("select count() from " + src.getName() + " where ts in '2022-08-09' and s = null", "count\n302\n");

                createPopulateTable(
                        1,
                        dst.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL)
                                .timestamp("ts"),
                        10000,
                        "2022-08-01",
                        10);

                compile("alter table " + dst.getName() + " drop partition list '2022-08-09'");

                attachFromSrcIntoDst(src, dst, "2022-08-09");
            }
        });
    }

    @Test
    public void testAttachPartitionsWithSymbolsValueMatchWithNoIndex() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src40", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst40", PartitionBy.DAY)) {

                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL)
                                .timestamp("ts"),
                        10000,
                        "2022-08-01",
                        10);

                createPopulateTable(
                        1,
                        dst.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 4096)
                                .timestamp("ts"),
                        10000,
                        "2022-08-01",
                        10);

                compile("alter table " + dst.getName() + " drop partition list '2022-08-09'");

                try {
                    attachFromSrcIntoDst(src, dst, "2022-08-09");
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(),
                            "Symbol index value file does not exist"
                    );
                }
            }
        });
    }

    @Test
    public void testAttachPartitionsWithSymbolsValueMatchWithNoIndexKeyFile() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src41", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst41", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 4096)
                                .timestamp("ts"),
                        10000,
                        "2022-08-01",
                        10);

                createPopulateTable(
                        1,
                        dst.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).indexed(true, 4096)
                                .timestamp("ts"),
                        10000,
                        "2022-08-01",
                        10);

                compile("alter table " + dst.getName() + " drop partition list '2022-08-09'");

                // remove .k
                engine.clear();
                Assert.assertTrue(Files.remove(
                        path.of(configuration.getRoot()).concat(src.getName()).concat("2022-08-09").concat("s.k").$()
                ));

                try {
                    attachFromSrcIntoDst(src, dst, "2022-08-09");
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Symbol index key file does not exist");
                }
            }
        });
    }

    @Test
    public void testAttachSamePartitionTwice() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src42", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst42", PartitionBy.DAY)) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        10000,
                        "2022-08-01",
                        10);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                attachFromSrcIntoDst(src, dst, "2022-08-09");

                String alterCommand = "ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST '2022-08-09'";

                try {
                    compile(alterCommand, sqlExecutionContext);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not attach partition");
                }
            }
        });
    }

    @Test
    public void testCannotMapTimestampColumn() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            private int tsdFd;

            @Override
            public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
                if (tsdFd != fd) {
                    return super.mmap(fd, len, offset, flags, memoryTag);
                }
                tsdFd = 0;
                return -1;
            }

            @Override
            public int openRO(LPSZ name) {
                int fd = super.openRO(name);
                if (Chars.endsWith(name, "ts.d") && counter.decrementAndGet() == 0) {
                    this.tsdFd = fd;
                }
                return fd;
            }
        };

        testSqlFailedOnFsOperation(ff, "srcMap", "dstMap", false, "could not mmap");
    }

    @Test
    public void testCannotReadTimestampColumn() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            @Override
            public int openRO(LPSZ name) {
                if (Chars.endsWith(name, "ts.d") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRO(name);
            }
        };

        testSqlFailedOnFsOperation(ff, "srcTs", "dstTs", false, "could not open read-only");
    }

    @Test
    public void testCannotReadTimestampColumnFileDoesNotExist() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            @Override
            public int openRO(LPSZ name) {
                if (Chars.endsWith(name, "ts.d") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRO(name);
            }
        };

        testSqlFailedOnFsOperation(ff, "srcTs2", "dstTs2", false, "could not open read-only", "ts.d");
    }

    @Test
    public void testCannotRenameDetachedFolderOnAttach() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (Chars.contains(to, "2020-01-01") && counter.decrementAndGet() == 0) {
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };

        testSqlFailedOnFsOperation(ff, "srcRen", "dstRen", false, ATTACH_ERR_RENAME.name());
    }

    @Test
    public void testCannotSwitchPartition() throws Exception {
        AtomicInteger counter = new AtomicInteger(1);
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            @Override
            public int openRW(LPSZ name, long opts) {
                if (Chars.contains(name, "dst" + testName.getMethodName()) && Chars.contains(name, "2020-01-01") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        testSqlFailedOnFsOperation(ff, "src" + testName.getMethodName(), "dst" + testName.getMethodName(), true, " is distressed");
    }

    @Test
    public void testDetachAttachDifferentPartitionTableReaderReload() throws Exception {
        if (FilesFacadeImpl.INSTANCE.isRestrictedFileSystem()) {
            // cannot remove opened files on Windows, test  not relevant
            return;
        }

        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src47", PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dst47", PartitionBy.DAY)) {

                int partitionRowCount = 5;
                createPopulateTable(
                        1,
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("str", ColumnType.STRING)
                                .timestamp("ts"),
                        partitionRowCount,
                        "2020-01-09",
                        2);

                createPopulateTable(
                        1,
                        dst.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("str", ColumnType.STRING)
                                .timestamp("ts"),
                        partitionRowCount - 3,
                        "2020-01-09",
                        2);

                try (TableReader dstReader = new TableReader(configuration, dst.getTableName())) {
                    dstReader.openPartition(0);
                    dstReader.openPartition(1);
                    dstReader.goPassive();

                    long timestamp = TimestampFormatUtils.parseTimestamp("2020-01-09T00:00:00.000z");

                    try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, dst.getTableName(), "testing")) {
                        writer.removePartition(timestamp);
                        copyPartitionToAttachable(src.getName(), "2020-01-09", dst.getName(), "2020-01-09");
                        Assert.assertEquals(AttachDetachStatus.OK, writer.attachPartition(timestamp));
                    }

                    // Go active
                    Assert.assertTrue(dstReader.reload());
                    try (TableReader srcReader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, src.getTableName())) {
                        String expected =
                                "l\ti\tstr\tts\n" +
                                        "1\t1\t1\t2020-01-09T09:35:59.800000Z\n" +
                                        "2\t2\t2\t2020-01-09T19:11:59.600000Z\n" +
                                        "3\t3\t3\t2020-01-10T04:47:59.400000Z\n" +
                                        "4\t4\t4\t2020-01-10T14:23:59.200000Z\n" +
                                        "5\t5\t5\t2020-01-10T23:59:59.000000Z\n";
                        assertCursor(expected, srcReader.getCursor(), srcReader.getMetadata(), true);

                        // Check that first 2 lines of partition 2020-01-09 match for src and dst tables
                        assertCursor("l\ti\tstr\tts\n" +
                                        "1\t1\t1\t2020-01-09T09:35:59.800000Z\n" +
                                        "2\t2\t2\t2020-01-09T19:11:59.600000Z\n" +
                                        "2\t2\t2\t2020-01-10T23:59:59.000000Z\n",
                                dstReader.getCursor(),
                                dstReader.getMetadata(),
                                true);
                    }
                }
            }
        });
    }

    private void assertSchemaMatch(AddColumn tm, int idx) throws Exception {
        if (idx > 0) {
            setUp();
        }
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "srcCM" + idx, PartitionBy.DAY);
                 TableModel dst = new TableModel(configuration, "dstCM" + idx, PartitionBy.DAY)) {
                src.col("l", ColumnType.LONG)
                        .col("i", ColumnType.INT)
                        .timestamp("ts");
                tm.add(src);

                createPopulateTable(
                        src,
                        10000,
                        "2022-08-01",
                        10);

                dst.col("l", ColumnType.LONG)
                        .col("i", ColumnType.INT)
                        .timestamp("ts");
                tm.add(dst);

                CairoTestUtils.create(dst);
                attachFromSrcIntoDst(src, dst, "2022-08-01");
            }
        });
        if (idx > 0) {
            tearDown();
        }
    }

    private void assertSchemaMismatch(
            String srcTableName,
            AddColumn srcTransform,
            String dstTableName,
            AddColumn dstTransform,
            AddColumn afterCreateSrc,
            String errorMessage
    ) throws Exception {
        try (
                TableModel src = new TableModel(configuration, srcTableName, PartitionBy.DAY);
                TableModel dst = new TableModel(configuration, dstTableName, PartitionBy.DAY)
        ) {
            srcTransform.add(src);
            createPopulateTable(
                    1,
                    src,
                    45000,
                    "2022-08-01",
                    10
            );

            if (afterCreateSrc != null) {
                afterCreateSrc.add(src);
            }

            // make dst a copy of src
            int tsIdx = src.getTimestampIndex();
            for (int i = 0, limit = src.getColumnCount(); i < limit; i++) {
                if (i != tsIdx) {
                    dst.col(src.getColumnName(i), src.getColumnType(i));
                    if (src.isIndexed(i)) {
                        dst.indexed(true, src.getIndexBlockCapacity(i));
                    }
                } else {
                    dst.timestamp(src.getColumnName(i));
                }
            }

            // apply transform to dst and create table
            dstTransform.add(dst);
            CairoTestUtils.create(dst);
            try {
                attachFromSrcIntoDst(src, dst, "2022-08-01");
                Assert.fail("Expected exception with '" + errorMessage + "' message");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), errorMessage);
            }
            Files.rmdir(path.of(root).concat(dstTableName).concat("2022-08-01").put(configuration.getAttachPartitionSuffix()).$());
        }
    }

    private void attachFromSrcIntoDst(TableModel src, TableModel dst, String... partitionList) throws SqlException, NumericException {
        partitions.clear();
        for (int i = 0; i < partitionList.length; i++) {
            String partition = partitionList[i];
            if (i > 0) {
                partitions.put(",");
            }
            partitions.put("'");
            partitions.put(partition);
            partitions.put("'");
        }

        engine.clear();
        path.of(configuration.getRoot()).concat(src.getName());
        int pathLen = path.length();
        otherPath.of(configuration.getRoot()).concat(dst.getName());
        int otherLen = otherPath.length();
        for (int i = 0; i < partitionList.length; i++) {
            String partition = partitionList[i];
            path.trimTo(pathLen).concat(partition).$();
            otherPath.trimTo(otherLen).concat(partition).put(configuration.getAttachPartitionSuffix()).$();
            TestUtils.copyDirectory(path, otherPath, configuration.getMkDirMode());
        }

        int rowCount = readAllRows(dst.getName());
        engine.clear();
        compile("ALTER TABLE " + dst.getName() + " ATTACH PARTITION LIST " + partitions + ";", sqlExecutionContext);
        int newRowCount = readAllRows(dst.getName());
        Assert.assertTrue(newRowCount > rowCount);

        long timestamp = 0;
        for (String s : partitionList) {
            long ts = TimestampFormatUtils.parseTimestamp(s
                    + (src.getPartitionBy() == PartitionBy.YEAR ? "-01-01" : "")
                    + (src.getPartitionBy() == PartitionBy.MONTH ? "-01" : "")
                    + "T23:59:59.999z");
            if (ts > timestamp) {
                timestamp = ts;
            }
        }

        // Check table is writable after partition attach
        engine.clear();
        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, dst.getName(), "testing")) {
            TableWriter.Row row = writer.newRow(timestamp);
            row.putInt(1, 1);
            row.append();
            writer.commit();
        }
    }

    private void copyPartitionAndMetadata(
            CharSequence srcRoot,
            String srcTableName,
            String srcPartitionName,
            CharSequence dstRoot,
            String dstTableName,
            String dstPartitionName,
            String dstPartitionNameSuffix
    ) {
        path.of(srcRoot)
                .concat(srcTableName)
                .concat(srcPartitionName)
                .slash$();
        otherPath.of(dstRoot)
                .concat(dstTableName)
                .concat(dstPartitionName);

        if (!Chars.isBlank(dstPartitionNameSuffix)) {
            otherPath.put(dstPartitionNameSuffix);
        }
        otherPath.slash$();

        TestUtils.copyDirectory(path, otherPath, configuration.getMkDirMode());

        // copy _meta
        Files.copy(
                path.parent().parent().concat(TableUtils.META_FILE_NAME).$(),
                otherPath.parent().concat(TableUtils.META_FILE_NAME).$()
        );
        // copy _cv
        Files.copy(
                path.parent().concat(TableUtils.COLUMN_VERSION_FILE_NAME).$(),
                otherPath.parent().concat(TableUtils.COLUMN_VERSION_FILE_NAME).$()
        );
        // copy _txn
        Files.copy(
                path.parent().concat(TableUtils.TXN_FILE_NAME).$(),
                otherPath.parent().concat(TableUtils.TXN_FILE_NAME).$()
        );
    }

    private void copyPartitionToAttachable(
            String srcTableName,
            String srcPartitionName,
            String dstTableName,
            String dstPartitionName
    ) {
        copyPartitionAndMetadata(
                configuration.getRoot(),
                srcTableName,
                srcPartitionName,
                configuration.getRoot(),
                dstTableName,
                dstPartitionName,
                configuration.getAttachPartitionSuffix()
        );
    }

    private void copyToDifferentLocationAndMakeAttachableViaSoftLink(String tableName, CharSequence partitionName, String otherLocation) throws IOException {
        engine.releaseAllReaders();
        engine.releaseAllWriters();

        // copy .detached folder to the different location
        // then remove the original .detached folder
        final CharSequence s3Buckets = temp.newFolder(otherLocation).getAbsolutePath();
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
        otherPath.of(s3Buckets) // <-- the copy of the now lost .detached folder
                .concat(tableName)
                .concat(detachedPartitionName)
                .$();
        path.of(configuration.getRoot()) // <-- soft link path
                .concat(tableName)
                .concat(partitionName)
                .put(configuration.getAttachPartitionSuffix())
                .$();
        Assert.assertEquals(0, ff.softLink(otherPath, path));
        Assert.assertFalse(ff.isSoftLink(otherPath));
        Assert.assertTrue(Os.type == Os.WINDOWS || ff.isSoftLink(path)); // TODO: isSoftLink does not work for windows
    }

    private int readAllRows(String tableName) {
        try (FullFwdDataFrameCursor cursor = new FullFwdDataFrameCursor()) {
            cursor.of(engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName));
            DataFrame frame;
            int count = 0;
            while ((frame = cursor.next()) != null) {
                for (long index = frame.getRowHi() - 1, lo = frame.getRowLo() - 1; index > lo; index--) {
                    count++;
                }
            }
            return count;
        } catch (CairoException err) {
            return 0;
        }
    }

    private void testSqlFailedOnFsOperation(
            FilesFacadeImpl ff,
            String srcTableName,
            String dstTableName,
            boolean catchAll,
            String... errorContains
    ) throws Exception {
        assertMemoryLeak(ff, () -> {
            try (
                    TableModel src = new TableModel(configuration, srcTableName, PartitionBy.DAY);
                    TableModel dst = new TableModel(configuration, dstTableName, PartitionBy.DAY)
            ) {

                createPopulateTable(
                        1,
                        src.timestamp("ts")
                                .col("i", ColumnType.INT)
                                .col("l", ColumnType.LONG),
                        100,
                        "2020-01-01",
                        3);

                CairoTestUtils.create(dst.timestamp("ts")
                        .col("i", ColumnType.INT)
                        .col("l", ColumnType.LONG));

                try {
                    attachFromSrcIntoDst(src, dst, "2020-01-01");
                    Assert.fail();
                } catch (CairoException | SqlException e) {
                    for (String error : errorContains) {
                        TestUtils.assertContains(e.getFlyweightMessage(), error);
                    }
                } catch (Throwable e) {
                    if (catchAll) {
                        for (String error : errorContains) {
                            TestUtils.assertContains(e.getMessage(), error);
                        }
                    } else {
                        throw e;
                    }
                }

                // second attempt without FilesFacade override should work ok
                attachFromSrcIntoDst(src, dst, "2020-01-02");
            }
        });
    }

    private void writeToStrIndexFile(TableModel src, String partition, String columnFileName, long value, long offset) {
        FilesFacade ff = FilesFacadeImpl.INSTANCE;
        int fd = -1;
        long writeBuff = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            // .i file
            engine.clear();
            path.of(configuration.getRoot()).concat(src.getName()).concat(partition).concat(columnFileName).$();
            fd = ff.openRW(path, CairoConfiguration.O_NONE);
            Unsafe.getUnsafe().putLong(writeBuff, value);
            ff.write(fd, writeBuff, Long.BYTES, offset);
        } finally {
            ff.close(fd);
            Unsafe.free(writeBuff, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @FunctionalInterface
    private interface AddColumn {
        void add(TableModel tm);
    }
}
