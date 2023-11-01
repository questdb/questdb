/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class O3PartitionPurgeTest extends AbstractCairoTest {
    private static O3PartitionPurgeJob purgeJob;

    @BeforeClass
    public static void begin() {
        purgeJob = new O3PartitionPurgeJob(engine.getMessageBus(), engine.getSnapshotAgent(), 1);
    }

    @AfterClass
    public static void end() {
        purgeJob = Misc.free(purgeJob);
    }

    @Test
    public void test2ReadersUsePartition() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl as (select x, cast('1970-01-10T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // OOO insert
            insert("insert into tbl select 4, '1970-01-10T09'");

            // This should lock partition 1970-01-10.1 from being deleted from disk
            try (TableReader rdr = getReader("tbl")) {

                try (TableReader rdr2 = getReader("tbl")) {
                    // in order insert
                    insert("insert into tbl select 2, '1970-01-10T11'");

                    // OOO insert
                    insert("insert into tbl select 4, '1970-01-10T09'");

                    runPartitionPurgeJobs();

                    rdr2.openPartition(0);
                }

                runPartitionPurgeJobs();

                // This should not fail
                rdr.openPartition(0);
            }
            runPartitionPurgeJobs();

            try (Path path = new Path()) {
                path.concat(engine.getConfiguration().getRoot()).concat("tbl").concat("1970-01-10");
                int len = path.size();
                for (int i = 0; i < 3; i++) {
                    path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                    Assert.assertFalse(Utf8s.toString(path), Files.exists(path));
                }
            }
        });
    }

    @Test
    public void testAsyncPurgeOnBusyWriter() throws Exception {
        int tableCount = 3;
        assertMemoryLeak(() -> {
            for (int i = 0; i < tableCount; i++) {
                ddl("create table tbl" + i + " as (select x, cast('1970-01-01T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY");
            }

            final CyclicBarrier barrier = new CyclicBarrier(3);
            AtomicInteger done = new AtomicInteger();
            // Open a reader so that writer will not delete partitions easily
            ObjList<TableReader> readers = new ObjList<>(tableCount);
            for (int i = 0; i < tableCount; i++) {
                readers.add(getReader("tbl" + i));
            }

            Thread writeThread = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < 32; i++) {
                        for (int j = 0; j < tableCount; j++) {
                            insert("insert into tbl" + j +
                                    " select 2, '1970-01-10T10' from long_sequence(1) " +
                                    "union all " +
                                    "select 1, '1970-01-09T09'  from long_sequence(1)");
                        }
                    }
                    Path.clearThreadLocals();
                    done.incrementAndGet();
                } catch (Throwable ex) {
                    LOG.error().$(ex).$();
                    done.decrementAndGet();
                }
            });

            Thread readThread = new Thread(() -> {
                try {
                    barrier.await();
                    while (done.get() == 0) {
                        for (int i = 0; i < tableCount; i++) {
                            readers.get(i).openPartition(0);
                            readers.get(i).reload();
                        }
                        Os.pause();
                        Path.clearThreadLocals();
                    }
                } catch (Throwable ex) {
                    LOG.error().$(ex).$();
                    done.addAndGet(-2);
                }
            });

            writeThread.start();
            readThread.start();

            barrier.await();
            while (done.get() == 0) {
                runPartitionPurgeJobs();
                Os.pause();
            }
            runPartitionPurgeJobs();

            Assert.assertEquals(1, done.get());
            writeThread.join();
            readThread.join();
            Misc.freeObjList(readers);
        });
    }

    @Test
    public void testInvalidFolderNames() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl as (select x, cast('1970-01-10T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // This should lock partition 1970-01-10.1 from being deleted from disk
            try (TableReader ignored = getReader("tbl")) {
                // OOO insert
                insert("insert into tbl select 4, '1970-01-10T09'");
            }

            TableToken tableToken = engine.verifyTableName("tbl");
            try (Path path = new Path()) {
                Files.mkdir(path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("invalid_folder.123").$(), 509);
                Files.mkdir(path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-01.invalid").$(), 509);

                runPartitionPurgeJobs();

                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path));

                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10.1").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path));
            }
        });
    }

    @Test
    public void testLastPartitionDeletedAsyncAfterDroppedBySql() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                ddl("create table tbl as (select x, timestamp_sequence('1970-01-10', 60*60*1000000L) ts from long_sequence(5)) timestamp(ts) partition by HOUR");

                TableToken tableToken = engine.verifyTableName("tbl");
                try (TableReader rdr = getReader("tbl")) {
                    try (TableReader rdr2 = getReader("tbl")) {
                        ddl("alter table tbl drop partition where ts >= '1970-01-10T03'", sqlExecutionContext);
                        runPartitionPurgeJobs();

                        // This should not fail
                        rdr2.openPartition(0);
                    }
                    runPartitionPurgeJobs();
                    path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10T03").concat("x.d").$();
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path));

                    // This should not fail
                    rdr.openPartition(0);
                }
                runPartitionPurgeJobs();

                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10T03").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path));
                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10T04").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path));
                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10T05").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path));
            }
        });
    }

    @Test
    public void testManyReadersOpenClosedAscDense() throws Exception {
        testManyReadersOpenClosedDense(0, 1, 5);
    }

    @Test
    public void testManyReadersOpenClosedAscSparse() throws Exception {
        testManyReadersOpenClosedSparse(0, 1, 4);
    }

    @Test
    public void testManyReadersOpenClosedDescDense() throws Exception {
        testManyReadersOpenClosedDense(3, -1, 4);
    }

    @Test
    public void testManyReadersOpenClosedDescSparse() throws Exception {
        testManyReadersOpenClosedSparse(4, -1, 5);
    }

    @Test
    public void testManyTablesFuzzTest() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        int tableCount = 1;
        int testIterations = 100;

        assertMemoryLeak(() -> {
            for (int i = 0; i < tableCount; i++) {
                ddl("create table tbl" + i + " as (select x, cast('1970-01-10T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY");
            }

            ObjList<TableReader> readers = new ObjList<>();
            for (int i = 0; i < testIterations; i++) {
                String tableName = "tbl" + rnd.nextInt(tableCount);
                String partition = "1970-0" + (1 + rnd.nextInt(1)) + "-01";

                runPartitionPurgeJobs();

                if (rnd.nextBoolean()) {
                    // deffo OOO insert
                    insert("insert into " + tableName + " select 4, '" + partition + "T09'");
                } else {
                    // in order insert if last partition
                    insert("insert into " + tableName + " select 2, '" + partition + "T11'");
                }

                // lock reader on this transaction
                readers.add(getReader(tableName));
            }

            runPartitionPurgeJobs();

            for (int i = 0; i < testIterations; i++) {
                runPartitionPurgeJobs();
                TableReader reader = readers.get(i);
                reader.openPartition(0);
                reader.close();
            }

            try (
                    Path path = new Path();
                    TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade())
            ) {
                for (int i = 0; i < tableCount; i++) {
                    String tableName = "tbl" + i;
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(engine.getConfiguration().getRoot()).concat(tableToken);
                    int len = path.size();
                    int partitionBy = PartitionBy.DAY;
                    txReader.ofRO(path.concat(TXN_FILE_NAME).$(), partitionBy);
                    txReader.unsafeLoadAll();

                    Assert.assertEquals(2, txReader.getPartitionCount());
                    for (int p = 0; p < 2; p++) {
                        long partitionTs = txReader.getPartitionTimestampByIndex(p);
                        long partitionNameVersion = txReader.getPartitionNameTxn(p);

                        for (int v = 0; v < partitionNameVersion + 5; v++) {
                            path.trimTo(len);
                            TableUtils.setPathForPartition(path, partitionBy, partitionTs, v);
                            path.concat("x.d").$();
                            Assert.assertEquals(Utf8s.toString(path), v == partitionNameVersion, Files.exists(path));
                        }
                    }
                    txReader.clear();
                }
            }
        });
    }

    @Test
    public void testNonAsciiTableName() throws Exception {
        String tableName = "таблица";

        assertMemoryLeak(() -> {
            ddl("create table " + tableName + " as (select x, cast('1970-01-10T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // OOO insert
            insert("insert into " + tableName + " select 4, '1970-01-10T09'");

            // in order insert
            insert("insert into " + tableName + " select 2, '1970-01-10T11'");

            // This should lock partition 1970-01-10.1 from being deleted from disk
            try (TableReader rdr = getReader(tableName)) {
                // OOO insert
                insert("insert into " + tableName + " select 4, '1970-01-10T09'");

                // This should not fail
                rdr.openPartition(0);
            }

            runPartitionPurgeJobs();

            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.concat(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10");
                int len = path.size();
                for (int i = 0; i < 3; i++) {
                    path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                    Assert.assertFalse(Utf8s.toString(path), Files.exists(path));
                }
            }
        });
    }

    @Test
    public void testPartitionDeletedAsyncAfterDroppedBySql() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                ddl("create table tbl as (select x, cast('1970-01-10T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY");

                // OOO inserts partition 1970-01-09
                insert("insert into tbl select 4, '1970-01-09T10'");

                TableToken tableToken = engine.verifyTableName("tbl");
                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-09.0").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path));

                try (TableReader rdr = getReader("tbl")) {
                    // OOO inserts partition 1970-01-09
                    insert("insert into tbl select 4, '1970-01-09T09'");

                    path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-09.2").concat("x.d").$();
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path));

                    try (TableReader rdr2 = getReader("tbl")) {
                        ddl("alter table tbl drop partition list '1970-01-09'", sqlExecutionContext);
                        runPartitionPurgeJobs();

                        // This should not fail
                        rdr2.openPartition(0);
                    }
                    runPartitionPurgeJobs();
                    Assert.assertFalse(Utf8s.toString(path), Files.exists(path));

                    // This should not fail
                    rdr.openPartition(0);
                }
                runPartitionPurgeJobs();

                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-09.0").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path));
            }
        });
    }

    @Test
    public void testPartitionSplitWithReaders() throws Exception {
        assertMemoryLeak(() -> {
            node1.getConfigurationOverrides().setPartitionO3SplitThreshold(100);

            TableToken token;
            try (TableModel tm = new TableModel(configuration, "tbl", PartitionBy.DAY)
                    .col("x", ColumnType.INT).timestamp()) {
                token = createPopulateTable(1, tm, 2000, "2022-02-24T04", 2);
            }

            Path path = Path.getThreadLocal("");

            // This should lock partition 1970-01-10.1 from being deleted from disk
            try (TableReader rdr = getReader("tbl")) {
                // OOO insert
                insert("insert into tbl select 4, '2022-02-24T19'");

                try (TableReader rdr2 = getReader("tbl")) {
                    // in order insert
                    insert("insert into tbl select 2, '2022-02-26T19'");

                    path.of(engine.getConfiguration().getRoot()).concat(token).concat("2022-02-24T185959-687501.1");
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path));

                    // OOO insert
                    insert("insert into tbl select 4, '2022-02-24T19'");

                    runPartitionPurgeJobs();

                    rdr2.openPartition(0);
                }

                runPartitionPurgeJobs();

                // This should not fail
                rdr.openPartition(0);
            }
            runPartitionPurgeJobs();

            path.of(engine.getConfiguration().getRoot()).concat(token).concat("2022-02-24T185959-687501.1");
            Assert.assertFalse(Utf8s.toString(path), Files.exists(path));

            path.of(engine.getConfiguration().getRoot()).concat(token).concat("2022-02-24T185959-687501.3");
            Assert.assertTrue(Utf8s.toString(path), Files.exists(path));
        });
    }

    @Test
    public void testPartitionsNotVacuumedBeforeCommit() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl as (" +
                    "select x, " +
                    "timestamp_sequence('1970-01-01', 10 * 60 * 60 * 1000000L) ts " +
                    "from long_sequence(1)" +
                    ") timestamp(ts) partition by HOUR");

            try (Path path = new Path()) {
                try (TableWriter writer = getWriter("tbl")) {
                    long startTimestamp = Timestamps.HOUR_MICROS + 10;

                    for (int i = 0; i < 10; i++) {
                        TableWriter.Row row = writer.newRow(startTimestamp);
                        row.putLong(0, i + 1);
                        row.append();
                        startTimestamp += Timestamps.HOUR_MICROS;
                    }

                    TableToken tableToken = engine.verifyTableName("tbl");
                    path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-01T01.0").concat("x.d").$();
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path));

                    ddl("vacuum table tbl");
                    runPartitionPurgeJobs();

                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path));

                    writer.commit();
                }
            }
        });
    }

    @Test
    public void testPurgeFailed() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger deleteAttempts = new AtomicInteger();
            ff = new TestFilesFacadeImpl() {
                @Override
                public boolean rmdir(Path name, boolean lazy) {
                    if (Utf8s.endsWithAscii(name, "1970-01-10")) {
                        deleteAttempts.incrementAndGet();
                        return false;
                    }
                    return super.rmdir(name, lazy);
                }
            };

            ddl("create table tbl as (select x, cast('1970-01-10T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // This should lock partition 1970-01-10.1 from being deleted from disk
            try (TableReader ignored = getReader("tbl")) {
                // OOO insert
                insert("insert into tbl select 4, '1970-01-10T09'");
            }

            try (Path path = new Path()) {
                runPartitionPurgeJobs();

                Assert.assertEquals(2, deleteAttempts.get()); // One message from Writer, one from Reader

                TableToken tableToken = engine.verifyTableName("tbl");
                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10.1").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path));
            }
        });
    }

    @Test
    public void testPurgeFailedAndVacuumed() throws Exception {
        runPartitionPurgeJobs();
        assertMemoryLeak(() -> {
            AtomicInteger deleteAttempts = new AtomicInteger();
            ff = new TestFilesFacadeImpl() {
                @Override
                public boolean rmdir(Path name, boolean lazy) {
                    if (Utf8s.endsWithAscii(name, "1970-01-10")) {
                        if (deleteAttempts.incrementAndGet() < 3) {
                            return false;
                        }
                    }
                    return super.rmdir(name, lazy);
                }
            };

            ddl("create table tbl as (select x, cast('1970-01-10T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // This should lock partition 1970-01-10.1 from being deleted from disk
            try (TableReader ignored = getReader("tbl")) {
                // OOO insert
                insert("insert into tbl select 4, '1970-01-10T09'");
            }

            try (Path path = new Path()) {
                runPartitionPurgeJobs();

                Assert.assertEquals(2, deleteAttempts.get()); // One message from Writer, one from Reader

                TableToken tableToken = engine.verifyTableName("tbl");
                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path));

                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10.1").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path));

                // VACUUM SQL should delete partition version 1970-01-10 on attempt 3
                ddl("vacuum partitions tbl");
                runPartitionPurgeJobs();
                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path));
            }
        });
    }

    @Test
    public void testReaderUsesPartition() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl as (select x, cast('1970-01-10T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // OOO insert
            insert("insert into tbl select 4, '1970-01-10T09'");

            // This should lock partition 1970-01-10.1 from being deleted from disk
            try (TableReader rdr = getReader("tbl")) {

                // in order insert
                insert("insert into tbl select 2, '1970-01-10T11'");

                // OOO insert
                insert("insert into tbl select 4, '1970-01-10T09'");

                // This should not fail
                rdr.openPartition(0);
            }
            runPartitionPurgeJobs();

            try (Path path = new Path()) {
                path.concat(engine.getConfiguration().getRoot()).concat("tbl").concat("1970-01-10");
                int len = path.size();
                for (int i = 0; i < 3; i++) {
                    path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                    Assert.assertFalse(Utf8s.toString(path), Files.exists(path));
                }
            }
        });
    }

    @Test
    public void testTableDropAfterPurgeScheduled() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl as (select x, cast('1970-01-10T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // This should lock partition 1970-01-10.1 to not do delete in writer
            try (TableReader ignored = getReader("tbl")) {
                // OOO insert
                insert("insert into tbl select 4, '1970-01-10T09'");
            }

            engine.releaseInactive();
            drop("drop table tbl");

            // Main assert here is that job runs without exceptions
            runPartitionPurgeJobs();
        });
    }

    @Test
    public void testTableWriterDeletePartitionWhenNoReadersOpen() throws Exception {
        String tableName = "tbl";

        assertMemoryLeak(() -> {
            ddl("create table " + tableName + " as (select x, cast('1970-01-10T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY");

            insert("insert into " + tableName +
                    " select 2, '1970-01-11T09' from long_sequence(1) " +
                    "union all " +
                    " select 2, '1970-01-12T09' from long_sequence(1) " +
                    "union all " +
                    " select 2, '1970-01-11T08' from long_sequence(1) " +
                    "union all " +
                    " select 2, '1970-01-10T09' from long_sequence(1) " +
                    "union all " +
                    "select 1, '1970-01-09T09'  from long_sequence(1)");

            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path));

                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10.0").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path));

                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10.1").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path));

                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-11").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path));

                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-11.0").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path));

                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-11.1").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path));
            }
        });
    }

    @Test
    public void testTheOnlyPartitionDeletedAsyncAfterDroppedBySql() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                ddl("create table tbl as (select x, timestamp_sequence('1970-01-09T22', 60*60*1000000L) ts" +
                        " from long_sequence(10)) " +
                        " timestamp(ts) partition by HOUR");

                // Remove middle partition
                TableToken tableToken = engine.verifyTableName("tbl");
                try (TableReader rdr = getReader("tbl")) {
                    try (TableReader rdr2 = getReader("tbl")) {
                        ddl("alter table tbl drop partition list '1970-01-10T00'", sqlExecutionContext);
                        runPartitionPurgeJobs();

                        // This should not fail
                        rdr2.openPartition(0);
                    }
                    runPartitionPurgeJobs();

                    path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10T00").concat("x.d").$();
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path));

                    // This should not fail
                    rdr.openPartition(0);
                }
                runPartitionPurgeJobs();

                // Remove last partition
                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10T00").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path));

                try (TableReader rdr = getReader("tbl")) {
                    try (TableReader rdr2 = getReader("tbl")) {
                        ddl("alter table tbl drop partition list '1970-01-10T07'", sqlExecutionContext);
                        runPartitionPurgeJobs();

                        // This should not fail
                        rdr2.openPartition(0);
                    }
                    runPartitionPurgeJobs();

                    path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10T07").concat("x.d").$();
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path));

                    // This should not fail
                    rdr.openPartition(0);
                }
                runPartitionPurgeJobs();

                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10T07").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path));
            }
        });
    }

    private void runPartitionPurgeJobs() {
        // when reader is returned to pool it remains in open state
        // holding files such that purge fails with access violation
        engine.releaseInactive();
        purgeJob.drain(0);
    }

    private void testManyReadersOpenClosedDense(int start, int increment, int iterations) throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl as (select x, cast('1970-01-10T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY");

            TableReader[] readers = new TableReader[iterations];
            for (int i = 0; i < iterations; i++) {
                TableReader rdr = getReader("tbl");
                readers[i] = rdr;

                // OOO insert
                insert("insert into tbl select 4, '1970-01-10T09'");

                runPartitionPurgeJobs();
            }

            // Unwind readers one by one old to new
            for (int i = start; i >= 0 && i < iterations; i += increment) {
                TableReader reader = readers[i];

                reader.openPartition(0);
                reader.close();

                runPartitionPurgeJobs();
            }

            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName("tbl");
                path.concat(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10");
                int len = path.size();

                Assert.assertFalse(Utf8s.toString(path.concat("x.d")), Files.exists(path));
                for (int i = 0; i < iterations; i++) {
                    path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                    Assert.assertFalse(Utf8s.toString(path), Files.exists(path));
                }

                path.trimTo(len).put(".").put(Integer.toString(iterations)).concat("x.d").$();
                Assert.assertTrue(Files.exists(path));
            }
        });
    }

    private void testManyReadersOpenClosedSparse(int start, int increment, int iterations) throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tbl as (select x, cast('1970-01-10T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY");
            TableReader[] readers = new TableReader[2 * iterations];

            for (int i = 0; i < iterations; i++) {
                TableReader rdr = getReader("tbl");
                readers[2 * i] = rdr;

                // in order insert
                insert("insert into tbl select 2, '1970-01-10T11'");

                runPartitionPurgeJobs();

                TableReader rdr2 = getReader("tbl");
                readers[2 * i + 1] = rdr2;
                // OOO insert
                insert("insert into tbl select 4, '1970-01-10T09'");

                runPartitionPurgeJobs();
            }

            // Unwind readers one by in set order
            for (int i = start; i >= 0 && i < iterations; i += increment) {
                TableReader reader = readers[2 * i];
                reader.openPartition(0);
                reader.close();

                runPartitionPurgeJobs();

                reader = readers[2 * i + 1];
                reader.openPartition(0);
                reader.close();

                runPartitionPurgeJobs();
            }

            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName("tbl");
                path.concat(engine.getConfiguration().getRoot()).concat(tableToken).concat("1970-01-10");
                int len = path.size();

                Assert.assertFalse(Utf8s.toString(path.concat("x.d")), Files.exists(path));
                for (int i = 0; i < 2 * iterations; i++) {
                    path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                    Assert.assertFalse(Utf8s.toString(path), Files.exists(path));
                }

                path.trimTo(len).put(".").put(Integer.toString(2 * iterations)).concat("x.d").$();
                Assert.assertTrue(Files.exists(path));
            }
        });
    }
}
