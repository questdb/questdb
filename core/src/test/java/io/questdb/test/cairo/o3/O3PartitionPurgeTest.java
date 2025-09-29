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

package io.questdb.test.cairo.o3;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TxReader;
import io.questdb.log.Log;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

@RunWith(Parameterized.class)
public class O3PartitionPurgeTest extends AbstractCairoTest {
    private static int SCOREBOARD_FORMAT = 1;
    private static O3PartitionPurgeJob purgeJob;
    private final TestTimestampType timestampType;

    public O3PartitionPurgeTest(int version, TestTimestampType timestampType) throws Exception {
        if (version != SCOREBOARD_FORMAT) {
            SCOREBOARD_FORMAT = version;
            tearDownStatic();
            setUpStatic();
        }
        this.timestampType = timestampType;
    }

    @AfterClass
    public static void end() {
        purgeJob = Misc.free(purgeJob);
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_TXN_SCOREBOARD_FORMAT, SCOREBOARD_FORMAT);
        AbstractCairoTest.setUpStatic();
        purgeJob = new O3PartitionPurgeJob(engine, 1);
    }

    @Parameterized.Parameters(name = "V{0}-{1}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {1, TestTimestampType.MICRO},
                {1, TestTimestampType.NANO},
                {2, TestTimestampType.MICRO},
                {2, TestTimestampType.NANO},
        });
    }

    @Test
    public void test2ReadersUsePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // OOO insert
            execute("insert into tbl select 4, '1970-01-10T09'");

            // This should lock partition 1970-01-10.1 from being deleted from the disk
            try (TableReader rdr = getReader("tbl")) {

                try (TableReader rdr2 = getReader("tbl")) {
                    // in order insert
                    execute("insert into tbl select 2, '1970-01-10T11'");

                    // OOO insert
                    execute("insert into tbl select 4, '1970-01-10T09'");

                    runPartitionPurgeJobs();

                    rdr2.openPartition(0);
                }

                runPartitionPurgeJobs();

                // This should not fail
                rdr.openPartition(0);
            }
            runPartitionPurgeJobs();

            try (Path path = new Path()) {
                path.concat(engine.getConfiguration().getDbRoot()).concat("tbl").concat("1970-01-10");
                int len = path.size();
                for (int i = 0; i < 3; i++) {
                    path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                    Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
                }
            }
        });
    }

    @Test
    public void testAsyncPurgeOnBusyWriter() throws Exception {
        int tableCount = 3;
        assertMemoryLeak(() -> {
            for (int i = 0; i < tableCount; i++) {
                execute("create table tbl" + i + " as (select x, cast('1970-01-01T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");
            }

            final CyclicBarrier barrier = new CyclicBarrier(3);
            AtomicInteger done = new AtomicInteger();
            // Open a reader so that the writer will not delete partitions easily
            ObjList<TableReader> readers = new ObjList<>(tableCount);
            try {
                for (int i = 0; i < tableCount; i++) {
                    readers.add(getReader("tbl" + i));
                }

                Thread writeThread = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int i = 0; i < 32; i++) {
                            for (int j = 0; j < tableCount; j++) {
                                execute("insert into tbl" + j +
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
            } finally {
                Misc.freeObjList(readers);
            }
        });
    }

    @Test
    public void testCheckpointDoesNotBlockPurge() throws Exception {
        Assume.assumeTrue(SCOREBOARD_FORMAT == 2 && Os.type != Os.WINDOWS);

        assertMemoryLeak(() -> {
            try (Path path = new Path()) {

                execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

                path.concat(engine.getConfiguration().getDbRoot()).concat(engine.verifyTableName("tbl")).concat("1970-01-10");
                int len = path.size();

                // OOO insert
                execute("insert into tbl select 4, '1970-01-10T09'");

                // This should lock partition 1970-01-10.1 from being deleted from the disk
                engine.checkpointCreate(sqlExecutionContext);
                runPartitionPurgeJobs();
                testPartitionExist(path, len, true, false, false);

                // OOO insert
                execute("insert into tbl select 2, '1970-01-10T09'");
                runPartitionPurgeJobs();
                testPartitionExist(path, len, true, true, false);

                // OOO insert
                execute("insert into tbl select 4, '1970-01-10T08'");
                runPartitionPurgeJobs();
                testPartitionExist(path, len, true, false, true);

                engine.checkpointRelease();
                runPartitionPurgeJobs();
                testPartitionExist(path, len, false, false, true);
            }
        });
    }

    @Test
    public void testInvalidFolderNames() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // This should lock partition 1970-01-10.1 from being deleted from the disk
            try (TableReader ignored = getReader("tbl")) {
                // OOO insert
                execute("insert into tbl select 4, '1970-01-10T09'");
            }

            TableToken tableToken = engine.verifyTableName("tbl");
            try (Path path = new Path()) {
                Files.mkdir(path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("invalid_folder.123").$(), 509);
                Files.mkdir(path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-01.invalid").$(), 509);

                runPartitionPurgeJobs();

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10.1").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));
            }
        });
    }

    @Test
    public void testLastPartitionDeletedAsyncAfterDroppedBySql() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                execute("create table tbl as (select x, timestamp_sequence('1970-01-10', 60*60*1000000L)::" + timestampType.getTypeName() + " ts from long_sequence(5)) timestamp(ts) partition by HOUR");

                TableToken tableToken = engine.verifyTableName("tbl");
                try (TableReader rdr = getReader("tbl")) {
                    try (TableReader rdr2 = getReader("tbl")) {
                        execute("alter table tbl drop partition where ts >= '1970-01-10T03'", sqlExecutionContext);
                        runPartitionPurgeJobs();

                        // This should not fail
                        rdr2.openPartition(0);
                    }
                    runPartitionPurgeJobs();
                    path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10T03").concat("x.d").$();
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                    // This should not fail
                    rdr.openPartition(0);
                }
                runPartitionPurgeJobs();

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10T03").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10T04").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10T05").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
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
                execute("create table tbl" + i + " as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");
            }

            ObjList<TableReader> readers = new ObjList<>();
            for (int i = 0; i < testIterations; i++) {
                String tableName = "tbl" + rnd.nextInt(tableCount);
                String partition = "1970-0" + (1 + rnd.nextInt(1)) + "-01";

                runPartitionPurgeJobs();

                if (rnd.nextBoolean()) {
                    // deffo OOO insert
                    execute("insert into " + tableName + " select 4, '" + partition + "T09'");
                } else {
                    // in order insert if last partition
                    execute("insert into " + tableName + " select 2, '" + partition + "T11'");
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
                    path.of(engine.getConfiguration().getDbRoot()).concat(tableToken);
                    int len = path.size();
                    int partitionBy = PartitionBy.DAY;
                    txReader.ofRO(path.concat(TXN_FILE_NAME).$(), ColumnType.TIMESTAMP, partitionBy);
                    txReader.unsafeLoadAll();

                    Assert.assertEquals(2, txReader.getPartitionCount());
                    for (int p = 0; p < 2; p++) {
                        long partitionTs = txReader.getPartitionTimestampByIndex(p);
                        long partitionNameVersion = txReader.getPartitionNameTxn(p);

                        for (int v = 0; v < partitionNameVersion + 5; v++) {
                            path.trimTo(len);
                            TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP, partitionBy, partitionTs, v);
                            path.concat("x.d").$();
                            Assert.assertEquals(Utf8s.toString(path), v == partitionNameVersion, Files.exists(path.$()));
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
            execute("create table " + tableName + " as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // OOO insert
            execute("insert into " + tableName + " select 4, '1970-01-10T09'");

            // in order insert
            execute("insert into " + tableName + " select 2, '1970-01-10T11'");

            // This should lock partition 1970-01-10.1 from being deleted from the disk
            try (TableReader rdr = getReader(tableName)) {
                // OOO insert
                execute("insert into " + tableName + " select 4, '1970-01-10T09'");

                // This should not fail
                rdr.openPartition(0);
            }

            runPartitionPurgeJobs();

            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.concat(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10");
                int len = path.size();
                for (int i = 0; i < 3; i++) {
                    path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                    Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
                }
            }
        });
    }

    @Test
    public void testPartitionDeletedAsyncAfterDroppedBySql() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

                // OOO inserts partition 1970-01-09
                execute("insert into tbl select 4, '1970-01-09T10'");

                TableToken tableToken = engine.verifyTableName("tbl");
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-09.0").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                try (TableReader rdr = getReader("tbl")) {
                    // OOO inserts partition 1970-01-09
                    execute("insert into tbl select 4, '1970-01-09T09'");

                    path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-09.2").concat("x.d").$();
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                    try (TableReader rdr2 = getReader("tbl")) {
                        execute("alter table tbl drop partition list '1970-01-09'", sqlExecutionContext);
                        runPartitionPurgeJobs();

                        // This should not fail
                        rdr2.openPartition(0);
                    }
                    runPartitionPurgeJobs();
                    Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));

                    // This should not fail
                    rdr.openPartition(0);
                }
                runPartitionPurgeJobs();

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-09.0").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
            }
        });
    }

    @Test
    public void testPartitionSplitWithReaders() throws Exception {
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 100);

            TableToken token;
            TableModel tm = new TableModel(configuration, "tbl", PartitionBy.DAY)
                    .col("x", ColumnType.INT)
                    .timestamp("timestamp", timestampType.getTimestampType());
            token = createPopulateTable(1, tm, 2000, "2022-02-24T04", 2);

            Path path = Path.getThreadLocal("");

            // This should lock partition 1970-01-10.1 from being deleted from the disk
            try (TableReader rdr = getReader("tbl")) {
                // OOO insert
                execute("insert into tbl select 4, '2022-02-24T19'");

                try (TableReader rdr2 = getReader("tbl")) {
                    // in order insert
                    execute("insert into tbl select 2, '2022-02-26T19'");

                    path.of(engine.getConfiguration().getDbRoot()).concat(token)
                            .concat(timestampType == TestTimestampType.NANO ? "2022-02-24T185959-687500001.1" : "2022-02-24T185959-687501.1");
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                    // OOO insert
                    execute("insert into tbl select 4, '2022-02-24T19'");

                    runPartitionPurgeJobs();

                    rdr2.openPartition(0);
                }

                runPartitionPurgeJobs();

                // This should not fail
                rdr.openPartition(0);
            }
            runPartitionPurgeJobs();

            path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TIMESTAMP_NS_TYPE_NAME.equals(timestampType.getTypeName()) ? "2022-02-24T185959-687500001.1" : "2022-02-24T185959-687501.1");
            Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));

            path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TIMESTAMP_NS_TYPE_NAME.equals(timestampType.getTypeName()) ? "2022-02-24T185959-687500001.3" : "2022-02-24T185959-687501.3");
            Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));
        });
    }

    @Test
    public void testPartitionsNotVacuumedBeforeCommit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl as (" +
                    "select x, " +
                    "timestamp_sequence('1970-01-01', 10 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(1)" +
                    ") timestamp(ts) partition by HOUR");

            try (Path path = new Path()) {
                try (TableWriter writer = getWriter("tbl")) {
                    TimestampDriver driver = ColumnType.getTimestampDriver(writer.getTimestampType());
                    long startTimestamp = driver.fromHours(1) + driver.fromMicros(10);

                    for (int i = 0; i < 10; i++) {
                        TableWriter.Row row = writer.newRow(startTimestamp);
                        row.putLong(0, i + 1);
                        row.append();
                        startTimestamp += driver.fromHours(1);
                    }

                    TableToken tableToken = engine.verifyTableName("tbl");
                    path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-01T01.0").concat("x.d").$();
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                    execute("vacuum table tbl");
                    runPartitionPurgeJobs();

                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

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

            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // This should lock partition 1970-01-10.1 from being deleted from the disk
            try (TableReader ignored = getReader("tbl")) {
                // OOO insert
                execute("insert into tbl select 4, '1970-01-10T09'");
            }

            try (Path path = new Path()) {
                runPartitionPurgeJobs();

                Assert.assertEquals(2, deleteAttempts.get()); // One message from Writer, one from Reader

                TableToken tableToken = engine.verifyTableName("tbl");
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10.1").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));
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

            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // This should lock partition 1970-01-10.1 from being deleted from the disk
            try (TableReader ignored = getReader("tbl")) {
                // OOO insert
                execute("insert into tbl select 4, '1970-01-10T09'");
            }

            try (Path path = new Path()) {
                runPartitionPurgeJobs();

                Assert.assertEquals(2, deleteAttempts.get()); // One message from Writer, one from Reader

                TableToken tableToken = engine.verifyTableName("tbl");
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10.1").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                // VACUUM SQL should delete partition version 1970-01-10 on attempt 3
                execute("vacuum partitions tbl");
                runPartitionPurgeJobs();
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
            }
        });
    }

    @Test
    public void testReaderUsesPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // OOO insert
            execute("insert into tbl select 4, '1970-01-10T09'");

            // This should lock partition 1970-01-10.1 from being deleted from the disk
            try (TableReader rdr = getReader("tbl")) {

                // in order insert
                execute("insert into tbl select 2, '1970-01-10T11'");

                // OOO insert
                execute("insert into tbl select 4, '1970-01-10T09'");

                // This should not fail
                rdr.openPartition(0);
            }
            runPartitionPurgeJobs();

            try (Path path = new Path()) {
                path.concat(engine.getConfiguration().getDbRoot()).concat("tbl").concat("1970-01-10");
                int len = path.size();
                for (int i = 0; i < 3; i++) {
                    path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                    Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
                }
            }
        });
    }

    @Test
    public void testRollbackWithActiveReaders() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, "1970-01-09.3")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            try (Path path = new Path()) {
                execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");
                TableToken tableToken = engine.verifyTableName("tbl");

                // Open a reader to not make partition remove trivial
                try (TableReader ignored = getReader("tbl")) {
                    // In order inserts partition 1970-01-09
                    execute("insert into tbl select 4, '1970-01-09T10'");
                    execute("insert into tbl select 4, '1970-01-09T09:59'");

                    // Simulate a rolled back commit, add a directory with name 1970-01-09.2
                    try {
                        execute("insert into tbl select 4, '1970-01-09T09'");
                        Assert.fail("expected file open error");
                    } catch (CairoException e) {
                        // If the message bus does not have empty slots, error message can vary
                        if (!Chars.contains(e.getFlyweightMessage(), "could not open read-write")) {
                            TestUtils.assertContains(e.getMessage(), "failed and will be rolled back");
                        }
                    }
                    execute("insert into tbl select 4, '1970-01-09T10'");
                    // Close this reader so that the purge job is potentially able to delete the partition version '.0'
                }

                try (TableReader rdr = getReader("tbl")) {

                    // Make in order insert to bump txn number
                    execute("insert into tbl select 4, '1970-01-09T10'");

                    // OOO inserts partition 1970-01-09
                    execute("insert into tbl select 5, '1970-01-09T09'");
                    execute("insert into tbl select 5, '1970-01-09T08'");

                    path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-09.5").concat("x.d").$();
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                    runPartitionPurgeJobs();

                    // This should not fail
                    rdr.openPartition(0);
                }
            }
        });
    }

    @Test
    public void testTableDropAfterPurgeScheduled() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // This should lock partition 1970-01-10.1 to not do delete in the writer
            try (TableReader ignored = getReader("tbl")) {
                // OOO insert
                execute("insert into tbl select 4, '1970-01-10T09'");
            }

            engine.releaseInactive();
            execute("drop table tbl");

            // The main assertion here is that job runs without exceptions
            runPartitionPurgeJobs();
        });
    }

    @Test
    public void testTableRecreatedInBeforePartitionDirDelete() throws Exception {
        //Windows sometimes fails to drop the table on CI, and it's not easily reproducible locally
        Assume.assumeFalse(Os.isWindows());
        AtomicBoolean dropTable = new AtomicBoolean();
        Thread recreateTable = new Thread(() -> {
            try {
                execute("drop table tbl");
                execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");
            } catch (Throwable e) {
                LOG.info().$("Failed to recreate table: ").$(e).$();
            } finally {
                Path.clearThreadLocals();
            }
        });

        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean unlinkOrRemove(Path path, Log LOG) {
                if (dropTable.get() && Utf8s.endsWithAscii(path, "1970-01-10")) {
                    dropTable.set(false);
                    recreateTable.start();
                    Os.sleep(50);
                }
                int checkedType = isSoftLink(path.$()) ? Files.DT_LNK : Files.DT_UNKNOWN;
                return unlinkOrRemove(path, checkedType, LOG);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // This should lock partition 1970-01-10.1 to not do delete in the writer
            try (TableReader rdr = getReader("tbl")) {
                rdr.openPartition(0);

                // ooo insert
                execute("insert into tbl select 4, '1970-01-10T07'");
            }

            dropTable.set(true);
            purgeJob.drain(0);
            recreateTable.join();

            try (TableReader rdr = getReader("tbl")) {
                rdr.openPartition(0);
            }
        });
    }

    @Test
    public void testTableRecreatedViaRenameInBeforePartitionDirDelete() throws Exception {
        // Windows sometimes fails to drop the table on CI, and it's not easily reproducible locally
        Assume.assumeFalse(Os.isWindows());
        AtomicBoolean dropTable = new AtomicBoolean();
        Thread recreateTable = new Thread(() -> {
            try {
                execute("drop table tbl");
                execute("create table tbl1 as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");
                execute("rename table tbl1 to tbl");
            } catch (Throwable e) {
                LOG.info().$("Failed to recreate table: ").$(e).$();
            } finally {
                Path.clearThreadLocals();
            }
        });

        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean unlinkOrRemove(Path path, Log LOG) {
                if (dropTable.get() && Utf8s.endsWithAscii(path, "1970-01-10")) {
                    recreateTable.start();
                    Os.sleep(50);
                }
                int checkedType = isSoftLink(path.$()) ? Files.DT_LNK : Files.DT_UNKNOWN;
                return unlinkOrRemove(path, checkedType, LOG);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // This should lock partition 1970-01-10.1 to not do delete in the writer
            try (TableReader rdr = getReader("tbl")) {
                rdr.openPartition(0);

                // ooo insert
                execute("insert into tbl select 4, '1970-01-10T07'");
            }

            dropTable.set(true);
            purgeJob.drain(0);
            dropTable.set(false);
            recreateTable.join();

            try (TableReader rdr = getReader("tbl")) {
                rdr.openPartition(0);
            }
        });
    }

    @Test
    public void testTableWriterDeletePartitionWhenNoReadersOpen() throws Exception {
        String tableName = "tbl";

        assertMemoryLeak(() -> {
            execute("create table " + tableName + " as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            execute("insert into " + tableName +
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
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10.0").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10.1").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-11").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-11.0").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-11.1").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));
            }
        });
    }

    @Test
    public void testTheOnlyPartitionDeletedAsyncAfterDroppedBySql() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                execute("create table tbl as (select x, timestamp_sequence('1970-01-09T22', 60*60*1000000L)::" + timestampType.getTypeName() + " ts" +
                        " from long_sequence(10)) " +
                        " timestamp(ts) partition by HOUR");

                // Remove middle partition
                TableToken tableToken = engine.verifyTableName("tbl");
                try (TableReader rdr = getReader("tbl")) {
                    try (TableReader rdr2 = getReader("tbl")) {
                        execute("alter table tbl drop partition list '1970-01-10T00'", sqlExecutionContext);
                        runPartitionPurgeJobs();

                        // This should not fail
                        rdr2.openPartition(0);
                    }
                    runPartitionPurgeJobs();

                    path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10T00").concat("x.d").$();
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                    // This should not fail
                    rdr.openPartition(0);
                }
                runPartitionPurgeJobs();

                // Remove last partition
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10T00").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));

                try (TableReader rdr = getReader("tbl")) {
                    try (TableReader rdr2 = getReader("tbl")) {
                        execute("alter table tbl drop partition list '1970-01-10T07'", sqlExecutionContext);
                        runPartitionPurgeJobs();

                        // This should not fail
                        rdr2.openPartition(0);
                    }
                    runPartitionPurgeJobs();

                    path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10T07").concat("x.d").$();
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                    // This should not fail
                    rdr.openPartition(0);
                }
                runPartitionPurgeJobs();

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10T07").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
            }
        });
    }

    private static void testPartitionExist(Path path, int len, boolean... existence) {
        for (int i = 0, n = existence.length; i < n; i++) {
            path.trimTo(len).put(".").put(Integer.toString(i + 1)).concat("x.d").$();
            Assert.assertEquals(Utf8s.toString(path), existence[i], Files.exists(path.$()));
        }
    }

    private void runPartitionPurgeJobs() {
        // when the reader is returned to pool, it remains in open state
        // holding files such that purge fails with access violation
        engine.releaseInactive();
        purgeJob.drain(0);
    }

    private void testManyReadersOpenClosedDense(int start, int increment, int iterations) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            TableReader[] readers = new TableReader[iterations];
            try {
                for (int i = 0; i < iterations; i++) {
                    TableReader rdr = getReader("tbl");
                    readers[i] = rdr;

                    // OOO insert
                    execute("insert into tbl select 4, '1970-01-10T09'");

                    runPartitionPurgeJobs();
                }

                // Unwind readers one by one old to new
                for (int i = start; i >= 0 && i < iterations; i += increment) {
                    TableReader reader = readers[i];

                    reader.openPartition(0);
                    reader.close();
                    readers[i] = null;

                    runPartitionPurgeJobs();
                }

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName("tbl");
                    path.concat(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10");
                    int len = path.size();

                    Assert.assertFalse(Utf8s.toString(path.concat("x.d")), Files.exists(path.$()));
                    for (int i = 0; i < iterations; i++) {
                        path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                        Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
                    }

                    path.trimTo(len).put(".").put(Integer.toString(iterations)).concat("x.d").$();
                    Assert.assertTrue(Files.exists(path.$()));
                }
            } finally {
                Misc.free(readers);
            }
        });
    }

    private void testManyReadersOpenClosedSparse(int start, int increment, int iterations) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");
            TableReader[] readers = new TableReader[2 * iterations];

            try {
                for (int i = 0; i < iterations; i++) {
                    TableReader rdr = getReader("tbl");
                    readers[2 * i] = rdr;

                    // in order insert
                    execute("insert into tbl select 2, '1970-01-10T11'");

                    runPartitionPurgeJobs();

                    TableReader rdr2 = getReader("tbl");
                    readers[2 * i + 1] = rdr2;
                    // OOO insert
                    execute("insert into tbl select 4, '1970-01-10T09'");

                    runPartitionPurgeJobs();
                }

                // Unwind readers one by in set order
                for (int i = start; i >= 0 && i < iterations; i += increment) {
                    TableReader reader = readers[2 * i];
                    reader.openPartition(0);
                    reader.close();
                    readers[2 * i] = null;

                    runPartitionPurgeJobs();

                    reader = readers[2 * i + 1];
                    reader.openPartition(0);
                    reader.close();
                    readers[2 * i + 1] = null;

                    runPartitionPurgeJobs();
                }

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName("tbl");
                    path.concat(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10");
                    int len = path.size();

                    Assert.assertFalse(Utf8s.toString(path.concat("x.d")), Files.exists(path.$()));
                    for (int i = 0; i < 2 * iterations; i++) {
                        path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                        Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
                    }

                    path.trimTo(len).put(".").put(Integer.toString(2 * iterations)).concat("x.d").$();
                    Assert.assertTrue(Files.exists(path.$()));
                }
            } finally {
                Misc.free(readers);
            }
        });
    }
}
