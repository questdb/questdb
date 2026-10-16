/*+*****************************************************************************
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

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.idx.BitmapIndexUtils;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.griffin.SqlException;
import io.questdb.mp.Sequence;
import io.questdb.std.Files;
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.ColumnPurgeTask;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(Parameterized.class)
public class ColumnPurgeJobTest extends AbstractCairoTest {
    // the rows of y that insertRowsOfIndexReAddedAtTxnZero() writes, as ts, s, d, vc, l
    private static final String ROWS_OF_INDEX_RE_ADDED_AT_TXN_ZERO = """
            2024-01-01T01:00:00.000000Z\ty\t1.5\ta1\t10
            2024-01-01T02:00:00.000000Z\ty\tnull\t\tnull
            2024-01-01T04:00:00.000000Z\ty\t4.5\t\u00e94\t40
            """;
    private static final String THIRD_ROW_OF_INDEX_RE_ADDED_AT_TXN_ZERO = "(5, '2024-01-01T05:00:00.000000Z', 'y', 5.5, 'a5', 50)";
    private final TestTimestampType timestampType;
    private int iteration = 1;

    public ColumnPurgeJobTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Before
    public void setUpUpdates() {
        iteration = 1;
        setCurrentMicros(0);
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
    }

    @Test
    public void testConvertColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("create table up_part_o3_many as" +
                        " (select timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY");

                try (TableReader rdr1 = getReader("up_part_o3_many")) {
                    execute("insert into up_part_o3_many " +
                            " select timestamp_sequence('1970-01-02T01', 24 * 60 * 60 * 1000000L) ts," +
                            " x," +
                            " rnd_str('a', 'b', 'c', 'd') str," +
                            " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                            " rnd_symbol('1', '2', '3', '4') sym2" +
                            " from long_sequence(3)");

                    try (TableReader ignored = getReader("up_part_o3_many")) {
                        update("ALTER TABLE up_part_o3_many alter column sym2 type varchar");
                        runPurgeJob(purgeJob);

                        setCurrentMicros(currentMicros + 1);
                    }
                    rdr1.openPartition(0);
                }

                String[] partitions = new String[]{"1970-01-03.1", "1970-01-04.1", "1970-01-05"};
                try (Path path = new Path()) {
                    assertIndexFilesExist(partitions, path, "", true);

                    runPurgeJob(purgeJob);

                    assertIndexFilesExist(partitions, path, "", false);
                }
            }
        });
    }

    @Test
    public void testDropIndex() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("create table up_part_o3_many as" +
                        " (select timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY");

                try (TableReader rdr1 = getReader("up_part_o3_many")) {
                    execute("insert into up_part_o3_many " +
                            " select timestamp_sequence('1970-01-02T01', 24 * 60 * 60 * 1000000L) ts," +
                            " x," +
                            " rnd_str('a', 'b', 'c', 'd') str," +
                            " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                            " rnd_symbol('1', '2', '3', '4') sym2" +
                            " from long_sequence(3)");

                    try (TableReader ignored = getReader("up_part_o3_many")) {
                        update("ALTER TABLE up_part_o3_many alter column sym2 drop index");
                        runPurgeJob(purgeJob);

                        setCurrentMicros(currentMicros + 1);
                    }
                    rdr1.openPartition(0);
                }

                String[] partitions = new String[]{"1970-01-03.1", "1970-01-04.1", "1970-01-05"};
                try (Path path = new Path()) {
                    assertIndexFilesExist(partitions, path, "", true);

                    runPurgeJob(purgeJob);

                    assertIndexFilesExist(partitions, path, "", false);
                }

                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());

                try (TableReader rdr1 = getReader("up_part_o3_many")) {
                    try (TableReader ignored = getReader("up_part_o3_many")) {
                        update("ALTER TABLE up_part_o3_many alter column sym2 add index");
                        runPurgeJob(purgeJob);

                        try (TableReader ignored2 = getReader("up_part_o3_many")) {
                            update("ALTER TABLE up_part_o3_many alter column sym2 drop index");
                            runPurgeJob(purgeJob);
                        }
                        setCurrentMicros(currentMicros + 1);
                    }
                    rdr1.openPartition(0);
                }

                try (Path path = new Path()) {
                    assertIndexFilesExist(partitions, path, ".2", true);

                    runPurgeJob(purgeJob);

                    assertIndexFilesExist(partitions, path, ".2", false);
                }

                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testHandlesDroppedTablesAfterRestart() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                createTable("up_part_o3");
                createTable("up_part_o3_2");

                drainWalQueue();
                try (TableReader rdr = getReader("up_part_o3")) {
                    try (TableReader rdr2 = getReader("up_part_o3_2")) {
                        update("UPDATE up_part_o3 SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-03'");
                        update("UPDATE up_part_o3_2 SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-03'");
                        drainWalQueue();

                        execute("drop table up_part_o3");

                        runPurgeJob(purgeJob);
                        rdr.openPartition(0);
                        rdr2.openPartition(0);
                        runPurgeJob(purgeJob);
                    }
                }
            }


            String purgeLogTableName;
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                purgeLogTableName = purgeJob.getLogTableName();
            }

            assertQuery("up_part_o3_2")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix("""
                            ts\tx\tstr\tsym1\tsym2
                            1970-01-01T02:00:00.000000Z\t1\ta\tA\t2
                            1970-01-02T02:00:00.000000Z\t2\tb\tC\t4
                            1970-01-03T02:00:00.000000Z\t100\tabcd\tA\tEE
                            1970-01-04T02:00:00.000000Z\t100\tabcd\tA\tEE
                            1970-01-05T02:00:00.000000Z\t100\tabcd\tD\tEE
                            """, timestampType.getTypeName()));

            // cleaned everything, table is truncated
            assertQuery(purgeLogTableName)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n");

            // Check logging is ok. This test reproduces logging failure because of exception in the middle of logging.
            // The result can be that this loop never finishes.
            for (int i = 0; i < 1025; i++) {
                LOG.infoW().$("test").$();
            }
        });
    }

    @Test
    public void testManyUpdatesInserts() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("create table up_part_o3_many as" +
                        " (select timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY");

                try (TableReader rdr1 = getReader("up_part_o3_many")) {
                    execute("insert into up_part_o3_many " +
                            " select timestamp_sequence('1970-01-02T01', 24 * 60 * 60 * 1000000L) ts," +
                            " x," +
                            " rnd_str('a', 'b', 'c', 'd') str," +
                            " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                            " rnd_symbol('1', '2', '3', '4') sym2" +
                            " from long_sequence(3)");

                    try (TableReader rdr2 = getReader("up_part_o3_many")) {
                        update("UPDATE up_part_o3_many SET x = 100, str='u1', sym2='EE' WHERE ts >= '1970-01-03'");
                        runPurgeJob(purgeJob);

                        setCurrentMicros(currentMicros + 1);
                        try (TableReader rdr3 = getReader("up_part_o3_many")) {
                            update("UPDATE up_part_o3_many SET x = 200, str='u2', sym2='EE' WHERE x = 100");
                            runPurgeJob(purgeJob);
                            rdr3.openPartition(0);
                        }

                        rdr2.openPartition(0);
                    }
                    rdr1.openPartition(0);
                }

                try (Path path = new Path()) {
                    String[] partitions = new String[]{"1970-01-03.1", "1970-01-04.1", "1970-01-05"};
                    assertFilesExist(partitions, path, "up_part_o3_many", "", true);
                    assertFilesExist(partitions, path, "up_part_o3_many", ".2", true);
                    assertFilesExist(partitions, path, "up_part_o3_many", ".3", true);

                    runPurgeJob(purgeJob);

                    assertFilesExist(partitions, path, "up_part_o3_many", "", false);
                    assertFilesExist(partitions, path, "up_part_o3_many", ".2", false);
                    assertFilesExist(partitions, path, "up_part_o3_many", ".3", true);
                }

                assertQuery("up_part_o3_many")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns(replaceTimestampSuffix("""
                                ts\tx\tstr\tsym1\tsym2
                                1970-01-01T02:00:00.000000Z\t1\ta\tC\t2
                                1970-01-02T01:00:00.000000Z\t1\ta\tA\t2
                                1970-01-02T02:00:00.000000Z\t2\td\tB\t4
                                1970-01-03T01:00:00.000000Z\t200\tu2\tC\tEE
                                1970-01-03T02:00:00.000000Z\t200\tu2\tD\tEE
                                1970-01-04T01:00:00.000000Z\t200\tu2\tA\tEE
                                1970-01-04T02:00:00.000000Z\t200\tu2\tA\tEE
                                1970-01-05T02:00:00.000000Z\t200\tu2\tD\tEE
                                """, timestampType.getTypeName()));

                assertQuery(purgeJob.getLogTableName())
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted
                                1970-01-01T00:00:00.000010Z\tup_part_o3_many~\tx\t2\t0\t6\t0\t3\t-1\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z
                                1970-01-01T00:00:00.000010Z\tup_part_o3_many~\tx\t2\t0\t6\t0\t3\t-1\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z
                                1970-01-01T00:00:00.000010Z\tup_part_o3_many~\tx\t2\t0\t6\t0\t3\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000151Z
                                1970-01-01T00:00:00.000011Z\tup_part_o3_many~\tstr\t2\t0\t11\t0\t3\t-1\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z
                                1970-01-01T00:00:00.000011Z\tup_part_o3_many~\tstr\t2\t0\t11\t0\t3\t-1\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z
                                1970-01-01T00:00:00.000011Z\tup_part_o3_many~\tstr\t2\t0\t11\t0\t3\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000151Z
                                1970-01-01T00:00:00.000012Z\tup_part_o3_many~\tsym2\t2\t0\t12\t0\t3\t-1\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z
                                1970-01-01T00:00:00.000012Z\tup_part_o3_many~\tsym2\t2\t0\t12\t0\t3\t-1\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z
                                1970-01-01T00:00:00.000012Z\tup_part_o3_many~\tsym2\t2\t0\t12\t0\t3\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000151Z
                                1970-01-01T00:00:00.000061Z\tup_part_o3_many~\tx\t2\t0\t6\t0\t4\t2\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z
                                1970-01-01T00:00:00.000061Z\tup_part_o3_many~\tx\t2\t0\t6\t0\t4\t2\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z
                                1970-01-01T00:00:00.000061Z\tup_part_o3_many~\tx\t2\t0\t6\t0\t4\t2\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000151Z
                                1970-01-01T00:00:00.000062Z\tup_part_o3_many~\tstr\t2\t0\t11\t0\t4\t2\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z
                                1970-01-01T00:00:00.000062Z\tup_part_o3_many~\tstr\t2\t0\t11\t0\t4\t2\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z
                                1970-01-01T00:00:00.000062Z\tup_part_o3_many~\tstr\t2\t0\t11\t0\t4\t2\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000151Z
                                1970-01-01T00:00:00.000063Z\tup_part_o3_many~\tsym2\t2\t0\t12\t0\t4\t2\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z
                                1970-01-01T00:00:00.000063Z\tup_part_o3_many~\tsym2\t2\t0\t12\t0\t4\t2\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z
                                1970-01-01T00:00:00.000063Z\tup_part_o3_many~\tsym2\t2\t0\t12\t0\t4\t2\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000151Z
                                """);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testPurge() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                TableToken tn1 = new TableToken("tbl_name", "tbl_name", null, 123, false, false, false);
                ColumnPurgeTask task = createTask(tn1, "col", 1, ColumnType.INT, 43, 11, "2022-03-29", -1);
                task.appendColumnInfo(-1, timestampType.getDriver().parseFloorLiteral("2022-04-05"), 2);
                appendTaskToQueue(task);

                TableToken tn2 = new TableToken("tbl_name2", "tbl_name2", null, 123, false, false, false);
                ColumnPurgeTask task2 = createTask(tn2, "col2", 2, ColumnType.SYMBOL, 33, -1, "2022-02-13", 3);
                appendTaskToQueue(task2);

                purgeJob.run();
                assertQuery(purgeJob.getLogTableName())
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted
                                1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t11\t2022-03-29T00:00:00.000000Z\t-1\t
                                1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t-1\t2022-04-05T00:00:00.000000Z\t2\t
                                1970-01-01T00:00:00.000001Z\ttbl_name2\tcol2\t2\t0\t12\t3\t33\t-1\t2022-02-13T00:00:00.000000Z\t3\t
                                """);

                runPurgeJob(purgeJob);
                assertQuery(purgeJob.getLogTableName())
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted
                                1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t11\t2022-03-29T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000010Z
                                1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t-1\t2022-04-05T00:00:00.000000Z\t2\t1970-01-01T00:00:00.000010Z
                                1970-01-01T00:00:00.000001Z\ttbl_name2\tcol2\t2\t0\t12\t3\t33\t-1\t2022-02-13T00:00:00.000000Z\t3\t1970-01-01T00:00:00.000010Z
                                """);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testPurgeCannotAllocateFailure() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            ff = new TestFilesFacadeImpl() {
                @Override
                public boolean allocate(long fd, long size) {
                    if (this.fd == fd) {
                        throw new RuntimeException("TEST ERROR");
                    }
                    return super.allocate(fd, size);
                }

                public long openRW(LPSZ name, int opts) {
                    long fd = super.openRW(name, opts);
                    if (Utf8s.endsWithAscii(name, "completed.d")) {
                        this.fd = fd;
                    }
                    return fd;
                }
            };

            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("create table up_part as" +
                        " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY");

                try (TableReader ignored = getReader("up_part")) {
                    update("UPDATE up_part SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-02'");
                    // cannot purge column versions because of active reader
                    runPurgeJob(purgeJob);
                }

                try (Path path = new Path()) {
                    String[] partitions = new String[]{"1970-01-02", "1970-01-03", "1970-01-04", "1970-01-05"};
                    assertFilesExist(partitions, path, "up_part", "", true);

                    // reader has been closed, now it can purge but FilesFacade will throw error and purge log table will not be populated
                    runPurgeJob(purgeJob);
                    assertFilesExist(partitions, path, "up_part", "", false);
                }

                assertQuery("up_part")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns(replaceTimestampSuffix("""
                                ts\tx\tstr\tsym1\tsym2
                                1970-01-01T00:00:00.000000Z\t1\ta\tC\t2
                                1970-01-02T00:00:00.000000Z\t100\tabcd\tB\tEE
                                1970-01-03T00:00:00.000000Z\t100\tabcd\tD\tEE
                                1970-01-04T00:00:00.000000Z\t100\tabcd\tA\tEE
                                1970-01-05T00:00:00.000000Z\t100\tabcd\tD\tEE
                                """, timestampType.getTypeName()));

                assertQuery(purgeJob.getLogTableName())
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n");
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testPurgeHandlesLogPartitionChange() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(Micros.DAY_MICROS * 30);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("create table up_part_o3 as" +
                        " (select timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY");

                execute("insert into up_part_o3 " +
                        " select timestamp_sequence('1970-01-02T01', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(3)");

                try (TableReader rdr = getReader("up_part_o3")) {
                    update("UPDATE up_part_o3 SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-03'");

                    runPurgeJob(purgeJob);
                    rdr.openPartition(0);
                }
            }

            setCurrentMicros(Micros.DAY_MICROS * 32);
            try (Path path = new Path()) {
                String[] partitions = new String[]{"1970-01-03.1", "1970-01-04.1", "1970-01-05"};
                assertFilesExist(partitions, path, "up_part_o3", "", true);

                try (ColumnPurgeJob purgeJob = createPurgeJob()) {

                    try (TableReader ignored = getReader("up_part_o3")) {
                        update("UPDATE up_part_o3 SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-03'");
                    }

                    runPurgeJob(purgeJob);
                    // Need a second run, first will only re-schedule outstanding tasks
                    runPurgeJob(purgeJob);

                    assertFilesExist(partitions, path, "up_part_o3", "", false);

                    assertQuery("up_part_o3")
                            .noLeakCheck()
                            .expectSize()
                            .timestamp("ts")
                            .returns(replaceTimestampSuffix("""
                                    ts\tx\tstr\tsym1\tsym2
                                    1970-01-01T02:00:00.000000Z\t1\ta\tC\t2
                                    1970-01-02T01:00:00.000000Z\t1\ta\tA\t2
                                    1970-01-02T02:00:00.000000Z\t2\td\tB\t4
                                    1970-01-03T01:00:00.000000Z\t100\tabcd\tC\tEE
                                    1970-01-03T02:00:00.000000Z\t100\tabcd\tD\tEE
                                    1970-01-04T01:00:00.000000Z\t100\tabcd\tA\tEE
                                    1970-01-04T02:00:00.000000Z\t100\tabcd\tA\tEE
                                    1970-01-05T02:00:00.000000Z\t100\tabcd\tD\tEE
                                    """, timestampType.getTypeName()));

                    assertQuery(purgeJob.getLogTableName())
                            .noLeakCheck()
                            .expectSize()
                            .timestamp("ts")
                            .returns("""
                                    ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted
                                    1970-02-02T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t4\t2\t1970-01-03T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z
                                    1970-02-02T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t4\t2\t1970-01-04T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z
                                    1970-02-02T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t4\t2\t1970-01-05T00:00:00.000000Z\t-1\t1970-02-02T00:00:00.000070Z
                                    1970-02-02T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t4\t2\t1970-01-03T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z
                                    1970-02-02T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t4\t2\t1970-01-04T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z
                                    1970-02-02T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t4\t2\t1970-01-05T00:00:00.000000Z\t-1\t1970-02-02T00:00:00.000070Z
                                    1970-02-02T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t4\t2\t1970-01-03T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z
                                    1970-02-02T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t4\t2\t1970-01-04T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z
                                    1970-02-02T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t4\t2\t1970-01-05T00:00:00.000000Z\t-1\t1970-02-02T00:00:00.000070Z
                                    """);
                    Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                }
            }
        });
    }

    @Test
    public void testPurgeIOFailureRetried() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            ff = new TestFilesFacadeImpl() {
                int count = 0;

                @Override
                public boolean removeQuiet(LPSZ name) {
                    if (Utf8s.endsWithAscii(name, "str.i")) {
                        if (count++ < 6) {
                            return false;
                        }
                    }
                    return super.removeQuiet(name);
                }
            };

            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("create table up_part as" +
                        " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY");

                try (TableReader rdr = getReader("up_part")) {
                    update("UPDATE up_part SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-02'");

                    runPurgeJob(purgeJob);
                    rdr.openPartition(0);
                }

                try (Path path = new Path()) {
                    String[] partitions = new String[]{"1970-01-02", "1970-01-03", "1970-01-04", "1970-01-05"};
                    assertFilesExist(partitions, path, "up_part", "", true);

                    runPurgeJob(purgeJob);
                    // Delete failure
                    TableToken tableToken = engine.verifyTableName("up_part");
                    path.of(configuration.getDbRoot()).concat(tableToken).concat("1970-01-02").concat("str.i").$();
                    Assert.assertTrue(Utf8s.toString(path), TestFilesFacadeImpl.INSTANCE.exists(path.$()));

                    // Should retry
                    runPurgeJob(purgeJob);
                    assertFilesExist(partitions, path, "up_part", "", false);
                }

                assertQuery("up_part")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns(replaceTimestampSuffix("""
                                ts\tx\tstr\tsym1\tsym2
                                1970-01-01T00:00:00.000000Z\t1\ta\tC\t2
                                1970-01-02T00:00:00.000000Z\t100\tabcd\tB\tEE
                                1970-01-03T00:00:00.000000Z\t100\tabcd\tD\tEE
                                1970-01-04T00:00:00.000000Z\t100\tabcd\tA\tEE
                                1970-01-05T00:00:00.000000Z\t100\tabcd\tD\tEE
                                """, timestampType.getTypeName()));

                assertQuery(purgeJob.getLogTableName())
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted
                                1970-01-01T00:00:00.000010Z\tup_part~\tx\t2\t0\t6\t0\t2\t-1\t1970-01-02T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000010Z\tup_part~\tx\t2\t0\t6\t0\t2\t-1\t1970-01-03T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000010Z\tup_part~\tx\t2\t0\t6\t0\t2\t-1\t1970-01-04T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000010Z\tup_part~\tx\t2\t0\t6\t0\t2\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000011Z\tup_part~\tstr\t2\t0\t11\t0\t2\t-1\t1970-01-02T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000150Z
                                1970-01-01T00:00:00.000011Z\tup_part~\tstr\t2\t0\t11\t0\t2\t-1\t1970-01-03T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000150Z
                                1970-01-01T00:00:00.000011Z\tup_part~\tstr\t2\t0\t11\t0\t2\t-1\t1970-01-04T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000150Z
                                1970-01-01T00:00:00.000011Z\tup_part~\tstr\t2\t0\t11\t0\t2\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000150Z
                                1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-02T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-03T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-04T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                """);

                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testPurgeLimitsTaskLoadOnRestart() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("create table up_part_o3 as" +
                        " (select timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY");

                execute("insert into up_part_o3 " +
                        " select timestamp_sequence('1970-01-02T01', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(3)");

                try (TableReader rdr = getReader("up_part_o3")) {
                    update("UPDATE up_part_o3 SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-03'");

                    runPurgeJob(purgeJob);
                    rdr.openPartition(0);
                }
            }

            try (Path path = new Path()) {
                String[] partitions = new String[]{"1970-01-03.1", "1970-01-04.1", "1970-01-05"};
                assertFilesExist(partitions, path, "up_part_o3", "", true);
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {

                    assertFilesExist(partitions, path, "up_part_o3", "", false);
                    assertQuery("up_part_o3")
                            .noLeakCheck()
                            .expectSize()
                            .timestamp("ts")
                            .returns(replaceTimestampSuffix("""
                                    ts\tx\tstr\tsym1\tsym2
                                    1970-01-01T02:00:00.000000Z\t1\ta\tC\t2
                                    1970-01-02T01:00:00.000000Z\t1\ta\tA\t2
                                    1970-01-02T02:00:00.000000Z\t2\td\tB\t4
                                    1970-01-03T01:00:00.000000Z\t100\tabcd\tC\tEE
                                    1970-01-03T02:00:00.000000Z\t100\tabcd\tD\tEE
                                    1970-01-04T01:00:00.000000Z\t100\tabcd\tA\tEE
                                    1970-01-04T02:00:00.000000Z\t100\tabcd\tA\tEE
                                    1970-01-05T02:00:00.000000Z\t100\tabcd\tD\tEE
                                    """, timestampType.getTypeName()));

                    assertQuery(purgeJob.getLogTableName())
                            .noLeakCheck()
                            .timestamp("ts")
                            .returns("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n");
                    Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                }
            }
        });
    }

    @Test
    public void testPurgeRespectsOpenReaderDailyPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("create table up_part as" +
                        " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY");

                try (TableReader rdr = getReader("up_part")) {
                    update("UPDATE up_part SET x = 100, str='abcd',SYM2='EE' WHERE ts >= '1970-01-02'");

                    runPurgeJob(purgeJob);
                    rdr.openPartition(0);
                }

                try (Path path = new Path()) {
                    String[] partitions = new String[]{"1970-01-02", "1970-01-03", "1970-01-04", "1970-01-05"};
                    assertFilesExist(partitions, path, "up_part", "", true);

                    runPurgeJob(purgeJob);
                    assertFilesExist(partitions, path, "up_part", "", false);
                }

                assertQuery("up_part")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns(replaceTimestampSuffix("""
                                ts\tx\tstr\tsym1\tsym2
                                1970-01-01T00:00:00.000000Z\t1\ta\tC\t2
                                1970-01-02T00:00:00.000000Z\t100\tabcd\tB\tEE
                                1970-01-03T00:00:00.000000Z\t100\tabcd\tD\tEE
                                1970-01-04T00:00:00.000000Z\t100\tabcd\tA\tEE
                                1970-01-05T00:00:00.000000Z\t100\tabcd\tD\tEE
                                """, timestampType.getTypeName()));

                assertQuery(purgeJob.getLogTableName())
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted
                                1970-01-01T00:00:00.000010Z\tup_part~\tx\t2\t0\t6\t0\t2\t-1\t1970-01-02T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000010Z\tup_part~\tx\t2\t0\t6\t0\t2\t-1\t1970-01-03T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000010Z\tup_part~\tx\t2\t0\t6\t0\t2\t-1\t1970-01-04T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000010Z\tup_part~\tx\t2\t0\t6\t0\t2\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000011Z\tup_part~\tstr\t2\t0\t11\t0\t2\t-1\t1970-01-02T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000011Z\tup_part~\tstr\t2\t0\t11\t0\t2\t-1\t1970-01-03T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000011Z\tup_part~\tstr\t2\t0\t11\t0\t2\t-1\t1970-01-04T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000011Z\tup_part~\tstr\t2\t0\t11\t0\t2\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-02T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-03T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-04T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                """);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testPurgeRespectsOpenReaderNonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("create table up as" +
                        " (select timestamp_sequence(0, 1000000)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts)");

                try (TableReader rdr = getReader("up")) {
                    update("UPDATE up SET x = 100, str='abcd', sym2='EE'");

                    runPurgeJob(purgeJob);
                    rdr.openPartition(0);
                }

                try (Path path = new Path()) {
                    assertFilesExist(path, "up", "default", "", true);
                    runPurgeJob(purgeJob);
                    assertFilesExist(path, "up", "default", "", false);
                }

                assertQuery("up")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns(replaceTimestampSuffix("""
                                ts\tx\tstr\tsym1\tsym2
                                1970-01-01T00:00:00.000000Z\t100\tabcd\tC\tEE
                                1970-01-01T00:00:01.000000Z\t100\tabcd\tB\tEE
                                1970-01-01T00:00:02.000000Z\t100\tabcd\tD\tEE
                                1970-01-01T00:00:03.000000Z\t100\tabcd\tA\tEE
                                1970-01-01T00:00:04.000000Z\t100\tabcd\tD\tEE
                                """, timestampType.getTypeName()));

                assertQuery(purgeJob.getLogTableName())
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted
                                1970-01-01T00:00:00.000010Z\tup~\tx\t2\t0\t6\t3\t2\t-1\t1970-01-01T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000011Z\tup~\tstr\t2\t0\t11\t3\t2\t-1\t1970-01-01T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000012Z\tup~\tsym2\t2\t0\t12\t3\t2\t-1\t1970-01-01T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                """);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testPurgeRespectsTableRecreate() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("create table up as" +
                        " (select timestamp_sequence(0, 1000000)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts)");

                try (TableReader rdr = getReader("up")) {
                    update("UPDATE up SET x = 100, str='abcd'");

                    runPurgeJob(purgeJob);
                    rdr.openPartition(0);
                }
                engine.releaseInactive();

                execute("drop table up");

                execute("create table up as" +
                        " (select timestamp_sequence(0, 1000000)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts)");

                runPurgeJob(purgeJob);

                assertQuery("up")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns(replaceTimestampSuffix("""
                                ts\tx\tstr\tsym1\tsym2
                                1970-01-01T00:00:00.000000Z\t1\ta\tA\t2
                                1970-01-01T00:00:01.000000Z\t2\tb\tC\t4
                                1970-01-01T00:00:02.000000Z\t3\td\tA\t2
                                1970-01-01T00:00:03.000000Z\t4\td\tA\t3
                                1970-01-01T00:00:04.000000Z\t5\ta\tD\t1
                                """, timestampType.getTypeName()));
            }
        });
    }

    @Test
    public void testPurgeRespectsTableTruncates() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("create table testPurgeRespectsTableTruncates as" +
                        " (select timestamp_sequence(0, 1000000)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY");

                try (TableReader rdr = getReader("testPurgeRespectsTableTruncates")) {
                    update("UPDATE testPurgeRespectsTableTruncates SET x = 100, str='abcd'");

                    runPurgeJob(purgeJob);
                    rdr.openPartition(0);
                }
                engine.releaseInactive();

                execute("truncate table testPurgeRespectsTableTruncates");

                execute("insert into testPurgeRespectsTableTruncates " +
                        " select timestamp_sequence(0, 1000000) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)");

                runPurgeJob(purgeJob);

                assertQuery("testPurgeRespectsTableTruncates")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns(replaceTimestampSuffix("""
                                ts\tx\tstr\tsym1\tsym2
                                1970-01-01T00:00:00.000000Z\t1\ta\tA\t2
                                1970-01-01T00:00:01.000000Z\t2\tb\tC\t4
                                1970-01-01T00:00:02.000000Z\t3\td\tA\t2
                                1970-01-01T00:00:03.000000Z\t4\td\tA\t3
                                1970-01-01T00:00:04.000000Z\t5\ta\tD\t1
                                """, timestampType.getTypeName()));
            }
        });
    }

    @Test
    public void testPurgeRetriesAfterRestart() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("create table up_part_o3 as" +
                        " (select timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY");

                execute("insert into up_part_o3 " +
                        " select timestamp_sequence('1970-01-02T01', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(3)");

                try (TableReader rdr = getReader("up_part_o3")) {
                    update("UPDATE up_part_o3 SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-03'");

                    runPurgeJob(purgeJob);
                    rdr.openPartition(0);
                }
            }

            try (Path path = new Path()) {

                String[] partitions = new String[]{"1970-01-03.1", "1970-01-04.1", "1970-01-05"};
                assertFilesExist(partitions, path, "up_part_o3", "", true);

                String purgeLogTableName;
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    assertFilesExist(partitions, path, "up_part_o3", "", false);
                    Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                    purgeLogTableName = purgeJob.getLogTableName();
                }

                assertQuery("up_part_o3")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns(replaceTimestampSuffix("""
                                ts\tx\tstr\tsym1\tsym2
                                1970-01-01T02:00:00.000000Z\t1\ta\tC\t2
                                1970-01-02T01:00:00.000000Z\t1\ta\tA\t2
                                1970-01-02T02:00:00.000000Z\t2\td\tB\t4
                                1970-01-03T01:00:00.000000Z\t100\tabcd\tC\tEE
                                1970-01-03T02:00:00.000000Z\t100\tabcd\tD\tEE
                                1970-01-04T01:00:00.000000Z\t100\tabcd\tA\tEE
                                1970-01-04T02:00:00.000000Z\t100\tabcd\tA\tEE
                                1970-01-05T02:00:00.000000Z\t100\tabcd\tD\tEE
                                """, timestampType.getTypeName()));

                // cleaned everything, table is truncated
                assertQuery(purgeLogTableName)
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n");
            }
        });
    }

    @Test
    public void testPurgeTaskRecycle() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_TASK_POOL_CAPACITY, 1);
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("create table up_part_o3_many as" +
                        " (select timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY");

                update("UPDATE up_part_o3_many SET x = x + 1, str = str || 'u2', sym2 = sym2 || '2'");

                try (Path path = new Path()) {
                    for (int i = 1; i < 10; i++) {
                        try (TableReader ignore = getReader("up_part_o3_many")) {
                            update("UPDATE up_part_o3_many SET x = x + 1, str = str || 'u2', sym2 = sym2 || '2'");
                            runPurgeJob(purgeJob);
                        }

                        String[] partitions = new String[]{"1970-01-02", "1970-01-03", "1970-01-04", "1970-01-05"};
                        assertFilesExist(partitions, path, "up_part_o3_many", "." + i, true);

                        runPurgeJob(purgeJob);

                        assertFilesExist(partitions, path, "up_part_o3_many", "." + i, false);
                    }
                }
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testPurgeWithOutOfOrderUpdate() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("create table up_part_o3 as" +
                        " (select timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY");

                execute("insert into up_part_o3 " +
                        " select timestamp_sequence('1970-01-02T01', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(3)");

                try (TableReader rdr = getReader("up_part_o3")) {
                    update("UPDATE up_part_o3 SET x = 100, str='abcd', sym2 = 'EE' WHERE ts >= '1970-01-03'");

                    setCurrentMicros(20);
                    runPurgeJob(purgeJob);
                    rdr.openPartition(0);
                }

                try (Path path = new Path()) {
                    String[] partitions = new String[]{"1970-01-03.1", "1970-01-04.1", "1970-01-05"};
                    assertFilesExist(partitions, path, "up_part_o3", "", true);

                    setCurrentMicros(40);
                    runPurgeJob(purgeJob);

                    assertFilesExist(partitions, path, "up_part_o3", "", false);
                }

                assertQuery("up_part_o3")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns(replaceTimestampSuffix("""
                                ts\tx\tstr\tsym1\tsym2
                                1970-01-01T02:00:00.000000Z\t1\ta\tC\t2
                                1970-01-02T01:00:00.000000Z\t1\ta\tA\t2
                                1970-01-02T02:00:00.000000Z\t2\td\tB\t4
                                1970-01-03T01:00:00.000000Z\t100\tabcd\tC\tEE
                                1970-01-03T02:00:00.000000Z\t100\tabcd\tD\tEE
                                1970-01-04T01:00:00.000000Z\t100\tabcd\tA\tEE
                                1970-01-04T02:00:00.000000Z\t100\tabcd\tA\tEE
                                1970-01-05T02:00:00.000000Z\t100\tabcd\tD\tEE
                                """, timestampType.getTypeName()));

                assertQuery(purgeJob.getLogTableName())
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted
                                1970-01-01T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t3\t-1\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z
                                1970-01-01T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t3\t-1\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z
                                1970-01-01T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t3\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000070Z
                                1970-01-01T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t3\t-1\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z
                                1970-01-01T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t3\t-1\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z
                                1970-01-01T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t3\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000070Z
                                1970-01-01T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t3\t-1\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z
                                1970-01-01T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t3\t-1\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z
                                1970-01-01T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t3\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000070Z
                                """);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testSavesDataToPurgeLogTable() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                TableToken tn1 = new TableToken("tbl_name", "tbl_name", null, 123, false, false, false);
                ColumnPurgeTask task = createTask(tn1, "col", 1, ColumnType.INT, 43, 11, "2022-03-29", -1);
                task.appendColumnInfo(-1, timestampType.getDriver().parseFloorLiteral("2022-04-05"), 2);
                appendTaskToQueue(task);


                TableToken tn2 = new TableToken("tbl_name2", "tbl_name2", null, 123, false, false, false);
                ColumnPurgeTask task2 = createTask(tn2, "col2", 2, ColumnType.SYMBOL, 33, -1, "2022-02-13", 3);
                appendTaskToQueue(task2);

                purgeJob.run();
                assertQuery(purgeJob.getLogTableName())
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted
                                1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t11\t2022-03-29T00:00:00.000000Z\t-1\t
                                1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t-1\t2022-04-05T00:00:00.000000Z\t2\t
                                1970-01-01T00:00:00.000001Z\ttbl_name2\tcol2\t2\t0\t12\t3\t33\t-1\t2022-02-13T00:00:00.000000Z\t3\t
                                """);

                runPurgeJob(purgeJob);
                assertQuery(purgeJob.getLogTableName())
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted
                                1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t11\t2022-03-29T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000010Z
                                1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t-1\t2022-04-05T00:00:00.000000Z\t2\t1970-01-01T00:00:00.000010Z
                                1970-01-01T00:00:00.000001Z\ttbl_name2\tcol2\t2\t0\t12\t3\t33\t-1\t2022-02-13T00:00:00.000000Z\t3\t1970-01-01T00:00:00.000010Z
                                """);
            }
        });
    }

    private void appendTaskToQueue(ColumnPurgeTask task) {
        long cursor = -1L;
        Sequence pubSeq = engine.getMessageBus().getColumnPurgePubSeq();
        while (cursor < 0) {
            cursor = pubSeq.next();
            if (cursor > -1L) {
                ColumnPurgeTask queueTask = engine.getMessageBus().getColumnPurgeQueue().get(cursor);
                queueTask.copyFrom(task);
                pubSeq.done(cursor);
            }
        }
    }

    private void assertFilesExist(String[] partitions, Path path, String tableName, String colSuffix, boolean exist) {
        for (int i = 0; i < partitions.length; i++) {
            String partition = partitions[i];
            assertFilesExist(path, tableName, partition, colSuffix, exist);
        }
    }

    private void assertFilesExist(Path path, String up_part, String partition, String colSuffix, boolean exist) {
        TableToken tableToken = engine.verifyTableName(up_part);
        path.of(configuration.getDbRoot()).concat(tableToken).concat(partition).concat("x.d").put(colSuffix).$();
        Assert.assertEquals(Utf8s.toString(path), exist, TestFilesFacadeImpl.INSTANCE.exists(path.$()));

        path.of(configuration.getDbRoot()).concat(tableToken).concat(partition).concat("str.d").put(colSuffix).$();
        Assert.assertEquals(Utf8s.toString(path), exist, TestFilesFacadeImpl.INSTANCE.exists(path.$()));

        path.of(configuration.getDbRoot()).concat(tableToken).concat(partition).concat("str.i").put(colSuffix).$();
        Assert.assertEquals(Utf8s.toString(path), exist, TestFilesFacadeImpl.INSTANCE.exists(path.$()));

        assertIndexFilesExist(path, up_part, partition, colSuffix, exist);
    }

    @Test
    public void testDropPostingIndex() throws Exception {
        // Exercises ColumnPurgeOperator.couldNotRemoveIndexFiles(),
        // existsIndexFile(), and removeSidecarFiles() for POSTING index type.
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("CREATE TABLE t_posting AS" +
                        " (SELECT timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym" +
                        " FROM long_sequence(5))" +
                        " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

                execute("ALTER TABLE t_posting ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (x)");

                // Hold a reader open so the purge job can't clean up immediately
                try (TableReader rdr1 = getReader("t_posting")) {
                    execute("INSERT INTO t_posting" +
                            " SELECT timestamp_sequence('1970-01-02T01', 24 * 60 * 60 * 1000000L) ts," +
                            " x," +
                            " rnd_symbol('A', 'B', 'C', 'D') sym" +
                            " FROM long_sequence(3)");

                    try (TableReader ignored = getReader("t_posting")) {
                        update("ALTER TABLE t_posting ALTER COLUMN sym DROP INDEX");
                        // Purge job runs but can't delete because reader is open
                        runPurgeJob(purgeJob);
                        setCurrentMicros(currentMicros + 1);
                    }
                    rdr1.openPartition(0);
                }

                // Now all readers are closed — purge should clean up posting index files
                runPurgeJob(purgeJob);

                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());

                // Verify the table is still queryable after purge
                assertQuery("SELECT count() AS count FROM t_posting WHERE x > 0")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                count
                                8
                                """);
            }
        });
    }

    @Test
    public void testUpdateWithPostingIndex() throws Exception {
        // Exercises ColumnPurgeOperator posting index cleanup via UPDATE
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("CREATE TABLE t_posting_upd AS" +
                        " (SELECT timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym" +
                        " FROM long_sequence(5))" +
                        " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

                execute("ALTER TABLE t_posting_upd ALTER COLUMN sym ADD INDEX TYPE POSTING");

                try (TableReader ignored = getReader("t_posting_upd")) {
                    update("UPDATE t_posting_upd SET x = 100 WHERE ts >= '1970-01-03'");
                    runPurgeJob(purgeJob);
                    setCurrentMicros(currentMicros + 1);
                }

                runPurgeJob(purgeJob);

                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    /**
     * Red test for kafka1991 PR #7095 review finding #1:
     * {@code ColumnPurgeOperator.existsIndexFile} at
     * {@code ColumnPurgeOperator.java:222} reads the live sealTxn from the
     * .pk file with {@code readSealTxnFromKeyFile}. When that call returns
     * {@code -1} (file gone, openRO failure, persistent seqlock-read failure),
     * the local {@code sealTxn} falls back to {@code columnVersion}. The
     * .pv existence probe at line 229 then looks at the wrong filename
     * (the real .pv has sealTxn != columnVersion) and returns false. The
     * caller adds the row to {@code completedRowIds} and the purge is
     * permanently marked done -- the orphan .pv (and its sealed sidecars,
     * caught by removeAllSealedFiles when called) survive forever because
     * the purge queue will never revisit them.
     * <p>
     * Reproduction:
     * <ol>
     *   <li>ALTER ADD INDEX TYPE POSTING creates {@code sym.pk} +
     *       {@code sym.pv.0} (columnNameTxn == COLUMN_NAME_TXN_NONE,
     *       sealTxn == 0).</li>
     *   <li>UPDATE bumps the column version and queues the old
     *       {@code sym.pk}/{@code sym.pv.0} for purge.</li>
     *   <li>A reader holds the column version pinned; the first purge
     *       run cannot proceed.</li>
     *   <li>Before the reader is released, manually remove just
     *       {@code sym.pk}. The real {@code sym.pv.0} still sits in the
     *       partition directory.</li>
     *   <li>Second purge run: existsIndexFile sees no .pk, falls back to
     *       {@code sealTxn = columnVersion = -1}, probes
     *       {@code sym.pv.-1}, gets false, marks the task done.</li>
     * </ol>
     * After the fix, the purge should either resolve the live sealTxn from
     * the partition layout (e.g. scanSealedFiles for any .pv matching the
     * columnVersion) or refuse to mark the task complete -- either way the
     * orphan {@code sym.pv.0} must not survive a fully drained purge queue.
     */
    @Test
    public void testPurgeMarksTaskCompleteAndLeaksPostingValueWhenKeyFileMissing() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("CREATE TABLE t_purge_leak AS" +
                        " (SELECT timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym" +
                        " FROM long_sequence(5))" +
                        " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("ALTER TABLE t_purge_leak ALTER COLUMN sym ADD INDEX TYPE POSTING");

                String partition = "1970-01-03";
                try (Path path = new Path()) {
                    // Sanity: sym.pk + sym.pv.0 exist before the UPDATE.
                    assertPostingKeyFileExists(path, "t_purge_leak", partition, true);
                    assertPartitionFileExists(path, partition, "sym.pv.0", true);
                }

                try (TableReader rdr = getReader("t_purge_leak")) {
                    update("UPDATE t_purge_leak SET sym = 'Z' WHERE ts >= '1970-01-03'");
                    // First run records the task in the purge log but cannot
                    // delete files because the reader still pins the old
                    // column version.
                    runPurgeJob(purgeJob);
                    rdr.openPartition(0);
                }

                // Reproduce a partial-purge state: a prior cleanup run
                // succeeded on sym.d, sym.o, sym.pk but failed on sym.pv.
                // The next purge pass therefore enters
                // existsIndexFile(POSTING, ..., columnVersion=-1) at
                // ColumnPurgeOperator.java:212 with .d/.o already gone,
                // which routes through the broken sealTxn fallback below.
                TableToken tt = engine.verifyTableName("t_purge_leak");
                try (Path path = new Path()) {
                    for (String fileName : new String[]{"sym.d", "sym.o", "sym.pk"}) {
                        path.of(configuration.getDbRoot()).concat(tt).concat(partition).concat(fileName).$();
                        Assert.assertTrue("precondition: " + fileName + " present before manual delete",
                                TestFilesFacadeImpl.INSTANCE.removeQuiet(path.$()));
                    }
                    assertPartitionFileExists(path, partition, "sym.d", false);
                    assertPartitionFileExists(path, partition, "sym.o", false);
                    assertPostingKeyFileExists(path, "t_purge_leak", partition, false);
                    assertPartitionFileExists(path, partition, "sym.pv.0", true);
                }

                runPurgeJob(purgeJob);

                // Drain assertion: the purge queue treats this task as
                // resolved -- on the buggy path the row landed in
                // completedRowIds when existsIndexFile returned false.
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());

                // Red assertion: the orphan sym.pv.0 must not survive a
                // drained purge queue. On master the column purge silently
                // walks away from it (current behaviour, this assertion
                // fails); the fix must either clean it up or keep the task
                // outstanding for retry.
                try (Path path = new Path()) {
                    assertPartitionFileExists(path, partition, "sym.pv.0", false);
                }
            }
        });
    }

    @Test
    public void testPurgeJobRemovesOrphanPostingSidecarsOfEveryPartition() throws Exception {
        // UPDATE with an open reader leaves the old instance of sym in three partitions. The test deletes
        // its sym.d and sym.pk in the last two partitions, or in all three, so only the .pci, .pv and .pc
        // files of the old instance remain there. One retry of the purge task removes the old instance
        // from every partition.
        assertMemoryLeak(() -> {
            for (boolean isFirstPartitionOrphaned : new boolean[]{false, true}) {
                final String context = isFirstPartitionOrphaned ? "all partitions orphaned" : "first partition complete";
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    final String[] liveFiles = createOrphanPostingSidecars(context, purgeJob, isFirstPartitionOrphaned);
                    Assert.assertEquals(context, 1, purgeJob.getOutstandingPurgeTasks());

                    engine.releaseInactive();
                    setCurrentMicros(currentMicros + 100_000_000L);
                    purgeJob.run();
                    Assert.assertEquals(context, 0, purgeJob.getOutstandingPurgeTasks());
                    assertOrphanPostingSidecarsPurged(context, liveFiles);
                }
            }
        });
    }

    @Test
    public void testPurgeRemovesRootSymbolFilesOfOldCapacity() throws Exception {
        // A symbol capacity change with an open reader leaves the symbol table files of the old version
        // in the table root. One run of the purge job removes all four of them.
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("CREATE TABLE t_capacity AS" +
                        " (SELECT timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym" +
                        " FROM long_sequence(3))" +
                        " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                final File tableDir = new File(configuration.getDbRoot().toString(), engine.verifyTableName("t_capacity").getDirName());
                Assert.assertEquals("[sym.c, sym.k, sym.o, sym.v]", symbolRootFiles(tableDir).toString());
                final long symbolTableNameTxn;
                try (TableReader ignore = getReader("t_capacity")) {
                    execute("ALTER TABLE t_capacity ALTER COLUMN sym SYMBOL CAPACITY 1024");
                    try (TableReader reader = getReader("t_capacity")) {
                        symbolTableNameTxn = reader.getColumnVersionReader().getSymbolTableNameTxn(1);
                    }
                    runPurgeJob(purgeJob);
                    Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
                }
                Assert.assertTrue(symbolTableNameTxn > 0);
                final String suffix = "." + symbolTableNameTxn;
                Assert.assertEquals(
                        "[sym.c, sym.c" + suffix + ", sym.k, sym.k" + suffix + ", sym.o, sym.o" + suffix + ", sym.v, sym.v" + suffix + "]",
                        symbolRootFiles(tableDir).toString()
                );

                engine.releaseInactive();
                setCurrentMicros(currentMicros + 100_000_000L);
                purgeJob.run();
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                Assert.assertEquals(
                        "[sym.c" + suffix + ", sym.k" + suffix + ", sym.o" + suffix + ", sym.v" + suffix + "]",
                        symbolRootFiles(tableDir).toString()
                );
                assertQuery("SELECT count() FROM t_capacity WHERE sym IN ('A', 'B', 'C', 'D')")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count
                                3
                                """);
            }
        });
    }

    @Test
    public void testPurgeRemovesPostingCoverFilesOfNameTxnZero() throws Exception {
        // s is the first column added to the empty table t_z, so its column name txn is 0, and its .pc
        // files carry the token 0 that txn NONE also writes: s.pc<N>.0[.<covered txn>].<seal>. An
        // earlier version left a superseded seal of s on disk, planted here by name. Each change moves
        // s, or its index, away from column version 0, and the removal of that version takes every
        // .pc file of it, with an open reader through the purge job and without one in the writer.
        // The covered values read what the twin, with s in CREATE TABLE, reads.
        assertMemoryLeak(() -> {
            for (String change : new String[]{"DROP INDEX", "DROP COLUMN", "RENAME COLUMN", "UPDATE"}) {
                for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                    for (boolean isReaderOpen : new boolean[]{true, false}) {
                        final String context = change + ", " + walMode + (isReaderOpen ? ", reader open" : "");
                        try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                            createTxnZeroCoveringTables(walMode);
                            final String dir = partitionDir("t_z", 0);
                            for (String name : new String[]{"s.pc0.0.1.77", "s.pc2.0.77", "s.pv.0.77"}) {
                                Assert.assertTrue(context + ", " + name, new File(dir, name).createNewFile());
                            }
                            final String sql = switch (change) {
                                case "DROP INDEX" -> "ALTER TABLE {t} ALTER COLUMN s DROP INDEX";
                                case "DROP COLUMN" -> "ALTER TABLE {t} DROP COLUMN s";
                                case "RENAME COLUMN" -> "ALTER TABLE {t} RENAME COLUMN s TO s2";
                                default -> "UPDATE {t} SET s = 'z' WHERE s = 'y' AND ts IN '2024-01-01'";
                            };
                            if (isReaderOpen) {
                                try (TableReader ignored = getReader("t_z")) {
                                    execute(sql.replace("{t}", "t_z"));
                                    execute(sql.replace("{t}", "t_twin"));
                                    drainWalQueue();
                                    runPurgeJob(purgeJob);
                                }
                            } else {
                                execute(sql.replace("{t}", "t_z"));
                                execute(sql.replace("{t}", "t_twin"));
                                drainWalQueue();
                            }
                            runPurgeJob(purgeJob);
                            Assert.assertEquals(context, 0, purgeJob.getOutstandingPurgeTasks());
                            Assert.assertEquals(context, "[]", txnZeroIndexFilesOfS(partitionDir("t_z", 0)).toString());
                            final String sName = change.equals("RENAME COLUMN") ? "s2" : "s";
                            if (!change.startsWith("DROP")) {
                                assertCoverFilesPerSeal(context, partitionDir("t_z", 0), sName);
                                assertCoverFilesPerSeal(context, partitionDir("t_z", 1), sName);
                            }
                            final String projection = change.equals("DROP COLUMN") ? "ts, d, vc" : "ts, " + sName + ", d, vc";
                            for (String query : change.startsWith("DROP")
                                    ? new String[]{"SELECT " + projection + " FROM {t}"}
                                    : new String[]{
                                    "SELECT " + projection + " FROM {t} WHERE " + sName + " = 'x'",
                                    "SELECT " + projection + " FROM {t} WHERE " + sName + " IN ('y', 'z')",
                                    "SELECT " + projection + " FROM {t}"
                            }) {
                                assertSqlCursors(query.replace("{t}", "t_twin"), query.replace("{t}", "t_z"));
                            }
                            if (!change.startsWith("DROP")) {
                                assertQuery("SELECT count() c, sum(d) sd FROM t_z WHERE " + sName + " = 'x'")
                                        .noLeakCheck()
                                        .noRandomAccess()
                                        .expectSize()
                                        .withPlanContaining("CoveringIndex on: " + sName)
                                        .returns("c\tsd\n3\t14.5\n");
                            }
                            execute("DROP TABLE t_z");
                            execute("DROP TABLE t_twin");
                            drainWalQueue();
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testPurgeRetriesPostingCoverFilesOfNameTxnZero() throws Exception {
        // The purge of the index of s, column name txn 0, cannot remove one of its .pc files the first
        // time. It reports the task incomplete and removes the file on the retry.
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicBoolean hasFailed = new AtomicBoolean();
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                if (isArmed.get() && Utf8s.containsAscii(name, Files.SEPARATOR + "s.pc0.0.") && hasFailed.compareAndSet(false, true)) {
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                createTxnZeroCoveringTables("BYPASS WAL");
                try (TableReader ignored = getReader("t_z")) {
                    execute("ALTER TABLE t_z ALTER COLUMN s DROP INDEX");
                    execute("ALTER TABLE t_twin ALTER COLUMN s DROP INDEX");
                    runPurgeJob(purgeJob);
                }
                isArmed.set(true);
                engine.releaseInactive();
                setCurrentMicros(currentMicros + 10L * iteration++);
                purgeJob.run();
                Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
                Assert.assertTrue(hasFailed.get());
                Assert.assertEquals(1, coverDataFileCount(partitionDir("t_z", 0), "s") + coverDataFileCount(partitionDir("t_z", 1), "s"));
                runPurgeJob(purgeJob);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                Assert.assertEquals("[]", txnZeroIndexFilesOfS(partitionDir("t_z", 0)).toString());
                Assert.assertEquals("[]", txnZeroIndexFilesOfS(partitionDir("t_z", 1)).toString());
                assertSqlCursors("SELECT ts, s, d, vc FROM t_twin WHERE s = 'x'", "SELECT ts, s, d, vc FROM t_z WHERE s = 'x'");
            }
        });
    }

    @Test
    public void testPurgeRetriesPostingCoverInfoFile() throws Exception {
        // UPDATE with a reader open leaves the old instance of sym in the last three of five partitions.
        // The purge job cannot remove the .pci file of the old instance in the middle one of them, once
        // or three times in a row: each failed run keeps the task, and the next run removes the file
        // after every other file of the old instance is gone. The old instance has the column name txn
        // NONE, or the txn of an earlier UPDATE whose synchronous purge completed.
        final AtomicReference<String> failingFileSuffix = new AtomicReference<>();
        final AtomicInteger remainingFailures = new AtomicInteger();
        final AtomicInteger failedRemoveCount = new AtomicInteger();
        ff = newFailingRemoveFilesFacade(failingFileSuffix, remainingFailures, failedRemoveCount);
        assertMemoryLeak(ff, () -> {
            final String[][] cases = {
                    // WAL mode, column name txn of the old instance, covered columns, failed removals
                    {"BYPASS WAL", "NONE", "x", "1"},
                    {"WAL", "NONE", "x", "1"},
                    {"BYPASS WAL", "NONE", "d, vc, str", "1"},
                    {"BYPASS WAL", "UPDATED", "x", "1"},
                    {"WAL", "UPDATED", "d, vc, str", "1"},
                    {"BYPASS WAL", "UPDATED", "d, vc, str", "3"},
            };
            for (String[] testCase : cases) {
                final String context = String.join(", ", testCase);
                final boolean isUpdatedBefore = testCase[1].equals("UPDATED");
                final int failures = Integer.parseInt(testCase[3]);
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    createCoverInfoTable(testCase[0], testCase[2]);
                    if (isUpdatedBefore) {
                        // no reader: the writer purges the instance NONE itself
                        update("UPDATE t_pci SET sym = 'B' WHERE ts >= '1970-01-03'");
                        drainWalQueue();
                        runPurgeJob(purgeJob);
                        Assert.assertEquals(context, 0, purgeJob.getOutstandingPurgeTasks());
                    }
                    final String[] oldInstanceFiles = filesOfSymInPartitions2To4(context);
                    final String[] coverInfoFiles = new String[3];
                    for (int i = 0; i < 3; i++) {
                        final long columnNameTxn = getColumnNameTxn("t_pci", "sym", i + 2);
                        if (isUpdatedBefore) {
                            Assert.assertTrue(context, columnNameTxn > 0);
                        } else {
                            Assert.assertEquals(context, TableUtils.COLUMN_NAME_TXN_NONE, columnNameTxn);
                        }
                        coverInfoFiles[i] = coverInfoFileName("sym", columnNameTxn);
                        Assert.assertTrue(context + ", " + oldInstanceFiles[i], (" " + oldInstanceFiles[i] + " ").contains(" " + coverInfoFiles[i] + " "));
                    }

                    try (TableReader ignore = getReader("t_pci")) {
                        update("UPDATE t_pci SET sym = 'Z' WHERE ts >= '1970-01-03'");
                        drainWalQueue();
                        runPurgeJob(purgeJob);
                        Assert.assertEquals(context, 1, purgeJob.getOutstandingPurgeTasks());
                    }
                    final String[] liveFiles = liveFilesOfPartitions2To4(context, oldInstanceFiles);

                    final String middleDir = partitionDir("t_pci", 3);
                    failingFileSuffix.set(Files.SEPARATOR + new File(middleDir).getName() + Files.SEPARATOR + coverInfoFiles[1]);
                    remainingFailures.set(failures);
                    failedRemoveCount.set(0);
                    for (int k = 1; k <= failures; k++) {
                        engine.releaseInactive();
                        setCurrentMicros(currentMicros + 100_000_000L);
                        purgeJob.run();
                        Assert.assertEquals(context, k, failedRemoveCount.get());
                        Assert.assertEquals(context, 1, purgeJob.getOutstandingPurgeTasks());
                        Assert.assertEquals(context, liveFiles[0], listFiles(partitionDir("t_pci", 2)));
                        Assert.assertEquals(context, joinSorted(liveFiles[1], coverInfoFiles[1]), listFiles(middleDir));
                        Assert.assertEquals(context, liveFiles[2], listFiles(partitionDir("t_pci", 4)));
                    }

                    engine.releaseInactive();
                    setCurrentMicros(currentMicros + 100_000_000L);
                    purgeJob.run();
                    Assert.assertEquals(context, failures, failedRemoveCount.get());
                    Assert.assertEquals(context, 0, purgeJob.getOutstandingPurgeTasks());
                    assertLiveFilesOfPartitions2To4(context, liveFiles);
                    assertRowsOfCoverInfoTable(context);
                    failingFileSuffix.set(null);
                    execute("DROP TABLE t_pci");
                    drainWalQueue();
                }
            }
        });
    }

    @Test
    public void testPurgeRetriesPostingCoverInfoFileAfterUpdateWithoutReader() throws Exception {
        // No reader holds the old instance of sym, so the writer purges it during UPDATE. It cannot
        // remove the .pci file in the middle one of the three updated partitions and hands the purge to
        // the purge job, whose first run removes the file.
        final AtomicReference<String> failingFileSuffix = new AtomicReference<>();
        final AtomicInteger remainingFailures = new AtomicInteger();
        final AtomicInteger failedRemoveCount = new AtomicInteger();
        ff = newFailingRemoveFilesFacade(failingFileSuffix, remainingFailures, failedRemoveCount);
        assertMemoryLeak(ff, () -> {
            for (String walMode : new String[]{"BYPASS WAL", "WAL"}) {
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    createCoverInfoTable(walMode, "x");
                    final String[] oldInstanceFiles = filesOfSymInPartitions2To4(walMode);
                    final String middleDir = partitionDir("t_pci", 3);
                    failingFileSuffix.set(Files.SEPARATOR + new File(middleDir).getName() + Files.SEPARATOR + "sym.pci");
                    remainingFailures.set(1);
                    failedRemoveCount.set(0);

                    update("UPDATE t_pci SET sym = 'Z' WHERE ts >= '1970-01-03'");
                    drainWalQueue();
                    Assert.assertEquals(walMode, 1, failedRemoveCount.get());
                    final String[] liveFiles = new String[3];
                    for (int i = 0; i < 3; i++) {
                        final TreeSet<String> files = new TreeSet<>(Arrays.asList(listFiles(partitionDir("t_pci", i + 2)).split(" ")));
                        files.removeAll(Arrays.asList(oldInstanceFiles[i].split(" ")));
                        liveFiles[i] = String.join(" ", files);
                    }
                    Assert.assertEquals(walMode, liveFiles[0], listFiles(partitionDir("t_pci", 2)));
                    Assert.assertEquals(walMode, joinSorted(liveFiles[1], "sym.pci"), listFiles(middleDir));
                    Assert.assertEquals(walMode, liveFiles[2], listFiles(partitionDir("t_pci", 4)));

                    runPurgeJob(purgeJob);
                    Assert.assertEquals(walMode, 1, failedRemoveCount.get());
                    Assert.assertEquals(walMode, 0, purgeJob.getOutstandingPurgeTasks());
                    assertLiveFilesOfPartitions2To4(walMode, liveFiles);
                    assertRowsOfCoverInfoTable(walMode);
                    failingFileSuffix.set(null);
                    execute("DROP TABLE t_pci");
                    drainWalQueue();
                }
            }
        });
    }

    @Test
    public void testPurgeRetriesPostingCoverInfoFileOfDroppedOrRenamedColumn() throws Exception {
        // DROP COLUMN and RENAME COLUMN with a reader open leave the instance NONE of sym in all five
        // partitions. The purge job cannot remove its .pci file in the middle partition the first time
        // and removes it on the retry.
        final AtomicReference<String> failingFileSuffix = new AtomicReference<>();
        final AtomicInteger remainingFailures = new AtomicInteger();
        final AtomicInteger failedRemoveCount = new AtomicInteger();
        ff = newFailingRemoveFilesFacade(failingFileSuffix, remainingFailures, failedRemoveCount);
        assertMemoryLeak(ff, () -> {
            for (String change : new String[]{"DROP COLUMN", "RENAME COLUMN"}) {
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    createCoverInfoTable("BYPASS WAL", "x");
                    final String[] oldInstanceFiles = new String[5];
                    for (int i = 0; i < 5; i++) {
                        oldInstanceFiles[i] = String.join(" ", filesWithPrefix(listFiles(partitionDir("t_pci", i)), "sym."));
                        Assert.assertTrue(change + ", " + oldInstanceFiles[i], (" " + oldInstanceFiles[i] + " ").contains(" sym.pci "));
                    }
                    try (TableReader ignore = getReader("t_pci")) {
                        execute(change.equals("DROP COLUMN") ? "ALTER TABLE t_pci DROP COLUMN sym" : "ALTER TABLE t_pci RENAME COLUMN sym TO s2");
                        runPurgeJob(purgeJob);
                        Assert.assertEquals(change, 1, purgeJob.getOutstandingPurgeTasks());
                    }
                    final String[] liveFiles = new String[5];
                    for (int i = 0; i < 5; i++) {
                        final String listing = listFiles(partitionDir("t_pci", i));
                        final TreeSet<String> files = new TreeSet<>(Arrays.asList(listing.split(" ")));
                        Assert.assertTrue(change + ", " + listing, files.containsAll(Arrays.asList(oldInstanceFiles[i].split(" "))));
                        files.removeAll(Arrays.asList(oldInstanceFiles[i].split(" ")));
                        Assert.assertEquals(change + ", " + listing, change.equals("RENAME COLUMN"), !filesWithPrefix(String.join(" ", files), "s2.pci").isEmpty());
                        liveFiles[i] = String.join(" ", files);
                    }

                    final String middleDir = partitionDir("t_pci", 2);
                    failingFileSuffix.set(Files.SEPARATOR + new File(middleDir).getName() + Files.SEPARATOR + "sym.pci");
                    remainingFailures.set(1);
                    failedRemoveCount.set(0);
                    engine.releaseInactive();
                    setCurrentMicros(currentMicros + 100_000_000L);
                    purgeJob.run();
                    Assert.assertEquals(change, 1, failedRemoveCount.get());
                    Assert.assertEquals(change, 1, purgeJob.getOutstandingPurgeTasks());
                    for (int i = 0; i < 5; i++) {
                        Assert.assertEquals(change, i == 2 ? joinSorted(liveFiles[i], "sym.pci") : liveFiles[i], listFiles(partitionDir("t_pci", i)));
                    }

                    engine.releaseInactive();
                    setCurrentMicros(currentMicros + 100_000_000L);
                    purgeJob.run();
                    Assert.assertEquals(change, 0, purgeJob.getOutstandingPurgeTasks());
                    for (int i = 0; i < 5; i++) {
                        Assert.assertEquals(change, liveFiles[i], listFiles(partitionDir("t_pci", i)));
                    }
                    if (change.equals("DROP COLUMN")) {
                        assertQuery("SELECT ts, x FROM t_pci")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns(replaceTimestampSuffix("""
                                        ts\tx
                                        1970-01-01T02:00:00.000000Z\t1
                                        1970-01-02T02:00:00.000000Z\t2
                                        1970-01-03T02:00:00.000000Z\t3
                                        1970-01-04T02:00:00.000000Z\t4
                                        1970-01-05T02:00:00.000000Z\t5
                                        """, timestampType.getTypeName()));
                    } else {
                        assertQuery("SELECT ts, s2, x FROM t_pci WHERE s2 = 'A'")
                                .noLeakCheck()
                                .noRandomAccess()
                                .expectSize()
                                .timestamp("ts")
                                .withPlanContaining("CoveringIndex on: s2 with: ts, x")
                                .returns(replaceTimestampSuffix("""
                                        ts\ts2\tx
                                        1970-01-01T02:00:00.000000Z\tA\t1
                                        1970-01-04T02:00:00.000000Z\tA\t4
                                        """, timestampType.getTypeName()));
                    }
                    failingFileSuffix.set(null);
                    execute("DROP TABLE t_pci");
                }
            }
        });
    }

    @Test
    public void testPurgeRetriesPostingCoverInfoFileOfNameTxnZero() throws Exception {
        // DROP INDEX with a reader open leaves instance 0 of s in both partitions. The purge job cannot
        // remove s.pci.0 in the first partition the first time and removes it on the retry.
        final AtomicReference<String> failingFileSuffix = new AtomicReference<>();
        final AtomicInteger remainingFailures = new AtomicInteger();
        final AtomicInteger failedRemoveCount = new AtomicInteger();
        ff = newFailingRemoveFilesFacade(failingFileSuffix, remainingFailures, failedRemoveCount);
        assertMemoryLeak(ff, () -> {
            for (String walMode : new String[]{"BYPASS WAL", "WAL"}) {
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    createTxnZeroCoveringTables(walMode);
                    final String[] oldInstanceFiles = new String[2];
                    for (int i = 0; i < 2; i++) {
                        oldInstanceFiles[i] = String.join(" ", filesWithPrefix(listFiles(partitionDir("t_z", i)), "s."));
                        Assert.assertTrue(walMode + ", " + oldInstanceFiles[i], (" " + oldInstanceFiles[i] + " ").contains(" s.pci.0 "));
                    }
                    try (TableReader ignored = getReader("t_z")) {
                        execute("ALTER TABLE t_z ALTER COLUMN s DROP INDEX");
                        execute("ALTER TABLE t_twin ALTER COLUMN s DROP INDEX");
                        drainWalQueue();
                        runPurgeJob(purgeJob);
                        Assert.assertEquals(walMode, 1, purgeJob.getOutstandingPurgeTasks());
                    }
                    final String[] liveFiles = new String[2];
                    for (int i = 0; i < 2; i++) {
                        final TreeSet<String> files = new TreeSet<>(Arrays.asList(listFiles(partitionDir("t_z", i)).split(" ")));
                        Assert.assertTrue(walMode, files.containsAll(Arrays.asList(oldInstanceFiles[i].split(" "))));
                        files.removeAll(Arrays.asList(oldInstanceFiles[i].split(" ")));
                        liveFiles[i] = String.join(" ", files);
                    }

                    final String firstDir = partitionDir("t_z", 0);
                    failingFileSuffix.set(Files.SEPARATOR + new File(firstDir).getName() + Files.SEPARATOR + "s.pci.0");
                    remainingFailures.set(1);
                    failedRemoveCount.set(0);
                    engine.releaseInactive();
                    setCurrentMicros(currentMicros + 100_000_000L);
                    purgeJob.run();
                    Assert.assertEquals(walMode, 1, failedRemoveCount.get());
                    Assert.assertEquals(walMode, 1, purgeJob.getOutstandingPurgeTasks());
                    Assert.assertEquals(walMode, joinSorted(liveFiles[0], "s.pci.0"), listFiles(firstDir));
                    Assert.assertEquals(walMode, liveFiles[1], listFiles(partitionDir("t_z", 1)));

                    engine.releaseInactive();
                    setCurrentMicros(currentMicros + 100_000_000L);
                    purgeJob.run();
                    Assert.assertEquals(walMode, 0, purgeJob.getOutstandingPurgeTasks());
                    Assert.assertEquals(walMode, liveFiles[0], listFiles(firstDir));
                    Assert.assertEquals(walMode, liveFiles[1], listFiles(partitionDir("t_z", 1)));
                    assertSqlCursors("SELECT ts, s, d, vc FROM t_twin WHERE s = 'x'", "SELECT ts, s, d, vc FROM t_z WHERE s = 'x'");
                    assertSqlCursors("SELECT ts, s, d, vc FROM t_twin", "SELECT ts, s, d, vc FROM t_z");
                    failingFileSuffix.set(null);
                    execute("DROP TABLE t_z");
                    execute("DROP TABLE t_twin");
                    drainWalQueue();
                }
            }
        });
    }

    @Test
    public void testReplayedPurgeRetriesPostingCoverInfoFile() throws Exception {
        // The purge job cannot remove the .pci file of the old instance of sym in the middle partition
        // and closes with the task incomplete in the purge log. A new purge job replays the task and
        // removes the file.
        final AtomicReference<String> failingFileSuffix = new AtomicReference<>();
        final AtomicInteger remainingFailures = new AtomicInteger();
        final AtomicInteger failedRemoveCount = new AtomicInteger();
        ff = newFailingRemoveFilesFacade(failingFileSuffix, remainingFailures, failedRemoveCount);
        assertMemoryLeak(ff, () -> {
            final String[] liveFiles;
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                createCoverInfoTable("BYPASS WAL", "x");
                final String[] oldInstanceFiles = filesOfSymInPartitions2To4("replay");
                try (TableReader ignore = getReader("t_pci")) {
                    update("UPDATE t_pci SET sym = 'Z' WHERE ts >= '1970-01-03'");
                    runPurgeJob(purgeJob);
                    Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
                }
                liveFiles = liveFilesOfPartitions2To4("replay", oldInstanceFiles);
                final String middleDir = partitionDir("t_pci", 3);
                failingFileSuffix.set(Files.SEPARATOR + new File(middleDir).getName() + Files.SEPARATOR + "sym.pci");
                remainingFailures.set(1);
                engine.releaseInactive();
                setCurrentMicros(currentMicros + 100_000_000L);
                purgeJob.run();
                Assert.assertEquals(1, failedRemoveCount.get());
                Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
                Assert.assertEquals(joinSorted(liveFiles[1], "sym.pci"), listFiles(middleDir));
                // the purge log holds a row per partition of the task and completes them all at once
                assertIncompletePurgeLogRows(purgeJob, 3);
            }
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                Assert.assertEquals(1, failedRemoveCount.get());
                assertLiveFilesOfPartitions2To4("replay", liveFiles);
                assertRowsOfCoverInfoTable("replay");
            }
        });
    }

    @Test
    public void testPurgeKeepsPostingCoverFilesOfIndexReAddedAtTxnZero() throws Exception {
        // DROP INDEX as the first change of a table that is not partitioned gives the column the column
        // name txn 0 in default, next to the files of instance NONE, whose purge an open reader defers.
        // ADD INDEX then writes the .pc files of instance 0, whose host token 0 NONE also writes. The
        // purge of NONE, by the purge job or replayed from the purge log by a new one, takes only the
        // files of NONE. The old BITMAP index purged by the purge job guards the shape: its purge never
        // runs the POSTING removal.
        assertMemoryLeak(() -> {
            final String[][] cases = {
                    // column name, old index, new index, replayed
                    {"s", "INDEX TYPE POSTING INCLUDE (d, vc)", "POSTING INCLUDE (d, vc, l)", "false"},
                    {"s", "INDEX TYPE POSTING", "POSTING DELTA INCLUDE (d, vc, l)", "false"},
                    {"s", "INDEX TYPE POSTING DELTA INCLUDE (d, vc)", "POSTING EF INCLUDE (d, vc, l)", "false"},
                    {"s", "INDEX TYPE POSTING EF INCLUDE (d, vc)", "POSTING INCLUDE (d, vc, l)", "false"},
                    {"s", "INDEX", "POSTING INCLUDE (d, vc, l)", "false"},
                    {"\u0441\u0438\u043c", "INDEX TYPE POSTING INCLUDE (d, vc)", "POSTING INCLUDE (d, vc, l)", "false"},
                    {"s", "INDEX TYPE POSTING INCLUDE (d, vc)", "POSTING INCLUDE (d, vc, l)", "true"},
                    {"s", "INDEX", "POSTING EF INCLUDE (d, vc, l)", "true"},
            };
            for (String[] testCase : cases) {
                final String col = testCase[0];
                final boolean isReplayed = Boolean.parseBoolean(testCase[3]);
                final String context = col + ", " + testCase[1] + " -> " + testCase[2] + (isReplayed ? ", replayed" : "");
                createIndexDroppedAtTxnZeroTables(col, testCase[1]);
                final String before;
                if (isReplayed) {
                    try (TableReader ignored = getReader("pt")) {
                        execute("ALTER TABLE pt ALTER COLUMN " + col + " DROP INDEX");
                        try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                            runPurgeJob(purgeJob);
                            Assert.assertEquals(context, 1, purgeJob.getOutstandingPurgeTasks());
                        }
                        reAddIndexAfterTxnZero(col, testCase[2]);
                    }
                    before = listFiles(defaultDir("pt"));
                    engine.releaseInactive();
                    try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                        Assert.assertEquals(context, 0, purgeJob.getOutstandingPurgeTasks());
                    }
                } else {
                    try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                        try (TableReader ignored = getReader("pt")) {
                            execute("ALTER TABLE pt ALTER COLUMN " + col + " DROP INDEX");
                            runPurgeJob(purgeJob);
                            Assert.assertEquals(context, 1, purgeJob.getOutstandingPurgeTasks());
                            reAddIndexAfterTxnZero(col, testCase[2]);
                        }
                        before = listFiles(defaultDir("pt"));
                        runPurgeJob(purgeJob);
                        Assert.assertEquals(context, 0, purgeJob.getOutstandingPurgeTasks());
                    }
                }
                assertIndexReAddedAtTxnZeroBeforePurge(context, col, testCase[1].startsWith("INDEX TYPE POSTING"), before);
                Assert.assertEquals(context, withoutFilesOfInstanceNone(before, col), listFiles(defaultDir("pt")));
                Assert.assertEquals(context, 0, getColumnNameTxnOfDefault("pt", col));
                assertRowsOfIndexReAddedAtTxnZero(context, col, ROWS_OF_INDEX_RE_ADDED_AT_TXN_ZERO, "3\t6.0");

                // a new seal after the purge, and the rows of a reopened table
                execute("INSERT INTO pt VALUES " + THIRD_ROW_OF_INDEX_RE_ADDED_AT_TXN_ZERO);
                execute("INSERT INTO tw VALUES " + THIRD_ROW_OF_INDEX_RE_ADDED_AT_TXN_ZERO);
                final String rowsAfterThirdCommit = ROWS_OF_INDEX_RE_ADDED_AT_TXN_ZERO + "2024-01-01T05:00:00.000000Z\ty\t5.5\ta5\t50\n";
                assertRowsOfIndexReAddedAtTxnZero(context, col, rowsAfterThirdCommit, "4\t11.5");
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                assertRowsOfIndexReAddedAtTxnZero(context, col, rowsAfterThirdCommit, "4\t11.5");
                execute("DROP TABLE pt");
                execute("DROP TABLE tw");
            }
        });
    }

    @Test
    public void testPurgeDuringSealKeepsPostingCoverFilesOfIndexReAddedAtTxnZero() throws Exception {
        // The purge job runs the deferred purge of instance NONE of s on another thread while the writer
        // seals the first .pc files of instance 0: s.pc0.0.<seal> is on disk, s.pc1.0.<seal> is next.
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicReference<ColumnPurgeJob> purgeJobRef = new AtomicReference<>();
        final AtomicReference<Throwable> purgeError = new AtomicReference<>();
        final AtomicBoolean hasPurged = new AtomicBoolean();
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isArmed.get() && Utf8s.containsAscii(name, Files.SEPARATOR + "s.pc1.0.") && isArmed.compareAndSet(true, false)) {
                    final CountDownLatch sealStarted = new CountDownLatch(1);
                    final Thread purgeThread = new Thread(() -> {
                        try {
                            sealStarted.await();
                            purgeJobRef.get().run();
                            hasPurged.set(true);
                        } catch (Throwable th) {
                            purgeError.set(th);
                        }
                    });
                    purgeThread.start();
                    sealStarted.countDown();
                    try {
                        purgeThread.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                createIndexDroppedAtTxnZeroTables("s", "INDEX TYPE POSTING INCLUDE (d, vc)");
                try (TableReader ignored = getReader("pt")) {
                    execute("ALTER TABLE pt ALTER COLUMN s DROP INDEX");
                    runPurgeJob(purgeJob);
                    Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
                    execute("ALTER TABLE pt ADD COLUMN l LONG");
                    execute("ALTER TABLE pt ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (d, vc, l)");
                }
                engine.releaseInactive();
                setCurrentMicros(currentMicros + 10L * iteration++);
                purgeJobRef.set(purgeJob);
                isArmed.set(true);
                insertRowsOfIndexReAddedAtTxnZero("pt");
                Assert.assertFalse(isArmed.get());
                Assert.assertNull(purgeError.get());
                Assert.assertTrue(hasPurged.get());
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());

                execute("ALTER TABLE tw ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (d, vc, l)");
                insertRowsOfIndexReAddedAtTxnZero("tw");
                final String listing = listFiles(defaultDir("pt"));
                Assert.assertEquals(listing, "[]", filesOfInstanceNone(listing, "s").toString());
                assertFilesOfInstanceZero("after the purge", "s", listing);
                assertRowsOfIndexReAddedAtTxnZero("after the purge", "s", ROWS_OF_INDEX_RE_ADDED_AT_TXN_ZERO, "3\t6.0");
            }
        });
    }

    @Test
    public void testPurgeOfIndexDroppedAtTxnZeroRetriesWithoutRemovingReAddedCoverFiles() throws Exception {
        // The purge of instance NONE of s cannot remove s.pv.0 the first time. s.d and s.pk are gone
        // when it retries, so the retry finds the files of NONE through the directory scan, which also
        // reports the .pc files of instance 0: the retry removes s.pv.0 and keeps them. A second DROP
        // INDEX, after the first commit, purges instance 0 with every .pc file of the token 0.
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicBoolean hasFailed = new AtomicBoolean();
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                if (isArmed.get() && Utf8s.endsWithAscii(name, Files.SEPARATOR + "s.pv.0") && hasFailed.compareAndSet(false, true)) {
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                createIndexDroppedAtTxnZeroTables("s", "INDEX TYPE POSTING INCLUDE (d, vc)");
                try (TableReader ignored = getReader("pt")) {
                    execute("ALTER TABLE pt ALTER COLUMN s DROP INDEX");
                    runPurgeJob(purgeJob);
                    reAddIndexAfterTxnZero("s", "POSTING INCLUDE (d, vc, l)");
                }
                final String before = listFiles(defaultDir("pt"));
                assertIndexReAddedAtTxnZeroBeforePurge("before", "s", true, before);
                Assert.assertTrue(before, (" " + before + " ").contains(" s.pv.0 "));

                isArmed.set(true);
                engine.releaseInactive();
                setCurrentMicros(currentMicros + 10L * iteration++);
                purgeJob.run();
                Assert.assertTrue(hasFailed.get());
                Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
                Assert.assertEquals("[s.pv.0]", filesOfInstanceNone(listFiles(defaultDir("pt")), "s").toString());
                Assert.assertEquals(withoutFilesOfInstanceNone(before, "s"), withoutFilesOfInstanceNone(listFiles(defaultDir("pt")), "s"));
                assertRowsOfIndexReAddedAtTxnZero("failed purge", "s", ROWS_OF_INDEX_RE_ADDED_AT_TXN_ZERO, "3\t6.0");

                runPurgeJob(purgeJob);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                Assert.assertEquals(withoutFilesOfInstanceNone(before, "s"), listFiles(defaultDir("pt")));
                assertRowsOfIndexReAddedAtTxnZero("retried purge", "s", ROWS_OF_INDEX_RE_ADDED_AT_TXN_ZERO, "3\t6.0");

                // DROP INDEX after the first commit moves s to the table txn and purges instance 0
                final long dropTxn;
                try (TableReader reader = getReader("pt")) {
                    dropTxn = reader.getTxn();
                }
                Assert.assertTrue(dropTxn > 0);
                execute("ALTER TABLE pt ALTER COLUMN s DROP INDEX");
                runPurgeJob(purgeJob);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                final TreeSet<String> filesOfS = new TreeSet<>();
                for (String file : listFiles(defaultDir("pt")).split(" ")) {
                    if (file.startsWith("s.")) {
                        filesOfS.add(file);
                    }
                }
                Assert.assertEquals("[s.d." + dropTxn + "]", filesOfS.toString());
                Assert.assertEquals(dropTxn, getColumnNameTxnOfDefault("pt", "s"));
                assertQuery("SELECT ts, s, d, vc, l FROM pt WHERE s = 'y'")
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns("ts\ts\td\tvc\tl\n" + timestamps(ROWS_OF_INDEX_RE_ADDED_AT_TXN_ZERO));
            }
        });
    }

    @Test
    public void testPurgeOfIndexDroppedAtTxnZeroRetriesAfterReplay() throws Exception {
        // The purge job cannot remove s.k of the BITMAP index of s that DROP INDEX at txn 0 dropped, so
        // the task stays incomplete in the purge log. A new purge job replays it with the index type of
        // the current metadata, POSTING after ADD INDEX. s.d is gone and s.pk never existed, so the
        // directory scan finds only the .pc files of instance 0: the replay removes s.k and s.v and keeps
        // them.
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicBoolean hasFailed = new AtomicBoolean();
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                if (isArmed.get() && Utf8s.endsWithAscii(name, Files.SEPARATOR + "s.k") && hasFailed.compareAndSet(false, true)) {
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            createIndexDroppedAtTxnZeroTables("s", "INDEX");
            final String before;
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                try (TableReader ignored = getReader("pt")) {
                    execute("ALTER TABLE pt ALTER COLUMN s DROP INDEX");
                    runPurgeJob(purgeJob);
                    Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
                    reAddIndexAfterTxnZero("s", "POSTING INCLUDE (d, vc, l)");
                }
                before = listFiles(defaultDir("pt"));
                assertIndexReAddedAtTxnZeroBeforePurge("before", "s", false, before);

                isArmed.set(true);
                engine.releaseInactive();
                setCurrentMicros(currentMicros + 10L * iteration++);
                purgeJob.run();
                Assert.assertTrue(hasFailed.get());
                Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
                Assert.assertEquals("[s.k, s.v]", filesOfInstanceNone(listFiles(defaultDir("pt")), "s").toString());
                Assert.assertEquals(withoutFilesOfInstanceNone(before, "s"), withoutFilesOfInstanceNone(listFiles(defaultDir("pt")), "s"));
                assertIncompletePurgeLogRows(purgeJob, 1);
            }
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                Assert.assertEquals(withoutFilesOfInstanceNone(before, "s"), listFiles(defaultDir("pt")));
                assertRowsOfIndexReAddedAtTxnZero("replayed purge", "s", ROWS_OF_INDEX_RE_ADDED_AT_TXN_ZERO, "3\t6.0");
            }
        });
    }

    /**
     * The files of instance 0 of {@code col} in {@code listing}, as ADD INDEX ... INCLUDE (d, vc, l) after
     * DROP INDEX at txn 0 and two commits write them: d and vc have the column name txn NONE, l the txn 1.
     */
    private static void assertFilesOfInstanceZero(String context, String col, String listing) {
        final String paddedListing = " " + listing + " ";
        for (String file : new String[]{col + ".d.0", col + ".pk.0", col + ".pci.0"}) {
            Assert.assertTrue(context + ", " + file + " in " + listing, paddedListing.contains(" " + file + " "));
        }
        for (String coverFilePrefix : new String[]{col + ".pc0.0.", col + ".pc1.0.", col + ".pc2.0.1."}) {
            Assert.assertTrue(context + ", " + coverFilePrefix + " in " + listing, paddedListing.contains(" " + coverFilePrefix));
        }
    }

    /**
     * {@code listing} holds the files of instance 0 of {@code col} next to the data file and the index
     * key file of instance NONE, whose index was POSTING or BITMAP.
     */
    private static void assertIndexReAddedAtTxnZeroBeforePurge(String context, String col, boolean isOldIndexPosting, String listing) {
        final String paddedListing = " " + listing + " ";
        for (String file : new String[]{col + ".d", isOldIndexPosting ? col + ".pk" : col + ".k"}) {
            Assert.assertTrue(context + ", " + file + " in " + listing, paddedListing.contains(" " + file + " "));
        }
        assertFilesOfInstanceZero(context, col, listing);
    }

    private void assertLiveFilesOfPartitions2To4(String context, String[] liveFiles) {
        for (int i = 0; i < 3; i++) {
            Assert.assertEquals(context, liveFiles[i], listFiles(partitionDir("t_pci", i + 2)));
        }
    }

    /**
     * The rows of t_pci after UPDATE ... SET sym = 'Z' WHERE ts >= '1970-01-03', through the index of
     * sym and through a filter that reads no index.
     */
    private void assertRowsOfCoverInfoTable(String context) throws Exception {
        final String query = "SELECT ts, sym, x, d, vc, str FROM t_pci WHERE sym = 'Z'";
        try {
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix("""
                            ts\tsym\tx\td\tvc\tstr
                            1970-01-03T02:00:00.000000Z\tZ\t3\t3.5\tv3\ts3
                            1970-01-04T02:00:00.000000Z\tZ\t4\t4.5\t\u00e94\ts4
                            1970-01-05T02:00:00.000000Z\tZ\t5\tnull\t\t
                            """, timestampType.getTypeName()));
            assertSqlCursors("SELECT ts, sym, x, d, vc, str FROM t_pci WHERE sym::STRING = 'Z'", query);
        } catch (AssertionError e) {
            throw new AssertionError(context, e);
        }
    }

    /**
     * Creates t_pci with one row in each of five daily partitions and a POSTING index on sym that covers
     * {@code includeClause}, so each partition holds sym.pci.
     */
    private void createCoverInfoTable(String walMode, String includeClause) throws SqlException {
        execute("CREATE TABLE t_pci (ts " + timestampType.getTypeName() + ", x LONG, d DOUBLE, vc VARCHAR, str STRING, sym SYMBOL)"
                + " TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        execute("""
                INSERT INTO t_pci VALUES
                ('1970-01-01T02:00:00.000000Z', 1, 1.5, 'v1', 's1', 'A'),
                ('1970-01-02T02:00:00.000000Z', 2, 2.5, 'v2', 's2', 'B'),
                ('1970-01-03T02:00:00.000000Z', 3, 3.5, 'v3', 's3', 'C'),
                ('1970-01-04T02:00:00.000000Z', 4, 4.5, '\u00e94', 's4', 'A'),
                ('1970-01-05T02:00:00.000000Z', 5, NULL, NULL, NULL, 'B')""");
        execute("ALTER TABLE t_pci ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (" + includeClause + ")");
        drainWalQueue();
    }

    private static String coverInfoFileName(String columnName, long columnNameTxn) {
        try (Path path = new Path()) {
            return Utf8s.stringFromUtf8Bytes(PostingIndexUtils.coverInfoFileName(path.of(""), columnName, columnNameTxn));
        }
    }

    // the sym.* files of the last three partitions of t_pci, space-separated in name order
    private String[] filesOfSymInPartitions2To4(String context) {
        final String[] files = new String[3];
        for (int i = 0; i < 3; i++) {
            files[i] = String.join(" ", filesWithPrefix(listFiles(partitionDir("t_pci", i + 2)), "sym."));
            Assert.assertTrue(context + ", " + files[i], (" " + files[i] + " ").contains(" sym.pv."));
        }
        return files;
    }

    private static TreeSet<String> filesWithPrefix(String listing, String prefix) {
        final TreeSet<String> files = new TreeSet<>();
        for (String file : listing.split(" ")) {
            if (file.startsWith(prefix)) {
                files.add(file);
            }
        }
        return files;
    }

    private static long getColumnNameTxn(String tableName, String col, int partitionIndex) {
        try (TableReader reader = getReader(tableName)) {
            final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(col));
            return reader.getColumnVersionReader().getColumnNameTxn(reader.getPartitionTimestampByIndex(partitionIndex), writerIndex);
        }
    }

    // the space-separated file names of the listings, in name order
    private static String joinSorted(String... listings) {
        final TreeSet<String> files = new TreeSet<>();
        for (String listing : listings) {
            files.addAll(Arrays.asList(listing.split(" ")));
        }
        return String.join(" ", files);
    }

    /**
     * The files of the last three partitions of t_pci without {@code oldInstanceFiles}, which each
     * partition still holds.
     */
    private String[] liveFilesOfPartitions2To4(String context, String[] oldInstanceFiles) {
        final String[] liveFiles = new String[3];
        for (int i = 0; i < 3; i++) {
            final String listing = listFiles(partitionDir("t_pci", i + 2));
            final TreeSet<String> files = new TreeSet<>(Arrays.asList(listing.split(" ")));
            Assert.assertTrue(context + ", " + listing, files.containsAll(Arrays.asList(oldInstanceFiles[i].split(" "))));
            files.removeAll(Arrays.asList(oldInstanceFiles[i].split(" ")));
            liveFiles[i] = String.join(" ", files);
        }
        return liveFiles;
    }

    /**
     * A files facade that fails the next {@code remainingFailures} removals of the file whose path ends
     * with {@code failingFileSuffix}, and counts them in {@code failedRemoveCount}.
     */
    private static TestFilesFacadeImpl newFailingRemoveFilesFacade(
            AtomicReference<String> failingFileSuffix,
            AtomicInteger remainingFailures,
            AtomicInteger failedRemoveCount
    ) {
        return new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                final String suffix = failingFileSuffix.get();
                if (suffix != null && Utf8s.endsWithAscii(name, suffix) && remainingFailures.get() > 0) {
                    remainingFailures.decrementAndGet();
                    failedRemoveCount.incrementAndGet();
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
    }

    private void assertIncompletePurgeLogRows(ColumnPurgeJob purgeJob, int expectedCount) throws Exception {
        assertQuery("SELECT count() c FROM \"" + purgeJob.getLogTableName() + "\" WHERE completed = null")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("c\n" + expectedCount + "\n");
    }

    private static String defaultDir(String tableName) {
        return new File(new File(configuration.getDbRoot().toString(), engine.verifyTableName(tableName).getDirName()), "default").getAbsolutePath();
    }

    /**
     * The files in {@code listing} that instance NONE of {@code col} owns in an empty table: the data
     * file, the BITMAP index files, and the POSTING key, cover info and first value files.
     */
    private static TreeSet<String> filesOfInstanceNone(String listing, String col) {
        final TreeSet<String> namesOfNone = new TreeSet<>();
        try (Path path = new Path()) {
            namesOfNone.add(Utf8s.stringFromUtf8Bytes(TableUtils.dFile(path.of(""), col, TableUtils.COLUMN_NAME_TXN_NONE)));
            namesOfNone.add(Utf8s.stringFromUtf8Bytes(BitmapIndexUtils.keyFileName(path.of(""), col, TableUtils.COLUMN_NAME_TXN_NONE)));
            namesOfNone.add(Utf8s.stringFromUtf8Bytes(BitmapIndexUtils.valueFileName(path.of(""), col, TableUtils.COLUMN_NAME_TXN_NONE)));
            namesOfNone.add(Utf8s.stringFromUtf8Bytes(PostingIndexUtils.keyFileName(path.of(""), col, TableUtils.COLUMN_NAME_TXN_NONE)));
            namesOfNone.add(Utf8s.stringFromUtf8Bytes(PostingIndexUtils.coverInfoFileName(path.of(""), col, TableUtils.COLUMN_NAME_TXN_NONE)));
            namesOfNone.add(Utf8s.stringFromUtf8Bytes(PostingIndexUtils.valueFileName(path.of(""), col, TableUtils.COLUMN_NAME_TXN_NONE, 0)));
        }
        final TreeSet<String> files = new TreeSet<>();
        for (String file : listing.split(" ")) {
            if (namesOfNone.contains(file)) {
                files.add(file);
            }
        }
        return files;
    }

    private static long getColumnNameTxnOfDefault(String tableName, String col) {
        try (TableReader reader = getReader(tableName)) {
            final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(col));
            return reader.getColumnVersionReader().getColumnNameTxn(reader.getPartitionTimestampByIndex(0), writerIndex);
        }
    }

    private static String withoutFilesOfInstanceNone(String listing, String col) {
        final TreeSet<String> filesOfNone = filesOfInstanceNone(listing, col);
        final StringBuilder remaining = new StringBuilder();
        for (String file : listing.split(" ")) {
            if (!filesOfNone.contains(file)) {
                if (!remaining.isEmpty()) {
                    remaining.append(' ');
                }
                remaining.append(file);
            }
        }
        return remaining.toString();
    }

    /**
     * Asserts the rows of {@code col} = 'y' in pt through its covering index, the same rows through a
     * filter that reads no index and through the twin tw, which never dropped its index, and their
     * count and sum of d.
     */
    private void assertRowsOfIndexReAddedAtTxnZero(String context, String col, String rowsOfY, String countAndSumOfY) throws Exception {
        final String projection = "ts, " + col + ", d, vc, l";
        final String query = "SELECT " + projection + " FROM pt WHERE " + col + " = 'y'";
        try {
            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex on: " + col + " with: ts, d, vc, l")
                    .returns("ts\t" + col + "\td\tvc\tl\n" + timestamps(rowsOfY));
            assertSqlCursors("SELECT " + projection + " FROM pt WHERE " + col + "::STRING = 'y'", query);
            assertSqlCursors(query.replace("FROM pt", "FROM tw"), query);
            final String inQuery = "SELECT " + projection + " FROM {t} WHERE " + col + " IN ('y', 'x')";
            assertSqlCursors(inQuery.replace("{t}", "tw"), inQuery.replace("{t}", "pt"));
            assertQuery("SELECT count() c, sum(d) sd FROM pt WHERE " + col + " = 'y'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("CoveringIndex on: " + col)
                    .returns("c\tsd\n" + countAndSumOfY + "\n");
        } catch (AssertionError e) {
            throw new AssertionError(context, e);
        }
    }

    /**
     * Creates pt, not partitioned, with {@code col} indexed by {@code oldIndexClause}, and its twin tw
     * with the columns pt has after {@link #reAddIndexAfterTxnZero}, without an index.
     */
    private void createIndexDroppedAtTxnZeroTables(String col, String oldIndexClause) throws SqlException {
        final String ts = timestampType.getTypeName();
        execute("CREATE TABLE pt (v INT, ts " + ts + ", " + col + " SYMBOL " + oldIndexClause + ", d DOUBLE, vc VARCHAR) TIMESTAMP(ts)");
        execute("CREATE TABLE tw (v INT, ts " + ts + ", " + col + " SYMBOL, d DOUBLE, vc VARCHAR, l LONG) TIMESTAMP(ts)");
    }

    /**
     * The first commit seals one row, the second three rows, one with NULL covered values. A table
     * that is not partitioned takes no O3 rows.
     */
    private void insertRowsOfIndexReAddedAtTxnZero(String tableName) throws SqlException {
        execute("INSERT INTO " + tableName + " VALUES (1, '2024-01-01T01:00:00.000000Z', 'y', 1.5, 'a1', 10)");
        execute("INSERT INTO " + tableName + """
                 VALUES (2, '2024-01-01T02:00:00.000000Z', 'y', NULL, NULL, NULL),
                (3, '2024-01-01T03:00:00.000000Z', 'x', 3.5, 'a3', 30), (4, '2024-01-01T04:00:00.000000Z', 'y', 4.5, '\u00e94', 40)
                """);
    }

    /**
     * After DROP INDEX at txn 0 committed: adds l to pt at txn 1, indexes {@code col} of pt and tw with
     * {@code newIndexClause}, which covers l, and inserts the same rows into both.
     */
    private void reAddIndexAfterTxnZero(String col, String newIndexClause) throws SqlException {
        execute("ALTER TABLE pt ADD COLUMN l LONG");
        for (String tableName : new String[]{"pt", "tw"}) {
            execute("ALTER TABLE " + tableName + " ALTER COLUMN " + col + " ADD INDEX TYPE " + newIndexClause);
            insertRowsOfIndexReAddedAtTxnZero(tableName);
        }
    }

    private String timestamps(String rows) {
        return timestampType == TestTimestampType.NANO ? rows.replace(".000000Z", ".000000000Z") : rows;
    }

    @Test
    public void testPurgeKeepsPostingFilesOfColumnConvertedAtTxnZero() throws Exception {
        // Guard: ALTER COLUMN TYPE at txn 0 writes s.d.0 and s.i.0 next to the POSTING files of instance
        // NONE. The purge of NONE removes them and keeps the files of the STRING column.
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("CREATE TABLE pt (v INT, ts " + timestampType.getTypeName() + ", s SYMBOL INDEX TYPE POSTING INCLUDE (d, vc), d DOUBLE, vc VARCHAR) TIMESTAMP(ts)");
                final String before;
                try (TableReader ignored = getReader("pt")) {
                    execute("ALTER TABLE pt ALTER COLUMN s TYPE STRING");
                    runPurgeJob(purgeJob);
                    Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
                    before = listFiles(defaultDir("pt"));
                }
                Assert.assertEquals("[s.d, s.pk, s.pv.0]", filesOfInstanceNone(before, "s").toString());
                Assert.assertTrue(before, (" " + before + " ").contains(" s.d.0 "));
                Assert.assertTrue(before, (" " + before + " ").contains(" s.i.0 "));
                runPurgeJob(purgeJob);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                Assert.assertEquals(withoutFilesOfInstanceNone(before, "s"), listFiles(defaultDir("pt")));
                execute("INSERT INTO pt VALUES (1, '2024-01-01T01:00:00.000000Z', 'y', 1.5, 'a1'), (2, '2024-01-01T02:00:00.000000Z', 'x', 2.5, 'a2')");
                assertQuery("SELECT v, s, d, vc FROM pt WHERE s = 'y'")
                        .noLeakCheck()
                        .returns("v\ts\td\tvc\n1\ty\t1.5\ta1\n");
            }
        });
    }

    /**
     * The live seal of the covering index of {@code columnName} in {@code dir}, the newest
     * {@code <col>.pv.<txn>.<seal>}, has a .pc file for each covered column d, vc and ts:
     * {@code <col>.pc<N>.<txn>[.<covered txn>].<seal>}, N in 0..2. Every .pc file belongs to a seal
     * that has its .pv file.
     */
    private static void assertCoverFilesPerSeal(String context, String dir, String columnName) {
        final String[] names = new File(dir).list();
        Assert.assertNotNull(names);
        final String failureContext = context + ", " + Arrays.toString(names);
        final TreeSet<Long> valueFileSeals = new TreeSet<>();
        for (String name : names) {
            if (name.startsWith(columnName + ".pv.")) {
                valueFileSeals.add(Long.parseLong(name.substring(name.lastIndexOf('.') + 1)));
            }
        }
        Assert.assertFalse(failureContext, valueFileSeals.isEmpty());
        final long liveSeal = valueFileSeals.last();
        final TreeSet<String> liveCoverFiles = new TreeSet<>();
        for (String name : names) {
            if (name.startsWith(columnName + ".pc") && !name.startsWith(columnName + ".pci")) {
                final long seal = Long.parseLong(name.substring(name.lastIndexOf('.') + 1));
                Assert.assertTrue(failureContext, valueFileSeals.contains(seal));
                if (seal == liveSeal) {
                    liveCoverFiles.add(name.substring(columnName.length() + 1, columnName.length() + 4));
                }
            }
        }
        Assert.assertEquals(failureContext, "[pc0, pc1, pc2]", liveCoverFiles.toString());
    }

    private static int coverDataFileCount(String dir, String columnName) {
        final String[] names = new File(dir).list((_, name) -> name.startsWith(columnName + ".pc") && !name.startsWith(columnName + ".pci"));
        Assert.assertNotNull(names);
        return names.length;
    }

    /**
     * Creates t_z, where s is the first column added to the empty table (column name txn 0), with a
     * covering index on s, and t_twin, which declares s in CREATE TABLE, with the same index and rows
     * in partitions 2024-01-01 and 2024-01-02.
     */
    private void createTxnZeroCoveringTables(String walMode) throws SqlException {
        final String ts = timestampType.getTypeName();
        execute("CREATE TABLE t_z (v INT, ts " + ts + ") TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        execute("ALTER TABLE t_z ADD COLUMN s SYMBOL");
        execute("ALTER TABLE t_z ADD COLUMN d DOUBLE");
        execute("ALTER TABLE t_z ADD COLUMN vc VARCHAR");
        execute("CREATE TABLE t_twin (v INT, ts " + ts + ", s SYMBOL, d DOUBLE, vc VARCHAR) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        for (String table : new String[]{"t_z", "t_twin"}) {
            execute("ALTER TABLE " + table + " ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (d, vc)");
            execute("INSERT INTO " + table + """
                     VALUES (1, '2024-01-01T01:00:00.000000Z', 'x', 1.5, 'vc1'), (2, '2024-01-01T02:00:00.000000Z', 'y', 2.5, 'vc2'),
                    (3, '2024-01-01T03:00:00.000000Z', NULL, NULL, NULL), (4, '2024-01-01T04:00:00.000000Z', 'x', 4.5, 'vc4'),
                    (5, '2024-01-02T01:00:00.000000Z', 'y', 5.5, 'vc5'), (6, '2024-01-02T02:00:00.000000Z', 'x', 8.5, 'vc6')
                    """);
        }
        drainWalQueue();
        try (TableReader reader = getReader("t_z")) {
            final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("s"));
            Assert.assertEquals(0, reader.getColumnVersionReader().getColumnNameTxn(reader.getPartitionTimestampByIndex(0), writerIndex));
        }
    }

    private String partitionDir(String tableName, int partitionIndex) {
        try (TableReader reader = getReader(tableName); Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(reader.getTableToken());
            TableUtils.setPathForNativePartition(
                    path,
                    reader.getMetadata().getTimestampType(),
                    PartitionBy.DAY,
                    reader.getTxFile().getPartitionTimestampByIndex(partitionIndex),
                    reader.getTxFile().getPartitionNameTxn(partitionIndex)
            );
            return path.toString();
        }
    }

    /**
     * The index files of column version 0 of s in {@code dir}: s.pk.0, s.pci.0, s.pv.0.<seal> and
     * s.pc<N>.0[.<covered txn>].<seal>.
     */
    private static TreeSet<String> txnZeroIndexFilesOfS(String dir) {
        final String[] names = new File(dir).list();
        Assert.assertNotNull(names);
        final TreeSet<String> files = new TreeSet<>();
        for (String name : names) {
            if (name.equals("s.pk.0") || name.equals("s.pci.0") || name.startsWith("s.pv.0.") || name.matches("s\\.pc[0-9]+\\.0\\..*")) {
                files.add(name);
            }
        }
        return files;
    }

    private void assertPartitionFileExists(Path path, String partition, String fileName, boolean exist) {
        TableToken tt = engine.verifyTableName("t_purge_leak");
        path.of(configuration.getDbRoot()).concat(tt).concat(partition).concat(fileName).$();
        Assert.assertEquals(Utf8s.toString(path), exist, TestFilesFacadeImpl.INSTANCE.exists(path.$()));
    }

    @Test
    public void testReplayedPurgeRemovesPostingFiles() throws Exception {
        // Regression test for ColumnPurgeJob.processTableRecords using the wrong
        // table token when re-deriving indexType on restart-replay. The buggy
        // code reads metadata from the purge-log table; for any user column
        // not coincidentally named like a log column, indexType silently
        // defaults to BITMAP and the POSTING-specific .pk file is never
        // removed by replayed purge tasks.
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("CREATE TABLE t_posting_replay AS" +
                        " (SELECT timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym" +
                        " FROM long_sequence(5))" +
                        " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("ALTER TABLE t_posting_replay ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (x)");

                try (TableReader rdr = getReader("t_posting_replay")) {
                    update("UPDATE t_posting_replay SET sym = 'Z' WHERE ts >= '1970-01-03'");
                    runPurgeJob(purgeJob); // task persisted to log; purge blocked by reader
                    rdr.openPartition(0);
                }
                // Reader released, but purge job is closing without re-running purge,
                // so the persisted task survives to be replayed by the next instance.
            }

            try (Path path = new Path()) {
                // The bare sym.pk file (column version 0) is OLD after the UPDATE
                // bumped the column to version 2. It must still exist before
                // replay because purge was blocked by the open reader.
                assertPostingKeyFileExists(path, "t_posting_replay", "1970-01-03", true);
                assertPostingKeyFileExists(path, "t_posting_replay", "1970-01-04", true);
                assertPostingKeyFileExists(path, "t_posting_replay", "1970-01-05", true);

                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    // Constructor's processTableRecords replays the persisted task.
                    // With the fix, metadata is read from the user table and
                    // indexType=POSTING, so the .pk file is purged.
                    // Without the fix, indexType defaulted to BITMAP because
                    // metadata was read from the purge-log table (no such
                    // column), and .pk would survive.
                    assertPostingKeyFileExists(path, "t_posting_replay", "1970-01-03", false);
                    assertPostingKeyFileExists(path, "t_posting_replay", "1970-01-04", false);
                    assertPostingKeyFileExists(path, "t_posting_replay", "1970-01-05", false);
                    Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                }
            }
        });
    }

    @Test
    public void testReplayedPurgeRemovesOrphanPostingSidecarsOfEveryPartition() throws Exception {
        // Same shape as testPurgeJobRemovesOrphanPostingSidecarsOfEveryPartition, with the purge task
        // replayed from the purge log by a new purge job.
        assertMemoryLeak(() -> {
            final String[] liveFiles;
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                liveFiles = createOrphanPostingSidecars("replay", purgeJob, true);
            }
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                assertOrphanPostingSidecarsPurged("replay", liveFiles);
                assertQuery(purgeJob.getLogTableName())
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n");
            }
        });
    }

    private static String listFiles(String dir) {
        final String[] names = new File(dir).list();
        Assert.assertNotNull(dir, names);
        Arrays.sort(names);
        return String.join(" ", names);
    }

    private static TreeSet<String> symbolRootFiles(File tableDir) {
        final String[] names = tableDir.list((dir, name) -> name.startsWith("sym."));
        Assert.assertNotNull(names);
        return new TreeSet<>(Arrays.asList(names));
    }

    private void assertOrphanPostingSidecarsPurged(String context, String[] liveFiles) throws Exception {
        for (int i = 0; i < liveFiles.length; i++) {
            Assert.assertEquals(context, liveFiles[i], listFiles(partitionDir("t_orphan", i + 2)));
        }
        assertQuery("SELECT ts, sym, x FROM t_orphan WHERE sym = 'Z'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .timestamp("ts")
                .withPlanContaining("CoveringIndex on: sym with: ts, x")
                .returns(replaceTimestampSuffix("""
                        ts\tsym\tx
                        1970-01-03T02:00:00.000000Z\tZ\t3
                        1970-01-04T02:00:00.000000Z\tZ\t4
                        1970-01-05T02:00:00.000000Z\tZ\t5
                        """, timestampType.getTypeName()));
    }

    /**
     * Creates t_orphan with a POSTING INCLUDE index on sym in five daily partitions, and updates sym in
     * the last three with a reader open, so the purge job keeps the purge task of the old instance of
     * sym. Then deletes sym.d and sym.pk of the old instance in the last two of those partitions, and
     * in the first one too when {@code isFirstPartitionOrphaned}. Returns the files of the three
     * partitions without the old instance.
     */
    private String[] createOrphanPostingSidecars(String context, ColumnPurgeJob purgeJob, boolean isFirstPartitionOrphaned) throws SqlException {
        execute("DROP TABLE IF EXISTS t_orphan");
        execute("CREATE TABLE t_orphan AS" +
                " (SELECT timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                " x," +
                " rnd_symbol('A', 'B', 'C', 'D') sym" +
                " FROM long_sequence(5))" +
                " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("ALTER TABLE t_orphan ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (x)");
        final String[] oldInstanceFiles = new String[3];
        for (int i = 0; i < 3; i++) {
            final TreeSet<String> files = new TreeSet<>();
            for (String file : listFiles(partitionDir("t_orphan", i + 2)).split(" ")) {
                if (file.startsWith("sym.")) {
                    files.add(file);
                }
            }
            Assert.assertTrue(context + ", " + files, files.containsAll(Arrays.asList("sym.d", "sym.pci", "sym.pk")));
            oldInstanceFiles[i] = String.join(" ", files);
        }

        try (TableReader ignore = getReader("t_orphan")) {
            update("UPDATE t_orphan SET sym = 'Z' WHERE ts >= '1970-01-03'");
            // the reader holds the old instance
            runPurgeJob(purgeJob);
        }

        final String[] liveFiles = new String[3];
        for (int i = 0; i < 3; i++) {
            final String dir = partitionDir("t_orphan", i + 2);
            final TreeSet<String> files = new TreeSet<>(Arrays.asList(listFiles(dir).split(" ")));
            Assert.assertTrue(context, files.containsAll(Arrays.asList(oldInstanceFiles[i].split(" "))));
            files.removeAll(Arrays.asList(oldInstanceFiles[i].split(" ")));
            liveFiles[i] = String.join(" ", files);
            if (i > 0 || isFirstPartitionOrphaned) {
                Assert.assertTrue(new File(dir, "sym.d").delete());
                Assert.assertTrue(new File(dir, "sym.pk").delete());
            }
        }
        return liveFiles;
    }

    private void assertPostingKeyFileExists(Path path, String tableName, String partition, boolean exist) {
        TableToken tt = engine.verifyTableName(tableName);
        path.of(configuration.getDbRoot()).concat(tt).concat(partition).concat("sym").put(".pk").$();
        Assert.assertEquals(Utf8s.toString(path), exist, TestFilesFacadeImpl.INSTANCE.exists(path.$()));
    }

    private void assertIndexFilesExist(String[] partitions, Path path, String colSuffix, boolean exist) {
        for (int i = 0; i < partitions.length; i++) {
            String partition = partitions[i];
            assertIndexFilesExist(path, "up_part_o3_many", partition, colSuffix, exist);
        }
    }

    private void assertIndexFilesExist(Path path, String up_part, String partition, String colSuffix, boolean exist) {
        TableToken tableToken = engine.verifyTableName(up_part);
        path.of(configuration.getDbRoot()).concat(tableToken).concat(partition).concat("sym2.d").put(colSuffix).$();
        Assert.assertEquals(Utf8s.toString(path), exist, TestFilesFacadeImpl.INSTANCE.exists(path.$()));

        path.of(configuration.getDbRoot()).concat(tableToken).concat(partition).concat("sym2.k").put(colSuffix).$();
        Assert.assertEquals(Utf8s.toString(path), exist, TestFilesFacadeImpl.INSTANCE.exists(path.$()));

        path.of(configuration.getDbRoot()).concat(tableToken).concat(partition).concat("sym2.v").put(colSuffix).$();
        Assert.assertEquals(Utf8s.toString(path), exist, TestFilesFacadeImpl.INSTANCE.exists(path.$()));
    }

    @NotNull
    private ColumnPurgeJob createPurgeJob() throws SqlException {
        return new ColumnPurgeJob(engine);
    }

    private void createTable(String upPartO3) throws SqlException {
        execute("create table " + upPartO3 + " as" +
                " (select timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts," +
                " x," +
                " rnd_str('a', 'b', 'c', 'd') str," +
                " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                " rnd_symbol('1', '2', '3', '4') sym2" +
                " from long_sequence(5)), index(sym2)" +
                " timestamp(ts) PARTITION BY DAY WAL");
    }

    private ColumnPurgeTask createTask(
            TableToken tblName,
            String colName,
            int tableId,
            int columnType,
            long updateTxn,
            long columnVersion,
            String partitionTs,
            long partitionNameTxn
    ) throws NumericException {
        ColumnPurgeTask tsk = new ColumnPurgeTask();
        TimestampDriver timestampDriver = timestampType.getDriver();
        tsk.of(tblName, colName, tableId, 0, columnType, IndexType.BITMAP, timestampDriver.getTimestampType(), PartitionBy.NONE, updateTxn, new LongList());
        tsk.appendColumnInfo(columnVersion, timestampDriver.parseFloorLiteral(partitionTs), partitionNameTxn);
        return tsk;
    }

    private void runPurgeJob(ColumnPurgeJob purgeJob) {
        engine.releaseInactive();
        setCurrentMicros(currentMicros + 10L * iteration++);
        purgeJob.run();
        setCurrentMicros(currentMicros + 10L * iteration++);
        purgeJob.run();
    }

    private void update(String updateSql) throws SqlException {
        execute(updateSql);
    }

    @Test
    public void testReloadingPurgeLogWithDeletedTableSameTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTable("tab");
            drainWalQueue();

            // hog table reader and update the table
            try (TableReader ignore = getReader("tab")) {
                update("UPDATE tab SET x = 100");
                drainWalQueue();

                // create purge job and run it - this will create a purge record
                // but won't purge because the reader is open
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    runPurgeJob(purgeJob);
                }
            }

            // drop the table while purge log records still exist
            execute("drop table tab");

            // if we get here without NPE, the bug is fixed
            createPurgeJob().close();
        });
    }
}
