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
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.mp.Sequence;
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

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ColumnPurgeJobTest extends AbstractCairoTest {
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
                    assertIndexFilesExist(partitions, path, "up_part_o3_many", "", true);

                    runPurgeJob(purgeJob);

                    assertIndexFilesExist(partitions, path, "up_part_o3_many", "", false);
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
                    assertIndexFilesExist(partitions, path, "up_part_o3_many", "", true);

                    runPurgeJob(purgeJob);

                    assertIndexFilesExist(partitions, path, "up_part_o3_many", "", false);
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
                    assertIndexFilesExist(partitions, path, "up_part_o3_many", ".2", true);

                    runPurgeJob(purgeJob);

                    assertIndexFilesExist(partitions, path, "up_part_o3_many", ".2", false);
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

            assertSql(
                    replaceTimestampSuffix("ts\tx\tstr\tsym1\tsym2\n" +
                            "1970-01-01T02:00:00.000000Z\t1\ta\tA\t2\n" +
                            "1970-01-02T02:00:00.000000Z\t2\tb\tC\t4\n" +
                            "1970-01-03T02:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                            "1970-01-04T02:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                            "1970-01-05T02:00:00.000000Z\t100\tabcd\tD\tEE\n", timestampType.getTypeName()),
                    "up_part_o3_2"
            );

            // cleaned everything, table is truncated
            assertSql("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n", purgeLogTableName);

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

                assertSql(
                        replaceTimestampSuffix("ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T02:00:00.000000Z\t1\ta\tC\t2\n" +
                                "1970-01-02T01:00:00.000000Z\t1\ta\tA\t2\n" +
                                "1970-01-02T02:00:00.000000Z\t2\td\tB\t4\n" +
                                "1970-01-03T01:00:00.000000Z\t200\tu2\tC\tEE\n" +
                                "1970-01-03T02:00:00.000000Z\t200\tu2\tD\tEE\n" +
                                "1970-01-04T01:00:00.000000Z\t200\tu2\tA\tEE\n" +
                                "1970-01-04T02:00:00.000000Z\t200\tu2\tA\tEE\n" +
                                "1970-01-05T02:00:00.000000Z\t200\tu2\tD\tEE\n", timestampType.getTypeName()), "up_part_o3_many"
                );

                assertSql("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
                        "1970-01-01T00:00:00.000010Z\tup_part_o3_many~\tx\t2\t0\t6\t0\t3\t-1\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z\n" +
                        "1970-01-01T00:00:00.000010Z\tup_part_o3_many~\tx\t2\t0\t6\t0\t3\t-1\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z\n" +
                        "1970-01-01T00:00:00.000010Z\tup_part_o3_many~\tx\t2\t0\t6\t0\t3\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000151Z\n" +
                        "1970-01-01T00:00:00.000011Z\tup_part_o3_many~\tstr\t2\t0\t11\t0\t3\t-1\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z\n" +
                        "1970-01-01T00:00:00.000011Z\tup_part_o3_many~\tstr\t2\t0\t11\t0\t3\t-1\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z\n" +
                        "1970-01-01T00:00:00.000011Z\tup_part_o3_many~\tstr\t2\t0\t11\t0\t3\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000151Z\n" +
                        "1970-01-01T00:00:00.000012Z\tup_part_o3_many~\tsym2\t2\t0\t12\t0\t3\t-1\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z\n" +
                        "1970-01-01T00:00:00.000012Z\tup_part_o3_many~\tsym2\t2\t0\t12\t0\t3\t-1\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z\n" +
                        "1970-01-01T00:00:00.000012Z\tup_part_o3_many~\tsym2\t2\t0\t12\t0\t3\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000151Z\n" +
                        "1970-01-01T00:00:00.000061Z\tup_part_o3_many~\tx\t2\t0\t6\t0\t4\t2\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z\n" +
                        "1970-01-01T00:00:00.000061Z\tup_part_o3_many~\tx\t2\t0\t6\t0\t4\t2\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z\n" +
                        "1970-01-01T00:00:00.000061Z\tup_part_o3_many~\tx\t2\t0\t6\t0\t4\t2\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000151Z\n" +
                        "1970-01-01T00:00:00.000062Z\tup_part_o3_many~\tstr\t2\t0\t11\t0\t4\t2\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z\n" +
                        "1970-01-01T00:00:00.000062Z\tup_part_o3_many~\tstr\t2\t0\t11\t0\t4\t2\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z\n" +
                        "1970-01-01T00:00:00.000062Z\tup_part_o3_many~\tstr\t2\t0\t11\t0\t4\t2\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000151Z\n" +
                        "1970-01-01T00:00:00.000063Z\tup_part_o3_many~\tsym2\t2\t0\t12\t0\t4\t2\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z\n" +
                        "1970-01-01T00:00:00.000063Z\tup_part_o3_many~\tsym2\t2\t0\t12\t0\t4\t2\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000151Z\n" +
                        "1970-01-01T00:00:00.000063Z\tup_part_o3_many~\tsym2\t2\t0\t12\t0\t4\t2\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000151Z\n", purgeJob.getLogTableName()
                );
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

                purgeJob.run(0);
                assertSql("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
                        "1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t11\t2022-03-29T00:00:00.000000Z\t-1\t\n" +
                        "1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t-1\t2022-04-05T00:00:00.000000Z\t2\t\n" +
                        "1970-01-01T00:00:00.000001Z\ttbl_name2\tcol2\t2\t0\t12\t3\t33\t-1\t2022-02-13T00:00:00.000000Z\t3\t\n", purgeJob.getLogTableName());

                runPurgeJob(purgeJob);
                assertSql("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
                        "1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t11\t2022-03-29T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000010Z\n" +
                        "1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t-1\t2022-04-05T00:00:00.000000Z\t2\t1970-01-01T00:00:00.000010Z\n" +
                        "1970-01-01T00:00:00.000001Z\ttbl_name2\tcol2\t2\t0\t12\t3\t33\t-1\t2022-02-13T00:00:00.000000Z\t3\t1970-01-01T00:00:00.000010Z\n", purgeJob.getLogTableName());
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

                assertSql(
                        replaceTimestampSuffix("ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T00:00:00.000000Z\t1\ta\tC\t2\n" +
                                "1970-01-02T00:00:00.000000Z\t100\tabcd\tB\tEE\n" +
                                "1970-01-03T00:00:00.000000Z\t100\tabcd\tD\tEE\n" +
                                "1970-01-04T00:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                "1970-01-05T00:00:00.000000Z\t100\tabcd\tD\tEE\n", timestampType.getTypeName()), "up_part"
                );

                assertSql("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n", purgeJob.getLogTableName());
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

                    assertSql(
                            replaceTimestampSuffix("ts\tx\tstr\tsym1\tsym2\n" +
                                    "1970-01-01T02:00:00.000000Z\t1\ta\tC\t2\n" +
                                    "1970-01-02T01:00:00.000000Z\t1\ta\tA\t2\n" +
                                    "1970-01-02T02:00:00.000000Z\t2\td\tB\t4\n" +
                                    "1970-01-03T01:00:00.000000Z\t100\tabcd\tC\tEE\n" +
                                    "1970-01-03T02:00:00.000000Z\t100\tabcd\tD\tEE\n" +
                                    "1970-01-04T01:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                    "1970-01-04T02:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                    "1970-01-05T02:00:00.000000Z\t100\tabcd\tD\tEE\n", timestampType.getTypeName()), "up_part_o3"
                    );

                    assertSql("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
                            "1970-02-02T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t4\t2\t1970-01-03T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z\n" +
                            "1970-02-02T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t4\t2\t1970-01-04T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z\n" +
                            "1970-02-02T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t4\t2\t1970-01-05T00:00:00.000000Z\t-1\t1970-02-02T00:00:00.000070Z\n" +
                            "1970-02-02T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t4\t2\t1970-01-03T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z\n" +
                            "1970-02-02T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t4\t2\t1970-01-04T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z\n" +
                            "1970-02-02T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t4\t2\t1970-01-05T00:00:00.000000Z\t-1\t1970-02-02T00:00:00.000070Z\n" +
                            "1970-02-02T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t4\t2\t1970-01-03T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z\n" +
                            "1970-02-02T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t4\t2\t1970-01-04T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z\n" +
                            "1970-02-02T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t4\t2\t1970-01-05T00:00:00.000000Z\t-1\t1970-02-02T00:00:00.000070Z\n", purgeJob.getLogTableName());
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

                assertSql(
                        replaceTimestampSuffix("ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T00:00:00.000000Z\t1\ta\tC\t2\n" +
                                "1970-01-02T00:00:00.000000Z\t100\tabcd\tB\tEE\n" +
                                "1970-01-03T00:00:00.000000Z\t100\tabcd\tD\tEE\n" +
                                "1970-01-04T00:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                "1970-01-05T00:00:00.000000Z\t100\tabcd\tD\tEE\n", timestampType.getTypeName()), "up_part"
                );

                assertSql("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
                        "1970-01-01T00:00:00.000010Z\tup_part~\tx\t2\t0\t6\t0\t2\t-1\t1970-01-02T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000010Z\tup_part~\tx\t2\t0\t6\t0\t2\t-1\t1970-01-03T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000010Z\tup_part~\tx\t2\t0\t6\t0\t2\t-1\t1970-01-04T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000010Z\tup_part~\tx\t2\t0\t6\t0\t2\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000011Z\tup_part~\tstr\t2\t0\t11\t0\t2\t-1\t1970-01-02T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000150Z\n" +
                        "1970-01-01T00:00:00.000011Z\tup_part~\tstr\t2\t0\t11\t0\t2\t-1\t1970-01-03T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000150Z\n" +
                        "1970-01-01T00:00:00.000011Z\tup_part~\tstr\t2\t0\t11\t0\t2\t-1\t1970-01-04T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000150Z\n" +
                        "1970-01-01T00:00:00.000011Z\tup_part~\tstr\t2\t0\t11\t0\t2\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000150Z\n" +
                        "1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-02T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-03T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-04T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n", purgeJob.getLogTableName());

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
                    assertSql(
                            replaceTimestampSuffix("ts\tx\tstr\tsym1\tsym2\n" +
                                    "1970-01-01T02:00:00.000000Z\t1\ta\tC\t2\n" +
                                    "1970-01-02T01:00:00.000000Z\t1\ta\tA\t2\n" +
                                    "1970-01-02T02:00:00.000000Z\t2\td\tB\t4\n" +
                                    "1970-01-03T01:00:00.000000Z\t100\tabcd\tC\tEE\n" +
                                    "1970-01-03T02:00:00.000000Z\t100\tabcd\tD\tEE\n" +
                                    "1970-01-04T01:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                    "1970-01-04T02:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                    "1970-01-05T02:00:00.000000Z\t100\tabcd\tD\tEE\n", timestampType.getTypeName()), "up_part_o3"
                    );

                    assertSql("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n", purgeJob.getLogTableName()
                    );
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

                assertSql(
                        replaceTimestampSuffix("ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T00:00:00.000000Z\t1\ta\tC\t2\n" +
                                "1970-01-02T00:00:00.000000Z\t100\tabcd\tB\tEE\n" +
                                "1970-01-03T00:00:00.000000Z\t100\tabcd\tD\tEE\n" +
                                "1970-01-04T00:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                "1970-01-05T00:00:00.000000Z\t100\tabcd\tD\tEE\n", timestampType.getTypeName()), "up_part"
                );

                assertSql("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
                        "1970-01-01T00:00:00.000010Z\tup_part~\tx\t2\t0\t6\t0\t2\t-1\t1970-01-02T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000010Z\tup_part~\tx\t2\t0\t6\t0\t2\t-1\t1970-01-03T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000010Z\tup_part~\tx\t2\t0\t6\t0\t2\t-1\t1970-01-04T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000010Z\tup_part~\tx\t2\t0\t6\t0\t2\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000011Z\tup_part~\tstr\t2\t0\t11\t0\t2\t-1\t1970-01-02T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000011Z\tup_part~\tstr\t2\t0\t11\t0\t2\t-1\t1970-01-03T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000011Z\tup_part~\tstr\t2\t0\t11\t0\t2\t-1\t1970-01-04T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000011Z\tup_part~\tstr\t2\t0\t11\t0\t2\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-02T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-03T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-04T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n", purgeJob.getLogTableName());
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

                assertSql(
                        replaceTimestampSuffix("ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T00:00:00.000000Z\t100\tabcd\tC\tEE\n" +
                                "1970-01-01T00:00:01.000000Z\t100\tabcd\tB\tEE\n" +
                                "1970-01-01T00:00:02.000000Z\t100\tabcd\tD\tEE\n" +
                                "1970-01-01T00:00:03.000000Z\t100\tabcd\tA\tEE\n" +
                                "1970-01-01T00:00:04.000000Z\t100\tabcd\tD\tEE\n", timestampType.getTypeName()), "up"
                );

                assertSql("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
                        "1970-01-01T00:00:00.000010Z\tup~\tx\t2\t0\t6\t3\t2\t-1\t1970-01-01T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000011Z\tup~\tstr\t2\t0\t11\t3\t2\t-1\t1970-01-01T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000012Z\tup~\tsym2\t2\t0\t12\t3\t2\t-1\t1970-01-01T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n", purgeJob.getLogTableName());
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

                assertSql(
                        replaceTimestampSuffix("ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T00:00:00.000000Z\t1\ta\tA\t2\n" +
                                "1970-01-01T00:00:01.000000Z\t2\tb\tC\t4\n" +
                                "1970-01-01T00:00:02.000000Z\t3\td\tA\t2\n" +
                                "1970-01-01T00:00:03.000000Z\t4\td\tA\t3\n" +
                                "1970-01-01T00:00:04.000000Z\t5\ta\tD\t1\n", timestampType.getTypeName()), "up"
                );
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

                assertSql(
                        replaceTimestampSuffix("ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T00:00:00.000000Z\t1\ta\tA\t2\n" +
                                "1970-01-01T00:00:01.000000Z\t2\tb\tC\t4\n" +
                                "1970-01-01T00:00:02.000000Z\t3\td\tA\t2\n" +
                                "1970-01-01T00:00:03.000000Z\t4\td\tA\t3\n" +
                                "1970-01-01T00:00:04.000000Z\t5\ta\tD\t1\n", timestampType.getTypeName()), "testPurgeRespectsTableTruncates"
                );
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

                assertSql(
                        replaceTimestampSuffix("ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T02:00:00.000000Z\t1\ta\tC\t2\n" +
                                "1970-01-02T01:00:00.000000Z\t1\ta\tA\t2\n" +
                                "1970-01-02T02:00:00.000000Z\t2\td\tB\t4\n" +
                                "1970-01-03T01:00:00.000000Z\t100\tabcd\tC\tEE\n" +
                                "1970-01-03T02:00:00.000000Z\t100\tabcd\tD\tEE\n" +
                                "1970-01-04T01:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                "1970-01-04T02:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                "1970-01-05T02:00:00.000000Z\t100\tabcd\tD\tEE\n", timestampType.getTypeName()), "up_part_o3"
                );

                // cleaned everything, table is truncated
                assertSql("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n", purgeLogTableName);
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

                assertSql(
                        replaceTimestampSuffix("ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T02:00:00.000000Z\t1\ta\tC\t2\n" +
                                "1970-01-02T01:00:00.000000Z\t1\ta\tA\t2\n" +
                                "1970-01-02T02:00:00.000000Z\t2\td\tB\t4\n" +
                                "1970-01-03T01:00:00.000000Z\t100\tabcd\tC\tEE\n" +
                                "1970-01-03T02:00:00.000000Z\t100\tabcd\tD\tEE\n" +
                                "1970-01-04T01:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                "1970-01-04T02:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                "1970-01-05T02:00:00.000000Z\t100\tabcd\tD\tEE\n", timestampType.getTypeName()), "up_part_o3"
                );

                assertSql(
                        "ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
                                "1970-01-01T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t3\t-1\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z\n" +
                                "1970-01-01T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t3\t-1\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z\n" +
                                "1970-01-01T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t3\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000070Z\n" +
                                "1970-01-01T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t3\t-1\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z\n" +
                                "1970-01-01T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t3\t-1\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z\n" +
                                "1970-01-01T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t3\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000070Z\n" +
                                "1970-01-01T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t3\t-1\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z\n" +
                                "1970-01-01T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t3\t-1\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z\n" +
                                "1970-01-01T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t3\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000070Z\n", purgeJob.getLogTableName()
                );
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

                purgeJob.run(0);
                assertSql("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
                        "1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t11\t2022-03-29T00:00:00.000000Z\t-1\t\n" +
                        "1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t-1\t2022-04-05T00:00:00.000000Z\t2\t\n" +
                        "1970-01-01T00:00:00.000001Z\ttbl_name2\tcol2\t2\t0\t12\t3\t33\t-1\t2022-02-13T00:00:00.000000Z\t3\t\n", purgeJob.getLogTableName());

                runPurgeJob(purgeJob);
                assertSql("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
                        "1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t11\t2022-03-29T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000010Z\n" +
                        "1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t-1\t2022-04-05T00:00:00.000000Z\t2\t1970-01-01T00:00:00.000010Z\n" +
                        "1970-01-01T00:00:00.000001Z\ttbl_name2\tcol2\t2\t0\t12\t3\t33\t-1\t2022-02-13T00:00:00.000000Z\t3\t1970-01-01T00:00:00.000010Z\n", purgeJob.getLogTableName());
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

    private void assertIndexFilesExist(String[] partitions, Path path, String tableName, String colSuffix, boolean exist) {
        for (int i = 0; i < partitions.length; i++) {
            String partition = partitions[i];
            assertIndexFilesExist(path, tableName, partition, colSuffix, exist);
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
        tsk.of(tblName, colName, tableId, 0, columnType, timestampDriver.getTimestampType(), PartitionBy.NONE, updateTxn, new LongList());
        tsk.appendColumnInfo(columnVersion, timestampDriver.parseFloorLiteral(partitionTs), partitionNameTxn);
        return tsk;
    }

    private void runPurgeJob(ColumnPurgeJob purgeJob) {
        engine.releaseInactive();
        setCurrentMicros(currentMicros + 10L * iteration++);
        purgeJob.run(0);
        setCurrentMicros(currentMicros + 10L * iteration++);
        purgeJob.run(0);
    }

    private void update(String updateSql) throws SqlException {
        execute(updateSql);
    }
}
