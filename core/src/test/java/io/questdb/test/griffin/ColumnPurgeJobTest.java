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
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.mp.Sequence;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.tasks.ColumnPurgeTask;
import io.questdb.test.AbstractGriffinTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ColumnPurgeJobTest extends AbstractGriffinTest {
    private int iteration = 1;

    @Before
    public void setUpUpdates() {
        iteration = 1;
        currentMicros = 0;
        columnPurgeRetryDelay = 1;
    }

    @Test
    public void testManyUpdatesInserts() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                compiler.compile("create table up_part_o3_many as" +
                        " (select timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY", sqlExecutionContext);

                try (TableReader rdr1 = getReader("up_part_o3_many")) {
                    compile("insert into up_part_o3_many " +
                            " select timestamp_sequence('1970-01-02T01', 24 * 60 * 60 * 1000000L) ts," +
                            " x," +
                            " rnd_str('a', 'b', 'c', 'd') str," +
                            " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                            " rnd_symbol('1', '2', '3', '4') sym2" +
                            " from long_sequence(3)");

                    try (TableReader rdr2 = getReader("up_part_o3_many")) {
                        executeUpdate("UPDATE up_part_o3_many SET x = 100, str='u1', sym2='EE' WHERE ts >= '1970-01-03'");
                        runPurgeJob(purgeJob);

                        currentMicros++;
                        try (TableReader rdr3 = getReader("up_part_o3_many")) {
                            executeUpdate("UPDATE up_part_o3_many SET x = 200, str='u2', sym2='EE' WHERE x = 100");
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
                        "up_part_o3_many",
                        "ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T02:00:00.000000Z\t1\ta\tC\t2\n" +
                                "1970-01-02T01:00:00.000000Z\t1\ta\tA\t2\n" +
                                "1970-01-02T02:00:00.000000Z\t2\td\tB\t4\n" +
                                "1970-01-03T01:00:00.000000Z\t200\tu2\tC\tEE\n" +
                                "1970-01-03T02:00:00.000000Z\t200\tu2\tD\tEE\n" +
                                "1970-01-04T01:00:00.000000Z\t200\tu2\tA\tEE\n" +
                                "1970-01-04T02:00:00.000000Z\t200\tu2\tA\tEE\n" +
                                "1970-01-05T02:00:00.000000Z\t200\tu2\tD\tEE\n"
                );

                assertSql(purgeJob.getLogTableName(),
                        "ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
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
                                "1970-01-01T00:00:00.000063Z\tup_part_o3_many~\tsym2\t2\t0\t12\t0\t4\t2\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000151Z\n");
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testPurge() throws Exception {
        assertMemoryLeak(() -> {
            currentMicros = 0;
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                TableToken tn1 = new TableToken("tbl_name", "tbl_name", 123, false);
                ColumnPurgeTask task = createTask(tn1, "col", 1, ColumnType.INT, 43, 11, "2022-03-29", -1);
                task.appendColumnInfo(-1, IntervalUtils.parseFloorPartialTimestamp("2022-04-05"), 2);
                appendTaskToQueue(task);

                TableToken tn2 = new TableToken("tbl_name2", "tbl_name2", 123, false);
                ColumnPurgeTask task2 = createTask(tn2, "col2", 2, ColumnType.SYMBOL, 33, -1, "2022-02-13", 3);
                appendTaskToQueue(task2);

                purgeJob.run(0);
                assertSql(purgeJob.getLogTableName(), "ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
                        "1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t11\t2022-03-29T00:00:00.000000Z\t-1\t\n" +
                        "1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t-1\t2022-04-05T00:00:00.000000Z\t2\t\n" +
                        "1970-01-01T00:00:00.000001Z\ttbl_name2\tcol2\t2\t0\t12\t3\t33\t-1\t2022-02-13T00:00:00.000000Z\t3\t\n");

                runPurgeJob(purgeJob);
                assertSql(purgeJob.getLogTableName(), "ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
                        "1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t11\t2022-03-29T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000010Z\n" +
                        "1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t-1\t2022-04-05T00:00:00.000000Z\t2\t1970-01-01T00:00:00.000010Z\n" +
                        "1970-01-01T00:00:00.000001Z\ttbl_name2\tcol2\t2\t0\t12\t3\t33\t-1\t2022-02-13T00:00:00.000000Z\t3\t1970-01-01T00:00:00.000010Z\n");
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testPurgeCannotAllocateFailure() throws Exception {
        assertMemoryLeak(() -> {
            currentMicros = 0;
            ff = new TestFilesFacadeImpl() {

                @Override
                public boolean allocate(int fd, long size) {
                    if (this.fd == fd) {
                        throw new RuntimeException("TEST ERROR");
                    }
                    return super.allocate(fd, size);
                }

                public int openRW(LPSZ name, long opts) {
                    int fd = super.openRW(name, opts);
                    if (Chars.endsWith(name, "completed.d")) {
                        this.fd = fd;
                    }
                    return fd;
                }
            };

            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                compiler.compile("create table up_part as" +
                        " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY", sqlExecutionContext);

                try (TableReader ignored = getReader("up_part")) {
                    executeUpdate("UPDATE up_part SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-02'");
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
                        "up_part",
                        "ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T00:00:00.000000Z\t1\ta\tC\t2\n" +
                                "1970-01-02T00:00:00.000000Z\t100\tabcd\tB\tEE\n" +
                                "1970-01-03T00:00:00.000000Z\t100\tabcd\tD\tEE\n" +
                                "1970-01-04T00:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                "1970-01-05T00:00:00.000000Z\t100\tabcd\tD\tEE\n"
                );

                assertSql(purgeJob.getLogTableName(), "ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n");
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testPurgeHandlesLogPartitionChange() throws Exception {
        assertMemoryLeak(() -> {
            currentMicros = Timestamps.DAY_MICROS * 30;
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                compiler.compile("create table up_part_o3 as" +
                        " (select timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY", sqlExecutionContext);

                compile("insert into up_part_o3 " +
                        " select timestamp_sequence('1970-01-02T01', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(3)");

                try (TableReader rdr = getReader("up_part_o3")) {
                    executeUpdate("UPDATE up_part_o3 SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-03'");

                    runPurgeJob(purgeJob);
                    rdr.openPartition(0);
                }
            }

            currentMicros = Timestamps.DAY_MICROS * 32;
            try (Path path = new Path()) {
                String[] partitions = new String[]{"1970-01-03.1", "1970-01-04.1", "1970-01-05"};
                assertFilesExist(partitions, path, "up_part_o3", "", true);

                try (ColumnPurgeJob purgeJob = createPurgeJob()) {

                    try (TableReader ignored = getReader("up_part_o3")) {
                        executeUpdate("UPDATE up_part_o3 SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-03'");
                    }

                    runPurgeJob(purgeJob);
                    // Need a second run, first will only re-schedule outstanding tasks
                    runPurgeJob(purgeJob);

                    assertFilesExist(partitions, path, "up_part_o3", "", false);

                    assertSql(
                            "up_part_o3",
                            "ts\tx\tstr\tsym1\tsym2\n" +
                                    "1970-01-01T02:00:00.000000Z\t1\ta\tC\t2\n" +
                                    "1970-01-02T01:00:00.000000Z\t1\ta\tA\t2\n" +
                                    "1970-01-02T02:00:00.000000Z\t2\td\tB\t4\n" +
                                    "1970-01-03T01:00:00.000000Z\t100\tabcd\tC\tEE\n" +
                                    "1970-01-03T02:00:00.000000Z\t100\tabcd\tD\tEE\n" +
                                    "1970-01-04T01:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                    "1970-01-04T02:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                    "1970-01-05T02:00:00.000000Z\t100\tabcd\tD\tEE\n"
                    );

                    assertSql(purgeJob.getLogTableName(), "ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
                            "1970-02-02T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t4\t2\t1970-01-03T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z\n" +
                            "1970-02-02T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t4\t2\t1970-01-04T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z\n" +
                            "1970-02-02T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t4\t2\t1970-01-05T00:00:00.000000Z\t-1\t1970-02-02T00:00:00.000070Z\n" +
                            "1970-02-02T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t4\t2\t1970-01-03T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z\n" +
                            "1970-02-02T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t4\t2\t1970-01-04T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z\n" +
                            "1970-02-02T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t4\t2\t1970-01-05T00:00:00.000000Z\t-1\t1970-02-02T00:00:00.000070Z\n" +
                            "1970-02-02T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t4\t2\t1970-01-03T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z\n" +
                            "1970-02-02T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t4\t2\t1970-01-04T00:00:00.000000Z\t1\t1970-02-02T00:00:00.000070Z\n" +
                            "1970-02-02T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t4\t2\t1970-01-05T00:00:00.000000Z\t-1\t1970-02-02T00:00:00.000070Z\n");
                    Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                }
            }
        });
    }

    @Test
    public void testPurgeIOFailureRetried() throws Exception {
        assertMemoryLeak(() -> {
            currentMicros = 0;
            ff = new TestFilesFacadeImpl() {
                int count = 0;

                @Override
                public boolean remove(LPSZ name) {
                    if (Chars.endsWith(name, "str.i")) {
                        if (count++ < 6) {
                            return false;
                        }
                    }
                    return super.remove(name);
                }
            };

            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                compiler.compile("create table up_part as" +
                        " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY", sqlExecutionContext);

                try (TableReader rdr = getReader("up_part")) {
                    executeUpdate("UPDATE up_part SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-02'");

                    runPurgeJob(purgeJob);
                    rdr.openPartition(0);
                }

                try (Path path = new Path()) {
                    String[] partitions = new String[]{"1970-01-02", "1970-01-03", "1970-01-04", "1970-01-05"};
                    assertFilesExist(partitions, path, "up_part", "", true);

                    runPurgeJob(purgeJob);
                    // Delete failure
                    TableToken tableToken = engine.verifyTableName("up_part");
                    path.of(configuration.getRoot()).concat(tableToken).concat("1970-01-02").concat("str.i").$();
                    Assert.assertTrue(Chars.toString(path), TestFilesFacadeImpl.INSTANCE.exists(path));

                    // Should retry
                    runPurgeJob(purgeJob);
                    assertFilesExist(partitions, path, "up_part", "", false);
                }

                assertSql(
                        "up_part",
                        "ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T00:00:00.000000Z\t1\ta\tC\t2\n" +
                                "1970-01-02T00:00:00.000000Z\t100\tabcd\tB\tEE\n" +
                                "1970-01-03T00:00:00.000000Z\t100\tabcd\tD\tEE\n" +
                                "1970-01-04T00:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                "1970-01-05T00:00:00.000000Z\t100\tabcd\tD\tEE\n"
                );

                assertSql(purgeJob.getLogTableName(), "ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
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
                        "1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n");

                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testPurgeLimitsTaskLoadOnRestart() throws Exception {
        assertMemoryLeak(() -> {
            currentMicros = 0;
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                compiler.compile("create table up_part_o3 as" +
                        " (select timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY", sqlExecutionContext);

                compile("insert into up_part_o3 " +
                        " select timestamp_sequence('1970-01-02T01', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(3)");

                try (TableReader rdr = getReader("up_part_o3")) {
                    executeUpdate("UPDATE up_part_o3 SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-03'");

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
                            "up_part_o3",
                            "ts\tx\tstr\tsym1\tsym2\n" +
                                    "1970-01-01T02:00:00.000000Z\t1\ta\tC\t2\n" +
                                    "1970-01-02T01:00:00.000000Z\t1\ta\tA\t2\n" +
                                    "1970-01-02T02:00:00.000000Z\t2\td\tB\t4\n" +
                                    "1970-01-03T01:00:00.000000Z\t100\tabcd\tC\tEE\n" +
                                    "1970-01-03T02:00:00.000000Z\t100\tabcd\tD\tEE\n" +
                                    "1970-01-04T01:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                    "1970-01-04T02:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                    "1970-01-05T02:00:00.000000Z\t100\tabcd\tD\tEE\n"
                    );

                    assertSql(purgeJob.getLogTableName(),
                            "ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n");
                    Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                }
            }
        });
    }

    @Test
    public void testPurgeRespectsOpenReaderDailyPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            currentMicros = 0;
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                compiler.compile("create table up_part as" +
                        " (select timestamp_sequence('1970-01-01', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY", sqlExecutionContext);

                try (TableReader rdr = getReader("up_part")) {
                    executeUpdate("UPDATE up_part SET x = 100, str='abcd',SYM2='EE' WHERE ts >= '1970-01-02'");

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
                        "up_part",
                        "ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T00:00:00.000000Z\t1\ta\tC\t2\n" +
                                "1970-01-02T00:00:00.000000Z\t100\tabcd\tB\tEE\n" +
                                "1970-01-03T00:00:00.000000Z\t100\tabcd\tD\tEE\n" +
                                "1970-01-04T00:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                "1970-01-05T00:00:00.000000Z\t100\tabcd\tD\tEE\n"
                );

                assertSql(purgeJob.getLogTableName(), "ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
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
                        "1970-01-01T00:00:00.000012Z\tup_part~\tsym2\t2\t0\t12\t0\t2\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n");
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testPurgeRespectsOpenReaderNonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                compiler.compile("create table up as" +
                        " (select timestamp_sequence(0, 1000000) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts)", sqlExecutionContext);

                try (TableReader rdr = getReader("up")) {
                    executeUpdate("UPDATE up SET x = 100, str='abcd', sym2='EE'");

                    runPurgeJob(purgeJob);
                    rdr.openPartition(0);
                }

                try (Path path = new Path()) {
                    assertFilesExist(path, "up", "default", "", true);
                    runPurgeJob(purgeJob);
                    assertFilesExist(path, "up", "default", "", false);
                }

                assertSql(
                        "up",
                        "ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T00:00:00.000000Z\t100\tabcd\tC\tEE\n" +
                                "1970-01-01T00:00:01.000000Z\t100\tabcd\tB\tEE\n" +
                                "1970-01-01T00:00:02.000000Z\t100\tabcd\tD\tEE\n" +
                                "1970-01-01T00:00:03.000000Z\t100\tabcd\tA\tEE\n" +
                                "1970-01-01T00:00:04.000000Z\t100\tabcd\tD\tEE\n"
                );

                assertSql(purgeJob.getLogTableName(), "ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
                        "1970-01-01T00:00:00.000010Z\tup~\tx\t2\t0\t6\t3\t2\t-1\t1970-01-01T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000011Z\tup~\tstr\t2\t0\t11\t3\t2\t-1\t1970-01-01T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n" +
                        "1970-01-01T00:00:00.000012Z\tup~\tsym2\t2\t0\t12\t3\t2\t-1\t1970-01-01T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z\n");
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testPurgeRespectsTableRecreate() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                compiler.compile("create table up as" +
                        " (select timestamp_sequence(0, 1000000) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts)", sqlExecutionContext);

                try (TableReader rdr = getReader("up")) {
                    executeUpdate("UPDATE up SET x = 100, str='abcd'");

                    runPurgeJob(purgeJob);
                    rdr.openPartition(0);
                }
                engine.releaseInactive();

                compiler.compile("drop table up", sqlExecutionContext);

                compiler.compile("create table up as" +
                        " (select timestamp_sequence(0, 1000000) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts)", sqlExecutionContext);

                runPurgeJob(purgeJob);

                assertSql(
                        "up",
                        "ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T00:00:00.000000Z\t1\ta\tA\t2\n" +
                                "1970-01-01T00:00:01.000000Z\t2\tb\tC\t4\n" +
                                "1970-01-01T00:00:02.000000Z\t3\td\tA\t2\n" +
                                "1970-01-01T00:00:03.000000Z\t4\td\tA\t3\n" +
                                "1970-01-01T00:00:04.000000Z\t5\ta\tD\t1\n"
                );
            }
        });
    }

    @Test
    public void testPurgeRespectsTableTruncates() throws Exception {
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                compiler.compile("create table testPurgeRespectsTableTruncates as" +
                        " (select timestamp_sequence(0, 1000000) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY", sqlExecutionContext);

                try (TableReader rdr = getReader("testPurgeRespectsTableTruncates")) {
                    executeUpdate("UPDATE testPurgeRespectsTableTruncates SET x = 100, str='abcd'");

                    runPurgeJob(purgeJob);
                    rdr.openPartition(0);
                }
                engine.releaseInactive();

                compiler.compile("truncate table testPurgeRespectsTableTruncates", sqlExecutionContext);

                compiler.compile("insert into testPurgeRespectsTableTruncates " +
                        " select timestamp_sequence(0, 1000000) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)", sqlExecutionContext);

                runPurgeJob(purgeJob);

                assertSql(
                        "testPurgeRespectsTableTruncates",
                        "ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T00:00:00.000000Z\t1\ta\tA\t2\n" +
                                "1970-01-01T00:00:01.000000Z\t2\tb\tC\t4\n" +
                                "1970-01-01T00:00:02.000000Z\t3\td\tA\t2\n" +
                                "1970-01-01T00:00:03.000000Z\t4\td\tA\t3\n" +
                                "1970-01-01T00:00:04.000000Z\t5\ta\tD\t1\n"
                );
            }
        });
    }

    @Test
    public void testPurgeRetriesAfterRestart() throws Exception {
        assertMemoryLeak(() -> {
            currentMicros = 0;
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                compiler.compile("create table up_part_o3 as" +
                        " (select timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY", sqlExecutionContext);

                compile("insert into up_part_o3 " +
                        " select timestamp_sequence('1970-01-02T01', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(3)");

                try (TableReader rdr = getReader("up_part_o3")) {
                    executeUpdate("UPDATE up_part_o3 SET x = 100, str='abcd', sym2='EE' WHERE ts >= '1970-01-03'");

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
                        "up_part_o3",
                        "ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T02:00:00.000000Z\t1\ta\tC\t2\n" +
                                "1970-01-02T01:00:00.000000Z\t1\ta\tA\t2\n" +
                                "1970-01-02T02:00:00.000000Z\t2\td\tB\t4\n" +
                                "1970-01-03T01:00:00.000000Z\t100\tabcd\tC\tEE\n" +
                                "1970-01-03T02:00:00.000000Z\t100\tabcd\tD\tEE\n" +
                                "1970-01-04T01:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                "1970-01-04T02:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                "1970-01-05T02:00:00.000000Z\t100\tabcd\tD\tEE\n"
                );

                // cleaned everything, table is truncated
                assertSql(purgeLogTableName, "ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n");
            }
        });
    }

    @Test
    public void testPurgeTaskRecycle() throws Exception {
        configOverrideColumnVersionTaskPoolCapacity(1);
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                compiler.compile("create table up_part_o3_many as" +
                        " (select timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY", sqlExecutionContext);

                executeUpdate("UPDATE up_part_o3_many SET x = x + 1, str = str || 'u2', sym2 = sym2 || '2'");

                try (Path path = new Path()) {
                    for (int i = 1; i < 10; i++) {
                        try (TableReader ignore = getReader("up_part_o3_many")) {
                            executeUpdate("UPDATE up_part_o3_many SET x = x + 1, str = str || 'u2', sym2 = sym2 || '2'");
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
            currentMicros = 0;
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                compiler.compile("create table up_part_o3 as" +
                        " (select timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(5)), index(sym2)" +
                        " timestamp(ts) PARTITION BY DAY", sqlExecutionContext);

                compile("insert into up_part_o3 " +
                        " select timestamp_sequence('1970-01-02T01', 24 * 60 * 60 * 1000000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " from long_sequence(3)");

                try (TableReader rdr = getReader("up_part_o3")) {
                    executeUpdate("UPDATE up_part_o3 SET x = 100, str='abcd', sym2 = 'EE' WHERE ts >= '1970-01-03'");

                    currentMicros = 20;
                    runPurgeJob(purgeJob);
                    rdr.openPartition(0);
                }

                try (Path path = new Path()) {
                    String[] partitions = new String[]{"1970-01-03.1", "1970-01-04.1", "1970-01-05"};
                    assertFilesExist(partitions, path, "up_part_o3", "", true);

                    currentMicros = 40;
                    runPurgeJob(purgeJob);

                    assertFilesExist(partitions, path, "up_part_o3", "", false);
                }

                assertSql(
                        "up_part_o3",
                        "ts\tx\tstr\tsym1\tsym2\n" +
                                "1970-01-01T02:00:00.000000Z\t1\ta\tC\t2\n" +
                                "1970-01-02T01:00:00.000000Z\t1\ta\tA\t2\n" +
                                "1970-01-02T02:00:00.000000Z\t2\td\tB\t4\n" +
                                "1970-01-03T01:00:00.000000Z\t100\tabcd\tC\tEE\n" +
                                "1970-01-03T02:00:00.000000Z\t100\tabcd\tD\tEE\n" +
                                "1970-01-04T01:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                "1970-01-04T02:00:00.000000Z\t100\tabcd\tA\tEE\n" +
                                "1970-01-05T02:00:00.000000Z\t100\tabcd\tD\tEE\n"
                );

                assertSql(
                        purgeJob.getLogTableName(),
                        "ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
                                "1970-01-01T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t3\t-1\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z\n" +
                                "1970-01-01T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t3\t-1\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z\n" +
                                "1970-01-01T00:00:00.000030Z\tup_part_o3~\tx\t2\t0\t6\t0\t3\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000070Z\n" +
                                "1970-01-01T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t3\t-1\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z\n" +
                                "1970-01-01T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t3\t-1\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z\n" +
                                "1970-01-01T00:00:00.000031Z\tup_part_o3~\tstr\t2\t0\t11\t0\t3\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000070Z\n" +
                                "1970-01-01T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t3\t-1\t1970-01-03T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z\n" +
                                "1970-01-01T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t3\t-1\t1970-01-04T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000070Z\n" +
                                "1970-01-01T00:00:00.000032Z\tup_part_o3~\tsym2\t2\t0\t12\t0\t3\t-1\t1970-01-05T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000070Z\n"
                );
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testSavesDataToPurgeLogTable() throws Exception {
        assertMemoryLeak(() -> {
            currentMicros = 0;
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                TableToken tn1 = new TableToken("tbl_name", "tbl_name", 123, false);
                ColumnPurgeTask task = createTask(tn1, "col", 1, ColumnType.INT, 43, 11, "2022-03-29", -1);
                task.appendColumnInfo(-1, IntervalUtils.parseFloorPartialTimestamp("2022-04-05"), 2);
                appendTaskToQueue(task);


                TableToken tn2 = new TableToken("tbl_name2", "tbl_name2", 123, false);
                ColumnPurgeTask task2 = createTask(tn2, "col2", 2, ColumnType.SYMBOL, 33, -1, "2022-02-13", 3);
                appendTaskToQueue(task2);

                purgeJob.run(0);
                assertSql(purgeJob.getLogTableName(), "ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
                        "1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t11\t2022-03-29T00:00:00.000000Z\t-1\t\n" +
                        "1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t-1\t2022-04-05T00:00:00.000000Z\t2\t\n" +
                        "1970-01-01T00:00:00.000001Z\ttbl_name2\tcol2\t2\t0\t12\t3\t33\t-1\t2022-02-13T00:00:00.000000Z\t3\t\n");

                runPurgeJob(purgeJob);
                assertSql(purgeJob.getLogTableName(), "ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n" +
                        "1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t11\t2022-03-29T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000010Z\n" +
                        "1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t-1\t2022-04-05T00:00:00.000000Z\t2\t1970-01-01T00:00:00.000010Z\n" +
                        "1970-01-01T00:00:00.000001Z\ttbl_name2\tcol2\t2\t0\t12\t3\t33\t-1\t2022-02-13T00:00:00.000000Z\t3\t1970-01-01T00:00:00.000010Z\n");
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

    private void assertFilesExist(String[] partitions, Path path, String up_part, String colSuffix, boolean exist) {
        for (int i = 0; i < partitions.length; i++) {
            String partition = partitions[i];
            assertFilesExist(path, up_part, partition, colSuffix, exist);
        }
    }

    private void assertFilesExist(Path path, String up_part, String partition, String colSuffix, boolean exist) {
        TableToken tableToken = engine.verifyTableName(up_part);
        path.of(configuration.getRoot()).concat(tableToken).concat(partition).concat("x.d").put(colSuffix).$();
        Assert.assertEquals(Chars.toString(path), exist, TestFilesFacadeImpl.INSTANCE.exists(path));

        path.of(configuration.getRoot()).concat(tableToken).concat(partition).concat("str.d").put(colSuffix).$();
        Assert.assertEquals(Chars.toString(path), exist, TestFilesFacadeImpl.INSTANCE.exists(path));

        path.of(configuration.getRoot()).concat(tableToken).concat(partition).concat("str.i").put(colSuffix).$();
        Assert.assertEquals(Chars.toString(path), exist, TestFilesFacadeImpl.INSTANCE.exists(path));

        path.of(configuration.getRoot()).concat(tableToken).concat(partition).concat("sym2.d").put(colSuffix).$();
        Assert.assertEquals(Chars.toString(path), exist, TestFilesFacadeImpl.INSTANCE.exists(path));

        path.of(configuration.getRoot()).concat(tableToken).concat(partition).concat("sym2.k").put(colSuffix).$();
        Assert.assertEquals(Chars.toString(path), exist, TestFilesFacadeImpl.INSTANCE.exists(path));

        path.of(configuration.getRoot()).concat(tableToken).concat(partition).concat("sym2.v").put(colSuffix).$();
        Assert.assertEquals(Chars.toString(path), exist, TestFilesFacadeImpl.INSTANCE.exists(path));
    }

    @NotNull
    private ColumnPurgeJob createPurgeJob() throws SqlException {
        return new ColumnPurgeJob(engine, null);
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
        tsk.of(tblName, colName, tableId, 0, columnType, PartitionBy.NONE, updateTxn, new LongList());
        tsk.appendColumnInfo(columnVersion, IntervalUtils.parseFloorPartialTimestamp(partitionTs), partitionNameTxn);
        return tsk;
    }

    private void executeUpdate(String query) throws SqlException {
        final CompiledQuery cq = compiler.compile(query, sqlExecutionContext);
        Assert.assertEquals(CompiledQuery.UPDATE, cq.getType());
        try (OperationFuture fut = cq.execute(null)) {
            fut.await();
        }
    }

    private void runPurgeJob(ColumnPurgeJob purgeJob) {
        engine.releaseInactive();
        currentMicros += 10L * iteration++;
        purgeJob.run(0);
        currentMicros += 10L * iteration++;
        purgeJob.run(0);
    }
}
