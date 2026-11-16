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
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.idx.BitmapIndexUtils;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.griffin.PurgingOperator;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.mp.Sequence;
import io.questdb.std.Files;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.ColumnPurgeTask;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;
import java.util.function.ObjIntConsumer;
import java.util.regex.Pattern;

@RunWith(Parameterized.class)
public class ColumnPurgeJobTest extends AbstractCairoTest {
    // the rows of y that insertRowsOfIndexReAddedAtTxnZero() writes, as ts, s, d, vc, l
    private static final String ROWS_OF_INDEX_RE_ADDED_AT_TXN_ZERO = """
            2024-01-01T01:00:00.000000Z\ty\t1.5\ta1\t10
            2024-01-01T02:00:00.000000Z\ty\tnull\t\tnull
            2024-01-01T04:00:00.000000Z\ty\t4.5\t\u00e94\t40
            """;
    private static final String THIRD_ROW_OF_INDEX_RE_ADDED_AT_TXN_ZERO = "(5, '2024-01-01T05:00:00.000000Z', 'y', 5.5, 'a5', 50)";
    // the rows of t_replay that createReplayTable() writes with sym = 'A', as ts, x
    private static final String ROWS_OF_A_IN_REPLAY_TABLE = """
            ts\tx
            1970-01-01T02:00:00.000000Z\t1
            1970-01-04T02:00:00.000000Z\t4
            """;
    // the columns of the purge log as the versions before partition_timestamp_raw created it
    private static final String LEGACY_PURGE_LOG_COLUMNS = """
            ts timestamp, table_name symbol, column_name symbol, table_id int, truncate_version long, columnType int, \
            table_partition_by int, updated_txn long, column_version long, partition_timestamp timestamp, \
            partition_name_txn long, completed timestamp""";
    // the columns of the purge log, as table_columns() lists them
    private static final String PURGE_LOG_COLUMNS = """
            column\ttype
            ts\tTIMESTAMP
            table_name\tSYMBOL
            column_name\tSYMBOL
            table_id\tINT
            truncate_version\tLONG
            columnType\tINT
            table_partition_by\tINT
            updated_txn\tLONG
            column_version\tLONG
            partition_timestamp\tTIMESTAMP
            partition_name_txn\tLONG
            completed\tTIMESTAMP
            partition_timestamp_raw\tLONG
            """;
    // the rows of t_split that createSplitTable() writes, as ts, x
    private static final String ROWS_OF_SPLIT_TABLE = """
            ts\tx
            1970-01-01T02:00:00.000000Z\t1
            1970-01-02T02:00:00.000000Z\t2
            1970-01-03T01:00:00.000000Z\t3
            1970-01-03T02:00:00.000000Z\t4
            1970-01-03T03:00:00.000000Z\t5
            1970-01-03T04:00:00.000000Z\t6
            1970-01-03T05:00:00.000000Z\t7
            1970-01-03T06:00:00.000000Z\t8
            1970-01-03T07:00:00.000000Z\t9
            1970-01-03T08:00:00.000000Z\t10
            1970-01-03T09:00:00.000000Z\t11
            1970-01-03T09:30:00.000000Z\t15
            1970-01-03T10:00:00.000000Z\t12
            1970-01-04T02:00:00.000000Z\t13
            1970-01-05T02:00:00.000000Z\t14
            """;
    // the partition of t_split that the O3 row of createSplitTable() splits off 1970-01-03
    private static final int SPLIT_PARTITION_INDEX = 3;
    private final TestTimestampType timestampType;
    // the entries that the purge job reported as refused because the truncate version changed, see
    // listenToTruncateRefusals()
    private final ArrayList<String> truncateRefusedEntries = new ArrayList<>();
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
        truncateRefusedEntries.clear();
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


            // Under adaptive the save points still name the versions the UPDATE of up_part_o3_2 left, one task per
            // column, so the startup purge keeps those tasks: it writes them back to the truncated log and queues
            // them for the running job.
            final int heldTaskCount = engine.getConfiguration().getCommitMode() == CommitMode.ADAPTIVE ? 3 : 0;
            String purgeLogTableName;
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                Assert.assertEquals(heldTaskCount, purgeJob.getOutstandingPurgeTasks());
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

            // the log is truncated, apart from the held tasks
            assertQuery("SELECT count_distinct(column_name) columns, count(completed) completed FROM '" + purgeLogTableName + "'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("columns\tcompleted\n" + heldTaskCount + "\t0\n");

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
                        .returns(withRawPartitionTimestamps("""
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
                                """));
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
                        .returns(withRawPartitionTimestamps("""
                                ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted
                                1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t11\t2022-03-29T00:00:00.000000Z\t-1\t
                                1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t-1\t2022-04-05T00:00:00.000000Z\t2\t
                                1970-01-01T00:00:00.000001Z\ttbl_name2\tcol2\t2\t0\t12\t3\t33\t-1\t2022-02-13T00:00:00.000000Z\t3\t
                                """));

                runPurgeJob(purgeJob);
                assertQuery(purgeJob.getLogTableName())
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns(withRawPartitionTimestamps("""
                                ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted
                                1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t11\t2022-03-29T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000010Z
                                1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t-1\t2022-04-05T00:00:00.000000Z\t2\t1970-01-01T00:00:00.000010Z
                                1970-01-01T00:00:00.000001Z\ttbl_name2\tcol2\t2\t0\t12\t3\t33\t-1\t2022-02-13T00:00:00.000000Z\t3\t1970-01-01T00:00:00.000010Z
                                """));
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
                        .returns(withRawPartitionTimestamps("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n"));
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testPurgeDropsFailedDeleteAfterRestart() throws Exception {
        // The startup purge keeps only a task a save point holds. A task left unfinished by a failed delete goes with
        // the log truncate, as it always has: its file stays on disk until VACUUM TABLE.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            final String failingFile = "1970-01-03.1" + Files.SEPARATOR + "str.i";
            final AtomicBoolean failRemove = new AtomicBoolean();
            ff = new TestFilesFacadeImpl() {
                @Override
                public boolean removeQuiet(LPSZ name) {
                    if (Utf8s.endsWithAscii(name, failingFile) && failRemove.compareAndSet(true, false)) {
                        return false;
                    }
                    return super.removeQuiet(name);
                }
            };

            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("CREATE TABLE up_part_o3 AS" +
                        " (SELECT timestamp_sequence('1970-01-01T02', 24 * 60 * 60 * 1_000_000L)::" + timestampType.getTypeName() + " ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " FROM long_sequence(5)), INDEX(sym2)" +
                        " TIMESTAMP(ts) PARTITION BY DAY");

                execute("INSERT INTO up_part_o3 " +
                        " SELECT timestamp_sequence('1970-01-02T01', 24 * 60 * 60 * 1_000_000L) ts," +
                        " x," +
                        " rnd_str('a', 'b', 'c', 'd') str," +
                        " rnd_symbol('A', 'B', 'C', 'D') sym1," +
                        " rnd_symbol('1', '2', '3', '4') sym2" +
                        " FROM long_sequence(3)");

                try (TableReader rdr = getReader("up_part_o3")) {
                    update("UPDATE up_part_o3 SET x = 100, str = 'abcd', sym2 = 'EE' WHERE ts >= '1970-01-03'");

                    runPurgeJob(purgeJob);
                    rdr.openPartition(0);
                }
            }

            try (Path path = new Path()) {
                final String[] partitions = new String[]{"1970-01-03.1", "1970-01-04.1", "1970-01-05"};
                assertFilesExist(partitions, path, "up_part_o3", "", true);

                failRemove.set(true);
                final String purgeLogTableName;
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    Assert.assertFalse("startup purge tried the delete", failRemove.get());
                    Assert.assertEquals("tasks kept for a retry", 0, purgeJob.getOutstandingPurgeTasks());
                    purgeLogTableName = purgeJob.getLogTableName();
                }

                path.of(configuration.getDbRoot()).concat(engine.verifyTableName("up_part_o3")).concat(failingFile).$();
                Assert.assertTrue(Utf8s.toString(path), TestFilesFacadeImpl.INSTANCE.exists(path.$()));
                assertFilesExist(new String[]{"1970-01-04.1", "1970-01-05"}, path, "up_part_o3", "", false);

                assertQuery(purgeLogTableName)
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns(withRawPartitionTimestamps("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n"));
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
                            .returns(withRawPartitionTimestamps("""
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
                                    """));
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
                        .returns(withRawPartitionTimestamps("""
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
                                """));

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
                            .returns(withRawPartitionTimestamps("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n"));
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
                        .returns(withRawPartitionTimestamps("""
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
                                """));
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
                        .returns(withRawPartitionTimestamps("""
                                ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted
                                1970-01-01T00:00:00.000010Z\tup~\tx\t2\t0\t6\t3\t2\t-1\t1970-01-01T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000011Z\tup~\tstr\t2\t0\t11\t3\t2\t-1\t1970-01-01T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                1970-01-01T00:00:00.000012Z\tup~\tsym2\t2\t0\t12\t3\t2\t-1\t1970-01-01T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000060Z
                                """));
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
    public void testPurgeCompletedBeforeTruncateIsNotReportedAsRefused() throws Exception {
        // The purge of the first UPDATE completes before DROP PARTITION, and the purge of the second UPDATE
        // waits for a reader until after it. The purge log holds the rows of both with the truncate version
        // from before DROP PARTITION and marks them all completed, but the job refuses only the second
        // purge: it reports the instance that the second UPDATE replaced, in the partitions that DROP
        // PARTITION keeps. Under ADAPTIVE the purge of the first UPDATE also waits for the save points.
        assertMemoryLeak(() -> {
            listenToTruncateRefusals();
            for (String walMode : new String[]{"BYPASS WAL", "WAL"}) {
                startTruncateIteration();
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    createTruncateTable(walMode, "INDEX");
                    updateTruncateTableWithReader(purgeJob, walMode);
                    if (isAdaptiveWal(walMode)) {
                        // the save points still name the instance that UPDATE replaced
                        runPurgeJob(purgeJob);
                        Assert.assertEquals(walMode, 3, purgeJob.getOutstandingPurgeTasks());
                        assertPurgeLogRows(purgeJob, 9, 0);
                        releaseSavePointHolds("t");
                    }
                    runPurgeJob(purgeJob);
                    Assert.assertEquals(walMode, 0, purgeJob.getOutstandingPurgeTasks());
                    assertPurgeLogRows(purgeJob, 9, 9);
                    final String[] dirs = {partitionDir("t", 0), partitionDir("t", 2)};
                    for (String dir : dirs) {
                        for (String col : new String[]{"x", "str", "sym"}) {
                            Assert.assertEquals(walMode + ", " + dir, "[]", filesOfInstance(listFiles(dir), col, TableUtils.COLUMN_NAME_TXN_NONE).toString());
                        }
                    }
                    final List<String> expected = new ArrayList<>();
                    for (String col : new String[]{"x", "str", "sym"}) {
                        expected.add(truncateRefusalKeyOfT(col, "1970-01-01", 0, getColumnNameTxn("t", col, 0)));
                        expected.add(truncateRefusalKeyOfT(col, "1970-01-03", 2, getColumnNameTxn("t", col, 2)));
                    }
                    updateTruncateTableWithReader(purgeJob, walMode);
                    Assert.assertEquals(walMode, "[]", truncateRefusedEntries.toString());

                    dropMiddlePartitionOfTruncateTable(walMode);
                    runPurgeJob(purgeJob);
                    Assert.assertEquals(walMode, sorted(expected), sorted(truncateRefusedEntries));
                    Assert.assertEquals(walMode, 0, purgeJob.getOutstandingPurgeTasks());
                    assertPurgeLogRows(purgeJob, 18, 18);
                    // the rows of the completed purge carry the old truncate version too
                    assertQuery("SELECT count() c, count(completed) completed FROM \"" + purgeJob.getLogTableName()
                            + "\" WHERE truncate_version <> 1 AND column_version = " + TableUtils.COLUMN_NAME_TXN_NONE)
                            .noLeakCheck()
                            .expectSize()
                            .noRandomAccess()
                            .returns("c\tcompleted\n9\t9\n");
                }
                dropTruncateTable();
            }
        });
    }

    @Test
    public void testPurgeRefusedByTruncateAfterPartialRemoval() throws Exception {
        // The job removes sym.d of instance NONE of sym in 1970-01-01, fails to remove its sym.k and
        // purges the other partitions. DROP PARTITION follows, and the job refuses the retry: it reports
        // the entry of 1970-01-01 only, whose sym.k and sym.v stay. The entry of 1970-01-03 has no files
        // left, and the existence check completes it.
        final AtomicReference<String> failingFileSuffix = new AtomicReference<>();
        final AtomicInteger remainingFailures = new AtomicInteger();
        final AtomicInteger failedRemoveCount = new AtomicInteger();
        ff = newFailingRemoveFilesFacade(failingFileSuffix, remainingFailures, failedRemoveCount);
        assertMemoryLeak(ff, () -> {
            listenToTruncateRefusals();
            for (String walMode : new String[]{"BYPASS WAL", "WAL"}) {
                startTruncateIteration();
                failedRemoveCount.set(0);
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    createTruncateTable(walMode, "INDEX");
                    updateTruncateTableWithReader(purgeJob, walMode);
                    // under ADAPTIVE the save points still name the instance that UPDATE replaced
                    releaseSavePointHolds("t");
                    final String firstDir = partitionDir("t", 0);
                    final String lastDir = partitionDir("t", 2);
                    final String expected = truncateRefusalKeyOfT("sym", "1970-01-01", 0, TableUtils.COLUMN_NAME_TXN_NONE);
                    failingFileSuffix.set(Files.SEPARATOR + new File(firstDir).getName() + Files.SEPARATOR + "sym.k");
                    remainingFailures.set(1);
                    engine.releaseInactive();
                    setCurrentMicros(currentMicros + 100_000_000L);
                    purgeJob.run();
                    Assert.assertEquals(walMode, 1, failedRemoveCount.get());
                    Assert.assertEquals(walMode, 1, purgeJob.getOutstandingPurgeTasks());
                    Assert.assertEquals(walMode, "[sym.k, sym.v]", filesOfInstance(listFiles(firstDir), "sym", TableUtils.COLUMN_NAME_TXN_NONE).toString());
                    Assert.assertEquals(walMode, "[]", filesOfInstance(listFiles(lastDir), "sym", TableUtils.COLUMN_NAME_TXN_NONE).toString());
                    Assert.assertEquals(walMode, "[]", truncateRefusedEntries.toString());

                    dropMiddlePartitionOfTruncateTable(walMode);
                    engine.releaseInactive();
                    setCurrentMicros(currentMicros + 100_000_000L);
                    purgeJob.run();
                    Assert.assertEquals(walMode, "[" + expected + "]", truncateRefusedEntries.toString());
                    Assert.assertEquals(walMode, 0, purgeJob.getOutstandingPurgeTasks());
                    Assert.assertEquals(walMode, 1, failedRemoveCount.get());
                    Assert.assertEquals(walMode, "[sym.k, sym.v]", filesOfInstance(listFiles(firstDir), "sym", TableUtils.COLUMN_NAME_TXN_NONE).toString());
                }
                dropTruncateTable();
            }
        });
    }

    @Test
    public void testPurgeRefusedByTruncateAfterPartialRemovalOfPostingIndex() throws Exception {
        // The job fails to remove a sealed .pv file of instance NONE of sym in each partition, and
        // removes sym.d and sym.pk. DROP PARTITION follows, and the job refuses the retry: the scan for
        // sealed files finds the .pv files, so it reports the entries of 1970-01-01 and 1970-01-03, whose
        // .pv files stay.
        final AtomicBoolean isValueFileFailureArmed = new AtomicBoolean();
        // the partition directory and the name of the .pv file whose removal failed there
        final Map<String, String> failedValueFiles = new ConcurrentHashMap<>();
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                if (isValueFileFailureArmed.get()) {
                    final File file = new File(Utf8s.stringFromUtf8Bytes(name));
                    // the first sealed .pv file of sym in each partition
                    if (file.getName().startsWith("sym.pv.") && failedValueFiles.putIfAbsent(file.getParent(), file.getName()) == null) {
                        return false;
                    }
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            listenToTruncateRefusals();
            for (String walMode : new String[]{"BYPASS WAL", "WAL"}) {
                startTruncateIteration();
                failedValueFiles.clear();
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    createTruncateTable(walMode, "INDEX TYPE POSTING");
                    updateTruncateTableWithReader(purgeJob, walMode);
                    final String[] dirs = {partitionDir("t", 0), partitionDir("t", 2)};
                    for (String dir : dirs) {
                        final String files = filesOfInstance(listFiles(dir), "sym", TableUtils.COLUMN_NAME_TXN_NONE).toString();
                        Assert.assertTrue(walMode + ", " + files, files.contains("sym.pk") && files.contains("sym.pv."));
                    }
                    final List<String> expected = Arrays.asList(
                            truncateRefusalKeyOfT("sym", "1970-01-01", 0, TableUtils.COLUMN_NAME_TXN_NONE),
                            truncateRefusalKeyOfT("sym", "1970-01-03", 2, TableUtils.COLUMN_NAME_TXN_NONE)
                    );
                    // under ADAPTIVE the save points still name the instance that UPDATE replaced
                    releaseSavePointHolds("t");
                    engine.releaseInactive();
                    setCurrentMicros(currentMicros + 100_000_000L);
                    isValueFileFailureArmed.set(true);
                    purgeJob.run();
                    isValueFileFailureArmed.set(false);
                    Assert.assertEquals(walMode, 3, failedValueFiles.size());
                    Assert.assertEquals(walMode, 1, purgeJob.getOutstandingPurgeTasks());
                    final String[] failedNames = new String[dirs.length];
                    for (int i = 0; i < dirs.length; i++) {
                        failedNames[i] = failedValueFiles.get(new File(dirs[i]).getPath());
                        Assert.assertNotNull(walMode + ", " + dirs[i] + " in " + failedValueFiles, failedNames[i]);
                        Assert.assertEquals(walMode, "[" + failedNames[i] + "]", filesOfInstance(listFiles(dirs[i]), "sym", TableUtils.COLUMN_NAME_TXN_NONE).toString());
                    }
                    Assert.assertEquals(walMode, "[]", truncateRefusedEntries.toString());

                    dropMiddlePartitionOfTruncateTable(walMode);
                    engine.releaseInactive();
                    setCurrentMicros(currentMicros + 100_000_000L);
                    purgeJob.run();
                    Assert.assertEquals(walMode, sorted(expected), sorted(truncateRefusedEntries));
                    Assert.assertEquals(walMode, 0, purgeJob.getOutstandingPurgeTasks());
                    for (int i = 0; i < dirs.length; i++) {
                        Assert.assertEquals(walMode, "[" + failedNames[i] + "]", filesOfInstance(listFiles(dirs[i]), "sym", TableUtils.COLUMN_NAME_TXN_NONE).toString());
                    }
                }
                dropTruncateTable();
            }
        });
    }

    @Test
    public void testPurgeTruncateRefusalListenerPassesThroughConfigurationWrapper() {
        // CairoConfigurationWrapper must hand out the delegate's listener, or a wrapped configuration
        // silently drops the refusal reports
        final ObjIntConsumer<ColumnPurgeTask> listener = (task, offset) -> {
        };
        node1.getConfigurationOverrides().setColumnPurgeTruncateRefusalListener(listener);
        Assert.assertSame(listener, new CairoConfigurationWrapper(configuration).getColumnPurgeTruncateRefusalListener());
    }

    @Test
    public void testPurgeRefusedByTruncateCompletesWithoutRetry() throws Exception {
        // DROP PARTITION changes the truncate version while the purge of UPDATE waits for a reader. The job
        // refuses the purge, marks its rows completed, does not retry it, and leaves the files of instance
        // NONE of x, str and sym in the partitions that DROP PARTITION keeps.
        assertMemoryLeak(() -> {
            // the test installs no listener, and Overrides.reset() cleared that of an earlier test
            Assert.assertNull(engine.getConfiguration().getColumnPurgeTruncateRefusalListener());
            for (String walMode : new String[]{"BYPASS WAL", "WAL"}) {
                startTruncateIteration();
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    createTruncateTable(walMode, "INDEX");
                    updateTruncateTableWithReader(purgeJob, walMode);
                    final String[] dirs = {partitionDir("t", 0), partitionDir("t", 2)};
                    final String[] listings = new String[dirs.length];
                    for (int i = 0; i < dirs.length; i++) {
                        listings[i] = listFiles(dirs[i]);
                        Assert.assertEquals(walMode, "[x.d]", filesOfInstance(listings[i], "x", TableUtils.COLUMN_NAME_TXN_NONE).toString());
                        Assert.assertEquals(walMode, "[str.d, str.i]", filesOfInstance(listings[i], "str", TableUtils.COLUMN_NAME_TXN_NONE).toString());
                        Assert.assertEquals(walMode, "[sym.d, sym.k, sym.v]", filesOfInstance(listings[i], "sym", TableUtils.COLUMN_NAME_TXN_NONE).toString());
                    }

                    dropMiddlePartitionOfTruncateTable(walMode);
                    for (int repeat = 0; repeat < 2; repeat++) {
                        runPurgeJob(purgeJob);
                        Assert.assertEquals(walMode, 0, purgeJob.getOutstandingPurgeTasks());
                        // the first run after DROP PARTITION, at 60 micros, completes all 9 rows
                        assertQuery("SELECT count() c, count(completed) completed, min(completed) first, max(completed) last FROM \""
                                + purgeJob.getLogTableName() + "\"")
                                .noLeakCheck()
                                .expectSize()
                                .noRandomAccess()
                                .returns("""
                                        c\tcompleted\tfirst\tlast
                                        9\t9\t1970-01-01T00:00:00.000060Z\t1970-01-01T00:00:00.000060Z
                                        """);
                        for (int i = 0; i < dirs.length; i++) {
                            Assert.assertEquals(walMode, listings[i], listFiles(dirs[i]));
                        }
                    }
                }
                dropTruncateTable();
            }
        });
    }

    @Test
    public void testPurgeRefusedByTruncateReportsEachEntryWithFiles() throws Exception {
        // Two UPDATEs run while a reader holds the table, and DROP PARTITION follows. The job refuses the
        // purges of both and reports each entry of instance NONE and of the instance of the first UPDATE
        // in the partitions that DROP PARTITION keeps. The entries of the dropped partition have no files
        // left, and the existence check completes them. Later runs report nothing.
        assertMemoryLeak(() -> {
            listenToTruncateRefusals();
            for (String walMode : new String[]{"BYPASS WAL", "WAL"}) {
                startTruncateIteration();
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    createTruncateTable(walMode, "INDEX");
                    final List<String> expected = new ArrayList<>();
                    for (String col : new String[]{"x", "str", "sym"}) {
                        expected.add(truncateRefusalKeyOfT(col, "1970-01-01", 0, TableUtils.COLUMN_NAME_TXN_NONE));
                        expected.add(truncateRefusalKeyOfT(col, "1970-01-03", 2, TableUtils.COLUMN_NAME_TXN_NONE));
                    }
                    try (TableReader ignore = getReader("t")) {
                        execute("UPDATE t SET x = 100, str = 'z', sym = 'Z'");
                        drainWalQueue();
                        for (String col : new String[]{"x", "str", "sym"}) {
                            final long firstColumnNameTxn = getColumnNameTxn("t", col, 0);
                            final long lastColumnNameTxn = getColumnNameTxn("t", col, 2);
                            Assert.assertTrue(walMode + ", " + col, firstColumnNameTxn > 0 && lastColumnNameTxn > 0);
                            expected.add(truncateRefusalKeyOfT(col, "1970-01-01", 0, firstColumnNameTxn));
                            expected.add(truncateRefusalKeyOfT(col, "1970-01-03", 2, lastColumnNameTxn));
                        }
                        // a reader of the first UPDATE keeps its instance, which UPDATE would purge at once otherwise
                        try (TableReader ignore2 = getReader("t")) {
                            execute("UPDATE t SET x = 200, str = 'y', sym = 'Y'");
                            drainWalQueue();
                            runPurgeJob(purgeJob);
                        }
                    }
                    Assert.assertEquals(walMode, 6, purgeJob.getOutstandingPurgeTasks());
                    assertPurgeLogRows(purgeJob, 18, 0);

                    dropMiddlePartitionOfTruncateTable(walMode);
                    runPurgeJob(purgeJob);
                    Assert.assertEquals(walMode, sorted(expected), sorted(truncateRefusedEntries));
                    Assert.assertEquals(walMode, 0, purgeJob.getOutstandingPurgeTasks());
                    assertPurgeLogRows(purgeJob, 18, 18);

                    runPurgeJob(purgeJob);
                    Assert.assertEquals(walMode, sorted(expected), sorted(truncateRefusedEntries));
                }
                dropTruncateTable();
            }
        });
    }

    @Test
    public void testPurgeRefusedByTruncateReportsSymbolRootFiles() throws Exception {
        // RENAME COLUMN of sym runs while a reader holds the table, and DROP PARTITION follows. The job
        // refuses the purge of the old name and reports its entries: the symbol table files in the table
        // root and the column files in the partitions that DROP PARTITION keeps. All of them stay.
        assertMemoryLeak(() -> {
            listenToTruncateRefusals();
            for (String walMode : new String[]{"BYPASS WAL", "WAL"}) {
                startTruncateIteration();
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    createTruncateTable(walMode, "INDEX");
                    final File tableDir = new File(configuration.getDbRoot().toString(), engine.verifyTableName("t").getDirName());
                    Assert.assertEquals(walMode, "[sym.c, sym.k, sym.o, sym.v]", symbolRootFiles(tableDir).toString());
                    final List<String> expected = Arrays.asList(
                            truncateRefusalKey("sym", PurgingOperator.TABLE_ROOT_PARTITION, -1, TableUtils.COLUMN_NAME_TXN_NONE),
                            truncateRefusalKeyOfT("sym", "1970-01-01", 0, TableUtils.COLUMN_NAME_TXN_NONE),
                            truncateRefusalKeyOfT("sym", "1970-01-03", 2, TableUtils.COLUMN_NAME_TXN_NONE)
                    );
                    final String[] dirs = {partitionDir("t", 0), partitionDir("t", 2)};
                    try (TableReader ignore = getReader("t")) {
                        execute("ALTER TABLE t RENAME COLUMN sym TO sym2");
                        drainWalQueue();
                        runPurgeJob(purgeJob);
                    }
                    Assert.assertEquals(walMode, 1, purgeJob.getOutstandingPurgeTasks());

                    dropMiddlePartitionOfTruncateTable(walMode);
                    runPurgeJob(purgeJob);
                    Assert.assertEquals(walMode, sorted(expected), sorted(truncateRefusedEntries));
                    Assert.assertEquals(walMode, 0, purgeJob.getOutstandingPurgeTasks());
                    Assert.assertEquals(walMode, "[sym.c, sym.k, sym.o, sym.v]", symbolRootFiles(tableDir).toString());
                    for (String dir : dirs) {
                        Assert.assertEquals(walMode, "[sym.d, sym.k, sym.v]", filesOfInstance(listFiles(dir), "sym", TableUtils.COLUMN_NAME_TXN_NONE).toString());
                    }
                }
                dropTruncateTable();
            }
        });
    }

    @Test
    public void testPurgeDuringDropTable() throws Exception {
        assertPurgeDuringDropTable(false);
    }

    @Test
    public void testPurgeDuringFailedDropTable() throws Exception {
        assertPurgeDuringDropTable(true);
    }

    @Test
    public void testPurgeMixesRetryAndLiveEntries() throws Exception {
        // The table id of t cannot be read for the first entry of the purge only. The job retries that
        // entry and purges the other two partitions in the same run; the purge log marks the rows of the
        // task complete once every entry is done.
        final AtomicBoolean isMetaOpenFailureArmed = new AtomicBoolean();
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (isMetaFileOfT(name) && isMetaOpenFailureArmed.compareAndSet(true, false)) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                final String[] oldFiles = dropIndexOfRetryTableWithReader(purgeJob, "BYPASS WAL");
                final String[] listings = listRetryTablePartitions(oldFiles);
                engine.releaseInactive();
                setCurrentMicros(currentMicros + 10L * iteration++);
                isMetaOpenFailureArmed.set(true);
                purgeJob.run();
                Assert.assertFalse(isMetaOpenFailureArmed.get());
                Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
                assertPurgeLogRows(purgeJob, 3, 0);
                Assert.assertEquals(listings[0], listFiles(partitionDir("t", 0)));
                for (int i = 1; i < 3; i++) {
                    Assert.assertEquals(withoutFiles(listings[i], oldFiles[i]), listFiles(partitionDir("t", i)));
                }

                runPurgeJob(purgeJob);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                assertPurgeLogRows(purgeJob, 3, 3);
                for (int i = 0; i < 3; i++) {
                    Assert.assertEquals(withoutFiles(listings[i], oldFiles[i]), listFiles(partitionDir("t", i)));
                }
            }
            assertRowsOfRetryTable();
        });
    }

    @Test
    public void testPurgeOfRecreatedTableWithUnreadableMetaIsOrphan() throws Exception {
        // t is dropped and created again under the same name while its purge waits. The registry holds
        // another table id, so the job drops the purge even though it cannot read _meta, and the files
        // of the new table stay.
        final AtomicBoolean isMetaOpenFailing = new AtomicBoolean();
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (isMetaOpenFailing.get() && isMetaFileOfT(name)) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(() -> {
            listenToTruncateRefusals();
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                dropIndexOfRetryTableWithReader(purgeJob, "BYPASS WAL");
                engine.releaseInactive();
                execute("DROP TABLE t");
                createRetryTable("BYPASS WAL");
                final String[] listings = new String[3];
                for (int i = 0; i < 3; i++) {
                    listings[i] = listFiles(partitionDir("t", i));
                    Assert.assertTrue(listings[i], (" " + listings[i] + " ").contains(" sym.d "));
                }
                engine.releaseInactive();
                isMetaOpenFailing.set(true);
                runPurgeJob(purgeJob);
                isMetaOpenFailing.set(false);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                assertPurgeLogRows(purgeJob, 3, 3);
                for (int i = 0; i < 3; i++) {
                    Assert.assertEquals(listings[i], listFiles(partitionDir("t", i)));
                }
                // the job drops the purge of an orphan table, which no truncate refused
                Assert.assertEquals("[]", truncateRefusedEntries.toString());
            }
            assertRowsOfRetryTable();
        });
    }

    @Test
    public void testPurgeOfTableWithMetaOfAnotherTableIsOrphan() throws Exception {
        // The registry lists t with the table id of the purge, but _meta of t holds the id of another
        // table. The job drops the purge and leaves the files.
        final AtomicBoolean isMetaOfOtherTable = new AtomicBoolean();
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (isMetaOfOtherTable.get() && isMetaFileOfT(name)) {
                    try (Path other = new Path()) {
                        other.of(configuration.getDbRoot()).concat(engine.verifyTableName("other")).concat(TableUtils.META_FILE_NAME);
                        return super.openRO(other.$());
                    }
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(() -> {
            execute("CREATE TABLE other (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            listenToTruncateRefusals();
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                final String[] oldFiles = dropIndexOfRetryTableWithReader(purgeJob, "BYPASS WAL");
                final String[] listings = listRetryTablePartitions(oldFiles);
                engine.releaseInactive();
                isMetaOfOtherTable.set(true);
                runPurgeJob(purgeJob);
                isMetaOfOtherTable.set(false);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                assertPurgeLogRows(purgeJob, 3, 3);
                for (int i = 0; i < 3; i++) {
                    Assert.assertEquals(listings[i], listFiles(partitionDir("t", i)));
                }
                // the job drops the purge of an orphan table, which no truncate refused
                Assert.assertEquals("[]", truncateRefusedEntries.toString());
            }
            assertRowsOfRetryTable();
        });
    }

    @Test
    public void testPurgeRetriesEntryAfterShortReadOfMeta() throws Exception {
        // _meta of t opens, but the read of the table id returns fewer bytes than an int has, once. The
        // job retries the first entry of the purge rather than drop it as a purge of another table, and
        // purges the other two partitions in the same run; the next run purges the first one.
        final AtomicBoolean isMetaShortReadArmed = new AtomicBoolean();
        final AtomicLong metaFd = new AtomicLong(-1);
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                final long fd = super.openRO(name);
                if (isMetaShortReadArmed.get() && isMetaFileOfT(name)) {
                    metaFd.set(fd);
                }
                return fd;
            }

            @Override
            public long read(long fd, long buf, long len, long offset) {
                if (fd == metaFd.get() && offset == TableUtils.META_OFFSET_TABLE_ID && isMetaShortReadArmed.compareAndSet(true, false)) {
                    metaFd.set(-1);
                    // the bytes past the short read hold what the buffer held before, not the table id
                    Unsafe.getUnsafe().setMemory(buf, len, (byte) 0xFF);
                    return super.read(fd, buf, len - 1, offset);
                }
                return super.read(fd, buf, len, offset);
            }
        };
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                final String[] oldFiles = dropIndexOfRetryTableWithReader(purgeJob, "BYPASS WAL");
                final String[] listings = listRetryTablePartitions(oldFiles);
                engine.releaseInactive();
                setCurrentMicros(currentMicros + 10L * iteration++);
                isMetaShortReadArmed.set(true);
                purgeJob.run();
                Assert.assertFalse(isMetaShortReadArmed.get());
                Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
                assertPurgeLogRows(purgeJob, 3, 0);
                Assert.assertEquals(listings[0], listFiles(partitionDir("t", 0)));
                for (int i = 1; i < 3; i++) {
                    Assert.assertEquals(withoutFiles(listings[i], oldFiles[i]), listFiles(partitionDir("t", i)));
                }

                runPurgeJob(purgeJob);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                assertPurgeLogRows(purgeJob, 3, 3);
                for (int i = 0; i < 3; i++) {
                    Assert.assertEquals(withoutFiles(listings[i], oldFiles[i]), listFiles(partitionDir("t", i)));
                }
            }
            assertRowsOfRetryTable();
        });
    }

    @Test
    public void testPurgeRetriesEntryDuringMetaSwap() throws Exception {
        assertPurgeRetriesEntryDuringMetaSwap(false);
    }

    @Test
    public void testPurgeRetryAfterMetaSwapReplayedOnRestart() throws Exception {
        assertPurgeRetriesEntryDuringMetaSwap(true);
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
                        .returns(withRawPartitionTimestamps("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n"));
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
                        .returns(withRawPartitionTimestamps("""
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
                                """));
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
                        .returns(withRawPartitionTimestamps("""
                                ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted
                                1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t11\t2022-03-29T00:00:00.000000Z\t-1\t
                                1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t-1\t2022-04-05T00:00:00.000000Z\t2\t
                                1970-01-01T00:00:00.000001Z\ttbl_name2\tcol2\t2\t0\t12\t3\t33\t-1\t2022-02-13T00:00:00.000000Z\t3\t
                                """));

                runPurgeJob(purgeJob);
                assertQuery(purgeJob.getLogTableName())
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns(withRawPartitionTimestamps("""
                                ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted
                                1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t11\t2022-03-29T00:00:00.000000Z\t-1\t1970-01-01T00:00:00.000010Z
                                1970-01-01T00:00:00.000000Z\ttbl_name\tcol\t1\t0\t5\t3\t43\t-1\t2022-04-05T00:00:00.000000Z\t2\t1970-01-01T00:00:00.000010Z
                                1970-01-01T00:00:00.000001Z\ttbl_name2\tcol2\t2\t0\t12\t3\t33\t-1\t2022-02-13T00:00:00.000000Z\t3\t1970-01-01T00:00:00.000010Z
                                """));
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
                            // under ADAPTIVE the save points still name the version the change moved away from
                            releaseSavePointHolds("t_z");
                            releaseSavePointHolds("t_twin");
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
                        // no reader: the writer purges the instance NONE itself, or under ADAPTIVE, whose save
                        // points still name it, leaves it to the purge job
                        update("UPDATE t_pci SET sym = 'B' WHERE ts >= '1970-01-03'");
                        drainWalQueue();
                        releaseSavePointHolds("t_pci");
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
                    // under ADAPTIVE the save points still name the instance that UPDATE replaced
                    releaseSavePointHolds("t_pci");

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
        // the purge job, whose first run removes the file. Under ADAPTIVE the save points still name the
        // old instance, so the writer hands the whole purge to the purge job, whose first run after the
        // save points fails on the .pci file the same way.
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
                    if (isAdaptiveWal(walMode)) {
                        Assert.assertEquals(walMode, 0, failedRemoveCount.get());
                        for (int i = 0; i < 3; i++) {
                            final String listing = listFiles(partitionDir("t_pci", i + 2));
                            Assert.assertTrue(
                                    walMode + ", " + listing,
                                    new TreeSet<>(Arrays.asList(listing.split(" "))).containsAll(Arrays.asList(oldInstanceFiles[i].split(" ")))
                            );
                        }
                        releaseSavePointHolds("t_pci");
                        // the first run takes the task off the queue, the second one purges
                        runPurgeJob(purgeJob);
                        Assert.assertEquals(walMode, 1, purgeJob.getOutstandingPurgeTasks());
                    }
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
                        // under ADAPTIVE the save points still name instance 0; only the reader holds it then
                        releaseSavePointHolds("t_z");
                        releaseSavePointHolds("t_twin");
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
    public void testReplayedPurgeRefusedByTruncateReportsEntries() throws Exception {
        // The purge job closes with the purge of UPDATE incomplete in its log, and DROP PARTITION follows.
        // A new purge job replays the purge, refuses it, reports each entry in the partitions that DROP
        // PARTITION keeps, leaves the files there and truncates the log.
        assertMemoryLeak(() -> {
            listenToTruncateRefusals();
            for (String walMode : new String[]{"BYPASS WAL", "WAL"}) {
                startTruncateIteration();
                final String[] dirs;
                final String[] listings;
                final List<String> expected = new ArrayList<>();
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    createTruncateTable(walMode, "INDEX");
                    updateTruncateTableWithReader(purgeJob, walMode);
                    dirs = new String[]{partitionDir("t", 0), partitionDir("t", 2)};
                    listings = new String[]{listFiles(dirs[0]), listFiles(dirs[1])};
                    for (String col : new String[]{"x", "str", "sym"}) {
                        expected.add(truncateRefusalKeyOfT(col, "1970-01-01", 0, TableUtils.COLUMN_NAME_TXN_NONE));
                        expected.add(truncateRefusalKeyOfT(col, "1970-01-03", 2, TableUtils.COLUMN_NAME_TXN_NONE));
                    }
                }
                dropMiddlePartitionOfTruncateTable(walMode);
                engine.releaseInactive();
                Assert.assertEquals(walMode, "[]", truncateRefusedEntries.toString());
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    Assert.assertEquals(walMode, sorted(expected), sorted(truncateRefusedEntries));
                    Assert.assertEquals(walMode, 0, purgeJob.getOutstandingPurgeTasks());
                    assertPurgeLogRows(purgeJob, 0, 0);
                    for (int i = 0; i < dirs.length; i++) {
                        Assert.assertEquals(walMode, listings[i], listFiles(dirs[i]));
                    }
                }
                dropTruncateTable();
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
        // the task stays incomplete in the purge log. A new purge job replays it with the POSTING removal,
        // which the replay takes for every index type, as the purge log keeps none. s.d is gone and s.pk
        // never existed, so the directory scan finds only the .pc files of instance 0: the replay removes
        // s.k and s.v and keeps them.
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

    /**
     * The job runs while non-WAL DROP TABLE t removes the table directory, after _meta is gone and before
     * the registry forgets t: it retries the purge. With {@code isDropFailing}, the removal fails, DROP
     * TABLE fails, the registry still lists t without _meta, and the job keeps retrying until DROP TABLE
     * runs again. Once the directory is gone, the existence check completes the purge.
     */
    private void assertPurgeDuringDropTable(boolean isDropFailing) throws Exception {
        final AtomicBoolean isPurgeInDropArmed = new AtomicBoolean();
        final AtomicReference<ColumnPurgeJob> purgeJobRef = new AtomicReference<>();
        final AtomicReference<String[]> listingsInDrop = new AtomicReference<>();
        final AtomicInteger outstandingInDrop = new AtomicInteger(-1);
        final String[] partitionNames = new String[3];
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean unlinkOrRemove(Path path, Log log) {
                if (new File(Utf8s.stringFromUtf8Bytes(path.$())).getName().startsWith("t~") && isPurgeInDropArmed.compareAndSet(true, false)) {
                    final int len = path.size();
                    Assert.assertTrue(removeQuiet(path.concat(TableUtils.META_FILE_NAME).$()));
                    path.trimTo(len);
                    setCurrentMicros(currentMicros + 10L * iteration++);
                    purgeJobRef.get().run();
                    outstandingInDrop.set(purgeJobRef.get().getOutstandingPurgeTasks());
                    final String[] listings = new String[3];
                    for (int i = 0; i < 3; i++) {
                        listings[i] = listFiles(new File(Utf8s.stringFromUtf8Bytes(path.$()), partitionNames[i]).getAbsolutePath());
                    }
                    listingsInDrop.set(listings);
                    if (isDropFailing) {
                        return false;
                    }
                }
                return super.unlinkOrRemove(path, log);
            }
        };
        assertMemoryLeak(() -> {
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                purgeJobRef.set(purgeJob);
                final String[] oldFiles = dropIndexOfRetryTableWithReader(purgeJob, "BYPASS WAL");
                final String[] listings = listRetryTablePartitions(oldFiles);
                final File tableDir = new File(configuration.getDbRoot().toString(), engine.verifyTableName("t").getDirName());
                for (int i = 0; i < 3; i++) {
                    partitionNames[i] = new File(partitionDir("t", i)).getName();
                }
                engine.releaseInactive();
                isPurgeInDropArmed.set(true);
                try {
                    execute("DROP TABLE t");
                    Assert.assertFalse(isDropFailing);
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage(), isDropFailing);
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not remove table [table=t~");
                }
                Assert.assertFalse(isPurgeInDropArmed.get());
                Assert.assertEquals(1, outstandingInDrop.get());
                Assert.assertArrayEquals(listings, listingsInDrop.get());

                if (isDropFailing) {
                    Assert.assertNotNull(engine.getTableTokenIfExists("t"));
                    runPurgeJob(purgeJob);
                    Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
                    assertPurgeLogRows(purgeJob, 3, 0);
                    for (int i = 0; i < 3; i++) {
                        Assert.assertEquals(listings[i], listFiles(new File(tableDir, partitionNames[i]).getAbsolutePath()));
                    }
                    execute("DROP TABLE t");
                }
                Assert.assertNull(engine.getTableTokenIfExists("t"));
                Assert.assertFalse(tableDir.exists());
                runPurgeJob(purgeJob);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                assertPurgeLogRows(purgeJob, 3, 3);
            }
        });
    }

    /**
     * The job runs while ALTER TABLE t ADD COLUMN swaps _meta, between the renames of _meta to
     * _meta.prev and of _meta.swp to _meta, and retries the purge rather than dropping it. Then the job
     * runs again, or, with {@code isRestarted}, a new job replays the purge from the log.
     */
    private void assertPurgeRetriesEntryDuringMetaSwap(boolean isRestarted) throws Exception {
        final AtomicBoolean isPurgeInMetaSwapArmed = new AtomicBoolean();
        final AtomicReference<ColumnPurgeJob> purgeJobRef = new AtomicReference<>();
        final AtomicInteger outstandingInMetaSwap = new AtomicInteger(-1);
        ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                final int result = super.rename(from, to);
                if (isMetaFileOfT(from) && isPurgeInMetaSwapArmed.compareAndSet(true, false)) {
                    Assert.assertFalse(exists(from));
                    purgeJobRef.get().run();
                    outstandingInMetaSwap.set(purgeJobRef.get().getOutstandingPurgeTasks());
                }
                return result;
            }
        };
        assertMemoryLeak(() -> {
            for (String walMode : new String[]{"BYPASS WAL", "WAL"}) {
                execute("DROP TABLE IF EXISTS t");
                final String[] oldFiles;
                final String[] listings;
                ColumnPurgeJob purgeJob = createPurgeJob();
                try {
                    purgeJobRef.set(purgeJob);
                    oldFiles = dropIndexOfRetryTableWithReader(purgeJob, walMode);
                    engine.releaseInactive();
                    setCurrentMicros(currentMicros + 10L * iteration++);
                    outstandingInMetaSwap.set(-1);
                    isPurgeInMetaSwapArmed.set(true);
                    execute("ALTER TABLE t ADD COLUMN y INT");
                    drainWalQueue();
                    Assert.assertFalse(walMode, isPurgeInMetaSwapArmed.get());
                    Assert.assertEquals(walMode, 1, outstandingInMetaSwap.get());
                    Assert.assertEquals(walMode, 1, purgeJob.getOutstandingPurgeTasks());
                    assertPurgeLogRows(purgeJob, 3, 0);
                    listings = listRetryTablePartitions(oldFiles);
                    // under ADAPTIVE the save points still name the instance that DROP INDEX replaced, and the
                    // replay at start keeps such a task
                    releaseSavePointHolds("t");
                    if (isRestarted) {
                        purgeJob = Misc.free(purgeJob);
                        purgeJob = createPurgeJob();
                        purgeJobRef.set(purgeJob);
                    } else {
                        runPurgeJob(purgeJob);
                        assertPurgeLogRows(purgeJob, 3, 3);
                    }
                    Assert.assertEquals(walMode, 0, purgeJob.getOutstandingPurgeTasks());
                } finally {
                    Misc.free(purgeJob);
                }
                for (int i = 0; i < 3; i++) {
                    Assert.assertEquals(walMode, withoutFiles(listings[i], oldFiles[i]), listFiles(partitionDir("t", i)));
                }
                assertRowsOfRetryTable();
            }
        });
    }

    // the rows of t, which createRetryTable() writes
    private void assertRowsOfRetryTable() throws Exception {
        assertQuery("SELECT ts, x, sym FROM t")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns(replaceTimestampSuffix("""
                        ts\tx\tsym
                        1970-01-01T02:00:00.000000Z\t1\tA
                        1970-01-02T02:00:00.000000Z\t2\tB
                        1970-01-03T02:00:00.000000Z\t3\tA
                        """, timestampType.getTypeName()));
    }

    /**
     * Creates t with x LONG, str STRING and sym SYMBOL {@code symIndexClause}, and one row in each of the
     * daily partitions 1970-01-01, 1970-01-02 and 1970-01-03.
     */
    private void createTruncateTable(String walMode, String symIndexClause) throws SqlException {
        execute("CREATE TABLE t (ts " + timestampType.getTypeName() + ", x LONG, str STRING, sym SYMBOL " + symIndexClause
                + ") TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        execute("""
                INSERT INTO t VALUES
                ('1970-01-01T02:00:00.000000Z', 1, 'a', 'A'),
                ('1970-01-02T02:00:00.000000Z', 2, 'b', 'B'),
                ('1970-01-03T02:00:00.000000Z', 3, 'c', 'A')""");
        drainWalQueue();
    }

    /**
     * DROP PARTITION of 1970-01-02 of t moves the truncate version of t from 0 to 1. With no reader open, the
     * writer removes the dropped partition at once, but under ADAPTIVE the save points of a WAL table still
     * list it, and it stays until both are past the DROP. This takes them, so that the purge job finds the
     * dropped partition gone in every mode.
     */
    private void dropMiddlePartitionOfTruncateTable(String walMode) throws SqlException {
        engine.releaseInactive();
        final File droppedDir = new File(partitionDir("t", 1));
        execute("ALTER TABLE t DROP PARTITION LIST '1970-01-02'");
        drainWalQueue();
        try (TableReader reader = getReader("t")) {
            Assert.assertEquals(walMode, 1, reader.getTxFile().getTruncateVersion());
            Assert.assertEquals(walMode, 2, reader.getTxFile().getPartitionCount());
        }
        Assert.assertEquals(walMode, isAdaptiveWal(walMode), droppedDir.exists());
        engine.releaseInactive();
        releaseSavePointHolds("t");
        Assert.assertFalse(walMode, droppedDir.exists());
    }

    private void dropTruncateTable() throws SqlException {
        execute("DROP TABLE t");
        drainWalQueue();
    }

    private static long getPartitionNameTxn(String tableName, int partitionIndex) {
        try (TableReader reader = getReader(tableName)) {
            return reader.getTxFile().getPartitionNameTxn(partitionIndex);
        }
    }

    // Whether the table of walMode runs under ADAPTIVE, whose save points hold the versions a change supersedes
    private static boolean isAdaptiveWal(String walMode) {
        return "WAL".equals(walMode) && engine.getResolvedCommitMode() == CommitMode.ADAPTIVE;
    }

    // records the entries that the purge job reports as refused because the truncate version changed
    private void listenToTruncateRefusals() {
        node1.getConfigurationOverrides().setColumnPurgeTruncateRefusalListener((task, offset) -> {
            final LongList info = task.getUpdatedColumnInfo();
            truncateRefusedEntries.add(truncateRefusalKey(
                    task.getColumnName(),
                    info.getQuick(offset + ColumnPurgeTask.OFFSET_PARTITION_TIMESTAMP),
                    info.getQuick(offset + ColumnPurgeTask.OFFSET_PARTITION_NAME_TXN),
                    info.getQuick(offset + ColumnPurgeTask.OFFSET_COLUMN_VERSION)
            ));
        });
    }

    private static List<String> sorted(List<String> keys) {
        final List<String> sorted = new ArrayList<>(keys);
        Collections.sort(sorted);
        return sorted;
    }

    // resets the clock of the purge job runs and the reported entries for the next iteration over WAL modes
    private void startTruncateIteration() {
        iteration = 1;
        setCurrentMicros(0);
        truncateRefusedEntries.clear();
    }

    // the key of an entry of a purge task that the truncate refusal listener receives
    private static String truncateRefusalKey(CharSequence columnName, long partitionTimestamp, long partitionNameTxn, long columnVersion) {
        return columnName + "/" + partitionTimestamp + "/" + partitionNameTxn + "/" + columnVersion;
    }

    // the key of the entry of columnName in the partition of t that holds the day partition, at partitionIndex
    private String truncateRefusalKeyOfT(String columnName, String partition, int partitionIndex, long columnVersion) throws NumericException {
        return truncateRefusalKey(
                columnName,
                timestampType.getDriver().parseFloorLiteral(partition),
                getPartitionNameTxn("t", partitionIndex),
                columnVersion
        );
    }

    /**
     * Runs UPDATE of x, str and sym of t while a reader holds the table, so the purge job saves the three
     * purges in its log, one row per partition, and purges none.
     */
    private void updateTruncateTableWithReader(ColumnPurgeJob purgeJob, String walMode) throws Exception {
        final int outstandingPurgeTasks = purgeJob.getOutstandingPurgeTasks();
        try (TableReader ignore = getReader("t")) {
            execute("UPDATE t SET x = 100, str = 'z', sym = 'Z'");
            drainWalQueue();
            runPurgeJob(purgeJob);
        }
        Assert.assertEquals(walMode, outstandingPurgeTasks + 3, purgeJob.getOutstandingPurgeTasks());
    }

    // the number of rows of the purge log, and of those the purge job marked complete
    private void assertPurgeLogRows(ColumnPurgeJob purgeJob, int expectedCount, int expectedCompletedCount) throws Exception {
        assertQuery("SELECT count() c, count(completed) completed FROM \"" + purgeJob.getLogTableName() + "\"")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("c\tcompleted\n" + expectedCount + "\t" + expectedCompletedCount + "\n");
    }

    /**
     * Creates t with one row in each of three daily partitions and sym, declared in CREATE TABLE (column
     * name txn NONE), under a POSTING index that covers x.
     */
    private void createRetryTable(String walMode) throws SqlException {
        execute("CREATE TABLE t (ts " + timestampType.getTypeName() + ", x LONG, sym SYMBOL INDEX TYPE POSTING INCLUDE (x))"
                + " TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        execute("""
                INSERT INTO t VALUES
                ('1970-01-01T02:00:00.000000Z', 1, 'A'),
                ('1970-01-02T02:00:00.000000Z', 2, 'B'),
                ('1970-01-03T02:00:00.000000Z', 3, 'A')""");
        drainWalQueue();
    }

    /**
     * Creates t, see {@link #createRetryTable(String)}, and drops the index of sym while a reader holds
     * the table, so the purge job only saves the purge of instance NONE of sym in its log, one row per
     * partition. Returns the files of that instance in each partition.
     */
    private String[] dropIndexOfRetryTableWithReader(ColumnPurgeJob purgeJob, String walMode) throws Exception {
        createRetryTable(walMode);
        final String[] oldFiles = new String[3];
        for (int i = 0; i < 3; i++) {
            Assert.assertEquals(TableUtils.COLUMN_NAME_TXN_NONE, getColumnNameTxn("t", "sym", i));
            final String listing = listFiles(partitionDir("t", i));
            oldFiles[i] = String.join(" ", filesOfInstance(listing, "sym", TableUtils.COLUMN_NAME_TXN_NONE));
            Assert.assertEquals(String.join(" ", filesWithPrefix(listing, "sym.")), oldFiles[i]);
            for (String file : new String[]{" sym.d ", " sym.pc0.0.", " sym.pci ", " sym.pk ", " sym.pv."}) {
                Assert.assertTrue(file + " in " + oldFiles[i], (" " + oldFiles[i] + " ").contains(file));
            }
        }
        try (TableReader ignored = getReader("t")) {
            execute("ALTER TABLE t ALTER COLUMN sym DROP INDEX");
            drainWalQueue();
            runPurgeJob(purgeJob);
        }
        Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
        assertPurgeLogRows(purgeJob, 3, 0);
        return oldFiles;
    }

    // whether name is the _meta file of the table t
    private static boolean isMetaFileOfT(LPSZ name) {
        final File file = new File(Utf8s.stringFromUtf8Bytes(name));
        final File parent = file.getParentFile();
        return TableUtils.META_FILE_NAME.equals(file.getName()) && parent != null && parent.getName().startsWith("t~");
    }

    // the listing of each partition of t, which still holds the files of the old instance of sym
    private String[] listRetryTablePartitions(String[] oldFiles) {
        final String[] listings = new String[3];
        for (int i = 0; i < 3; i++) {
            listings[i] = listFiles(partitionDir("t", i));
            Assert.assertEquals(oldFiles[i], String.join(" ", filesOfInstance(listings[i], "sym", TableUtils.COLUMN_NAME_TXN_NONE)));
        }
        return listings;
    }

    // the space-separated listing without the space-separated files
    private static String withoutFiles(String listing, String files) {
        final TreeSet<String> remaining = new TreeSet<>(Arrays.asList(listing.split(" ")));
        remaining.removeAll(Arrays.asList(files.split(" ")));
        return String.join(" ", remaining);
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
                        .returns(withRawPartitionTimestamps("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\n"));
            }
        });
    }

    @Test
    public void testReplayedPurgeRemovesPostingFilesOfChangedIndex() throws Exception {
        // The purge log keeps no index type. The replayed purge of the old instance of sym removes its
        // POSTING files whatever index the live column has, and when no live column has its name.
        assertMemoryLeak(() -> {
            final String[][] changes = {
                    // live column, changes
                    {"sym", "ALTER TABLE t_replay ALTER COLUMN sym DROP INDEX"},
                    {"sym", "ALTER TABLE t_replay ALTER COLUMN sym DROP INDEX", "ALTER TABLE t_replay ALTER COLUMN sym ADD INDEX TYPE BITMAP"},
                    {"sym", "ALTER TABLE t_replay ALTER COLUMN sym DROP INDEX", "ALTER TABLE t_replay ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (x)"},
                    {null, "ALTER TABLE t_replay DROP COLUMN sym"},
                    {"sym2", "ALTER TABLE t_replay RENAME COLUMN sym TO sym2"},
            };
            for (String oldIndexClause : new String[]{"POSTING", "POSTING DELTA", "POSTING EF", "POSTING INCLUDE (x)"}) {
                for (String[] change : changes) {
                    assertReplayedPurgeOfChangedIndex("PARTITION BY DAY BYPASS WAL", oldIndexClause, change);
                }
            }
            // the old BITMAP index: the replay removes its .k and .v files
            assertReplayedPurgeOfChangedIndex("PARTITION BY DAY BYPASS WAL", "BITMAP", changes[0]);
            assertReplayedPurgeOfChangedIndex("PARTITION BY DAY BYPASS WAL", "BITMAP", changes[2]);
        });
    }

    @Test
    public void testReplayedPurgeRemovesPostingFilesOfDroppedIndexOfEveryTableKind() throws Exception {
        assertMemoryLeak(() -> {
            final String[] dropIndex = {"sym", "ALTER TABLE t_replay ALTER COLUMN sym DROP INDEX"};
            assertReplayedPurgeOfChangedIndex("PARTITION BY DAY WAL", "POSTING INCLUDE (x)", dropIndex);
            // not partitioned
            assertReplayedPurgeOfChangedIndex("", "POSTING INCLUDE (x)", dropIndex);
            assertReplayedPurgeOfChangedIndex("", "POSTING", dropIndex);
        });
    }

    @Test
    public void testReplayedPurgeRemovesPostingFilesOfRetypedColumn() throws Exception {
        assertMemoryLeak(() -> {
            for (String newType : new String[]{"VARCHAR", "STRING"}) {
                final String[] oldFiles = assertReplayedPurgeOfChangedIndex(
                        "PARTITION BY DAY BYPASS WAL",
                        "POSTING INCLUDE (x)",
                        new String[]{"sym", "ALTER TABLE t_replay ALTER COLUMN sym TYPE " + newType},
                        false
                );
                for (int i = 0; i < oldFiles.length; i++) {
                    final String listing = listFiles(partitionDir("t_replay", i));
                    final long liveTxn = getColumnNameTxn("t_replay", "sym", i);
                    Assert.assertTrue(newType + ", " + listing, liveTxn > 0);
                    final TreeSet<String> liveFiles = filesOfInstance(listing, "sym", liveTxn);
                    Assert.assertEquals(newType + ", " + listing, "[sym.d." + liveTxn + ", sym.i." + liveTxn + "]", liveFiles.toString());
                }
                execute("DROP TABLE t_replay");
            }
        });
    }

    @Test
    public void testReplayedPurgeRemovesPostingFilesOfUpdatedColumn() throws Exception {
        // UPDATE gives sym the column name txn of the update in the last three partitions, and the purge
        // job removes the files of instance NONE there. The replayed purge of the next change removes the
        // POSTING files named with that txn, sym.pv.<txn>.<seal> and sym.pc<N>.<txn>.<seal> among them.
        assertMemoryLeak(() -> {
            final String[][] changes = {
                    {"sym", "ALTER TABLE t_replay ALTER COLUMN sym DROP INDEX"},
                    {null, "ALTER TABLE t_replay DROP COLUMN sym"},
            };
            for (String[] change : changes) {
                createReplayTable("PARTITION BY DAY BYPASS WAL", "POSTING INCLUDE (x)");
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    update("UPDATE t_replay SET sym = 'Z' WHERE ts >= '1970-01-03'");
                    runPurgeJob(purgeJob);
                    Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                }
                final long updateTxn = getColumnNameTxn("t_replay", "sym", 2);
                Assert.assertTrue(updateTxn > 0);
                // the UPDATE leaves 'A' in the first row only; the rows 1 and 4 when sym is dropped
                final String expectedRows = change[0] != null ? """
                        ts\tx
                        1970-01-01T02:00:00.000000Z\t1
                        """ : ROWS_OF_A_IN_REPLAY_TABLE;
                final String[] oldFiles = assertReplayedPurgeRemovesOldInstance(
                        change[0],
                        replaceTimestampSuffix(expectedRows, timestampType.getTypeName()),
                        null,
                        null,
                        Arrays.copyOfRange(change, 1, change.length)
                );
                for (int i = 0; i < oldFiles.length; i++) {
                    final String suffix = i < 2 ? "" : "." + updateTxn;
                    final String paddedFiles = " " + oldFiles[i] + " ";
                    for (String file : new String[]{" sym.d" + suffix + " ", " sym.pk" + suffix + " ", " sym.pci" + suffix + " ", " sym.pv" + suffix + ".", " sym.pc0." + (i < 2 ? 0 : updateTxn) + "."}) {
                        Assert.assertTrue(change[1] + ", " + file + " in " + oldFiles[i], paddedFiles.contains(file));
                    }
                }
                execute("DROP TABLE t_replay");
            }
        });
    }

    @Test
    public void testReplayedPurgeRemovesPostingFilesOfColumnAddedAfterData() throws Exception {
        // sym, added with a POSTING index after the first two partitions, has no files in the first one.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_replay (ts " + timestampType.getTypeName() + ", x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_replay VALUES ('1970-01-01T02:00:00.000000Z', 1), ('1970-01-02T02:00:00.000000Z', 2)");
            execute("ALTER TABLE t_replay ADD COLUMN sym SYMBOL INDEX TYPE POSTING");
            execute("""
                    INSERT INTO t_replay VALUES
                    ('1970-01-03T02:00:00.000000Z', 3, 'C'),
                    ('1970-01-04T02:00:00.000000Z', 4, 'A'),
                    ('1970-01-05T02:00:00.000000Z', 5, 'B')""");
            final String[] oldFiles = assertReplayedPurgeRemovesOldInstance("sym", replaceTimestampSuffix("""
                    ts\tx
                    1970-01-04T02:00:00.000000Z\t4
                    """, timestampType.getTypeName()), null, null, "ALTER TABLE t_replay ALTER COLUMN sym DROP INDEX");
            Assert.assertEquals("", oldFiles[0]);
            for (int i = 2; i < oldFiles.length; i++) {
                Assert.assertTrue(oldFiles[i], oldFiles[i].matches("sym\\.d\\.[1-9][0-9]* sym\\.pk\\.[1-9][0-9]* sym\\.pv\\.[1-9][0-9]*\\.[0-9]+.*"));
            }
        });
    }

    @Test
    public void testReplayedPurgeRemovesPostingFilesOfParquetPartition() throws Exception {
        // CONVERT PARTITION TO PARQUET keeps the POSTING index files next to data.parquet.
        assertMemoryLeak(() -> {
            createReplayTable("PARTITION BY DAY BYPASS WAL", "POSTING INCLUDE (x)");
            execute("ALTER TABLE t_replay CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            final String[] oldFiles = assertReplayedPurgeRemovesOldInstance(
                    "sym",
                    replaceTimestampSuffix(ROWS_OF_A_IN_REPLAY_TABLE, timestampType.getTypeName()),
                    null,
                    null,
                    "ALTER TABLE t_replay ALTER COLUMN sym DROP INDEX"
            );
            final String paddedFiles = " " + oldFiles[0] + " ";
            for (String file : new String[]{" sym.pk ", " sym.pci ", " sym.pv.", " sym.pc0.0."}) {
                Assert.assertTrue(file + " in " + oldFiles[0], paddedFiles.contains(file));
            }
            Assert.assertTrue((" " + listFiles(partitionDir("t_replay", 0)) + " ").contains(" data.parquet "));
        });
    }

    @Test
    public void testReplayedPurgeOfDroppedPostingIndexWithFailingRemove() throws Exception {
        // The replay cannot remove sym.pv.1 of the old instance in the middle partition and removes every
        // other file of the old instance.
        final AtomicReference<String> failingFileSuffix = new AtomicReference<>();
        final AtomicInteger remainingFailures = new AtomicInteger();
        final AtomicInteger failedRemoveCount = new AtomicInteger();
        ff = newFailingRemoveFilesFacade(failingFileSuffix, remainingFailures, failedRemoveCount);
        assertMemoryLeak(ff, () -> {
            createReplayTable("PARTITION BY DAY BYPASS WAL", "POSTING INCLUDE (x)");
            final String middleDir = partitionDir("t_replay", 2);
            final String[] leftoverFiles = {null, null, "sym.pv.1", null, null};
            final String[] oldFiles = assertReplayedPurgeRemovesOldInstance(
                    "sym",
                    replaceTimestampSuffix(ROWS_OF_A_IN_REPLAY_TABLE, timestampType.getTypeName()),
                    () -> {
                        failingFileSuffix.set(Files.SEPARATOR + new File(middleDir).getName() + Files.SEPARATOR + "sym.pv.1");
                        remainingFailures.set(1);
                    },
                    leftoverFiles,
                    "ALTER TABLE t_replay ALTER COLUMN sym DROP INDEX"
            );
            Assert.assertEquals(1, failedRemoveCount.get());
            Assert.assertTrue(oldFiles[2], (" " + oldFiles[2] + " ").contains(" sym.pv.1 "));
        });
    }

    @Test
    public void testReplayedPurgeOfDroppedPostingIndexAfterFailedPurge() throws Exception {
        // No reader holds the old instance of sym, so DROP INDEX purges it at once. Neither DROP INDEX nor
        // the first purge job can remove its sym.pci in partition 2 and one of its sealed .pv files in
        // partition 3, and the job closes with the task incomplete in the purge log. Its .d and .pk files
        // are gone, and the live column has no index. A new purge job replays the task and removes both
        // files: its existence probe finds the .pci in partition 2 and the .pv by the directory scan in
        // partition 3.
        final AtomicBoolean isArmed = new AtomicBoolean();
        final String[] failingFileSuffixes = new String[2];
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                if (isArmed.get()) {
                    for (String suffix : failingFileSuffixes) {
                        if (Utf8s.endsWithAscii(name, suffix)) {
                            return false;
                        }
                    }
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            createReplayTable("PARTITION BY DAY BYPASS WAL", "POSTING INCLUDE (x)");
            final String[] dirs = new String[5];
            final long[] oldTxns = new long[5];
            for (int i = 0; i < 5; i++) {
                dirs[i] = partitionDir("t_replay", i);
                oldTxns[i] = getColumnNameTxn("t_replay", "sym", i);
            }
            final String sealedValueFile = filesWithPrefix(listFiles(dirs[3]), "sym.pv.").first();
            Assert.assertTrue(sealedValueFile, sealedValueFile.matches("sym\\.pv\\.[0-9]+"));
            failingFileSuffixes[0] = Files.SEPARATOR + new File(dirs[2]).getName() + Files.SEPARATOR + "sym.pci";
            failingFileSuffixes[1] = Files.SEPARATOR + new File(dirs[3]).getName() + Files.SEPARATOR + sealedValueFile;
            isArmed.set(true);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                execute("ALTER TABLE t_replay ALTER COLUMN sym DROP INDEX");
                runPurgeJob(purgeJob);
                Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
                // a row per partition of the task
                assertIncompletePurgeLogRows(purgeJob, 2);
            }
            isArmed.set(false);
            engine.releaseInactive();

            final String[] expectedOldFiles = {"[]", "[]", "[sym.pci]", "[" + sealedValueFile + "]", "[]"};
            final String[] listingsBefore = new String[5];
            for (int i = 0; i < 5; i++) {
                listingsBefore[i] = listFiles(dirs[i]);
                Assert.assertEquals(listingsBefore[i], expectedOldFiles[i], filesOfInstance(listingsBefore[i], "sym", oldTxns[i]).toString());
            }

            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                assertPurgeLogRows(purgeJob, 0, 0);
            }
            for (int i = 0; i < 5; i++) {
                final TreeSet<String> expectedFiles = new TreeSet<>(Arrays.asList(listingsBefore[i].split(" ")));
                expectedFiles.removeAll(filesOfInstance(listingsBefore[i], "sym", oldTxns[i]));
                Assert.assertEquals(dirs[i], String.join(" ", expectedFiles), listFiles(dirs[i]));
            }
            final String query = "SELECT ts, x FROM t_replay WHERE sym = 'A'";
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(replaceTimestampSuffix(ROWS_OF_A_IN_REPLAY_TABLE, timestampType.getTypeName()));
            assertSqlCursors("SELECT ts, x FROM t_replay WHERE sym::STRING = 'A'", query);
        });
    }

    @Test
    public void testReplayedPurgeOfPostingIndexDroppedAtTxnZero() throws Exception {
        // DROP INDEX as the first change of a table that is not partitioned gives s the column name txn 0
        // in default, next to the files of instance NONE. The replayed purge removes them and keeps s.d.0.
        assertMemoryLeak(() -> {
            createIndexDroppedAtTxnZeroTables("s", "INDEX TYPE POSTING INCLUDE (d, vc)");
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                try (TableReader ignored = getReader("pt")) {
                    execute("ALTER TABLE pt ALTER COLUMN s DROP INDEX");
                    runPurgeJob(purgeJob);
                    Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
                }
            }
            final String before = listFiles(defaultDir("pt"));
            Assert.assertEquals("[s.d, s.pk, s.pv.0]", filesOfInstanceNone(before, "s").toString());
            Assert.assertTrue(before, (" " + before + " ").contains(" s.d.0 "));
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                assertPurgeLogRows(purgeJob, 0, 0);
            }
            Assert.assertEquals(withoutFilesOfInstanceNone(before, "s"), listFiles(defaultDir("pt")));
            Assert.assertEquals(0, getColumnNameTxnOfDefault("pt", "s"));
            execute("ALTER TABLE pt ADD COLUMN l LONG");
            insertRowsOfIndexReAddedAtTxnZero("pt");
            final String query = "SELECT ts, s, d, vc, l FROM pt WHERE s = 'y'";
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\ts\td\tvc\tl\n" + timestamps(ROWS_OF_INDEX_RE_ADDED_AT_TXN_ZERO));
            assertSqlCursors("SELECT ts, s, d, vc, l FROM pt WHERE s::STRING = 'y'", query);
        });
    }

    @Test
    public void testPurgeLogGainsRawPartitionTimestampColumn() throws Exception {
        // A log that a version before partition_timestamp_raw created gets the column when the purge job starts,
        // and from then on the job saves there the partition timestamps as _txn has them.
        setSplitProperties();
        assertMemoryLeak(() -> {
            createLegacyPurgeLog();
            createSplitTable("BYPASS WAL", "c LONG, s SYMBOL", ColumnPurgeJobTest::longAndSymbolValues);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                assertPurgeLogColumns(PURGE_LOG_COLUMNS);
                dropColumnsWithReader(purgeJob, "c", "s");
                assertRawPartitionTimestamps(purgeJob, "c", false);
                assertRawPartitionTimestamps(purgeJob, "s", true);
            }
            replayPurgeLog("t_split");
            Assert.assertEquals("", filesOfColumns("t_split", "c", "s"));
            assertRowsOfSplitTable();
        });
    }

    @Test
    public void testPurgeLogIgnoresRawPartitionTimestampColumnOfAnotherType() throws Exception {
        // The job did not write a partition_timestamp_raw of another type than LONG: it logs and replays without
        // the column, as the versions before it did.
        setSplitProperties();
        assertMemoryLeak(() -> {
            createLegacyPurgeLog();
            execute("ALTER TABLE '" + purgeLogTableName() + "' ADD COLUMN partition_timestamp_raw INT");
            createSplitTable("BYPASS WAL", "c LONG, s SYMBOL", ColumnPurgeJobTest::longAndSymbolValues);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                dropColumnsWithReader(purgeJob, "c", "s");
                // six partitions of c, six of s and the symbol map of s
                assertQuery("SELECT count(), count(partition_timestamp_raw) FROM '" + purgeJob.getLogTableName() + "'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count\tcount1
                                13\t0
                                """);
            }
            replayPurgeLog("t_split");
            assertReplayOfRowsWithoutRawPartitionTimestamp();
        });
    }

    @Test
    public void testPurgeLogWithoutRawPartitionTimestampWhenColumnAddDistressesWriter() throws Exception {
        // The file of the new column fails to open, and so does the rollback of the metadata: the column add throws
        // CairoError. The job takes a new writer, which restores the metadata of the log from _meta.prev.
        final AtomicBoolean isColumnFileFailureArmed = new AtomicBoolean(true);
        final AtomicBoolean isMetaFailureArmed = new AtomicBoolean();
        final AtomicInteger failureCount = new AtomicInteger();
        setSplitProperties();
        assertMemoryLeak(() -> assertPurgeLogWithoutRawPartitionTimestamp(new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isRawColumnDataFile(name) && isColumnFileFailureArmed.compareAndSet(true, false)) {
                    failureCount.incrementAndGet();
                    isMetaFailureArmed.set(true);
                    return -1;
                }
                return super.openRW(name, opts);
            }

            @Override
            public boolean removeQuiet(LPSZ name) {
                if (isMetaFailureArmed.get() && TableUtils.META_FILE_NAME.equals(purgeLogFileName(name))) {
                    failureCount.incrementAndGet();
                    return false;
                }
                return super.removeQuiet(name);
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (TableUtils.META_FILE_NAME.equals(purgeLogFileName(from)) && isMetaFailureArmed.compareAndSet(true, false)) {
                    failureCount.incrementAndGet();
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        }, () -> Assert.assertEquals(3, failureCount.get())));
    }

    @Test
    public void testPurgeLogWithoutRawPartitionTimestampWhenColumnFileFailsToOpen() throws Exception {
        // The file of the new column fails to open: TableWriter rolls the metadata of the log back and asks for a
        // new writer, which the job takes.
        final AtomicBoolean isFailureArmed = new AtomicBoolean(true);
        final AtomicInteger failureCount = new AtomicInteger();
        setSplitProperties();
        assertMemoryLeak(() -> assertPurgeLogWithoutRawPartitionTimestamp(new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isRawColumnDataFile(name) && isFailureArmed.compareAndSet(true, false)) {
                    failureCount.incrementAndGet();
                    return -1;
                }
                return super.openRW(name, opts);
            }
        }, () -> Assert.assertEquals(1, failureCount.get())));
    }

    @Test
    public void testPurgeLogWithoutRawPartitionTimestampWhenMetadataSwapFails() throws Exception {
        // The column add fails before it changes the log: the writer stays usable.
        final AtomicBoolean isFailureArmed = new AtomicBoolean(true);
        final AtomicInteger failureCount = new AtomicInteger();
        setSplitProperties();
        assertMemoryLeak(() -> assertPurgeLogWithoutRawPartitionTimestamp(new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                // every swap file the writer tries: _meta.swp, _meta.swp.1 and on
                final String fileName = purgeLogFileName(name);
                if (isFailureArmed.get() && fileName != null && fileName.startsWith(TableUtils.META_SWAP_FILE_NAME)) {
                    failureCount.incrementAndGet();
                    return -1;
                }
                return super.openRW(name, opts);
            }
        }, () -> {
            Assert.assertTrue(failureCount.get() > 0);
            isFailureArmed.set(false);
        }));
    }

    @Test
    public void testReplayedLegacyPurgeHeldBySavePointWaitsForSnapshotTxn() throws Exception {
        // Under ADAPTIVE a save point holds the replayed purge, and the job writes it back to the log for the running
        // job, with the partitions the replay located and the snapshot txn it waits for. The first partition's own
        // version died after the update that logged the row, at the second update: the save point between the two
        // updates still names it, so the purge has to wait for it.
        Assume.assumeTrue("a TIMESTAMP partition starts on a whole microsecond", timestampType == TestTimestampType.NANO);
        setSplitProperties();
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        assertMemoryLeak(() -> {
            createSameMicrosecondTable("WAL");
            final String firstDir = partitionDir("t_same_us", 1);
            final String secondDir = partitionDir("t_same_us", 2);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                updateSecondPartitionWithReader(purgeJob, "c2 = 0");
                Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
            }
            final long firstUpdateTxn;
            try (TableReader reader = getReader("t_same_us")) {
                firstUpdateTxn = reader.getTxn();
            }
            // the save point names the txn of the first update, at which the first partition reads c2.d
            releaseSavePointHolds("t_same_us");
            update("UPDATE t_same_us SET c2 = 1 WHERE ts >= '2022-02-27T01:00:00.000000000Z' AND ts < '2022-02-27T01:00:00.000000201Z'");
            drainWalQueue();
            final long snapshotTxn;
            final long firstTimestamp;
            final long secondTimestamp;
            try (TableReader reader = getReader("t_same_us")) {
                snapshotTxn = reader.getTxn();
                firstTimestamp = reader.getTxFile().getPartitionTimestampByIndex(1);
                secondTimestamp = reader.getTxFile().getPartitionTimestampByIndex(2);
            }
            Assert.assertTrue(snapshotTxn > firstUpdateTxn);
            final long firstTxn = getColumnNameTxn("t_same_us", "c2", 1);
            final long secondTxn = getColumnNameTxn("t_same_us", "c2", 2);
            Assert.assertEquals("c2.d c2.d." + firstTxn, String.join(" ", filesWithPrefix(listFiles(firstDir), "c2.")));
            Assert.assertEquals("c2.d c2.d." + secondTxn, String.join(" ", filesWithPrefix(listFiles(secondDir), "c2.")));
            eraseRawPartitionTimestamps();
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                // c2.d is dead in both partitions, and the save point holds both
                Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
                Assert.assertEquals("c2.d c2.d." + firstTxn, String.join(" ", filesWithPrefix(listFiles(firstDir), "c2.")));
                Assert.assertEquals("c2.d c2.d." + secondTxn, String.join(" ", filesWithPrefix(listFiles(secondDir), "c2.")));
                assertQuery("SELECT column_version, partition_timestamp_raw, updated_txn, completed FROM '" + purgeJob.getLogTableName() + "'")
                        .noLeakCheck()
                        .expectSize()
                        .returns("column_version\tpartition_timestamp_raw\tupdated_txn\tcompleted\n"
                                + "-1\t" + firstTimestamp + "\t" + snapshotTxn + "\t\n"
                                + "-1\t" + secondTimestamp + "\t" + snapshotTxn + "\t\n");
                releaseSavePointHolds("t_same_us");
                runPurgeJob(purgeJob);
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
            Assert.assertEquals("c2.d." + firstTxn, String.join(" ", filesWithPrefix(listFiles(firstDir), "c2.")));
            Assert.assertEquals("c2.d." + secondTxn, String.join(" ", filesWithPrefix(listFiles(secondDir), "c2.")));
            // c2 is x before 01:00 and 1 in the 22 rows of the first partition at 01:00
            assertQuery("SELECT count(), sum(c2) FROM t_same_us WHERE ts < '2022-02-27T01:00:00.000000201Z'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            122\t5072
                            """);
        });
    }

    @Test
    public void testReplayedLegacyPurgeKeepsFilesOfDayPartitionInSameMicrosecond() throws Exception {
        // The layout the server default split size gives a table whose day partition holds more rows in its first
        // microsecond than that size, at a split size of 1 byte: an O3 row splits the day after those rows, and the
        // split partition starts in the microsecond of the day partition. One commit rewrites both.
        Assume.assumeTrue("a TIMESTAMP partition starts on a whole microsecond", timestampType == TestTimestampType.NANO);
        setSplitProperties();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_day_us (ts TIMESTAMP_NS, v LONG, c2 LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_day_us VALUES ('2022-02-27T00:00:00.000000000Z'::TIMESTAMP_NS, 1, 1)");
            execute("INSERT INTO t_day_us SELECT '2022-02-27T00:00:00.000000500Z'::TIMESTAMP_NS, x + 1, x + 1 FROM long_sequence(10)");
            execute("INSERT INTO t_day_us VALUES ('2022-02-27T02:00:00.000000000Z'::TIMESTAMP_NS, 100, 100)");
            // splits the day after its rows at 00:00:00.000000500, so the new partition starts at 00:00:00.000000501
            execute("INSERT INTO t_day_us VALUES ('2022-02-27T01:00:00.000000000Z'::TIMESTAMP_NS, 200, 200)");
            // rows after 02:00 in the new partition, so that the next commit rewrites it rather than splits it
            execute("INSERT INTO t_day_us SELECT ('2022-02-27T02:00:00.000000000Z'::TIMESTAMP_NS + x * 1_000_000_000L)::TIMESTAMP_NS, 100 + x, 100 + x FROM long_sequence(10)");
            // one commit rewrites both, which then share their name txn
            execute("INSERT INTO t_day_us VALUES ('2022-02-27T00:00:00.000000001Z'::TIMESTAMP_NS, 300, 300), ('2022-02-27T01:30:00.000000000Z'::TIMESTAMP_NS, 301, 301)");
            try (TableReader reader = getReader("t_day_us")) {
                final TimestampDriver driver = timestampType.getDriver();
                Assert.assertEquals(2, reader.getPartitionCount());
                Assert.assertEquals(driver.parseFloorLiteral("2022-02-27"), reader.getTxFile().getPartitionTimestampByIndex(0));
                Assert.assertEquals(driver.parseFloorLiteral("2022-02-27") + 501, reader.getTxFile().getPartitionTimestampByIndex(1));
                Assert.assertEquals(reader.getTxFile().getPartitionNameTxn(0), reader.getTxFile().getPartitionNameTxn(1));
            }
            final String dayDir = partitionDir("t_day_us", 0);
            final String splitDir = partitionDir("t_day_us", 1);
            final String dayFiles = listFiles(dayDir);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                try (TableReader ignore = getReader("t_day_us")) {
                    update("UPDATE t_day_us SET c2 = 0 WHERE ts >= '2022-02-27T00:00:00.000000501Z'");
                    runPurgeJob(purgeJob);
                }
                Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
            }
            eraseRawPartitionTimestamps();
            replayPurgeLog("t_day_us");
            Assert.assertEquals(dayFiles, listFiles(dayDir));
            Assert.assertEquals("c2.d." + getColumnNameTxn("t_day_us", "c2", 1), String.join(" ", filesWithPrefix(listFiles(splitDir), "c2.")));
            // c2 is x in the day partition: 1, 2..11 and 300
            assertQuery("SELECT count(), sum(c2) FROM t_day_us WHERE ts < '2022-02-27T00:00:00.000000501Z'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            12\t366
                            """);
            assertQuery("SELECT count(), sum(c2) FROM t_day_us WHERE ts >= '2022-02-27T00:00:00.000000501Z'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            13\t0
                            """);
        });
    }

    @Test
    public void testReplayedLegacyPurgeKeepsFilesOfPartitionInSameMicrosecond() throws Exception {
        // A version before partition_timestamp_raw logged the partition timestamp in microseconds only. A replay that
        // took the logged microsecond for the timestamp of the partition built the directory of the first partition of
        // the microsecond, which has the same name txn, and removed the column versions that partition still reads.
        // The replay locates the partitions of such a row in a snapshot of the table, and purges a version only where
        // it is not live: here the second partition, the one the row was written for. s has a POSTING index.
        Assume.assumeTrue("a TIMESTAMP partition starts on a whole microsecond", timestampType == TestTimestampType.NANO);
        setSplitProperties();
        assertMemoryLeak(() -> {
            for (String walMode : new String[]{"BYPASS WAL", "WAL"}) {
                createSameMicrosecondTable(walMode);
                final String firstDir = partitionDir("t_same_us", 1);
                final String secondDir = partitionDir("t_same_us", 2);
                final String firstFiles = listFiles(firstDir);
                try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                    updateSecondPartitionWithReader(purgeJob, "c2 = 0, s = 'b'");
                    Assert.assertEquals(walMode, 2, purgeJob.getOutstandingPurgeTasks());
                }
                eraseRawPartitionTimestamps();
                replayPurgeLog("t_same_us");
                Assert.assertEquals(walMode, firstFiles, listFiles(firstDir));
                final String secondFiles = listFiles(secondDir);
                Assert.assertEquals(walMode, "c2.d." + getColumnNameTxn("t_same_us", "c2", 2), String.join(" ", filesWithPrefix(secondFiles, "c2.")));
                Assert.assertEquals(walMode, "[]", filesOfInstance(secondFiles, "s", TableUtils.COLUMN_NAME_TXN_NONE).toString());
                Assert.assertFalse(walMode, filesOfInstance(secondFiles, "s", getColumnNameTxn("t_same_us", "s", 2)).isEmpty());
                assertRowsOfSameMicrosecondTable();
                execute("DROP TABLE t_same_us");
            }
        });
    }

    @Test
    public void testReplayedLegacyPurgeKeepsFilesOfPartitionRewrittenAfterUpdate() throws Exception {
        // An O3 commit rewrote the second partition after the update, under another name txn, into a directory without
        // the replaced versions. The first partition, which kept the name txn of the log row, reads its versions: the
        // replay keeps its files.
        Assume.assumeTrue("a TIMESTAMP partition starts on a whole microsecond", timestampType == TestTimestampType.NANO);
        setSplitProperties();
        assertMemoryLeak(() -> {
            createSameMicrosecondTable("BYPASS WAL");
            final String firstDir = partitionDir("t_same_us", 1);
            final String firstFiles = listFiles(firstDir);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                updateSecondPartitionWithReader(purgeJob, "c2 = 0, s = 'b'");
                Assert.assertEquals(2, purgeJob.getOutstandingPurgeTasks());
            }
            execute("INSERT INTO t_same_us VALUES ('2022-02-27T01:00:00.000000400Z'::TIMESTAMP_NS, 700, 0, 'b')");
            Assert.assertNotEquals(getPartitionNameTxn("t_same_us", 1), getPartitionNameTxn("t_same_us", 2));
            eraseRawPartitionTimestamps();
            replayPurgeLog("t_same_us");
            Assert.assertEquals(firstFiles, listFiles(firstDir));
            assertQuery("SELECT s, count(), sum(c2) FROM t_same_us ORDER BY s")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tcount\tsum
                            a\t122\t10260
                            b\t4\t0
                            """);
        });
    }

    @Test
    public void testReplayedLegacyPurgeKeepsFilesOfPartitionSquashedIntoSibling() throws Exception {
        // The squash folded the second partition into the first, in place, after the update: the first keeps its name
        // txn and is the only partition of the microsecond the log row names, and its column files hold the rows of
        // both. The replay keeps them, as the versions are live.
        Assume.assumeTrue("a TIMESTAMP partition starts on a whole microsecond", timestampType == TestTimestampType.NANO);
        setSplitProperties();
        // a save point would make the squash copy the partition under a new name txn
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            createSameMicrosecondTable("WAL");
            final String firstDir = partitionDir("t_same_us", 1);
            final long firstNameTxn = getPartitionNameTxn("t_same_us", 1);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                updateSecondPartitionWithReader(purgeJob, "c2 = 0, s = 'b'");
                Assert.assertEquals(2, purgeJob.getOutstandingPurgeTasks());
            }
            // the next commit squashes the smallest pair of partitions of the day, as the day may have one split
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 2);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
            engine.releaseInactive();
            execute("INSERT INTO t_same_us VALUES ('2022-02-28T00:00:00.000000000Z'::TIMESTAMP_NS, 900, 900, 'a')");
            drainWalQueue();
            try (TableReader reader = getReader("t_same_us")) {
                final TimestampDriver driver = timestampType.getDriver();
                Assert.assertEquals(3, reader.getPartitionCount());
                Assert.assertEquals(driver.parseFloorLiteral("2022-02-27T01:00:00.000000Z"), reader.getTxFile().getPartitionTimestampByIndex(1));
                Assert.assertEquals(firstNameTxn, reader.getTxFile().getPartitionNameTxn(1));
                Assert.assertEquals(25, reader.getTxFile().getPartitionSize(1));
            }
            final String firstFiles = listFiles(firstDir);
            eraseRawPartitionTimestamps();
            replayPurgeLog("t_same_us");
            Assert.assertEquals(firstFiles, listFiles(firstDir));
            assertQuery("SELECT s, count(), sum(c2) FROM t_same_us ORDER BY s")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tcount\tsum
                            a\t123\t11160
                            b\t3\t0
                            """);
        });
    }

    @Test
    public void testReplayedLegacyPurgeKeepsVersionAtOrPastSnapshotTxn() throws Exception {
        // A column version at or past the txn of the snapshot may belong to a transaction in flight, as VACUUM TABLE
        // assumes, so the replay leaves it, in every partition of the microsecond the row names.
        Assume.assumeTrue("a TIMESTAMP partition starts on a whole microsecond", timestampType == TestTimestampType.NANO);
        setSplitProperties();
        assertMemoryLeak(() -> {
            createSameMicrosecondTable("BYPASS WAL");
            createPurgeJob().close();
            final TableToken tableToken = engine.verifyTableName("t_same_us");
            final long tableTxn;
            final long partitionTimestamp;
            try (TableReader reader = getReader("t_same_us")) {
                tableTxn = reader.getTxn();
                partitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(1);
            }
            final long futureTxn = tableTxn + 100;
            final String firstDir = partitionDir("t_same_us", 1);
            final String secondDir = partitionDir("t_same_us", 2);
            Assert.assertTrue(new File(firstDir, "c2.d." + futureTxn).createNewFile());
            Assert.assertTrue(new File(secondDir, "c2.d." + futureTxn).createNewFile());
            execute("INSERT INTO '" + purgeLogTableName() + "' (ts, table_name, column_name, table_id, truncate_version, columnType, table_partition_by, updated_txn, column_version, partition_timestamp, partition_name_txn) VALUES ("
                    + "0, '" + tableToken.getDirName() + "', 'c2', " + tableToken.getTableId() + ", 0, " + ColumnType.LONG + ", " + PartitionBy.DAY + ", " + tableTxn + ", "
                    + futureTxn + ", " + timestampType.getDriver().toMicros(partitionTimestamp) + ", " + getPartitionNameTxn("t_same_us", 1) + ")");
            replayPurgeLog("t_same_us");
            Assert.assertEquals("c2.d c2.d." + futureTxn, String.join(" ", filesWithPrefix(listFiles(firstDir), "c2.")));
            Assert.assertEquals("c2.d c2.d." + futureTxn, String.join(" ", filesWithPrefix(listFiles(secondDir), "c2.")));
        });
    }

    @Test
    public void testReplayedLegacyPurgeLeavesGeometryGeneration() throws Exception {
        // The purge of a retired _geometry generation tells a newer incarnation of the file by the txn of the change
        // that logged it, which the replay would have to raise, and a sibling's live generation can have the same
        // number. The replay leaves the file of a generation that the log names in microseconds only.
        Assume.assumeTrue("a TIMESTAMP partition starts on a whole microsecond", timestampType == TestTimestampType.NANO);
        setSplitProperties();
        assertMemoryLeak(() -> {
            createSameMicrosecondTable("BYPASS WAL");
            createPurgeJob().close();
            final TableToken tableToken = engine.verifyTableName("t_same_us");
            final long tableTxn;
            final long partitionTimestamp;
            try (TableReader reader = getReader("t_same_us")) {
                tableTxn = reader.getTxn();
                partitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(1);
            }
            // a file that the purge of a generation would remove: its record at offset 0 does not verify
            final File geometryFile = new File(partitionDir("t_same_us", 1), "_geometry.0");
            Assert.assertTrue(geometryFile.createNewFile());
            execute("INSERT INTO '" + purgeLogTableName() + "' (ts, table_name, column_name, table_id, truncate_version, columnType, table_partition_by, updated_txn, column_version, partition_timestamp, partition_name_txn) VALUES ("
                    + "0, '" + tableToken.getDirName() + "', '" + TableUtils.PARTITION_GEOMETRY_FILE_NAME + "', " + tableToken.getTableId() + ", 0, " + ColumnType.NULL + ", " + PartitionBy.DAY + ", " + tableTxn + ", "
                    + "0, " + timestampType.getDriver().toMicros(partitionTimestamp) + ", " + getPartitionNameTxn("t_same_us", 1) + ")");
            replayPurgeLog("t_same_us");
            Assert.assertTrue(geometryFile.exists());
        });
    }

    @Test
    public void testReplayedLegacyPurgeOfExactEntriesWaitsForUpdateTxnOnly() throws Exception {
        // The rows of a TIMESTAMP table name their partitions exactly in microseconds, and the rows of the table root
        // name it with the root marker: the replay purges them as logged, so they wait only for the readers and save
        // points before the change that logged them. Here a save point names the txn of the change, when the purged
        // versions were already dead, and a later commit moves the table on.
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_exact (ts " + timestampType.getTypeName() + ", x LONG, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t_exact VALUES ('1970-01-01T02:00:00.000000Z', 1, 'a'), ('1970-01-02T02:00:00.000000Z', 2, 'b')");
            drainWalQueue();
            // a TIMESTAMP table updates x in every partition, a TIMESTAMP_NS table rebuilds the symbol map of s
            final boolean isMicros = timestampType == TestTimestampType.MICRO;
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                try (TableReader ignore = getReader("t_exact")) {
                    execute(isMicros ? "UPDATE t_exact SET x = 0" : "ALTER TABLE t_exact ALTER COLUMN s SYMBOL CAPACITY 1024");
                    drainWalQueue();
                    runPurgeJob(purgeJob);
                }
                Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
            }
            releaseSavePointHolds("t_exact");
            execute("INSERT INTO t_exact VALUES ('1970-01-03T02:00:00.000000Z', 3, 'c')");
            drainWalQueue();
            final String column = isMicros ? "x" : "s";
            final String before = filesOfColumns("t_exact", column);
            Assert.assertTrue(before, isMicros ? before.contains("/x.d ") : before.contains(" s.c "));
            eraseRawPartitionTimestamps();
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                assertPurgeLogRows(purgeJob, 0, 0);
            }
            final String after = filesOfColumns("t_exact", column);
            // the versions of the column that the change replaced, txn NONE, are gone
            Assert.assertFalse(after, isMicros ? after.contains("/x.d ") : after.contains(" s.c ") || after.contains(" s.o "));
            assertQuery("SELECT x, s FROM t_exact")
                    .noLeakCheck()
                    .expectSize()
                    .returns(isMicros ? """
                            x\ts
                            0\ta
                            0\tb
                            3\tc
                            """ : """
                            x\ts
                            1\ta
                            2\tb
                            3\tc
                            """);
        });
    }

    @Test
    public void testReplayedLegacyPurgeOfNonPartitionedTable() throws Exception {
        // A non-partitioned table has one directory, whatever the logged timestamp: the replay takes the row as it is,
        // without a snapshot of the table, so a table it cannot read still loses the dropped column.
        final AtomicBoolean isFailureArmed = new AtomicBoolean();
        final AtomicInteger failureCount = new AtomicInteger();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_none (ts " + timestampType.getTypeName() + ", x LONG, c LONG) TIMESTAMP(ts) PARTITION BY NONE BYPASS WAL");
            execute("INSERT INTO t_none VALUES ('1970-01-01T02:00:00.000000Z', 1, 10), ('1970-01-02T02:00:00.000000Z', 2, 20)");
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                try (TableReader ignore = getReader("t_none")) {
                    execute("ALTER TABLE t_none DROP COLUMN c");
                    runPurgeJob(purgeJob);
                }
                Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
            }
            final String tableDir = engine.verifyTableName("t_none").getDirName();
            final String defaultDir = new File(new File(configuration.getDbRoot().toString(), tableDir), TableUtils.DEFAULT_PARTITION_NAME).getPath();
            Assert.assertEquals("c.d", String.join(" ", filesWithPrefix(listFiles(defaultDir), "c.")));
            eraseRawPartitionTimestamps();
            engine.releaseInactive();
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    if (isFailureArmed.get() && Utf8s.endsWithAscii(name, tableDir + Files.SEPARATOR + TableUtils.COLUMN_VERSION_FILE_NAME)) {
                        failureCount.incrementAndGet();
                        return -1;
                    }
                    return super.openRO(name);
                }
            };
            isFailureArmed.set(true);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                assertPurgeLogRows(purgeJob, 0, 0);
            }
            isFailureArmed.set(false);
            Assert.assertEquals(0, failureCount.get());
            Assert.assertEquals("", String.join(" ", filesWithPrefix(listFiles(defaultDir), "c.")));
            assertQuery("SELECT * FROM t_none")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns(replaceTimestampSuffix("""
                            ts\tx
                            1970-01-01T02:00:00.000000Z\t1
                            1970-01-02T02:00:00.000000Z\t2
                            """, timestampType.getTypeName()));
        });
    }

    @Test
    public void testReplayedLegacyPurgeOfTableThatCannotBeRead() throws Exception {
        // The replay cannot open a reader of the table, so it cannot tell which partitions the rows that the log keeps
        // in microseconds name, nor whether their versions are live there. It leaves their files to VACUUM TABLE, and
        // purges the symbol map, which the log names with the table root marker.
        Assume.assumeTrue("a TIMESTAMP partition starts on a whole microsecond", timestampType == TestTimestampType.NANO);
        final AtomicBoolean isFailureArmed = new AtomicBoolean();
        final AtomicInteger failureCount = new AtomicInteger();
        setSplitProperties();
        assertMemoryLeak(() -> {
            createSplitTable("BYPASS WAL", "c LONG, s SYMBOL", ColumnPurgeJobTest::longAndSymbolValues);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                dropColumnsWithReader(purgeJob, "c", "s");
            }
            final String tableDir = engine.verifyTableName("t_split").getDirName();
            final String files = filesOfColumns("t_split", "c", "s");
            final StringSink partitionFiles = new StringSink();
            for (String file : files.split(" ")) {
                if (file.contains("/")) {
                    partitionFiles.put(partitionFiles.length() > 0 ? " " : "").put(file);
                }
            }
            // the symbol map of s in the table directory, and c.d and s.d in each of the six partitions
            Assert.assertEquals(files, 16, files.split(" ").length);
            Assert.assertEquals(files, 12, partitionFiles.toString().split(" ").length);
            eraseRawPartitionTimestamps();
            engine.releaseInactive();
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    if (isFailureArmed.get() && Utf8s.endsWithAscii(name, tableDir + Files.SEPARATOR + TableUtils.COLUMN_VERSION_FILE_NAME)) {
                        failureCount.incrementAndGet();
                        return -1;
                    }
                    return super.openRO(name);
                }
            };
            isFailureArmed.set(true);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                assertPurgeLogRows(purgeJob, 0, 0);
            }
            isFailureArmed.set(false);
            Assert.assertTrue(failureCount.get() > 0);
            // the symbol map of s went, and the files of c and s in every partition stayed
            Assert.assertEquals(partitionFiles.toString(), filesOfColumns("t_split", "c", "s"));
            execute("VACUUM TABLE t_split");
            Assert.assertEquals("", filesOfColumns("t_split", "c", "s"));
            assertRowsOfSplitTable();
        });
    }

    @Test
    public void testReplayedLegacyPurgeOfTableWithChangedTruncateVersion() throws Exception {
        // DROP PARTITION changed the truncate version after the update, so the purge refuses the entries the replay
        // located, and every file stays.
        Assume.assumeTrue("a TIMESTAMP partition starts on a whole microsecond", timestampType == TestTimestampType.NANO);
        setSplitProperties();
        listenToTruncateRefusals();
        assertMemoryLeak(() -> {
            createSameMicrosecondTable("BYPASS WAL");
            execute("INSERT INTO t_same_us VALUES ('2022-02-28T00:00:00.000000000Z'::TIMESTAMP_NS, 900, 900, 'a')");
            final String firstDir = partitionDir("t_same_us", 1);
            final String secondDir = partitionDir("t_same_us", 2);
            final String firstFiles = listFiles(firstDir);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                updateSecondPartitionWithReader(purgeJob, "c2 = 0");
                Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
            }
            execute("ALTER TABLE t_same_us DROP PARTITION LIST '2022-02-28'");
            eraseRawPartitionTimestamps();
            replayPurgeLog("t_same_us");
            Assert.assertEquals(
                    "[" + truncateRefusalKey("c2", partitionTimestampOf("t_same_us", 2), getPartitionNameTxn("t_same_us", 2), TableUtils.COLUMN_NAME_TXN_NONE) + "]",
                    truncateRefusedEntries.toString()
            );
            Assert.assertEquals(firstFiles, listFiles(firstDir));
            Assert.assertEquals("c2.d c2.d." + getColumnNameTxn("t_same_us", "c2", 2), String.join(" ", filesWithPrefix(listFiles(secondDir), "c2.")));
        });
    }

    @Test
    public void testReplayedLegacyPurgeRemovesFilesOfBothPartitionsInSameMicrosecond() throws Exception {
        // One update rewrote c2 in both partitions of the microsecond, so the log holds the same row for each. The
        // replay locates both partitions for each row, where the version is dead, and removes the files of both.
        Assume.assumeTrue("a TIMESTAMP partition starts on a whole microsecond", timestampType == TestTimestampType.NANO);
        setSplitProperties();
        assertMemoryLeak(() -> {
            createSameMicrosecondTable("BYPASS WAL");
            final String firstDir = partitionDir("t_same_us", 1);
            final String secondDir = partitionDir("t_same_us", 2);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                try (TableReader ignore = getReader("t_same_us")) {
                    update("UPDATE t_same_us SET c2 = 0 WHERE ts >= '2022-02-27T01:00:00.000000000Z' AND ts < '2022-02-27T02:00:00.000000000Z'");
                    runPurgeJob(purgeJob);
                }
                Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
            }
            eraseRawPartitionTimestamps();
            replayPurgeLog("t_same_us");
            Assert.assertEquals("c2.d." + getColumnNameTxn("t_same_us", "c2", 1), String.join(" ", filesWithPrefix(listFiles(firstDir), "c2.")));
            Assert.assertEquals("c2.d." + getColumnNameTxn("t_same_us", "c2", 2), String.join(" ", filesWithPrefix(listFiles(secondDir), "c2.")));
            // c2 is x before 01:00, and 300 in the row at 02:00
            assertQuery("SELECT count(), sum(c2) FROM t_same_us")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            125\t5350
                            """);
        });
    }

    @Test
    public void testReplayedLegacyPurgeRemovesFilesOfSplitPartition() throws Exception {
        // As testReplayedPurgeRemovesFilesOfSplitPartition(), with rows that keep the partition timestamp in
        // microseconds only: the replay locates the split partition in a snapshot of the table.
        assertReplayedPurgeRemovesFilesOfSplitPartition(true);
    }

    @Test
    public void testReplayedPurgeKeepsFilesOfPartitionInSameMicrosecond() throws Exception {
        // Two split partitions of a TIMESTAMP_NS table can start in the same microsecond and carry the same name
        // txn. A replay that read the timestamp of the second in microseconds built the directory of the first, and
        // removed the column version that the first still reads.
        Assume.assumeTrue("a TIMESTAMP partition starts on a whole microsecond", timestampType == TestTimestampType.NANO);
        setSplitProperties();
        assertMemoryLeak(() -> {
            createSameMicrosecondTable("BYPASS WAL");
            final String firstDir = partitionDir("t_same_us", 1);
            final String secondDir = partitionDir("t_same_us", 2);
            Assert.assertEquals("c2.d", String.join(" ", filesWithPrefix(listFiles(firstDir), "c2.")));
            Assert.assertEquals("c2.d", String.join(" ", filesWithPrefix(listFiles(secondDir), "c2.")));
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                // rewrites c2 in the second partition only
                updateSecondPartitionWithReader(purgeJob, "c2 = 0");
                Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
            }
            final long updateTxn = getColumnNameTxn("t_same_us", "c2", 2);
            Assert.assertTrue(updateTxn > 0);
            Assert.assertEquals(TableUtils.COLUMN_NAME_TXN_NONE, getColumnNameTxn("t_same_us", "c2", 1));
            replayPurgeLog("t_same_us");
            Assert.assertEquals("c2.d", String.join(" ", filesWithPrefix(listFiles(firstDir), "c2.")));
            Assert.assertEquals("c2.d." + updateTxn, String.join(" ", filesWithPrefix(listFiles(secondDir), "c2.")));
            // c2 is x in the rows before the second partition: 1..100 in the day partition, 201..220, 400 and 600
            assertQuery("SELECT count(), sum(c2) FROM t_same_us WHERE ts < '2022-02-27T01:00:00.000000201Z'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            122\t10260
                            """);
            assertQuery("SELECT count(), sum(c2) FROM t_same_us WHERE ts >= '2022-02-27T01:00:00.000000201Z'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            3\t0
                            """);
        });
    }

    @Test
    public void testReplayedPurgeOfRowsWithoutRawPartitionTimestamp() throws Exception {
        // Rows that a version before partition_timestamp_raw wrote hold the partition timestamp in microseconds,
        // see assertReplayOfRowsWithoutRawPartitionTimestamp().
        setSplitProperties();
        assertMemoryLeak(() -> {
            createSplitTable("BYPASS WAL", "c LONG, s SYMBOL", ColumnPurgeJobTest::longAndSymbolValues);
            try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                dropColumnsWithReader(purgeJob, "c", "s");
            }
            eraseRawPartitionTimestamps();
            replayPurgeLog("t_split");
            assertReplayOfRowsWithoutRawPartitionTimestamp();
        });
    }

    @Test
    public void testReplayedPurgeRemovesFilesOfSplitPartition() throws Exception {
        // An O3 split names the new partition after the last row it leaves behind plus one tick, which in a
        // TIMESTAMP_NS table is a timestamp with a sub-microsecond part. The purge log keeps the timestamp whole in
        // partition_timestamp_raw, so the replayed purge finds the split partition. A SYMBOL also has its symbol map
        // in the table directory, which the log names with the root partition marker.
        assertReplayedPurgeRemovesFilesOfSplitPartition(false);
    }

    /**
     * Drops a column of each kind, LONG, VARCHAR, SYMBOL and SYMBOL with each index type, from t_split on WAL and
     * non-WAL tables while a reader holds it, and asserts that the replay of the purge log removes every file of the
     * column. With {@code isLegacy}, the rows of the log lack partition_timestamp_raw, as a version before the column
     * wrote them.
     */
    private void assertReplayedPurgeRemovesFilesOfSplitPartition(boolean isLegacy) throws Exception {
        setSplitProperties();
        assertMemoryLeak(() -> {
            final String[] columns = {
                    "c LONG",
                    "c VARCHAR",
                    "c SYMBOL",
                    "c SYMBOL INDEX TYPE BITMAP",
                    "c SYMBOL INDEX TYPE POSTING INCLUDE (x)"
            };
            for (String walMode : new String[]{"BYPASS WAL", "WAL"}) {
                for (String column : columns) {
                    final String context = walMode + ", " + column;
                    final boolean isSymbol = column.contains("SYMBOL");
                    final boolean isVarchar = column.contains("VARCHAR");
                    createSplitTable(
                            walMode,
                            column,
                            x -> isSymbol ? "'s" + (x % 3) + "'" : isVarchar ? "'v" + x + "'" : String.valueOf(x * 10)
                    );
                    final String splitDir = new File(partitionDir("t_split", SPLIT_PARTITION_INDEX)).getName();
                    final String files = " " + filesOfColumns("t_split", "c") + " ";
                    Assert.assertTrue(context + ", " + files, files.contains(" " + splitDir + "/c.d "));
                    Assert.assertEquals(context + ", " + files, isSymbol, files.contains(" c.c "));
                    try (ColumnPurgeJob purgeJob = createPurgeJob()) {
                        dropColumnsWithReader(purgeJob, "c");
                        assertRawPartitionTimestamps(purgeJob, "c", isSymbol);
                    }
                    if (isLegacy) {
                        eraseRawPartitionTimestamps();
                    }
                    replayPurgeLog("t_split");
                    Assert.assertEquals(context, "", filesOfColumns("t_split", "c"));
                    assertRowsOfSplitTable();
                    execute("DROP TABLE t_split");
                }
            }
        });
    }

    /**
     * {@link #assertReplayedPurgeOfChangedIndex(String, String, String[], boolean)}, then drops t_replay.
     */
    private void assertReplayedPurgeOfChangedIndex(String partitionByAndWal, String oldIndexClause, String[] changes) throws Exception {
        assertReplayedPurgeOfChangedIndex(partitionByAndWal, oldIndexClause, changes, true);
    }

    /**
     * Creates t_replay, indexes sym with {@code oldIndexClause}, and asserts the replayed purge of the old
     * instance of sym after {@code changes[1..]}, which leave {@code changes[0]} the live column, or no
     * live column when null. Every partition holds the data file and the index files of the old instance.
     * Drops t_replay when {@code isDropped}. Returns the files of the old instance in each partition
     * directory.
     */
    private String[] assertReplayedPurgeOfChangedIndex(
            String partitionByAndWal,
            String oldIndexClause,
            String[] changes,
            boolean isDropped
    ) throws Exception {
        final String context = partitionByAndWal + ", " + oldIndexClause + ", " + String.join("; ", changes);
        createReplayTable(partitionByAndWal, oldIndexClause);
        final String[] oldFiles;
        try {
            oldFiles = assertReplayedPurgeRemovesOldInstance(
                    changes[0],
                    replaceTimestampSuffix(ROWS_OF_A_IN_REPLAY_TABLE, timestampType.getTypeName()),
                    null,
                    null,
                    Arrays.copyOfRange(changes, 1, changes.length)
            );
        } catch (AssertionError e) {
            throw new AssertionError(context, e);
        }
        final boolean isPosting = oldIndexClause.startsWith("POSTING");
        final boolean isCovering = oldIndexClause.contains("INCLUDE");
        for (String files : oldFiles) {
            final String paddedFiles = " " + files + " ";
            final String[] expectedFiles = isPosting
                    ? (isCovering ? new String[]{" sym.d ", " sym.pk ", " sym.pv.", " sym.pci ", " sym.pc0.0."} : new String[]{" sym.d ", " sym.pk ", " sym.pv."})
                    : new String[]{" sym.d ", " sym.k ", " sym.v "};
            for (String file : expectedFiles) {
                Assert.assertTrue(context + ", " + file + " in " + files, paddedFiles.contains(file));
            }
        }
        if (isDropped) {
            execute("DROP TABLE t_replay");
        }
        return oldFiles;
    }

    /**
     * Runs {@code changes} on t_replay with a reader open, so the first purge job keeps the purge task of
     * the old instance of sym in the purge log, and replays the task in a new purge job, after
     * {@code beforeReplay} when not null. Asserts that the replay removes every file of the old instance,
     * named with its column name txn in each partition, but {@code leftoverFiles}, and keeps the other
     * files, that the new job leaves the purge log empty, and the rows of 'A' in {@code liveColumn}
     * through its index and through a filter that reads no index, or the rows 1 and 4 when no live
     * column is given. Returns the files of the old instance in each partition directory, space-separated.
     */
    private String[] assertReplayedPurgeRemovesOldInstance(
            String liveColumn,
            String expectedRows,
            Runnable beforeReplay,
            String[] leftoverFiles,
            String... changes
    ) throws Exception {
        final String[] dirs;
        try (TableReader reader = getReader("t_replay")) {
            dirs = new String[PartitionBy.isPartitioned(reader.getPartitionedBy()) ? reader.getPartitionCount() : 1];
        }
        final long[] oldTxns = new long[dirs.length];
        for (int i = 0; i < dirs.length; i++) {
            dirs[i] = dirs.length > 1 ? partitionDir("t_replay", i) : defaultDir("t_replay");
            oldTxns[i] = getColumnNameTxn("t_replay", "sym", i);
        }
        try (ColumnPurgeJob purgeJob = createPurgeJob()) {
            try (TableReader ignored = getReader("t_replay")) {
                for (String change : changes) {
                    execute(change);
                }
                drainWalQueue();
                runPurgeJob(purgeJob);
                Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
            }
        }
        engine.releaseInactive();
        final String[] listingsBefore = new String[dirs.length];
        final String[] oldFiles = new String[dirs.length];
        for (int i = 0; i < dirs.length; i++) {
            listingsBefore[i] = listFiles(dirs[i]);
            oldFiles[i] = String.join(" ", filesOfInstance(listingsBefore[i], "sym", oldTxns[i]));
        }
        if (beforeReplay != null) {
            beforeReplay.run();
        }
        // under ADAPTIVE the save points still name the old instance, and the replay keeps such a task
        releaseSavePointHolds("t_replay");
        try (ColumnPurgeJob purgeJob = createPurgeJob()) {
            Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            assertPurgeLogRows(purgeJob, 0, 0);
        }
        for (int i = 0; i < dirs.length; i++) {
            final TreeSet<String> expectedFiles = new TreeSet<>(Arrays.asList(listingsBefore[i].split(" ")));
            expectedFiles.removeAll(Arrays.asList(oldFiles[i].split(" ")));
            if (leftoverFiles != null && leftoverFiles[i] != null) {
                expectedFiles.add(leftoverFiles[i]);
            }
            Assert.assertEquals(dirs[i] + ", old instance " + oldFiles[i], String.join(" ", expectedFiles), listFiles(dirs[i]));
        }
        if (liveColumn != null) {
            final String query = "SELECT ts, x FROM t_replay WHERE " + liveColumn + " = 'A'";
            // a covering index of the live column reads the rows without random access, and knows their count
            final boolean isLiveIndexCovering;
            try (TableReader reader = getReader("t_replay")) {
                final int columnIndex = reader.getMetadata().getColumnIndex(liveColumn);
                final IntList coveredColumnIndices = reader.getMetadata().getCoveringColumnIndices(columnIndex);
                isLiveIndexCovering = IndexType.isPosting(reader.getMetadata().getColumnIndexType(columnIndex))
                        && coveredColumnIndices != null && coveredColumnIndices.size() > 0;
            }
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .supportsRandomAccess(!isLiveIndexCovering)
                    .expectSize(isLiveIndexCovering)
                    .returns(expectedRows);
            assertSqlCursors("SELECT ts, x FROM t_replay WHERE " + liveColumn + "::STRING = 'A'", query);
        } else {
            assertQuery("SELECT ts, x FROM t_replay WHERE x IN (1, 4)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedRows);
        }
        return oldFiles;
    }

    /**
     * Appends partition_timestamp_raw to the purge log rows of {@code expected}, which lists the other columns: the
     * partition_timestamp of each row in the unit of the table timestamp. The partitions of those rows start on
     * whole microseconds, which partition_timestamp holds exactly.
     */
    private String withRawPartitionTimestamps(String expected) throws NumericException {
        final StringSink sink = new StringSink();
        final String[] lines = expected.split("\n");
        sink.put(lines[0]).put("\tpartition_timestamp_raw\n");
        for (int i = 1; i < lines.length; i++) {
            final String partitionTimestamp = lines[i].split("\t", -1)[9];
            sink.put(lines[i]).put('\t').put(timestampType.getDriver().fromMicros(MicrosTimestampDriver.floor(partitionTimestamp))).put('\n');
        }
        return sink.toString();
    }

    private static void addFilesOfColumns(TreeSet<String> files, File dir, String prefix, String[] columns) {
        final String[] names = dir.list();
        Assert.assertNotNull(dir.toString(), names);
        for (String name : names) {
            for (String column : columns) {
                if (name.startsWith(column + '.')) {
                    files.add(prefix + name);
                }
            }
        }
    }

    private void assertPurgeLogColumns(String expected) throws Exception {
        assertQuery("SELECT \"column\", type FROM table_columns('" + purgeLogTableName() + "')")
                .noLeakCheck()
                .noRandomAccess()
                .returns(expected);
    }

    /**
     * Starts the purge job with {@code facade} on a log that a version before partition_timestamp_raw created, so
     * that the job fails to add the column, and runs {@code afterJobStart}. Asserts that the job logs the purges of
     * t_split as that version did, and that the next job adds the column and replays them, see
     * {@link #assertReplayOfRowsWithoutRawPartitionTimestamp()}.
     */
    private void assertPurgeLogWithoutRawPartitionTimestamp(TestFilesFacadeImpl facade, Runnable afterJobStart) throws Exception {
        createLegacyPurgeLog();
        // a completed row in the last partition of the log, where the column add creates the file of the column
        execute("INSERT INTO '" + purgeLogTableName() + "' (ts, table_name, column_name, completed) VALUES (0, 'gone~', 'c', 1)");
        createSplitTable("BYPASS WAL", "c LONG, s SYMBOL", ColumnPurgeJobTest::longAndSymbolValues);
        // the job opens a new writer of the log, which reaches the files through the facade
        engine.releaseInactive();
        ff = facade;
        try (ColumnPurgeJob purgeJob = createPurgeJob()) {
            afterJobStart.run();
            assertPurgeLogColumns(PURGE_LOG_COLUMNS.replace("partition_timestamp_raw\tLONG\n", ""));
            // the reader keeps both purges in the log
            try (TableReader ignore = getReader("t_split")) {
                execute("ALTER TABLE t_split DROP COLUMN c");
                runPurgeJob(purgeJob);
                // the purge of s goes to the next partition of the log: the column add of the next job creates the
                // file of the column there, and none in the partition of the purge of c
                setCurrentMicros(currentMicros + 31 * Micros.DAY_MICROS);
                execute("ALTER TABLE t_split DROP COLUMN s");
                runPurgeJob(purgeJob);
            }
            Assert.assertEquals(2, purgeJob.getOutstandingPurgeTasks());
            // six partitions of c, six of s and the symbol map of s, the completed row went with the startup truncate
            assertPurgeLogRows(purgeJob, 13, 0);
            assertQuery("SELECT name FROM table_partitions('" + purgeJob.getLogTableName() + "')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            name
                            1970-01
                            1970-02
                            """);
        }
        replayPurgeLog("t_split");
        assertPurgeLogColumns(PURGE_LOG_COLUMNS);
        assertReplayOfRowsWithoutRawPartitionTimestamp();
    }

    // partition_timestamp_raw of the logged purge of column of t_split: the timestamp of each partition as _txn has
    // it, and the root partition marker for the symbol map of a SYMBOL
    private void assertRawPartitionTimestamps(ColumnPurgeJob purgeJob, String column, boolean isSymbol) throws Exception {
        final StringSink expected = new StringSink();
        expected.put("partition_timestamp_raw\n");
        if (isSymbol) {
            expected.put(PurgingOperator.TABLE_ROOT_PARTITION).put('\n');
        }
        try (TableReader reader = getReader("t_split")) {
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                expected.put(reader.getTxFile().getPartitionTimestampByIndex(i)).put('\n');
            }
        }
        assertQuery("SELECT partition_timestamp_raw FROM '" + purgeJob.getLogTableName() + "' WHERE column_name = '" + column + "' ORDER BY partition_timestamp_raw")
                .noLeakCheck()
                .returns(expected);
    }

    /**
     * Asserts that the replay of rows without partition_timestamp_raw removed every file of c and s in t_split. Such
     * a row holds the partition timestamp in microseconds, in partition_timestamp. The replay restores every timestamp
     * of a TIMESTAMP table, and the root partition marker, which the conversion to microseconds moved in a
     * TIMESTAMP_NS table. It locates the partitions of a TIMESTAMP_NS table in a snapshot of the table: the split
     * partition starts one nanosecond after a whole microsecond.
     */
    private void assertReplayOfRowsWithoutRawPartitionTimestamp() throws Exception {
        Assert.assertEquals("", filesOfColumns("t_split", "c", "s"));
        assertRowsOfSplitTable();
    }

    private void assertRowsOfSplitTable() throws Exception {
        assertQuery("SELECT ts, x FROM t_split")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(replaceTimestampSuffix(ROWS_OF_SPLIT_TABLE, timestampType.getTypeName()));
    }

    private static void createLegacyPurgeLog() throws SqlException {
        execute("CREATE TABLE '" + purgeLogTableName() + "' (" + LEGACY_PURGE_LOG_COLUMNS + ") timestamp(ts) partition by MONTH BYPASS WAL");
    }

    /**
     * Creates t_split with ts, x and {@code columns}, which hold {@code values} of x in the row x: one row in each of
     * 1970-01-01, 1970-01-02, 1970-01-04 and 1970-01-05, and one each hour from 01:00 to 10:00 in 1970-01-03. Then a
     * row at 09:30 splits 1970-01-03 after its 09:00 row, see {@link #ROWS_OF_SPLIT_TABLE}. The new partition, the
     * fourth of six, starts one tick after that row: at 1970-01-03T09:00:00.000001 in a TIMESTAMP table and at
     * 1970-01-03T09:00:00.000000001 in a TIMESTAMP_NS one.
     */
    private void createSplitTable(String walMode, String columns, LongFunction<String> values) throws Exception {
        execute("CREATE TABLE t_split (ts " + timestampType.getTypeName() + ", x LONG, " + columns + ") TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        final String[] hours = {
                "1970-01-01T02", "1970-01-02T02", "1970-01-03T01", "1970-01-03T02", "1970-01-03T03", "1970-01-03T04", "1970-01-03T05",
                "1970-01-03T06", "1970-01-03T07", "1970-01-03T08", "1970-01-03T09", "1970-01-03T10", "1970-01-04T02", "1970-01-05T02"
        };
        final StringSink sql = new StringSink();
        sql.put("INSERT INTO t_split VALUES ");
        for (int i = 0; i < hours.length; i++) {
            sql.put(i > 0 ? ", " : "").put("('").put(hours[i]).put(":00:00.000000Z', ").put(i + 1).put(", ").put(values.apply(i + 1)).put(')');
        }
        execute(sql);
        drainWalQueue();
        execute("INSERT INTO t_split VALUES ('1970-01-03T09:30:00.000000Z', 15, " + values.apply(15) + ")");
        drainWalQueue();
        try (TableReader reader = getReader("t_split")) {
            Assert.assertEquals(6, reader.getPartitionCount());
            Assert.assertEquals(
                    timestampType.getDriver().parseFloorLiteral("1970-01-03T09:00:00.000000Z") + 1,
                    reader.getTxFile().getPartitionTimestampByIndex(SPLIT_PARTITION_INDEX)
            );
        }
    }

    /**
     * Creates t_same_us, a TIMESTAMP_NS table with a POSTING index on s, whose 2022-02-27 has three partitions: the
     * day, 01:00 exactly and 01:00:00.000000201. The last two start in the same microsecond and carry the same name
     * txn, as one commit rewrote both. c2 is x and s is 'a' in each row, see
     * {@link #assertRowsOfSameMicrosecondTable()}.
     */
    private void createSameMicrosecondTable(String walMode) throws Exception {
        execute("CREATE TABLE t_same_us (ts TIMESTAMP_NS, v LONG, c2 LONG, s SYMBOL INDEX TYPE POSTING) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        final String[] inserts = {
                // 100 rows before 01:00, the last at 00:59:59.999999999
                "INSERT INTO t_same_us SELECT ('2022-02-27T00:00:00.000000000Z'::TIMESTAMP_NS + (x - 1) * 1_000_000_000L)::TIMESTAMP_NS, x, x, 'a' FROM long_sequence(99)",
                "INSERT INTO t_same_us VALUES ('2022-02-27T00:59:59.999999999Z'::TIMESTAMP_NS, 100, 100, 'a')",
                // 20 rows in the microsecond after 01:00, and one at 02:00
                "INSERT INTO t_same_us SELECT ('2022-02-27T01:00:00.000000000Z'::TIMESTAMP_NS + x * 10)::TIMESTAMP_NS, 200 + x, 200 + x, 'a' FROM long_sequence(20)",
                "INSERT INTO t_same_us VALUES ('2022-02-27T02:00:00.000000000Z'::TIMESTAMP_NS, 300, 300, 'a')",
                // splits the day after 00:59:59.999999999, so the new partition starts at 01:00 exactly
                "INSERT INTO t_same_us VALUES ('2022-02-27T01:00:00.000000000Z'::TIMESTAMP_NS, 400, 400, 'a')",
                // splits that partition after 01:00:00.000000200, so the next one starts at 01:00:00.000000201
                "INSERT INTO t_same_us VALUES ('2022-02-27T01:00:00.000000500Z'::TIMESTAMP_NS, 500, 500, 'a')",
                // one commit rewrites both, which then share their name txn
                "INSERT INTO t_same_us VALUES ('2022-02-27T01:00:00.000000005Z'::TIMESTAMP_NS, 600, 600, 'a'), ('2022-02-27T01:00:00.000000300Z'::TIMESTAMP_NS, 601, 601, 'a')"
        };
        for (String insert : inserts) {
            execute(insert);
            drainWalQueue();
        }
        try (TableReader reader = getReader("t_same_us")) {
            final TimestampDriver driver = timestampType.getDriver();
            Assert.assertEquals(3, reader.getPartitionCount());
            Assert.assertEquals(driver.parseFloorLiteral("2022-02-27T01:00:00.000000Z"), reader.getTxFile().getPartitionTimestampByIndex(1));
            Assert.assertEquals(driver.parseFloorLiteral("2022-02-27T01:00:00.000000Z") + 201, reader.getTxFile().getPartitionTimestampByIndex(2));
            Assert.assertEquals(reader.getTxFile().getPartitionNameTxn(1), reader.getTxFile().getPartitionNameTxn(2));
        }
    }

    /**
     * Asserts the rows of t_same_us after the update of the partition at 01:00:00.000000201: before it c2 is x and s
     * is 'a', 1..100 in the day partition, and 201..220, 400 and 600 at 01:00; in it c2 is 0 and s is 'b'. The
     * filters on s read its POSTING index.
     */
    private void assertRowsOfSameMicrosecondTable() throws Exception {
        assertQuery("SELECT count(), sum(c2) FROM t_same_us WHERE s = 'a'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("Index")
                .returns("""
                        count\tsum
                        122\t10260
                        """);
        assertQuery("SELECT count(), sum(c2) FROM t_same_us WHERE s = 'b'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("Index")
                .returns("""
                        count\tsum
                        3\t0
                        """);
        assertQuery("SELECT count(), sum(c2) FROM t_same_us WHERE ts < '2022-02-27T01:00:00.000000201Z'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("""
                        count\tsum
                        122\t10260
                        """);
    }

    // Erases partition_timestamp_raw of every row of the purge log, as a version before the column wrote the rows: the
    // rows keep the partition timestamp in microseconds only, in partition_timestamp
    private void eraseRawPartitionTimestamps() throws SqlException {
        update("UPDATE '" + purgeLogTableName() + "' SET partition_timestamp_raw = NULL");
    }

    // the timestamp of the partition of tableName at partitionIndex, as _txn has it
    private static long partitionTimestampOf(String tableName, int partitionIndex) {
        try (TableReader reader = getReader(tableName)) {
            return reader.getTxFile().getPartitionTimestampByIndex(partitionIndex);
        }
    }

    // Updates the partition of t_same_us at 01:00:00.000000201 while a reader holds the table, so the purge job only
    // logs the purges of the versions the update replaced, one task per column
    private void updateSecondPartitionWithReader(ColumnPurgeJob purgeJob, String setClause) throws Exception {
        try (TableReader ignore = getReader("t_same_us")) {
            update("UPDATE t_same_us SET " + setClause + " WHERE ts >= '2022-02-27T01:00:00.000000201Z'");
            drainWalQueue();
            runPurgeJob(purgeJob);
        }
    }

    // drops columns of t_split while a reader holds the table, so the purge job only logs their purges, one task each
    private void dropColumnsWithReader(ColumnPurgeJob purgeJob, String... columns) throws Exception {
        final int outstandingPurgeTasks = purgeJob.getOutstandingPurgeTasks();
        try (TableReader ignore = getReader("t_split")) {
            for (String column : columns) {
                execute("ALTER TABLE t_split DROP COLUMN " + column);
            }
            drainWalQueue();
            runPurgeJob(purgeJob);
        }
        Assert.assertEquals(outstandingPurgeTasks + columns.length, purgeJob.getOutstandingPurgeTasks());
    }

    /**
     * The files of {@code columns} in the table directory and in the partition directories of {@code tableName}, as
     * paths within the table directory, sorted and space-separated.
     */
    private String filesOfColumns(String tableName, String... columns) {
        final TreeSet<String> files = new TreeSet<>();
        addFilesOfColumns(files, new File(configuration.getDbRoot().toString(), engine.verifyTableName(tableName).getDirName()), "", columns);
        final int partitionCount;
        try (TableReader reader = getReader(tableName)) {
            partitionCount = reader.getPartitionCount();
        }
        for (int i = 0; i < partitionCount; i++) {
            final File dir = new File(partitionDir(tableName, i));
            addFilesOfColumns(files, dir, dir.getName() + "/", columns);
        }
        return String.join(" ", files);
    }

    // the values of c LONG and s SYMBOL in the row x of t_split
    private static String longAndSymbolValues(long x) {
        return x * 10 + ", 's" + x % 3 + "'";
    }

    // the name of the file at path when it is in the directory of the purge log or in one of its partitions, or null
    private static String purgeLogFileName(LPSZ path) {
        final File file = new File(Utf8s.stringFromUtf8Bytes(path));
        final File dir = file.getParentFile();
        if (dir != null && (isPurgeLogDir(dir) || (dir.getParentFile() != null && isPurgeLogDir(dir.getParentFile())))) {
            return file.getName();
        }
        return null;
    }

    // whether name is the data file of partition_timestamp_raw in a partition of the purge log, named with the txn
    // of the column add
    private static boolean isRawColumnDataFile(LPSZ name) {
        final String fileName = purgeLogFileName(name);
        return fileName != null && fileName.startsWith("partition_timestamp_raw.d");
    }

    private static boolean isPurgeLogDir(File dir) {
        return dir.getName().startsWith(purgeLogTableName() + TableUtils.SYSTEM_TABLE_NAME_SUFFIX);
    }

    private static String purgeLogTableName() {
        return configuration.getSystemTableNamePrefix() + "column_versions_purge_log";
    }

    // A new purge job, as after a restart, replays the purge log, truncates it, and keeps no task for a retry
    private void replayPurgeLog(String tableName) throws Exception {
        engine.releaseInactive();
        // under ADAPTIVE the save points still name the versions that the change replaced, and the replay keeps them
        releaseSavePointHolds(tableName);
        try (ColumnPurgeJob purgeJob = createPurgeJob()) {
            Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            assertPurgeLogRows(purgeJob, 0, 0);
        }
    }

    private static void setSplitProperties() {
        // an O3 row splits a partition whatever the size of the rows before it, rather than adding a piece to it
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 20);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 20);
    }

    /**
     * Creates t_replay with one row in each of five daily partitions, or in default when
     * {@code partitionByAndWal} names no partitioning, and indexes sym with {@code indexClause}.
     */
    private void createReplayTable(String partitionByAndWal, String indexClause) throws SqlException {
        execute("CREATE TABLE t_replay (ts " + timestampType.getTypeName() + ", x LONG, sym SYMBOL) TIMESTAMP(ts) " + partitionByAndWal);
        execute("""
                INSERT INTO t_replay VALUES
                ('1970-01-01T02:00:00.000000Z', 1, 'A'),
                ('1970-01-02T02:00:00.000000Z', 2, 'B'),
                ('1970-01-03T02:00:00.000000Z', 3, 'C'),
                ('1970-01-04T02:00:00.000000Z', 4, 'A'),
                ('1970-01-05T02:00:00.000000Z', 5, 'B')""");
        drainWalQueue();
        execute("ALTER TABLE t_replay ALTER COLUMN sym ADD INDEX TYPE " + indexClause);
        drainWalQueue();
    }

    /**
     * The files in {@code listing} of the instance of {@code col} with the column name txn
     * {@code columnNameTxn}: the data and var-size index files, the BITMAP key and value files, the
     * POSTING key and cover info files, the sealed .pv files, and the .pc files of the host token of the
     * txn, which is 0 for txn NONE.
     */
    private static TreeSet<String> filesOfInstance(String listing, String col, long columnNameTxn) {
        final String txnSuffix = columnNameTxn > TableUtils.COLUMN_NAME_TXN_NONE ? "\\." + columnNameTxn : "";
        final Pattern names = Pattern.compile(Pattern.quote(col) + "\\.(?:(?:d|i|k|v|pk|pci)" + txnSuffix
                + "|pv" + txnSuffix + "\\.[0-9]+"
                + "|pc[0-9]+\\." + Math.max(0, columnNameTxn) + "(?:\\.[0-9]+)?\\.[0-9]+)");
        final TreeSet<String> files = new TreeSet<>();
        for (String file : listing.split(" ")) {
            if (names.matcher(file).matches()) {
                files.add(file);
            }
        }
        return files;
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
