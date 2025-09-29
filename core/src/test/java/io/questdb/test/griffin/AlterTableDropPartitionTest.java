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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;

public class AlterTableDropPartitionTest extends AbstractCairoTest {

    @Test
    public void testAddColumnAndDropPartition() throws Exception {
        assertMemoryLeak(() -> {
                    execute(
                            "create table x as (" +
                                    "select x as i," +
                                    "x as j," +
                                    "x as g," +
                                    "timestamp_sequence('2018-01-01', 72000000L) ts " +
                                    "from long_sequence(1000)" +
                                    ") timestamp (ts) partition by DAY"
                    );

                    execute("insert into x select x, x, x, timestamp_sequence('2018-01-02T12', 72000000L), x from long_sequence(20)");
                    execute("alter table x add column new_col int");
                    execute("insert into x select x, x, x, timestamp_sequence('2018-01-02T12', 72000000L), x from long_sequence(1000)");
                    execute("insert into x select x, x, x, timestamp_sequence('2018-01-02T12', 72000000L), x from long_sequence(1000)");
                    try (TableReader ignored = getReader("x")) {
                        // Open table reader and all partitions
                        assertSql("column\n" +
                                "1\n", "select sum(i) / sum(i) from x");
                    }

                    execute("alter table x add column new_col2 int");
                    execute("alter table x DROP partition list '2018-01-02'");

                    try (TableReader ignored = getReader("x")) {
                        assertSql("column\n" +
                                "1\n", "select sum(i) / sum(i) from x");
                    }
                }
        );
    }

    @Test
    public void testDropMalformedPartition0() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 72000000);

                    try {
                        execute("alter table x drop partition list '2017-01-no'", sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals(34, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "'yyyy-MM-dd' expected");
                    }
                }
        );
    }

    @Test
    public void testDropMalformedPartition1() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 72000000);

                    try {
                        execute("alter table x drop partition list '2017-01'", sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals(34, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "'yyyy-MM-dd' expected, found [ts=2017-01]");
                    }
                }
        );
    }

    @Test
    public void testDropNonExistentPartition() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 72000000);

                    try {
                        execute("alter table x drop partition list '2017-01-05'", sqlExecutionContext);
                        Assert.fail();
                    } catch (CairoException e) {
                        Assert.assertEquals(34, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not remove partition");
                    }
                }
        );
    }

    @Test
    public void testDropPartitionExpectListOrWhere() throws Exception {
        createXAndAssertException("alter table x drop partition", 28, "'list' or 'where' expected");
    }

    @Test
    public void testDropPartitionExpectName0() throws Exception {
        createXAndAssertException("alter table x drop partition list", 33, "partition name expected");
    }

    @Test
    public void testDropPartitionExpectName1() throws Exception {
        createXAndAssertException("alter table x drop partition list,", 33, "partition name missing");
    }

    @Test
    public void testDropPartitionExpectName2() throws Exception {
        createXAndAssertException("alter table x drop partition list;", 33, "partition name expected");
    }

    @Test
    public void testDropPartitionInvalidTimestampColumn() throws Exception {
        createXAndAssertException("alter table x drop partition where a > 1", 35, "Invalid column: a");
    }

    @Test
    public void testDropPartitionListWithMixedWeekDayFormats() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trade as (" +
                    "select" +
                    "  rnd_symbol('A', 'B', 'C') sym," +
                    "  rnd_long(1, 10000000000, 0) px," +
                    "  rnd_float() * 100 leverage," +
                    "  rnd_timestamp(" +
                    "    to_timestamp('2022-06-01', 'yyyy-MM-dd')," +
                    "    to_timestamp('2024-01-03', 'yyyy-MM-dd')," +
                    "    0) ts" +
                    "  from long_sequence(360)" +
                    "), index(sym capacity 128) timestamp(ts) partition by week;");
            assertSql(
                    "year\tweek_of_year\twoy\n" +
                            "2024\t1\t2024-W01\n" +
                            "2023\t52\t2023-W52\n" +
                            "2023\t51\t2023-W51\n" +
                            "2023\t50\t2023-W50\n" +
                            "2023\t49\t2023-W49\n" +
                            "2023\t48\t2023-W48\n" +
                            "2023\t47\t2023-W47\n" +
                            "2023\t46\t2023-W46\n" +
                            "2023\t45\t2023-W45\n" +
                            "2023\t44\t2023-W44\n", "WITH timestamps AS (SELECT first(ts) ts FROM trade SAMPLE BY d ALIGN TO CALENDAR)" +
                            "SELECT DISTINCT year(ts), week_of_year(ts), to_str(ts, 'yyyy-Www') woy FROM timestamps ORDER BY year DESC, week_of_year DESC" +
                            "  LIMiT 10"
            );

            execute("ALTER TABLE trade DROP PARTITION LIST '2023-W51', '2023-W50', '2023-12-05T23:47:21.038145Z'", sqlExecutionContext);

            assertSql(
                    "year\tweek_of_year\twoy\n" +
                            "2024\t1\t2024-W01\n" +
                            "2023\t52\t2023-W52\n" +
                            "2023\t48\t2023-W48\n" +
                            "2023\t47\t2023-W47\n" +
                            "2023\t46\t2023-W46\n" +
                            "2023\t45\t2023-W45\n" +
                            "2023\t44\t2023-W44\n" +
                            "2023\t43\t2023-W43\n" +
                            "2023\t42\t2023-W42\n" +
                            "2023\t41\t2023-W41\n", "WITH timestamps AS (SELECT first(ts) ts FROM trade SAMPLE BY d ALIGN TO CALENDAR)" +
                            "SELECT DISTINCT year(ts), week_of_year(ts), to_str(ts, 'yyyy-Www') woy FROM timestamps ORDER BY year DESC, week_of_year DESC" +
                            "  LIMiT 10"
            );
        });
    }

    @Test
    public void testDropPartitionListWithOneItem0() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = "count\n" +
                            "120\n";

                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    execute("alter table x DROP partition list '2018-01-05', '2018-01-07'");

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult(expectedAfterDrop, "2018-01-05");
                    assertPartitionResult(expectedAfterDrop, "2018-01-07");
                }
        );
    }

    @Test
    public void testDropPartitionListWithOneItem1() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = "count\n" +
                            "120\n";

                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    // names have extra characters
                    execute("alter table x DROP partition list '2018-01-05T23', '2018-01-07T15'");

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult(expectedAfterDrop, "2018-01-05");
                    assertPartitionResult(expectedAfterDrop, "2018-01-07");
                }
        );
    }

    @Test
    public void testDropPartitionListWithOneItemTwice() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = "count\n" +
                            "120\n";

                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    execute("alter table x DROP partition list '2018-01-05';");
                    execute("alter table x DROP partition list '2018-01-07'; \n\n");

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult(expectedAfterDrop, "2018-01-05");
                    assertPartitionResult(expectedAfterDrop, "2018-01-07");
                }
        );
    }

    @Test
    public void testDropPartitionMacFileReadTimeoutError() throws Exception {
        assertMemoryLeak(new FilesFacadeImpl() {
                             private boolean returnError = true;

                             @Override
                             public int errno() {
                                 if (returnError) {
                                     returnError = false;
                                     return CairoException.ERRNO_FILE_READ_TIMEOUT_MACOS;
                                 }
                                 return Os.errno();
                             }

                             @Override
                             public long openRO(LPSZ name) {
                                 try {
                                     if (name.asAsciiCharSequence().toString().endsWith("x~/_meta")) {
                                         throw new RuntimeException();
                                     }
                                 } catch (Exception e) {
                                     final StackTraceElement ste = e.getStackTrace()[6];
                                     if (returnError && ste.getClassName().equals("io.questdb.cairo.TableReaderMetadata") && ste.getMethodName().equals("load")) {
                                         return -1;
                                     }
                                 }
                                 return Files.openRO(name);
                             }
                         },
                () -> {
                    createX("DAY", 72000000);
                    execute("alter table x drop partition list '2018-01-01'", sqlExecutionContext);
                }
        );
    }

    @Test
    public void testDropPartitionNameMissing0() throws Exception {
        createXAndAssertException("alter table x drop partition list ,", 34, "partition name missing");
    }

    @Test
    public void testDropPartitionNameMissing1() throws Exception {
        createXAndAssertException("alter table x drop partition list ;", 34, "partition name expected");
    }

    @Test
    public void testDropPartitionNameMissing2() throws Exception {
        createXAndAssertException("alter table x drop partition list '202';", 34, "'yyyy' expected, found [ts=202]");
    }

    @Test
    public void testDropPartitionWhereExpressionMissing() throws Exception {
        createXAndAssertException("alter table x drop partition where ", 34, "boolean expression expected");
    }

    @Test
    public void testDropPartitionWhereTimestampColumnNameIsOtherThanTimestamp() throws Exception {
        assertMemoryLeak(() -> {
                    createXWithDifferentTimestampName();

                    assertPartitionResultForTimestampColumnNameTs("count\n" +
                                    "145\n",
                            "2018");

                    assertPartitionResultForTimestampColumnNameTs("count\n" +
                            "147\n", "2020");

                    execute("alter table x drop partition where ts < dateadd('d', -1, now() ) AND ts < now()");

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResultForTimestampColumnNameTs(expectedAfterDrop, "2018");
                    assertPartitionResultForTimestampColumnNameTs(expectedAfterDrop, "2020");
                }
        );
    }

    @Test
    public void testDropPartitionWhereTimestampEquals() throws Exception {
        assertMemoryLeak(() -> {
                    createX("YEAR", 3 * 72000000000L);

                    assertPartitionResult("count\n" +
                                    "145\n",
                            "2018");

                    assertPartitionResult("count\n" +
                            "147\n", "2020");

                    execute("alter table x drop partition where timestamp = to_timestamp('2020-01-01:00:00:00', 'yyyy-MM-dd:HH:mm:ss')");

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult(
                            "count\n" +
                                    "145\n",
                            "2018"
                    );
                    assertPartitionResult(expectedAfterDrop, "2020");
                }
        );
    }

    @Test
    public void testDropPartitionWhereTimestampGreaterThanZero() throws Exception {
        assertMemoryLeak(() -> {
                    createX("YEAR", 3 * 72000000000L);

                    assertPartitionResult("count\n" +
                                    "145\n",
                            "2018");

                    assertPartitionResult("count\n" +
                            "147\n", "2020");

                    execute("alter table x drop partition where timestamp > 0 ");

                    String zeroCount = "count\n0\n";
                    for (int i = 2018; i < 2025; i++) {
                        assertPartitionResult(zeroCount, String.valueOf(i));
                    }
                }
        );
    }

    @Test
    public void testDropPartitionWhereTimestampsIsActivePartition() throws Exception {
        assertMemoryLeak(() -> {
                    createX("YEAR", 3 * 72000000000L);

                    assertPartitionResult("count\n" +
                                    "145\n",
                            "2018");

                    assertPartitionResult("count\n" +
                            "147\n", "2020");

                    execute("alter table x drop partition where timestamp = to_timestamp('2022-01-01:00:00:00', 'yyyy-MM-dd:HH:mm:ss')");

                    assertPartitionResult("count\n" +
                                    "145\n",
                            "2018");

                    assertPartitionResult("count\n" +
                            "147\n", "2020");
                }
        );
    }

    @Test
    public void testDropPartitionWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "x";
            TableModel tm = new TableModel(engine.getConfiguration(), tableName, PartitionBy.DAY);
            tm.col("inn", ColumnType.INT).timestamp("ts");
            createPopulateTable(tm, 100, "2022-02-24", 3);

            execute("alter table x add column lo LONG");
            execute("insert into x " +
                    "select x, timestamp_sequence('2022-02-26T23:59:59', 1000000), x " +
                    "from long_sequence(199)");

            execute("alter table x drop partition list '2022-02-26'");
            execute("insert into x " +
                    "select x, timestamp_sequence('2022-02-26T12', 10*60*1000000), x " +
                    "from long_sequence(10)");

            assertSql("inn\tts\tlo\n" +
                    "1\t2022-02-26T12:00:00.000000Z\t1\n" +
                    "2\t2022-02-26T12:10:00.000000Z\t2\n" +
                    "3\t2022-02-26T12:20:00.000000Z\t3\n" +
                    "4\t2022-02-26T12:30:00.000000Z\t4\n" +
                    "5\t2022-02-26T12:40:00.000000Z\t5\n" +
                    "6\t2022-02-26T12:50:00.000000Z\t6\n" +
                    "7\t2022-02-26T13:00:00.000000Z\t7\n" +
                    "8\t2022-02-26T13:10:00.000000Z\t8\n" +
                    "9\t2022-02-26T13:20:00.000000Z\t9\n" +
                    "10\t2022-02-26T13:30:00.000000Z\t10\n", "x where ts in '2022-02-26'");
        });
    }

    @Test
    public void testDropPartitionWithO3Version() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "x";
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TableModel tm = new TableModel(engine.getConfiguration(), tableName, PartitionBy.DAY);
                tm.timestamp();
                TestUtils.createPopulateTable(compiler, sqlExecutionContext, tm, 100, "2020-01-01", 5);
            }
            execute("insert into " + tableName + " " +
                    "select timestamp_sequence('2020-01-01', " + Micros.HOUR_MICROS + "L) " +
                    "from long_sequence(50)");

            assertPartitionResult("count\n44\n", "2020-01-01");

            TableToken tableToken = engine.verifyTableName(tableName);
            try (Path path = new Path().of(engine.getConfiguration().getDbRoot()).concat(tableToken)) {
                path.concat("2020-01-01.1").concat("timestamp.d").$();
                Assert.assertTrue(TestFilesFacadeImpl.INSTANCE.exists(path.$()));
                engine.releaseAllReaders();

                execute("alter table x drop partition where timestamp = '2020-01-01'", sqlExecutionContext);

                assertPartitionResult("count\n0\n", "2020-01-01");
                Assert.assertFalse(TestFilesFacadeImpl.INSTANCE.exists(path.$()));
            }
        });
    }

    @Test
    public void testDropPartitionWriteInOrder() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "x";
            TableModel tm = new TableModel(engine.getConfiguration(), tableName, PartitionBy.HOUR);
            tm.col("x", ColumnType.INT).timestamp("ts");
            createPopulateTable(tm, 1, "2022-12-12T09:05", 1);

            assertReader("x\tts\n" +
                    "1\t2022-12-12T10:04:59.000000Z\n", "x");

            TableToken tableToken = engine.verifyTableName(tableName);
            TableReader rdr1 = getReader(tableToken);
            try (TableWriter tw = getWriter(tableToken)) {

                TableWriter.Row row;

                row = tw.newRow(MicrosTimestampDriver.floor("2022-12-12T11:55"));
                row.putInt(0, 1);
                row.append();
                tw.commit();

                Assert.assertEquals(2, tw.size());
                tw.removePartition(MicrosTimestampDriver.floor("2022-12-12T10:00"));
                Assert.assertEquals(1, tw.size());

                // Reader refresh after table partition remove.
                rdr1.close();
                assertReader("x\tts\n" +
                        "1\t2022-12-12T11:55:00.000000Z\n", "x");

                row = tw.newRow(MicrosTimestampDriver.floor("2022-12-12T11:56"));
                row.putInt(0, 2);
                row.append();

                row = tw.newRow(MicrosTimestampDriver.floor("2022-12-12T12:00"));
                row.putInt(0, 3);
                row.append();
                tw.commit();

                row = tw.newRow(MicrosTimestampDriver.floor("2022-12-12T12:55"));
                row.putInt(0, 4);
                row.append();

                tw.removePartition(MicrosTimestampDriver.floor("2022-12-12T11:00"));
                Assert.assertEquals(2, tw.size());
                assertReader("x\tts\n" +
                        "3\t2022-12-12T12:00:00.000000Z\n" +
                        "4\t2022-12-12T12:55:00.000000Z\n", "x");

                row = tw.newRow(MicrosTimestampDriver.floor("2022-12-12T12:56"));
                row.putInt(0, 5);
                row.append();

                row = tw.newRow(MicrosTimestampDriver.floor("2022-12-12T13:00"));
                row.putInt(0, 6);
                row.append();
                tw.commit();

                Assert.assertEquals(4, tw.size());
                assertReader("x\tts\n" +
                        "3\t2022-12-12T12:00:00.000000Z\n" +
                        "4\t2022-12-12T12:55:00.000000Z\n" +
                        "5\t2022-12-12T12:56:00.000000Z\n" +
                        "6\t2022-12-12T13:00:00.000000Z\n", "x");
            }
        });
    }

    @Test
    public void testDropPartitionWrongSeparator() throws Exception {
        createXAndAssertException("alter table x DROP partition list '2018';'2018'", 41, "',' expected");
    }

    @Test
    public void testDropPartitionsByDayUsingWhereClause() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = "count\n" +
                            "120\n";

                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    execute("alter table x drop partition where timestamp = to_timestamp('2018-01-05:00:00:00', 'yyyy-MM-dd:HH:mm:ss') ");

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult(expectedAfterDrop, "2018-01-05");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                }
        );
    }

    @Test
    public void testDropPartitionsUsingWhereClauseAfterRenamingColumn1() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = "count\n" +
                            "120\n";

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    execute("alter table x rename column timestamp to ts ");

                    assertPartitionResultForTimestampColumnNameTs(expectedBeforeDrop, "2018-01-05");

                    execute("alter table x drop partition where ts = to_timestamp('2018-01-05:00:00:00', 'yyyy-MM-dd:HH:mm:ss') ");

                    assertPartitionResultForTimestampColumnNameTs(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResultForTimestampColumnNameTs(expectedAfterDrop, "2018-01-05");
                }
        );
    }

    @Test
    public void testDropPartitionsUsingWhereClauseAfterRenamingColumn2() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = "count\n" +
                            "120\n";

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    execute("alter table x rename column b to bbb ");

                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    execute("alter table x drop partition list '2018-01-05' ");

                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedAfterDrop, "2018-01-05");
                }
        );
    }

    @Test
    public void testDropPartitionsUsingWhereClauseForTableWithoutDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
                    createXWithoutDesignatedColumn();

                    try {
                        execute("alter table x drop partition " +
                                        "where timestamp = to_timestamp('2018-01-05:00:00:00', 'yyyy-MM-dd:HH:mm:ss') ",
                                sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals(19, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "table is not partitioned");
                    }
                }
        );
    }

    @Test
    public void testDropSplitLastPartition() throws Exception {
        assertMemoryLeak
                (() -> {
                            createXSplit(2000, 750); // 2000 records per day
                            execute("alter table x drop partition list '2018-01-01'", sqlExecutionContext);
                            assertSql("count\n0\n", "select count() from x where timestamp in '2018-01-01'");
                        }
                );
    }

    @Test
    public void testDropSplitMidPartition() throws Exception {
        assertMemoryLeak(
                () -> {
                    createXSplit(Micros.DAY_MICROS / 300, 299); // 300 records per day
                    execute("alter table x drop partition list '2018-01-01'", sqlExecutionContext);
                    assertSql("count\n0\n", "select count() from x where timestamp in '2018-01-01'");
                }
        );
    }

    @Test
    public void testDropSplitMidPartitionFails() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {
            int i = 0;

            @Override
            public long read(long fd, long buf, long len, long offset) {
                if (offset == 0 && len == 8 && i++ == 1) {
                    return -1;
                }
                return Files.read(fd, buf, len, offset);
            }

        };
        assertMemoryLeak(ff,
                () -> {
                    createXSplit(Micros.DAY_MICROS / 300, 290);
                    assertSql("count\n308\n", "select count() from x where timestamp in '2018-01-01'");

                    try {
                        execute("alter table x drop partition list '2018-01-01'", sqlExecutionContext);
                        Assert.fail();
                    } catch (CairoException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "could not read long");
                    }

                    // no split partition deleted
                    assertSql("count\n308\n", "select count() from x where timestamp in '2018-01-01'");

                    // Retry should work
                    execute("alter table x drop partition list '2018-01-01'", sqlExecutionContext);
                    assertSql("count\n0\n", "select count() from x where timestamp in '2018-01-01'");
                }
        );
    }

    @Test
    public void testDropTwoPartitionsByDay() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = "count\n" +
                            "120\n";

                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    execute("alter table x drop partition list '2018-01-05', '2018-01-07'");

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult(expectedAfterDrop, "2018-01-05");
                    assertPartitionResult(expectedAfterDrop, "2018-01-07");
                }
        );
    }

    @Test
    public void testDropTwoPartitionsByDayUpperCase() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = "count\n" +
                            "120\n";

                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    execute("alter table x DROP partition list '2018-01-05', '2018-01-07'");

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult(expectedAfterDrop, "2018-01-05");
                    assertPartitionResult(expectedAfterDrop, "2018-01-07");
                }
        );
    }

    @Test
    public void testDropTwoPartitionsByMonth() throws Exception {
        assertMemoryLeak(() -> {
                    createX("MONTH", 3 * 7200000000L);

                    assertPartitionResult("count\n" +
                                    "112\n",
                            "2018-02");

                    assertPartitionResult("count\n" +
                            "120\n", "2018-04");

                    execute("alter table x drop partition list '2018-02', '2018-04'");

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult(expectedAfterDrop, "2018-02");
                    assertPartitionResult(expectedAfterDrop, "2018-04");
                }
        );
    }

    @Test
    public void testDropTwoPartitionsByYear() throws Exception {
        assertMemoryLeak(() -> {
                    createX("YEAR", 3 * 72000000000L);

                    assertPartitionResult("count\n" +
                                    "147\n",
                            "2020");

                    assertPartitionResult("count\n" +
                            "146\n", "2022");

                    execute("alter table x drop partition list '2020', '2022'");

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult(expectedAfterDrop, "2020");
                    assertPartitionResult(expectedAfterDrop, "2022");
                }
        );
    }

    @Test
    public void testForceDropPartitionExpectDrop() throws Exception {
        createXAndAssertException("alter table x force partition list '2022-02-04';", 20, "'drop' expected");
    }

    @Test
    public void testForceDropPartitionExpectList() throws Exception {
        createXAndAssertException("alter table x force drop partition where ts < '2022-02-04';", 35, "'list' expected");
    }

    @Test
    public void testForceDropPartitionExpectPartition() throws Exception {
        createXAndAssertException("alter table x force drop list '2022-02-04';", 25, "'partition' expected");
    }

    @Test
    public void testPartitionDeletedFromDiskAfterOpening() throws Exception {
        String expected = "[0] Table 'src' data directory does not exist on the disk at ";
        String startDate = "2020-01-01";
        int day = PartitionBy.NONE;
        int partitionToCheck = -1;
        String partitionDirBaseName = "default";
        int deletedPartitionIndex = 0;
        int rowCount = 10000;
        testPartitionDirDeleted(expected, startDate, day, partitionToCheck, partitionDirBaseName, deletedPartitionIndex, 5, 1, rowCount, rowCount / 5);
    }

    @Test
    public void testPartitionDeletedFromDiskWithoutDropAfterOpeningByDay() throws Exception {
        // Cannot run this on Windows - e.g. delete opened files
        if (!configuration.getFilesFacade().isRestrictedFileSystem()) {
            String startDate = "2020-01-01";
            int day = PartitionBy.DAY;
            int partitionToCheck = 0;
            String partitionDirBaseName = "2020-01-02";
            int deletedPartitionIndex = 0;
            int rowCount = 10000;
            testPartitionDirDeleted(null, startDate, day, partitionToCheck, partitionDirBaseName, deletedPartitionIndex, 5, 1, rowCount, rowCount / 5);
        }
    }

    @Test
    public void testPartitionDeletedFromDiskWithoutDropByDay() throws Exception {
        String expected = "[0] Partition '2020-01-02' does not exist in table 'src' directory. " +
                "Run [ALTER TABLE src FORCE DROP PARTITION LIST '2020-01-02'] " +
                "to repair the table or the database from the backup.";
        String startDate = "2020-01-01";
        int day = PartitionBy.DAY;
        int partitionToCheck = 0;
        String partitionDirBaseName = "2020-01-02";
        int deletedPartitionIndex = 1;
        int rowCount = 10000;
        testPartitionDirDeleted(expected, startDate, day, partitionToCheck, partitionDirBaseName, deletedPartitionIndex, 5, 1, rowCount, rowCount / 5);
    }

    @Test
    public void testPartitionDeletedFromDiskWithoutDropByDayNoVersionInErrorMsg() throws Exception {
        String expected = "[0] Partition '2020-01-02' does not exist in table 'src' directory. " +
                "Run [ALTER TABLE src FORCE DROP PARTITION LIST '2020-01-02'] " +
                "to repair the table or the database from the backup.";
        String startDate = "2020-01-01";
        int day = PartitionBy.DAY;
        int partitionToCheck = 0;
        String partitionDirBaseName = "2020-01-02";
        int deletedPartitionIndex = 1;
        int rowCount = 1000;
        testPartitionDirDeleted(expected, startDate, day, partitionToCheck, partitionDirBaseName, deletedPartitionIndex, 5, 5, rowCount, rowCount / 5);
    }

    @Test
    public void testPartitionDeletedFromDiskWithoutDropByMonth() throws Exception {
        String expected = "[0] Partition '2020-02' does not exist in table 'src' directory. " +
                "Run [ALTER TABLE src FORCE DROP PARTITION LIST '2020-02'] " +
                "to repair the table or the database from the backup.";
        String startDate = "2020-01-01";
        int day = PartitionBy.MONTH;
        int partitionToCheck = 0;
        String partitionDirBaseName = "2020-02";
        int deletedPartitionIndex = 1;
        int rowCount = 10000;
        testPartitionDirDeleted(expected, startDate, day, partitionToCheck, partitionDirBaseName, deletedPartitionIndex, 5, 1, rowCount, 2039);
    }

    @Test
    public void testPartitionDeletedFromDiskWithoutDropByNone() throws Exception {
        String expected = "[0] Table 'src' data directory does not exist on the disk at ";
        String startDate = "2020-01-01";
        int day = PartitionBy.NONE;
        int partitionToCheck = -1;
        String partitionDirBaseName = "default";
        int deletedPartitionIndex = 0;
        int rowCount = 1000;
        testPartitionDirDeleted(expected, startDate, day, partitionToCheck, partitionDirBaseName, deletedPartitionIndex, 1, 1, rowCount, rowCount);
    }

    @Test
    public void testPartitionDeletedFromDiskWithoutDropByWeek() throws Exception {
        String expected = "[0] Partition '2020-W02' does not exist in table 'src' directory. " +
                "Run [ALTER TABLE src FORCE DROP PARTITION LIST '2020-W02'] " +
                "to repair the table or the database from the backup.";
        String startDate = "2020-01-01";
        int day = PartitionBy.WEEK;
        int partitionToCheck = 0;
        String folderToDelete = "2020-W02";
        int deletedPartitionIndex = 1;
        int rowCount = 10000;
        testPartitionDirDeleted(expected, startDate, day, partitionToCheck, folderToDelete, deletedPartitionIndex, 5, 1, rowCount, 1428);
    }

    @Test
    public void testSimpleWhere() throws Exception {
        assertMemoryLeak(() -> {
                    createX("YEAR", 3 * 72000000000L);

                    assertPartitionResult("count\n" +
                                    "145\n",
                            "2018");

                    assertPartitionResult("count\n" +
                            "147\n", "2020");

                    execute("alter table x drop partition where timestamp  < to_timestamp('2020', 'yyyy')) ");

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult(expectedAfterDrop, "2018");
                    assertPartitionResult("count\n" +
                            "147\n", "2020");
                }
        );
    }

    private void assertPartitionResult(String expectedBeforeDrop, String intervalSearch) throws SqlException {
        assertSql(
                expectedBeforeDrop, "select count() from x where timestamp in '" + intervalSearch + "'"
        );
    }

    private void assertPartitionResultForTimestampColumnNameTs(String expectedBeforeDrop, String intervalSearch) throws SqlException {
        assertSql(
                expectedBeforeDrop, "select count() from x where ts in '" + intervalSearch + "'"
        );
    }

    private void createX(String partitionBy, long increment) throws SqlException {
        execute(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * " + increment + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(1000)" +
                        ") timestamp (timestamp)" +
                        " partition by " + partitionBy
        );
    }

    private void createXAndAssertException(String sql, int position, String message) throws Exception {
        assertMemoryLeak(() -> {
            createX("YEAR", 720000000);
            assertExceptionNoLeakCheck(sql, position, message);
        });
    }

    private void createXSplit(long increment, int splitAfter) throws SqlException {
        Overrides overrides = node1.getConfigurationOverrides();
        overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        createX("DAY", increment);

        try (TableReader ignore = engine.getReader(engine.verifyTableName("x"))) {
            try {
                long nextTimestamp = MicrosTimestampDriver.floor("2018-01-01") + increment * splitAfter + 1;
                String nextTsStr = Micros.toUSecString(nextTimestamp);
                execute("insert into x " +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " cast('" + nextTsStr + "' as timestamp) + x * " + increment + " ts," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(100)");
            } catch (NumericException e) {
                throw new RuntimeException(e);
            }
        }
    }


    private void createXWithDifferentTimestampName() throws SqlException {
        execute(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * " + 216000000000L + " ts," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(1000)" +
                        ") timestamp (ts)" +
                        "partition by " + "YEAR"
        );
    }

    private void createXWithoutDesignatedColumn() throws SqlException {
        execute(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * " + 720000000L + " ts," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(1000)" +
                        ")"
        );
    }

    private void deleteDir(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                deleteDir(f);
            }
        }
        if (!file.delete()) {
            Assert.fail("Failed to delete dir: " + file.getAbsolutePath());
        }
    }

    private long readSumLongColumn(TableReader reader, int partitionRowCount, int colIndex) {
        long sum = 0L;
        for (int i = 0; i < partitionRowCount; i++) {
            long aLong = reader.getColumn(colIndex).getLong(i * 8L);
            sum += aLong;
        }
        return sum;
    }

    private void testPartitionDirDeleted(
            String expected,
            String startDate,
            int partitionBy,
            int partitionToCheck,
            String partitionDirBaseName,
            int deletedPartitionIndex,
            int partitionCount,
            int insertIterations,
            int totalRowsPerIteration,
            int partitionRowCount
    ) throws Exception {
        final int totalPartitionRowCount = insertIterations * partitionRowCount;
        assertMemoryLeak(() -> {
            TableModel src = new TableModel(configuration, "src", partitionBy);
            createPopulateTable(
                    1,
                    src.col("l", ColumnType.LONG)
                            .col("i", ColumnType.INT)
                            .timestamp("ts"),
                    insertIterations,
                    totalRowsPerIteration,
                    startDate,
                    partitionCount
            );

            engine.clear();

            try (final TableReader reader = getReader(src.getName())) {
                long sum = 0;
                int colIndex = 0;
                boolean opened = false;
                if (partitionToCheck > -1) {
                    Assert.assertEquals(totalPartitionRowCount, reader.openPartition(partitionToCheck));
                    opened = true;

                    // read first column on first partition
                    colIndex = TableReader.getPrimaryColumnIndex(reader.getColumnBase(partitionToCheck), 0);
                    Assert.assertTrue(colIndex > 0); // This can change with refactoring, test has to be updated to get col index correctly
                    sum = readSumLongColumn(reader, totalPartitionRowCount, colIndex);
                    long expectedSumFrom0ToPartitionCount = (long) (insertIterations * (partitionRowCount * (partitionRowCount + 1.0) / 2.0));
                    Assert.assertEquals(expectedSumFrom0ToPartitionCount, sum);
                }

                // Delete partition directory
                String dirToDelete = insertIterations > 1 ? partitionDirBaseName + "." + (insertIterations - 1) : partitionDirBaseName;
                TableToken tableToken = engine.verifyTableName(src.getName());
                File dir = new File(Paths.get(root, tableToken.getDirName(), dirToDelete).toString());
                deleteDir(dir);

                if (opened) {
                    // Should not affect open partition
                    reader.reload();
                    long sum2 = readSumLongColumn(reader, totalPartitionRowCount, colIndex);
                    Assert.assertEquals(sum, sum2);
                }

                if (expected == null) {
                    // Don't check that partition open fails if it's already opened
                    Assert.assertEquals(totalPartitionRowCount, reader.openPartition(deletedPartitionIndex));
                } else {
                    // Should throw something meaningful
                    try {
                        reader.openPartition(deletedPartitionIndex);
                        Assert.fail();
                    } catch (CairoException ex) {
                        TestUtils.assertContains(ex.getMessage(), expected);
                    }

                    if (partitionBy != PartitionBy.NONE) {
                        execute("ALTER TABLE " + src.getName() + " FORCE DROP PARTITION LIST '" + partitionDirBaseName + "';", sqlExecutionContext);
                    }
                }
            }
        });
    }
}
