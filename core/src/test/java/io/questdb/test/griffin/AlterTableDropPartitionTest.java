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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
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
                        assertQuery("select sum(i) / sum(i) from x")
                                .noLeakCheck()
                                .expectSize()
                                .noRandomAccess()
                                .returns("""
                                        column
                                        1
                                        """);
                    }

                    execute("alter table x add column new_col2 int");
                    execute("alter table x DROP partition list '2018-01-02'");

                    try (TableReader ignored = getReader("x")) {
                        assertQuery("select sum(i) / sum(i) from x")
                                .noLeakCheck()
                                .expectSize()
                                .noRandomAccess()
                                .returns("""
                                        column
                                        1
                                        """);
                    }
                }
        );
    }

    @Test
    public void testConvertPartitionWhereNegatedBindVariableStrideIsAccepted() throws Exception {
        assertMemoryLeak(() -> {
            createFourDailyPartitions("cnv3", false);
            try {
                bindVariableService.clear();
                bindVariableService.setInt(0, 2);
                execute("ALTER TABLE cnv3 CONVERT PARTITION TO PARQUET WHERE ts < dateadd('d', -$1, '2024-07-11T00:00:00.000000Z'::timestamp)");
                // CONVERT leaves the row count alone, so the count on its own says nothing about
                // which partition the WHERE clause selected; the parquet flag does. The stride is
                // -2 days from 2024-07-11, so only 2024-07-08 falls below the cut-off.
                assertQuery("SELECT count() FROM cnv3").noLeakCheck().expectSize().noRandomAccess().returns("count\n4\n");
                assertQuery("SELECT name, isParquet FROM table_partitions('cnv3')")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                name\tisParquet
                                2024-07-08\ttrue
                                2024-07-09\tfalse
                                2024-07-10\tfalse
                                2024-07-11\tfalse
                                """);
            } finally {
                bindVariableService.clear();
            }
        });
    }

    @Test
    public void testConvertPartitionWhereNegatedBindVariableStrideIsAcceptedForWalTable() throws Exception {
        assertMemoryLeak(() -> {
            // a WAL table compiles the statement twice - once to sequence it, once to apply it -
            // and the guard has to take the unary negation both times. CONVERT leaves the row count
            // alone, so the count says nothing about which compile ran; the parquet flag does. Only
            // the WAL apply owns a reader, so only it can select a partition and convert it.
            createFourDailyPartitions("cnv4", true);
            try {
                bindVariableService.clear();
                bindVariableService.setInt(0, 2);
                execute("ALTER TABLE cnv4 CONVERT PARTITION TO PARQUET WHERE ts < dateadd('d', -$1, '2024-07-11T00:00:00.000000Z'::timestamp)");
                drainWalQueue();
                assertQuery("SELECT count() FROM cnv4").noLeakCheck().expectSize().noRandomAccess().returns("count\n4\n");
                assertQuery("SELECT name, isParquet FROM table_partitions('cnv4')")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                name\tisParquet
                                2024-07-08\ttrue
                                2024-07-09\tfalse
                                2024-07-10\tfalse
                                2024-07-11\tfalse
                                """);
            } finally {
                bindVariableService.clear();
            }
        });
    }

    @Test
    public void testDetachPartitionWhereNegatedBindVariableStrideIsAccepted() throws Exception {
        assertMemoryLeak(() -> {
            createFourDailyPartitions("det3", false);
            try {
                bindVariableService.clear();
                bindVariableService.setInt(0, 2);
                execute("ALTER TABLE det3 DETACH PARTITION WHERE ts < dateadd('d', -$1, '2024-07-11T00:00:00.000000Z'::timestamp)");
                assertQuery("SELECT count() FROM det3").noLeakCheck().expectSize().noRandomAccess().returns("count\n3\n");
            } finally {
                bindVariableService.clear();
            }
        });
    }

    @Test
    public void testDetachPartitionWhereNegatedBindVariableStrideIsAcceptedForWalTable() throws Exception {
        assertMemoryLeak(() -> {
            // a WAL table compiles the statement twice - once to sequence it, once to apply it -
            // and the guard has to take the unary negation both times. The sequencing compile owns
            // no reader, so it selects no partition and detaches nothing; the row count below falls
            // only if the WAL apply re-compiled the statement and took it again.
            createFourDailyPartitions("det4", true);
            try {
                bindVariableService.clear();
                bindVariableService.setInt(0, 2);
                execute("ALTER TABLE det4 DETACH PARTITION WHERE ts < dateadd('d', -$1, '2024-07-11T00:00:00.000000Z'::timestamp)");
                drainWalQueue();
                assertQuery("SELECT count() FROM det4").noLeakCheck().expectSize().noRandomAccess().returns("count\n3\n");
            } finally {
                bindVariableService.clear();
            }
        });
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
            assertQuery("WITH timestamps AS (SELECT first(ts) ts FROM trade SAMPLE BY d ALIGN TO CALENDAR)" +
                    "SELECT DISTINCT year(ts), week_of_year(ts), to_str(ts, 'yyyy-Www') woy FROM timestamps ORDER BY year DESC, week_of_year DESC" +
                    "  LIMiT 10")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            year\tweek_of_year\twoy
                            2024\t1\t2024-W01
                            2023\t52\t2023-W52
                            2023\t51\t2023-W51
                            2023\t50\t2023-W50
                            2023\t49\t2023-W49
                            2023\t48\t2023-W48
                            2023\t47\t2023-W47
                            2023\t46\t2023-W46
                            2023\t45\t2023-W45
                            2023\t44\t2023-W44
                            """);

            execute("ALTER TABLE trade DROP PARTITION LIST '2023-W51', '2023-W50', '2023-12-05T23:47:21.038145Z'", sqlExecutionContext);

            assertQuery("WITH timestamps AS (SELECT first(ts) ts FROM trade SAMPLE BY d ALIGN TO CALENDAR)" +
                    "SELECT DISTINCT year(ts), week_of_year(ts), to_str(ts, 'yyyy-Www') woy FROM timestamps ORDER BY year DESC, week_of_year DESC" +
                    "  LIMiT 10")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            year\tweek_of_year\twoy
                            2024\t1\t2024-W01
                            2023\t52\t2023-W52
                            2023\t48\t2023-W48
                            2023\t47\t2023-W47
                            2023\t46\t2023-W46
                            2023\t45\t2023-W45
                            2023\t44\t2023-W44
                            2023\t43\t2023-W43
                            2023\t42\t2023-W42
                            2023\t41\t2023-W41
                            """);
        });
    }

    @Test
    public void testDropPartitionListWithOneItem0() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = """
                            count
                            120
                            """;

                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    execute("alter table x DROP partition list '2018-01-05', '2018-01-07'");

                    String expectedAfterDrop = """
                            count
                            0
                            """;

                    assertPartitionResult(expectedAfterDrop, "2018-01-05");
                    assertPartitionResult(expectedAfterDrop, "2018-01-07");
                }
        );
    }

    @Test
    public void testDropPartitionListWithOneItem1() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = """
                            count
                            120
                            """;

                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    // names have extra characters
                    execute("alter table x DROP partition list '2018-01-05T23', '2018-01-07T15'");

                    String expectedAfterDrop = """
                            count
                            0
                            """;

                    assertPartitionResult(expectedAfterDrop, "2018-01-05");
                    assertPartitionResult(expectedAfterDrop, "2018-01-07");
                }
        );
    }

    @Test
    public void testDropPartitionListWithOneItemTwice() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = """
                            count
                            120
                            """;

                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    execute("alter table x DROP partition list '2018-01-05';");
                    execute("alter table x DROP partition list '2018-01-07'; \n\n");

                    String expectedAfterDrop = """
                            count
                            0
                            """;

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
    public void testDropPartitionReadonlyFailsAtExecutionTime() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "select x as i, timestamp_sequence('2018-01-01', 72_000_000L) ts " +
                            "from long_sequence(100)" +
                            ") timestamp(ts) partition by DAY"
            );

            SqlExecutionContext allowAllContext = new SqlExecutionContextImpl(engine, 1).with(
                    AllowAllSecurityContext.INSTANCE,
                    bindVariableService,
                    null,
                    -1,
                    null
            );
            SqlExecutionContext readOnlyContext = new SqlExecutionContextImpl(engine, 1).with(
                    ReadOnlySecurityContext.INSTANCE,
                    bindVariableService,
                    null,
                    -1,
                    null
            );

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery cq = compiler.compile("ALTER TABLE x DROP PARTITION LIST '2018-01-01'", allowAllContext);
                Assert.assertEquals(CompiledQuery.ALTER, cq.getType());
                try {
                    cq.execute(readOnlyContext, null, false);
                    Assert.fail();
                } catch (CairoException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(), "permission denied");
                }
            }

            // verify partition was not dropped
            assertQuery("SELECT count() FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n100\n");
        });
    }

    @Test
    public void testDropPartitionWhereExpressionMissing() throws Exception {
        createXAndAssertException("alter table x drop partition where ", 34, "boolean expression expected");
    }

    @Test
    public void testPartitionWhereUsesWrappedIntValue() throws Exception {
        assertMemoryLeak(() -> {
            createFourDailyPartitions("drop_expr", false);
            createFourDailyPartitions("drop_value", false);
            execute("ALTER TABLE drop_expr DROP PARTITION WHERE ts > 1_720_468_802 * 1_000_000");
            execute("ALTER TABLE drop_value DROP PARTITION WHERE ts > -607_497_088");
            assertSqlCursors("SELECT count() FROM drop_value", "SELECT count() FROM drop_expr");

            createFourDailyPartitions("detach_expr", false);
            createFourDailyPartitions("detach_value", false);
            execute("ALTER TABLE detach_expr DETACH PARTITION WHERE ts < dateadd('u', 1_000_000 * 5_000, '2024-07-10T00:00:00.000000Z'::timestamp)");
            execute("ALTER TABLE detach_value DETACH PARTITION WHERE ts < dateadd('u', 705_032_704, '2024-07-10T00:00:00.000000Z'::timestamp)");
            assertSqlCursors("SELECT count() FROM detach_value", "SELECT count() FROM detach_expr");

            createFourDailyPartitions("convert_expr", false);
            createFourDailyPartitions("convert_value", false);
            execute("ALTER TABLE convert_expr CONVERT PARTITION TO PARQUET WHERE ts < dateadd('u', 1_000_000 * 5_000, '2024-07-10T00:00:00.000000Z'::timestamp)");
            execute("ALTER TABLE convert_value CONVERT PARTITION TO PARQUET WHERE ts < dateadd('u', 705_032_704, '2024-07-10T00:00:00.000000Z'::timestamp)");
            assertSqlCursors(
                    "SELECT name, isParquet FROM table_partitions('convert_value')",
                    "SELECT name, isParquet FROM table_partitions('convert_expr')"
            );
        });
    }

    @Test
    public void testPartitionWhereUsesWrappedIntValueForWalTable() throws Exception {
        assertMemoryLeak(() -> {
            createFourDailyPartitions("wal_expr", true);
            createFourDailyPartitions("wal_value", true);
            execute("ALTER TABLE wal_expr DROP PARTITION WHERE ts > 1_720_468_802 * 1_000_000");
            execute("ALTER TABLE wal_value DROP PARTITION WHERE ts > -607_497_088");
            drainWalQueue();
            assertSqlCursors("SELECT count() FROM wal_value", "SELECT count() FROM wal_expr");
        });
    }

    @Test
    public void testDropPartitionWhereInRangeIntArithmeticIsAccepted() throws Exception {
        assertMemoryLeak(() -> {
            // INT arithmetic that stays inside the INT range computes exactly what it reads like,
            // so it is accepted whatever its operands are spelled as
            createFourDailyPartitions("c1", false);
            execute("ALTER TABLE c1 DROP PARTITION WHERE ts > abs(5) * 2");
            assertQuery("SELECT count() FROM c1").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");

            // a quoted operand is read through the engine's own implicit cast, so this is 10 - the
            // quoting makes no difference to the value and must make none to the verdict
            createFourDailyPartitions("c2", false);
            execute("ALTER TABLE c2 DROP PARTITION WHERE ts > '5' * 2");
            assertQuery("SELECT count() FROM c2").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");

            // the largest INT is still an INT
            createFourDailyPartitions("c3", false);
            execute("ALTER TABLE c3 DROP PARTITION WHERE ts > 2_147_483_647 + 0");
            assertQuery("SELECT count() FROM c3").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");

            // arithmetic buried inside a wider expression: the guard walks into it and finds it sound
            createFourDailyPartitions("c4", false);
            execute("ALTER TABLE c4 DROP PARTITION WHERE ts + 1_000_000 * 60 > 1_720_483_200_000_000L");
            assertQuery("SELECT count() FROM c4").noLeakCheck().expectSize().noRandomAccess().returns("count\n1\n");

            // floating point does not wrap
            createFourDailyPartitions("c5", false);
            execute("ALTER TABLE c5 DROP PARTITION WHERE ts > 1e6 * 1_720_468_802");
            assertQuery("SELECT count() FROM c5").noLeakCheck().expectSize().noRandomAccess().returns("count\n1\n");

            // no underscore separator here: the lexer takes them in integer literals only
            createFourDailyPartitions("c6", false);
            execute("ALTER TABLE c6 DROP PARTITION WHERE ts > 1_720_468_802 * 1000000.0");
            assertQuery("SELECT count() FROM c6").noLeakCheck().expectSize().noRandomAccess().returns("count\n1\n");

            // the other two spellings of the "drop everything" idiom
            createFourDailyPartitions("c7", false);
            execute("ALTER TABLE c7 DROP PARTITION WHERE ts >= 0");
            assertQuery("SELECT count() FROM c7").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");

            createFourDailyPartitions("c8", false);
            execute("ALTER TABLE c8 DROP PARTITION WHERE ts > -1");
            assertQuery("SELECT count() FROM c8").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
        });
    }

    @Test
    public void testDropPartitionWhereIntArithmeticEvaluatingToNullIsAccepted() throws Exception {
        assertMemoryLeak(() -> {
            // every narrow-int factory answers NULL for a NULL operand or a zero divisor, and a
            // NULL bound matches no partition floor, so nothing can be over-matched: the guard
            // stays out of the way and the pre-existing empty-match check reports the statement
            assertPartitionFilterMatchesNothing("nul1", "ts > null * 1_000_000");
            assertPartitionFilterMatchesNothing("nul2", "ts > 1_000_000 / 0");
            assertPartitionFilterMatchesNothing("nul3", "ts > 1_000_000 % 0");
        });
    }

    @Test
    public void testDropPartitionWhereTimestampColumnNameIsOtherThanTimestamp() throws Exception {
        assertMemoryLeak(() -> {
                    createXWithDifferentTimestampName();

                    assertPartitionResultForTimestampColumnNameTs("""
                                    count
                                    145
                                    """,
                            "2018");

                    assertPartitionResultForTimestampColumnNameTs("""
                            count
                            147
                            """, "2020");

                    execute("alter table x drop partition where ts < dateadd('d', -1, now() ) AND ts < now()");

                    String expectedAfterDrop = """
                            count
                            0
                            """;

                    assertPartitionResultForTimestampColumnNameTs(expectedAfterDrop, "2018");
                    assertPartitionResultForTimestampColumnNameTs(expectedAfterDrop, "2020");
                }
        );
    }

    @Test
    public void testDropPartitionWhereTimestampEquals() throws Exception {
        assertMemoryLeak(() -> {
                    createX("YEAR", 3 * 72000000000L);

                    assertPartitionResult("""
                                    count
                                    145
                                    """,
                            "2018");

                    assertPartitionResult("""
                            count
                            147
                            """, "2020");

                    execute("alter table x drop partition where timestamp = to_timestamp('2020-01-01:00:00:00', 'yyyy-MM-dd:HH:mm:ss')");

                    String expectedAfterDrop = """
                            count
                            0
                            """;

                    assertPartitionResult(
                            """
                                    count
                                    145
                                    """,
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

                    assertPartitionResult("""
                                    count
                                    145
                                    """,
                            "2018");

                    assertPartitionResult("""
                            count
                            147
                            """, "2020");

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

                    assertPartitionResult("""
                                    count
                                    145
                                    """,
                            "2018");

                    assertPartitionResult("""
                            count
                            147
                            """, "2020");

                    execute("alter table x drop partition where timestamp = to_timestamp('2022-01-01:00:00:00', 'yyyy-MM-dd:HH:mm:ss')");

                    assertPartitionResult("""
                                    count
                                    145
                                    """,
                            "2018");

                    assertPartitionResult("""
                            count
                            147
                            """, "2020");
                }
        );
    }

    @Test
    public void testDropPartitionWhereWiderBoundSpellingsAreAccepted() throws Exception {
        assertMemoryLeak(() -> {
            // widening one operand keeps the arithmetic at 64 bits, so the bound is the intended one
            createFourDailyPartitions("a1", false);
            execute("ALTER TABLE a1 DROP PARTITION WHERE ts > 1_720_468_802 * 1_000_000L");
            assertQuery("SELECT count() FROM a1").noLeakCheck().expectSize().noRandomAccess().returns("count\n1\n");

            // a plain 64-bit literal is the same bound
            createFourDailyPartitions("a2", false);
            execute("ALTER TABLE a2 DROP PARTITION WHERE ts > 1_720_468_802_000_000L");
            assertQuery("SELECT count() FROM a2").noLeakCheck().expectSize().noRandomAccess().returns("count\n1\n");

            // a timestamp literal is unaffected
            createFourDailyPartitions("a3", false);
            execute("ALTER TABLE a3 DROP PARTITION WHERE ts > '2024-07-08T00:00:00.000000Z'");
            assertQuery("SELECT count() FROM a3").noLeakCheck().expectSize().noRandomAccess().returns("count\n1\n");

            // INT arithmetic that does not wrap stays legal, wherever it sits
            createFourDailyPartitions("a4", false);
            execute("ALTER TABLE a4 DROP PARTITION WHERE ts < dateadd('d', 2 * 7, now())");
            assertQuery("SELECT count() FROM a4").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");

            // a bare INT literal bound keeps working; this is the documented "drop everything" idiom
            createFourDailyPartitions("a5", false);
            execute("ALTER TABLE a5 DROP PARTITION WHERE ts > 0");
            assertQuery("SELECT count() FROM a5").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
        });
    }

    @Test
    public void testDropPartitionWhereWiderBoundSpellingsWithComputedOperandsAreAccepted() throws Exception {
        assertMemoryLeak(() -> {
            // a 64-bit function result keeps the product at 64 bits
            createFourDailyPartitions("b1", false);
            execute("ALTER TABLE b1 DROP PARTITION WHERE ts > extract(epoch from '2024-07-08'::timestamp) * 1_000_000");
            assertQuery("SELECT count() FROM b1").noLeakCheck().expectSize().noRandomAccess().returns("count\n1\n");

            createFourDailyPartitions("b2", false);
            execute("ALTER TABLE b2 DROP PARTITION WHERE ts > datediff('s', 0::timestamp, '2024-07-08'::timestamp) * 1_000_000");
            assertQuery("SELECT count() FROM b2").noLeakCheck().expectSize().noRandomAccess().returns("count\n1\n");

            // widening one OPERAND is the documented fix and it is accepted
            createFourDailyPartitions("b3", false);
            execute("ALTER TABLE b3 DROP PARTITION WHERE ts > (24 * 3600)::long * 1_000_000");
            assertQuery("SELECT count() FROM b3").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");

            // a cast over a constant leaf stays exempt, whichever the target type
            createFourDailyPartitions("b4", false);
            execute("ALTER TABLE b4 DROP PARTITION WHERE ts > 1_720_468_802_000_000::timestamp");
            assertQuery("SELECT count() FROM b4").noLeakCheck().expectSize().noRandomAccess().returns("count\n1\n");

            createFourDailyPartitions("b5", false);
            execute("ALTER TABLE b5 DROP PARTITION WHERE ts > '2024-07-08'::timestamp");
            assertQuery("SELECT count() FROM b5").noLeakCheck().expectSize().noRandomAccess().returns("count\n1\n");

            // a widening cast over 64-bit arithmetic is looked through and found sound
            createFourDailyPartitions("b6", false);
            execute("ALTER TABLE b6 DROP PARTITION WHERE ts > (extract(epoch from '2024-07-08'::timestamp) * 1_000_000)::timestamp");
            assertQuery("SELECT count() FROM b6").noLeakCheck().expectSize().noRandomAccess().returns("count\n1\n");

            // the interval shapes with string bounds are untouched
            createFourDailyPartitions("b7", false);
            execute("ALTER TABLE b7 DROP PARTITION WHERE ts in ('2024-07-09', '2024-07-10')");
            assertQuery("SELECT count() FROM b7").noLeakCheck().expectSize().noRandomAccess().returns("count\n2\n");

            createFourDailyPartitions("b8", false);
            execute("ALTER TABLE b8 DROP PARTITION WHERE ts between '2024-07-09' and '2024-07-10'");
            assertQuery("SELECT count() FROM b8").noLeakCheck().expectSize().noRandomAccess().returns("count\n2\n");

            // a TIMESTAMP-returning function bound
            createFourDailyPartitions("b9", false);
            execute("ALTER TABLE b9 DROP PARTITION WHERE ts = to_timestamp('2024-07-09', 'yyyy-MM-dd')");
            assertQuery("SELECT count() FROM b9").noLeakCheck().expectSize().noRandomAccess().returns("count\n3\n");
        });
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

            assertQuery("x where ts in '2022-02-26'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            inn\tts\tlo
                            1\t2022-02-26T12:00:00.000000Z\t1
                            2\t2022-02-26T12:10:00.000000Z\t2
                            3\t2022-02-26T12:20:00.000000Z\t3
                            4\t2022-02-26T12:30:00.000000Z\t4
                            5\t2022-02-26T12:40:00.000000Z\t5
                            6\t2022-02-26T12:50:00.000000Z\t6
                            7\t2022-02-26T13:00:00.000000Z\t7
                            8\t2022-02-26T13:10:00.000000Z\t8
                            9\t2022-02-26T13:20:00.000000Z\t9
                            10\t2022-02-26T13:30:00.000000Z\t10
                            """);
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

            assertReader("""
                    x\tts
                    1\t2022-12-12T10:04:59.000000Z
                    """, "x");

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
                assertReader("""
                        x\tts
                        1\t2022-12-12T11:55:00.000000Z
                        """, "x");

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
                assertReader("""
                        x\tts
                        3\t2022-12-12T12:00:00.000000Z
                        4\t2022-12-12T12:55:00.000000Z
                        """, "x");

                row = tw.newRow(MicrosTimestampDriver.floor("2022-12-12T12:56"));
                row.putInt(0, 5);
                row.append();

                row = tw.newRow(MicrosTimestampDriver.floor("2022-12-12T13:00"));
                row.putInt(0, 6);
                row.append();
                tw.commit();

                Assert.assertEquals(4, tw.size());
                assertReader("""
                        x\tts
                        3\t2022-12-12T12:00:00.000000Z
                        4\t2022-12-12T12:55:00.000000Z
                        5\t2022-12-12T12:56:00.000000Z
                        6\t2022-12-12T13:00:00.000000Z
                        """, "x");
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

                    String expectedBeforeDrop = """
                            count
                            120
                            """;

                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    execute("alter table x drop partition where timestamp = to_timestamp('2018-01-05:00:00:00', 'yyyy-MM-dd:HH:mm:ss') ");

                    String expectedAfterDrop = """
                            count
                            0
                            """;

                    assertPartitionResult(expectedAfterDrop, "2018-01-05");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                }
        );
    }

    @Test
    public void testDropPartitionsUsingWhereClauseAfterRenamingColumn1() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = """
                            count
                            120
                            """;

                    String expectedAfterDrop = """
                            count
                            0
                            """;

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

                    String expectedBeforeDrop = """
                            count
                            120
                            """;

                    String expectedAfterDrop = """
                            count
                            0
                            """;

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
                            assertQuery("select count() from x where timestamp in '2018-01-01'")
                                    .noLeakCheck()
                                    .expectSize()
                                    .noRandomAccess()
                                    .returns("count\n0\n");
                        }
                );
    }

    @Test
    public void testDropSplitMidPartition() throws Exception {
        assertMemoryLeak(
                () -> {
                    createXSplit(Micros.DAY_MICROS / 300, 299); // 300 records per day
                    execute("alter table x drop partition list '2018-01-01'", sqlExecutionContext);
                    assertQuery("select count() from x where timestamp in '2018-01-01'")
                            .noLeakCheck()
                            .expectSize()
                            .noRandomAccess()
                            .returns("count\n0\n");
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
                    assertQuery("select count() from x where timestamp in '2018-01-01'")
                            .noLeakCheck()
                            .expectSize()
                            .noRandomAccess()
                            .returns("count\n308\n");

                    try {
                        execute("alter table x drop partition list '2018-01-01'", sqlExecutionContext);
                        Assert.fail();
                    } catch (CairoException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "could not read long");
                    }

                    // no split partition deleted
                    assertQuery("select count() from x where timestamp in '2018-01-01'")
                            .noLeakCheck()
                            .expectSize()
                            .noRandomAccess()
                            .returns("count\n308\n");

                    // Retry should work
                    execute("alter table x drop partition list '2018-01-01'", sqlExecutionContext);
                    assertQuery("select count() from x where timestamp in '2018-01-01'")
                            .noLeakCheck()
                            .expectSize()
                            .noRandomAccess()
                            .returns("count\n0\n");
                }
        );
    }

    @Test
    public void testDropTwoPartitionsByDay() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = """
                            count
                            120
                            """;

                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    execute("alter table x drop partition list '2018-01-05', '2018-01-07'");

                    String expectedAfterDrop = """
                            count
                            0
                            """;

                    assertPartitionResult(expectedAfterDrop, "2018-01-05");
                    assertPartitionResult(expectedAfterDrop, "2018-01-07");
                }
        );
    }

    @Test
    public void testDropTwoPartitionsByDayUpperCase() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = """
                            count
                            120
                            """;

                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    execute("alter table x DROP partition list '2018-01-05', '2018-01-07'");

                    String expectedAfterDrop = """
                            count
                            0
                            """;

                    assertPartitionResult(expectedAfterDrop, "2018-01-05");
                    assertPartitionResult(expectedAfterDrop, "2018-01-07");
                }
        );
    }

    @Test
    public void testDropTwoPartitionsByMonth() throws Exception {
        assertMemoryLeak(() -> {
                    createX("MONTH", 3 * 7200000000L);

                    assertPartitionResult("""
                                    count
                                    112
                                    """,
                            "2018-02");

                    assertPartitionResult("""
                            count
                            120
                            """, "2018-04");

                    execute("alter table x drop partition list '2018-02', '2018-04'");

                    String expectedAfterDrop = """
                            count
                            0
                            """;

                    assertPartitionResult(expectedAfterDrop, "2018-02");
                    assertPartitionResult(expectedAfterDrop, "2018-04");
                }
        );
    }

    @Test
    public void testDropTwoPartitionsByYear() throws Exception {
        assertMemoryLeak(() -> {
                    createX("YEAR", 3 * 72000000000L);

                    assertPartitionResult("""
                                    count
                                    147
                                    """,
                            "2020");

                    assertPartitionResult("""
                            count
                            146
                            """, "2022");

                    execute("alter table x drop partition list '2020', '2022'");

                    String expectedAfterDrop = """
                            count
                            0
                            """;

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
                    assertPartitionResult("count\n145\n", "2018");
                    assertPartitionResult("count\n147\n", "2020");

                    execute("alter table x drop partition where timestamp  < to_timestamp('2020', 'yyyy')");
                    String expectedAfterDrop = "count\n0\n";

                    assertPartitionResult(expectedAfterDrop, "2018");
                    assertPartitionResult("count\n147\n", "2020");
                }
        );
    }

    private void assertPartitionFilterMatchesNothing(String tableName, String predicate) throws Exception {
        createFourDailyPartitions(tableName, false);
        try {
            execute("ALTER TABLE " + tableName + " DROP PARTITION WHERE " + predicate);
            Assert.fail("statement was accepted: " + predicate);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "no partitions matched WHERE clause");
        }
        assertQuery("SELECT count() FROM " + tableName).noLeakCheck().expectSize().noRandomAccess().returns("count\n4\n");
    }

    private void assertPartitionResult(String expectedBeforeDrop, String intervalSearch) throws Exception {
        assertQuery("select count() from x where timestamp in '" + intervalSearch + "'")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns(expectedBeforeDrop);
    }

    private void assertPartitionResultForTimestampColumnNameTs(String expectedBeforeDrop, String intervalSearch) throws Exception {
        assertQuery("select count() from x where ts in '" + intervalSearch + "'")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns(expectedBeforeDrop);
    }

    private void createFourDailyPartitions(String tableName, boolean walEnabled) throws Exception {
        execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY"
                + (walEnabled ? " WAL" : " BYPASS WAL"));
        execute("INSERT INTO " + tableName + " VALUES" +
                " ('2024-07-08T00:00:00.000000Z', 1)," +
                " ('2024-07-09T00:00:00.000000Z', 2)," +
                " ('2024-07-10T00:00:00.000000Z', 3)," +
                " ('2024-07-11T00:00:00.000000Z', 4)");
        drainWalQueue();
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
