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
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;

import static io.questdb.griffin.CompiledQuery.ALTER;

public class AlterTableDropPartitionTest extends AbstractGriffinTest {

    @Test
    public void testDropMalformedPartition() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 72000000);

                    try {
                        compile("alter table x drop partition list '2017-01'", sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals(34, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "'YYYY-MM-DD' expected");
                    }
                }
        );
    }

    @Test
    public void testDropNonExistentPartition() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 72000000);

                    try {
                        compile("alter table x drop partition list '2017-01-05'", sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals(19, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not remove partition");
                    }
                }
        );
    }

    @Test
    public void testDropPartitionExpectListOrWhere() throws Exception {
        assertFailure("alter table x drop partition", 28, "'list' or 'where' expected");
    }

    @Test
    public void testDropPartitionExpectName() throws Exception {
        assertFailure("alter table x drop partition list", 33, "partition name expected");
    }

    @Test
    public void testDropPartitionInvalidTimestampColumn() throws Exception {
        assertFailure("alter table x drop partition where a > 1", 35, "Invalid column: a");
    }

    @Test
    public void testDropPartitionListWithOneItem() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = "count\n" +
                            "120\n";

                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    Assert.assertEquals(ALTER, compile("alter table x DROP partition list '2018-01-05', '2018-01-07'", sqlExecutionContext).getType());

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

                    Assert.assertEquals(ALTER, compile("alter table x DROP partition list '2018-01-05';", sqlExecutionContext).getType());
                    Assert.assertEquals(ALTER, compile("alter table x DROP partition list '2018-01-07'; \n\n", sqlExecutionContext).getType());

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult(expectedAfterDrop, "2018-01-05");
                    assertPartitionResult(expectedAfterDrop, "2018-01-07");
                }
        );
    }

    @Test
    public void testDropPartitionNameMissing() throws Exception {
        assertFailure("alter table x drop partition list ,", 34, "partition name missing");
    }

    @Test
    public void testDropPartitionNameMissing2() throws Exception {
        assertFailure("alter table x drop partition list ;", 34, "'YYYY' expected");
    }

    @Test
    public void testDropPartitionWhereExpressionMissing() throws Exception {
        assertFailure("alter table x drop partition where ", 34, "boolean expression expected");
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

                    Assert.assertEquals(ALTER, compile("alter table x drop partition where ts < dateadd('d', -1, now() ) AND ts < now()", sqlExecutionContext).getType());

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

                    Assert.assertEquals(ALTER, compile("alter table x drop partition where timestamp = to_timestamp('2020-01-01:00:00:00', 'yyyy-MM-dd:HH:mm:ss')", sqlExecutionContext).getType());

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult("count\n" +
                                    "145\n",
                            "2018");
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

                    Assert.assertEquals(ALTER, compile("alter table x drop partition where timestamp > 0 ", sqlExecutionContext).getType());

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

                    Assert.assertEquals(ALTER, compile("alter table x drop partition where timestamp = to_timestamp('2022-01-01:00:00:00', 'yyyy-MM-dd:HH:mm:ss')", sqlExecutionContext).getType());

                    assertPartitionResult("count\n" +
                                    "145\n",
                            "2018");

                    assertPartitionResult("count\n" +
                            "147\n", "2020");
                }
        );
    }

    @Test
    public void testDropPartitionWithO3Version() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "x";
            try (TableModel tm = new TableModel(engine.getConfiguration(), tableName, PartitionBy.DAY)) {
                tm.timestamp();
                TestUtils.createPopulateTable(compiler, sqlExecutionContext, tm, 100, "2020-01-01", 5);
            }
            compiler.compile("insert into " + tableName + " " +
                    "select timestamp_sequence('2020-01-01', " + Timestamps.HOUR_MICROS + "L) " +
                    "from long_sequence(50)", sqlExecutionContext);

            assertPartitionResult("count\n44\n", "2020-01-01");

            try (Path path = new Path().of(engine.getConfiguration().getRoot()).concat(tableName)) {
                path.concat("2020-01-01.1").concat("timestamp.d").$();
                Assert.assertTrue(FilesFacadeImpl.INSTANCE.exists(path));
                if (Os.type == Os.WINDOWS) {
                    engine.releaseAllReaders();
                }

                compile("alter table x drop partition where timestamp = '2020-01-01'", sqlExecutionContext);

                assertPartitionResult("count\n0\n", "2020-01-01");
                Assert.assertFalse(FilesFacadeImpl.INSTANCE.exists(path));
            }
        });
    }

    @Test
    public void testDropPartitionWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "x";
            try (TableModel tm = new TableModel(engine.getConfiguration(), tableName, PartitionBy.DAY)) {
                tm.col("inn", ColumnType.INT).timestamp("ts");
                createPopulateTable(tm, 100, "2022-02-24", 3);
            }

            compile("alter table x add column lo LONG");
            compile("insert into x " +
                    "select x, timestamp_sequence('2022-02-26T23:59:59', 1000000), x " +
                    "from long_sequence(199)");

            compile("alter table x drop partition list '2022-02-26'");
            compile("insert into x " +
                    "select x, timestamp_sequence('2022-02-26T12', 10*60*1000000), x " +
                    "from long_sequence(10)");

            assertSql("x where ts in '2022-02-26'", "inn\tts\tlo\n" +
                    "1\t2022-02-26T12:00:00.000000Z\t1\n" +
                    "2\t2022-02-26T12:10:00.000000Z\t2\n" +
                    "3\t2022-02-26T12:20:00.000000Z\t3\n" +
                    "4\t2022-02-26T12:30:00.000000Z\t4\n" +
                    "5\t2022-02-26T12:40:00.000000Z\t5\n" +
                    "6\t2022-02-26T12:50:00.000000Z\t6\n" +
                    "7\t2022-02-26T13:00:00.000000Z\t7\n" +
                    "8\t2022-02-26T13:10:00.000000Z\t8\n" +
                    "9\t2022-02-26T13:20:00.000000Z\t9\n" +
                    "10\t2022-02-26T13:30:00.000000Z\t10\n");
        });
    }

    @Test
    public void testDropPartitionWrongSeparator() throws Exception {
        assertFailure("alter table x DROP partition list '2018';'2018'", 41, "',' expected");
    }

    @Test
    public void testDropPartitionsByDayUsingWhereClause() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = "count\n" +
                            "120\n";

                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    Assert.assertEquals(ALTER, compile("alter table x drop partition where timestamp = to_timestamp('2018-01-05:00:00:00', 'yyyy-MM-dd:HH:mm:ss') ", sqlExecutionContext).getType());

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

                    Assert.assertEquals(ALTER, compile("alter table x rename column timestamp to ts ", sqlExecutionContext).getType());

                    assertPartitionResultForTimestampColumnNameTs(expectedBeforeDrop, "2018-01-05");

                    Assert.assertEquals(ALTER, compile("alter table x drop partition where ts = to_timestamp('2018-01-05:00:00:00', 'yyyy-MM-dd:HH:mm:ss') ", sqlExecutionContext).getType());

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

                    Assert.assertEquals(ALTER, compile("alter table x rename column b to bbb ", sqlExecutionContext).getType());

                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    Assert.assertEquals(ALTER, compile("alter table x drop partition list '2018-01-05' ", sqlExecutionContext).getType());

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
                        compile("alter table x drop partition " +
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
    public void testDropTwoPartitionsByDay() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = "count\n" +
                            "120\n";

                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    Assert.assertEquals(ALTER, compile("alter table x drop partition list '2018-01-05', '2018-01-07'", sqlExecutionContext).getType());

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

                    Assert.assertEquals(ALTER, compile("alter table x DROP partition list '2018-01-05', '2018-01-07'", sqlExecutionContext).getType());

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

                    Assert.assertEquals(ALTER, compile("alter table x drop partition list '2018-02', '2018-04'", sqlExecutionContext).getType());

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

                    Assert.assertEquals(ALTER, compile("alter table x drop partition list '2020', '2022'", sqlExecutionContext).getType());

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult(expectedAfterDrop, "2020");
                    assertPartitionResult(expectedAfterDrop, "2022");
                }
        );
    }

    @Test
    public void testPartitionDeletedFromDiskAfterOpening() throws Exception {
        String expected = "[0] Table 'src' data directory does not exist on the disk at ";
        String startDate = "2020-01-01";
        int day = PartitionBy.NONE;
        int partitionToCheck = -1;
        String folderToDelete = "default";
        int deletedPartitionIndex = 0;
        int rowCount = 10000;
        testPartitionDirDeleted(expected, startDate, day, partitionToCheck, folderToDelete, deletedPartitionIndex, 5, rowCount, rowCount / 5);
    }

    @Test
    public void testPartitionDeletedFromDiskWithoutDropAfterOpeningByDay() throws Exception {
        // Cannot run this on Windows - e.g. delete opened files
        if (!configuration.getFilesFacade().isRestrictedFileSystem()) {
            String startDate = "2020-01-01";
            int day = PartitionBy.DAY;
            int partitionToCheck = 0;
            String folderToDelete = "2020-01-02";
            int deletedPartitionIndex = 0;
            int rowCount = 10000;
            testPartitionDirDeleted(null, startDate, day, partitionToCheck, folderToDelete, deletedPartitionIndex, 5, rowCount, rowCount / 5);
        }
    }

    @Test
    public void testPartitionDeletedFromDiskWithoutDropByDay() throws Exception {
        String expected = "[0] Partition '2020-01-02' does not exist in table 'src' directory. " +
                "Run [ALTER TABLE src DROP PARTITION LIST '2020-01-02'] " +
                "to repair the table or restore the partition directory.";
        String startDate = "2020-01-01";
        int day = PartitionBy.DAY;
        int partitionToCheck = 0;
        String folderToDelete = "2020-01-02";
        int deletedPartitionIndex = 1;
        int rowCount = 10000;
        testPartitionDirDeleted(expected, startDate, day, partitionToCheck, folderToDelete, deletedPartitionIndex, 5, rowCount, rowCount / 5);
    }

    @Test
    public void testPartitionDeletedFromDiskWithoutDropByMonth() throws Exception {
        String expected = "[0] Partition '2020-02' does not exist in table 'src' directory. " +
                "Run [ALTER TABLE src DROP PARTITION LIST '2020-02'] " +
                "to repair the table or restore the partition directory.";
        String startDate = "2020-01-01";
        int day = PartitionBy.MONTH;
        int partitionToCheck = 0;
        String folderToDelete = "2020-02";
        int deletedPartitionIndex = 1;
        int rowCount = 10000;
        testPartitionDirDeleted(expected, startDate, day, partitionToCheck, folderToDelete, deletedPartitionIndex, 5, rowCount, 2039);
    }

    @Test
    public void testPartitionDeletedFromDiskWithoutDropByNone() throws Exception {
        String expected = "[0] Table 'src' data directory does not exist on the disk at ";
        String startDate = "2020-01-01";
        int day = PartitionBy.NONE;
        int partitionToCheck = -1;
        String folderToDelete = "default";
        int deletedPartitionIndex = 0;
        int rowCount = 1000;
        testPartitionDirDeleted(expected, startDate, day, partitionToCheck, folderToDelete, deletedPartitionIndex, 1, rowCount, rowCount);
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

                    Assert.assertEquals(ALTER, compile("alter table x drop partition where timestamp  < to_timestamp('2020', 'yyyy')) ", sqlExecutionContext).getType());

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult(expectedAfterDrop, "2018");
                    assertPartitionResult("count\n" +
                            "147\n", "2020");
                }
        );
    }

    private void assertFailure(String sql, int position, String message) throws Exception {
        assertMemoryLeak(() -> {
            try {
                createX("YEAR", 720000000);
                compiler.compile(sql, sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }
        });
    }

    private void assertPartitionResult(String expectedBeforeDrop, String intervalSearch) throws SqlException {
        assertSql(
                "select count() from x where timestamp in '" + intervalSearch + "'",
                expectedBeforeDrop
        );
    }

    private void assertPartitionResultForTimestampColumnNameTs(String expectedBeforeDrop, String intervalSearch) throws SqlException {
        assertSql(
                "select count() from x where ts in '" + intervalSearch + "'",
                expectedBeforeDrop
        );
    }

    private void createX(String partitionBy, long increment) throws SqlException {
        compiler.compile(
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
                        "partition by " + partitionBy,
                sqlExecutionContext
        );
    }

    private void createXWithDifferentTimestampName() throws SqlException {
        compiler.compile(
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
                        "partition by " + "YEAR",
                sqlExecutionContext
        );
    }

    private void createXWithoutDesignatedColumn() throws SqlException {
        compiler.compile(
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
                        ")",
                sqlExecutionContext
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
            String folderToDelete,
            int deletedPartitionIndex,
            int partitionCount,
            int rowCount, int partitionRowCount) throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel src = new TableModel(configuration, "src", partitionBy)) {
                createPopulateTable(
                        src.col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .timestamp("ts"),
                        rowCount,
                        startDate,
                        partitionCount);

                engine.clear();

                try (final TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, src.getName())) {
                    long sum = 0;
                    int colIndex = 0;
                    boolean opened = false;
                    if (partitionToCheck > -1) {
                        Assert.assertEquals(partitionRowCount, reader.openPartition(partitionToCheck));
                        opened = true;

                        // read first column on first partition
                        colIndex = TableReader.getPrimaryColumnIndex(reader.getColumnBase(partitionToCheck), 0);
                        Assert.assertTrue(colIndex > 0); // This can change with refactoring, test has to be updated to get col index correctly
                        sum = readSumLongColumn(reader, partitionRowCount, colIndex);
                        long expectedSumFrom0ToPartitionCount = (long) (partitionRowCount * (partitionRowCount + 1.0) / 2.0);
                        Assert.assertEquals(expectedSumFrom0ToPartitionCount, sum);
                    }

                    // Delete partition folder
                    File dir = new File(Paths.get(root.toString(), src.getName(), folderToDelete).toString());
                    deleteDir(dir);

                    if (opened) {
                        // Should not affect open partition
                        reader.reload();
                        long sum2 = readSumLongColumn(reader, partitionRowCount, colIndex);
                        Assert.assertEquals(sum, sum2);
                    }

                    if (expected == null) {
                        // Don't check that partition open fails if it's already opened
                        Assert.assertEquals(partitionRowCount, reader.openPartition(deletedPartitionIndex));
                    } else {
                        // Should throw something meaningful
                        try {
                            reader.openPartition(deletedPartitionIndex);
                            Assert.fail();
                        } catch (CairoException ex) {
                            TestUtils.assertContains(ex.getMessage(), expected);
                        }

                        if (partitionBy != PartitionBy.NONE) {
                            compile("alter TABLE " + src.getName() + " DROP PARTITION LIST '" + folderToDelete + "';", sqlExecutionContext);
                        }
                    }
                }
            }
        });
    }
}
