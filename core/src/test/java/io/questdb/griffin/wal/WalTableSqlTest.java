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

package io.questdb.griffin.wal;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableDroppedException;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_VERSION_FILE_NAME;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class WalTableSqlTest extends AbstractGriffinTest {
    @Test
    public void createWalAndInsertFromSql() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            executeInsert("insert into " + tableName +
                    " values (101, 'dfd', '2022-02-24T01', 'asd')");

            drainWalQueue();

            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\n" +
                    "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T00:00:03.000000Z\tFG\n" +
                    "5\tAB\t2022-02-24T00:00:04.000000Z\tDE\n" +
                    "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\n");
        });
    }

    @Test
    public void test2InsertsAtSameTime() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                    " values (101, 'a1a1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);

            try (InsertOperation insertOperation = compiledQuery.getInsertOperation();
                 InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)) {
                insertMethod.execute();

                CompiledQuery compiledQuery2 = compiler.compile("insert into " + tableName +
                        " values (102, 'bbb', '2022-02-24T02', 'ccc')", sqlExecutionContext);
                try (InsertOperation insertOperation2 = compiledQuery2.getInsertOperation();
                     InsertMethod insertMethod2 = insertOperation2.createMethod(sqlExecutionContext)) {
                    insertMethod2.execute();
                    insertMethod2.commit();
                }
                insertMethod.commit();
            }

            executeInsert("insert into " + tableName + " values (103, 'dfd', '2022-02-24T01', 'asdd')");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "103\tdfd\t2022-02-24T01:00:00.000000Z\tasdd\n" +
                    "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\n" +
                    "102\tbbb\t2022-02-24T02:00:00.000000Z\tccc\n");
        });
    }

    @Test
    public void testAddColumnWalRollsWalSegment() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "str string," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                    " values (101, 'a1a1', 'str-1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
            try (
                    InsertOperation insertOperation = compiledQuery.getInsertOperation();
                    InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
            ) {
                insertMethod.execute();
                insertMethod.execute();
                insertMethod.commit();

                insertMethod.execute();
                compile("alter table " + tableName + " add column new_column int");
                insertMethod.commit();
            }

            executeInsert("insert into " + tableName + " values (103, 'dfd', 'str-2', '2022-02-24T02', 'asdd', 1234)");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tstr\tts\tsym2\tnew_column\n" +
                    "101\ta1a1\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\tNaN\n" +
                    "101\ta1a1\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\tNaN\n" +
                    "101\ta1a1\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\tNaN\n" +
                    "103\tdfd\tstr-2\t2022-02-24T02:00:00.000000Z\tasdd\t1234\n");
        });
    }

    @Test
    public void testAddFixedSizeColumnBeforeInsertCommit() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                    " values (101, 'a1a1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
            try (
                    InsertOperation insertOperation = compiledQuery.getInsertOperation();
                    InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
            ) {

                insertMethod.execute();
                compile("alter table " + tableName + " add column jjj int");
                insertMethod.commit();
            }

            executeInsert("insert into " + tableName + " values (103, 'dfd', '2022-02-24T01', 'asdd', 1234)");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\tjjj\n" +
                    "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\tNaN\n" +
                    "103\tdfd\t2022-02-24T01:00:00.000000Z\tasdd\t1234\n");

        });
    }

    @Test
    public void testAddMultipleWalColumnsBeforeCommit() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                    " values (101, 'a1a1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
            try (
                    InsertOperation insertOperation = compiledQuery.getInsertOperation();
                    InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
            ) {

                insertMethod.execute();
                compile("alter table " + tableName + " add column jjj int");
                compile("alter table " + tableName + " add column col_str string");
                insertMethod.commit();
            }

            executeInsert("insert into " + tableName + " values (103, 'dfd', '2022-02-24T01', 'asdd', 1234, 'sss-value')");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\tjjj\tcol_str\n" +
                    "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\tNaN\t\n" +
                    "103\tdfd\t2022-02-24T01:00:00.000000Z\tasdd\t1234\tsss-value\n");

        });
    }

    @Test
    public void testAddWalColumnAfterCommit() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                    " values (101, 'a1a1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
            try (
                    InsertOperation insertOperation = compiledQuery.getInsertOperation();
                    InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
            ) {

                insertMethod.execute();
                insertMethod.commit();
                compile("alter table " + tableName + " add column jjj int");
            }

            executeInsert("insert into " + tableName + " values (103, 'dfd', '2022-02-24T01', 'asdd', 1234)");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\tjjj\n" +
                    "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\tNaN\n" +
                    "103\tdfd\t2022-02-24T01:00:00.000000Z\tasdd\t1234\n");

        });
    }

    @Test
    public void testCreateDropCreate() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            String sysTableName1 = Chars.toString(engine.getSystemTableName(tableName));
            compile("drop table " + tableName);

            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            String sysTableName2 = Chars.toString(engine.getSystemTableName(tableName));
            MatcherAssert.assertThat(sysTableName1, Matchers.not(Matchers.equalTo(sysTableName2)));

            engine.releaseAllReaders();
            drainWalQueue();

            checkTableFilesDropped(sysTableName1, "2022-02-24", "x.d");

            assertSql(tableName, "x\tts\n" +
                    "1\t2022-02-24T00:00:00.000000Z\n");


        });
    }

    @Test
    public void testCreateDropWalReuseCreate() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            try (
                    WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName);
                    WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName);
                    WalWriter walWriter3 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {
                String sysTableName1 = Chars.toString(engine.getSystemTableName(tableName));
                compile("insert into " + tableName + " values(1, 'A', 'B', '2022-02-24T01')");

                compile("drop table " + tableName);
                try {
                    compile("insert into " + tableName + " values(1, 'A', 'B', '2022-02-24T01')");
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "able does not exist");
                }

                TableWriter.Row row = walWriter1.newRow(IntervalUtils.parseFloorPartialTimestamp("2022-02-24T01"));
                row.putLong(1, 1);
                row.append();

                try {
                    walWriter1.commit();
                    Assert.fail();
                } catch (TableDroppedException e) {
                }
                MatcherAssert.assertThat(walWriter1.isDistressed(), Matchers.is(true));

                // Structural change
                try {
                    addColumn(walWriter2, "sym3", ColumnType.SYMBOL);
                    Assert.fail();
                } catch (TableDroppedException e) {
                }
                MatcherAssert.assertThat(walWriter2.isDistressed(), Matchers.is(true));

                // Nonstructural change
                try {
                    AlterOperationBuilder dropPartition = new AlterOperationBuilder().ofDropPartition(0, tableName, 0);
                    dropPartition.ofPartition(IntervalUtils.parseFloorPartialTimestamp("2022-02-24"));
                    AlterOperation dropAlter = dropPartition.build();
                    dropAlter.withContext(sqlExecutionContext);
                    walWriter3.applyAlter(dropAlter, true);
                    Assert.fail();
                } catch (TableDroppedException e) {
                }
                MatcherAssert.assertThat(walWriter3.isDistressed(), Matchers.is(true));

                compile("create table " + tableName + " as (" +
                        "select x, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts " +
                        " from long_sequence(1)" +
                        ") timestamp(ts) partition by DAY WAL"
                );

                String sysTableName2 = Chars.toString(engine.getSystemTableName(tableName));
                MatcherAssert.assertThat(sysTableName1, Matchers.not(Matchers.equalTo(sysTableName2)));

                engine.releaseAllReaders();
                drainWalQueue();

                checkTableFilesDropped(sysTableName1, "2022-02-24", "x.d");

                assertSql(tableName, "x\tts\n" +
                        "1\t2022-02-24T00:00:00.000000Z\n");
            }
        });
    }

    private void checkTableFilesDropped(String sysTableName1, String partition, String fileName) {
        Path sysPath = Path.PATH.get().of(configuration.getRoot()).concat(sysTableName1).concat(TXN_FILE_NAME);
        MatcherAssert.assertThat(Files.exists(sysPath.$()), Matchers.is(false));

        sysPath = Path.PATH.get().of(configuration.getRoot()).concat(sysTableName1).concat(COLUMN_VERSION_FILE_NAME);
        MatcherAssert.assertThat(Files.exists(sysPath.$()), Matchers.is(false));

        sysPath.of(configuration.getRoot()).concat(sysTableName1).concat("sym.c");
        MatcherAssert.assertThat(Files.exists(sysPath.$()), Matchers.is(false));

        sysPath = Path.PATH.get().of(configuration.getRoot()).concat(sysTableName1).concat(partition).concat(fileName);
        MatcherAssert.assertThat(Files.exists(sysPath.$()), Matchers.is(false));
    }

    @Test
    public void testCreateWalDropColumnInsert() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            compile("alter table " + tableName + " drop column sym");
            compile("insert into " + tableName + "(x, ts) values (2, '2022-02-24T23:00:01')");
            drainWalQueue();

            assertSql(tableName, "x\tsym2\tts\n" +
                    "1\tEF\t2022-02-24T00:00:00.000000Z\n" +
                    "2\t\t2022-02-24T23:00:01.000000Z\n");
        });
    }

    @Test
    public void testCreateWalTableAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            drainWalQueue();

            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\n" +
                    "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T00:00:03.000000Z\tFG\n" +
                    "5\tAB\t2022-02-24T00:00:04.000000Z\tDE\n");
        });
    }

    @Test
    public void testCreateWalTableAsSelectAndInsertAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            compile("insert into " + tableName +
                    " select x + 100, rnd_symbol('AB2', 'BC2', 'CD2') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE2', null, 'EF2', 'FG2') sym2 " +
                    " from long_sequence(3)");

            drainWalQueue();

            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "101\tBC2\t2022-02-24T00:00:00.000000Z\tDE2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "102\tBC2\t2022-02-24T00:00:01.000000Z\tFG2\n" +
                    "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\n" +
                    "103\tBC2\t2022-02-24T00:00:02.000000Z\tDE2\n" +
                    "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T00:00:03.000000Z\tFG\n" +
                    "5\tAB\t2022-02-24T00:00:04.000000Z\tDE\n");
        });
    }

    @Test
    public void testRemoveColumnWalRollsWalSegment() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "str string," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                    " values (101, 'a1a1', 'str-1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
            try (
                    InsertOperation insertOperation = compiledQuery.getInsertOperation();
                    InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
            ) {
                insertMethod.execute();
                insertMethod.execute();
                insertMethod.commit();

                insertMethod.execute();
                compile("alter table " + tableName + " drop column sym");
                insertMethod.commit();
            }

            executeInsert("insert into " + tableName + " values (103, 'str-2', '2022-02-24T02', 'asdd')");

            drainWalQueue();
            assertSql(tableName, "x\tstr\tts\tsym2\n" +
                    "101\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\n" +
                    "101\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\n" +
                    "101\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\n" +
                    "103\tstr-2\t2022-02-24T02:00:00.000000Z\tasdd\n");

        });
    }

    @Test
    public void testRogueTableWriterBlocksApplyJob() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            executeInsert("insert into " + tableName +
                    " values (101, 'dfd', '2022-02-24T01', 'asd')");


            try (TableWriter ignore = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "Rogue")) {
                drainWalQueue();
                assertSql(tableName, "x\tsym\tts\tsym2\n");
            }

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\n" +
                    "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T00:00:03.000000Z\tFG\n" +
                    "5\tAB\t2022-02-24T00:00:04.000000Z\tDE\n" +
                    "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\n");

            // Next insert should fix it
            executeInsert("insert into " + tableName +
                    " values (101, 'dfd', '2022-02-24T01', 'asd')");
            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\n" +
                    "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T00:00:03.000000Z\tFG\n" +
                    "5\tAB\t2022-02-24T00:00:04.000000Z\tDE\n" +
                    "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\n" +
                    "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\n");
        });
    }

    @Test
    public void testVarSizeColumnBeforeInsertCommit() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                    " values (101, 'a1a1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
            try (
                    InsertOperation insertOperation = compiledQuery.getInsertOperation();
                    InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
            ) {

                insertMethod.execute();
                compile("alter table " + tableName + " add column sss string");
                insertMethod.commit();
            }

            executeInsert("insert into " + tableName + " values (103, 'dfd', '2022-02-24T01', 'asdd', '1234')");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\tsss\n" +
                    "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\t\n" +
                    "103\tdfd\t2022-02-24T01:00:00.000000Z\tasdd\t1234\n");

        });
    }
}
