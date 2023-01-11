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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.cairo.TableUtils.COLUMN_VERSION_FILE_NAME;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

@SuppressWarnings("SameParameterValue")
public class WalTableSqlTest extends AbstractGriffinTest {
    @BeforeClass
    public static void setUpStatic() {
        walTxnNotificationQueueCapacity = 8;
        AbstractGriffinTest.setUpStatic();
    }

    @Test
    public void createWalAndInsertFromSql() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName() + "_लаблअца";
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
            TableToken sysTableName1 = engine.getTableToken(tableName);
            compile("drop table " + tableName);

            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            TableToken sysTableName2 = engine.getTableToken(tableName);
            MatcherAssert.assertThat(sysTableName1, Matchers.not(Matchers.equalTo(sysTableName2)));

            engine.releaseInactive();
            drainWalQueue();

            checkTableFilesExist(sysTableName1, "2022-02-24", "x.d", false);
            checkWalFilesRemoved(sysTableName1);

            assertSql(tableName, "x\tts\n" +
                    "1\t2022-02-24T00:00:00.000000Z\n");

        });
    }

    @Test
    public void testCreateDropCreateWalNonWal() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String createSql = "create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY";

            compile(createSql + " WAL");
            compile("drop table " + tableName);

            SharedRandom.RANDOM.get().reset();
            compile(createSql);
            assertSql(tableName, "x\tsym\tsym2\tts\n" +
                    "1\tAB\tEF\t2022-02-24T00:00:00.000000Z\n");

            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                compile("drop table " + tableName);
                SharedRandom.RANDOM.get().reset();
                compile(createSql + " WAL");
                drainWalQueue(walApplyJob);
                assertSql(tableName, "x\tsym\tsym2\tts\n" +
                        "1\tAB\tEF\t2022-02-24T00:00:00.000000Z\n");

                compile("drop table " + tableName);
                SharedRandom.RANDOM.get().reset();
                compile(createSql);
                assertSql(tableName, "x\tsym\tsym2\tts\n" +
                        "1\tAB\tEF\t2022-02-24T00:00:00.000000Z\n");

                compile("drop table " + tableName);
                SharedRandom.RANDOM.get().reset();
                compile(createSql + " WAL");
                drainWalQueue(walApplyJob);
                assertSql(tableName, "x\tsym\tsym2\tts\n" +
                        "1\tAB\tEF\t2022-02-24T00:00:00.000000Z\n");
            }

        });
    }

    @Test
    public void testCreateDropWalReuseCreate() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName() + "Â";
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            TableToken tableToken = engine.getTableToken(tableName);
            try (
                    WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableToken);
                    WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableToken);
                    WalWriter walWriter3 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableToken)
            ) {
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
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table is dropped ");
                }
                MatcherAssert.assertThat(walWriter1.isDistressed(), Matchers.is(true));

                // Structural change
                try {
                    addColumn(walWriter2, "sym3", ColumnType.SYMBOL);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table is dropped ");
                }
                MatcherAssert.assertThat(walWriter2.isDistressed(), Matchers.is(true));

                // Nonstructural change
                try {
                    AlterOperationBuilder dropPartition = new AlterOperationBuilder().ofDropPartition(0, tableToken, 1);
                    dropPartition.addPartitionToList(IntervalUtils.parseFloorPartialTimestamp("2022-02-24"), 0);
                    AlterOperation dropAlter = dropPartition.build();
                    dropAlter.withContext(sqlExecutionContext);
                    dropAlter.withSqlStatement("alter table " + tableName + " drop partition list '2022-02-24'");
                    walWriter3.apply(dropAlter, true);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table is dropped ");
                }
                MatcherAssert.assertThat(walWriter3.isDistressed(), Matchers.is(true));

                compile("create table " + tableName + " as (" +
                        "select x, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts " +
                        " from long_sequence(1)" +
                        ") timestamp(ts) partition by DAY WAL"
                );

                TableToken sysTableName2 = engine.getTableToken(tableName);
                MatcherAssert.assertThat(tableToken, Matchers.not(Matchers.equalTo(sysTableName2)));

                engine.releaseAllReaders();
                drainWalQueue();

                checkTableFilesExist(tableToken, "2022-02-24", "x.d", false);
            }
            checkWalFilesRemoved(tableToken);

            assertSql(tableName, "x\tts\n" +
                    "1\t2022-02-24T00:00:00.000000Z\n");
        });
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
    public void testDropFailedWhileDataFileLocked() throws Exception {
        testDropFailedWhileDataFileLocked("x.d");
    }

    @Test
    public void testDropFailedWhileSymbolFileLocked() throws Exception {
        testDropFailedWhileDataFileLocked("sym.c");
    }

    @Test
    public void testDropPartitionRenameTable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String newTableName = testName.getMethodName() + "_new";
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 24 * 60 * 60 * 1000000L) ts " +
                    " from long_sequence(2)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            compile("alter table " + tableName + " drop partition list '2022-02-24'");
            TableToken table2directoryName = engine.getTableToken(tableName);
            compile("rename table " + tableName + " to " + newTableName);

            TableToken newTabledirectoryName = engine.getTableToken(newTableName);
            Assert.assertEquals(table2directoryName.getDirName(), newTabledirectoryName.getDirName());

            drainWalQueue();

            assertSql(newTableName, "x\tsym2\tts\n" +
                    "2\tEF\t2022-02-25T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testDropRemovesFromCatalogFunctions() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String tableNameNonWal = testName.getMethodName() + "_non_wal";

            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            compile("create table " + tableNameNonWal + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY BYPASS WAL"
            );

            assertSql("all_tables() order by table", "table\n" +
                    tableName + "\n" +
                    tableNameNonWal + "\n");
            assertSql("select name from tables() order by name", "name\n" +
                    tableName + "\n" +
                    tableNameNonWal + "\n");
            assertSql("select relname from pg_class() order by relname", "relname\npg_class\n" +
                    tableName + "\n" +
                    tableNameNonWal + "\n");


            compile("drop table " + tableName);

            assertSql("all_tables() order by table", "table\n" +
                    tableNameNonWal + "\n");
            assertSql("select name from tables() order by name", "name\n" +
                    tableNameNonWal + "\n");
            assertSql("select relname from pg_class() order by relname", "relname\npg_class\n" +
                    tableNameNonWal + "\n");

            drainWalQueue();

            assertSql("all_tables() order by table", "table\n" +
                    tableNameNonWal + "\n");
            assertSql("select name from tables() order by name", "name\n" +
                    tableNameNonWal + "\n");
            assertSql("select relname from pg_class() order by relname", "relname\npg_class\n" +
                    tableNameNonWal + "\n");


            refreshTablesInBaseEngine();

            assertSql("all_tables() order by table", "table\n" +
                    tableNameNonWal + "\n");
            assertSql("select name from tables() order by name", "name\n" +
                    tableNameNonWal + "\n");
            assertSql("select relname from pg_class() order by relname", "relname\npg_class\n" +
                    tableNameNonWal + "\n");

            compile("drop table " + tableNameNonWal);

            assertSql("all_tables() order by table", "table\n");
            assertSql("select name from tables() order by name", "name\n");
            assertSql("select relname from pg_class() order by relname", "relname\npg_class\n");
        });
    }

    @Test
    public void testDropRetriedWhenReaderOpen() throws Exception {
        assertMemoryLeak(ff, () -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                drainWalQueue(walApplyJob);
                TableToken sysTableName1 = engine.getTableToken(tableName);

                try (TableReader ignore = sqlExecutionContext.getReader(sysTableName1)) {
                    compile("drop table " + tableName);
                    drainWalQueue(walApplyJob);
                    checkTableFilesExist(sysTableName1, "2022-02-24", "x.d", true);
                }

                if (Os.type == Os.WINDOWS) {
                    // Release WAL writers
                    engine.releaseInactive();
                }
                drainWalQueue(walApplyJob);

                checkTableFilesExist(sysTableName1, "2022-02-24", "x.d", false);
                checkWalFilesRemoved(sysTableName1);
            }
        });
    }

    @Test
    public void testDropTxnNotificationQueueOverflow() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + "1 as (" +
                    "select x, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            compile("create table " + tableName + "2 as (" +
                    "select x, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                drainWalQueue(walApplyJob);

                for (int i = 0; i < walTxnNotificationQueueCapacity; i++) {
                    compile("insert into " + tableName + "1 values (101, 'a1a1', '2022-02-24T01')");
                }

                TableToken table2directoryName = engine.getTableToken(tableName + "2");
                compile("drop table " + tableName + "2");

                drainWalQueue(walApplyJob);

                checkTableFilesExist(table2directoryName, "2022-02-24", "x.d", false);
            }
        });
    }

    @Test
    public void testDropedTableHappendIntheMiddleOfWalApplication() throws Exception {
        String tableName = testName.getMethodName();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int openRO(LPSZ name) {
                if (Chars.endsWith(name, Files.SEPARATOR + "0" + Files.SEPARATOR + "sym.d")) {
                    try {
                        compile("drop table " + tableName);
                    } catch (SqlException e) {
                        throw new RuntimeException(e);
                    }
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            TableToken sysTableName1 = engine.getTableToken(tableName);

            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                drainWalQueue(walApplyJob);

                if (Os.type == Os.WINDOWS) {
                    // Release WAL writers
                    engine.releaseInactive();
                    drainWalQueue(walApplyJob);
                }

                checkTableFilesExist(sysTableName1, "2022-02-24", "x.d", false);
                checkWalFilesRemoved(sysTableName1);
            }
        });
    }

    @Test
    public void testDropedTableSequencerRecreated() throws Exception {
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

            drainWalQueue();

            TableToken sysTableName1 = engine.getTableToken(tableName);
            engine.getTableSequencerAPI().releaseInactive();
            compile("drop table " + tableName);

            if (Os.type == Os.WINDOWS) {
                // Release WAL writers
                engine.releaseInactive();
            }
            drainWalQueue();

            checkTableFilesExist(sysTableName1, "2022-02-24", "x.d", false);
            checkWalFilesRemoved(sysTableName1);
        });
    }

    @Test
    public void testInsertManyThenDrop() throws Exception {
        assertMemoryLeak(ff, () -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            TableToken sysTableName = engine.getTableToken(tableName);

            compile("drop table " + tableName);
            drainWalQueue();

            refreshTablesInBaseEngine();
            engine.notifyWalTxnCommitted(sysTableName, 10);
            drainWalQueue();

            checkTableFilesExist(sysTableName, "2022-02-24", "x.d", false);
            checkWalFilesRemoved(sysTableName);
        });
    }

    @Test
    public void testQueryNullSymbols() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select " +
                    " cast(case when x % 2 = 0 then null else 'abc' end as symbol) sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    "), index(sym) timestamp(ts) partition by DAY WAL");

            drainWalQueue();

            assertSql(
                    "select * from " + tableName + " where sym != 'abc'",
                    "sym\tts\n" +
                            "\t2022-02-24T00:00:01.000000Z\n" +
                            "\t2022-02-24T00:00:03.000000Z\n"
            );
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
    public void testRenameDropTable() throws Exception {
        String tableName = testName.getMethodName();
        String newTableName = testName.getMethodName() + "_new";

        FilesFacade ff = new TestFilesFacadeImpl() {
            int i = 0;

            @Override
            public int openRW(LPSZ name, long opts) {
                int fd = super.openRW(name, opts);
                if (Chars.contains(name, "2022-02-25") && i++ == 0) {
                    try {
                        compile("drop table " + newTableName);
                    } catch (SqlException e) {
                        throw new RuntimeException(e);
                    }
                }
                return fd;
            }
        };

        assertMemoryLeak(ff, () -> {
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 24 * 60 * 60 * 1000000L) ts " +
                    " from long_sequence(2)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            TableToken table2directoryName = engine.getTableToken(tableName);
            compile("insert into " + tableName + " values (1, 'abc', '2022-02-25')");
            compile("rename table " + tableName + " to " + newTableName);

            TableToken newTabledirectoryName = engine.getTableToken(newTableName);
            Assert.assertEquals(table2directoryName.getDirName(), newTabledirectoryName.getDirName());

            drainWalQueue();

            try {
                assertSql(newTableName, "x\tsym2\tts\n" +
                        "2\tEF\t2022-02-25T00:00:00.000000Z\n");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "does not exist");
            }
        });
    }

    @Test
    public void testRenameNonWalTable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String newTableName = testName.getMethodName() + "_new中";
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY BYPASS WAL"
            );

            drainWalQueue();

            TableToken table2directoryName = engine.getTableToken(tableName);
            compile("rename table " + tableName + " to " + newTableName);
            compile("insert into " + newTableName + "(x, ts) values (100, '2022-02-25')");

            TableToken newTabledirectoryName = engine.getTableToken(newTableName);
            Assert.assertNotEquals(table2directoryName.getDirName(), newTabledirectoryName.getDirName());

            drainWalQueue();

            try (TableWriter writer = getWriter(newTableName)) {
                Assert.assertEquals(newTableName, writer.getTableToken().getTableName());
            }

            assertSql(newTableName, "x\tsym2\tts\n" +
                    "1\tDE\t2022-02-24T00:00:00.000000Z\n" +
                    "100\t\t2022-02-25T00:00:00.000000Z\n");

            assertSql("select name from tables() order by name", "name\n" +
                    newTableName + "\n");

            for (int i = 0; i < 2; i++) {
                engine.releaseInactive();
                refreshTablesInBaseEngine();

                TableToken newTabledirectoryName2 = engine.getTableToken(newTableName);
                Assert.assertEquals(newTabledirectoryName, newTabledirectoryName2);
                assertSql(newTableName, "x\tsym2\tts\n" +
                        "1\tDE\t2022-02-24T00:00:00.000000Z\n" +
                        "100\t\t2022-02-25T00:00:00.000000Z\n");
            }

            assertSql("select name, directoryName from tables() order by name", "name\tdirectoryName\n" +
                    newTableName + "\t" + newTabledirectoryName.getDirName() + "\n");
            assertSql("select table from all_tables()", "table\n" +
                    newTableName + "\n");
            assertSql("select relname from pg_class() order by relname", "relname\npg_class\n" +
                    newTableName + "\n");
        });
    }

    @Test
    public void testRenameTable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String newTableName = testName.getMethodName() + "_new中";
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            drainWalQueue();

            TableToken table2directoryName = engine.getTableToken(tableName);
            compile("rename table " + tableName + " to " + newTableName);
            compile("insert into " + newTableName + "(x, ts) values (100, '2022-02-25')");

            TableToken newTabledirectoryName = engine.getTableToken(newTableName);
            Assert.assertEquals(table2directoryName.getDirName(), newTabledirectoryName.getDirName());

            drainWalQueue();

            try (TableWriter writer = getWriter(newTableName)) {
                Assert.assertEquals(newTableName, writer.getTableToken().getTableName());
            }

            assertSql(newTableName, "x\tsym2\tts\n" +
                    "1\tDE\t2022-02-24T00:00:00.000000Z\n" +
                    "100\t\t2022-02-25T00:00:00.000000Z\n");

            assertSql("select name from tables() order by name", "name\n" +
                    newTableName + "\n");

            for (int i = 0; i < 2; i++) {
                engine.releaseInactive();
                refreshTablesInBaseEngine();

                TableToken newTabledirectoryName2 = engine.getTableToken(newTableName);
                Assert.assertEquals(newTabledirectoryName, newTabledirectoryName2);
                assertSql(newTableName, "x\tsym2\tts\n" +
                        "1\tDE\t2022-02-24T00:00:00.000000Z\n" +
                        "100\t\t2022-02-25T00:00:00.000000Z\n");
            }

            assertSql("select name, directoryName from tables() order by name", "name\tdirectoryName\n" +
                    newTableName + "\t" + newTabledirectoryName.getDirName() + "\n");
            assertSql("select table from all_tables()", "table\n" +
                    newTableName + "\n");
            assertSql("select relname from pg_class() order by relname", "relname\npg_class\n" +
                    newTableName + "\n");
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

            try (TableWriter ignore = getWriter(tableName)) {
                drainWalQueue();
                assertSql(tableName, "x\tsym\tts\tsym2\n");
            }

            drainWalQueue();
            executeInsert("insert into " + tableName +
                    " values (102, 'dfd', '2022-02-24T01', 'asd')");

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

    private void checkTableFilesExist(TableToken sysTableName, String partition, String fileName, boolean value) {
        Path sysPath = Path.PATH.get().of(configuration.getRoot()).concat(sysTableName).concat(TXN_FILE_NAME);
        MatcherAssert.assertThat(Chars.toString(sysPath), Files.exists(sysPath.$()), Matchers.is(value));

        sysPath = Path.PATH.get().of(configuration.getRoot()).concat(sysTableName).concat(COLUMN_VERSION_FILE_NAME);
        MatcherAssert.assertThat(Chars.toString(sysPath), Files.exists(sysPath.$()), Matchers.is(value));

        sysPath.of(configuration.getRoot()).concat(sysTableName).concat("sym.c");
        MatcherAssert.assertThat(Chars.toString(sysPath), Files.exists(sysPath.$()), Matchers.is(value));

        sysPath = Path.PATH.get().of(configuration.getRoot()).concat(sysTableName).concat(partition).concat(fileName);
        MatcherAssert.assertThat(Chars.toString(sysPath), Files.exists(sysPath.$()), Matchers.is(value));
    }

    private void checkWalFilesRemoved(TableToken sysTableName) {
        Path sysPath = Path.PATH.get().of(configuration.getRoot()).concat(sysTableName).concat(WalUtils.WAL_NAME_BASE).put(1);
        MatcherAssert.assertThat(Chars.toString(sysPath), Files.exists(sysPath.$()), Matchers.is(true));

        engine.releaseInactiveTableSequencers();
        try (WalPurgeJob job = new WalPurgeJob(engine, configuration.getFilesFacade(), configuration.getMicrosecondClock())) {
            job.run(0);
        }

        sysPath.of(configuration.getRoot()).concat(sysTableName).concat(WalUtils.WAL_NAME_BASE).put(1);
        MatcherAssert.assertThat(Chars.toString(sysPath), Files.exists(sysPath.$()), Matchers.is(false));

        sysPath.of(configuration.getRoot()).concat(sysTableName).concat(WalUtils.SEQ_DIR);
        MatcherAssert.assertThat(Chars.toString(sysPath), Files.exists(sysPath.$()), Matchers.is(false));
    }

    private void testDropFailedWhileDataFileLocked(final String fileName) throws Exception {
        AtomicBoolean latch = new AtomicBoolean();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean remove(LPSZ name) {
                if (Chars.endsWith(name, fileName) && latch.get()) {
                    return false;
                }
                return super.remove(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                drainWalQueue();

                TableToken sysTableName1 = engine.getTableToken(tableName);
                compile("drop table " + tableName);

                latch.set(true);
                drainWalQueue(walApplyJob);

                latch.set(false);
                if (Os.type == Os.WINDOWS) {
                    // Release WAL writers
                    engine.releaseInactive();
                }

                drainWalQueue(walApplyJob);

                checkTableFilesExist(sysTableName1, "2022-02-24", "x.d", false);
                checkWalFilesRemoved(sysTableName1);

                try {
                    compile(tableName);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals(0, e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), "does not exist");
                }
            }
        });
    }
}
