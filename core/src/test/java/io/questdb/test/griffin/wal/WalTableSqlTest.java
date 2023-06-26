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

package io.questdb.test.griffin.wal;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.wal.*;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.mp.Job;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.TableUtils.COLUMN_VERSION_FILE_NAME;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.SEQ_DIR;

@SuppressWarnings("SameParameterValue")
public class WalTableSqlTest extends AbstractGriffinTest {
    @BeforeClass
    public static void setUpStatic() throws Exception {
        walTxnNotificationQueueCapacity = 8;
        AbstractGriffinTest.setUpStatic();
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
                    "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\n" +
                    "103\tdfd\t2022-02-24T01:00:00.000000Z\tasdd\n" +
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
    public void testApplyFromLag() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            Rnd rnd = TestUtils.generateRandom(LOG);
            compile("create table " + tableName + " (" +
                    "x long," +
                    "ts timestamp" +
                    ") timestamp(ts) partition by HOUR WAL WITH maxUncommittedRows=" + rnd.nextInt(20));

            int count = rnd.nextInt(22);
            long rowCount = 0;
            for (int i = 0; i < 2; i++) {
                int rows = rnd.nextInt(200);
                compile("insert into " + tableName +
                        " select x, timestamp_sequence('2022-02-24T0" + i + "', 1000000*60) from long_sequence(" + rows + ")", sqlExecutionContext);
                rowCount += rows;

            }

            // Eject after every transaction
            node1.getConfigurationOverrides().setWalApplyTableTimeQuote(1);

            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                for (int i = 0; i < count; i++) {
                    walApplyJob.run(0);
                    engine.releaseInactive();
                    int rows = rnd.nextInt(200);
                    compile("insert into " + tableName +
                            " select x, timestamp_sequence('2022-02-24T" + String.format("%02d", i + 2) + "', 1000000*60) from long_sequence(" + rows + ")", sqlExecutionContext);
                    rowCount += rows;
                }
            }
            node1.getConfigurationOverrides().setWalApplyTableTimeQuote(Timestamps.MINUTE_MICROS);
            drainWalQueue();

            assertSql("select count(*) from " + tableName, "count\n" + rowCount + "\n");
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
            TableToken sysTableName1 = engine.verifyTableName(tableName);
            compile("drop table " + tableName);

            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            TableToken sysTableName2 = engine.verifyTableName(tableName);
            Assert.assertNotEquals(sysTableName2, sysTableName1);

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
    public void testCreateDropRestartRestart() throws Exception {
        testCreateDropRestartRestart0();
    }

    @Test
    public void testCreateDropRestartRestartNoRegistryCompaction() throws Exception {
        node1.getConfigurationOverrides().setRegistryCompactionThreshold(100);
        testCreateDropRestartRestart0();
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
            TableToken tableToken = engine.verifyTableName(tableName);
            try (
                    WalWriter walWriter1 = engine.getWalWriter(tableToken);
                    WalWriter walWriter2 = engine.getWalWriter(tableToken);
                    WalWriter walWriter3 = engine.getWalWriter(tableToken)
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
                Assert.assertTrue(walWriter1.isDistressed());

                // Structural change
                try {
                    addColumn(walWriter2, "sym3", ColumnType.SYMBOL);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table is dropped ");
                }
                Assert.assertTrue(walWriter2.isDistressed());

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
                Assert.assertTrue(walWriter3.isDistressed());

                compile("create table " + tableName + " as (" +
                        "select x, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts " +
                        " from long_sequence(1)" +
                        ") timestamp(ts) partition by DAY WAL"
                );

                TableToken sysTableName2 = engine.verifyTableName(tableName);
                Assert.assertNotEquals(sysTableName2, tableToken);

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
    public void testCreateWalAndInsertFromSql() throws Exception {
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
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "101\tBC2\t2022-02-24T00:00:00.000000Z\tDE2\n" +
                    "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\n" +
                    "102\tBC2\t2022-02-24T00:00:01.000000Z\tFG2\n" +
                    "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\n" +
                    "103\tBC2\t2022-02-24T00:00:02.000000Z\tDE2\n" +
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
            TableToken table2directoryName = engine.verifyTableName(tableName);
            compile("rename table " + tableName + " to " + newTableName);

            TableToken newTableDirectoryName = engine.verifyTableName(newTableName);
            Assert.assertEquals(table2directoryName.getDirName(), newTableDirectoryName.getDirName());

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
                TableToken sysTableName1 = engine.verifyTableName(tableName);

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
    public void testConvertToWalAfterAlter() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY BYPASS WAL"
            );
            compile("alter table " + tableName + " add col1 int");
            TableToken sysTableName = engine.verifyTableName(tableName);
            compile("alter table " + tableName + " set type wal", sqlExecutionContext);
            engine.releaseInactive();
            ObjList<TableToken> convertedTables = TableConverter.convertTables(configuration, engine.getTableSequencerAPI());
            engine.reloadTableNames(convertedTables);

            compile("alter table " + tableName + " add col2 int");
            compile("insert into " + tableName + "(ts, col1, col2) values('2022-02-24T01', 1, 2)");
            drainWalQueue();

            assertSql("select ts, col1, col2 from " + tableName, "ts\tcol1\tcol2\n" +
                    "2022-02-24T00:00:00.000000Z\tNaN\tNaN\n" +
                    "2022-02-24T01:00:00.000000Z\t1\t2\n");

            compile("alter table " + tableName + " set type bypass wal", sqlExecutionContext);
            engine.releaseInactive();
            convertedTables = TableConverter.convertTables(configuration, engine.getTableSequencerAPI());
            engine.reloadTableNames(convertedTables);

            compile("alter table " + tableName + " drop column col2");
            compile("alter table " + tableName + " add col3 int");
            compile("insert into " + tableName + "(ts, col1, col3) values('2022-02-24T01', 3, 4)");

            assertSql("select ts, col1, col3 from " + tableName, "ts\tcol1\tcol3\n" +
                    "2022-02-24T00:00:00.000000Z\tNaN\tNaN\n" +
                    "2022-02-24T01:00:00.000000Z\t1\tNaN\n" +
                    "2022-02-24T01:00:00.000000Z\t3\t4\n");
        });
    }

    @Test
    public void testDropTableAndConvertAnother() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            TableToken sysTableName1 = engine.verifyTableName(tableName);

            compile("create table " + tableName + "2 as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY"
            );
            TableToken sysTableName2 = engine.verifyTableName(tableName + "2");

            compile("alter table " + tableName + "2 set type wal", sqlExecutionContext);
            compile("drop table " + tableName);
            engine.releaseInactive();

            final ObjList<TableToken> convertedTables = new ObjList<>();
            convertedTables.add(sysTableName2);
            engine.reloadTableNames(convertedTables);

            drainWalQueue();
            runWalPurgeJob();

            engine.releaseInactive();

            drainWalQueue();
            runWalPurgeJob();

            checkTableFilesExist(sysTableName1, "2022-02-24", "sym.d", false);
        });
    }

    @Test
    public void testDropTableHalfDeleted() throws Exception {
        AtomicReference<String> pretendNotExist = new AtomicReference<>("<>");
        FilesFacade ff = new TestFilesFacadeImpl() {
            int count = 0;

            @Override
            public boolean exists(LPSZ name) {
                if (Chars.startsWith(name, pretendNotExist.get()) && count++ == 0) {
                    return false;
                }
                return super.exists(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            TableToken sysTableName1 = engine.verifyTableName(tableName);
            compile("drop table " + tableName);

            pretendNotExist.set(Path.getThreadLocal(root).concat(sysTableName1).toString());
            engine.reloadTableNames();

            runWalPurgeJob();

            engine.reloadTableNames();

            drainWalQueue();
            runWalPurgeJob();

            checkTableFilesExist(sysTableName1, "2022-02-24", "sym.d", false);
        });
    }

    @Test
    public void testDropTableLeftSomeFilesOnDisk() throws Exception {
        AtomicReference<String> pretendNotExist = new AtomicReference<>("<>");
        FilesFacade ff = new TestFilesFacadeImpl() {
            int count = 0;

            @Override
            public int rmdir(Path path) {
                if (Chars.equals(path, pretendNotExist.get()) && count++ == 0) {
                    super.rmdir(Path.getThreadLocal(pretendNotExist.get()).concat(SEQ_DIR).$());
                    return -1;
                }
                return super.rmdir(path);
            }
        };

        assertMemoryLeak(ff, () -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            TableToken sysTableName1 = engine.verifyTableName(tableName);
            compile("drop table " + tableName);

            pretendNotExist.set(Path.getThreadLocal(root).concat(sysTableName1).slash$().toString());
            engine.reloadTableNames();

            runWalPurgeJob();

            drainWalQueue();
            runWalPurgeJob();

            checkTableFilesExist(sysTableName1, "2022-02-24", "sym.d", false);
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

                TableToken table2directoryName = engine.verifyTableName(tableName + "2");
                compile("drop table " + tableName + "2");

                drainWalQueue(walApplyJob);

                checkTableFilesExist(table2directoryName, "2022-02-24", "x.d", false);
            }
        });
    }

    @Test
    public void testDroppedTableHappendIntheMiddleOfWalApplication() throws Exception {
        String tableName = testName.getMethodName();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int openRO(LPSZ name) {
                if (Chars.endsWith(name, Files.SEPARATOR + "0" + Files.SEPARATOR + "sym.d")) {
                    TestUtils.unchecked(() -> compile("drop table " + tableName));
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
            TableToken sysTableName1 = engine.verifyTableName(tableName);

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
    public void testDroppedTableSequencerRecreated() throws Exception {
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

            TableToken sysTableName1 = engine.verifyTableName(tableName);
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
    public void testDroppedTableTriggersWalCheckJob() throws Exception {
        assertMemoryLeak(ff, () -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            compile("drop table " + tableName);

            engine.reloadTableNames();

            long walNotification = engine.getMessageBus().getWalTxnNotificationPubSequence().current();
            CheckWalTransactionsJob checkWalTransactionsJob = new CheckWalTransactionsJob(engine);
            checkWalTransactionsJob.run(0);

            drainWalQueue();
            Assert.assertTrue(walNotification < engine.getMessageBus().getWalTxnNotificationPubSequence().current());
        });
    }

    @Test
    public void testEmptyTruncate() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
                    "A INT," +
                    "ts TIMESTAMP)" +
                    " timestamp(ts) partition by DAY WAL");

            compile("truncate table " + tableName);

            drainWalQueue();

            assertSql("wal_tables()", "name\tsuspended\twriterTxn\tsequencerTxn\n" +
                    "testEmptyTruncate\tfalse\t1\t1\n");
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
            TableToken sysTableName = engine.verifyTableName(tableName);

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
    public void testNoLagUsedWhenDataIsInOrder() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "str string," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            // In order
            compile("insert into " + tableName + " values (101, 'a1a1', 'str-1', '2022-02-24T00', 'a2a2')");
            compile("insert into " + tableName + " values (101, 'a1a1', 'str-1', '2022-02-24T01', 'a2a2')");
            compile("insert into " + tableName + " values (101, 'a1a1', 'str-1', '2022-02-24T02', 'a2a2')");
            compile("insert into " + tableName + " values (101, 'a1a1', 'str-1', '2022-02-24T03', 'a2a2')");
            compile("insert into " + tableName + " values (101, 'a1a1', 'str-1', '2022-02-24T02', 'a2a2')");


            node1.getConfigurationOverrides().setWalApplyTableTimeQuote(0);
            runApplyOnce();

            TableToken token = engine.verifyTableName(tableName);
            try (TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade())) {
                txReader.ofRO(Path.getThreadLocal(root).concat(token).concat(TXN_FILE_NAME).$(), PartitionBy.DAY);
                txReader.unsafeLoadAll();

                Assert.assertEquals(0, txReader.getLagTxnCount());
                Assert.assertEquals(0, txReader.getLagRowCount());
                Assert.assertEquals("2022-02-24T00:00:00.000Z", Timestamps.toString(txReader.getMaxTimestamp()));

                runApplyOnce();
                txReader.unsafeLoadAll();

                Assert.assertEquals(0, txReader.getLagTxnCount());
                Assert.assertEquals(0, txReader.getLagRowCount());
                Assert.assertEquals("2022-02-24T01:00:00.000Z", Timestamps.toString(txReader.getMaxTimestamp()));

                runApplyOnce();
                txReader.unsafeLoadAll();

                Assert.assertEquals(0, txReader.getLagTxnCount());
                Assert.assertEquals(0, txReader.getLagRowCount());
                Assert.assertEquals("2022-02-24T02:00:00.000Z", Timestamps.toString(txReader.getMaxTimestamp()));

                runApplyOnce();
                txReader.unsafeLoadAll();

                Assert.assertEquals(1, txReader.getLagTxnCount());
                Assert.assertEquals(1, txReader.getLagRowCount());
                Assert.assertEquals("2022-02-24T02:00:00.000Z", Timestamps.toString(txReader.getMaxTimestamp()));

                runApplyOnce();
                txReader.unsafeLoadAll();

                Assert.assertEquals(0, txReader.getLagTxnCount());
                Assert.assertEquals(0, txReader.getLagRowCount());
                Assert.assertEquals("2022-02-24T03:00:00.000Z", Timestamps.toString(txReader.getMaxTimestamp()));
            }
        });
    }

    @Test
    public void testQueryNullSymbols() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();

            compile("create table temp as (" +
                    "select " +
                    " cast(case when x % 2 = 0 then null else 'abc' end as symbol) sym, " +
                    " x," +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ")");

            compile("create table " + tableName + " as (" +
                    "select * from temp), index(sym) timestamp(ts) partition by DAY WAL");


            drainWalQueue();

            String allRows = "sym\tx\tts\n" +
                    "abc\t1\t2022-02-24T00:00:00.000000Z\n" +
                    "\t2\t2022-02-24T00:00:01.000000Z\n" +
                    "abc\t3\t2022-02-24T00:00:02.000000Z\n" +
                    "\t4\t2022-02-24T00:00:03.000000Z\n" +
                    "abc\t5\t2022-02-24T00:00:04.000000Z\n";

            assertSql("temp", allRows);
            assertSql(tableName, allRows);

            assertSql(
                    "select * from " + tableName + " where sym != 'abc'",
                    "sym\tx\tts\n" +
                            "\t2\t2022-02-24T00:00:01.000000Z\n" +
                            "\t4\t2022-02-24T00:00:03.000000Z\n"
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
                    TestUtils.unchecked(() -> compile("drop table " + newTableName));
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

            TableToken table2directoryName = engine.verifyTableName(tableName);
            compile("insert into " + tableName + " values (1, 'abc', '2022-02-25')");
            compile("rename table " + tableName + " to " + newTableName);

            TableToken newTableDirectoryName = engine.verifyTableName(newTableName);
            Assert.assertEquals(table2directoryName.getDirName(), newTableDirectoryName.getDirName());

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

            TableToken table2directoryName = engine.verifyTableName(tableName);
            compile("rename table " + tableName + " to " + newTableName);
            compile("insert into " + newTableName + "(x, ts) values (100, '2022-02-25')");

            TableToken newTabledirectoryName = engine.verifyTableName(newTableName);
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

                TableToken newTabledirectoryName2 = engine.verifyTableName(newTableName);
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

            TableToken table2directoryName = engine.verifyTableName(tableName);
            compile("rename table " + tableName + " to " + newTableName);
            compile("insert into " + newTableName + "(x, ts) values (100, '2022-02-25')");

            TableToken newTableDirectoryName = engine.verifyTableName(newTableName);
            Assert.assertEquals(table2directoryName.getDirName(), newTableDirectoryName.getDirName());

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

                TableToken newTabledirectoryName2 = engine.verifyTableName(newTableName);
                Assert.assertEquals(newTableDirectoryName, newTabledirectoryName2);
                assertSql(newTableName, "x\tsym2\tts\n" +
                        "1\tDE\t2022-02-24T00:00:00.000000Z\n" +
                        "100\t\t2022-02-25T00:00:00.000000Z\n");
            }

            assertSql("select name, directoryName from tables() order by name", "name\tdirectoryName\n" +
                    newTableName + "\t" + newTableDirectoryName.getDirName() + "\n");
            assertSql("select table from all_tables()", "table\n" +
                    newTableName + "\n");
            assertSql("select relname from pg_class() order by relname", "relname\npg_class\n" +
                    newTableName + "\n");
        });
    }

    @Test
    public void testRenameTableToCaseInsensitive() throws Exception {
        String tableName = testName.getMethodName();
        String upperCaseName = testName.getMethodName().toUpperCase();
        String newTableName = testName.getMethodName() + "_new";

        assertMemoryLeak(ff, () -> {
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 24 * 60 * 60 * 1000000L) ts " +
                    " from long_sequence(2)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            TableToken table2directoryName = engine.verifyTableName(tableName);
            compile("rename table " + tableName + " to " + upperCaseName);
            compile("insert into " + upperCaseName + " values (1, 'abc', '2022-02-25')");
            compile("insert into " + tableName + " values (1, 'abc', '2022-02-25')");

            TableToken newTableDirectoryName = engine.verifyTableName(upperCaseName);
            Assert.assertEquals(table2directoryName.getDirName(), newTableDirectoryName.getDirName());

            drainWalQueue();

            assertSql("select * from " + upperCaseName, "x\tsym2\tts\n" +
                    "1\tDE\t2022-02-24T00:00:00.000000Z\n" +
                    "2\tEF\t2022-02-25T00:00:00.000000Z\n" +
                    "1\tabc\t2022-02-25T00:00:00.000000Z\n" +
                    "1\tabc\t2022-02-25T00:00:00.000000Z\n");

            compile("rename table " + upperCaseName + " to " + newTableName);

            assertSql("select * from " + newTableName, "x\tsym2\tts\n" +
                    "1\tDE\t2022-02-24T00:00:00.000000Z\n" +
                    "2\tEF\t2022-02-25T00:00:00.000000Z\n" +
                    "1\tabc\t2022-02-25T00:00:00.000000Z\n" +
                    "1\tabc\t2022-02-25T00:00:00.000000Z\n");
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
    public void testSavedDataInTxnFile() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "str string," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            // In order
            compile("insert into " + tableName + " values (101, 'a1a1', 'str-1', '2022-02-24T01', 'a2a2')");

            // Out of order
            compile("insert into " + tableName + " values (101, 'a1a1', 'str-1', '2022-02-24T00', 'a2a2')");

            // In order
            compile("insert into " + tableName + " values (101, 'a1a1', 'str-1', '2022-02-24T02', 'a2a2')");


            node1.getConfigurationOverrides().setWalApplyTableTimeQuote(0);
            runApplyOnce();

            TableToken token = engine.verifyTableName(tableName);
            try (TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade())) {
                txReader.ofRO(Path.getThreadLocal(root).concat(token).concat(TXN_FILE_NAME).$(), PartitionBy.DAY);
                txReader.unsafeLoadAll();

                Assert.assertEquals(1, txReader.getLagTxnCount());
                Assert.assertEquals(1, txReader.getLagRowCount());
                Assert.assertTrue(txReader.isLagOrdered());
                Assert.assertEquals("2022-02-24T01:00:00.000Z", Timestamps.toString(txReader.getLagMinTimestamp()));
                Assert.assertEquals("2022-02-24T01:00:00.000Z", Timestamps.toString(txReader.getLagMaxTimestamp()));

                runApplyOnce();

                txReader.unsafeLoadAll();

                Assert.assertEquals(2, txReader.getLagTxnCount());
                Assert.assertEquals(2, txReader.getLagRowCount());
                Assert.assertFalse(txReader.isLagOrdered());
                Assert.assertEquals("2022-02-24T00:00:00.000Z", Timestamps.toString(txReader.getLagMinTimestamp()));
                Assert.assertEquals("2022-02-24T01:00:00.000Z", Timestamps.toString(txReader.getLagMaxTimestamp()));
            }
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

    @Test
    public void testWhenApplyJobTerminatesEarlierLagCommitted() throws Exception {
        AtomicBoolean isTerminating = new AtomicBoolean();
        Job.RunStatus runStatus = isTerminating::get;

        FilesFacade ff = new TestFilesFacadeImpl() {
            // terminate WAL apply Job as soon as first wal segment is opened.
            @Override
            public int openRO(LPSZ name) {
                if (Chars.contains(name, Files.SEPARATOR + "wal1" + Files.SEPARATOR + "0" + Files.SEPARATOR + "x.d")) {
                    isTerminating.set(true);
                }
                return super.openRO(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            String tableName = testName.getMethodName() + "_लаблअца";
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24T02', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL");

            executeInsert("insert into " + tableName +
                    " values (101, 'dfd', '2022-02-24T01:01', 'asd')");

            executeInsert("insert into " + tableName +
                    " values (102, 'dfd', '2022-02-24T01:02', 'asd')");

            executeInsert("insert into " + tableName +
                    " values (103, 'dfd', '2022-02-24T01:03', 'asd')");

            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                walApplyJob.run(0, runStatus);

                engine.releaseInactive();

                isTerminating.set(false);

                //noinspection StatementWithEmptyBody
                while (walApplyJob.run(0, runStatus)) ;

                engine.releaseInactive();

                assertSql(tableName, "x\tsym\tts\tsym2\n" +
                        "101\tdfd\t2022-02-24T01:01:00.000000Z\tasd\n" +
                        "102\tdfd\t2022-02-24T01:02:00.000000Z\tasd\n" +
                        "103\tdfd\t2022-02-24T01:03:00.000000Z\tasd\n" +
                        "1\tAB\t2022-02-24T02:00:00.000000Z\tEF\n");
            }
        });
    }

    private void checkTableFilesExist(TableToken sysTableName, String partition, String fileName, boolean value) {
        Path sysPath = Path.PATH.get().of(configuration.getRoot()).concat(sysTableName).concat(TXN_FILE_NAME);
        Assert.assertEquals(Chars.toString(sysPath), value, Files.exists(sysPath.$()));

        sysPath = Path.PATH.get().of(configuration.getRoot()).concat(sysTableName).concat(COLUMN_VERSION_FILE_NAME);
        Assert.assertEquals(Chars.toString(sysPath), value, Files.exists(sysPath.$()));

        sysPath.of(configuration.getRoot()).concat(sysTableName).concat("sym.c");
        Assert.assertEquals(Chars.toString(sysPath), value, Files.exists(sysPath.$()));

        sysPath = Path.PATH.get().of(configuration.getRoot()).concat(sysTableName).concat(partition).concat(fileName);
        Assert.assertEquals(Chars.toString(sysPath), value, Files.exists(sysPath.$()));
    }

    private void checkWalFilesRemoved(TableToken sysTableName) {
        Path sysPath = Path.PATH.get().of(configuration.getRoot()).concat(sysTableName).concat(WalUtils.WAL_NAME_BASE).put(1);
        Assert.assertTrue(Chars.toString(sysPath), Files.exists(sysPath.$()));

        engine.releaseInactiveTableSequencers();
        try (WalPurgeJob job = new WalPurgeJob(engine, configuration.getFilesFacade(), configuration.getMicrosecondClock())) {
            job.run(0);
        }

        sysPath.of(configuration.getRoot()).concat(sysTableName).concat(WalUtils.WAL_NAME_BASE).put(1);
        Assert.assertFalse(Chars.toString(sysPath), Files.exists(sysPath.$()));

        sysPath.of(configuration.getRoot()).concat(sysTableName).concat(SEQ_DIR);
        Assert.assertFalse(Chars.toString(sysPath), Files.exists(sysPath.$()));
    }

    private void runApplyOnce() {
        try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 1, 1, null)) {
            walApplyJob.run(0);
        }
    }

    private void testCreateDropRestartRestart0() throws Exception {
        assertMemoryLeak(() -> {

            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            compile("create table " + tableName + "2 as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            TableToken sysTableName1 = engine.verifyTableName(tableName);
            TableToken sysTableName2 = engine.verifyTableName(tableName + "2");

            // Fully delete table2
            compile("drop table " + tableName + "2");
            engine.releaseInactive();
            drainWalQueue();
            runWalPurgeJob();
            checkTableFilesExist(sysTableName2, "2022-02-24", "sym.d", false);

            // Mark table1 as deleted
            compile("drop table " + tableName);

            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            engine.reloadTableNames();
            engine.reloadTableNames();
            engine.reloadTableNames();
            engine.reloadTableNames();
            engine.reloadTableNames();

            engine.releaseInactive();

            drainWalQueue();
            runWalPurgeJob();

            checkTableFilesExist(sysTableName1, "2022-02-24", "sym.d", false);
        });
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

                TableToken sysTableName1 = engine.verifyTableName(tableName);
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
