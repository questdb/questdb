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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.mp.Job;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.PropertyKey.CAIRO_WAL_TXN_NOTIFICATION_QUEUE_CAPACITY;
import static io.questdb.cairo.TableUtils.COLUMN_VERSION_FILE_NAME;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.SEQ_DIR;

@SuppressWarnings("SameParameterValue")
public class WalTableSqlTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(CAIRO_WAL_TXN_NOTIFICATION_QUEUE_CAPACITY, 8);
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void test2InsertsAtSameTime() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute(
                    "create table " + tableName + " (" +
                            "x long," +
                            "sym symbol," +
                            "ts timestamp," +
                            "sym2 symbol" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                        " values (101, 'a1a1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
                try (
                        InsertOperation insertOperation = compiledQuery.popInsertOperation();
                        InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
                ) {
                    insertMethod.execute(sqlExecutionContext);

                    CompiledQuery compiledQuery2 = compiler.compile("insert into " + tableName +
                            " values (102, 'bbb', '2022-02-24T02', 'ccc')", sqlExecutionContext);
                    try (InsertOperation insertOperation2 = compiledQuery2.popInsertOperation();
                         InsertMethod insertMethod2 = insertOperation2.createMethod(sqlExecutionContext)) {
                        insertMethod2.execute(sqlExecutionContext);
                        insertMethod2.commit();
                    }
                    insertMethod.commit();
                }
            }

            execute("insert into " + tableName + " values (103, 'dfd', '2022-02-24T01', 'asdd')");

            drainWalQueue();
            assertQueryNoLeakCheck(
                    "x\tsym\tts\tsym2\n" +
                            "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\n" +
                            "103\tdfd\t2022-02-24T01:00:00.000000Z\tasdd\n" +
                            "102\tbbb\t2022-02-24T02:00:00.000000Z\tccc\n",
                    tableName,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testAddColumnWalRollsWalSegment() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute(
                    "create table " + tableName + " (" +
                            "x long," +
                            "sym symbol," +
                            "str string," +
                            "ts timestamp," +
                            "sym2 symbol" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                        " values (101, 'a1a1', 'str-1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
                try (
                        InsertOperation insertOperation = compiledQuery.popInsertOperation();
                        InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
                ) {
                    insertMethod.execute(sqlExecutionContext);
                    insertMethod.execute(sqlExecutionContext);
                    insertMethod.commit();

                    insertMethod.execute(sqlExecutionContext);
                    execute("alter table " + tableName + " add column new_column int");
                    insertMethod.commit();
                }
            }

            execute("insert into " + tableName + " values (103, 'dfd', 'str-2', '2022-02-24T02', 'asdd', 1234)");

            drainWalQueue();
            assertQueryNoLeakCheck(
                    "x\tsym\tstr\tts\tsym2\tnew_column\n" +
                            "101\ta1a1\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\tnull\n" +
                            "101\ta1a1\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\tnull\n" +
                            "101\ta1a1\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\tnull\n" +
                            "103\tdfd\tstr-2\t2022-02-24T02:00:00.000000Z\tasdd\t1234\n",
                    tableName,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testAddFixedSizeColumnBeforeInsertCommit() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                        " values (101, 'a1a1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
                try (
                        InsertOperation insertOperation = compiledQuery.popInsertOperation();
                        InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
                ) {

                    insertMethod.execute(sqlExecutionContext);
                    execute("alter table " + tableName + " add column jjj int");
                    insertMethod.commit();
                }
            }

            execute("insert into " + tableName + " values (103, 'dfd', '2022-02-24T01', 'asdd', 1234)");

            drainWalQueue();
            assertQueryNoLeakCheck(
                    "x\tsym\tts\tsym2\tjjj\n" +
                            "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\tnull\n" +
                            "103\tdfd\t2022-02-24T01:00:00.000000Z\tasdd\t1234\n",
                    tableName,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testAddMultipleWalColumnsBeforeCommit() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                        " values (101, 'a1a1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
                try (
                        InsertOperation insertOperation = compiledQuery.popInsertOperation();
                        InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
                ) {

                    insertMethod.execute(sqlExecutionContext);
                    execute("alter table " + tableName + " add column jjj int");
                    execute("alter table " + tableName + " add column col_str string");
                    execute("alter table " + tableName + " add column col_var varchar");
                    insertMethod.commit();
                }
            }

            execute("insert into " + tableName + " values (103, 'dfd', '2022-02-24T01', 'asdd', 1234, 'sss-value', 'var-val')");

            drainWalQueue();
            assertQueryNoLeakCheck(
                    "x\tsym\tts\tsym2\tjjj\tcol_str\tcol_var\n" +
                            "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\tnull\t\t\n" +
                            "103\tdfd\t2022-02-24T01:00:00.000000Z\tasdd\t1234\tsss-value\tvar-val\n",
                    tableName,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testAddWalColumnAfterCommit() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                        " values (101, 'a1a1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
                try (
                        InsertOperation insertOperation = compiledQuery.popInsertOperation();
                        InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
                ) {
                    insertMethod.execute(sqlExecutionContext);
                    insertMethod.commit();
                    execute("alter table " + tableName + " add column jjj int");
                }
            }

            execute("insert into " + tableName + " values (103, 'dfd', '2022-02-24T01', 'asdd', 1234)");

            drainWalQueue();
            assertQueryNoLeakCheck(
                    "x\tsym\tts\tsym2\tjjj\n" +
                            "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\tnull\n" +
                            "103\tdfd\t2022-02-24T01:00:00.000000Z\tasdd\t1234\n",
                    tableName,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testAddWalTxnsExceedingSequencerChunks() throws Exception {
        assertMemoryLeak(() -> {
            int txnChunk = 64;
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, txnChunk);
            String tableName = testName.getMethodName() + "Â";
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            int n = (int) (2.5 * txnChunk);
            for (int i = 0; i < n; i++) {
                execute("insert into " + tableName + " values (" + i + ", '2022-02-24T01')");
            }
            drainWalQueue();

            assertSql(
                    "count\n" +
                            (n + 1) + "\n",
                    "select count(*) from " + tableName
            );
        });
    }

    @Test
    public void testApplyFromLag() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            Rnd rnd = TestUtils.generateRandom(LOG);
            execute("create table " + tableName + " (" +
                    "x long," +
                    "ts timestamp" +
                    ") timestamp(ts) partition by HOUR WAL WITH maxUncommittedRows=" + rnd.nextInt(20));

            int count = rnd.nextInt(22);
            long rowCount = 0;
            for (int i = 0; i < 2; i++) {
                int rows = rnd.nextInt(200);
                execute("insert into " + tableName +
                        " select x, timestamp_sequence('2022-02-24T0" + i + "', 1000000*60) from long_sequence(" + rows + ")");
                rowCount += rows;

            }

            // Eject after every transaction
            Overrides overrides1 = node1.getConfigurationOverrides();
            overrides1.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 1);

            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                for (int i = 0; i < count; i++) {
                    walApplyJob.run(0);
                    engine.releaseInactive();
                    int rows = rnd.nextInt(200);
                    execute("insert into " + tableName +
                            " select x, timestamp_sequence('2022-02-24T" + String.format("%02d", i + 2) + "', 1000000*60) from long_sequence(" + rows + ")");
                    rowCount += rows;
                }
            }
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, Micros.MINUTE_MICROS);
            drainWalQueue();

            assertSql("count\n" + rowCount + "\n", "select count(*) from " + tableName);
        });
    }

    @Test
    public void testCanApplyTransactionWhenWritingAnotherOne() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute(
                    "create table " + tableName + " (" +
                            "x long," +
                            "sym symbol," +
                            "ts timestamp," +
                            "sym2 symbol" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                        " values (101, 'a1a1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
                try (
                        InsertOperation insertOperation = compiledQuery.popInsertOperation();
                        InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
                ) {
                    // 3 transactions
                    for (int i = 0; i < 3; i++) {
                        insertMethod.execute(sqlExecutionContext);
                        insertMethod.commit();
                    }
                }
            }

            // Add indicator that _event file has more transaction than _event.i.
            // Apply Job should handle inconsistencies between _event and _event.i files
            // as long as both have the committed transactions.
            try (Path path = new Path()) {
                path.concat(configuration.getDbRoot()).concat(engine.verifyTableName(tableName))
                        .concat(WalUtils.WAL_NAME_BASE).put("1").concat("0")
                        .concat(WalUtils.EVENT_FILE_NAME).$();
                FilesFacade ff = engine.getConfiguration().getFilesFacade();
                long fd = TableUtils.openRW(ff, path.$(), LOG, configuration.getWriterFileOpenOpts());
                long intAddr = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
                Unsafe.getUnsafe().putInt(intAddr, 10);
                ff.write(fd, intAddr, 4, 0);
                Unsafe.free(intAddr, 4, MemoryTag.NATIVE_DEFAULT);
                ff.close(fd);
            }
            drainWalQueue();

            assertQueryNoLeakCheck(
                    "x\tsym\tts\tsym2\n" +
                            "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\n" +
                            "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\n" +
                            "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\n",
                    tableName,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testConvertToFromWalWithLagSet() throws Exception {
        String tableName = testName.getMethodName();
        execute("create table " + tableName + " as (" +
                "select x, " +
                " timestamp_sequence('2022-02-24', 1000000L) ts " +
                " from long_sequence(1)" +
                ") timestamp(ts) partition by DAY WAL"
        );
        drainWalQueue();

        TableToken tt = engine.verifyTableName(tableName);
        int timestampType;
        int partitionBy;
        try (TableMetadata m = engine.getTableMetadata(tt)) {
            timestampType = m.getTimestampType();
            partitionBy = m.getPartitionBy();
        }

        try (TxWriter tw = new TxWriter(engine.getConfiguration().getFilesFacade(), engine.getConfiguration())) {
            Path p = Path.getThreadLocal(engine.getConfiguration().getDbRoot()).concat(tt).concat(TXN_FILE_NAME);
            tw.ofRW(p.$(), timestampType, partitionBy);
            tw.setLagTxnCount(1);
            tw.setLagRowCount(1000);
            tw.setLagMinTimestamp(1_000_000L);
            tw.setLagMaxTimestamp(100_000_000L);
            tw.commit(new ObjList<>());
        }

        execute("alter table " + tableName + " set type bypass wal");
        engine.releaseInactive();
        engine.load();

        try (TxWriter tw = new TxWriter(engine.getConfiguration().getFilesFacade(), engine.getConfiguration())) {
            Path p = Path.getThreadLocal(engine.getConfiguration().getDbRoot()).concat(tt).concat(TXN_FILE_NAME);
            tw.ofRW(p.$(), timestampType, partitionBy);
            Assert.assertEquals(0, tw.getLagRowCount());
            Assert.assertEquals(0, tw.getLagTxnCount());
            Assert.assertEquals(Long.MAX_VALUE, tw.getLagMinTimestamp());
            Assert.assertEquals(Long.MIN_VALUE, tw.getLagMaxTimestamp());
            // Mess again

            tw.setLagTxnCount(1);
            tw.setLagRowCount(1000);
            tw.setLagMinTimestamp(1_000_000L);
            tw.setLagMaxTimestamp(100_000_000L);
            tw.commit(new ObjList<>());
        }

        execute("alter table " + tableName + " set type wal", sqlExecutionContext);
        engine.releaseInactive();
        engine.load();

        try (TxWriter tw = new TxWriter(engine.getConfiguration().getFilesFacade(), engine.getConfiguration())) {
            Path p = Path.getThreadLocal(engine.getConfiguration().getDbRoot()).concat(tt).concat(TXN_FILE_NAME);
            tw.ofRW(p.$(), timestampType, partitionBy);
            Assert.assertEquals(0, tw.getLagRowCount());
            Assert.assertEquals(0, tw.getLagTxnCount());
            Assert.assertEquals(Long.MAX_VALUE, tw.getLagMinTimestamp());
            Assert.assertEquals(Long.MIN_VALUE, tw.getLagMaxTimestamp());
        }
    }

    @Test
    public void testConvertToWalAfterAlter() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute(
                    "create table " + tableName + " as (" +
                            "select x, " +
                            " rnd_symbol('AB', 'BC', 'CD') sym, " +
                            " timestamp_sequence('2022-02-24', 1000000L) ts " +
                            " from long_sequence(1)" +
                            ") timestamp(ts) partition by DAY BYPASS WAL"
            );
            execute("alter table " + tableName + " add col1 int");
            execute("alter table " + tableName + " set type wal", sqlExecutionContext);
            engine.releaseInactive();
            engine.load();

            execute("alter table " + tableName + " add col2 int");
            execute("insert into " + tableName + "(ts, col1, col2) values('2022-02-24T01', 1, 2)");
            drainWalQueue();

            assertQueryNoLeakCheck(
                    "ts\tcol1\tcol2\n" +
                            "2022-02-24T00:00:00.000000Z\tnull\tnull\n" +
                            "2022-02-24T01:00:00.000000Z\t1\t2\n",
                    "select ts, col1, col2 from " + tableName,
                    "ts",
                    true,
                    true
            );

            execute("alter table " + tableName + " set type bypass wal");
            engine.releaseInactive();
            engine.load();

            execute("alter table " + tableName + " drop column col2");
            execute("alter table " + tableName + " add col3 int");
            execute("insert into " + tableName + "(ts, col1, col3) values('2022-02-24T01', 3, 4)");

            assertQueryNoLeakCheck(
                    "ts\tcol1\tcol3\n" +
                            "2022-02-24T00:00:00.000000Z\tnull\tnull\n" +
                            "2022-02-24T01:00:00.000000Z\t1\tnull\n" +
                            "2022-02-24T01:00:00.000000Z\t3\t4\n",
                    "select ts, col1, col3 from " + tableName,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testCreateDropCreate() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute(
                    "create table " + tableName + " as (" +
                            "select x, " +
                            " rnd_symbol('AB', 'BC', 'CD') sym, " +
                            " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                            " timestamp_sequence('2022-02-24', 1000000L) ts " +
                            " from long_sequence(1)" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            TableToken sysTableName1 = engine.verifyTableName(tableName);
            execute("drop table " + tableName);

            execute("create table " + tableName + " as (" +
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

            assertQueryNoLeakCheck(
                    "x\tts\n" +
                            "1\t2022-02-24T00:00:00.000000Z\n",
                    tableName,
                    "ts",
                    true,
                    true
            );
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

            execute(createSql + " WAL");
            execute("drop table " + tableName);

            SharedRandom.RANDOM.get().reset();
            execute(createSql);
            assertQueryNoLeakCheck(
                    "x\tsym\tsym2\tts\n" +
                            "1\tAB\tEF\t2022-02-24T00:00:00.000000Z\n",
                    tableName,
                    "ts",
                    true,
                    true
            );

            execute("drop table " + tableName);
            SharedRandom.RANDOM.get().reset();
            execute(createSql + " WAL");
            drainWalQueue();
            assertQueryNoLeakCheck(
                    "x\tsym\tsym2\tts\n" +
                            "1\tAB\tEF\t2022-02-24T00:00:00.000000Z\n",
                    tableName,
                    "ts",
                    true,
                    true
            );

            execute("drop table " + tableName);
            SharedRandom.RANDOM.get().reset();
            execute(createSql);
            assertQueryNoLeakCheck(
                    "x\tsym\tsym2\tts\n" +
                            "1\tAB\tEF\t2022-02-24T00:00:00.000000Z\n",
                    tableName,
                    "ts",
                    true,
                    true
            );

            execute("drop table " + tableName);
            SharedRandom.RANDOM.get().reset();
            execute(createSql + " WAL");
            drainWalQueue();
            assertQueryNoLeakCheck(
                    "x\tsym\tsym2\tts\n" +
                            "1\tAB\tEF\t2022-02-24T00:00:00.000000Z\n",
                    tableName,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testCreateDropRestartRestart() throws Exception {
        testCreateDropRestartRestart0();
    }

    @Test
    public void testCreateDropRestartRestartNoRegistryCompaction() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_TABLE_REGISTRY_COMPACTION_THRESHOLD, 100);
        testCreateDropRestartRestart0();
    }

    @Test
    public void testCreateDropWalReuseCreate() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName() + "Â";
            execute("create table " + tableName + " as (" +
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
                execute("insert into " + tableName + " values(1, 'A', 'B', '2022-02-24T01')");

                execute("drop table " + tableName);
                try {
                    assertExceptionNoLeakCheck("insert into " + tableName + " values(1, 'A', 'B', '2022-02-24T01')");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "able does not exist");
                }

                TableWriter.Row row = walWriter1.newRow(MicrosTimestampDriver.floor("2022-02-24T01"));
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
                    dropPartition.addPartitionToList(MicrosTimestampDriver.floor("2022-02-24"), 0);
                    AlterOperation dropAlter = dropPartition.build();
                    dropAlter.withContext(sqlExecutionContext);
                    dropAlter.withSqlStatement("alter table " + tableName + " drop partition list '2022-02-24'");
                    walWriter3.apply(dropAlter, true);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table is dropped ");
                }
                Assert.assertTrue(walWriter3.isDistressed());

                execute("create table " + tableName + " as (" +
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

            assertQueryNoLeakCheck(
                    "x\tts\n" +
                            "1\t2022-02-24T00:00:00.000000Z\n",
                    tableName,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testCreateWalAndInsertFromSql() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName() + "_लаблअца";
            execute(
                    "create table " + tableName + " as (" +
                            "select x, " +
                            " rnd_symbol('AB', 'BC', 'CD') sym, " +
                            " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                            " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                            " from long_sequence(5)" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            execute(
                    "insert into " + tableName +
                            " values (101, 'dfd', '2022-02-24T01', 'asd')"
            );

            drainWalQueue();

            assertQueryNoLeakCheck(
                    "x\tsym\tts\tsym2\n" +
                            "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                            "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\n" +
                            "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\n" +
                            "4\tCD\t2022-02-24T00:00:03.000000Z\tFG\n" +
                            "5\tAB\t2022-02-24T00:00:04.000000Z\tDE\n" +
                            "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\n",
                    tableName,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testCreateWalDropColumnInsert() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            execute("alter table " + tableName + " drop column sym");
            execute("insert into " + tableName + "(x, ts) values (2, '2022-02-24T23:00:01')");
            drainWalQueue();

            assertQueryNoLeakCheck(
                    "x\tsym2\tts\n" +
                            "1\tEF\t2022-02-24T00:00:00.000000Z\n" +
                            "2\t\t2022-02-24T23:00:01.000000Z\n",
                    tableName,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testCreateWalTableAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            drainWalQueue();

            assertQueryNoLeakCheck(
                    "x\tsym\tts\tsym2\n" +
                            "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                            "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\n" +
                            "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\n" +
                            "4\tCD\t2022-02-24T00:00:03.000000Z\tFG\n" +
                            "5\tAB\t2022-02-24T00:00:04.000000Z\tDE\n",
                    tableName,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testCreateWalTableAsSelectAndInsertAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            execute("insert into " + tableName +
                    " select x + 100, rnd_symbol('AB2', 'BC2', 'CD2') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE2', null, 'EF2', 'FG2') sym2 " +
                    " from long_sequence(3)");

            drainWalQueue();

            assertQueryNoLeakCheck(
                    "x\tsym\tts\tsym2\n" +
                            "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                            "101\tBC2\t2022-02-24T00:00:00.000000Z\tDE2\n" +
                            "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\n" +
                            "102\tBC2\t2022-02-24T00:00:01.000000Z\tFG2\n" +
                            "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\n" +
                            "103\tBC2\t2022-02-24T00:00:02.000000Z\tDE2\n" +
                            "4\tCD\t2022-02-24T00:00:03.000000Z\tFG\n" +
                            "5\tAB\t2022-02-24T00:00:04.000000Z\tDE\n",
                    tableName,
                    "ts",
                    true,
                    true
            );
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
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 24 * 60 * 60 * 1000000L) ts " +
                    " from long_sequence(2)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            execute("alter table " + tableName + " drop partition list '2022-02-24'");
            TableToken table2directoryName = engine.verifyTableName(tableName);
            execute("rename table " + tableName + " to " + newTableName);

            TableToken newTableDirectoryName = engine.verifyTableName(newTableName);
            Assert.assertEquals(table2directoryName.getDirName(), newTableDirectoryName.getDirName());

            drainWalQueue();

            assertQueryNoLeakCheck(
                    "x\tsym2\tts\n" +
                            "2\tEF\t2022-02-25T00:00:00.000000Z\n",
                    newTableName,
                    "ts",
                    true,
                    true
            );
            Assert.assertEquals(3, engine.getTableSequencerAPI().getTxnTracker(newTableDirectoryName).getWriterTxn());
            Assert.assertEquals(3, engine.getTableSequencerAPI().getTxnTracker(newTableDirectoryName).getSeqTxn());
        });
    }

    @Test
    public void testDropRemovesFromCatalogFunctions() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String tableNameNonWal = testName.getMethodName() + "_non_wal";

            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            execute("create table " + tableNameNonWal + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY BYPASS WAL"
            );

            assertQueryNoLeakCheck(
                    "table_name\n" +
                            tableName + "\n" +
                            tableNameNonWal + "\n",
                    "all_tables() order by table_name",
                    null,
                    true
            );
            assertQueryNoLeakCheck(
                    "table_name\n" +
                            tableName + "\n" +
                            tableNameNonWal + "\n",
                    "select table_name from tables() order by table_name",
                    null,
                    true
            );
            assertQueryNoLeakCheck(
                    "relname\npg_class\n" +
                            tableName + "\n" +
                            tableNameNonWal + "\n",
                    "select relname from pg_class() order by relname",
                    null,
                    true
            );

            execute("drop table " + tableName);

            assertQueryNoLeakCheck(
                    "table_name\n" +
                            tableNameNonWal + "\n",
                    "all_tables() order by table_name",
                    null,
                    true
            );
            assertQueryNoLeakCheck(
                    "table_name\n" +
                            tableNameNonWal + "\n",
                    "select table_name from tables() order by table_name",
                    null,
                    true
            );
            assertQueryNoLeakCheck(
                    "relname\npg_class\n" +
                            tableNameNonWal + "\n",
                    "select relname from pg_class() order by relname",
                    null,
                    true
            );

            drainWalQueue();

            assertQueryNoLeakCheck(
                    "table_name\n" +
                            tableNameNonWal + "\n",
                    "all_tables() order by table_name",
                    null,
                    true
            );
            assertQueryNoLeakCheck(
                    "table_name\n" +
                            tableNameNonWal + "\n",
                    "select table_name from tables() order by table_name",
                    null,
                    true
            );
            assertQueryNoLeakCheck(
                    "relname\npg_class\n" +
                            tableNameNonWal + "\n",
                    "select relname from pg_class() order by relname",
                    null,
                    true
            );

            refreshTablesInBaseEngine();

            assertQueryNoLeakCheck(
                    "table_name\n" +
                            tableNameNonWal + "\n",
                    "all_tables() order by table_name",
                    null,
                    true
            );
            assertQueryNoLeakCheck(
                    "table_name\n" +
                            tableNameNonWal + "\n",
                    "select table_name from tables() order by table_name",
                    null,
                    true
            );
            assertQueryNoLeakCheck(
                    "relname\npg_class\n" +
                            tableNameNonWal + "\n",
                    "select relname from pg_class() order by relname",
                    null,
                    true
            );

            execute("drop table " + tableNameNonWal);

            assertQueryNoLeakCheck("table_name\n", "all_tables() order by table_name", null, true);
            assertQueryNoLeakCheck("table_name\n", "select table_name from tables() order by table_name", null, true);
            assertQueryNoLeakCheck("relname\npg_class\n", "select relname from pg_class() order by relname", null, true);
        });
    }

    @Test
    public void testDropRetriedWhenReaderOpen() throws Exception {
        assertMemoryLeak(ff, () -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            drainWalQueue();
            TableToken sysTableName1 = engine.verifyTableName(tableName);

            try (TableReader ignore = sqlExecutionContext.getReader(sysTableName1)) {
                execute("drop table " + tableName);
                drainWalQueue();
                checkTableFilesExist(sysTableName1, "2022-02-24", "x.d", true);
            }

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
    public void testDropSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE 'weather' (\n" +
                            "  city SYMBOL capacity 256,\n" +
                            "  temperature DOUBLE,\n" +
                            "  humidity DOUBLE,\n" +
                            "  timestamp TIMESTAMP,\n" +
                            "  country SYMBOL capacity 256 CACHE\n" +
                            ") timestamp (timestamp) PARTITION BY DAY WAL"
            );

            execute("insert into weather values('city', 1, 1, '1982-01-01', 'abc')");

            drainWalQueue();
            assertQueryNoLeakCheck(
                    "city\ttemperature\thumidity\ttimestamp\tcountry\n" +
                            "city\t1.0\t1.0\t1982-01-01T00:00:00.000000Z\tabc\n",
                    "select * from weather",
                    "timestamp",
                    true,
                    true
            );

            engine.releaseInactive();

            execute("alter table weather drop column city");
            execute("insert into weather values(1, 1, '1982-01-01', 'abc')");
            drainWalQueue();

            engine.releaseInactive();
            execute("alter table weather add column city symbol");
            execute("insert into weather values(1, 1, '1982-01-01', 'abc', 'city')");
            drainWalQueue();

            engine.releaseInactive();
            execute("alter table weather drop column city");
        });
    }

    @Test
    public void testDropTableAndConvertAnother() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            TableToken sysTableName1 = engine.verifyTableName(tableName);

            execute("create table " + tableName + "2 as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY"
            );
            TableToken sysTableName2 = engine.verifyTableName(tableName + "2");

            execute("alter table " + tableName + "2 set type wal", sqlExecutionContext);
            execute("drop table " + tableName);
            engine.releaseInactive();

            final ObjList<TableToken> convertedTables = new ObjList<>();
            convertedTables.add(sysTableName2);
            engine.reloadTableNames(convertedTables);

            drainWalQueue();
            drainPurgeJob();

            engine.releaseInactive();

            drainWalQueue();
            drainPurgeJob();

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
                if (Utf8s.startsWithAscii(name, pretendNotExist.get()) && count++ == 0) {
                    return false;
                }
                return super.exists(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            TableToken sysTableName1 = engine.verifyTableName(tableName);
            execute("drop table " + tableName);

            pretendNotExist.set(Path.getThreadLocal(root).concat(sysTableName1).toString());
            engine.reloadTableNames();

            drainPurgeJob();

            engine.reloadTableNames();

            drainWalQueue();
            drainPurgeJob();

            checkTableFilesExist(sysTableName1, "2022-02-24", "sym.d", false);
        });
    }

    @Test
    public void testDropTableLeftSomeFilesOnDisk() throws Exception {
        AtomicReference<String> pretendNotExist = new AtomicReference<>("<>");
        FilesFacade ff = new TestFilesFacadeImpl() {
            int count = 0;

            @Override
            public boolean rmdir(Path path, boolean lazy) {
                if (Utf8s.equalsAscii(pretendNotExist.get(), path) && count++ == 0) {
                    super.rmdir(Path.getThreadLocal(pretendNotExist.get()).concat(SEQ_DIR));
                    return false;
                }
                return super.rmdir(path, lazy);
            }
        };

        assertMemoryLeak(ff, () -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            TableToken sysTableName1 = engine.verifyTableName(tableName);
            execute("drop table " + tableName);

            pretendNotExist.set(Path.getThreadLocal(root).concat(sysTableName1).slash$().toString());
            engine.reloadTableNames();

            drainPurgeJob();

            drainWalQueue();
            drainPurgeJob();

            checkTableFilesExist(sysTableName1, "2022-02-24", "sym.d", false);
        });
    }

    @Test
    public void testDropTxnNotificationQueueOverflow() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + "1 as (" +
                    "select x, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            execute("create table " + tableName + "2 as (" +
                    "select x, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            drainWalQueue();

            for (int i = 0; i < configuration.getWalTxnNotificationQueueCapacity(); i++) {
                execute("insert into " + tableName + "1 values (101, 'a1a1', '2022-02-24T01')");
            }

            TableToken table2directoryName = engine.verifyTableName(tableName + "2");
            execute("drop table " + tableName + "2");

            drainWalQueue();

            checkTableFilesExist(table2directoryName, "2022-02-24", "x.d", false);
        });
    }

    @Test
    public void testDroppedTableHappendInTheMiddleOfWalApplication() throws Exception {
        String tableName = testName.getMethodName();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "0" + Files.SEPARATOR + "sym.d")) {
                    TestUtils.unchecked(() -> execute("drop table " + tableName));
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            TableToken sysTableName1 = engine.verifyTableName(tableName);

            drainWalQueue();

            if (Os.type == Os.WINDOWS) {
                // Release WAL writers
                engine.releaseInactive();
                drainWalQueue();
            }

            checkTableFilesExist(sysTableName1, "2022-02-24", "x.d", false);
            checkWalFilesRemoved(sysTableName1);
        });
    }

    @Test
    public void testDroppedTableSequencerRecreated() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
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
            execute("drop table " + tableName);

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
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            execute("drop table " + tableName);

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
            execute("create table " + tableName + " (" +
                    "A INT," +
                    "ts TIMESTAMP)" +
                    " timestamp(ts) partition by DAY WAL");

            execute("truncate table " + tableName);

            drainWalQueue();

            assertQueryNoLeakCheck(
                    "name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\n" +
                            "testEmptyTruncate\tfalse\t1\t0\t1\t\t\t0\n",
                    "wal_tables()",
                    null
            );
        });
    }

    @Test
    public void testInsertIntNoTimestampTableAsSelect() throws Exception {
        testInsertAsSelectBatched("");
    }

    @Test
    public void testInsertIntoWalTableAsSelect() throws Exception {
        testInsertAsSelectBatched("timestamp (timestamp) PARTITION BY DAY WAL DEDUP UPSERT KEYS(timestamp, nuuid)");
    }

    @Test
    public void testInsertManyThenDrop() throws Exception {
        assertMemoryLeak(ff, () -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            TableToken tableToken = engine.verifyTableName(tableName);

            execute("drop table " + tableName);
            drainWalQueue();

            engine.reloadTableNames();
            engine.notifyWalTxnCommitted(tableToken);
            drainWalQueue();

            checkTableFilesExist(tableToken, "2022-02-24", "x.d", false);
            checkWalFilesRemoved(tableToken);
        });
    }

    @Test
    public void testNoLagUsedWhenDataIsInOrder() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "str string," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            // In order
            execute("insert into " + tableName + " values (101, 'a1a1', 'str-1', '2022-02-24T00', 'a2a2')");
            execute("insert into " + tableName + " values (101, 'a1a1', 'str-1', '2022-02-24T01', 'a2a2')");
            execute("insert into " + tableName + " values (101, 'a1a1', 'str-1', '2022-02-24T02', 'a2a2')");
            execute("insert into " + tableName + " values (101, 'a1a1', 'str-1', '2022-02-24T03', 'a2a2')");
            // Out of order
            execute("insert into " + tableName + " values (101, 'a1a1', 'str-1', '2022-02-24T02', 'a2a2')");


            node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 0);
            TableToken token = engine.verifyTableName(tableName);
            runApplyOnce(token);

            int timestampType;
            int partitionBy;

            try (TableMetadata m = engine.getTableMetadata(token)) {
                timestampType = m.getTimestampType();
                partitionBy = m.getPartitionBy();
            }

            try (TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade())) {
                txReader.ofRO(Path.getThreadLocal(root).concat(token).concat(TXN_FILE_NAME).$(), timestampType, partitionBy);
                txReader.unsafeLoadAll();

                Assert.assertEquals(0, txReader.getLagTxnCount());
                Assert.assertEquals(0, txReader.getLagRowCount());
                Assert.assertEquals("2022-02-24T00:00:00.000Z", Micros.toString(txReader.getMaxTimestamp()));

                runApplyOnce(token);
                txReader.unsafeLoadAll();

                Assert.assertEquals(0, txReader.getLagTxnCount());
                Assert.assertEquals(0, txReader.getLagRowCount());
                Assert.assertEquals("2022-02-24T01:00:00.000Z", Micros.toString(txReader.getMaxTimestamp()));

                runApplyOnce(token);
                txReader.unsafeLoadAll();

                Assert.assertEquals(0, txReader.getLagTxnCount());
                Assert.assertEquals(0, txReader.getLagRowCount());
                Assert.assertEquals("2022-02-24T02:00:00.000Z", Micros.toString(txReader.getMaxTimestamp()));

                runApplyOnce(token);
                txReader.unsafeLoadAll();

                Assert.assertEquals(1, txReader.getLagTxnCount());
                Assert.assertEquals(1, txReader.getLagRowCount());
                Assert.assertEquals("2022-02-24T02:00:00.000Z", Micros.toString(txReader.getMaxTimestamp()));

                runApplyOnce(token);
                txReader.unsafeLoadAll();

                Assert.assertEquals(0, txReader.getLagTxnCount());
                Assert.assertEquals(0, txReader.getLagRowCount());
                Assert.assertEquals("2022-02-24T03:00:00.000Z", Micros.toString(txReader.getMaxTimestamp()));
            }
        });
    }

    @Test
    public void testQueryNullSymbols() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();

            execute("create table temp as (" +
                    "select " +
                    " cast(case when x % 2 = 0 then null else 'abc' end as symbol) sym, " +
                    " x," +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ")");

            execute("create table " + tableName + " as (" +
                    "select * from temp), index(sym) timestamp(ts) partition by DAY WAL");

            drainWalQueue();

            String allRows = "sym\tx\tts\n" +
                    "abc\t1\t2022-02-24T00:00:00.000000Z\n" +
                    "\t2\t2022-02-24T00:00:01.000000Z\n" +
                    "abc\t3\t2022-02-24T00:00:02.000000Z\n" +
                    "\t4\t2022-02-24T00:00:03.000000Z\n" +
                    "abc\t5\t2022-02-24T00:00:04.000000Z\n";

            assertQueryNoLeakCheck(allRows, "temp", null, true, true);
            assertQueryNoLeakCheck(allRows, tableName, "ts", true, true);

            assertQueryNoLeakCheck(
                    "sym\tx\tts\n" +
                            "\t2\t2022-02-24T00:00:01.000000Z\n" +
                            "\t4\t2022-02-24T00:00:03.000000Z\n",
                    "select * from " + tableName + " where sym != 'abc'",
                    "ts",
                    true
            );
        });
    }

    @Test
    public void testRemoveColumnWalRollsWalSegment() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "str string," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                        " values (101, 'a1a1', 'str-1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
                try (
                        InsertOperation insertOperation = compiledQuery.popInsertOperation();
                        InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
                ) {
                    insertMethod.execute(sqlExecutionContext);
                    insertMethod.execute(sqlExecutionContext);
                    insertMethod.commit();

                    insertMethod.execute(sqlExecutionContext);
                    execute("alter table " + tableName + " drop column sym");
                    insertMethod.commit();
                }
            }

            execute("insert into " + tableName + " values (103, 'str-2', '2022-02-24T02', 'asdd')");

            drainWalQueue();
            assertQueryNoLeakCheck(
                    "x\tstr\tts\tsym2\n" +
                            "101\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\n" +
                            "101\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\n" +
                            "101\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\n" +
                            "103\tstr-2\t2022-02-24T02:00:00.000000Z\tasdd\n",
                    tableName,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testRenameDropTable() throws Exception {
        String tableName = testName.getMethodName();
        String newTableName = testName.getMethodName() + "_new";

        FilesFacade ff = new TestFilesFacadeImpl() {
            int i = 0;

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                if (Utf8s.containsAscii(name, "2022-02-25") && i++ == 0) {
                    TestUtils.unchecked(() -> execute("drop table " + newTableName));
                }
                return fd;
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 24 * 60 * 60 * 1000000L) ts " +
                    " from long_sequence(2)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            TableToken table2directoryName = engine.verifyTableName(tableName);
            execute("insert into " + tableName + " values (1, 'abc', '2022-02-25')");
            execute("rename table " + tableName + " to " + newTableName);

            TableToken newTableDirectoryName = engine.verifyTableName(newTableName);
            Assert.assertEquals(table2directoryName.getDirName(), newTableDirectoryName.getDirName());

            drainWalQueue();

            try {
                assertSql(
                        "x\tsym2\tts\n" +
                                "2\tEF\t2022-02-25T00:00:00.000000Z\n",
                        newTableName
                );
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
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY BYPASS WAL"
            );

            drainWalQueue();

            TableToken table2directoryName = engine.verifyTableName(tableName);
            execute("rename table " + tableName + " to " + newTableName);
            execute("insert into " + newTableName + "(x, ts) values (100, '2022-02-25')");

            TableToken newTabledirectoryName = engine.verifyTableName(newTableName);
            Assert.assertNotEquals(table2directoryName.getDirName(), newTabledirectoryName.getDirName());

            drainWalQueue();

            try (TableWriter writer = getWriter(newTableName)) {
                Assert.assertEquals(newTableName, writer.getTableToken().getTableName());
            }

            assertQueryNoLeakCheck(
                    "x\tsym2\tts\n" +
                            "1\tDE\t2022-02-24T00:00:00.000000Z\n" +
                            "100\t\t2022-02-25T00:00:00.000000Z\n",
                    newTableName,
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    "table_name\n" +
                            newTableName + "\n",
                    "select table_name from tables() order by table_name",
                    null,
                    true
            );

            for (int i = 0; i < 2; i++) {
                engine.releaseInactive();
                refreshTablesInBaseEngine();

                TableToken newTableDirectoryName2 = engine.verifyTableName(newTableName);
                Assert.assertEquals(newTabledirectoryName, newTableDirectoryName2);
                assertQueryNoLeakCheck(
                        "x\tsym2\tts\n" +
                                "1\tDE\t2022-02-24T00:00:00.000000Z\n" +
                                "100\t\t2022-02-25T00:00:00.000000Z\n",
                        newTableName,
                        "ts",
                        true,
                        true
                );
            }

            assertQueryNoLeakCheck(
                    "table_name\tdirectoryName\n" +
                            newTableName + "\t" + newTabledirectoryName.getDirName() + "\n",
                    "select table_name, directoryName from tables() order by table_name",
                    null,
                    true
            );
            assertQueryNoLeakCheck(
                    "table_name\n" +
                            newTableName + "\n",
                    "select table_name from all_tables()",
                    null
            );
            assertQueryNoLeakCheck(
                    "relname\npg_class\n" +
                            newTableName + "\n",
                    "select relname from pg_class() order by relname",
                    null,
                    true
            );
        });
    }

    @Test
    public void testRenameTable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String newTableName = testName.getMethodName() + "_new中";
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            drainWalQueue();

            TableToken table2directoryName = engine.verifyTableName(tableName);
            execute("rename table " + tableName + " to " + newTableName);
            execute("insert into " + newTableName + "(x, ts) values (100, '2022-02-25')");

            TableToken newTableDirectoryName = engine.verifyTableName(newTableName);
            Assert.assertEquals(table2directoryName.getDirName(), newTableDirectoryName.getDirName());

            drainWalQueue();

            try (TableWriter writer = getWriter(newTableName)) {
                Assert.assertEquals(newTableName, writer.getTableToken().getTableName());
            }

            assertQueryNoLeakCheck(
                    "x\tsym2\tts\n" +
                            "1\tDE\t2022-02-24T00:00:00.000000Z\n" +
                            "100\t\t2022-02-25T00:00:00.000000Z\n",
                    newTableName,
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    "table_name\n" +
                            newTableName + "\n",
                    "select table_name from tables() order by table_name",
                    null,
                    true
            );

            for (int i = 0; i < 2; i++) {
                engine.releaseInactive();
                refreshTablesInBaseEngine();

                TableToken newTableDirectoryName2 = engine.verifyTableName(newTableName);
                Assert.assertEquals(newTableDirectoryName, newTableDirectoryName2);
                assertQueryNoLeakCheck(
                        "x\tsym2\tts\n" +
                                "1\tDE\t2022-02-24T00:00:00.000000Z\n" +
                                "100\t\t2022-02-25T00:00:00.000000Z\n",
                        newTableName,
                        "ts",
                        true,
                        true
                );
            }

            assertQueryNoLeakCheck(
                    "table_name\tdirectoryName\n" +
                            newTableName + "\t" + newTableDirectoryName.getDirName() + "\n",
                    "select table_name, directoryName from tables() order by table_name",
                    null,
                    true
            );
            assertQueryNoLeakCheck(
                    "table_name\n" +
                            newTableName + "\n",
                    "select table_name from all_tables()",
                    null
            );
            assertQueryNoLeakCheck(
                    "relname\npg_class\n" +
                            newTableName + "\n",
                    "select relname from pg_class() order by relname",
                    null,
                    true
            );
        });
    }

    @Test
    public void testRenameTableToCaseInsensitive() throws Exception {
        String tableName = testName.getMethodName();
        String upperCaseName = testName.getMethodName().toUpperCase();
        String newTableName = testName.getMethodName() + "_new";

        assertMemoryLeak(ff, () -> {
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 24 * 60 * 60 * 1000000L) ts " +
                    " from long_sequence(2)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            TableToken table2directoryName = engine.verifyTableName(tableName);
            execute("rename table " + tableName + " to " + upperCaseName);
            execute("insert into " + upperCaseName + " values (1, 'abc', '2022-02-25')");
            execute("insert into " + tableName + " values (1, 'abc', '2022-02-25')");

            TableToken newTableDirectoryName = engine.verifyTableName(upperCaseName);
            Assert.assertEquals(table2directoryName.getDirName(), newTableDirectoryName.getDirName());

            drainWalQueue();

            assertQueryNoLeakCheck(
                    "x\tsym2\tts\n" +
                            "1\tDE\t2022-02-24T00:00:00.000000Z\n" +
                            "2\tEF\t2022-02-25T00:00:00.000000Z\n" +
                            "1\tabc\t2022-02-25T00:00:00.000000Z\n" +
                            "1\tabc\t2022-02-25T00:00:00.000000Z\n",
                    "select * from " + upperCaseName,
                    "ts",
                    true,
                    true
            );

            execute("rename table " + upperCaseName + " to " + newTableName);

            assertQueryNoLeakCheck(
                    "x\tsym2\tts\n" +
                            "1\tDE\t2022-02-24T00:00:00.000000Z\n" +
                            "2\tEF\t2022-02-25T00:00:00.000000Z\n" +
                            "1\tabc\t2022-02-25T00:00:00.000000Z\n" +
                            "1\tabc\t2022-02-25T00:00:00.000000Z\n",
                    "select * from " + newTableName,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testRogueTableWriterBlocksApplyJob() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            execute("insert into " + tableName +
                    " values (101, 'dfd', '2022-02-24T01', 'asd')");

            try (TableWriter ignore = getWriter(tableName)) {
                drainWalQueue();
                assertQueryNoLeakCheck("x\tsym\tts\tsym2\n", tableName, "ts", true, true);
            }

            drainWalQueue();
            execute("insert into " + tableName +
                    " values (102, 'dfd', '2022-02-24T01', 'asd')");

            assertQueryNoLeakCheck(
                    "x\tsym\tts\tsym2\n" +
                            "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                            "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\n" +
                            "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\n" +
                            "4\tCD\t2022-02-24T00:00:03.000000Z\tFG\n" +
                            "5\tAB\t2022-02-24T00:00:04.000000Z\tDE\n" +
                            "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\n",
                    tableName,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testSavedDataInTxnFile() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "str string," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            // In order
            execute("insert into " + tableName + " values (101, 'a1a1', 'str-1', '2022-02-24T01', 'a2a2')");

            // Out of order
            execute("insert into " + tableName + " values (101, 'a1a1', 'str-1', '2022-02-24T00', 'a2a2')");

            // In order
            execute("insert into " + tableName + " values (101, 'a1a1', 'str-1', '2022-02-24T02', 'a2a2')");

            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 0);

            TableToken token = engine.verifyTableName(tableName);
            runApplyOnce(token);

            int timestampType;
            int partitionBy;

            try (TableMetadata m = engine.getTableMetadata(token)) {
                timestampType = m.getTimestampType();
                partitionBy = m.getPartitionBy();
            }

            try (TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade())) {
                txReader.ofRO(Path.getThreadLocal(root).concat(token).concat(TXN_FILE_NAME).$(), timestampType, partitionBy);
                txReader.unsafeLoadAll();

                Assert.assertEquals(1, txReader.getLagTxnCount());
                Assert.assertEquals(1, txReader.getLagRowCount());
                Assert.assertTrue(txReader.isLagOrdered());
                Assert.assertEquals("2022-02-24T01:00:00.000Z", Micros.toString(txReader.getLagMinTimestamp()));
                Assert.assertEquals("2022-02-24T01:00:00.000Z", Micros.toString(txReader.getLagMaxTimestamp()));

                runApplyOnce(token);
                txReader.unsafeLoadAll();

                Assert.assertEquals(2, txReader.getLagTxnCount());
                Assert.assertEquals(2, txReader.getLagRowCount());
                Assert.assertFalse(txReader.isLagOrdered());
                Assert.assertEquals(MicrosTimestampDriver.floor("2022-02-24T00"), txReader.getLagMinTimestamp());
                Assert.assertEquals(MicrosTimestampDriver.floor("2022-02-24T01"), txReader.getLagMaxTimestamp());

                runApplyOnce(token);
                txReader.unsafeLoadAll();

                Assert.assertEquals(0, txReader.getLagTxnCount());
                Assert.assertEquals(0, txReader.getLagRowCount());
                Assert.assertTrue(txReader.isLagOrdered());
                Assert.assertEquals(Long.MAX_VALUE, txReader.getLagMinTimestamp());
                Assert.assertEquals(Long.MIN_VALUE, txReader.getLagMaxTimestamp());
            }
        });
    }

    @Test
    public void testSuspendedTablesTriedOnceOnStart() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, "fail.d")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            execute("insert into " + tableName + " values (101, 'dfd', '2022-02-24T01')");
            execute("insert into " + tableName + " values (101, 'dfd', '2022-02-24T02')");
            execute("insert into " + tableName + " values (101, 'dfd', '2022-02-24T03')");
            execute("alter table " + tableName + " add column fail int");
            long walNotification = engine.getMessageBus().getWalTxnNotificationPubSequence().current();

            drainWalQueue();
            TableToken tableToken = engine.verifyTableName(tableName);
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(4, engine.getTableSequencerAPI().getTxnTracker(tableToken).getWriterTxn());
            Assert.assertEquals(5, engine.getTableSequencerAPI().getTxnTracker(tableToken).getSeqTxn());

            long notifications = engine.getMessageBus().getWalTxnNotificationPubSequence().current();
            Assert.assertEquals(walNotification, notifications);

            engine.getTableSequencerAPI().releaseAll();
            drainWalQueue();
            notifications = engine.getMessageBus().getWalTxnNotificationPubSequence().current();
            Assert.assertTrue(walNotification < notifications);

            // No notification second time
            drainWalQueue();
            Assert.assertEquals(notifications, engine.getMessageBus().getWalTxnNotificationPubSequence().current());
        });
    }

    @Test
    public void testVarSizeColumnBeforeInsertCommit() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                        " values (101, 'a1a1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
                try (
                        InsertOperation insertOperation = compiledQuery.popInsertOperation();
                        InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
                ) {

                    insertMethod.execute(sqlExecutionContext);
                    execute("alter table " + tableName + " add column sss string");
                    insertMethod.commit();
                }
            }

            execute("insert into " + tableName + " values (103, 'dfd', '2022-02-24T01', 'asdd', '1234')");
            drainWalQueue();

            assertQueryNoLeakCheck(
                    "x\tsym\tts\tsym2\tsss\n" +
                            "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\t\n" +
                            "103\tdfd\t2022-02-24T01:00:00.000000Z\tasdd\t1234\n",
                    tableName,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testWhenApplyJobTerminatesEarlierLagCommitted() throws Exception {
        AtomicBoolean isTerminating = new AtomicBoolean();
        Job.RunStatus runStatus = isTerminating::get;

        FilesFacade ff = new TestFilesFacadeImpl() {
            // terminate WAL apply Job as soon as first wal segment is opened.
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.containsAscii(name, Files.SEPARATOR + "wal1" + Files.SEPARATOR + "0" + Files.SEPARATOR + "x.d")) {
                    isTerminating.set(true);
                }
                return super.openRO(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            String tableName = testName.getMethodName() + "_लаблअца";
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24T02', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL");

            execute("insert into " + tableName +
                    " values (101, 'dfd', '2022-02-24T01:01', 'asd')");

            execute("insert into " + tableName +
                    " values (102, 'dfd', '2022-02-24T01:02', 'asd')");

            execute("insert into " + tableName +
                    " values (103, 'dfd', '2022-02-24T01:03', 'asd')");

            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                walApplyJob.run(0, runStatus);
                engine.releaseInactive();
                isTerminating.set(false);

                // Run one more time, we want to be on seqTxn 2 for the next step
                walApplyJob.run(0, runStatus);
                isTerminating.set(false);

                //noinspection StatementWithEmptyBody
                while (walApplyJob.run(0, runStatus)) ;

                engine.releaseInactive();

                assertQueryNoLeakCheck(
                        "x\tsym\tts\tsym2\n" +
                                "101\tdfd\t2022-02-24T01:01:00.000000Z\tasd\n" +
                                "102\tdfd\t2022-02-24T01:02:00.000000Z\tasd\n" +
                                "103\tdfd\t2022-02-24T01:03:00.000000Z\tasd\n" +
                                "1\tAB\t2022-02-24T02:00:00.000000Z\tEF\n",
                        tableName,
                        "ts",
                        true,
                        true
                );
            }
        });
    }

    private void checkTableFilesExist(TableToken sysTableName, String partition, String fileName, boolean value) {
        Path sysPath = Path.PATH.get().of(configuration.getDbRoot()).concat(sysTableName).concat(TXN_FILE_NAME);
        Assert.assertEquals(Utf8s.toString(sysPath), value, Files.exists(sysPath.$()));

        sysPath = Path.PATH.get().of(configuration.getDbRoot()).concat(sysTableName).concat(COLUMN_VERSION_FILE_NAME);
        Assert.assertEquals(Utf8s.toString(sysPath), value, Files.exists(sysPath.$()));

        sysPath.of(configuration.getDbRoot()).concat(sysTableName).concat("sym.c");
        Assert.assertEquals(Utf8s.toString(sysPath), value, Files.exists(sysPath.$()));

        sysPath = Path.PATH.get().of(configuration.getDbRoot()).concat(sysTableName).concat(partition).concat(fileName);
        Assert.assertEquals(Utf8s.toString(sysPath), value, Files.exists(sysPath.$()));
    }

    private void checkWalFilesRemoved(TableToken sysTableName) {
        Path sysPath = Path.PATH.get().of(configuration.getDbRoot()).concat(sysTableName).concat(WalUtils.WAL_NAME_BASE).put(1);
        Assert.assertTrue(Utf8s.toString(sysPath), Files.exists(sysPath.$()));

        engine.releaseInactiveTableSequencers();
        try (WalPurgeJob job = new WalPurgeJob(engine, configuration.getFilesFacade(), configuration.getMicrosecondClock())) {
            job.run(0);
        }

        sysPath.of(configuration.getDbRoot()).concat(sysTableName).concat(WalUtils.WAL_NAME_BASE).put(1);
        Assert.assertFalse(Utf8s.toString(sysPath), Files.exists(sysPath.$()));

        sysPath.of(configuration.getDbRoot()).concat(sysTableName).concat(SEQ_DIR);
        Assert.assertFalse(Utf8s.toString(sysPath), Files.exists(sysPath.$()));
    }

    private void runApplyOnce(TableToken token) {
        var control = engine.getTableSequencerAPI().getTxnTracker(token).getMemPressureControl();
        control.setMaxBlockRowCount(1);

        try (ApplyWal2TableJob walApplyJob = createWalApplyJob(engine)) {
            walApplyJob.run(0);
        }
    }

    private void testCreateDropRestartRestart0() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            execute("create table " + tableName + "2 as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );
            TableToken sysTableName1 = engine.verifyTableName(tableName);
            TableToken sysTableName2 = engine.verifyTableName(tableName + "2");

            // Fully delete table2
            execute("drop table " + tableName + "2");
            engine.releaseInactive();
            drainWalQueue();
            drainPurgeJob();
            checkTableFilesExist(sysTableName2, "2022-02-24", "sym.d", false);

            // Mark table1 as deleted
            execute("drop table " + tableName);

            execute("create table " + tableName + " as (" +
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
            drainPurgeJob();

            checkTableFilesExist(sysTableName1, "2022-02-24", "sym.d", false);
        });
    }

    private void testDropFailedWhileDataFileLocked(final String fileName) throws Exception {
        AtomicBoolean latch = new AtomicBoolean();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                if (Utf8s.endsWithAscii(name, fileName) && latch.get()) {
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            drainWalQueue();

            TableToken sysTableName1 = engine.verifyTableName(tableName);
            execute("drop table " + tableName);

            latch.set(true);
            drainWalQueue();

            latch.set(false);
            if (Os.type == Os.WINDOWS) {
                // Release WAL writers
                engine.releaseInactive();
            }

            drainWalQueue();

            checkTableFilesExist(sysTableName1, "2022-02-24", "x.d", false);
            checkWalFilesRemoved(sysTableName1);

            assertException(tableName, 0, "does not exist");
        });
    }

    private void testInsertAsSelectBatched(String destTableCreateAttr) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE \"nhs\" (\n" +
                    "  nuuid SYMBOL capacity 256 CACHE index capacity 256,\n" +
                    "  lns LONG,\n" +
                    "  hst STRING,\n" +
                    "  slt LONG,\n" +
                    "  ise BOOLEAN,\n" +
                    "  vvv STRING,\n" +
                    "  cut DOUBLE,\n" +
                    "  mup DOUBLE,\n" +
                    "  mub DOUBLE,\n" +
                    "  nrb DOUBLE,\n" +
                    "  ntb DOUBLE,\n" +
                    "  dup DOUBLE,\n" +
                    "  timestamp TIMESTAMP\n" +
                    ") " + destTableCreateAttr + ";");

            execute("CREATE TABLE 'nhs2' (\n" +
                    "  nuuid SYMBOL capacity 256 CACHE,\n" +
                    "  lns LONG,\n" +
                    "  hst STRING,\n" +
                    "  slt LONG,\n" +
                    "  vvv STRING,\n" +
                    "  timestamp TIMESTAMP,\n" +
                    "  ise BOOLEAN\n" +
                    ") timestamp (timestamp) PARTITION BY DAY BYPASS WAL;");

            execute("insert batch 100000 into nhs(nuuid, lns, hst, slt, ise, vvv,  \n" +
                    "timestamp)\n" +
                    "select nuuid, lns, hst, slt, ise, vvv, \n" +
                    "timestamp\n" +
                    "from nhs2");

            drainWalQueue();

            execute("insert into nhs2(nuuid, lns, hst, slt, ise, vvv, timestamp)\n" +
                    "values('asdf', 123, 'asdff', 222, true, '1.2.3', 12321312321L)");

            execute("insert batch 100000 into nhs(nuuid, lns, hst, slt, ise, vvv,  \n" +
                    "timestamp)\n" +
                    "select nuuid, lns, hst, slt, ise, vvv, \n" +
                    "timestamp\n" +
                    "from nhs2");

            drainWalQueue();

            assertSql(
                    "nuuid\tlns\thst\tslt\tise\tvvv\tcut\tmup\tmub\tnrb\tntb\tdup\ttimestamp\n" +
                            "asdf\t123\tasdff\t222\ttrue\t1.2.3\tnull\tnull\tnull\tnull\tnull\tnull\t1970-01-01T03:25:21.312321Z\n",
                    "nhs"
            );
        });
    }
}
