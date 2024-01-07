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

package io.questdb.test.cairo;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.CairoEngine.EMPTY_RESOLVER;
import static io.questdb.cairo.GrowOnlyTableNameRegistryStore.OPERATION_ADD;
import static io.questdb.cairo.wal.WalUtils.TABLE_REGISTRY_NAME_FILE;
import static io.questdb.std.Files.FILES_RENAME_OK;

public class TableNameRegistryTest extends AbstractCairoTest {

    private static final int FUZZ_APPLY = 3;
    private static final int FUZZ_CREATE = 0;
    private static final int FUZZ_DROP = 2;
    private static final int FUZZ_RENAME = 1;
    private static final int FUZZ_SWEEP = 4;

    @Test
    public void testConcurrentCreateDropRemove() throws Exception {
        assertMemoryLeak(() -> {
            int threadCount = 3;
            int tableCount = 100;
            AtomicReference<Throwable> ref = new AtomicReference<>();
            CyclicBarrier barrier = new CyclicBarrier(2 * threadCount + 2);

            ObjList<Thread> threads = new ObjList<>(threadCount + 2);
            for (int i = 0; i < threadCount; i++) {
                threads.add(new Thread(() -> {
                    try {
                        barrier.await();
                        try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                            for (int j = 0; j < tableCount; j++) {
                                try {
                                    drop("drop table tab" + j, executionContext);
                                } catch (TableReferenceOutOfDateException e) {
                                    // this is fine, query will have to recompile
                                } catch (SqlException | CairoException e) {
                                    if (!Chars.contains(e.getFlyweightMessage(), "table does not exist")
                                            && !Chars.contains(e.getFlyweightMessage(), "could not lock")
                                            && !Chars.contains(e.getFlyweightMessage(), "table name is reserved")) {
                                        throw e;
                                    }
                                }
                            }
                        }
                    } catch (Throwable e) {
                        ref.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }));
                threads.get(2 * i).start();

                threads.add(new Thread(() -> {
                    try {
                        barrier.await();
                        Rnd rnd = TestUtils.generateRandom(LOG);
                        try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                            for (int j = 0; j < tableCount; j++) {
                                boolean isWal = rnd.nextBoolean();
                                try {
                                    ddl(
                                            "create table tab" + j + " (x int, ts timestamp) timestamp(ts) Partition by DAY "
                                                    + (!isWal ? "BYPASS" : "")
                                                    + " WAL ",
                                            executionContext
                                    );
                                } catch (SqlException e) {
                                    TestUtils.assertContains(e.getFlyweightMessage(), "table already exists");
                                    continue;
                                }

                                try {
                                    drop("drop table tab" + j, executionContext);
                                } catch (TableReferenceOutOfDateException e) {
                                    // this is fine, query will have to recompile
                                } catch (SqlException | CairoException e) {
                                    // Should never fail on drop table.
                                    if (!Chars.contains(e.getFlyweightMessage(), "table does not exist")
                                            && !Chars.contains(e.getFlyweightMessage(), "could not lock")
                                            && !Chars.contains(e.getFlyweightMessage(), "table name is reserved")) {
                                        throw e;
                                    }
                                }
                            }
                        }
                    } catch (Throwable e) {
                        ref.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }));
                threads.get(2 * i + 1).start();
            }

            // Drain WAL jobs
            AtomicBoolean done = new AtomicBoolean(false);

            threads.add(new Thread(() -> {
                final CheckWalTransactionsJob checkWalTransactionsJob = new CheckWalTransactionsJob(engine);
                try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                    barrier.await();
                    while (!done.get()) {
                        //noinspection StatementWithEmptyBody
                        while (walApplyJob.run(0)) {
                            // run until empty
                        }

                        checkWalTransactionsJob.run(0);

                        // run once again as there might be notifications to handle now
                        //noinspection StatementWithEmptyBody
                        while (walApplyJob.run(0)) {
                            // run until empty
                        }
                    }
                } catch (Throwable e) {
                    ref.set(e);
                } finally {
                    Path.clearThreadLocals();
                }
            }));
            threads.getLast().start();

            threads.add(new Thread(() -> {
                try (WalPurgeJob job = new WalPurgeJob(engine, engine.getConfiguration().getFilesFacade(), engine.getConfiguration().getMicrosecondClock())) {
                    barrier.await();
                    engine.setWalPurgeJobRunLock(job.getRunLock());
                    //noinspection StatementWithEmptyBody
                    while (!done.get() && job.run(0)) {
                        // run until empty
                    }
                } catch (Throwable e) {
                    ref.set(e);
                } finally {
                    Path.clearThreadLocals();
                }
            }));
            threads.getLast().start();

            for (int i = 0; i < threads.size() - 2; i++) {
                threads.getQuick(i).join();
            }
            done.set(true);
            for (int i = threads.size() - 2; i < threads.size(); i++) {
                threads.getQuick(i).join();
            }

            if (ref.get() != null) {
                throw new RuntimeException(ref.get());
            }

            engine.reloadTableNames();
            drainWalQueue();

            engine.releaseInactive();
            runWalPurgeJob();

            drainWalQueue();

            final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
            engine.getTableTokens(tableTokenBucket, true);
            if (!tableTokenBucket.isEmpty()) {
                Assert.assertEquals(formatTableDirs(tableTokenBucket), 0, tableTokenBucket.size());
            }
        });
    }

    @Test
    public void testConcurrentReadWriteAndReload() throws Exception {
        assertMemoryLeak(() -> {
            int threadCount = 2;
            int tableCount = 400;
            AtomicReference<Throwable> ref = new AtomicReference<>();
            CyclicBarrier startBarrier = new CyclicBarrier(threadCount + 1);
            ObjList<Thread> threads = new ObjList<>(threadCount);
            SOCountDownLatch halted = new SOCountDownLatch(threadCount);
            AtomicBoolean done = new AtomicBoolean(false);

            for (int i = 0; i < threadCount; i++) {
                threads.add(new Thread(() -> {
                    try {
                        try (TableNameRegistryRO ro = new TableNameRegistryRO(configuration, EMPTY_RESOLVER)) {
                            startBarrier.await();
                            while (!done.get()) {
                                ro.reload();
                                Os.pause();
                            }

                            ro.reload();
                            Assert.assertEquals(tableCount, getNonDroppedSize(ro));
                        }
                    } catch (Throwable e) {
                        ref.set(e);
                    } finally {
                        Path.clearThreadLocals();
                        halted.countDown();
                    }
                }));
                threads.getLast().start();
            }

            try (
                    TableModel tm = new TableModel(configuration, "abc", PartitionBy.DAY)
                            .timestamp().col("c", ColumnType.TIMESTAMP);
                    Path rmPath = new Path().of(configuration.getRoot())
            ) {
                // Add / remove tables
                engine.closeNameRegistry();
                Rnd rnd = TestUtils.generateRandom(LOG);
                try (TableNameRegistryRW rw = new TableNameRegistryRW(configuration, EMPTY_RESOLVER)) {
                    rw.reload();
                    startBarrier.await();
                    int iteration = 0;
                    IntHashSet addedTables = new IntHashSet();
                    FilesFacade ff = configuration.getFilesFacade();
                    int rootLen = configuration.getRoot().length();
                    while (addedTables.size() < tableCount) {
                        iteration++;
                        if (rnd.nextDouble() > 0.2) {
                            // Add table
                            String tableName = "tab" + iteration;
                            TableToken tableToken = rw.lockTableName(tableName, tableName, iteration, true);
                            rw.registerName(tableToken);
                            addedTables.add(iteration);
                            TableUtils.createTable(configuration, tm.getMem(), tm.getPath(), tm, iteration, tableName);
                        } else if (addedTables.size() > 0) {
                            // Remove table
                            int tableId = addedTables.getLast();
                            String tableName = "tab" + tableId;
                            TableToken tableToken = rw.getTableToken(tableName);
                            rw.dropTable(tableToken);
                            addedTables.remove(tableId);

                            // Retry remove table folder, until success, if table folder not clearly removed, reload may pick it up
                            // Remove _txn file first
                            rmPath.trimTo(rootLen).concat(tableName);
                            int len = rmPath.size();
                            rmPath.concat(TableUtils.TXN_FILE_NAME).$();
                            ff.remove(rmPath);

                            // Remove table directory
                            rmPath.trimTo(len).$();
                            for (int i = 0; i < 1000 && !ff.rmdir(rmPath); i++) {
                                Os.sleep(50L);
                            }
                        }

                        if (rnd.nextBoolean()) {
                            // May run compaction
                            rw.reload();
                            Assert.assertEquals(addedTables.size(), getNonDroppedSize(rw));
                        }
                    }
                    rw.reload();
                    Assert.assertEquals(addedTables.size(), getNonDroppedSize(rw));
                } finally {
                    done.set(true);
                    halted.await(TimeUnit.SECONDS.toNanos(4L));
                    Path.clearThreadLocals();
                }
            }

            if (ref.get() != null) {
                throw new RuntimeException(ref.get());
            }
        });
    }

    @Test
    public void testConcurrentWALTableRename() throws Exception {
        assertMemoryLeak(() -> {
            int threadCount = 2;
            int tableCount = 100;
            AtomicReference<Throwable> ref = new AtomicReference<>();
            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            ObjList<Thread> threads = new ObjList<>(threadCount);

            try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                for (int j = 0; j < tableCount; j++) {
                    String tableName = "tab" + j;
                    if (j % 2 == 0) {
                        tableName = "Tab" + j;
                    }
                    ddl("create table " + tableName + " (x int, ts timestamp) timestamp(ts) Partition by DAY WAL", executionContext);
                }
            }

            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                threads.add(new Thread(() -> {
                    try {
                        barrier.await();
                        try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                            for (int j = 0; j < tableCount; j++) {
                                try {
                                    ddl("rename table tab" + j + " to renamed_" + threadId + "_" + j, executionContext);
                                } catch (SqlException | CairoException e) {
                                    if (!Chars.contains(e.getFlyweightMessage(), "table does not exist")) {
                                        throw e;
                                    }
                                }
                            }
                        }
                    } catch (Throwable e) {
                        ref.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }));
                threads.getLast().start();
            }

            for (int i = 0; i < threads.size(); i++) {
                threads.getQuick(i).join();
            }

            if (ref.get() != null) {
                throw new RuntimeException(ref.get());
            }

            final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
            engine.getTableTokens(tableTokenBucket, true);
            if (tableCount != tableTokenBucket.size()) {
                Assert.assertEquals(formatTableDirs(tableTokenBucket), 0, tableTokenBucket.size());
            }

            for (int i = 0; i < tableCount; i++) {
                ObjList<String> names = new ObjList<>();
                for (int j = 0; j < threadCount; j++) {
                    String tableName = "renamed_" + j + "_" + i;
                    TableToken tableTokenIfExists = engine.getTableTokenIfExists(tableName);
                    if (tableTokenIfExists != null) {
                        names.add(tableName + " -> " + tableTokenIfExists.getDirName());
                    }
                }
                Assert.assertEquals("table named tab" + i + " tokens: " + names, 1, names.size());
            }
        });
    }

    @Test
    public void testConvertDropRecreate() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tt1;
            tt1 = createTableWal("tab1");
            Assert.assertTrue(engine.isWalTable(tt1));

            ddl("alter table tab1 set type bypass wal");
            drop("drop table tab1");
            createTableWal("tab1");

            simulateEngineRestart();
            engine.reconcileTableNameRegistryState();

            compile("drop table tab1");
            createTableWal("tab1");

            simulateEngineRestart();
            engine.reconcileTableNameRegistryState();
        });
    }

    @Test
    public void testConvertDropRecreateNonWal() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tt1;
            tt1 = createTableWal("tab1");
            Assert.assertTrue(engine.isWalTable(tt1));

            ddl("alter table tab1 set type bypass wal");
            drop("drop table tab1");
            createTableNonWal("tab1");

            simulateEngineRestart();
            engine.reconcileTableNameRegistryState();

            compile("drop table tab1");
            createTableWal("tab1");

            simulateEngineRestart();
            engine.reconcileTableNameRegistryState();
        });
    }

    @Test
    public void testConvertDropRecreateNonWalCannotCompactRegistry() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (Utf8s.containsAscii(to, TABLE_REGISTRY_NAME_FILE)) {
                    return FILES_RENAME_OK - 1;
                }
                return super.rename(from, to);
            }
        };

        assertMemoryLeak(ff, () -> {
            TableToken tt1;
            tt1 = createTableWal("tab1");
            Assert.assertTrue(engine.isWalTable(tt1));

            ddl("alter table tab1 set type bypass wal");
            drop("drop table tab1");
            createTableNonWal("tab1");

            simulateEngineRestart();
            engine.reconcileTableNameRegistryState();

            compile("drop table tab1");
            createTableWal("tab1");

            simulateEngineRestart();
            engine.reconcileTableNameRegistryState();
        });
    }

    @Test
    public void testConvertDropRestartRecreate() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tt1;
            tt1 = createTableWal("tab1");
            Assert.assertTrue(engine.isWalTable(tt1));

            ddl("alter table tab1 set type bypass wal");
            drop("drop table tab1");

            simulateEngineRestart();
            engine.reconcileTableNameRegistryState();

            drainWalQueue();
            runWalPurgeJob();

            createTableWal("tab1");

            simulateEngineRestart();
            engine.reconcileTableNameRegistryState();
        });
    }

    @Test
    public void testConvertRestartDropRecreate() throws Exception {
        assertMemoryLeak(() -> {
            createTableNonWal("tab1");
            ddl("alter table tab1 set type wal");

            simulateEngineRestart();
            engine.reconcileTableNameRegistryState();

            drop("drop table tab1");
            createTableWal("tab1");

            compile("drop table tab1");
            createTableWal("tab1");

            simulateEngineRestart();
            engine.reconcileTableNameRegistryState();
        });
    }

    @Test
    public void testConvertedTableListPassedToRegistryOnLoad() throws Exception {
        testConvertedTableListPassedToRegistryOnLoad0(true);
    }

    @Test
    public void testConvertedTableListPassedToRegistrySequencerExists() throws Exception {
        testConvertedTableListPassedToRegistryOnLoad0(false);
    }

    @Test
    public void testCopiedAndThenDropped() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tt1 = createTableWal("tab1");

            Assert.assertTrue(engine.isWalTable(tt1));

            engine.closeNameRegistry();
            Assert.assertTrue(TestFilesFacadeImpl.INSTANCE.removeQuiet(Path.getThreadLocal(root).concat(TABLE_REGISTRY_NAME_FILE).putAscii(".0").$()));
            engine.reloadTableNames();
            engine.reconcileTableNameRegistryState();

            TableToken tt2;
            try (MemoryMARW mem = Vm.getMARWInstance()) {
                tt2 = engine.rename(
                        securityContext,
                        Path.getThreadLocal(""),
                        mem,
                        "tab1",
                        Path.getThreadLocal2(""),
                        "tab2"
                );

            }

            try (TableModel model = new TableModel(configuration, "tab1", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.INT)
                    .wal()
                    .timestamp()) {
                tt1 = createTable(model);
            }

            Assert.assertTrue(engine.isWalTable(tt1));

            simulateEngineRestart();
            engine.reconcileTableNameRegistryState();

            Assert.assertEquals(tt1, engine.verifyTableName("tab1"));
            Assert.assertEquals(tt2, engine.verifyTableName("tab2"));
        });
    }

    @Test
    public void testDropNonWalTable() throws Exception {
        assertMemoryLeak(() -> {
            createTableNonWal("tab1");
            drop("drop table tab1");
            createTableNonWal("tab2");

            simulateEngineRestart();
            engine.reconcileTableNameRegistryState();

            try {
                drop("drop table tab1");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist");
            }
            drop("drop table tab2");

            simulateEngineRestart();
            engine.reconcileTableNameRegistryState();

            try {
                drop("drop table tab2");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist");
            }
        });
    }

    @Test
    public void testFuzz() {
        int threadCount = 4;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        SOCountDownLatch haltLatch = new SOCountDownLatch(threadCount);
        AtomicInteger errorCounter = new AtomicInteger();
        for (int i = 0; i < threadCount; i++) {
            int k = i;
            new Thread(() -> {
                TestUtils.await(barrier);
                try {
                    testFuzz0(k);
                } catch (Throwable e) {
                    LOG.error().$(e).I$();
                    errorCounter.incrementAndGet();
                } finally {
                    haltLatch.countDown();
                }
            }).start();
        }

        haltLatch.await();
        engine.reconcileTableNameRegistryState();

        simulateEngineRestart();
        engine.reconcileTableNameRegistryState();

        Assert.assertEquals(0, errorCounter.get());
    }

    @Test
    public void testMalformedTableRegistryFileIsIgnored() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tt1;
            tt1 = createTableWal("tab1");
            Assert.assertTrue(engine.isWalTable(tt1));

            ddl("alter table tab1 set type bypass wal");
            drop("drop table tab1");
            createTableWal("tab1");

            engine.closeNameRegistry();
            try (TableNameRegistryStore store = new TableNameRegistryStore(engine.getConfiguration(), EMPTY_RESOLVER)) {
                store.lock();
                store.reload(new ConcurrentHashMap<>(1), new ConcurrentHashMap<>(1), null);
                store.writeEntry(new TableToken("tab1", "tab1~1", 1, true, false, false), OPERATION_ADD);
            }

            simulateEngineRestart();
            engine.reconcileTableNameRegistryState();

            compile("drop table tab1");
            createTableWal("tab1");

            simulateEngineRestart();
            engine.reconcileTableNameRegistryState();
        });
    }

    @Test
    public void testMissingDirsRemovedFromRegistryOnLoad() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tt1 = createTableWal("tab1");
            Assert.assertTrue(engine.isWalTable(tt1));

            TableToken tt2 = createTableWal("tab2");
            Assert.assertTrue(engine.isWalTable(tt2));

            engine.releaseInactive();
            FilesFacade ff = configuration.getFilesFacade();
            Assert.assertTrue(ff.rmdir(Path.getThreadLocal2(root).concat(tt1).$()));

            engine.reloadTableNames();

            Assert.assertNull(engine.getTableTokenIfExists("tab1"));
            Assert.assertEquals(tt2, engine.verifyTableName("tab2"));
        });
    }

    @Test
    public void testRenameTableAndCreateSameName() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tt1 = createTableWal("tab1");
            Assert.assertTrue(engine.isWalTable(tt1));

            TableToken tt2;
            try (MemoryMARW mem = Vm.getMARWInstance()) {
                tt2 = engine.rename(
                        securityContext,
                        Path.getThreadLocal(""),
                        mem,
                        "tab1",
                        Path.getThreadLocal2(""),
                        "tab2"
                );
                Assert.assertTrue(engine.isWalTable(tt2));

                drainWalQueue();

                try (TableModel model = new TableModel(configuration, "tab1", PartitionBy.DAY)
                        .col("a", ColumnType.INT)
                        .col("b", ColumnType.INT)
                        .wal()
                        .timestamp()) {
                    tt1 = createTable(model);
                }
            }

            simulateEngineRestart();
            engine.reconcileTableNameRegistryState();

            Assert.assertEquals(tt1, engine.verifyTableName("tab1"));
            Assert.assertEquals(tt2, engine.verifyTableName("tab2"));
        });
    }

    @Test
    public void testRestoreTableNamesFile() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tt1 = createTableWal("tab1");
            Assert.assertTrue(engine.isWalTable(tt1));

            TableToken tt2 = createTableWal("tab2");
            Assert.assertTrue(engine.isWalTable(tt2));

            TableToken tt3;
            try (TableModel model = new TableModel(configuration, "tab3", PartitionBy.NONE)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.INT)
                    .noWal()
                    .timestamp()) {
                tt3 = createTable(model);
            }
            Assert.assertFalse(engine.isWalTable(tt3));

            try (MemoryMARW mem = Vm.getMARWInstance()) {
                tt2 = engine.rename(
                        securityContext,
                        Path.getThreadLocal(""),
                        mem,
                        "tab2",
                        Path.getThreadLocal2(""),
                        "tab2_ࠄ"
                );
                Assert.assertTrue(engine.isWalTable(tt2));
                drainWalQueue();

                tt3 = engine.rename(
                        securityContext,
                        Path.getThreadLocal(""),
                        mem,
                        "tab3",
                        Path.getThreadLocal2(""),
                        "tab3_ࠄ"
                );
            }

            engine.closeNameRegistry();

            Assert.assertTrue(TestFilesFacadeImpl.INSTANCE.removeQuiet(Path.getThreadLocal(root).concat(TABLE_REGISTRY_NAME_FILE).putAscii(".0").$()));

            engine.reloadTableNames();

            Assert.assertEquals(tt1, engine.verifyTableName("tab1"));
            Assert.assertEquals(tt2, engine.verifyTableName("tab2_ࠄ"));
            Assert.assertEquals(tt3, engine.verifyTableName("tab3_ࠄ"));
        });
    }

    private static void createTableNonWal(String tableName) throws SqlException {
        ddl("create table " + tableName + " (x int, ts timestamp) timestamp(ts) Partition by DAY BYPASS WAL");
    }

    @NotNull
    private static TableToken createTableWal(String tab1) {
        TableToken tt1;
        try (TableModel model = new TableModel(configuration, tab1, PartitionBy.DAY)
                .col("a", ColumnType.INT)
                .col("b", ColumnType.INT)
                .wal()
                .timestamp()) {
            tt1 = createTable(model);
        }
        return tt1;
    }

    private static int getNonDroppedSize(TableNameRegistry ro) {
        return ro.getTableTokenCount(false);
    }

    private static void simulateEngineRestart() {
        engine.releaseInactive();
        engine.closeNameRegistry();
        engine.reloadTableNames(null);

        final ObjList<TableToken> convertedTables = TableConverter.convertTables(
                configuration,
                engine.getTableSequencerAPI(),
                engine.getProtectedTableResolver()
        );
        engine.closeNameRegistry();
        engine.reloadTableNames(convertedTables);
    }

    private static void testConvertedTableListPassedToRegistryOnLoad0(boolean releaseInactiveBeforeConversion) throws Exception {
        assertMemoryLeak(() -> {
            TableToken tt1 = createTableWal("tab1");
            Assert.assertTrue(engine.isWalTable(tt1));

            TableToken tt2 = createTableWal("tab2");
            Assert.assertTrue(engine.isWalTable(tt2));

            TableToken tt3;
            try (TableModel model = new TableModel(configuration, "tab3", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.INT)
                    .noWal()
                    .timestamp()) {
                tt3 = createTable(model);
            }
            Assert.assertFalse(engine.isWalTable(tt3));

            ddl("alter table " + tt2.getTableName() + " set type bypass wal");
            ddl("alter table " + tt3.getTableName() + " set type wal");
            if (releaseInactiveBeforeConversion) {
                engine.releaseInactive();
            }

            final ObjList<TableToken> convertedTables = TableConverter.convertTables(configuration, engine.getTableSequencerAPI(), engine.getProtectedTableResolver());

            if (!releaseInactiveBeforeConversion) {
                engine.releaseInactive();
            }
            engine.reloadTableNames(convertedTables);

            engine.reconcileTableNameRegistryState();
            Assert.assertEquals(tt1, engine.verifyTableName("tab1"));

            Assert.assertEquals(tt2.getTableId(), engine.verifyTableName("tab2").getTableId());
            Assert.assertEquals(tt2.getTableName(), engine.verifyTableName("tab2").getTableName());
            Assert.assertEquals(tt2.getDirName(), engine.verifyTableName("tab2").getDirName());
            Assert.assertTrue(tt2.isWal());
            Assert.assertFalse(engine.verifyTableName("tab2").isWal());

            Assert.assertEquals(tt3.getTableId(), engine.verifyTableName("tab3").getTableId());
            Assert.assertEquals(tt3.getTableName(), engine.verifyTableName("tab3").getTableName());
            Assert.assertEquals(tt3.getDirName(), engine.verifyTableName("tab3").getDirName());
            Assert.assertFalse(tt3.isWal());
            Assert.assertTrue(engine.verifyTableName("tab3").isWal());
        });
    }

    private String formatTableDirs(ObjHashSet<TableToken> tableTokenBucket) {
        StringSink ss = new StringSink();
        for (int i = 0, n = tableTokenBucket.size(); i < n; i++) {
            if (i > 0) {
                ss.put(", ");
            }
            ss.put(tableTokenBucket.get(i).getTableName());
        }
        return ss.toString();
    }

    private void testFuzz0(int thread) throws SqlException {
        // create sequence of metadata events for single table, it will always begin with CREATE=0
        // number of events in the sequence
        final int maxSteps = 100;
        final int loopCounter = 8;
        final Rnd rnd = TestUtils.generateRandom(LOG);

        try (WalPurgeJob purgeJob = new WalPurgeJob(engine, FilesFacadeImpl.INSTANCE, () -> 0)) {

            for (int j = 0; j < loopCounter; j++) {

                int n = rnd.nextInt(maxSteps);

                // steps
                // create = 0
                // rename = 1
                // drop = 2
                // reload = 3

                IntList steps = new IntList(maxSteps);
                // the initial step is always "create"
                for (int i = 0; i < n; i++) {
                    steps.add(rnd.nextInt(FUZZ_APPLY + 1));
                }

                final String ogTableName = Chars.toString(rnd.nextChars(10));

                ObjList<String> tableNames = new ObjList<>();
                BitSet success = new BitSet(n);

                // analyse steps and inject table names
                boolean tableExists = false;
                for (int i = 0; i < n; i++) {
                    int step = steps.getQuick(i);

                    switch (step) {
                        case FUZZ_CREATE:
                            tableNames.add(ogTableName);
                            if (!tableExists) {
                                // if table doesn't exist, "create" must succeed
                                success.set(i);
                            }
                            tableExists = true;
                            break;
                        case FUZZ_RENAME:
                            // with 1/4th probability generate the ogName
                            if (rnd.nextInt(3) == 0) {
                                tableNames.add(ogTableName);
                            } else {
                                tableNames.add(Chars.toString(rnd.nextChars(10)));
                            }

                            if (tableExists) {
                                success.set(i);
                                tableExists = Chars.equalsNc(tableNames.get(i), tableNames.get(i - 1));
                            }
                            break;
                        case FUZZ_DROP:
                            if (i > 0) {
                                tableNames.add(tableNames.get(i - 1));
                            } else {
                                tableNames.add(ogTableName);
                            }
                            if (tableExists) {
                                success.set(i);
                            }
                            tableExists = false;
                            break;
                        case FUZZ_APPLY:
                        case FUZZ_SWEEP:
                            success.set(i);
                            if (i > 0) {
                                tableNames.add(tableNames.get(i - 1));
                            } else {
                                tableNames.add(ogTableName);
                            }
                            break;
                        default:
                            assert false : step;
                    }
                }

                // verify steps
                for (int i = 0; i < n; i++) {
                    CharSequence oldTableName = null;
                    CharSequence tableName = tableNames.getQuick(i) + Thread.currentThread().getId();
                    if (i > 0) {
                        oldTableName = tableNames.getQuick(i - 1) + Thread.currentThread().getId();
                    }
                    if (!success.get(i)) {
                        continue;
                    }
                    final int step = steps.getQuick(i);
                    try {
                        switch (step) {
                            case FUZZ_CREATE:
                                compile("create table " + tableName + "(a int, t timestamp) timestamp(t) partition by day wal");
                                break;
                            case FUZZ_RENAME:
                                compile("rename table " + oldTableName + " to " + tableName);
                                break;
                            case FUZZ_DROP:
                                drop("drop table " + tableName);
                                break;
                            case FUZZ_APPLY:
                                drainWalQueue();
                                break;
                            case FUZZ_SWEEP:
                                purgeJob.run(thread);
                                break;
                            default:
                                assert false;
                        }
                        Assert.assertTrue(success.get(i));
                    } catch (Throwable e) {
                        if (success.get(i)) {
                            throw e;
                        }
                    }
                }
            }
        }
    }
}
