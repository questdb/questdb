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

package io.questdb.cairo.wal;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.wal.WalUtils.TABLE_REGISTRY_NAME_FILE;

public class TableNameRegistryTest extends AbstractCairoTest {
    protected static final Log LOG = LogFactory.getLog(TableNameRegistryTest.class);

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
                        try (
                                SqlCompiler compiler = new SqlCompiler(engine);
                                SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1, 1)
                        ) {
                            for (int j = 0; j < tableCount; j++) {
                                try {
                                    compiler.compile("drop table tab" + j, executionContext);
                                } catch (TableReferenceOutOfDateException e) {
                                    // this is fine, query will have to recompile
                                } catch (SqlException | CairoException e) {
                                    if (!Chars.contains(e.getFlyweightMessage(), "table does not exist")
                                            && !Chars.contains(e.getFlyweightMessage(), "Could not lock")
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
                        try (
                                SqlCompiler compiler = new SqlCompiler(engine);
                                SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1, 1)
                        ) {
                            for (int j = 0; j < tableCount; j++) {
                                boolean isWal = rnd.nextBoolean();
                                try {
                                    compiler.compile(
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
                                    compiler.compile("drop table tab" + j, executionContext);
                                } catch (TableReferenceOutOfDateException e) {
                                    // this is fine, query will have to recompile
                                } catch (SqlException | CairoException e) {
                                    // Should never fail on drop table.
                                    if (!Chars.contains(e.getFlyweightMessage(), "table does not exist")
                                            && !Chars.contains(e.getFlyweightMessage(), "Could not lock")
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
                    snapshotAgent.setWalPurgeJobRunLock(job.getRunLock());
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

            final ObjList<TableToken> tableTokenBucket = new ObjList<>();
            engine.getTableTokens(tableTokenBucket, true);
            if (0 != tableTokenBucket.size()) {
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
            CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
            ObjList<Thread> threads = new ObjList<>(threadCount);
            AtomicBoolean done = new AtomicBoolean(false);

            for (int i = 0; i < threadCount; i++) {
                threads.add(new Thread(() -> {
                    try {
                        try (TableNameRegistryRO ro = new TableNameRegistryRO(configuration)) {
                            barrier.await();
                            while (!done.get()) {
                                ro.reloadTableNameCache();
                                Os.pause();
                            }

                            ro.reloadTableNameCache();
                            int size = getNonDroppedSize(ro);
                            Assert.assertEquals(tableCount, size);
                        }
                    } catch (Throwable e) {
                        ref.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }));
                threads.getLast().start();
            }

            // Add / remove tables
            engine.closeNameRegistry();
            Rnd rnd = TestUtils.generateRandom(LOG);
            try (TableNameRegistryRW rw = new TableNameRegistryRW(configuration)) {
                rw.reloadTableNameCache();
                barrier.await();
                int iteration = 0;
                IntHashSet addedTables = new IntHashSet();
                while (addedTables.size() < tableCount) {
                    iteration++;
                    if (rnd.nextDouble() > 0.2) {
                        // Add table
                        String tableName = "tab" + iteration;
                        TableToken tableToken = rw.lockTableName(tableName, tableName, iteration, true);
                        addedTables.add(iteration);
                        rw.registerName(tableToken);
                    } else if (addedTables.size() > 0) {
                        // Remove table
                        int tableId = addedTables.getLast();
                        String tableName = "tab" + tableId;
                        TableToken tableToken = rw.getTableToken(tableName);
                        rw.dropTable(tableToken);
                        addedTables.remove(tableId);
                    }

                    if (rnd.nextBoolean()) {
                        // May run compaction
                        rw.reloadTableNameCache();
                        Assert.assertEquals(addedTables.size(), getNonDroppedSize(rw));
                    }
                }
                Path.clearThreadLocals();
            } finally {
                done.set(true);
            }

            for (int i = 0; i < threads.size(); i++) {
                threads.getQuick(i).join();
            }

            if (ref.get() != null) {
                throw new RuntimeException(ref.get());
            }
        });
    }

    @Test
    public void testRestoreTableNamesFile() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tt1;
            try (TableModel model = new TableModel(configuration, "tab1", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.INT)
                    .wal()
                    .timestamp()) {
                tt1 = engine.createTable(AllowAllCairoSecurityContext.INSTANCE, model.getMem(), model.getPath(), false, model, false);
            }
            Assert.assertTrue(engine.isWalTable(tt1));

            TableToken tt2;
            try (TableModel model = new TableModel(configuration, "tab2", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.INT)
                    .wal()
                    .timestamp()) {
                tt2 = engine.createTable(AllowAllCairoSecurityContext.INSTANCE, model.getMem(), model.getPath(), false, model, false);
            }
            Assert.assertTrue(engine.isWalTable(tt2));

            TableToken tt3;
            try (TableModel model = new TableModel(configuration, "tab3", PartitionBy.NONE)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.INT)
                    .noWal()
                    .timestamp()) {
                tt3 = engine.createTable(AllowAllCairoSecurityContext.INSTANCE, model.getMem(), model.getPath(), false, model, false);
            }
            Assert.assertFalse(engine.isWalTable(tt3));

            tt2 = engine.rename(AllowAllCairoSecurityContext.INSTANCE, Path.getThreadLocal(""), "tab2", Path.getThreadLocal2(""), "tab2_ࠄ");
            Assert.assertTrue(engine.isWalTable(tt2));
            drainWalQueue();

            engine.closeNameRegistry();

            Assert.assertTrue(TestFilesFacadeImpl.INSTANCE.remove(Path.getThreadLocal(root).concat(TABLE_REGISTRY_NAME_FILE).put(".0").$()));

            engine.reloadTableNames();

            Assert.assertEquals(tt1, engine.getTableToken("tab1"));
            Assert.assertEquals(tt2, engine.getTableToken("tab2_ࠄ"));
            Assert.assertEquals(tt3, engine.getTableToken("tab3"));
        });
    }

    private static int getNonDroppedSize(TableNameRegistry ro) {
        ObjList<TableToken> bucket = new ObjList<>();
        ro.getTableTokens(bucket, false);
        return bucket.size();
    }

    private String formatTableDirs(ObjList<TableToken> tableTokenBucket) {
        StringSink ss = new StringSink();
        for (int i = 0, n = tableTokenBucket.size(); i < n; i++) {
            if (i > 0) {
                ss.put(", ");
            }
            ss.put(tableTokenBucket.getQuick(i).getTableName());
        }
        return ss.toString();
    }
}
