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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.TableWriterFrontend;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.wal.fuzz.FuzzTransaction;
import io.questdb.griffin.wal.fuzz.FuzzTransactionGenerator;
import io.questdb.griffin.wal.fuzz.FuzzTransactionOperation;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class WalWriterFuzzTest extends AbstractGriffinTest {
    @Test
    public void testWalAddRemoveCommitFuzzO3() throws Exception {
        assertMemoryLeak(() -> {
            String tableNameWal = testName.getMethodName() + "_wal";
            String tableNameWal2 = testName.getMethodName() + "_wal_parallel";
            String tableNameNoWal = testName.getMethodName() + "_nonwal";

            createInitialTable(tableNameWal, true, 5000);
            createInitialTable(tableNameWal2, true, 5000);
            createInitialTable(tableNameNoWal, false, 5000);

            ObjList<FuzzTransaction> transactions;
            int tableId1, tableId2;
            try (TableReader reader = new TableReader(configuration, tableNameWal)) {
                TableReaderMetadata metadata = reader.getMetadata();
                tableId1 = metadata.getTableId();
                transactions = FuzzTransactionGenerator.generateSet(
                        metadata,
                        new Rnd(),
                        IntervalUtils.parseFloorPartialDate("2022-02-24T17"),
                        IntervalUtils.parseFloorPartialDate("2022-02-27T17"),
                        100_000,
                        true,
                        0.05,
                        0.2,
                        0.1,
                        0.005,
                        0.05,
                        0.05,
                        500,
                        20,
                        10000
                );
            }
            try (TableReader reader = new TableReader(configuration, tableNameWal)) {
                TableReaderMetadata metadata = reader.getMetadata();
                tableId2 = metadata.getTableId();
            }

            applyNonWal(transactions, tableNameNoWal, tableId2);

            applyWal(transactions, tableNameWal, tableId1, 3);
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal, tableNameWal, LOG);

            applyWalParallel(transactions, tableNameWal2, tableId1, 4);
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal, tableNameWal2, LOG);
        });
    }

    @Test
    public void testWalAddRemoveCommitFuzzInOrder() throws Exception {
        assertMemoryLeak(() -> {
            String tableNameWal = testName.getMethodName() + "_wal";
            String tableNameWal2 = testName.getMethodName() + "_wal_parallel";
            String tableNameNoWal = testName.getMethodName() + "_nonwal";

            createInitialTable(tableNameWal, true, 0);
            createInitialTable(tableNameWal2, true,0);
            createInitialTable(tableNameNoWal, false, 0);

            ObjList<FuzzTransaction> transactions;
            int tableId1, tableId2;
            try (TableReader reader = new TableReader(configuration, tableNameWal)) {
                TableReaderMetadata metadata = reader.getMetadata();
                tableId1 = metadata.getTableId();
                transactions = FuzzTransactionGenerator.generateSet(
                        metadata,
                        new Rnd(),
                        IntervalUtils.parseFloorPartialDate("2022-02-24T17"),
                        IntervalUtils.parseFloorPartialDate("2022-02-27T17"),
                        1000_000,
                        false,
                        0.05,
                        0.2,
                        0.1,
                        0.005,
                        0.05,
                        0.05,
                        1000,
                        20,
                        1000
                );
            }
            try (TableReader reader = new TableReader(configuration, tableNameWal)) {
                TableReaderMetadata metadata = reader.getMetadata();
                tableId2 = metadata.getTableId();
            }

            applyNonWal(transactions, tableNameNoWal, tableId2);

            applyWal(transactions, tableNameWal, tableId1, 4);
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal, tableNameWal, LOG);

            applyWalParallel(transactions, tableNameWal2, tableId1, 4);
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal, tableNameWal2, LOG);
        });
    }

    @Test
    public void testWalMetadataChangeHeavy() throws Exception {
        assertMemoryLeak(() -> {
            String tableNameWal = testName.getMethodName() + "_wal";
            String tableNameWal2 = testName.getMethodName() + "_wal_parallel";
            String tableNameNoWal = testName.getMethodName() + "_nonwal";

            createInitialTable(tableNameWal, true, 0);
            createInitialTable(tableNameWal2, true,0);
            createInitialTable(tableNameNoWal, false, 0);

            ObjList<FuzzTransaction> transactions;
            int tableId1, tableId2;
            try (TableReader reader = new TableReader(configuration, tableNameWal)) {
                TableReaderMetadata metadata = reader.getMetadata();
                tableId1 = metadata.getTableId();
                transactions = FuzzTransactionGenerator.generateSet(
                        metadata,
                        new Rnd(),
                        IntervalUtils.parseFloorPartialDate("2022-02-24T17"),
                        IntervalUtils.parseFloorPartialDate("2022-02-27T17"),
                        1000_000,
                        false,
                        0.05,
                        0.2,
                        0.1,
                        0.005,
                        // 50% chance of column add
                        0.5,
                        // 50% chance of column remove
                        0.5,
                        1000,
                        20,
                        1000
                );
            }
            try (TableReader reader = new TableReader(configuration, tableNameWal)) {
                TableReaderMetadata metadata = reader.getMetadata();
                tableId2 = metadata.getTableId();
            }

            applyNonWal(transactions, tableNameNoWal, tableId2);

            applyWal(transactions, tableNameWal, tableId1, 4);
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal, tableNameWal, LOG);

            applyWalParallel(transactions, tableNameWal2, tableId1, 4);
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal, tableNameWal2, LOG);
        });
    }

    private void createInitialTable(String tableName1, boolean isWal, int rowCount) throws SqlException {
        SharedRandom.RANDOM.set(new Rnd());
        compile("create table " + tableName1 + " as (" +
                "select x as c1, " +
                " rnd_symbol('AB', 'BC', 'CD') c2, " +
                " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                " rnd_symbol('DE', null, 'EF', 'FG') sym2," +
                " cast(x as int) c3," +
                " rnd_str('a', 'bdece', null, ' asdflakji idid', 'dk') " +
                " from long_sequence(" + rowCount + ")" +
                ") timestamp(ts) partition by DAY " + (isWal ? "WAL" : "BYPASS WAL"));
    }

    private void applyWal(ObjList<FuzzTransaction> transactions, String tableName, int tableId, int walWriterCount) {
        ObjList<WalWriter> writers = new ObjList<>();
        for(int i = 0; i < walWriterCount; i++) {
            writers.add((WalWriter) engine.getTableWriterFrontEnd(sqlExecutionContext.getCairoSecurityContext(), tableName, "apply trans test"));
        }

        IntList tempList = new IntList();
        Rnd writerRnd = new Rnd();
        for (int i = 0, n = transactions.size(); i < n; i++) {
            WalWriter writer = writers.getQuick(writerRnd.nextPositiveInt() % walWriterCount);
            writer.goActive();
            FuzzTransaction transaction = transactions.getQuick(i);
            for (int operationIndex = 0; operationIndex < transaction.operationList.size(); operationIndex++) {
                FuzzTransactionOperation operation = transaction.operationList.getQuick(operationIndex);
                operation.apply(writer, tableName, tableId, tempList);
            }

            if (transaction.rollback) {
                writer.rollback();
            } else {
                writer.commit();
            }
        }

        Misc.freeObjList(writers);
        drainWalQueue();
    }

    private void applyWalParallel(ObjList<FuzzTransaction> transactions, String tableName, int tableId, int walWriterCount) {
        ObjList<WalWriter> writers = new ObjList<>();

        Thread[] threads = new Thread[walWriterCount];
        AtomicLong structureVersion = new AtomicLong();
        AtomicInteger nextOperation = new AtomicInteger(-1);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        CountDownLatch latch = new CountDownLatch(walWriterCount);
        AtomicInteger done = new AtomicInteger();

        for(int i = 0; i < walWriterCount; i++) {
            final WalWriter walWriter = (WalWriter) engine.getTableWriterFrontEnd(sqlExecutionContext.getCairoSecurityContext(), tableName, "apply trans test");
            writers.add(walWriter);

            Thread thread = new Thread(() -> {
                IntList tempList = new IntList();
                int opIndex;

                try {
                    latch.countDown();
                    while ((opIndex = nextOperation.incrementAndGet()) < transactions.size() && errors.size() == 0) {
                        FuzzTransaction transaction = transactions.getQuick(opIndex);

                        // wait until structure version is applied
                        while (structureVersion.get() < transaction.structureVersion && errors.size() == 0) {
                            Os.sleep(1);
                        }

                        if (!walWriter.goActive(transaction.structureVersion)) {
                            throw CairoException.critical(0).put("cannot apply structure change");
                        }
                        if (walWriter.getStructureVersion() != transaction.structureVersion) {
                            throw CairoException.critical(0)
                                    .put("cannot update wal writer to correct structure version");
                        }

                        boolean increment = false;
                        for (int operationIndex = 0; operationIndex < transaction.operationList.size(); operationIndex++) {
                            FuzzTransactionOperation operation = transaction.operationList.getQuick(operationIndex);
                            increment |= operation.apply(walWriter, tableName, tableId, tempList);
                        }

                        if (transaction.rollback) {
                            walWriter.rollback();
                        } else {
                            walWriter.commit();
                        }
                        if (increment) {
                            structureVersion.incrementAndGet();
                        }
                    }
                } catch (Throwable e) {
                    errors.add(e);
                } finally {
                    Path.clearThreadLocals();
                }
            });
            threads[i] = thread;
            thread.start();
        }

        Thread applyThread = new Thread(() -> {
            try {
                int i = 0;
                try (ApplyWal2TableJob job = new ApplyWal2TableJob(engine)) {
                    while (done.get() == 0 && errors.size() == 0) {
                        Unsafe.getUnsafe().loadFence();
                        while (job.run(0)) {
                            // run until empty
                        }
                        Os.sleep(1);
                        i++;
                    }
                    while (job.run(0)) {
                        // run until empty
                    }
                }
                LOG.info().$("finished apply thread after iterations: ").$(i).$();
            } catch (Throwable e) {
                errors.add(e);
            } finally {
                Path.clearThreadLocals();
            }
        });
        applyThread.start();


        for (int i = 0; i < threads.length; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        done.incrementAndGet();
        Misc.freeObjList(writers);

        for(Throwable e : errors) {
            throw new RuntimeException(e);
        }

        try {
            applyThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void applyNonWal(ObjList<FuzzTransaction> transactions, String tableName, int tableId) {
        try(TableWriterFrontend writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), tableName, "apply trans test")) {
            IntList tempList = new IntList();
            int transactionSize = transactions.size();
            for (int i = 0; i < transactionSize; i++) {
                FuzzTransaction transaction = transactions.getQuick(i);
                int size = transaction.operationList.size();
                for (int operationIndex = 0; operationIndex < size; operationIndex++) {
                    FuzzTransactionOperation operation = transaction.operationList.getQuick(operationIndex);
                    operation.apply(writer, tableName, tableId, tempList);
                }

                if (transaction.rollback) {
                    writer.rollback();
                } else {
                    writer.commit();
                }
            }
        }
    }
}
