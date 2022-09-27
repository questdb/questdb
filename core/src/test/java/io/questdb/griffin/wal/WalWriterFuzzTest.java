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
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.test.tools.TestUtils.getZeroToOneDouble;

// These test is designed to produce unstable runs, e.g. random generator is created
// using current execution time.
// This improves coverate. To debug failures in CI find the line logging random seeds
// and change line
// Rnd rnd = TestUtils.generateRandom(LOG);
// to
// Rnd rnd = new Rnd(A, B);
// where A, B are seeds in the failed run log.
public class WalWriterFuzzTest extends AbstractGriffinTest {

    private int initialRowCount;
    private int fuzzRowCount;
    private boolean isO3;
    private double cancelRowsProb;
    private double notSetProb;
    private double nullSetProb;
    private double rollbackProb;
    private double collAddProb;
    private double collRemoveProb;
    private double colRenameProb;
    private int transactionCount;
    private int strLen;
    private int symbolStrLenMax;
    private int symbolCountMax;

    @Test
    @Ignore // TODO fix the test.
    public void testFail2() throws Exception {
        Rnd rnd = new Rnd(65515438461083L, 1664196196645L);
        testWalWriteFullRandom(rnd);
    }

    @Test
    public void testRollbackBeforeColumnAdd() throws Exception {
        Rnd rnd = new Rnd(65781883724750L, 1664196463085L);
        testWalWriteFullRandom(rnd);
    }

    @Test
    public void testWalAddRemoveCommitFuzzInOrder() throws Exception {
        setFuzzProbabilities(0.05,
                0.2,
                0.1,
                0.005,
                0.05,
                0.05,
                0.05);
        setFuzzCounts(true, 1_000_000, 500, 20, 1000, 20, 0);
        runFuzz(TestUtils.generateRandom(LOG));
    }

    @Test
    public void testWalAddRemoveCommitFuzzO3() throws Exception {
        setFuzzProbabilities(0.05,
                0.2,
                0.1,
                0.005,
                0.05,
                0.05,
                0.05);
        setFuzzCounts(true, 100_000, 500, 20, 1000, 20, 100_000);
        runFuzz(TestUtils.generateRandom(LOG));
    }

    @Test
    public void testWalMetadataChangeHeavy() throws Exception {
        setFuzzProbabilities(0.05, 0.2, 0.1, 0.005, 0.25, 0.25, 0.25);
        setFuzzCounts(false, 100_000, 300, 20, 1000, 1000, 100);
        runFuzz(TestUtils.generateRandom(LOG));
    }

    @Test
    public void testWalWriteFullRandom() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        testWalWriteFullRandom(rnd);
    }

    public void testWalWriteFullRandom(Rnd rnd) throws Exception {
        // Some tests rely on the random generator call sequence. Do not refactor to use runFuzz()
        assertMemoryLeak(() -> {
            String tableNameWal = testName.getMethodName() + "_wal";
            String tableNameWal2 = testName.getMethodName() + "_wal_parallel";
            String tableNameNoWal = testName.getMethodName() + "_nonwal";

            createInitialTable(tableNameWal, true, 100);
            createInitialTable(tableNameWal2, true, 100);
            createInitialTable(tableNameNoWal, false, 100);

            ObjList<FuzzTransaction> transactions;
            int tableId1, tableId2;
            try (TableReader reader = new TableReader(configuration, tableNameWal)) {
                TableReaderMetadata metadata = reader.getMetadata();
                tableId1 = metadata.getTableId();
                transactions = FuzzTransactionGenerator.generateSet(
                        metadata,
                        rnd,
                        IntervalUtils.parseFloorPartialTimestamp("2022-02-24T17"),
                        IntervalUtils.parseFloorPartialTimestamp("2022-02-24T17") + (long) (Timestamps.DAY_MICROS * getZeroToOneDouble(rnd) * 30),
                        10_000,
                        rnd.nextBoolean(),
                        getZeroToOneDouble(rnd),
                        getZeroToOneDouble(rnd),
                        getZeroToOneDouble(rnd),
                        getZeroToOneDouble(rnd),
                        // 25% chance of column add
                        getZeroToOneDouble(rnd),
                        // 25% chance of column remove
                        getZeroToOneDouble(rnd),
                        // 25% chance of column rename
                        getZeroToOneDouble(rnd),
                        300,
                        rnd.nextInt(50),
                        generateSymbols(rnd, rnd.nextInt(1000), rnd.nextInt(1000), tableNameNoWal)
                );
            }
            try (TableReader reader = new TableReader(configuration, tableNameWal)) {
                TableReaderMetadata metadata = reader.getMetadata();
                tableId2 = metadata.getTableId();
            }

            applyNonWal(transactions, tableNameNoWal, tableId2);

            applyWal(transactions, tableNameWal, tableId1, getRndParallelWalCount(rnd));
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal, tableNameWal, LOG);

            applyWalParallel(transactions, tableNameWal2, tableId1, getRndParallelWalCount(rnd));
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal, tableNameWal2, LOG);
        });
    }

    @Test
    public void testWalWriteRollbackHeavy() throws Exception {
        Rnd rnd1 = TestUtils.generateRandom(LOG);
        setFuzzProbabilities(0.7, 0.5, 0.1, 0.7, 0.05, 0.05, 0.05);
        setFuzzCounts(rnd1.nextBoolean(), 10_000, 300, 20, 1000, 1000, 100);
        runFuzz(rnd1);
    }

    private static void applyNonWal(ObjList<FuzzTransaction> transactions, String tableName, int tableId) {
        try (TableWriterFrontend writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), tableName, "apply trans test")) {
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

    private void applyWal(ObjList<FuzzTransaction> transactions, String tableName, int tableId, int walWriterCount) {
        ObjList<WalWriter> writers = new ObjList<>();
        for (int i = 0; i < walWriterCount; i++) {
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

        for (int i = 0; i < walWriterCount; i++) {
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
                        while (job.run(0)) ;
                        Os.sleep(1);
                        i++;
                    }
                    while (job.run(0)) ;
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

        for (Throwable e : errors) {
            throw new RuntimeException(e);
        }

        try {
            applyThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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

    private String[] generateSymbols(Rnd rnd, int totalSymbols, int strLen, String baseSymbolTableName) {
        String[] symbols = new String[totalSymbols];
        int symbolIndex = 0;

        try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), baseSymbolTableName)) {
            TableReaderMetadata metadata = reader.getMetadata();
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                int columnType = metadata.getColumnType(i);
                if (ColumnType.isSymbol(columnType)) {
                    SymbolMapReader symbolReader = reader.getSymbolMapReader(i);
                    for (int sym = 0; symbolIndex < totalSymbols && sym < symbolReader.getSymbolCount() - 1; sym++) {
                        symbols[symbolIndex++] = Chars.toString(symbolReader.valueOf(sym));
                    }
                }
            }
        }

        for (; symbolIndex < totalSymbols; symbolIndex++) {
            symbols[symbolIndex] = Chars.toString(rnd.nextChars(rnd.nextInt(strLen)));
        }
        return symbols;
    }

    private int getRndParallelWalCount(Rnd rnd) {
        return 1 + rnd.nextInt(4);
    }

    private void runFuzz(Rnd rnd) throws Exception {
        assertMemoryLeak(() -> {

            String tableNameWal = testName.getMethodName() + "_wal";
            String tableNameWal2 = testName.getMethodName() + "_wal_parallel";
            String tableNameNoWal = testName.getMethodName() + "_nonwal";

            createInitialTable(tableNameWal, true, initialRowCount);
            createInitialTable(tableNameWal2, true, initialRowCount);
            createInitialTable(tableNameNoWal, false, initialRowCount);

            ObjList<FuzzTransaction> transactions;
            int tableId1, tableId2;
            try (TableReader reader = new TableReader(configuration, tableNameWal)) {
                TableReaderMetadata metadata = reader.getMetadata();
                tableId1 = metadata.getTableId();

                transactions = FuzzTransactionGenerator.generateSet(
                        metadata,
                        rnd,
                        IntervalUtils.parseFloorPartialTimestamp("2022-02-24T17"),
                        IntervalUtils.parseFloorPartialTimestamp("2022-02-27T17"),
                        fuzzRowCount,
                        isO3,
                        cancelRowsProb,
                        notSetProb,
                        nullSetProb,
                        rollbackProb,
                        // 25% chance of column add
                        collAddProb,
                        // 25% chance of column remove
                        collRemoveProb,
                        // 25% chance of column rename
                        colRenameProb,
                        transactionCount,
                        strLen,
                        generateSymbols(rnd, rnd.nextInt(symbolCountMax - 5) + 5, symbolStrLenMax, tableNameNoWal)
                );
            }
            try (TableReader reader = new TableReader(configuration, tableNameWal)) {
                TableReaderMetadata metadata = reader.getMetadata();
                tableId2 = metadata.getTableId();
            }

            applyNonWal(transactions, tableNameNoWal, tableId2);

            applyWal(transactions, tableNameWal, tableId1, getRndParallelWalCount(rnd));
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal, tableNameWal, LOG);

            applyWalParallel(transactions, tableNameWal2, tableId1, getRndParallelWalCount(rnd));
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal, tableNameWal2, LOG);
        });
    }

    private void setFuzzCounts(boolean isO31, int fuzzRowCount1, int transactionCount1, int strLen1, int symbolStrLenMax1, int symbolCountMax1, int initialRowCount1) {
        isO3 = isO31;
        fuzzRowCount = fuzzRowCount1;
        transactionCount = transactionCount1;
        strLen = strLen1;
        symbolStrLenMax = symbolStrLenMax1;
        symbolCountMax = symbolCountMax1;
        initialRowCount = initialRowCount1;
    }

    private void setFuzzProbabilities(double cancelRowsProb1, double notSetProb1, double nullSetProb1, double rollbackProb1, double collAddProb1, double collRemoveProb1, double colRenameProb1) {
        cancelRowsProb = cancelRowsProb1;
        notSetProb = notSetProb1;
        nullSetProb = nullSetProb1;
        rollbackProb = rollbackProb1;
        collAddProb = collAddProb1;
        collRemoveProb = collRemoveProb1;
        colRenameProb = colRenameProb1;
    }
}
