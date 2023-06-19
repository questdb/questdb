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
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.mp.WorkerPool;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// These test is designed to produce unstable runs, e.g. random generator is created
// using current execution time.
// This improves coverage. To debug failures in CI find the line logging random seeds
// and change line
// Rnd rnd = generateRandom(LOG);
// to
// Rnd rnd = new Rnd(A, B);
// where A, B are seeds in the failed run log.
//
// When the same timestamp is used in multiple transactions
// the order of records when executed in parallel WAL writing is not guaranteed.
// The creates failures in tests that assume that the order of records is preserved.
// There are already measures to prevent invalid data generation, but it still can happen.
// In order to verify that the test is not broken we check that there are no duplicate
// timestamps for the record where the comparison fails.
public class WalWriterFuzzTest extends AbstractFuzzTest {


    protected final WorkerPool sharedWorkerPool = new TestWorkerPool(4, metrics);
    private final TableSequencerAPI.TableSequencerCallback checkNoSuspendedTablesRef = WalWriterFuzzTest::checkNoSuspendedTables;


    @BeforeClass
    public static void setUpStatic() throws Exception {
        walTxnNotificationQueueCapacity = 16;
        AbstractGriffinTest.setUpStatic();
    }

    @Before
    public void setUp() {
        configOverrideO3ColumnMemorySize(512 * 1024);
        setFuzzProperties(100, 1000, 2);
        super.setUp();
    }

    @Test
    public void testSimpleDataTransaction() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0, 0.2, 0.1, 0, 0, 0, 0, 1.0, 0.01, 0.01);
        setFuzzCounts(rnd.nextBoolean(), rnd.nextInt(10_000_000),
                rnd.nextInt(1500), 20, 10, 200, 0, 1);
        runFuzz(rnd);
    }

    @Test
    public void testWalAddRemoveCommitFuzzInOrder() throws Exception {
        setFuzzProbabilities(0.05, 0.2, 0.1, 0.005, 0.05, 0.05, 0.05, 1.0, 0.01, 0.01);
        setFuzzCounts(false, 1_000_000, 500, 20, 1000, 20, 0, 10);
        runFuzz(generateRandom(LOG));
    }

    @Test
    public void testWalAddRemoveCommitFuzzO3() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0.05, 0.2, 0.1, 0.005, 0.05, 0.05, 0.05, 1.0, 0.05, 0.01);
        setFuzzCounts(true, 100_000, 500, 20, 1000, 20, 100_000, 5);
        setFuzzProperties(rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        runFuzz(rnd);
    }

    @Test
    public void testWalApplyEjectsMultipleTables() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProperties(rnd.nextLong(50), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        int tableCount = Math.max(2, rnd.nextInt(3));
        fullRandomFuzz(rnd, tableCount);
    }

    @Test
    public void testWalMetadataChangeHeavy() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0.05, 0.2, 0.1, 0.005, 0.25, 0.25, 0.25, 1.0, 0.01, 0.01);
        setFuzzCounts(false, 50_000, 100, 20, 1000, 1000, 100, 5);
        setFuzzProperties(rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        runFuzz(rnd);
    }

    @Test
    public void testWalWriteEqualTimestamp() throws Exception {
        configOverrideO3QuickSortEnabled(true);
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0, 0, 0, 0, 0, 0, 0, 1, 0, 0.5);
        setFuzzCounts(
                true,
                5000,
                800,
                10,
                10,
                10,
                50,
                1
        );
        setFuzzProperties(rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        runFuzz(rnd, testName.getMethodName(), 1, false, false);
    }

    @Test
    public void testWalWriteFullRandom() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setRandomAppendPageSize(rnd);
        setFuzzProperties(rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        fullRandomFuzz(rnd);
    }

    @Test
    public void testWalWriteFullRandomMultipleTables() throws Exception {
        Rnd rnd = generateRandom(LOG);
        int tableCount = Math.max(2, rnd.nextInt(4));
        setFuzzProperties(rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        fullRandomFuzz(rnd, tableCount);
    }

    @Test
    public void testWalWriteManySmallTransactions() throws Exception {
        configOverrideO3QuickSortEnabled(true);
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0, 0, 0, 0, 0, 0, 0, 1, 0, 0.01);
        setFuzzCounts(
                true,
                1000,
                800,
                10,
                10,
                10,
                50,
                1
        );
        setFuzzProperties(rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        runFuzz(rnd, testName.getMethodName(), 1, false, false);
    }

    @Test
    public void testWalWriteManyTablesInOrder() throws Exception {
        configOverrideO3QuickSortEnabled(true);
        Rnd rnd = generateRandom(LOG);
        setRandomAppendPageSize(rnd);
        int tableCount = 3;
        setFuzzProbabilities(0, 0, 0, 0, 0, 0, 0, 1, 0.001, 0.01);
        setFuzzCounts(false, 500_000, 5_000, 10, 10, 5500, 0, 1);
        String tableNameBase = testName.getMethodName();
        runFuzz(rnd, tableNameBase, tableCount, false, false);
    }

    @Test
    public void testWalWriteRollbackHeavy() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0.5, 0.5, 0.1, 0.5, 0.05, 0.05, 0.05, 1.0, 0.01, 0.01);
        setFuzzCounts(rnd.nextBoolean(), 10_000, 300, 20, 1000, 1000, 100, 3);
        runFuzz(rnd);
    }

    @Test
    public void testWalWriteRollbackTruncateHeavy() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0.5, 0.5, 0.1, 0.5, 0.05, 0.05, 0.05, 1.0, 0.15, 0.01);
        setFuzzCounts(rnd.nextBoolean(), 300, 20, 20, 1000, 1000, 100, 3);
        runFuzz(rnd);
    }

    @Test
    public void testWalWriteTinyO3Memory() throws Exception {
        final int o3MemorySize = 256;
        configOverrideO3ColumnMemorySize(o3MemorySize);
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0, 0.2, 0.1, 0, 0, 0, 0, 1.0, 0.01, 0.01);
        setFuzzCounts(true, 100_000, 10, 10, 10, 10, 50, 1);
        runFuzz(rnd, testName.getMethodName(), 1, false, false);
        Assert.assertEquals(o3MemorySize, node1.getConfigurationOverrides().getO3ColumnMemorySize());
    }

    @Test
    public void testWriteO3DataOnlyBig() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(0, 0, 0, 0, 0, 0, 0, 1.0, 0.01, 0.01);
        setFuzzCounts(true, 1_000_000, 500, 20, 1000, 1000, 100, 20);
        setFuzzProperties(rnd.nextLong(MAX_WAL_APPLY_TIME_PER_TABLE_CEIL), getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd));
        runFuzz(rnd);
    }

    private static void checkNoSuspendedTables(int tableId, TableToken tableName, long lastTxn) {
        Assert.assertFalse(tableName.getTableName(), engine.getTableSequencerAPI().isSuspended(tableName));
    }

    @NotNull
    private static Thread createWalWriteThread(
            ObjList<FuzzTransaction> transactions,
            String tableName,
            ObjList<WalWriter> writers,
            AtomicLong waitBarrierVersion,
            AtomicLong doneCount,
            AtomicInteger nextOperation,
            ConcurrentLinkedQueue<Throwable> errors
    ) {
        final WalWriter walWriter = (WalWriter) engine.getTableWriterAPI(tableName, "apply trans test");
        writers.add(walWriter);

        return new Thread(() -> {
            int opIndex;

            try {
                Rnd tempRnd = new Rnd();
                while ((opIndex = nextOperation.incrementAndGet()) < transactions.size() && errors.size() == 0) {
                    FuzzTransaction transaction = transactions.getQuick(opIndex);

                    // wait until structure version, truncate is applied
                    while (waitBarrierVersion.get() < transaction.waitBarrierVersion && errors.size() == 0) {
                        Os.sleep(1);
                    }

                    if (transaction.waitAllDone) {
                        while (doneCount.get() != opIndex) {
                            Os.sleep(1);
                        }
                    }

                    if (!walWriter.goActive(transaction.structureVersion)) {
                        throw CairoException.critical(0).put("cannot apply structure change");
                    }
                    if (walWriter.getMetadataVersion() != transaction.structureVersion) {
                        throw CairoException.critical(0)
                                .put("cannot update wal writer to correct structure version");
                    }

                    boolean increment = false;
                    for (int operationIndex = 0; operationIndex < transaction.operationList.size(); operationIndex++) {
                        FuzzTransactionOperation operation = transaction.operationList.getQuick(operationIndex);
                        increment |= operation.apply(tempRnd, walWriter, -1);
                    }

                    if (transaction.rollback) {
                        walWriter.rollback();
                    } else {
                        walWriter.commit();
                    }
                    if (increment || transaction.waitAllDone) {
                        waitBarrierVersion.incrementAndGet();
                    }

                    doneCount.incrementAndGet();

                    // CREATE TABLE may release all inactive sequencers occasionally, so we do the same
                    // to make sure that there are no races between WAL writers and the engine.
                    engine.releaseInactiveTableSequencers();
                }
            } catch (Throwable e) {
                errors.add(e);
            } finally {
                Path.clearThreadLocals();
            }
        });
    }

    private static void forceReleaseTableWriter(Rnd applyRnd) {
        // Sometimes WAL Apply Job does not finish table in one go and return TableWriter to the pool
        // where it can be fully closed before continuing the WAL application Test TableWriter closures.
        if (applyRnd.nextDouble() < 0.8) {
            engine.releaseAllWriters();
        }
    }

    private static String getWalParallelApplyTableName(String tableNameBase, int i) {
        return tableNameBase + "_" + i + "_wal_parallel";
    }

    private void applyManyWalParallel(ObjList<ObjList<FuzzTransaction>> fuzzTransactions, Rnd rnd, String tableNameBase, boolean multiTable) {
        ObjList<WalWriter> writers = new ObjList<>();
        int tableCount = fuzzTransactions.size();
        AtomicInteger done = new AtomicInteger();
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        ObjList<Thread> threads = new ObjList<>();

        for (int i = 0; i < tableCount; i++) {
            String tableName = multiTable ? getWalParallelApplyTableName(tableNameBase, i) : tableNameBase;
            AtomicLong waitBarrierVersion = new AtomicLong();
            int parallelWalCount = Math.max(2, rnd.nextInt(5));
            AtomicInteger nextOperation = new AtomicInteger(-1);
            ObjList<FuzzTransaction> transactions = fuzzTransactions.get(i);
            AtomicLong doneCount = new AtomicLong();

            for (int j = 0; j < parallelWalCount; j++) {
                threads.add(createWalWriteThread(transactions, tableName, writers, waitBarrierVersion, doneCount, nextOperation, errors));
                threads.getLast().start();
            }
        }

        ObjList<Thread> applyThreads = new ObjList<>();
        int applyThreadCount = Math.max(fuzzTransactions.size(), 4);
        for (int thread = 0; thread < applyThreadCount; thread++) {
            final Rnd threadApplyRnd = new Rnd(rnd.getSeed0(), rnd.getSeed1());
            Thread applyThread = new Thread(() -> runApplyThread(done, errors, threadApplyRnd));
            applyThread.start();
            applyThreads.add(applyThread);
        }

        Thread purgeJobThread = new Thread(() -> runWalPurgeJob(done, errors));
        purgeJobThread.start();
        applyThreads.add(purgeJobThread);

        Thread purgePartitionThread = new Thread(() -> runPurgePartitionJob(done, errors, new Rnd(rnd.nextLong(), rnd.nextLong()), tableNameBase, tableCount, multiTable));
        purgePartitionThread.start();
        applyThreads.add(purgePartitionThread);

        for (int i = 0; i < threads.size(); i++) {
            int k = i;
            TestUtils.unchecked(() -> threads.get(k).join());
        }

        done.incrementAndGet();
        Misc.freeObjList(writers);

        for (Throwable e : errors) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < applyThreads.size(); i++) {
            int k = i;
            TestUtils.unchecked(() -> applyThreads.get(k).join());
        }
    }

    private void applyWal(ObjList<FuzzTransaction> transactions, String tableName, int walWriterCount, Rnd applyRnd) {
        ObjList<WalWriter> writers = new ObjList<>();
        for (int i = 0; i < walWriterCount; i++) {
            writers.add((WalWriter) engine.getTableWriterAPI(tableName, "apply trans test"));
        }

        Rnd tempRnd = new Rnd();
        for (int i = 0, n = transactions.size(); i < n; i++) {
            WalWriter writer = writers.getQuick(applyRnd.nextPositiveInt() % walWriterCount);
            writer.goActive();
            FuzzTransaction transaction = transactions.getQuick(i);
            for (int operationIndex = 0; operationIndex < transaction.operationList.size(); operationIndex++) {
                FuzzTransactionOperation operation = transaction.operationList.getQuick(operationIndex);
                operation.apply(tempRnd, writer, -1);
            }

            if (transaction.rollback) {
                writer.rollback();
            } else {
                writer.commit();
            }
        }

        Misc.freeObjList(writers);
        drainWalQueue(applyRnd, tableName);
    }

    private void applyWalParallel(ObjList<FuzzTransaction> transactions, String tableName, Rnd applyRnd) {
        ObjList<ObjList<FuzzTransaction>> tablesTransactions = new ObjList<>();
        tablesTransactions.add(transactions);
        applyManyWalParallel(tablesTransactions, applyRnd, tableName, false);
    }

    private void checkNoSuspendedTables(ObjHashSet<TableToken> tableTokenBucket) {
        engine.getTableSequencerAPI().forAllWalTables(tableTokenBucket, false, checkNoSuspendedTablesRef);
    }

    @NotNull
    private ObjList<FuzzTransaction> createTransactions(Rnd rnd, String tableNameBase) throws SqlException, NumericException {
        String tableNameNoWal = tableNameBase + "_nonwal";
        String tableNameWal = tableNameBase + "_wal_parallel";

        createInitialTable(tableNameNoWal, false, initialRowCount);
        createInitialTable(tableNameWal, true, initialRowCount);

        ObjList<FuzzTransaction> transactions;
        try (TableReader reader = newTableReader(configuration, tableNameNoWal)) {
            TableReaderMetadata metadata = reader.getMetadata();

            long start = IntervalUtils.parseFloorPartialTimestamp("2022-02-24T17");
            long end = start + partitionCount * Timestamps.DAY_MICROS;
            transactions = generateSet(rnd, metadata, start, end, tableNameNoWal);
        }

        applyNonWal(transactions, tableNameNoWal, rnd);

        // Release TW to reduce memory pressure
        engine.releaseInactive();

        return transactions;
    }

    private void drainWalQueue(Rnd applyRnd, String tableName) {
        try (ApplyWal2TableJob walApplyJob = createWalApplyJob();
             O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine.getMessageBus(), 1);
             TableReader rdr1 = getReader(tableName);
             TableReader rdr2 = getReader(tableName)
        ) {
            CheckWalTransactionsJob checkWalTransactionsJob = new CheckWalTransactionsJob(engine);
            while (walApplyJob.run(0) || checkWalTransactionsJob.run(0)) {
                forceReleaseTableWriter(applyRnd);
                purgeAndReloadReaders(applyRnd, rdr1, rdr2, purgeJob, 0.25);
            }
        }
    }

    private void fullRandomFuzz(Rnd rnd) throws Exception {
        setFuzzProbabilities(
                0.5 * rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.5 * rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.1 * rnd.nextDouble(), 0.01
        );

        setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(2_000_000),
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                rnd.nextInt(1_000_000),
                5 + rnd.nextInt(10)
        );

        runFuzz(rnd);
    }

    private void fullRandomFuzz(Rnd rnd, int tableCount) throws Exception {
        runFuzz(rnd, testName.getMethodName(), tableCount, true, true);
    }

    private int getRndParallelWalCount(Rnd rnd) {
        return 1 + rnd.nextInt(4);
    }

    private void runApplyThread(AtomicInteger done, ConcurrentLinkedQueue<Throwable> errors, Rnd applyRnd) {
        try {
            ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
            int i = 0;
            CheckWalTransactionsJob checkJob = new CheckWalTransactionsJob(engine);
            try (ApplyWal2TableJob job = new ApplyWal2TableJob(engine, 1, 1, null)) {
                while (done.get() == 0 && errors.size() == 0) {
                    Unsafe.getUnsafe().loadFence();
                    while (job.run(0) || checkJob.run(0)) {
                        // Sometimes WAL Apply Job does not finish table in one go and return TableWriter to the pool
                        // where it can be fully closed before continuing the WAL application Test TableWriter closures.
                        forceReleaseTableWriter(applyRnd);
                    }
                    Os.sleep(1);
                    checkNoSuspendedTables(tableTokenBucket);
                    i++;
                }
                while (job.run(0) || checkJob.run(0)) {
                    forceReleaseTableWriter(applyRnd);
                }
                i++;
            }
            LOG.info().$("finished apply thread after iterations: ").$(i).$();
        } catch (Throwable e) {
            errors.add(e);
        } finally {
            Path.clearThreadLocals();
        }
    }

    private void runFuzz(Rnd rnd) throws Exception {
        configOverrideO3ColumnMemorySize(rnd.nextInt(16 * 1024 * 1024));

        assertMemoryLeak(() -> {
            String tableNameBase = testName.getMethodName();
            String tableNameWal = tableNameBase + "_wal";
            String tableNameWal2 = tableNameBase + "_wal_parallel";
            String tableNameNoWal = tableNameBase + "_nonwal";

            createInitialTable(tableNameWal, true, initialRowCount);
            createInitialTable(tableNameWal2, true, initialRowCount);
            createInitialTable(tableNameNoWal, false, initialRowCount);

            ObjList<FuzzTransaction> transactions;
            try (TableReader reader = newTableReader(configuration, tableNameWal)) {
                TableReaderMetadata metadata = reader.getMetadata();

                long start = IntervalUtils.parseFloorPartialTimestamp("2022-02-24T17");
                long end = start + partitionCount * Timestamps.DAY_MICROS;
                transactions = generateSet(rnd, metadata, start, end, tableNameNoWal);
            }

            O3Utils.setupWorkerPool(sharedWorkerPool, engine, null, null);
            sharedWorkerPool.start(LOG);

            try {
                long startMicro = System.nanoTime() / 1000;
                applyNonWal(transactions, tableNameNoWal, rnd);
                long endNonWalMicro = System.nanoTime() / 1000;
                long nonWalTotal = endNonWalMicro - startMicro;

                applyWal(transactions, tableNameWal, getRndParallelWalCount(rnd), rnd);

                long endWalMicro = System.nanoTime() / 1000;
                long walTotal = endWalMicro - endNonWalMicro;

                String limit = "";
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal + limit, tableNameWal + limit, LOG);
                assertRandomIndexes(tableNameNoWal, tableNameWal, rnd);

                startMicro = System.nanoTime() / 1000;
                applyWalParallel(transactions, tableNameWal2, rnd);
                endWalMicro = System.nanoTime() / 1000;
                long totalWalParallel = endWalMicro - startMicro;

                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal, tableNameWal2, LOG);
                assertRandomIndexes(tableNameNoWal, tableNameWal2, rnd);

                LOG.infoW().$("=== non-wal(ms): ").$(nonWalTotal / 1000).$(" === wal(ms): ").$(walTotal / 1000).$(" === wal_parallel(ms): ").$(totalWalParallel / 1000).$();
            } finally {
                sharedWorkerPool.halt();
            }
        });
    }

    private void runFuzz(Rnd rnd, String tableNameBase, int tableCount, boolean randomiseProbs, boolean randomiseCounts) throws Exception {
        assertMemoryLeak(() -> {
            ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
            for (int i = 0; i < tableCount; i++) {
                String tableNameWal = tableNameBase + "_" + i;
                if (randomiseProbs) {
                    setFuzzProbabilities(
                            0.5 * rnd.nextDouble(),
                            rnd.nextDouble(),
                            rnd.nextDouble(),
                            0.5 * rnd.nextDouble(),
                            rnd.nextDouble(),
                            rnd.nextDouble(),
                            rnd.nextDouble(),
                            rnd.nextDouble(),
                            0.1 * rnd.nextDouble(), 0.01
                    );
                }
                if (randomiseCounts) {
                    setFuzzCounts(
                            rnd.nextBoolean(),
                            rnd.nextInt(2_000_000),
                            rnd.nextInt(1000),
                            rnd.nextInt(1000),
                            rnd.nextInt(1000),
                            rnd.nextInt(1000),
                            rnd.nextInt(1_000_000),
                            5 + rnd.nextInt(10)
                    );
                }

                ObjList<FuzzTransaction> transactions = createTransactions(rnd, tableNameWal);
                fuzzTransactions.add(transactions);
            }
            // Can help to reduce memory consumption.
            engine.releaseInactive();

            O3Utils.setupWorkerPool(sharedWorkerPool, engine, null, null);
            sharedWorkerPool.start(LOG);
            try {
                applyManyWalParallel(fuzzTransactions, rnd, tableNameBase, true);
            } finally {
                sharedWorkerPool.halt();
            }

            checkNoSuspendedTables(new ObjHashSet<>());

            for (int i = 0; i < tableCount; i++) {
                String tableNameNoWal = tableNameBase + "_" + i + "_nonwal";
                String tableNameWal = getWalParallelApplyTableName(tableNameBase, i);
                LOG.infoW().$("comparing tables ").$(tableNameNoWal).$(" and ").$(tableNameWal).$();
                String limit = "";
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal + limit, tableNameWal + limit, LOG);
                assertRandomIndexes(tableNameNoWal, tableNameWal, rnd);
            }
        });
    }

    private void runPurgePartitionJob(AtomicInteger done, ConcurrentLinkedQueue<Throwable> errors, Rnd runRnd, String tableNameBase, int tableCount, boolean multiTable) {
        ObjList<TableReader> readers = new ObjList<>();
        try {
            node1.getConfigurationOverrides().setWalPurgeInterval(0L);
            try (O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine.getMessageBus(), 1)) {
                for (int i = 0; i < tableCount; i++) {
                    String tableNameWal = multiTable ? getWalParallelApplyTableName(tableNameBase, i) : tableNameBase;
                    readers.add(getReader(tableNameWal));
                    readers.add(getReader(tableNameWal));
                }

                while (done.get() == 0 && errors.size() == 0) {
                    int reader = runRnd.nextInt(tableCount);
                    purgeAndReloadReaders(runRnd, readers.get(reader * 2), readers.get(reader * 2 + 1), purgeJob, 0.25);
                    Os.sleep(50);
                }
            }
        } catch (Throwable e) {
            errors.add(e);
        } finally {
            Misc.freeObjList(readers);
            Path.clearThreadLocals();
        }
    }

    private void runWalPurgeJob(AtomicInteger done, ConcurrentLinkedQueue<Throwable> errors) {
        try {
            node1.getConfigurationOverrides().setWalPurgeInterval(0L);
            try (WalPurgeJob job = new WalPurgeJob(engine)) {
                while (done.get() == 0 && errors.size() == 0) {
                    job.drain(0);
                    Os.sleep(1);
                }
            }
        } catch (Throwable e) {
            errors.add(e);
        } finally {
            Path.clearThreadLocals();
        }
    }
}
