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

package io.questdb.griffin.wal;

import io.questdb.cairo.*;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.wal.fuzz.FuzzTransaction;
import io.questdb.griffin.wal.fuzz.FuzzTransactionGenerator;
import io.questdb.griffin.wal.fuzz.FuzzTransactionOperation;
import io.questdb.mp.TestWorkerPool;
import io.questdb.mp.WorkerPool;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.*;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// These test is designed to produce unstable runs, e.g. random generator is created
// using current execution time.
// This improves coverage. To debug failures in CI find the line logging random seeds
// and change line
// Rnd rnd = TestUtils.generateRandom(LOG);
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

public class WalWriterFuzzTest extends AbstractGriffinTest {

    protected final WorkerPool sharedWorkerPool = new TestWorkerPool(4, metrics);
    private final TableSequencerAPI.TableSequencerCallback checkNoSuspendedTablesRef = WalWriterFuzzTest::checkNoSuspendedTables;
    private double cancelRowsProb;
    private double colRenameProb;
    private double collAddProb;
    private double collRemoveProb;
    private double dataAddProb;
    private int fuzzRowCount;
    private int initialRowCount;
    private boolean isO3;
    private double notSetProb;
    private double nullSetProb;
    private int partitionCount;
    private double rollbackProb;
    private int strLen;
    private int symbolCountMax;
    private int symbolStrLenMax;
    private int transactionCount;
    private double truncateProb;

    @BeforeClass
    public static void setUpStatic() {
        walTxnNotificationQueueCapacity = 16;
        AbstractGriffinTest.setUpStatic();
    }

    @Before
    public void setUp() {
        configOverrideO3ColumnMemorySize(512 * 1024);
        super.setUp();
    }

    @Test
    public void testSimpleDataTransaction() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setFuzzProbabilities(0, 0.2, 0.1, 0, 0, 0, 0, 1.0, 0.01);
        setFuzzCounts(rnd.nextBoolean(), rnd.nextInt(10_000_000),
                rnd.nextInt(1500), 20, 10, 200, 0, 1);
        runFuzz(TestUtils.generateRandom(LOG));
    }

    @Test
    public void testWalAddRemoveCommitFuzzInOrder() throws Exception {
        setFuzzProbabilities(0.05, 0.2, 0.1, 0.005, 0.05, 0.05, 0.05, 1.0, 0.01);
        setFuzzCounts(false, 1_000_000, 500, 20, 1000, 20, 0, 10);
        runFuzz(TestUtils.generateRandom(LOG));
    }

    @Test
    public void testWalAddRemoveCommitFuzzO3() throws Exception {
        setFuzzProbabilities(0.05, 0.2, 0.1, 0.005, 0.05, 0.05, 0.05, 1.0, 0.05);
        setFuzzCounts(true, 100_000, 500, 20, 1000, 20, 100_000, 5);
        runFuzz(TestUtils.generateRandom(LOG));
    }

    @Test
    public void testWalMetadataChangeHeavy() throws Exception {
        setFuzzProbabilities(0.05, 0.2, 0.1, 0.005, 0.25, 0.25, 0.25, 1.0, 0.01);
        setFuzzCounts(false, 50_000, 100, 20, 1000, 1000, 100, 5);
        runFuzz(TestUtils.generateRandom(LOG));
    }

    @Test
    public void testWalWriteFullRandom() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setRandomAppendPageSize(rnd);
        fullRandomFuzz(rnd);
    }

    @Test
    public void testWalWriteFullRandomMultipleTables() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        int tableCount = Math.max(2, rnd.nextInt(3));
        fullRandomFuzz(rnd, tableCount);
    }

    @Test
    public void testWalWriteManyTablesInOrder() throws Exception {
        configOverrideO3QuickSortEnabled(true);
        Rnd rnd = TestUtils.generateRandom(LOG);
        setRandomAppendPageSize(rnd);
        long s0 = rnd.getSeed0();
        long s1 = rnd.getSeed1();
        int tableCount = 3;
        try {
            setFuzzProbabilities(0, 0, 0, 0, 0, 0, 0, 1, 0.001);
            setFuzzCounts(false, 500_000, 5_000, 10, 10, 5500, 0, 1);
            String tableNameBase = testName.getMethodName();
            runFuzz(rnd, tableNameBase, tableCount, false, false);
        } finally {
            System.out.printf("random seeds: %dL, %dL%n", s0, s1);
        }
    }

    @Test
    public void testWalWriteRollbackHeavy() throws Exception {
        Rnd rnd1 = TestUtils.generateRandom(LOG);
        setFuzzProbabilities(0.5, 0.5, 0.1, 0.5, 0.05, 0.05, 0.05, 1.0, 0.01);
        setFuzzCounts(rnd1.nextBoolean(), 10_000, 300, 20, 1000, 1000, 100, 3);
        runFuzz(rnd1);
    }

    @Test
    public void testWalWriteRollbackHeavyToFix() throws Exception {
        Rnd rnd1 = TestUtils.generateRandom(LOG);
        setFuzzProbabilities(0.5, 0.5, 0.1, 0.5, 0.05, 0.05, 0.05, 1.0, 0.01);
        setFuzzCounts(rnd1.nextBoolean(), 10_000, 300, 20, 1000, 1000, 100, 3);
        runFuzz(rnd1);
    }

    @Test
    public void testWalWriteRollbackTruncateHeavy() throws Exception {
        Rnd rnd1 = TestUtils.generateRandom(LOG);
        setFuzzProbabilities(0.5, 0.5, 0.1, 0.5, 0.05, 0.05, 0.05, 1.0, 0.15);
        setFuzzCounts(rnd1.nextBoolean(), 300, 20, 20, 1000, 1000, 100, 3);
        runFuzz(rnd1);
    }

    @Test
    public void testWalWriteWithQuickSortEnabled() throws Exception {
        configOverrideO3QuickSortEnabled(true);
        Rnd rnd = new Rnd();
        int tableCount = Math.max(2, rnd.nextInt(10));
        setFuzzProbabilities(0, 0, 0, 0, 0, 0, 0, 1, 0);
        setFuzzCounts(
                true,
                1000,
                30,
                20,
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                50,
                3 + rnd.nextInt(20)
        );
        runFuzz(rnd, testName.getMethodName(), tableCount, false, true);
    }

    @Test
    public void testWriteO3DataOnlyBig() throws Exception {
        setFuzzProbabilities(0, 0, 0, 0, 0, 0, 0, 1.0, 0.01);
        setFuzzCounts(true, 1_000_000, 500, 20, 1000, 1000, 100, 20);
        runFuzz(TestUtils.generateRandom(LOG));
    }

    private static void applyNonWal(ObjList<FuzzTransaction> transactions, String tableName) {
        try (TableWriterAPI writer = getWriter(tableName)) {
            int transactionSize = transactions.size();
            Rnd rnd = new Rnd();
            for (int i = 0; i < transactionSize; i++) {
                FuzzTransaction transaction = transactions.getQuick(i);
                int size = transaction.operationList.size();
                for (int operationIndex = 0; operationIndex < size; operationIndex++) {
                    FuzzTransactionOperation operation = transaction.operationList.getQuick(operationIndex);
                    operation.apply(rnd, writer, -1);
                }

                if (transaction.rollback) {
                    writer.rollback();
                } else {
                    writer.commit();
                }
            }
        }
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
        TableToken tableToken = engine.getTableToken(tableName);
        final WalWriter walWriter = (WalWriter) engine.getTableWriterAPI(sqlExecutionContext.getCairoSecurityContext(), tableToken, "apply trans test");
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
                    if (walWriter.getStructureVersion() != transaction.structureVersion) {
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
        engine.releaseAllWriters();
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

        for (int i = 0; i < threads.size(); i++) {
            try {
                threads.get(i).join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        done.incrementAndGet();
        Misc.freeObjList(writers);

        for (Throwable e : errors) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < applyThreads.size(); i++) {
            try {
                applyThreads.get(i).join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void applyWal(ObjList<FuzzTransaction> transactions, String tableName, int walWriterCount, Rnd applyRnd) {
        ObjList<WalWriter> writers = new ObjList<>();
        for (int i = 0; i < walWriterCount; i++) {
            TableToken token = engine.getTableToken(tableName);
            writers.add((WalWriter) engine.getTableWriterAPI(sqlExecutionContext.getCairoSecurityContext(), token, "apply trans test"));
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
        drainWalQueue(applyRnd);
    }

    private void applyWalParallel(ObjList<FuzzTransaction> transactions, String tableName, Rnd applyRnd) {
        ObjList<ObjList<FuzzTransaction>> tablesTransactions = new ObjList<>();
        tablesTransactions.add(transactions);
        applyManyWalParallel(tablesTransactions, applyRnd, tableName, false);
    }

    private void checkNoSuspendedTables(ObjList<TableToken> tableTokenBucket) {
        engine.getTableSequencerAPI().forAllWalTables(tableTokenBucket, false, checkNoSuspendedTablesRef);
    }

    private void createInitialTable(String tableName1, boolean isWal, int rowCount) throws SqlException {
        SharedRandom.RANDOM.set(new Rnd());
        compile("create table " + tableName1 + " as (" +
                "select x as c1, " +
                " rnd_symbol('AB', 'BC', 'CD') c2, " +
                " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                " rnd_symbol('DE', null, 'EF', 'FG') sym2," +
                " cast(x as int) c3," +
                " rnd_bin() c4," +
                " to_long128(3 * x, 6 * x) c5," +
                " rnd_str('a', 'bdece', null, ' asdflakji idid', 'dk')," +
                " rnd_boolean() bool1 " +
                " from long_sequence(" + rowCount + ")" +
                ") timestamp(ts) partition by DAY " + (isWal ? "WAL" : "BYPASS WAL"));
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
            transactions = FuzzTransactionGenerator.generateSet(
                    metadata,
                    rnd,
                    start,
                    end,
                    fuzzRowCount,
                    transactionCount,
                    isO3,
                    cancelRowsProb,
                    notSetProb,
                    nullSetProb,
                    rollbackProb,
                    collAddProb,
                    collRemoveProb,
                    colRenameProb,
                    dataAddProb,
                    truncateProb,
                    strLen,
                    generateSymbols(rnd, rnd.nextInt(Math.max(1, symbolCountMax - 5)) + 5, symbolStrLenMax, tableNameNoWal)
            );
        }

        applyNonWal(transactions, tableNameNoWal);
        return transactions;
    }

    private void drainWalQueue(Rnd applyRnd) {
        try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
            CheckWalTransactionsJob checkWalTransactionsJob = new CheckWalTransactionsJob(engine);
            while (walApplyJob.run(0) || checkWalTransactionsJob.run(0)) {
                forceReleaseTableWriter(applyRnd);
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
                0.1 * rnd.nextDouble()
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

    private String[] generateSymbols(Rnd rnd, int totalSymbols, int strLen, String baseSymbolTableName) {
        String[] symbols = new String[totalSymbols];
        int symbolIndex = 0;

        try (TableReader reader = getReader(baseSymbolTableName)) {
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
            symbols[symbolIndex] = strLen > 0 ? Chars.toString(rnd.nextChars(rnd.nextInt(strLen))) : "";
        }
        return symbols;
    }

    private int getRndParallelWalCount(Rnd rnd) {
        return 1 + rnd.nextInt(4);
    }

    private void runApplyThread(AtomicInteger done, ConcurrentLinkedQueue<Throwable> errors, Rnd applyRnd) {
        try {
            ObjList<TableToken> tableTokenBucket = new ObjList<>();
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
                transactions = FuzzTransactionGenerator.generateSet(
                        metadata,
                        rnd,
                        start,
                        end,
                        fuzzRowCount,
                        transactionCount,
                        isO3,
                        cancelRowsProb,
                        notSetProb,
                        nullSetProb,
                        rollbackProb,
                        collAddProb,
                        collRemoveProb,
                        colRenameProb,
                        dataAddProb,
                        truncateProb,
                        strLen,
                        generateSymbols(rnd, rnd.nextInt(Math.max(1, symbolCountMax - 5)) + 5, symbolStrLenMax, tableNameNoWal)
                );
            }

            O3Utils.setupWorkerPool(sharedWorkerPool, engine, null, null);
            sharedWorkerPool.start(LOG);

            try {
                long startMicro = System.nanoTime() / 1000;
                applyNonWal(transactions, tableNameNoWal);
                long endNonWalMicro = System.nanoTime() / 1000;
                long nonWalTotal = endNonWalMicro - startMicro;

                applyWal(transactions, tableNameWal, getRndParallelWalCount(rnd), rnd);

                long endWalMicro = System.nanoTime() / 1000;
                long walTotal = endWalMicro - endNonWalMicro;

                String limit = "";
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal + limit, tableNameWal + limit, LOG);

                startMicro = System.nanoTime() / 1000;
                applyWalParallel(transactions, tableNameWal2, rnd);
                endWalMicro = System.nanoTime() / 1000;
                long totalWalParallel = endWalMicro - startMicro;

                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal, tableNameWal2, LOG);

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
                            0.1 * rnd.nextDouble());
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

            checkNoSuspendedTables(new ObjList<>());
            for (int i = 0; i < tableCount; i++) {
                String tableNameNoWal = tableNameBase + "_" + i + "_nonwal";
                String tableNameWal = getWalParallelApplyTableName(tableNameBase, i);
                LOG.infoW().$("comparing tables ").$(tableNameNoWal).$(" and ").$(tableNameWal).$();
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal, tableNameWal, LOG);
            }
        });
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

    private void setFuzzCounts(boolean isO3, int fuzzRowCount, int transactionCount, int strLen, int symbolStrLenMax, int symbolCountMax, int initialRowCount, int partitionCount) {
        this.isO3 = isO3;
        this.fuzzRowCount = fuzzRowCount;
        this.transactionCount = transactionCount;
        this.strLen = strLen;
        this.symbolStrLenMax = symbolStrLenMax;
        this.symbolCountMax = symbolCountMax;
        this.initialRowCount = initialRowCount;
        this.partitionCount = partitionCount;
    }

    private void setFuzzProbabilities(double cancelRowsProb, double notSetProb, double nullSetProb, double rollbackProb, double collAddProb, double collRemoveProb, double colRenameProb, double dataAddProb, double truncateProb) {
        this.cancelRowsProb = cancelRowsProb;
        this.notSetProb = notSetProb;
        this.nullSetProb = nullSetProb;
        this.rollbackProb = rollbackProb;
        this.collAddProb = collAddProb;
        this.collRemoveProb = collRemoveProb;
        this.colRenameProb = colRenameProb;
        this.dataAddProb = dataAddProb;
        this.truncateProb = truncateProb;
    }

    private void setRandomAppendPageSize(Rnd rnd) {
        int minPage = 18;
        dataAppendPageSize = 1L << (minPage + rnd.nextInt(22 - minPage)); // MAX page size 4Mb
        LOG.info().$("dataAppendPageSize=").$(dataAppendPageSize).$();
    }
}
