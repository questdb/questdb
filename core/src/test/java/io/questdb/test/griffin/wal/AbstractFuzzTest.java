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
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.mp.WorkerPool;
import io.questdb.std.*;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionGenerator;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.*;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class AbstractFuzzTest extends AbstractGriffinTest {
    protected final static int MAX_WAL_APPLY_O3_SPLIT_PARTITION_CEIL = 20000;
    protected final static int MAX_WAL_APPLY_O3_SPLIT_PARTITION_MIN = 200;
    protected final static int MAX_WAL_APPLY_TIME_PER_TABLE_CEIL = 250;
    protected final WorkerPool sharedWorkerPool = new TestWorkerPool(4, metrics);
    private final TableSequencerAPI.TableSequencerCallback checkNoSuspendedTablesRef = AbstractFuzzTest::checkNoSuspendedTables;
    protected int initialRowCount;
    protected int partitionCount;
    private double cancelRowsProb;
    private double colRenameProb;
    private double collAddProb;
    private double collRemoveProb;
    private double dataAddProb;
    private double equalTsRowsProb;
    private int fuzzRowCount;
    private boolean isO3;
    private double notSetProb;
    private double nullSetProb;
    private double rollbackProb;
    private long s0;
    private long s1;
    private int strLen;
    private int symbolCountMax;
    private int symbolStrLenMax;
    private int transactionCount;
    private double truncateProb;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        walTxnNotificationQueueCapacity = 16;
        AbstractGriffinTest.setUpStatic();
    }

    @AfterClass
    public static void tearDownStatic() throws Exception {
        AbstractGriffinTest.tearDownStatic();
    }

    public void applyWal(ObjList<FuzzTransaction> transactions, String tableName, int walWriterCount, Rnd applyRnd) {
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

    @Before
    public void clearSeeds() {
        s0 = 0;
        s1 = 0;
    }

    public Rnd generateRandom(Log log, long s0, long s1) {
        Rnd rnd = TestUtils.generateRandom(log, s0, s1);
        this.s0 = rnd.getSeed0();
        this.s1 = rnd.getSeed1();
        return rnd;
    }

    public Rnd generateRandom(Log log) {
        Rnd rnd = TestUtils.generateRandom(log);
        s0 = rnd.getSeed0();
        s1 = rnd.getSeed1();
        return rnd;
    }

    public ObjList<FuzzTransaction> generateSet(Rnd rnd, TableRecordMetadata metadata, long start, long end, String tableName) {
        return FuzzTransactionGenerator.generateSet(
                metadata,
                rnd,
                start,
                end,
                Math.max(1, fuzzRowCount),
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
                equalTsRowsProb,
                strLen,
                generateSymbols(rnd, rnd.nextInt(Math.max(1, symbolCountMax - 5)) + 5, symbolStrLenMax, tableName),
                (int) metadata.getMetadataVersion()
        );
    }

    @After
    public void logSeeds() {
        if (this.s0 != 0 || this.s1 != 0) {
            LOG.info().$("random seeds: ").$(s0).$("L, ").$(s1).$('L').$();
            System.out.printf("random seeds: %dL, %dL%n", s0, s1);
        }
    }

    @After
    public void tearDown() throws Exception {
        sharedWorkerPool.halt();
        super.tearDown();
    }

    private static void checkIndexRandomValueScan(String expectedTableName, String actualTableName, Rnd rnd, long recordCount, String columnName) throws SqlException {
        long randomRow = rnd.nextLong(recordCount);
        sink.clear();
        TestUtils.printSql(compiler, sqlExecutionContext, "select \"" + columnName + "\" as a from " + expectedTableName + " limit " + randomRow + ", 1", sink);
        String prefix = "a\n";
        String randomValue = sink.length() > prefix.length() + 2 ? sink.subSequence(prefix.length(), sink.length() - 1).toString() : null;
        String indexedWhereClause = " where \"" + columnName + "\" = " + (randomValue == null ? "null" : "'" + randomValue + "'");
        LOG.info().$("checking random index with filter: ").$(indexedWhereClause).I$();
        String limit = ""; // For debugging
        TestUtils.assertSqlCursors(compiler, sqlExecutionContext, expectedTableName + indexedWhereClause + limit, actualTableName + indexedWhereClause + limit, LOG);
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

    private static void reloadPartitions(TableReader rdr1) {
        if (rdr1.isActive()) {
            LOG.info().$("reloading partitions [table=").$(rdr1.getTableToken()).$(", txn=").$(rdr1.getTxn()).I$();
            for (int i = 0; i < rdr1.getPartitionCount(); i++) {
                rdr1.openPartition(i);
            }
        }
    }

    private static void reloadReader(Rnd reloadRnd, TableReader rdr1, CharSequence rdrId) {
        if (reloadRnd.nextBoolean()) {
            reloadPartitions(rdr1);
            LOG.info().$("releasing reader txn [rdr=").$(rdrId).$(", table=").$(rdr1.getTableToken()).$(", txn=").$(rdr1.getTxn()).I$();
            rdr1.goPassive();

            if (reloadRnd.nextBoolean()) {
                rdr1.goActive();
                LOG.info().$("acquired reader txn [rdr=").$(rdrId).$(", table=").$(rdr1.getTableToken()).$(", txn=").$(rdr1.getTxn()).I$();
            }
        }
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

    protected static void applyNonWal(ObjList<FuzzTransaction> transactions, String tableName, Rnd reloadRnd) {

        try (
                TableReader rdr1 = getReader(tableName);
                TableReader rdr2 = getReader(tableName);
                TableWriter writer = getWriter(tableName);
                O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine.getMessageBus(), 1)
        ) {
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
                purgeAndReloadReaders(reloadRnd, rdr1, rdr2, purgeJob, 0.25);
            }
        }
    }

    protected static int getRndO3PartitionSplit(Rnd rnd) {
        return MAX_WAL_APPLY_O3_SPLIT_PARTITION_MIN + rnd.nextInt(MAX_WAL_APPLY_O3_SPLIT_PARTITION_CEIL - MAX_WAL_APPLY_O3_SPLIT_PARTITION_MIN);
    }

    protected static int getRndO3PartitionSplitMaxCount(Rnd rnd) {
        return 1 + rnd.nextInt(2);
    }

    protected static void purgeAndReloadReaders(Rnd reloadRnd, TableReader rdr1, TableReader rdr2, O3PartitionPurgeJob purgeJob, double realoadThreashold) {
        if (reloadRnd.nextDouble() < realoadThreashold) {
            purgeJob.run(0);
            reloadReader(reloadRnd, rdr1, "1");
            reloadReader(reloadRnd, rdr2, "2");
        }
    }

    protected void assertRandomIndexes(String tableNameNoWal, String tableNameWal, Rnd rnd) throws SqlException {
        try (TableReader reader = newTableReader(configuration, tableNameNoWal)) {
            if (reader.size() > 0) {
                TableReaderMetadata metadata = reader.getMetadata();
                for (int columnIndex = 0; columnIndex < metadata.getColumnCount(); columnIndex++) {
                    if (ColumnType.isSymbol(metadata.getColumnType(columnIndex))
                            && metadata.isColumnIndexed(columnIndex)) {
                        checkIndexRandomValueScan(tableNameNoWal, tableNameWal, rnd, reader.size(), metadata.getColumnName(columnIndex));
                    }
                }
            }
        }
    }

    protected TableToken createInitialTable(String tableName1, boolean isWal, int rowCount) throws SqlException {
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
                "), index(sym2) timestamp(ts) partition by DAY " + (isWal ? "WAL" : "BYPASS WAL"));
        // force few column tops
        compile("alter table " + tableName1 + " add column long_top long");
        compile("alter table " + tableName1 + " add column str_top long");
        compile("alter table " + tableName1 + " add column sym_top symbol index");
        compile("alter table " + tableName1 + " add column ip4 ipv4");

        return engine.verifyTableName(tableName1);
    }

    protected void fullRandomFuzz(Rnd rnd) throws Exception {
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

    protected void fullRandomFuzz(Rnd rnd, int tableCount) throws Exception {
        runFuzz(rnd, getTestName(), tableCount, true, true);
    }

    protected String[] generateSymbols(Rnd rnd, int totalSymbols, int strLen, String baseSymbolTableName) {
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

    protected String getTestName() {
        return testName.getMethodName().replace('[', '_').replace(']', '_');
    }

    protected void runFuzz(Rnd rnd) throws Exception {
        configOverrideO3ColumnMemorySize(rnd.nextInt(16 * 1024 * 1024));

        assertMemoryLeak(() -> {
            String tableNameBase = getTestName();
            String tableNameWal = tableNameBase + "_wal";
            String tableNameWal2 = tableNameBase + "_wal_parallel";
            String tableNameNoWal = tableNameBase + "_nonwal";

            createInitialTable(tableNameWal, true, initialRowCount);
            createInitialTable(tableNameWal2, true, initialRowCount);
            createInitialTable(tableNameNoWal, false, initialRowCount);

            ObjList<FuzzTransaction> transactions;
            try (TableReader reader = newTableReader(configuration, tableNameNoWal)) {
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

    protected void runFuzz(Rnd rnd, String tableNameBase, int tableCount, boolean randomiseProbs, boolean randomiseCounts) throws Exception {
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

    protected void setFuzzCounts(boolean isO3, int fuzzRowCount, int transactionCount, int strLen, int symbolStrLenMax, int symbolCountMax, int initialRowCount, int partitionCount) {
        this.isO3 = isO3;
        this.fuzzRowCount = fuzzRowCount;
        this.transactionCount = transactionCount;
        this.strLen = strLen;
        this.symbolStrLenMax = symbolStrLenMax;
        this.symbolCountMax = symbolCountMax;
        this.initialRowCount = initialRowCount;
        this.partitionCount = partitionCount;
    }

    protected void setFuzzProbabilities(double cancelRowsProb, double notSetProb, double nullSetProb, double rollbackProb, double collAddProb, double collRemoveProb, double colRenameProb, double dataAddProb, double truncateProb, double equalTsRowsProb) {
        this.cancelRowsProb = cancelRowsProb;
        this.notSetProb = notSetProb;
        this.nullSetProb = nullSetProb;
        this.rollbackProb = rollbackProb;
        this.collAddProb = collAddProb;
        this.collRemoveProb = collRemoveProb;
        this.colRenameProb = colRenameProb;
        this.dataAddProb = dataAddProb;
        this.truncateProb = truncateProb;
        this.equalTsRowsProb = equalTsRowsProb;
    }

    protected void setFuzzProperties(long maxApplyTimePerTable, long splitPartitionThreshold, int o3PartitionSplitMaxCount) {
        node1.getConfigurationOverrides().setWalApplyTableTimeQuote(maxApplyTimePerTable);
        node1.getConfigurationOverrides().setPartitionO3SplitThreshold(splitPartitionThreshold);
        node1.getConfigurationOverrides().setO3PartitionSplitMaxCount(o3PartitionSplitMaxCount);
    }

    protected void setRandomAppendPageSize(Rnd rnd) {
        int minPage = 18;
        dataAppendPageSize = 1L << (minPage + rnd.nextInt(22 - minPage)); // MAX page size 4Mb
        LOG.info().$("dataAppendPageSize=").$(dataAppendPageSize).$();
    }

}
