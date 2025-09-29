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

package io.questdb.test.cairo.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DebugUtils;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.fuzz.FuzzDropCreateTableOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionGenerator;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;


public class FuzzRunner {
    public final static int MAX_WAL_APPLY_TIME_PER_TABLE_CEIL = 250;
    protected static final Log LOG = LogFactory.getLog(AbstractFuzzTest.class);
    protected static final StringSink sink = new StringSink();
    private final TableSequencerAPI.TableSequencerCallback checkNoSuspendedTablesRef;
    protected int initialRowCount;
    protected int partitionCount;
    private double cancelRowsProb;
    private double colAddProb;
    private double colRemoveProb;
    private double colRenameProb;
    private double colTypeChangeProb;
    private double dataAddProb;
    private CairoEngine engine;
    private double equalTsRowsProb;
    private FailureFileFacade ff;
    private int fuzzRowCount;
    private int ioFailureCount;
    private int ioFailureCreatedCount;
    private boolean isO3;
    private double notSetProb;
    private double nullSetProb;
    private int parallelWalCount;
    private double partitionDropProb;
    private double replaceInsertProb;
    private double rollbackProb;
    private long s0;
    private long s1;
    private double setTtlProb;
    private SqlExecutionContext sqlExecutionContext;
    private int strLen;
    private double symbolAccessValidationProb;
    private int symbolCountMax;
    private int symbolStrLenMax;
    private double tableDropProb;
    private int transactionCount;
    private double truncateProb;

    public FuzzRunner() {
        checkNoSuspendedTablesRef = this::checkNoSuspendedTables;
    }

    public static String getWalParallelApplyTableName(String tableNameBase, int i) {
        return tableNameBase + "_" + i;
    }

    public static void purgeAndReloadReaders(Rnd reloadRnd, TableReader rdr1, TableReader rdr2, O3PartitionPurgeJob purgeJob, double realoadThreashold) {
        if (reloadRnd.nextDouble() < realoadThreashold) {
            purgeJob.run(0);
            reloadReader(reloadRnd, rdr1, "1");
            reloadReader(reloadRnd, rdr2, "2");
        }
    }

    public void after() {
        if (this.s0 != 0 || this.s1 != 0) {
            LOG.info().$("random seeds: ").$(s0).$("L, ").$(s1).$('L').$();
            System.out.printf("random seeds: %dL, %dL%n", s0, s1);
        }
    }

    public void applyManyWalParallel(ObjList<ObjList<FuzzTransaction>> fuzzTransactions, Rnd rnd, String tableNameBase, boolean multiTable, boolean waitApply) {
        ObjList<WalWriter> writers = new ObjList<>();
        int tableCount = fuzzTransactions.size();
        AtomicInteger done = new AtomicInteger();
        AtomicInteger forceReaderReload = new AtomicInteger();
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        ObjList<Thread> applyThreads = new ObjList<>();
        ObjList<Thread> threads = new ObjList<>();

        try {
            for (int i = 0; i < tableCount; i++) {
                String tableName = multiTable ? getWalParallelApplyTableName(tableNameBase, i) : tableNameBase;
                AtomicLong waitBarrierVersion = new AtomicLong();
                int parallelWalCount = this.parallelWalCount > 0 ? this.parallelWalCount : Math.max(2, rnd.nextInt(5));
                AtomicInteger nextOperation = new AtomicInteger(-1);
                ObjList<FuzzTransaction> transactions = fuzzTransactions.get(i);
                AtomicLong doneCount = new AtomicLong();

                for (int j = 0; j < parallelWalCount; j++) {
                    threads.add(
                            createWalWriteThread(
                                    transactions,
                                    tableName,
                                    writers,
                                    waitBarrierVersion,
                                    doneCount,
                                    forceReaderReload,
                                    nextOperation,
                                    errors
                            )
                    );
                }
            }

            for (int j = 0; j < threads.size(); j++) {
                threads.get(j).start();
            }

            if (waitApply) {
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

                Thread purgePartitionThread = new Thread(() -> runPurgePartitionJob(
                        done,
                        forceReaderReload,
                        errors,
                        new Rnd(rnd.nextLong(), rnd.nextLong()),
                        tableNameBase,
                        tableCount,
                        multiTable
                ));
                purgePartitionThread.start();
                applyThreads.add(purgePartitionThread);
            }
        } finally {
            for (int i = 0; i < threads.size(); i++) {
                int k = i;
                TestUtils.unchecked(() -> threads.get(k).join());
            }

            done.incrementAndGet();
            Misc.freeObjList(writers);
        }

        for (Throwable e : errors) {
            throw new RuntimeException(e);
        }

        if (waitApply) {
            for (int i = 0; i < applyThreads.size(); i++) {
                int k = i;
                TestUtils.unchecked(() -> applyThreads.get(k).join());
            }
        }
        engine.releaseInactive();
    }

    public void applyNonWal(ObjList<FuzzTransaction> transactions, String tableName, Rnd reloadRnd) {
        TableWriter writer = TestUtils.getWriter(engine, tableName);
        TableReader rdr1 = getReader(tableName);
        TableReader rdr2 = getReader(tableName);

        calculateReplaceRanges(transactions);

        try (O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1)) {
            int transactionSize = transactions.size();
            Rnd rnd = new Rnd();
            int failuresObserved = 0;
            for (int i = 0; i < transactionSize; i++) {
                if (ioFailureCreatedCount < ioFailureCount && failuresObserved == ff.failureGenerated()) {
                    // Maybe it's time to plant an IO failure
                    int nextFailureInTransactions = (transactions.size() - i) / (ioFailureCount - ioFailureCreatedCount);
                    if (nextFailureInTransactions == 0 || rnd.nextInt(nextFailureInTransactions) == 0) {
                        ff.setToFailAfter(rnd.nextInt((int) (writer.getColumnCount() * 1.5)));
                        ioFailureCreatedCount++;
                    }
                }

                FuzzTransaction transaction = transactions.getQuick(i);
                TableWriter writerCopy = writer;

                try {
                    if (transaction.reopenTable) {
                        rdr1 = Misc.free(rdr1);
                        rdr2 = Misc.free(rdr2);
                        writer = Misc.free(writer);
                    }

                    int size = transaction.operationList.size();
                    for (int operationIndex = 0; operationIndex < size; operationIndex++) {
                        FuzzTransactionOperation operation = transaction.operationList.getQuick(operationIndex);
                        // Non-wal tables don't support replace range commits
                        // we simulate it by excluding from the commit all the rows that will be replaced
                        // in the future commits.
                        operation.apply(rnd, engine, writerCopy, -1, transaction.getNoCommitIntervals());
                    }

                    if (transaction.reopenTable) {
                        writer = TestUtils.getWriter(engine, tableName);
                        rdr1 = getReader(tableName);
                        rdr2 = getReader(tableName);
                    } else {
                        if (transaction.rollback) {
                            writer.rollback();
                        } else {
                            writer.commit();
                        }
                    }
                } catch (CairoException | CairoError e) {
                    boolean housekeeping = (e instanceof CairoException) && ((CairoException) e).isHousekeeping();
                    int failures = ff.failureGenerated();
                    if (failures > failuresObserved) {
                        failuresObserved = failures;
                        LOG.info().$("expected IO failure observed: ").$((Throwable) e).$();
                        writer = Misc.free(writer);

                        transaction = transactions.getQuick(i);

                        try {
                            writer = TestUtils.getWriter(engine, tableName);
                        } catch (CairoException ex) {
                            if (ex.isTableDoesNotExist() && transaction.operationList.get(0) instanceof FuzzDropCreateTableOperation) {
                                // Table is dropped, but failed to recreate.
                                // Create it again.
                                FuzzDropCreateTableOperation dropCreateTableOperation = (FuzzDropCreateTableOperation) transaction.operationList.get(0);
                                if (dropCreateTableOperation.recreateTable(engine)) {
                                    writer = TestUtils.getWriter(engine, tableName);
                                    // Drop and create cycle now complete, move to next transaction.
                                    i++;
                                } else {
                                    throw ex;
                                }
                            }
                        }
                        // Retry the last transaction now that the failure is handled.
                        if (!housekeeping) {
                            i--;
                        }
                    } else {
                        throw e;
                    }
                }

                try {
                    purgeAndReloadReaders(reloadRnd, rdr1, rdr2, purgeJob, 0.25);
                } catch (CairoException | CairoError e) {
                    int failures = ff.failureGenerated();
                    if (failures > failuresObserved) {
                        failuresObserved = failures;
                        LOG.info().$("expected IO failure observed: ").$((Throwable) e).$();
                        rdr1 = Misc.free(rdr1);
                        rdr2 = Misc.free(rdr2);
                        rdr1 = getReader(tableName);
                        rdr2 = getReader(tableName);
                    } else {
                        throw e;
                    }
                }
            }
        } finally {
            Misc.free(rdr1);
            Misc.free(rdr2);
            Misc.free(writer);
            if (ff != null) {
                ff.clearFailures();
            }
        }
    }

    public void applyToWal(ObjList<FuzzTransaction> transactions, String tableName, int walWriterCount, Rnd applyRnd) {
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
                // WAL tables support replace range commits
                // we apply them by using special commit rather than excluding Ts ranges from the commit
                operation.apply(tempRnd, engine, writer, -1, null);
            }

            if (transaction.reopenTable) {
                // Table is dropped, reopen all writers.
                for (int writerIndex = 0; writerIndex < walWriterCount; writerIndex++) {
                    writers.getQuick(writerIndex).close();
                    writers.setQuick(writerIndex, (WalWriter) engine.getTableWriterAPI(tableName, "apply trans test"));
                }
            } else {
                if (transaction.rollback) {
                    writer.rollback();
                } else {
                    if (transaction.hasReplaceRange()) {
                        writer.commitWithParams(
                                transaction.getReplaceLoTs(),
                                transaction.getReplaceHiTs(),
                                WAL_DEDUP_MODE_REPLACE_RANGE
                        );
                    } else {
                        writer.commit();
                    }
                }
            }
        }

        Misc.freeObjList(writers);
    }

    public void applyWal(ObjList<FuzzTransaction> transactions, String tableName, int walWriterCount, Rnd applyRnd) {
        TableToken tableToken = engine.verifyTableName(tableName);
        applyToWal(transactions, tableName, walWriterCount, applyRnd);
        drainWalQueue(applyRnd, tableName);
        Assert.assertFalse("Table is suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
    }

    public void assertRandomIndexes(String tableNameNoWal, String tableNameWal, Rnd rnd) throws SqlException {
        try (TableReader reader = getReader(tableNameNoWal)) {
            if (reader.size() > 0) {
                TableReaderMetadata metadata = reader.getMetadata();
                for (int columnIndex = 0; columnIndex < metadata.getColumnCount(); columnIndex++) {
                    if (ColumnType.isSymbol(metadata.getColumnType(columnIndex)) && metadata.isColumnIndexed(columnIndex)) {
                        checkIndexRandomValueScan(
                                tableNameNoWal,
                                tableNameWal,
                                rnd,
                                reader.size(),
                                metadata.getColumnName(columnIndex),
                                metadata.getColumnName(metadata.getTimestampIndex())
                        );
                    }
                }
            }
        }
    }

    public void checkNoSuspendedTables() {
        engine.getTableSequencerAPI().forAllWalTables(new ObjHashSet<>(), false, checkNoSuspendedTablesRef);
    }

    @Before
    public void clearSeeds() {
        s0 = 0;
        s1 = 0;
    }

    public TableToken createInitialTableEmptyNonWal(String tableName) throws SqlException {
        return createInitialTable(tableName, false, 0);
    }

    public TableToken createInitialTableNonWal(String tableName, ObjList<FuzzTransaction> transactions) throws SqlException {
        return createInitialTable(tableName, transactions);
    }

    public TableToken createInitialTableWal(String tableName) throws SqlException {
        return createInitialTable(tableName, true, initialRowCount);
    }

    public TableToken createInitialTableWal(String tableName, String timestampType) throws SqlException {
        return createInitialTable(tableName, true, initialRowCount, timestampType);
    }

    public TableToken createInitialTableWal(String tableName, int initialRowCount) throws SqlException {
        return createInitialTable(tableName, true, initialRowCount);
    }

    public Rnd generateRandom(Log log) {
        Rnd rnd = TestUtils.generateRandom(log);
        s0 = rnd.getSeed0();
        s1 = rnd.getSeed1();
        return rnd;
    }

    public Rnd generateRandom(Log log, long s0, long s1) {
        Rnd rnd = TestUtils.generateRandom(log, s0, s1);
        this.s0 = rnd.getSeed0();
        this.s1 = rnd.getSeed1();
        return rnd;
    }

    public ObjList<FuzzTransaction> generateSet(
            Rnd rnd,
            TableRecordMetadata sequencerMetadata,
            TableMetadata tableMetadata,
            long start,
            long end,
            String tableName
    ) {
        return FuzzTransactionGenerator.generateSet(
                initialRowCount,
                sequencerMetadata,
                tableMetadata,
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
                colAddProb,
                colRemoveProb,
                colRenameProb,
                colTypeChangeProb,
                dataAddProb,
                equalTsRowsProb,
                partitionDropProb,
                truncateProb,
                tableDropProb,
                setTtlProb,
                replaceInsertProb,
                symbolAccessValidationProb,
                strLen,
                generateSymbols(rnd, rnd.nextInt(Math.max(1, symbolCountMax - 5)) + 5, symbolStrLenMax, tableName),
                (int) sequencerMetadata.getMetadataVersion()
        );
    }

    public ObjList<FuzzTransaction> generateTransactions(String tableName, Rnd rnd, long start) {
        long end = start + partitionCount * Micros.DAY_MICROS;
        return generateTransactions(tableName, rnd, start, end);
    }

    public ObjList<FuzzTransaction> generateTransactions(String tableName, Rnd rnd) throws NumericException {
        long start = MicrosTimestampDriver.floor("2022-02-24T17");
        long end = start + partitionCount * Micros.DAY_MICROS;
        return generateTransactions(tableName, rnd, start, end);
    }

    public ObjList<FuzzTransaction> generateTransactions(String tableName, Rnd rnd, long start, long end) {
        TableToken tableToken = engine.verifyTableName(tableName);
        try (TableRecordMetadata sequencerMetadata = engine.getLegacyMetadata(tableToken);
             TableMetadata tableMetadata = engine.getTableMetadata(tableToken)
        ) {
            return generateSet(rnd, sequencerMetadata, tableMetadata, start, end, tableName);
        }
    }

    public FilesFacade getFileFacade() {
        return ff;
    }

    public int getTransactionCount() {
        return transactionCount;
    }

    public int randomiseStringLengths(Rnd rnd, int maxLen) {
        // Make extremely long strings rare
        // but still possible
        double randomDriver = rnd.nextDouble();

        // Linear up to 20 chars, then exponential
        if (20 < maxLen) {
            return (int) (20 * randomDriver + Math.round(Math.pow(maxLen - 20, randomDriver)));
        } else {
            return (int) (20 * randomDriver);
        }
    }

    public void setFuzzCounts(
            boolean isO3,
            int fuzzRowCount,
            int transactionCount,
            int strLen,
            int symbolStrLenMax,
            int symbolCountMax,
            int initialRowCount,
            int partitionCount
    ) {
        setFuzzCounts(
                isO3,
                fuzzRowCount,
                transactionCount,
                strLen,
                symbolStrLenMax,
                symbolCountMax,
                initialRowCount,
                partitionCount,
                -1,
                1
        );
    }

    public void setFuzzCounts(
            boolean isO3,
            int fuzzRowCount,
            int transactionCount,
            int strLen,
            int symbolStrLenMax,
            int symbolCountMax,
            int initialRowCount,
            int partitionCount,
            int parallelWalCount,
            int ioFailureCount
    ) {
        this.isO3 = isO3;
        this.fuzzRowCount = fuzzRowCount;
        this.transactionCount = transactionCount;
        this.strLen = strLen;
        this.symbolStrLenMax = symbolStrLenMax;
        this.symbolCountMax = symbolCountMax;
        this.initialRowCount = initialRowCount;
        this.partitionCount = partitionCount;
        this.parallelWalCount = parallelWalCount;
        this.ioFailureCount = ioFailureCount;
    }

    public void setFuzzProbabilities(
            double cancelRowsProb,
            double notSetProb,
            double nullSetProb,
            double rollbackProb,
            double colAddProb,
            double colRemoveProb,
            double colRenameProb,
            double colTypeChangeProb,
            double dataAddProb,
            double equalTsRowsProb,
            double partitionDropProb,
            double truncateProb,
            double tableDropProb,
            double setTtlProb,
            double replaceInsertProb,
            double symbolAccessValidationProb
    ) {
        this.cancelRowsProb = cancelRowsProb;
        this.notSetProb = notSetProb;
        this.nullSetProb = nullSetProb;
        this.rollbackProb = rollbackProb;
        this.colAddProb = colAddProb;
        this.colRemoveProb = colRemoveProb;
        this.colRenameProb = colRenameProb;
        this.colTypeChangeProb = colTypeChangeProb;
        this.dataAddProb = dataAddProb;
        this.equalTsRowsProb = equalTsRowsProb;
        this.partitionDropProb = partitionDropProb;
        this.truncateProb = truncateProb;
        this.tableDropProb = tableDropProb;
        this.setTtlProb = setTtlProb;
        this.replaceInsertProb = replaceInsertProb;
        this.symbolAccessValidationProb = symbolAccessValidationProb;
    }

    public void withDb(CairoEngine engine, SqlExecutionContext sqlExecutionContext) {
        this.engine = engine;
        this.sqlExecutionContext = sqlExecutionContext;
        this.ff = new FailureFileFacade(engine.getConfiguration().getFilesFacade());
    }

    private static void reloadPartitions(TableReader rdr1) {
        if (rdr1.isActive()) {
            LOG.info().$("reloading partitions [table=").$(rdr1.getTableToken()).$(", txn=").$(rdr1.getTxn()).I$();
            for (int i = 0; i < rdr1.getPartitionCount(); i++) {
                rdr1.openPartition(i);
            }
        }
    }

    private static void reloadReader(Rnd reloadRnd, @Nullable TableReader reader, CharSequence rdrId) {
        if (reloadRnd.nextBoolean()) {
            if (reader != null && reader.isActive()) {
                reloadPartitions(reader);
                LOG.info().$("releasing reader txn [rdr=").$(rdrId).$(", table=").$(reader.getTableToken()).$(", txn=").$(reader.getTxn()).I$();
                reader.goPassive();
            }

            if (reloadRnd.nextBoolean() && reader != null && reader.isActive()) {
                reader.goActive();
                LOG.info().$("acquired reader txn [rdr=").$(rdrId).$(", table=").$(reader.getTableToken()).$(", txn=").$(reader.getTxn()).I$();
            }
        }
    }

    private void applyWalParallel(ObjList<FuzzTransaction> transactions, String tableName, Rnd applyRnd) {
        ObjList<ObjList<FuzzTransaction>> tablesTransactions = new ObjList<>();
        tablesTransactions.add(transactions);
        applyManyWalParallel(tablesTransactions, applyRnd, tableName, false, true);
    }

    private void assertMinMaxTimestamp(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String tableName) throws SqlException {
        try (TableReader reader = getReader(tableName)) {
            if (reader.getMinTimestamp() != Long.MAX_VALUE) {
                TestUtils.assertSql(
                        compiler,
                        sqlExecutionContext,
                        "select ts from " + tableName + " order by ts limit 1",
                        sink,
                        "ts\n" +
                                Micros.toUSecString(reader.getMinTimestamp())
                                + "\n"
                );
            }

            if (reader.getMaxTimestamp() != Long.MIN_VALUE) {
                TestUtils.assertSql(
                        compiler,
                        sqlExecutionContext,
                        "select ts from " + tableName + " order by ts limit -1",
                        sink,
                        "ts\n" +
                                Micros.toUSecString(reader.getMaxTimestamp())
                                + "\n"
                );
            }
        }
    }

    private LongList calculateReplaceRanges(ObjList<FuzzTransaction> transactions) {
        // If transactions have the replace ranges set
        // then we do not commit in between the ranges that are replaced
        // in future transactions.
        // Iterate in reverse order and calculate the union of all excluded ranges
        // and save in each transaction.
        LongList excludedIntervals = null;
        for (int i = transactions.size() - 1; i > -1; i--) {
            var transaction = transactions.getQuick(i);
            if (!transaction.rollback) {
                transaction.setNoCommitIntervals(excludedIntervals);
                if (transaction.hasReplaceRange()) {
                    var before = excludedIntervals;
                    excludedIntervals = unionIntervals(excludedIntervals, transaction.getReplaceLoTs(), transaction.getReplaceHiTs());
                    // We must create new copy of excluded intervals otherwise
                    // all transactions will point to the same object
                    assert before != excludedIntervals;
                }
            }
        }
        return excludedIntervals;
    }

    private void checkIndexRandomValueScan(
            String expectedTableName,
            String actualTableName,
            Rnd rnd,
            long recordCount,
            String symbolColumnName,
            String tsColumnName
    ) throws SqlException {
        long randomRow = rnd.nextLong(recordCount);
        sink.clear();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.printSql(compiler, sqlExecutionContext, "select \"" + symbolColumnName + "\" as a from " + expectedTableName + " limit " + randomRow + ", " + (randomRow + 1), sink);
            String prefix = "a\n";
            String randomValue = sink.length() > prefix.length() + 2 ? sink.subSequence(prefix.length(), sink.length() - 1).toString() : null;
            String indexedWhereClause = " where \"" + symbolColumnName + "\" = " + (randomValue == null ? "null" : "'" + randomValue + "'");
            LOG.info().$("checking random index with filter: ").$(indexedWhereClause).I$();
            String limit = ""; // For debugging
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, expectedTableName + indexedWhereClause + limit, actualTableName + indexedWhereClause + limit, LOG);
            // Now let's do backward order assertion
            String orderBy = " order by " + tsColumnName + " desc";
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, expectedTableName + indexedWhereClause + orderBy + limit, actualTableName + indexedWhereClause + orderBy + limit, LOG);
        }
    }

    private void checkNoSuspendedTables(int tableId, TableToken tableName, long lastTxn) {
        Assert.assertFalse(tableName.getTableName(), engine.getTableSequencerAPI().isSuspended(tableName));
    }

    private void checkNoSuspendedTables(ObjHashSet<TableToken> tableTokenBucket) {
        engine.getTableSequencerAPI().forAllWalTables(tableTokenBucket, false, checkNoSuspendedTablesRef);
    }

    private TableToken createInitialTable(String tableName, ObjList<FuzzTransaction> transactions) throws SqlException {
        SharedRandom.RANDOM.set(new Rnd());

        if (engine.getTableTokenIfExists(tableName) == null) {
            sink.clear();
            sink.put("create atomic table ")
                    .put(tableName)
                    .put(" as (")
                    .put("select c1, c2, ts, sym2, c3, c4, c5, rnd_str, bool1 from data_temp");

            if (transactions != null) {
                LongList noCommitRanges = calculateReplaceRanges(transactions);

                if (noCommitRanges != null) {
                    sink.put(" where");
                    for (int i = 0, n = noCommitRanges.size(); i < n; i += 2) {
                        long lo = noCommitRanges.getQuick(i);
                        long hi = noCommitRanges.getQuick(i + 1);
                        if (i > 0) {
                            sink.put(" and");
                        }
                        sink.put(" (ts not between '" + Micros.toUSecString(lo) + "' and '" + Micros.toUSecString(hi) + "')");
                    }
                }
            }

            sink.put("), index(sym2) timestamp(ts) partition by DAY BYPASS WAL");
            engine.execute(sink, sqlExecutionContext);
            // force few column tops
            engine.execute("alter table " + tableName + " add column long_top long", sqlExecutionContext);
            engine.execute("alter table " + tableName + " add column str_top long", sqlExecutionContext);
            engine.execute("alter table " + tableName + " add column sym_top symbol index", sqlExecutionContext);
            engine.execute("alter table " + tableName + " add column ip4 ipv4", sqlExecutionContext);
            engine.execute("alter table " + tableName + " add column var_top varchar", sqlExecutionContext);
        }
        return engine.verifyTableName(tableName);
    }

    private TableToken createInitialTable(String tableName, boolean isWal, int rowCount) throws SqlException {
        SharedRandom.RANDOM.set(new Rnd());
        TableToken tempTable = engine.getTableTokenIfExists("data_temp");

        if (tempTable == null) {
            engine.execute(
                    "create atomic table data_temp as (" +
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
                            ")",
                    sqlExecutionContext
            );
        }

        if (engine.getTableTokenIfExists(tableName) == null) {
            engine.execute(
                    "create atomic table " + tableName + " as (" +
                            "select * from data_temp" +
                            "), index(sym2) timestamp(ts) partition by DAY " + (isWal ? "WAL" : "BYPASS WAL"),
                    sqlExecutionContext
            );
            // force few column tops
            engine.execute("alter table " + tableName + " add column long_top long", sqlExecutionContext);
            engine.execute("alter table " + tableName + " add column str_top long", sqlExecutionContext);
            engine.execute("alter table " + tableName + " add column sym_top symbol index", sqlExecutionContext);
            engine.execute("alter table " + tableName + " add column ip4 ipv4", sqlExecutionContext);
            engine.execute("alter table " + tableName + " add column var_top varchar", sqlExecutionContext);
        }
        return engine.verifyTableName(tableName);
    }

    private TableToken createInitialTable(String tableName, boolean isWal, int rowCount, String timestampType) throws SqlException {
        SharedRandom.RANDOM.set(new Rnd());
        TableToken tempTable = engine.getTableTokenIfExists("data_temp");

        if (tempTable == null) {
            engine.execute(
                    "create atomic table data_temp as (" +
                            "select x as c1, " +
                            " rnd_symbol('AB', 'BC', 'CD') c2, " +
                            " timestamp_sequence('2022-02-24'::" + timestampType + ", 1000000L) ts, " +
                            " rnd_symbol('DE', null, 'EF', 'FG') sym2," +
                            " cast(x as int) c3," +
                            " rnd_bin() c4," +
                            " to_long128(3 * x, 6 * x) c5," +
                            " rnd_str('a', 'bdece', null, ' asdflakji idid', 'dk')," +
                            " rnd_boolean() bool1 " +
                            " from long_sequence(" + rowCount + ")" +
                            ")",
                    sqlExecutionContext
            );
        }

        if (engine.getTableTokenIfExists(tableName) == null) {
            engine.execute(
                    "create atomic table " + tableName + " as (" +
                            "select * from data_temp" +
                            "), index(sym2) timestamp(ts) partition by DAY " + (isWal ? "WAL" : "BYPASS WAL"),
                    sqlExecutionContext
            );
            // force few column tops
            engine.execute("alter table " + tableName + " add column long_top long", sqlExecutionContext);
            engine.execute("alter table " + tableName + " add column str_top long", sqlExecutionContext);
            engine.execute("alter table " + tableName + " add column sym_top symbol index", sqlExecutionContext);
            engine.execute("alter table " + tableName + " add column ip4 ipv4", sqlExecutionContext);
            engine.execute("alter table " + tableName + " add column var_top varchar", sqlExecutionContext);
        }
        return engine.verifyTableName(tableName);
    }

    @NotNull
    private ObjList<FuzzTransaction> createTransactions(Rnd rnd, String tableNameBase) throws SqlException, NumericException {
        String tableNameNoWal = tableNameBase + "_nonwal";

        createInitialTableWal(tableNameBase, initialRowCount);

        ObjList<FuzzTransaction> transactions = generateTransactions(tableNameBase, rnd);
        createInitialTableNonWal(tableNameNoWal, transactions);

        applyNonWal(transactions, tableNameNoWal, rnd);

        // Release TW to reduce memory pressure
        engine.releaseInactive();

        return transactions;
    }

    @NotNull
    private Thread createWalWriteThread(
            ObjList<FuzzTransaction> transactions,
            String tableName,
            ObjList<WalWriter> writers,
            AtomicLong waitBarrierVersion,
            AtomicLong doneCount,
            AtomicInteger forceReaderReload,
            AtomicInteger nextOperation,
            ConcurrentLinkedQueue<Throwable> errors
    ) {
        final int writerIndex;
        synchronized (writers) {
            writers.add((WalWriter) engine.getTableWriterAPI(tableName, "apply trans test"));
            writerIndex = writers.size() - 1;
        }

        return new Thread(() -> {
            int opIndex;

            try {
                Rnd tempRnd = new Rnd();
                while (errors.isEmpty() && (opIndex = nextOperation.incrementAndGet()) < transactions.size() && errors.isEmpty()) {
                    FuzzTransaction transaction = transactions.getQuick(opIndex);

                    // wait until structure version, truncate, replace commit is applied
                    while (waitBarrierVersion.get() < transaction.waitBarrierVersion && errors.isEmpty()) {
                        Os.sleep(1);
                        if (!errors.isEmpty()) {
                            LOG.errorW().$("waiting for barrier version interrupted due to errors [table=").$(tableName).I$();
                            return;
                        }
                    }

                    if (transaction.waitAllDone) {
                        while (doneCount.get() != opIndex) {
                            Os.sleep(1);
                            if (!errors.isEmpty()) {
                                LOG.errorW().$("waiting for all done interrupted due to errors [table=").$(tableName).I$();
                                return;
                            }
                        }
                    }

                    WalWriter walWriter;
                    synchronized (writers) {
                        walWriter = writers.get(writerIndex);
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
                        increment |= operation.apply(tempRnd, engine, walWriter, -1, null);
                    }

                    if (transaction.reopenTable) {
                        synchronized (writers) {
                            // Table is dropped, reload all writers
                            for (int ii = 0; ii < writers.size(); ii++) {
                                if (writers.get(ii).getTableToken().getTableName().equals(tableName)) {
                                    writers.get(ii).close();
                                    writers.setQuick(ii, (WalWriter) engine.getTableWriterAPI(tableName, "apply trans test"));
                                }
                            }
                        }
                        forceReaderReload.incrementAndGet();
                        engine.releaseInactive();
                    } else {
                        if (transaction.rollback) {
                            assert !transaction.hasReplaceRange();
                            walWriter.rollback();
                        } else {
                            if (transaction.hasReplaceRange()) {
                                walWriter.commitWithParams(
                                        transaction.getReplaceLoTs(),
                                        transaction.getReplaceHiTs(),
                                        WAL_DEDUP_MODE_REPLACE_RANGE
                                );
                                increment = true;
                            } else {
                                walWriter.commit();
                            }
                        }
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
                e.printStackTrace();
                errors.add(e);
            } finally {
                Path.clearThreadLocals();
            }
        });
    }

    private void drainWalQueue(Rnd applyRnd, String tableName) {
        try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0);
             O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1);
             TableReader rdr1 = getReaderHandleTableDropped(tableName);
             TableReader rdr2 = getReaderHandleTableDropped(tableName)
        ) {
            CheckWalTransactionsJob checkWalTransactionsJob = new CheckWalTransactionsJob(engine);
            while (walApplyJob.run(0) || checkWalTransactionsJob.run(0)) {
                forceReleaseTableWriter(applyRnd);
                purgeAndReloadReaders(applyRnd, rdr1, rdr2, purgeJob, 0.25);
            }
        }
    }

    private void forceReleaseTableWriter(Rnd applyRnd) {
        // Sometimes WAL Apply Job does not finish table in one go and return TableWriter to the pool
        // where it can be fully closed before continuing the WAL application Test TableWriter closures.
        if (applyRnd.nextDouble() < 0.8) {
            engine.releaseInactive();
        }
    }

    private TableReader getReader(String tableName) {
        return engine.getReader(engine.verifyTableName(tableName));
    }

    private TableReader getReaderHandleTableDropped(String tableNameWal) {
        while (true) {
            try {
                return getReader(tableNameWal);
            } catch (CairoException e) {
                if (Chars.contains(e.getFlyweightMessage(), "table does not exist")
                        || Chars.contains(e.getFlyweightMessage(), "table name is reserved")
                        || e instanceof EntryLockedException) {
                    LOG.error().$((Throwable) e).$();
                    Os.sleep(10);
                } else {
                    throw e;
                }
            }
        }
    }

    private void runApplyThread(AtomicInteger done, ConcurrentLinkedQueue<Throwable> errors, Rnd applyRnd) {
        try {
            ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
            int i = 0;
            CheckWalTransactionsJob checkJob = new CheckWalTransactionsJob(engine);
            try (ApplyWal2TableJob job = new ApplyWal2TableJob(engine, 0)) {
                while (done.get() == 0 && errors.isEmpty()) {
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

    private void runPurgePartitionJob(AtomicInteger done, AtomicInteger forceReaderReload, ConcurrentLinkedQueue<Throwable> errors, Rnd runRnd, String tableNameBase, int tableCount, boolean multiTable) {
        ObjList<TableReader> readers = new ObjList<>();
        try {
            try (O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1)) {
                int forceReloadNum = forceReaderReload.get();
                for (int i = 0; i < tableCount; i++) {
                    String tableNameWal = multiTable ? getWalParallelApplyTableName(tableNameBase, i) : tableNameBase;
                    readers.add(getReaderHandleTableDropped(tableNameWal));
                    readers.add(getReaderHandleTableDropped(tableNameWal));
                }

                while (done.get() == 0 && errors.isEmpty()) {
                    if (forceReaderReload.get() != forceReloadNum) {
                        forceReloadNum = forceReaderReload.get();
                        for (int i = 0; i < readers.size(); i++) {
                            String tableNameWal = multiTable ? getWalParallelApplyTableName(tableNameBase, i / 2) : tableNameBase;
                            readers.get(i).close();
                            readers.setQuick(i, getReaderHandleTableDropped(tableNameWal));
                        }
                    }
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
            try (WalPurgeJob job = new WalPurgeJob(engine)) {
                while (done.get() == 0 && errors.isEmpty()) {
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

    private LongList unionIntervals(LongList existingIntervals, long replaceLoTs, long replaceHiTs) {
        var intervals = new LongList();
        if (existingIntervals != null) {
            intervals.add(existingIntervals);
        }
        intervals.add(replaceLoTs);
        intervals.add(replaceHiTs);

        if (intervals.size() > 2) {
            IntervalUtils.unionInPlace(intervals, intervals.size() - 2);
        }

        return intervals;
    }

    void assertCounts(String tableNameWal, String timestampColumnName) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.assertEquals(
                    compiler,
                    sqlExecutionContext,
                    "select count() from " + tableNameWal,
                    "select count() from " + tableNameWal + " where " + timestampColumnName + " > '1970-01-01' or " + timestampColumnName + " < '2100-01-01'"
            );
        }
    }

    protected void assertStringColDensity(String tableNameWal) {
        try (TableReader reader = getReader(tableNameWal)) {
            TableReaderMetadata metadata = reader.getMetadata();
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                int columnType = metadata.getColumnType(i);
                if (ColumnType.isVarSize(columnType)) {
                    for (int partitionIndex = 0; partitionIndex < reader.getPartitionCount(); partitionIndex++) {
                        reader.openPartition(partitionIndex);
                        if (PartitionFormat.NATIVE == reader.getPartitionFormat(partitionIndex)) {
                            int columnBase = reader.getColumnBase(partitionIndex);
                            MemoryR dCol = reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, i));
                            MemoryR iCol = reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, i) + 1);

                            long colTop = reader.getColumnTop(columnBase, i);
                            long rowCount = reader.getPartitionRowCount(partitionIndex) - colTop;
                            long dColAddress = dCol == null ? 0 : dCol.getPageAddress(0);
                            if (DebugUtils.isSparseVarCol(rowCount, iCol.getPageAddress(0), dColAddress, columnType)) {
                                Assert.fail("var column " + reader.getMetadata().getColumnName(i) + ", columnType " + ColumnType.nameOf(columnType)
                                        + " is not dense, .i file record size is different from .d file record size");
                            }
                        }
                    }
                }
            }
        }
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

    protected void runFuzz(String tableName, Rnd rnd) throws Exception {
        String tableNameWal = tableName + "_wal";
        String tableNameWal2 = tableName + "_wal_parallel";
        String tableNameNoWal = tableName + "_nonwal";

        createInitialTableWal(tableNameWal, initialRowCount);
        createInitialTableWal(tableNameWal2, initialRowCount);
        ObjList<FuzzTransaction> transactions = generateTransactions(tableNameWal, rnd);

        TableToken nonWalTt = createInitialTableNonWal(tableNameNoWal, transactions);

        try {
            String timestampColumnName;
            try (TableMetadata meta = engine.getTableMetadata(nonWalTt)) {
                timestampColumnName = meta.getColumnName(meta.getTimestampIndex());
            }

            long startMicro = System.nanoTime() / 1000;
            applyNonWal(transactions, tableNameNoWal, rnd);
            long endNonWalMicro = System.nanoTime() / 1000;
            long nonWalTotal = endNonWalMicro - startMicro;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertMinMaxTimestamp(compiler, sqlExecutionContext, tableNameNoWal);
            }

            applyWal(transactions, tableNameWal, 1, rnd);

            long endWalMicro = System.nanoTime() / 1000;
            long walTotal = endWalMicro - endNonWalMicro;

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertMinMaxTimestamp(compiler, sqlExecutionContext, tableNameWal);

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

                assertMinMaxTimestamp(compiler, sqlExecutionContext, tableNameWal2);
            }

            assertCounts(tableNameWal, timestampColumnName);
            assertCounts(tableNameNoWal, timestampColumnName);
            assertStringColDensity(tableNameWal);
        } finally {
            Misc.freeObjListAndClear(transactions);
        }
    }

    protected void runFuzz(Rnd rnd, String tableNameBase, int tableCount, boolean randomiseProbs, boolean randomiseCounts) throws Exception {
        ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
        try {
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
                            rnd.nextDouble(),
                            0.01,
                            0.0,
                            0.1 * rnd.nextDouble(),
                            rnd.nextDouble(),
                            0.0,
                            0.05,
                            0.1 * rnd.nextDouble()
                    );
                }
                if (randomiseCounts) {
                    setFuzzCounts(
                            rnd.nextBoolean(),
                            rnd.nextInt(2_000_000),
                            rnd.nextInt(1000),
                            randomiseStringLengths(rnd, 1000),
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

            applyManyWalParallel(fuzzTransactions, rnd, tableNameBase, true, true);
            checkNoSuspendedTables(new ObjHashSet<>());

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (int i = 0; i < tableCount; i++) {
                    String tableNameNoWal = tableNameBase + "_" + i + "_nonwal";
                    String tableNameWal = getWalParallelApplyTableName(tableNameBase, i);
                    LOG.infoW().$("comparing tables ").$(tableNameNoWal).$(" and ").$(tableNameWal).$();
                    String limit = "";
                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal + limit, tableNameWal + limit, LOG);
                    assertRandomIndexes(tableNameNoWal, tableNameWal, rnd);
                    assertMinMaxTimestamp(compiler, sqlExecutionContext, tableNameNoWal);
                    assertMinMaxTimestamp(compiler, sqlExecutionContext, tableNameWal);
                }
            }
        } finally {
            for (int i = 0, n = fuzzTransactions.size(); i < n; i++) {
                Misc.freeObjListAndClear(fuzzTransactions.get(i));
            }
        }
    }
}
