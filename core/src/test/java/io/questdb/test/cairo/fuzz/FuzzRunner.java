/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.DebugUtils;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.PartitionCompactionScanJob;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.nanotime.Nanos;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.QueryAssertion;
import io.questdb.test.cairo.TestTableReaderRecord;
import io.questdb.test.fuzz.FuzzAddCoveringIndexOperation;
import io.questdb.test.fuzz.FuzzChangeColumnTypeOperation;
import io.questdb.test.fuzz.FuzzConvertPartitionToNativeOperation;
import io.questdb.test.fuzz.FuzzConvertPartitionToParquetOperation;
import io.questdb.test.fuzz.FuzzDropCreateTableOperation;
import io.questdb.test.fuzz.FuzzDropIndexOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionGenerator;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

public class FuzzRunner {
    public final static int MAX_WAL_APPLY_TIME_PER_TABLE_CEIL = 250;
    public static final String TXN_ZERO_SYMBOL_COLUMN = "sym0";
    protected static final Log LOG = LogFactory.getLog(AbstractFuzzTest.class);
    protected static final StringSink sink = new StringSink();
    // the CONVERT PARTITION operations that applyWalChecked() checked
    private static final AtomicInteger checkedFormatChanges = new AtomicInteger();
    // the widenings of columns without a NULL sentinel that applyWalChecked() checked, and those of them that ran
    // while the table had a parquet partition
    private static final AtomicInteger checkedNoSentinelWidenings = new AtomicInteger();
    private static final AtomicInteger checkedNoSentinelWideningsOnParquet = new AtomicInteger();
    // the widenings that applyWalChecked() checked over a day whose NULLs of the column do not lead the day: the rows
    // of a column top that come after rows with values, which a split or composite day converts into one row group;
    // and those of them that a lazy change decodes at the width of the source type, see
    // FuzzChangeColumnTypeOperation.isDecodedAtSourceWidth()
    private static final AtomicInteger checkedNoSentinelWideningsOverNonLeadingNulls = new AtomicInteger();
    private static final AtomicInteger checkedSourceWidthWideningsOverNonLeadingNulls = new AtomicInteger();
    private static final int MAX_COVERED_KEYS_CHECKED = 32;
    // Share of fuzz runs that use TIMESTAMP_NS designated timestamp instead of TIMESTAMP
    private static final int NANOS_TIMESTAMP_PERCENT = 40;
    // the table that convertNoSentinelValues() converts every value of a type without a NULL sentinel in
    private static final String NO_SENTINEL_VALUES_TABLE = "fuzz_no_sentinel_values";
    private static final long OPEN_RETRY_TIMEOUT_MILLIS = 10_000;
    // Columns a covering index on sym2 / sym_top may INCLUDE; sym_top's add the column-top columns.
    private static final String[] SYM2_INCLUDE_CANDIDATES = {"c1", "c2", "c3", "rnd_str", "bool1"};
    private static final String[] SYM_TOP_INCLUDE_CANDIDATES = {"c1", "c3", "bool1", "long_top", "str_top"};
    // the largest local row id stands for the partition size line of a snapshot
    private static final long ROW_ID_PARTITION_SIZE = Rows.toLocalRowID(-1L);
    private static final int SNAPSHOT_SAMPLE_ROWS = 16;
    private static final int SNAPSHOT_TAIL_ROWS = 16;
    private final TableSequencerAPI.TableSequencerCallback checkNoSuspendedTablesRef;
    // the native conversions of every value of a type without a NULL sentinel, by source type and target type
    private final Map<String, Map<String, String>> noSentinelConversions = new HashMap<>();
    private final Map<TableReader, ReaderSnapshot> readerSnapshots = Collections.synchronizedMap(new IdentityHashMap<>());
    // the columns the generator widened from a type without a NULL sentinel, see setNoSentinelWideningChecked()
    private final ObjHashSet<String> widenedColumns = new ObjHashSet<>();
    protected int initialRowCount;
    protected int partitionCount;
    private double addCoveringIndexProb;
    private long applySeed0;
    private long applySeed1;
    private double backfillProb;
    private double cancelRowsProb;
    private double colAddProb;
    private double colRemoveProb;
    private double colRenameProb;
    private double colTypeChangeProb;
    private boolean createWalAsParquet;
    private double dataAddProb;
    private double dropIndexProb;
    private CairoEngine engine;
    private double equalTsRowsProb;
    private FailureFileFacade ff;
    private int fuzzRowCount;
    private boolean hasApplySeeds;
    private boolean hasPoolColumnPurgeJob;
    private int ioFailureCount;
    private int ioFailureCreatedCount;
    private boolean isNoSentinelTypeChangeEnabled;
    private boolean isNoSentinelWideningChecked;
    private boolean isO3;
    private double notSetProb;
    private double nullSetProb;
    private int parallelWalCount;
    private long parquetConversionRequestHint = Long.MIN_VALUE;
    private double parquetConversionRequestProb;
    private double partitionDropProb;
    private double partitionToNativeProb;
    private double partitionToParquetProb;
    private double queryProb;
    private double replaceInsertProb;
    private double rollbackProb;
    private long s0;
    private long s1;
    private double setParquetEncodingProb;
    private double setTableFormatProb;
    private double setTtlProb;
    private SqlExecutionContext sqlExecutionContext;
    private int strLen;
    private double symbolAccessValidationProb;
    private int symbolCountMax;
    private int symbolStrLenMax;
    private double tableDropProb;
    private int timestampType = ColumnType.TIMESTAMP_MICRO;
    private int transactionCount;
    private double truncateProb;
    // INCLUDE list and index type of the covering index on sym0, the column the txn-0 setup adds to
    // the still-empty table; null when the setup is off
    private IntList txnZeroCoveredIncludeIndices;
    private String txnZeroCoveringIndexTypeName;
    private double vacuumProb = 0.001;

    public FuzzRunner() {
        checkNoSuspendedTablesRef = this::checkNoSuspendedTables;
    }

    public static int getCheckedFormatChanges() {
        return checkedFormatChanges.get();
    }

    public static int getCheckedNoSentinelWidenings() {
        return checkedNoSentinelWidenings.get();
    }

    public static int getCheckedNoSentinelWideningsOnParquet() {
        return checkedNoSentinelWideningsOnParquet.get();
    }

    public static int getCheckedNoSentinelWideningsOverNonLeadingNulls() {
        return checkedNoSentinelWideningsOverNonLeadingNulls.get();
    }

    public static int getCheckedSourceWidthWideningsOverNonLeadingNulls() {
        return checkedSourceWidthWideningsOverNonLeadingNulls.get();
    }

    public static String getWalParallelApplyTableName(String tableNameBase, int i) {
        return tableNameBase + "_" + i;
    }

    public static void resetCheckedChanges() {
        checkedFormatChanges.set(0);
        checkedNoSentinelWidenings.set(0);
        checkedNoSentinelWideningsOnParquet.set(0);
        checkedNoSentinelWideningsOverNonLeadingNulls.set(0);
        checkedSourceWidthWideningsOverNonLeadingNulls.set(0);
    }

    public void after() {
        readerSnapshots.clear();
        if (this.s0 != 0 || this.s1 != 0) {
            LOG.info().$("random seeds: ").$(s0).$("L, ").$(s1).$('L').$();
            System.out.printf("random seeds: %dL, %dL%n", s0, s1);
        }
    }

    public void applyManyWalParallel(ObjList<ObjList<FuzzTransaction>> fuzzTransactions, Rnd rnd, String tableNameBase, boolean multiTable, boolean waitApply) {
        applyManyWalParallel(fuzzTransactions, rnd, tableNameBase, multiTable, waitApply, null);
    }

    public void applyManyWalParallel(
            ObjList<ObjList<FuzzTransaction>> fuzzTransactions,
            Rnd rnd,
            String tableNameBase,
            boolean multiTable,
            boolean waitApply,
            @Nullable Runnable onWalWritersDone
    ) {
        final ObjList<WalWriter> writers = new ObjList<>();
        final int tableCount = fuzzTransactions.size();
        final AtomicInteger done = new AtomicInteger();
        final AtomicInteger forceReaderReload = new AtomicInteger();
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final ObjList<Thread> applyThreads = new ObjList<>();
        final ObjList<Thread> threads = new ObjList<>();

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

                Thread compactionJobThread = new Thread(() -> runPartitionCompactionJob(done, errors));
                compactionJobThread.start();
                applyThreads.add(compactionJobThread);

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
            try {
                for (int i = 0; i < threads.size(); i++) {
                    int k = i;
                    TestUtils.unchecked(() -> threads.get(k).join());
                }
            } finally {
                // Stop external WAL producers before the apply workers drain and exit. A producer
                // that waits for this method to return can otherwise keep their drain alive forever.
                if (onWalWritersDone != null) {
                    try {
                        onWalWritersDone.run();
                    } catch (Throwable th) {
                        errors.add(th);
                    }
                }
                done.incrementAndGet();
                Misc.freeObjList(writers);
                // Join the apply/purge workers before reading errors: a worker that fails after the
                // writers finished must still surface, and none of them may outlive this call.
                for (int i = 0; i < applyThreads.size(); i++) {
                    int k = i;
                    TestUtils.unchecked(() -> applyThreads.get(k).join());
                }
            }
        }

        for (Throwable e : errors) {
            throw new RuntimeException(e);
        }

        engine.releaseInactive();
    }

    public void applyNonWal(ObjList<FuzzTransaction> transactions, String tableName, Rnd reloadRnd) {
        TableWriter writer = TestUtils.getWriter(engine, tableName);
        TableReader rdr1 = getReader(tableName);
        TableReader rdr2 = getReader(tableName);
        snapshotReader(rdr1);
        snapshotReader(rdr2);
        // Half of the runs reload the readers after every commit, so a reader often holds the txn right
        // before a commit. The other half keep the 25% rate, under which readers hold old txns longer.
        final double reloadProbability = reloadRnd.nextBoolean() ? 0.25 : 1.0;

        calculateReplaceRanges(transactions);

        try (
                O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1);
                ColumnPurgeJob columnPurgeJob = openColumnPurgeJob()
        ) {
            int transactionSize = transactions.size();
            Rnd rnd = newApplyRnd();
            int failuresObserved = 0;
            for (int i = 0; i < transactionSize; i++) {
                if (writer == null) {
                    // A prior transaction's IO-failure recovery could not re-acquire the writer: a re-armed
                    // one-shot fault fired during that recovery's getWriter(), and the catch below handles
                    // only the drop/recreate case, so writer is null. Re-acquire it here, once the one-shot
                    // fault has cleared, so no operation dereferences a null writer. A persistent failure
                    // throws from here.
                    writer = TestUtils.getWriter(engine, tableName);
                }
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
                        snapshotReader(rdr1);
                        snapshotReader(rdr2);
                    } else {
                        if (transaction.rollback) {
                            writer.rollback();
                        } else {
                            writer.commit();
                        }
                        for (int opIdx = 0; opIdx < size; opIdx++) {
                            if (transaction.operationList.getQuick(opIdx) instanceof FuzzDropIndexOperation dropIndexOp) {
                                Misc.free(writer);
                                writer = null;
                                dropIndexOp.executePostDrain(engine, tableName);
                                writer = TestUtils.getWriter(engine, tableName);
                            }
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
                            if (ex.isTableDoesNotExist() && transaction.operationList.get(0) instanceof FuzzDropCreateTableOperation dropCreateTableOperation) {
                                // Table is dropped, but failed to recreate.
                                // Create it again.
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
                    purgeAndReloadReaders(reloadRnd, rdr1, rdr2, purgeJob, columnPurgeJob, reloadProbability);
                } catch (CairoException | CairoError e) {
                    int failures = ff.failureGenerated();
                    if (failures > failuresObserved) {
                        failuresObserved = failures;
                        LOG.info().$("expected IO failure observed: ").$((Throwable) e).$();
                        readerSnapshots.remove(rdr1);
                        readerSnapshots.remove(rdr2);
                        rdr1 = Misc.free(rdr1);
                        rdr2 = Misc.free(rdr2);
                        // A failed goActive() leaves the reader torn, and close() would hand it back to the pool.
                        // Drop the idle pooled readers so getReader() opens fresh ones.
                        engine.releaseInactive();
                        rdr1 = getReader(tableName);
                        rdr2 = getReader(tableName);
                    } else {
                        throw e;
                    }
                }
            }
            runColumnPurgeJob(columnPurgeJob);
        } finally {
            // Don't mess simulating failures anymore, we are done here.
            ff.clearFailures();
            Misc.free(rdr1);
            Misc.free(rdr2);
            Misc.free(writer);
            if (ff != null) {
                ff.clearFailures();
            }
        }
    }

    public void applyToWal(ObjList<FuzzTransaction> transactions, String tableName, int walWriterCount, Rnd applyRnd) {
        // Acquisition and use are inside the try so a mid-loop Error/Exception (e.g. a fault-injected crash
        // propagating synchronously out of writer.commit() under W=0) still returns every checked-out
        // WalWriter to the pool via the finally below, instead of leaking it (a leaked writer later fails a
        // pool's releaseAll() with "table left behind on pool shutdown").
        ObjList<WalWriter> writers = new ObjList<>();
        try {
            for (int i = 0; i < walWriterCount; i++) {
                writers.add((WalWriter) engine.getTableWriterAPI(tableName, "apply trans test"));
            }

            Rnd tempRnd = newApplyRnd();
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
        } finally {
            Misc.freeObjList(writers);
        }
    }

    public void applyWal(ObjList<FuzzTransaction> transactions, String tableName, int walWriterCount, Rnd applyRnd) {
        TableToken tableToken = engine.verifyTableName(tableName);
        if (isNoSentinelWideningChecked && walWriterCount == 1) {
            applyWalChecked(transactions, tableName, applyRnd);
        } else {
            applyToWal(transactions, tableName, walWriterCount, applyRnd);
            drainWalQueue(applyRnd, tableName);
        }
        if (executeDropIndexOps(transactions, tableName, applyRnd)) {
            drainWalQueue(applyRnd, tableName);
        }
        Assert.assertFalse("Table is suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
    }

    /**
     * Holds the index of every indexed SYMBOL column of the table against scans that read no index: for each distinct
     * key, up to {@link #MAX_COVERED_KEYS_CHECKED} of them, {@code WHERE sym = key} must return through the index the
     * rows a full scan returns. Unlike {@link #assertRandomIndexes}, it needs no second table, so it can check a table
     * that no other table mirrors, such as one a checkpoint restore just left.
     */
    public void assertIndexesMatchScans(String tableName) throws SqlException {
        final ObjList<String> indexedColumnNames = new ObjList<>();
        try (TableReader reader = getReader(tableName)) {
            final TableReaderMetadata metadata = reader.getMetadata();
            for (int columnIndex = 0, n = metadata.getColumnCount(); columnIndex < n; columnIndex++) {
                if (ColumnType.isSymbol(metadata.getColumnType(columnIndex)) && metadata.isColumnIndexed(columnIndex)) {
                    indexedColumnNames.add(metadata.getColumnName(columnIndex));
                }
            }
        }
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            for (int i = 0, n = indexedColumnNames.size(); i < n; i++) {
                checkIndexAgainstScan(compiler, tableName, indexedColumnNames.getQuick(i));
            }
        }
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

    public void createInitialTableWal(String tableName, String timestampType) throws SqlException {
        createInitialTable(tableName, true, initialRowCount, ColumnType.typeOf(timestampType));
        engine.verifyTableName(tableName);
    }

    public TableToken createInitialTableWal(String tableName, int initialRowCount) throws SqlException {
        return createInitialTable(tableName, true, initialRowCount);
    }

    public Rnd generateRandom(Log log) {
        Rnd rnd = TestUtils.generateRandom(log);
        s0 = rnd.getSeed0();
        s1 = rnd.getSeed1();
        initRandomParams(rnd, log);
        return rnd;
    }

    public Rnd generateRandom(Log log, long s0, long s1) {
        Rnd rnd = TestUtils.generateRandom(log, s0, s1);
        this.s0 = rnd.getSeed0();
        this.s1 = rnd.getSeed1();
        initRandomParams(rnd, log);
        return rnd;
    }

    /**
     * Generates fuzz transactions for the table. <code>start</code> and <code>end</code> are
     * in microseconds, this method converts them to the unit of the table's designated timestamp.
     */
    public ObjList<FuzzTransaction> generateSet(
            Rnd rnd,
            TableRecordMetadata sequencerMetadata,
            TableMetadata tableMetadata,
            long start,
            long end,
            String tableName
    ) {
        final TimestampDriver driver = ColumnType.getTimestampDriver(sequencerMetadata.getTimestampType());
        return FuzzTransactionGenerator.generateSet(
                initialRowCount,
                sequencerMetadata,
                tableMetadata,
                rnd,
                driver.fromMicros(start),
                driver.fromMicros(end),
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
                partitionToParquetProb,
                partitionToNativeProb,
                truncateProb,
                tableDropProb,
                setTtlProb,
                replaceInsertProb,
                symbolAccessValidationProb,
                queryProb,
                strLen,
                generateSymbols(rnd, rnd.nextInt(Math.max(1, symbolCountMax - 5)) + 5, symbolStrLenMax, tableName),
                (int) sequencerMetadata.getMetadataVersion(),
                setParquetEncodingProb,
                addCoveringIndexProb,
                dropIndexProb,
                setTableFormatProb,
                vacuumProb,
                backfillProb,
                isNoSentinelTypeChangeEnabled,
                isNoSentinelWideningChecked,
                widenedColumns
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
        // getTableMetadata reloads the table _meta and hits the same transient read timeout as a reader
        // open; getLegacyMetadata reads the sequencer from memory and cannot, so it stays bare.
        try (TableRecordMetadata sequencerMetadata = engine.getLegacyMetadata(tableToken);
             TableMetadata tableMetadata = openWithRetries(() -> engine.getTableMetadata(tableToken), false)
        ) {
            return generateSet(rnd, sequencerMetadata, tableMetadata, start, end, tableName);
        }
    }

    public FailureFileFacade getFileFacade() {
        return ff;
    }

    public int getTimestampType() {
        return timestampType;
    }

    public int getTransactionCount() {
        return transactionCount;
    }

    public void purgeAndReloadReaders(
            Rnd reloadRnd,
            TableReader rdr1,
            TableReader rdr2,
            O3PartitionPurgeJob purgeJob,
            @Nullable ColumnPurgeJob columnPurgeJob,
            double realoadThreashold
    ) {
        if (reloadRnd.nextDouble() < realoadThreashold) {
            purgeJob.run();
            runColumnPurgeJob(columnPurgeJob);
            reloadReader(reloadRnd, rdr1, "1");
            reloadReader(reloadRnd, rdr2, "2");
        }
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

    public void setBackfillProb(double backfillProb) {
        this.backfillProb = backfillProb;
    }

    /**
     * When true, fuzz-created WAL tables get FORMAT PARQUET in their CREATE TABLE
     * statement so every partition is born parquet from the very first WAL apply.
     */
    public void setCreateWalAsParquet(boolean createWalAsParquet) {
        this.createWalAsParquet = createWalAsParquet;
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
        setFuzzProbabilities(
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
                0.0,
                0.0,
                truncateProb,
                tableDropProb,
                setTtlProb,
                replaceInsertProb,
                symbolAccessValidationProb,
                0.0
        );
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
            double partitionToParquetProb,
            double partitionToNativeProb,
            double truncateProb,
            double tableDropProb,
            double setTtlProb,
            double replaceInsertProb,
            double symbolAccessValidationProb,
            double queryProb
    ) {
        setFuzzProbabilities(
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
                partitionToParquetProb,
                partitionToNativeProb,
                truncateProb,
                tableDropProb,
                setTtlProb,
                replaceInsertProb,
                symbolAccessValidationProb,
                queryProb,
                0.0
        );
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
            double partitionToParquetProb,
            double partitionToNativeProb,
            double truncateProb,
            double tableDropProb,
            double setTtlProb,
            double replaceInsertProb,
            double symbolAccessValidationProb,
            double queryProb,
            double setParquetEncodingProb
    ) {
        setFuzzProbabilities(
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
                partitionToParquetProb,
                partitionToNativeProb,
                truncateProb,
                tableDropProb,
                setTtlProb,
                replaceInsertProb,
                symbolAccessValidationProb,
                queryProb,
                setParquetEncodingProb,
                0.0,
                0.0
        );
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
            double partitionToParquetProb,
            double partitionToNativeProb,
            double truncateProb,
            double tableDropProb,
            double setTtlProb,
            double replaceInsertProb,
            double symbolAccessValidationProb,
            double queryProb,
            double setParquetEncodingProb,
            double setTableFormatProb,
            double addCoveringIndexProb
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
        this.partitionToParquetProb = partitionToParquetProb;
        this.partitionToNativeProb = partitionToNativeProb;
        this.truncateProb = truncateProb;
        this.tableDropProb = tableDropProb;
        this.setTtlProb = setTtlProb;
        this.replaceInsertProb = replaceInsertProb;
        this.symbolAccessValidationProb = symbolAccessValidationProb;
        this.queryProb = queryProb;
        this.setParquetEncodingProb = setParquetEncodingProb;
        this.setTableFormatProb = setTableFormatProb;
        this.addCoveringIndexProb = addCoveringIndexProb;
    }

    /**
     * Makes the generator drop the index of a symbol column, and index it again with BITMAP or a POSTING
     * kind five times in eight, see {@link FuzzDropIndexOperation}. Every setFuzzProbabilities() call
     * keeps it.
     */
    public void setDropIndexProbability(double dropIndexProb) {
        this.dropIndexProb = dropIndexProb;
    }

    /**
     * Tells the applies that a worker pool runs a {@link ColumnPurgeJob}. A pool job that fails to save a purge frees
     * the purge log but keeps draining the queue, so the purge log cannot tell.
     */
    public void setHasPoolColumnPurgeJob(boolean hasPoolColumnPurgeJob) {
        this.hasPoolColumnPurgeJob = hasPoolColumnPurgeJob;
    }

    /**
     * Makes the generator change a STRING, SYMBOL or VARCHAR column to BOOLEAN, BYTE or SHORT on a parquet
     * partition and those types back to text, with in-order writes only, see
     * FuzzTransactionGenerator.generateSet(). The parallel WAL apply needs one WAL writer to keep the order.
     */
    public void setNoSentinelTypeChangeEnabled(boolean isNoSentinelTypeChangeEnabled) {
        this.isNoSentinelTypeChangeEnabled = isNoSentinelTypeChangeEnabled;
    }

    /**
     * Makes the generator change BYTE and SHORT columns to types with NULL, the first time lazily on parquet
     * partitions, see FuzzTransactionGenerator.generateSet(). The single-writer WAL apply of
     * {@link #runFuzz(String, Rnd)} then drains the WAL before and after each CONVERT PARTITION and each column type
     * change, and checks the rows of the WAL table, see {@link #applyWalChecked}: a CONVERT PARTITION keeps every
     * row, and a widening reads every stored value as native ALTER COLUMN TYPE converts it, with a 0 or false that
     * may read NULL, as a column top does.
     * <p>
     * The WAL tables and the non-WAL twin keep different column tops: only the WAL tables convert partitions, and
     * their merges cover other rows than the twin's commits. The comparisons with the twin therefore take NULL and
     * the converted 0 of a widened column as one value, and the symbol index checks, which compare every column,
     * do not run. The generator should rename and drop no column and drop no table, so that the widened columns
     * keep their names.
     */
    public void setNoSentinelWideningChecked(boolean isNoSentinelWideningChecked) {
        this.isNoSentinelWideningChecked = isNoSentinelWideningChecked;
    }

    /**
     * Makes the next initial tables start with {@value #TXN_ZERO_SYMBOL_COLUMN}, a SYMBOL column that
     * ALTER TABLE ADD COLUMN adds while the table is still empty, so its column name txn is 0.
     * {@link FuzzAddCoveringIndexOperation} covers it before the first row goes in, and every fuzz
     * operation after that (O3, CONVERT PARTITION, RENAME and DROP COLUMN, purges) runs on that
     * covering index. A .pc sidecar name carries the token 0 for both txn 0 and txn NONE, and a scan
     * that read 0 as NONE once made CONVERT, RENAME and the purges miss the sidecars of such a column.
     * The WAL tables and the non-WAL twin get the same INCLUDE list and index type, drawn from rnd.
     */
    public void setTxnZeroCoveredSymbol(Rnd rnd) {
        // writer indexes of the data_temp columns c1 (LONG), c2 (SYMBOL), c3 (INT), rnd_str (STRING)
        // and bool1 (BOOLEAN)
        final IntList candidates = new IntList();
        candidates.add(0);
        candidates.add(1);
        candidates.add(4);
        candidates.add(7);
        candidates.add(8);
        txnZeroCoveredIncludeIndices = new IntList();
        for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
            final int pick = rnd.nextInt(candidates.size());
            txnZeroCoveredIncludeIndices.add(candidates.getQuick(pick));
            candidates.removeIndex(pick);
        }
        final double indexKindPick = rnd.nextDouble();
        txnZeroCoveringIndexTypeName = indexKindPick < 0.6
                ? "POSTING"
                : (indexKindPick < 0.8 ? "POSTING DELTA" : "POSTING EF");
        LOG.info().$("txn 0 covered symbol [index=").$(txnZeroCoveringIndexTypeName)
                .$(", include=").$(txnZeroCoveredIncludeIndices).I$();
    }

    /**
     * A timestamp whose partition {@link #requestParquetConversionDuringApply} converts half the time instead of a
     * random one: a test that knows where its next commit lands points the conversion there.
     * {@link Long#MIN_VALUE}, the default, leaves every pick random.
     */
    public void setParquetConversionRequestHint(long timestamp) {
        this.parquetConversionRequestHint = timestamp;
    }

    /**
     * Per WAL apply step of {@link #applyWal}, the probability of asking {@link PartitionCompactionScanJob} to convert
     * a random partition to parquet, with WAL apply landing between the conversion's build and its swap - see
     * {@link #requestParquetConversionDuringApply}. 0, the default, draws nothing from the apply seed.
     */
    public void setParquetConversionRequestProb(double parquetConversionRequestProb) {
        this.parquetConversionRequestProb = parquetConversionRequestProb;
    }

    /**
     * Sets designated timestamp type of the tables the runner creates. {@link #generateRandom(Log)}
     * picks it randomly, call this after it to pin the type.
     */
    public void setTimestampType(int timestampType) {
        this.timestampType = timestampType;
    }

    /**
     * Inserts VACUUM before each generated transaction with this probability, independently of operation weights.
     */
    public void setVacuumProbability(double vacuumProb) {
        this.vacuumProb = vacuumProb;
    }

    public void withDb(CairoEngine engine, SqlExecutionContext sqlExecutionContext) {
        this.engine = engine;
        this.sqlExecutionContext = sqlExecutionContext;
        this.ff = new FailureFileFacade(engine.getConfiguration().getFilesFacade());
    }

    // the first line that differs between two printed results, with the header line
    private static String firstDifference(String before, String after) {
        final String[] beforeLines = before.split("\n", -1);
        final String[] afterLines = after.split("\n", -1);
        for (int i = 0, n = Math.max(beforeLines.length, afterLines.length); i < n; i++) {
            final String beforeLine = i < beforeLines.length ? beforeLines[i] : "<no line>";
            final String afterLine = i < afterLines.length ? afterLines[i] : "<no line>";
            if (!beforeLine.equals(afterLine)) {
                return "line " + i + " of " + beforeLines.length + "\nheader: " + beforeLines[0]
                        + "\nbefore: " + beforeLine + "\nafter:  " + afterLine;
            }
        }
        return "no line differs";
    }

    // Tables with different designated timestamp types need separate source tables
    private static String getDataTempTableName(int timestampType) {
        return timestampType == ColumnType.TIMESTAMP_NANO ? "data_temp_ns" : "data_temp";
    }

    // the text printSql() shows for a NULL of the type
    private static String nullTextOf(int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.INT, ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE -> "null";
            default -> "";
        };
    }

    /**
     * Half the time a POSTING index covers 1-3 of {@code candidates}: returns the INCLUDE clause, else null.
     */
    private static String randomIncludeClause(Rnd rnd, String indexTypeClause, String[] candidates) {
        if (!indexTypeClause.startsWith("TYPE POSTING") || rnd.nextBoolean()) {
            return null;
        }
        final StringSink include = new StringSink();
        include.put(" include (");
        final int start = rnd.nextInt(candidates.length);
        final int count = 1 + rnd.nextInt(3);
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                include.put(", ");
            }
            include.put(candidates[(start + i) % candidates.length]);
        }
        return include.put(')').toString();
    }

    /**
     * Picks an index type clause for fuzz table creation. Returns either
     * an empty string (BITMAP — the default), or one of the POSTING
     * variants.
     */
    private static String randomIndexTypeClause(Rnd rnd) {
        double pick = rnd.nextDouble();
        if (pick < 0.5) {
            return ""; // BITMAP (the default for the existing INDEX clause)
        }
        if (pick < 0.7) {
            return "TYPE POSTING";
        }
        if (pick < 0.85) {
            return "TYPE POSTING DELTA";
        }
        return "TYPE POSTING EF";
    }

    private static String timestampToString(TimestampDriver driver, long timestamp) {
        StringSink tsSink = new StringSink();
        driver.append(tsSink, timestamp);
        return tsSink.toString();
    }

    // applies the transactions of run to the WAL table with one WAL writer, drains the WAL, and empties run
    private void applyRun(ObjList<FuzzTransaction> run, String tableName, Rnd applyRnd) {
        if (run.size() > 0) {
            applyToWal(run, tableName, 1, applyRnd);
            run.clear();
        }
        // a check reads the table after a drain: the WAL also holds the ALTERs that created the table
        drainWalQueue(applyRnd, tableName);
    }

    /**
     * Applies the transactions to the WAL table with one WAL writer, in runs that end before each CONVERT PARTITION
     * and each column type change, and checks what each of these does to the rows of the table: see
     * {@link #checkFormatChange} and {@link #checkNoSentinelWidening}. The drain after each run applies the WAL,
     * converts and compacts partitions as {@link #drainWalQueue} does for the whole WAL.
     */
    private void applyWalChecked(ObjList<FuzzTransaction> transactions, String tableName, Rnd applyRnd) {
        final ObjList<FuzzTransaction> run = new ObjList<>();
        for (int i = 0, n = transactions.size(); i < n; i++) {
            final FuzzTransaction transaction = transactions.getQuick(i);
            final FuzzTransactionOperation checkedOperation = findCheckedOperation(transaction);
            if (checkedOperation == null) {
                run.add(transaction);
                continue;
            }
            applyRun(run, tableName, applyRnd);
            run.add(transaction);
            if (checkedOperation instanceof FuzzChangeColumnTypeOperation typeChange) {
                checkNoSentinelWidening(typeChange, tableName, () -> applyRun(run, tableName, applyRnd));
            } else {
                checkFormatChange(tableName, () -> applyRun(run, tableName, applyRnd));
            }
        }
        applyRun(run, tableName, applyRnd);
    }

    private void applyWalParallel(ObjList<FuzzTransaction> transactions, String tableName, Rnd applyRnd) {
        ObjList<ObjList<FuzzTransaction>> tablesTransactions = new ObjList<>();
        tablesTransactions.add(transactions);
        applyManyWalParallel(tablesTransactions, applyRnd, tableName, false, true);
        if (executeDropIndexOps(transactions, tableName, applyRnd)) {
            drainWalQueue(applyRnd, tableName);
        }
    }

    private void assertMinMaxTimestamp(SqlExecutionContext sqlExecutionContext, String tableName) throws Exception {
        // No memory usage check: it reads process-wide memory counters, and the shared pool's purge jobs run
        // alongside these queries, so their scratch buffers would count as the cursor's
        try (TableReader reader = getReader(tableName)) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType());
            if (reader.getMinTimestamp() != Long.MAX_VALUE) {
                new QueryAssertion(engine, sqlExecutionContext, () -> {
                }, "select ts from " + tableName + " order by ts limit 1")
                        .noLeakCheck()
                        .noMemoryUsageCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns(
                                "ts\n" +
                                        timestampToString(driver, reader.getMinTimestamp())
                                        + "\n"
                        );
            }

            if (reader.getMaxTimestamp() != Long.MIN_VALUE) {
                new QueryAssertion(engine, sqlExecutionContext, () -> {
                }, "select ts from " + tableName + " order by ts limit -1")
                        .noLeakCheck()
                        .noMemoryUsageCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns(
                                "ts\n" +
                                        timestampToString(driver, reader.getMaxTimestamp())
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

    private void assertCoveredCursors(
            SqlCompiler compiler,
            String tableName,
            String symbolColumnName,
            CharSequence projection,
            String whereClause,
            boolean alsoCompareUnindexed
    ) throws SqlException {
        final String covered = "select " + projection + " from " + tableName + whereClause;
        final String uncovered = "select /*+ no_covering */ " + projection + " from " + tableName + whereClause;

        final StringSink plan = new StringSink();
        TestUtils.printSql(compiler, sqlExecutionContext, "explain " + covered, plan);

        // The node name and the key attribute are checked separately: a covering factory that
        // carries a backup plan prints "backup: true" between them.
        final String expectedKey = "on: " + symbolColumnName + " with:";
        Assert.assertTrue(
                "covering plan not chosen for " + covered + ", plan was:\n" + plan,
                Chars.contains(plan, "CoveringIndex") && Chars.contains(plan, expectedKey)
        );
        LOG.info().$("checking covered values: ").$safe(covered).I$();
        TestUtils.assertSqlCursors(compiler, sqlExecutionContext, uncovered, covered, LOG);
        if (alsoCompareUnindexed) {
            // no_covering still reaches the rows through the same index, so the two plans agree
            // whenever the index itself is what is wrong. no_index reads no index at all, which
            // is the only ground truth the covering plan can be held against.
            final String unindexed = "select /*+ no_index */ " + projection + " from " + tableName + whereClause;
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, unindexed, covered, LOG);
        }
    }

    /**
     * Builds {@code key, covered...} for the covering index on {@code symbolColumnName}, the only
     * projection that makes codegen construct a covering cursor at all. Returns false when the
     * column no longer carries a covering index, in which case there is nothing to check.
     */
    private boolean buildCoveredProjection(String tableName, String symbolColumnName, StringSink projection) {
        try (TableReader reader = getReader(tableName)) {
            final TableReaderMetadata metadata = reader.getMetadata();
            final int keyIndex = metadata.getColumnIndexQuiet(symbolColumnName);
            if (keyIndex < 0 || !metadata.isColumnIndexed(keyIndex)) {
                return false; // the column was dropped or un-indexed on this table
            }
            final IntList coveringIndices = metadata.getCoveringColumnIndices(keyIndex);
            if (coveringIndices == null || coveringIndices.size() == 0) {
                return false; // not a COVERING index
            }
            projection.put('"').put(symbolColumnName).put('"');
            for (int c = 0, cn = coveringIndices.size(); c < cn; c++) {
                final int writerIndex = coveringIndices.getQuick(c);
                if (writerIndex < 0) {
                    continue; // covered column was dropped
                }
                for (int r = 0, rn = metadata.getColumnCount(); r < rn; r++) {
                    if (metadata.getWriterIndex(r) == writerIndex) {
                        projection.put(", \"").put(metadata.getColumnName(r)).put('"');
                        break;
                    }
                }
            }
        }
        return true;
    }

    /**
     * The NULL key again, this time confined to a random time range.
     * <p>
     * {@link #checkCoveredValueScan} always reads the whole table, and a whole-table scan reads
     * every partition, so it can only ever ask "does the table have a column top". Which
     * partitions a scan reads is what decides whether the covering plan may serve a NULL key at
     * all, and the interesting boundaries live inside a partition: a range opening after the
     * point the key column was added, but inside a partition that started before it. A scan
     * bounded to a random range walks over those boundaries; an unbounded one never reaches them.
     * <p>
     * Two shapes are drawn: a range between two rows' timestamps, which opens and closes at
     * arbitrary points inside partitions, and a single whole day, which lands on partition
     * boundaries exactly.
     */
    private void checkCoveredNullScanOverInterval(
            SqlCompiler compiler,
            String tableName,
            String symbolColumnName,
            String tsColumnName,
            Rnd rnd,
            long recordCount
    ) throws SqlException {
        if (recordCount < 1) {
            return;
        }
        final StringSink projection = new StringSink();
        if (!buildCoveredProjection(tableName, symbolColumnName, projection)) {
            return;
        }
        final String lo = randomRowTimestamp(compiler, tableName, tsColumnName, rnd, recordCount);
        if (lo == null) {
            return; // the table shrank under us; nothing to bound the scan with
        }
        final String whereClause;
        if (rnd.nextBoolean()) {
            final String hi = randomRowTimestamp(compiler, tableName, tsColumnName, rnd, recordCount);
            if (hi == null) {
                return;
            }
            final String from = lo.compareTo(hi) <= 0 ? lo : hi;
            final String to = lo.compareTo(hi) <= 0 ? hi : lo;
            whereClause = " where \"" + symbolColumnName + "\" = null and \"" + tsColumnName
                    + "\" between '" + from + "' and '" + to + "'";
        } else {
            // the day the row falls in, so the range starts and ends on partition boundaries
            whereClause = " where \"" + symbolColumnName + "\" = null and \"" + tsColumnName
                    + "\" in '" + lo.substring(0, 10) + "'";
        }
        assertCoveredCursors(compiler, tableName, symbolColumnName, projection, whereClause, true);
    }

    /**
     * Reads the covered values back through the covering index and compares them
     * against the same projection with covering disabled.
     * <p>
     * The scans in {@link #checkIndexRandomValueScan} cannot do this: they project
     * {@code *}, so at least one projected column is not in the INCLUDE list,
     * {@code buildCoveringIndexMapping} returns null
     * (SqlCodeGenerator.java:915) and no covering cursor is ever constructed. That
     * left the whole covering sidecar lifecycle - including parquet round-trips -
     * unasserted by the fuzz suite. Projecting exactly the key column plus its
     * covered columns is what forces the covering plan.
     * <p>
     * Every distinct key is checked, not the single random value
     * {@link #checkIndexRandomValueScan} picks. That matters: a dropped sidecar
     * only shows up in the rows of the partition that lost it, and with one random
     * key the odds of sampling such a row are low enough that a deliberately
     * reintroduced parquet-&gt;native covering bug survived four fuzz runs
     * undetected. Keys are checked one at a time so that each comparison stays a
     * single-key equality, which both plans resolve through the same index and
     * therefore emit in the same order without needing an ORDER BY.
     */
    private void checkCoveredValueScan(
            SqlCompiler compiler,
            String tableName,
            String symbolColumnName
    ) throws SqlException {
        final StringSink projection = new StringSink();
        if (!buildCoveredProjection(tableName, symbolColumnName, projection)) {
            return;
        }

        sink.clear();
        TestUtils.printSql(compiler, sqlExecutionContext, "select distinct \"" + symbolColumnName + "\" a from " + tableName + " order by 1", sink);
        int checked = 0;
        for (int lo = sink.indexOf("\n") + 1, hi; lo > 0 && lo < sink.length() && checked < MAX_COVERED_KEYS_CHECKED; lo = hi + 1) {
            hi = sink.indexOf("\n", lo);
            if (hi < 0) {
                hi = sink.length();
            }
            final String value = sink.subSequence(lo, hi).toString();
            if (value.indexOf('\'') >= 0) {
                continue; // a quote would need escaping; skip this key
            }
            checked++;
            // The NULL key is the one a column top can make wrong, so it is the one held against
            // a plan that reads no index.
            assertCoveredCursors(compiler, tableName, symbolColumnName, projection,
                    " where \"" + symbolColumnName + "\" = " + (value.isEmpty() ? "null" : "'" + value + "'"),
                    value.isEmpty());
        }
        if (checked == MAX_COVERED_KEYS_CHECKED) {
            LOG.info().$("covered value check capped at ").$(MAX_COVERED_KEYS_CHECKED)
                    .$(" keys [table=").$safe(tableName).$(", column=").$safe(symbolColumnName).I$();
        }
    }

    // Compares, key by key, the rows an equality filter on the indexed column reads through the index with those it
    // reads from a scan that uses no index. Each comparison is a single-key equality, which both plans emit in the
    // same order. The keys come from a scan too: a DISTINCT over a posting index reads them from that index, and so
    // would never name a key the index lost. They come off the cursor rather than its printout, where the empty
    // string, a key of its own, reads the same as NULL.
    private void checkIndexAgainstScan(SqlCompiler compiler, String tableName, String symbolColumnName) throws SqlException {
        final ObjList<String> keys = new ObjList<>();
        try (
                RecordCursorFactory factory = compiler.compile(
                        "select /*+ no_index */ distinct \"" + symbolColumnName + "\" a from " + tableName + " order by 1",
                        sqlExecutionContext
                ).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                final CharSequence key = record.getSymA(0);
                keys.add(key != null ? key.toString() : null);
            }
        }
        final StringSink plan = new StringSink();
        int checked = 0;
        for (int i = 0, n = keys.size(); i < n && checked < MAX_COVERED_KEYS_CHECKED; i++) {
            final String key = keys.getQuick(i);
            if (key != null && key.indexOf('\'') >= 0) {
                continue; // a quote would need escaping; skip this key
            }
            checked++;
            final String whereClause = " where \"" + symbolColumnName + "\" = " + (key != null ? "'" + key + "'" : "null");
            final String indexed = "select * from " + tableName + whereClause;
            plan.clear();
            TestUtils.printSql(compiler, sqlExecutionContext, "explain " + indexed, plan);
            Assert.assertTrue(
                    "index not chosen for " + indexed + ", plan was:\n" + plan,
                    Chars.contains(plan, "on: " + symbolColumnName)
            );
            LOG.info().$("checking index against scan: ").$safe(indexed).I$();
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, "select /*+ no_index */ * from " + tableName + whereClause, indexed, LOG);
        }
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
            LOG.info().$("checking random index with filter: ").$safe(indexedWhereClause).I$();
            String limit = ""; // for debugging, e.g. " limit 100"
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, expectedTableName + indexedWhereClause + limit, actualTableName + indexedWhereClause + limit, LOG);
            // Now let's do backward order assertion
            String orderBy = " order by " + tsColumnName + " desc";
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, expectedTableName + indexedWhereClause + orderBy + limit, actualTableName + indexedWhereClause + orderBy + limit, LOG);
            checkCoveredValueScan(compiler, actualTableName, symbolColumnName);
            checkCoveredNullScanOverInterval(compiler, actualTableName, symbolColumnName, tsColumnName, rnd, recordCount);
        }
    }

    /**
     * The timestamp of a random row, printed the way SQL parses it back, or null when the row is
     * no longer there.
     */
    private String randomRowTimestamp(
            SqlCompiler compiler,
            String tableName,
            String tsColumnName,
            Rnd rnd,
            long recordCount
    ) throws SqlException {
        final long row = rnd.nextLong(recordCount);
        final StringSink ts = new StringSink();
        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                "select \"" + tsColumnName + "\" a from " + tableName + " limit " + row + ", " + (row + 1),
                ts
        );
        final String prefix = "a\n";
        if (ts.length() < prefix.length() + 11) {
            return null;
        }
        return ts.subSequence(prefix.length(), ts.length() - 1).toString();
    }

    // a CONVERT PARTITION, with the WAL apply, conversions and compactions of the drain after it, keeps every row
    private void checkFormatChange(String tableName, Runnable apply) {
        final String sql = "SELECT * FROM " + tableName;
        final String before = printToString(sql);
        apply.run();
        final String after = printToString(sql);
        checkedFormatChanges.incrementAndGet();
        LOG.info().$("checked CONVERT PARTITION [table=").$(tableName).$(", isChanged=").$(!before.equals(after)).I$();
        if (!before.equals(after)) {
            Assert.fail("CONVERT PARTITION changed the rows of " + tableName + ", " + firstDifference(before, after));
        }
    }

    /**
     * A change of a column without a NULL sentinel to a type with NULL reads every row as native ALTER COLUMN TYPE
     * converts the value it held, see {@link #convertNoSentinelValues}, but a 0 or false may read NULL instead: the
     * row can be a column top, which the source type reads as 0 or false. Any other column type change runs unchecked.
     */
    private void checkNoSentinelWidening(FuzzChangeColumnTypeOperation typeChange, String tableName, Runnable apply) {
        final int columnType;
        final String columnName;
        final String timestampColumnName;
        try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName(tableName))) {
            final int columnIndex = metadata.getColumnIndexQuiet(typeChange.getColumnName());
            columnType = columnIndex < 0 ? ColumnType.UNDEFINED : metadata.getColumnType(columnIndex);
            columnName = columnIndex < 0 ? null : metadata.getColumnName(columnIndex);
            timestampColumnName = metadata.getColumnName(metadata.getTimestampIndex());
        }
        final int newColumnType = typeChange.getNewColumnType();
        if (columnName == null || !FuzzChangeColumnTypeOperation.isNoSentinelWidening(columnType, newColumnType)) {
            apply.run();
            return;
        }
        final boolean isOnParquet = !"count\n0\n".equals(printToString("SELECT count() FROM table_partitions('" + tableName + "') WHERE isParquet"));
        final String sql = "SELECT \"" + columnName + "\" FROM " + tableName;
        final String[] before = printToString(sql).split("\n", -1);
        apply.run();
        final String[] after = printToString(sql).split("\n", -1);
        final Map<String, String> conversions = convertNoSentinelValues(columnType, newColumnType);
        final String zero = columnType == ColumnType.BOOLEAN ? "false" : "0";
        final String nullText = nullTextOf(newColumnType);
        final String change = columnName + " " + ColumnType.nameOf(columnType) + " -> " + ColumnType.nameOf(newColumnType)
                + " of " + tableName + (isOnParquet ? " with a parquet partition" : "");
        Assert.assertEquals("rows after the change of " + change, before.length, after.length);
        // the header line comes first and an empty line after the last row
        for (int row = 1, n = before.length - 1; row < n; row++) {
            final String expected = conversions.get(before[row]);
            Assert.assertNotNull("no conversion of " + before[row] + " for the change of " + change, expected);
            if (!after[row].equals(expected) && !(before[row].equals(zero) && after[row].equals(nullText))) {
                Assert.fail("the change of " + change + " read row " + (row - 1) + ", which held " + before[row]
                        + ", as [" + after[row] + "] instead of [" + expected + "]");
            }
        }
        checkedNoSentinelWidenings.incrementAndGet();
        if (isOnParquet) {
            checkedNoSentinelWideningsOnParquet.incrementAndGet();
        }
        final int nonLeadingNullDays = countNonLeadingNullDays(tableName, timestampColumnName, columnName);
        if (nonLeadingNullDays > 0) {
            checkedNoSentinelWideningsOverNonLeadingNulls.incrementAndGet();
            if (FuzzChangeColumnTypeOperation.isDecodedAtSourceWidth(columnType, newColumnType)) {
                checkedSourceWidthWideningsOverNonLeadingNulls.incrementAndGet();
            }
        }
        LOG.info().$("checked widening [change=").$(change)
                .$(", rows=").$(before.length - 2)
                .$(", nonLeadingNullDays=").$(nonLeadingNullDays)
                .I$();
    }

    private void checkNoSuspendedTables(int tableId, TableToken tableName, long lastTxn) {
        Assert.assertFalse(tableName.getTableName(), engine.getTableSequencerAPI().isSuspended(tableName));
    }

    private void checkNoSuspendedTables(ObjHashSet<TableToken> tableTokenBucket) {
        engine.getTableSequencerAPI().forAllWalTables(tableTokenBucket, false, checkNoSuspendedTablesRef);
    }

    /**
     * The columns every fuzz table gets after its CREATE, a few of them with tops, and sym2's covering index when
     * {@code sym2Include} is set: a covering index can only be added by ALTER.
     */
    private void addInitialColumns(String tableName, Rnd schemaRnd, String sym2IndexType, String sym2Include) throws SqlException {
        if (sym2Include != null) {
            engine.execute("alter table " + tableName + " alter column sym2 add index " + sym2IndexType + sym2Include, sqlExecutionContext);
        }
        // force few column tops
        engine.execute("alter table " + tableName + " add column long_top long", sqlExecutionContext);
        engine.execute("alter table " + tableName + " add column str_top long", sqlExecutionContext);
        final String symTopIndexType = randomIndexTypeClause(schemaRnd);
        final String symTopInclude = randomIncludeClause(schemaRnd, symTopIndexType, SYM_TOP_INCLUDE_CANDIDATES);
        if (symTopInclude == null) {
            engine.execute("alter table " + tableName + " add column sym_top symbol index " + symTopIndexType, sqlExecutionContext);
        } else {
            engine.execute("alter table " + tableName + " add column sym_top symbol", sqlExecutionContext);
            engine.execute("alter table " + tableName + " alter column sym_top add index " + symTopIndexType + symTopInclude, sqlExecutionContext);
        }
        engine.execute("alter table " + tableName + " add column ip4 ipv4", sqlExecutionContext);
        engine.execute("alter table " + tableName + " add column var_top varchar", sqlExecutionContext);
    }

    // A reader must return the same rows for as long as it holds its txn. Takes a snapshot when the reader has none.
    private void checkReaderSnapshot(TableReader reader) {
        final ReaderSnapshot snapshot = readerSnapshots.get(reader);
        if (snapshot == null || snapshot.txn() != reader.getTxn() || !snapshot.dirName().equals(reader.getTableToken().getDirName())) {
            snapshotReader(reader);
            return;
        }
        final String rows = printReaderRows(reader, snapshot.rowIds());
        if (!snapshot.rows().equals(rows)) {
            Assert.fail("reader rows changed while the reader held its txn [table=" + reader.getTableToken()
                    + ", txn=" + reader.getTxn() + "]\nexpected:\n" + snapshot.rows() + "\nactual:\n" + rows);
        }
    }

    // The sum of the seqTxns the WAL tables have committed, WAL lag excluded.
    private long committedWalTxnCount(ObjHashSet<TableToken> tableTokenBucket) {
        tableTokenBucket.clear();
        engine.getTableTokens(tableTokenBucket, false);
        long committed = 0;
        for (int i = 0, n = tableTokenBucket.size(); i < n; i++) {
            final TableToken tableToken = tableTokenBucket.get(i);
            if (tableToken.isWal()) {
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTrackerIfExists(tableToken);
                if (tracker != null) {
                    committed += tracker.getWriterTxn();
                }
            }
        }
        return committed;
    }

    /**
     * The SQL that reads {@code tableName} for a comparison with its twin: the columns of {@code metadataTableName}, a
     * widened column with NULL for a converted 0 or false, which the twin and the WAL tables can each hold as a column
     * top, see {@link #setNoSentinelWideningChecked}. Without a widened column, the name of the table.
     */
    private String comparedSql(String tableName, String metadataTableName) {
        if (widenedColumns.size() == 0) {
            return tableName;
        }
        try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName(metadataTableName))) {
            final StringBuilder sql = new StringBuilder("SELECT ");
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                final String name = metadata.getColumnName(i);
                if (isWidenedColumn(name)) {
                    sql.append("CASE WHEN \"").append(name).append("\"::VARCHAR IN ('0', 'false', '0.0', ")
                            .append("'1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.000000Z', '1970-01-01T00:00:00.000000000Z')")
                            .append(" THEN NULL ELSE \"").append(name).append("\" END \"").append(name).append('"');
                } else {
                    sql.append('"').append(name).append('"');
                }
            }
            return sql.append(" FROM ").append(tableName).toString();
        }
    }

    /**
     * The text of every value of {@code columnType}, a type without a NULL sentinel, mapped to its text after a native
     * ALTER COLUMN TYPE to {@code newColumnType} of a table that stores the values, without a column top.
     */
    private Map<String, String> convertNoSentinelValues(int columnType, int newColumnType) {
        final String key = ColumnType.nameOf(columnType) + " -> " + ColumnType.nameOf(newColumnType);
        Map<String, String> conversions = noSentinelConversions.get(key);
        if (conversions == null) {
            final String values = switch (columnType) {
                case ColumnType.BOOLEAN -> "SELECT x = 1 FROM long_sequence(2)";
                case ColumnType.BYTE -> "SELECT (x - 129)::BYTE FROM long_sequence(256)";
                default -> "SELECT (x - 32_769)::SHORT FROM long_sequence(65_536)";
            };
            try {
                engine.execute("DROP TABLE IF EXISTS " + NO_SENTINEL_VALUES_TABLE, sqlExecutionContext);
                engine.execute("CREATE TABLE " + NO_SENTINEL_VALUES_TABLE + " (v " + ColumnType.nameOf(columnType) + ")", sqlExecutionContext);
                engine.execute("INSERT INTO " + NO_SENTINEL_VALUES_TABLE + " " + values, sqlExecutionContext);
                final String[] sources = printToString("SELECT v FROM " + NO_SENTINEL_VALUES_TABLE).split("\n", -1);
                engine.execute("ALTER TABLE " + NO_SENTINEL_VALUES_TABLE + " ALTER COLUMN v TYPE " + ColumnType.nameOf(newColumnType), sqlExecutionContext);
                final String[] targets = printToString("SELECT v FROM " + NO_SENTINEL_VALUES_TABLE).split("\n", -1);
                engine.execute("DROP TABLE " + NO_SENTINEL_VALUES_TABLE, sqlExecutionContext);
                Assert.assertEquals(key, sources.length, targets.length);
                conversions = new HashMap<>();
                for (int i = 1, n = sources.length - 1; i < n; i++) {
                    conversions.put(sources[i], targets[i]);
                }
            } catch (SqlException e) {
                throw new AssertionError("could not convert the values of " + key, e);
            }
            noSentinelConversions.put(key, conversions);
        }
        return conversions;
    }

    // the days of tableName in which a NULL of the column comes after a value
    private int countNonLeadingNullDays(String tableName, String timestampColumnName, String columnName) {
        final String[] rows = printToString(
                "SELECT \"" + timestampColumnName + "\"::DATE::VARCHAR, \"" + columnName + "\" IS NULL FROM " + tableName
        ).split("\n", -1);
        int nonLeadingNullDays = 0;
        String day = null;
        boolean isValueSeen = false;
        boolean isDayCounted = false;
        // the header line comes first and an empty line after the last row
        for (int row = 1, n = rows.length - 1; row < n; row++) {
            final String[] cells = rows[row].split("\t", -1);
            // the date part of the DATE text
            final String rowDay = cells[0].substring(0, 10);
            if (!rowDay.equals(day)) {
                day = rowDay;
                isValueSeen = false;
                isDayCounted = false;
            }
            if ("false".equals(cells[1])) {
                isValueSeen = true;
            } else if (isValueSeen && !isDayCounted) {
                nonLeadingNullDays++;
                isDayCounted = true;
            }
        }
        return nonLeadingNullDays;
    }

    private TableToken createInitialTable(String tableName, ObjList<FuzzTransaction> transactions) throws SqlException {
        SharedRandom.RANDOM.set(newApplyRnd());
        final Rnd schemaRnd = newSchemaRnd();

        if (engine.getTableTokenIfExists(tableName) == null) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            final StringSink rowFilter = new StringSink();
            if (transactions != null) {
                LongList noCommitRanges = calculateReplaceRanges(transactions);

                if (noCommitRanges != null) {
                    rowFilter.put(" where");
                    for (int i = 0, n = noCommitRanges.size(); i < n; i += 2) {
                        long lo = noCommitRanges.getQuick(i);
                        long hi = noCommitRanges.getQuick(i + 1);
                        if (i > 0) {
                            rowFilter.put(" and");
                        }
                        rowFilter.put(" (ts not between '" + timestampToString(driver, lo) + "' and '" + timestampToString(driver, hi) + "')");
                    }
                }
            }

            final String sym2IndexType = randomIndexTypeClause(schemaRnd);
            final String sym2Include = randomIncludeClause(schemaRnd, sym2IndexType, SYM2_INCLUDE_CANDIDATES);
            final String dataTempTableName = getDataTempTableName(timestampType);
            if (txnZeroCoveringIndexTypeName != null) {
                createTxnZeroCoveredTable(tableName, dataTempTableName, rowFilter, "BYPASS WAL", false, sym2IndexType, sym2Include);
            } else {
                sink.clear();
                sink.put("create atomic table ")
                        .put(tableName)
                        .put(" as (")
                        .put("select c1, c2, ts, sym2, c3, c4, c5, rnd_str, bool1 from ")
                        .put(dataTempTableName)
                        .put(rowFilter)
                        .put(")");
                if (sym2Include == null) {
                    sink.put(", index(sym2 ").put(sym2IndexType).put(")");
                }
                sink.put(" timestamp(ts) partition by DAY BYPASS WAL");
                engine.execute(sink, sqlExecutionContext);
            }
            addInitialColumns(tableName, schemaRnd, sym2IndexType, sym2Include);
        }
        return engine.verifyTableName(tableName);
    }

    private TableToken createInitialTable(String tableName, boolean isWal, int rowCount) throws SqlException {
        createInitialTable(tableName, isWal, rowCount, timestampType);
        return engine.verifyTableName(tableName);
    }

    private void createInitialTable(String tableName, boolean isWal, int rowCount, int timestampType) throws SqlException {
        SharedRandom.RANDOM.set(newApplyRnd());
        final Rnd schemaRnd = newSchemaRnd();
        final String dataTempTableName = getDataTempTableName(timestampType);
        TableToken tempTable = engine.getTableTokenIfExists(dataTempTableName);

        if (tempTable == null) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            // timestamp_sequence() always returns TIMESTAMP, nanos need timestamp_sequence_ns()
            final String tsSequence = timestampType == ColumnType.TIMESTAMP_NANO ? "timestamp_sequence_ns" : "timestamp_sequence";
            engine.execute(
                    "create atomic table " + dataTempTableName + " as (" +
                            "select x as c1, " +
                            " rnd_symbol('AB', 'BC', 'CD') c2, " +
                            " " + tsSequence + "('2022-02-24'::" + ColumnType.nameOf(timestampType) + ", " + driver.fromSeconds(1) + "L) ts, " +
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
            String tail;
            if (isWal) {
                tail = createWalAsParquet ? "FORMAT PARQUET WAL" : "WAL";
            } else {
                tail = "BYPASS WAL";
            }
            final String sym2IndexType = randomIndexTypeClause(schemaRnd);
            final String sym2Include = randomIncludeClause(schemaRnd, sym2IndexType, SYM2_INCLUDE_CANDIDATES);
            if (txnZeroCoveringIndexTypeName != null) {
                createTxnZeroCoveredTable(tableName, dataTempTableName, "", tail, isWal, sym2IndexType, sym2Include);
            } else {
                engine.execute(
                        "create atomic table " + tableName + " as (" +
                                "select * from " + dataTempTableName +
                                ")" + (sym2Include == null ? ", index(sym2 " + sym2IndexType + ")" : "") + " timestamp(ts) partition by DAY " + tail,
                        sqlExecutionContext
                );
            }
            addInitialColumns(tableName, schemaRnd, sym2IndexType, sym2Include);
        }
    }

    @NotNull
    private ObjList<FuzzTransaction> createTransactions(Rnd rnd, String tableNameBase) throws SqlException, NumericException {
        createInitialTableWal(tableNameBase, initialRowCount);

        final String tableNameNoWal = tableNameBase + "_nonwal";
        final ObjList<FuzzTransaction> transactions = generateTransactions(tableNameBase, rnd);
        createInitialTableNonWal(tableNameNoWal, transactions);

        applyNonWal(transactions, tableNameNoWal, rnd);

        // Release TW to reduce memory pressure
        engine.releaseInactive();

        return transactions;
    }

    /**
     * Creates the shape of dataTempTableName, data_temp or data_temp_ns, with {@value #TXN_ZERO_SYMBOL_COLUMN}
     * added to the still-empty table, covers it with {@link FuzzAddCoveringIndexOperation}, then inserts the rows
     * of dataTempTableName that pass rowFilter. See {@link #setTxnZeroCoveredSymbol(Rnd)}.
     */
    private void createTxnZeroCoveredTable(
            String tableName,
            String dataTempTableName,
            CharSequence rowFilter,
            String tail,
            boolean isWal,
            String sym2IndexType,
            String sym2Include
    ) throws SqlException {
        // with sym2Include, addInitialColumns() adds the covering index of sym2 after the rows
        engine.execute(
                "create atomic table " + tableName + " as (select c1, c2, ts, sym2, c3, c4, c5, rnd_str, bool1 from " + dataTempTableName + " where c1 < 0)"
                        + (sym2Include == null ? ", index(sym2 " + sym2IndexType + ")" : "") + " timestamp(ts) partition by DAY " + tail,
                sqlExecutionContext
        );
        engine.execute("alter table " + tableName + " add column " + TXN_ZERO_SYMBOL_COLUMN + " symbol", sqlExecutionContext);
        if (isWal) {
            drainWalQueue(new Rnd(), tableName);
        }
        try (TableReader reader = getReader(tableName)) {
            final int columnIndex = reader.getMetadata().getColumnIndex(TXN_ZERO_SYMBOL_COLUMN);
            Assert.assertEquals(
                    "column name txn of " + TXN_ZERO_SYMBOL_COLUMN + " in " + tableName,
                    0,
                    reader.getColumnVersionReader().getDefaultColumnNameTxn(reader.getMetadata().getWriterIndex(columnIndex))
            );
            try (TableWriterAPI writer = engine.getTableWriterAPI(reader.getTableToken(), "fuzz txn 0 covering index")) {
                new FuzzAddCoveringIndexOperation(columnIndex, txnZeroCoveredIncludeIndices, txnZeroCoveringIndexTypeName)
                        .apply(new Rnd(), engine, writer, -1, null);
            }
        }
        if (isWal) {
            drainWalQueue(new Rnd(), tableName);
        }
        engine.execute(
                "insert into " + tableName + " select c1, c2, ts, sym2, c3, c4, c5, rnd_str, bool1,"
                        + " case when c1 % 4 = 0 then null else c2 end from " + dataTempTableName + rowFilter,
                sqlExecutionContext
        );
        if (isWal) {
            drainWalQueue(new Rnd(), tableName);
        }
        try (TableReader reader = getReader(tableName)) {
            final TableReaderMetadata metadata = reader.getMetadata();
            final int columnIndex = metadata.getColumnIndex(TXN_ZERO_SYMBOL_COLUMN);
            final IntList coveringIndices = metadata.getCoveringColumnIndices(columnIndex);
            Assert.assertTrue(
                    "covering index on " + TXN_ZERO_SYMBOL_COLUMN + " in " + tableName,
                    metadata.isColumnIndexed(columnIndex) && coveringIndices != null && coveringIndices.size() > 0
            );
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                Assert.assertEquals(
                        "column name txn of " + TXN_ZERO_SYMBOL_COLUMN + " in " + tableName + ", partition " + i,
                        0,
                        reader.getColumnVersionReader().getColumnNameTxn(reader.getPartitionTimestampByIndex(i), metadata.getWriterIndex(columnIndex))
                );
            }
        }
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

        final Rnd tempRnd = newApplyRnd();
        return new Thread(() -> {
            int opIndex;

            try {
                String updatedTableName = tableName;
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

                    walWriter.goActive(transaction.structureVersion);
                    if (walWriter.getMetadataVersion() != transaction.structureVersion) {
                        throw CairoException.critical(0)
                                .put("cannot update wal writer to correct structure version");
                    }

                    boolean increment = false;

                    // Writers of every table share one list, and a writer's token keeps the name it had when it was
                    // last opened or activated, so after a table rename another table's writer can carry this table's
                    // name. The dir name is unique to the table, so reopen only the writers that share it.
                    final String writerDirName = walWriter.getTableToken().getDirName();
                    if (transaction.reopenTable) {
                        TableToken updatedTableToken = engine.getTableTokenByDirName(writerDirName);
                        if (updatedTableToken == null) {
                            throw new IllegalStateException("table is missing after reopen [table=" + tableName + "]");
                        }
                        updatedTableName = updatedTableToken.getTableName();
                    }

                    for (int operationIndex = 0; operationIndex < transaction.operationList.size(); operationIndex++) {
                        FuzzTransactionOperation operation = transaction.operationList.getQuick(operationIndex);
                        increment |= operation.apply(tempRnd, engine, walWriter, -1, null);
                    }

                    if (transaction.reopenTable) {
                        synchronized (writers) {
                            for (int ii = 0; ii < writers.size(); ii++) {
                                if (writers.get(ii).getTableToken().getDirName().equals(writerDirName)) {
                                    writers.get(ii).close();
                                    writers.setQuick(ii, (WalWriter) engine.getTableWriterAPI(updatedTableName, "apply trans test"));
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
                e.printStackTrace(System.out);
                errors.add(e);
                // Unblock peers waiting on a barrier this worker was supposed to advance.
                // Without this, peers spin in the Os.sleep loop above because their wait
                // condition (waitBarrierVersion < target) stays true; the errors check
                // inside the loop relies on Os.sleep returning, which under heavy load
                // can be delayed indefinitely. Overshooting to MAX guarantees the wait
                // condition flips so peers reach the errors.isEmpty() check and return.
                waitBarrierVersion.set(Long.MAX_VALUE);
            } finally {
                Path.clearThreadLocals();
            }
        });
    }

    private void drainWalQueue(Rnd applyRnd, String tableName) {
        // the drain loop runs a timing-dependent number of times: a copy keeps its draws off applyRnd,
        // so that the seeded draws after the drain replay from the printed seeds
        final Rnd drainRnd = new Rnd(applyRnd.getSeed0(), applyRnd.getSeed1());
        try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0);
             O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1);
             ColumnPurgeJob columnPurgeJob = openColumnPurgeJob();
             PartitionCompactionScanJob compactionJob = new PartitionCompactionScanJob(engine);
             TableReader rdr1 = getReaderWithRetries(tableName, null);
             TableReader rdr2 = getReaderWithRetries(tableName, null)
        ) {
            snapshotReader(rdr1);
            snapshotReader(rdr2);
            CheckWalTransactionsJob checkWalTransactionsJob = new CheckWalTransactionsJob(engine);
            final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
            long committedAtRelease = -1;
            while (true) {
                if (parquetConversionRequestProb > 0 && drainRnd.nextDouble() < parquetConversionRequestProb) {
                    requestParquetConversionDuringApply(drainRnd, tableName, walApplyJob, compactionJob, rdr1, rdr2);
                }
                if (!walApplyJob.run() && !checkWalTransactionsJob.run()) {
                    break;
                }
                committedAtRelease = forceReleaseTableWriter(drainRnd, tableTokenBucket, committedAtRelease);
                purgeAndReloadReaders(drainRnd, rdr1, rdr2, purgeJob, columnPurgeJob, 0.25);
            }
            // This path is the ONLY compaction coverage for single-writer tables driven through
            // applyWal() -> applyToWal() (walWriterCount=1 never goes through
            // applyManyWalParallel()'s waitApply thread pool below), so it needs its own sweep,
            // not just the threaded one.
            compactionJob.drain(0);
            runColumnPurgeJob(columnPurgeJob);
        }
    }

    /**
     * Runs the {@link FuzzDropIndexOperation} of the transactions, which change the index once the WAL
     * drained, and returns whether there was any. {@link FuzzAddCoveringIndexOperation} applies in order
     * with the other operations instead.
     */
    private boolean executeDropIndexOps(ObjList<FuzzTransaction> transactions, String tableName, Rnd applyRnd) {
        boolean hasDropIndexOps = false;
        for (int i = 0, n = transactions.size(); i < n; i++) {
            ObjList<FuzzTransactionOperation> ops = transactions.getQuick(i).operationList;
            for (int j = 0, m = ops.size(); j < m; j++) {
                if (ops.getQuick(j) instanceof FuzzDropIndexOperation dropIndexOp) {
                    // the drop op skips the columns that other indexes cover, so it must read the
                    // applied metadata of the index changes queued before it
                    drainWalQueue(applyRnd, tableName);
                    dropIndexOp.executePostDrain(engine, tableName);
                    hasDropIndexOps = true;
                }
            }
        }
        return hasDropIndexOps;
    }

    // the CONVERT PARTITION or column type change of the transaction, which applyWalChecked() checks, or null
    private @Nullable FuzzTransactionOperation findCheckedOperation(FuzzTransaction transaction) {
        for (int i = 0, n = transaction.operationList.size(); i < n; i++) {
            final FuzzTransactionOperation operation = transaction.operationList.getQuick(i);
            if (operation instanceof FuzzConvertPartitionToParquetOperation
                    || operation instanceof FuzzConvertPartitionToNativeOperation
                    || operation instanceof FuzzChangeColumnTypeOperation) {
                return operation;
            }
        }
        return null;
    }

    // Sometimes WAL Apply Job does not finish table in one go and return TableWriter to the pool
    // where it can be fully closed before continuing the WAL application Test TableWriter closures.
    // A reopened writer applies its WAL lag again from the WAL, so the writers are closed only once
    // the apply has committed something since the last close. Closing them after every run would
    // discard a lag that takes several runs to commit (one transaction per run under a small apply
    // time quota) as often as it is rebuilt, and the apply would never finish. Returns the committed
    // count at the last close.
    private long forceReleaseTableWriter(Rnd applyRnd, ObjHashSet<TableToken> tableTokenBucket, long committedAtRelease) {
        if (applyRnd.nextDouble() < 0.8) {
            final long committed = committedWalTxnCount(tableTokenBucket);
            if (committed != committedAtRelease) {
                engine.releaseInactive();
                return committed;
            }
        }
        return committedAtRelease;
    }

    private TableReader getReader(String tableName) {
        return engine.getReader(engine.verifyTableName(tableName));
    }

    // Concurrent-read reader open: on top of the read-timeout retry, also waits out the table being
    // concurrently dropped and recreated (dropped / name reserved / entry locked). Returns null when
    // isCancelled reports that the run is over while the open still waits for the recreate.
    private @Nullable TableReader getReaderWithRetries(String tableName, @Nullable BooleanSupplier isCancelled) {
        return openWithRetries(() -> getReader(tableName), true, isCancelled, OPEN_RETRY_TIMEOUT_MILLIS);
    }

    private void initRandomParams(Rnd rnd, Log log) {
        timestampType = rnd.nextInt(100) < NANOS_TIMESTAMP_PERCENT
                ? ColumnType.TIMESTAMP_NANO
                : ColumnType.TIMESTAMP_MICRO;
        applySeed0 = rnd.nextLong();
        applySeed1 = rnd.nextLong();
        hasApplySeeds = true;
        log.info().$("fuzz designated timestamp type: ").$(ColumnType.nameOf(timestampType)).$();
    }

    // Every apply path (non-WAL, WAL, parallel WAL threads) gets an Rnd with the same seeds,
    // so apply-time random choices match across the tables being compared.
    // The seeds come from the test Rnd, see generateRandom().
    private boolean isWidenedColumn(String columnName) {
        for (int i = 0, n = widenedColumns.size(); i < n; i++) {
            if (Chars.equalsIgnoreCase(widenedColumns.get(i), columnName)) {
                return true;
            }
        }
        return false;
    }

    private Rnd newApplyRnd() {
        if (!hasApplySeeds) {
            // The test did not call generateRandom(), draw the seeds from a fresh random
            final Rnd rnd = TestUtils.generateRandom(LOG);
            applySeed0 = rnd.nextLong();
            applySeed1 = rnd.nextLong();
            hasApplySeeds = true;
        }
        return new Rnd(applySeed0, applySeed1);
    }

    /**
     * Opens the column purge job of an apply, or returns null when another job drains the queue, which takes one
     * consumer: the job of a worker pool, see {@link #setHasPoolColumnPurgeJob}, or a job that holds the purge log.
     */
    private @Nullable ColumnPurgeJob openColumnPurgeJob() {
        if (hasPoolColumnPurgeJob) {
            return null;
        }
        try {
            return new ColumnPurgeJob(engine);
        } catch (EntryUnavailableException e) {
            return null;
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
    }

    private String printReaderRows(TableReader reader, LongList rowIds) {
        final StringSink rows = new StringSink();
        final TestTableReaderRecord record = new TestTableReaderRecord();
        record.of(reader);
        for (int i = 0, n = rowIds.size(); i < n; i++) {
            final long rowId = rowIds.getQuick(i);
            final int partitionIndex = Rows.toPartitionIndex(rowId);
            final long row = Rows.toLocalRowID(rowId);
            if (row == ROW_ID_PARTITION_SIZE) {
                rows.put("partition ").put(partitionIndex).put(" size ").put(reader.openPartition(partitionIndex)).put('\n');
                continue;
            }
            record.jumpTo(partitionIndex, row);
            rows.put(partitionIndex).put(':').put(row).put('\t');
            CursorPrinter.println(record, reader.getMetadata(), rows);
        }
        return rows.toString();
    }

    private String printToString(CharSequence sql) {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final StringSink text = new StringSink();
            TestUtils.printSql(compiler, sqlExecutionContext, sql, text);
            return text.toString();
        } catch (SqlException e) {
            throw new AssertionError("could not print " + sql, e);
        }
    }

    /**
     * The schema choices of a run's tables. Every table of a run takes the same seed, so the expected and the actual
     * table get the same indexes, and the seed follows the run's own, so the choices vary between runs. Kept apart
     * from {@link SharedRandom}, which the rnd functions creating data_temp draw from only on the first table.
     */
    private Rnd newSchemaRnd() {
        return s0 == 0 && s1 == 0 ? new Rnd() : new Rnd(s0, s1);
    }

    private void reloadPartitions(TableReader rdr1) {
        if (rdr1.isActive()) {
            LOG.info().$("reloading partitions [table=").$(rdr1.getTableToken()).$(", txn=").$(rdr1.getTxn()).I$();
            for (int i = 0; i < rdr1.getPartitionCount(); i++) {
                rdr1.openPartition(i);
            }
        }
    }

    private void reloadReader(Rnd reloadRnd, @Nullable TableReader reader, CharSequence rdrId) {
        if (reader != null && reader.isActive()) {
            checkReaderSnapshot(reader);
        }
        if (reloadRnd.nextBoolean()) {
            if (reader != null && reader.isActive()) {
                reloadPartitions(reader);
                LOG.info().$("releasing reader txn [rdr=").$(rdrId).$(", table=").$(reader.getTableToken()).$(", txn=").$(reader.getTxn()).I$();
                readerSnapshots.remove(reader);
                reader.goPassive();
            }

            if (reloadRnd.nextBoolean() && reader != null && !reader.isActive()) {
                reader.goActive();
                LOG.info().$("acquired reader txn [rdr=").$(rdrId).$(", table=").$(reader.getTableToken()).$(", txn=").$(reader.getTxn()).I$();
                snapshotReader(reader);
            }
        }
    }

    /**
     * Asks the compaction job to convert a random partition of {@code tableName} to parquet, and runs WAL apply in
     * the window between the conversion's build, off a reader snapshot, and its swap: the moment the build's reader
     * goes back to the pool, a pool listener applies up to three WAL apply steps before the job publishes the swap.
     * The swap must then refuse a partition the apply changed, or it installs data the apply has overwritten - a
     * DEDUP upsert whose O3 split was squashed back in place keeps the name txn and the row count. Half the time
     * the drain's readers are released first, since a reader pinning the partition keeps the squash off it, and
     * half the time the partition is the one {@link #setParquetConversionRequestHint} names.
     */
    private void requestParquetConversionDuringApply(
            Rnd applyRnd,
            String tableName,
            ApplyWal2TableJob walApplyJob,
            PartitionCompactionScanJob compactionJob,
            TableReader rdr1,
            TableReader rdr2
    ) {
        final TableToken tableToken = engine.verifyTableName(tableName);
        final long logicalPartitionTimestamp;
        try (TableReader reader = getReaderWithRetries(tableName, null)) {
            assert reader != null;
            final TxReader txFile = reader.getTxFile();
            final int partitionCount = txFile.getPartitionCount();
            if (partitionCount < 2) {
                return;
            }
            // The last partition is active, and the job never converts it.
            final long partitionTimestamp = txFile.getPartitionTimestampByIndex(applyRnd.nextInt(partitionCount - 1));
            if (parquetConversionRequestHint != Long.MIN_VALUE && applyRnd.nextBoolean()) {
                logicalPartitionTimestamp = txFile.getLogicalPartitionTimestamp(parquetConversionRequestHint);
            } else {
                logicalPartitionTimestamp = txFile.getLogicalPartitionTimestamp(partitionTimestamp);
            }
        }
        final int applySteps = 1 + applyRnd.nextInt(3);
        if (applyRnd.nextBoolean()) {
            rdr1.goPassive();
            rdr2.goPassive();
        }
        if (!engine.getPartitionCompactionRequestQueue().tryRequestParquetConversion(tableToken, logicalPartitionTimestamp)) {
            return;
        }
        LOG.info().$("fuzz parquet conversion request during WAL apply [table=").$(tableToken)
                .$(", logicalPartitionTimestamp=").$(logicalPartitionTimestamp)
                .$(", applySteps=").$(applySteps)
                .I$();

        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final AtomicBoolean isApplied = new AtomicBoolean();
        final Thread compactor = new Thread(() -> {
            try {
                compactionJob.run();
            } catch (Throwable th) {
                errors.add(th);
            } finally {
                Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                Path.clearThreadLocals();
            }
        }, "fuzz-parquet-conversion");
        final long compactorThreadId = compactor.threadId();
        final PoolListener previousListener = engine.getPoolListener();
        engine.setPoolListener((factoryType, thread, token, event, segment, position) -> {
            if (previousListener != null) {
                previousListener.onEvent(factoryType, thread, token, event, segment, position);
            }
            if (factoryType == PoolListener.SRC_READER
                    && event == PoolListener.EV_RETURN
                    && thread == compactorThreadId
                    && tableToken.equals(token)
                    && isApplied.compareAndSet(false, true)) {
                // The build's reader just went passive and the swap is not published yet.
                final Thread applier = new Thread(() -> {
                    try {
                        for (int i = 0; i < applySteps; i++) {
                            walApplyJob.run();
                        }
                    } catch (Throwable th) {
                        errors.add(th);
                    } finally {
                        Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                        Path.clearThreadLocals();
                    }
                }, "fuzz-parquet-conversion-apply");
                applier.start();
                TestUtils.unchecked(() -> applier.join());
            }
        });
        try {
            compactor.start();
            TestUtils.unchecked(() -> compactor.join());
        } finally {
            engine.setPoolListener(previousListener);
        }
        for (Throwable e : errors) {
            throw new RuntimeException(e);
        }
    }

    private void runApplyThread(AtomicInteger done, ConcurrentLinkedQueue<Throwable> errors, Rnd applyRnd) {
        try {
            ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
            int i = 0;
            long committedAtRelease = -1;
            CheckWalTransactionsJob checkJob = new CheckWalTransactionsJob(engine);
            try (ApplyWal2TableJob job = new ApplyWal2TableJob(engine, 0)) {
                while (done.get() == 0 && errors.isEmpty()) {
                    Unsafe.loadFence();
                    while (job.run() || checkJob.run()) {
                        committedAtRelease = forceReleaseTableWriter(applyRnd, tableTokenBucket, committedAtRelease);
                    }
                    Os.sleep(1);
                    checkNoSuspendedTables(tableTokenBucket);
                    i++;
                }
                while (job.run() || checkJob.run()) {
                    committedAtRelease = forceReleaseTableWriter(applyRnd, tableTokenBucket, committedAtRelease);
                }
                i++;
            }
            LOG.info().$("finished apply thread after iterations: ").$(i).$();
        } catch (Throwable e) {
            errors.add(e);
        } finally {
            Path.clearThreadLocals();
            Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
        }
    }

    /**
     * Runs the column purge job, which purges the column versions that the queue holds and no reader uses. The job
     * swallows the failures of its file operations, so it does not run while a planted failure is pending: the failure
     * belongs to the operation it was planted for.
     */
    private void runColumnPurgeJob(@Nullable ColumnPurgeJob columnPurgeJob) {
        if (columnPurgeJob != null && !ff.isArmed()) {
            columnPurgeJob.run();
        }
    }

    private void runPartitionCompactionJob(AtomicInteger done, ConcurrentLinkedQueue<Throwable> errors) {
        try {
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                while (done.get() == 0 && errors.isEmpty()) {
                    job.drain(0);
                    Os.sleep(1);
                }
            }
        } catch (Throwable e) {
            errors.add(e);
        } finally {
            Path.clearThreadLocals();
            // A parquet rebuild leaves the O3 merge context in a thread local, which a worker pool would free on halt.
            Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
        }
    }

    private void runPurgePartitionJob(AtomicInteger done, AtomicInteger forceReaderReload, ConcurrentLinkedQueue<Throwable> errors, Rnd runRnd, String tableNameBase, int tableCount, boolean multiTable) {
        ObjList<TableReader> readers = new ObjList<>();
        // A reader open waits out a concurrent drop-and-recreate, which never lands if the run ends or
        // another worker fails first; this lets the open give up instead of stranding the thread.
        BooleanSupplier isRunOver = () -> done.get() != 0 || !errors.isEmpty();
        try {
            try (
                    O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1);
                    ColumnPurgeJob columnPurgeJob = openColumnPurgeJob()
            ) {
                int forceReloadNum = forceReaderReload.get();
                for (int i = 0; i < tableCount; i++) {
                    String tableNameWal = multiTable ? getWalParallelApplyTableName(tableNameBase, i) : tableNameBase;
                    for (int j = 0; j < 2; j++) {
                        TableReader reader = getReaderWithRetries(tableNameWal, isRunOver);
                        if (reader == null) {
                            return;
                        }
                        readers.add(reader);
                    }
                }

                while (done.get() == 0 && errors.isEmpty()) {
                    if (forceReaderReload.get() != forceReloadNum) {
                        forceReloadNum = forceReaderReload.get();
                        for (int i = 0; i < readers.size(); i++) {
                            String tableNameWal = multiTable ? getWalParallelApplyTableName(tableNameBase, i / 2) : tableNameBase;
                            readers.setQuick(i, Misc.free(readers.getQuick(i)));
                            TableReader reader = getReaderWithRetries(tableNameWal, isRunOver);
                            if (reader == null) {
                                return;
                            }
                            readers.setQuick(i, reader);
                        }
                    }
                    int reader = runRnd.nextInt(tableCount);
                    purgeAndReloadReaders(runRnd, readers.get(reader * 2), readers.get(reader * 2 + 1), purgeJob, columnPurgeJob, 0.25);
                    Os.sleep(50);
                }
                runColumnPurgeJob(columnPurgeJob);
            }
        } catch (Throwable e) {
            errors.add(e);
        } finally {
            Misc.freeObjList(readers);
            Path.clearThreadLocals();
            Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
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

    // Records each native partition's size, its first row, a sample of rows seeded by the txn, and its tail,
    // where in-place appends write. It skips parquet partitions because the test record reads native columns only.
    private void snapshotReader(@Nullable TableReader reader) {
        if (reader == null || !reader.isActive()) {
            return;
        }
        final LongList rowIds = new LongList();
        final Rnd rnd = new Rnd(reader.getTxn(), reader.getTableToken().getTableId());
        for (int partitionIndex = 0, n = reader.getPartitionCount(); partitionIndex < n; partitionIndex++) {
            final long size = reader.openPartition(partitionIndex);
            if (reader.getPartitionFormat(partitionIndex) == PartitionFormat.PARQUET) {
                continue;
            }
            rowIds.add(Rows.toRowID(partitionIndex, ROW_ID_PARTITION_SIZE));
            final long tailLo = Math.max(0, size - SNAPSHOT_TAIL_ROWS);
            if (tailLo > 0) {
                rowIds.add(Rows.toRowID(partitionIndex, 0));
                for (int i = 0; i < SNAPSHOT_SAMPLE_ROWS; i++) {
                    rowIds.add(Rows.toRowID(partitionIndex, rnd.nextLong(tailLo)));
                }
            }
            for (long row = tailLo; row < size; row++) {
                rowIds.add(Rows.toRowID(partitionIndex, row));
            }
        }
        readerSnapshots.put(reader, new ReaderSnapshot(reader.getTableToken().getDirName(), reader.getTxn(), rowIds, printReaderRows(reader, rowIds)));
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

    <T> T openWithRetries(Supplier<T> open, boolean isTableRecreateTolerated) {
        return openWithRetries(open, isTableRecreateTolerated, null, OPEN_RETRY_TIMEOUT_MILLIS);
    }

    /**
     * Retries a table-resource open past the transients a fuzz run produces.
     * <p>
     * A read timeout (metadata / transaction / column version) means the open sampled the table while a
     * structural change was mid-apply. Retries run against a single deadline, {@code retryTimeoutMillis}
     * from the first attempt, so the budget does not scale with the spin-lock timeout each attempt
     * already spends. With {@code isTableRecreateTolerated} the open also waits out a table that a
     * concurrent thread drops and recreates; the setup and verify callers leave it off, so a dropped /
     * name-reserved / entry-locked table there surfaces as the genuine failure it is.
     *
     * @return the open resource, or null when isCancelled reported the run over while the open waited
     */
    <T> @Nullable T openWithRetries(
            Supplier<T> open,
            boolean isTableRecreateTolerated,
            @Nullable BooleanSupplier isCancelled,
            long retryTimeoutMillis
    ) {
        final long deadlineNanos = System.nanoTime() + retryTimeoutMillis * Nanos.MILLI_NANOS;
        while (true) {
            try {
                return open.get();
            } catch (CairoException e) {
                final CharSequence message = e.getFlyweightMessage();
                final boolean isReadTimeout = Chars.contains(message, "read timeout");
                final boolean isTableRecreate = isTableRecreateTolerated
                        && (Chars.contains(message, "table does not exist")
                        || Chars.contains(message, "table name is reserved")
                        || e instanceof EntryLockedException);
                if (!isReadTimeout && !isTableRecreate) {
                    throw e;
                }
                if (isCancelled != null && isCancelled.getAsBoolean()) {
                    return null;
                }
                if (isReadTimeout) {
                    final long remainingNanos = deadlineNanos - System.nanoTime();
                    if (remainingNanos <= 0) {
                        throw e;
                    }
                    final long backoffMillis = Math.max(1, Math.min(100, remainingNanos / Nanos.MILLI_NANOS));
                    LOG.error().$("read timeout opening table resource, retrying [backoffMillis=").$(backoffMillis)
                            .$(", error=").$(message).I$();
                    Os.sleep(backoffMillis);
                } else {
                    LOG.error().$("table is being recreated, retrying open [error=").$(message).I$();
                    Os.sleep(10);
                }
            }
        }
    }

    protected void assertStringColDensity(String tableNameWal) {
        try (TableReader reader = openWithRetries(() -> getReader(tableNameWal), false)) {
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

        try (TableReader reader = openWithRetries(() -> getReader(baseSymbolTableName), false)) {
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
            assertMinMaxTimestamp(sqlExecutionContext, tableNameNoWal);

            applyWal(transactions, tableNameWal, 1, rnd);

            long endWalMicro = System.nanoTime() / 1000;
            long walTotal = endWalMicro - endNonWalMicro;

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertMinMaxTimestamp(sqlExecutionContext, tableNameWal);

                String limit = ""; // For debugging
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, comparedSql(tableNameNoWal, tableNameWal) + limit, comparedSql(tableNameWal, tableNameWal) + limit, LOG);
                if (widenedColumns.size() == 0) {
                    assertRandomIndexes(tableNameNoWal, tableNameWal, rnd);
                }

                startMicro = System.nanoTime() / 1000;
                applyWalParallel(transactions, tableNameWal2, rnd);
                endWalMicro = System.nanoTime() / 1000;
                long totalWalParallel = endWalMicro - startMicro;

                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, comparedSql(tableNameNoWal, tableNameWal2), comparedSql(tableNameWal2, tableNameWal2), LOG);
                if (widenedColumns.size() == 0) {
                    assertRandomIndexes(tableNameNoWal, tableNameWal2, rnd);
                }
                LOG.infoW().$("=== non-wal(ms): ").$(nonWalTotal / 1000).$(" === wal(ms): ").$(walTotal / 1000).$(" === wal_parallel(ms): ").$(totalWalParallel / 1000).$();

                assertMinMaxTimestamp(sqlExecutionContext, tableNameWal2);
            }

            assertCounts(tableNameWal, timestampColumnName);
            assertCounts(tableNameNoWal, timestampColumnName);
            assertStringColDensity(tableNameWal);
            Assert.assertEquals("expected 0 errors in partition mutation control", 0, engine.getPartitionOverwriteControl().getErrorCount());

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
                            0.0,
                            0.0,
                            0.1 * rnd.nextDouble(),
                            rnd.nextDouble(),
                            0.0,
                            0.05,
                            0.1 * rnd.nextDouble(),
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
            Assert.assertEquals("expected 0 errors in partition mutation control", 0, engine.getPartitionOverwriteControl().getErrorCount());

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (int i = 0; i < tableCount; i++) {
                    String tableNameNoWal = tableNameBase + "_" + i + "_nonwal";
                    String tableNameWal = getWalParallelApplyTableName(tableNameBase, i);
                    LOG.infoW().$("comparing tables ").$(tableNameNoWal).$(" and ").$(tableNameWal).$();
                    String limit = ""; // For debugging
                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal + limit, tableNameWal + limit, LOG);
                    assertRandomIndexes(tableNameNoWal, tableNameWal, rnd);
                    assertMinMaxTimestamp(sqlExecutionContext, tableNameNoWal);
                    assertMinMaxTimestamp(sqlExecutionContext, tableNameWal);
                }
            }
        } finally {
            for (int i = 0, n = fuzzTransactions.size(); i < n; i++) {
                Misc.freeObjListAndClear(fuzzTransactions.get(i));
            }
        }
    }

    private record ReaderSnapshot(String dirName, long txn, LongList rowIds, String rows) {
    }
}
