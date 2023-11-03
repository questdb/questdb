package io.questdb.test.griffin.wal;

import io.questdb.cairo.*;
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
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionGenerator;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class FuzzRunner {
    public final static int MAX_WAL_APPLY_TIME_PER_TABLE_CEIL = 250;
    protected static final Log LOG = LogFactory.getLog(AbstractFuzzTest.class);
    protected static final StringSink sink = new StringSink();
    private final TableSequencerAPI.TableSequencerCallback checkNoSuspendedTablesRef;
    protected int initialRowCount;
    protected int partitionCount;
    private double cancelRowsProb;
    private double colRenameProb;
    private double collAddProb;
    private double collRemoveProb;
    private double dataAddProb;
    private CairoEngine engine;
    private double equalTsRowsProb;
    private int fuzzRowCount;
    private boolean isO3;
    private double notSetProb;
    private double nullSetProb;
    private double rollbackProb;
    private long s0;
    private long s1;
    private SqlExecutionContext sqlExecutionContext;
    private int strLen;
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
                int parallelWalCount = Math.max(2, rnd.nextInt(5));
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

                Thread purgePartitionThread = new Thread(() -> runPurgePartitionJob(done, forceReaderReload, errors, new Rnd(rnd.nextLong(), rnd.nextLong()), tableNameBase, tableCount, multiTable));
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
        try (
                O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine.getMessageBus(), engine.getSnapshotAgent(), 1)
        ) {
            int transactionSize = transactions.size();
            Rnd rnd = new Rnd();
            for (int i = 0; i < transactionSize; i++) {
                FuzzTransaction transaction = transactions.getQuick(i);
                TableWriter writerCopy = writer;
                if (transaction.reopenTable) {
                    rdr1 = Misc.free(rdr1);
                    rdr2 = Misc.free(rdr2);
                    writer = Misc.free(writer);
                }

                int size = transaction.operationList.size();
                for (int operationIndex = 0; operationIndex < size; operationIndex++) {
                    FuzzTransactionOperation operation = transaction.operationList.getQuick(operationIndex);
                    operation.apply(rnd, engine, writerCopy, -1);
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
                purgeAndReloadReaders(reloadRnd, rdr1, rdr2, purgeJob, 0.25);
            }
        } finally {
            Misc.free(rdr1);
            Misc.free(rdr2);
            Misc.free(writer);
        }
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
                operation.apply(tempRnd, engine, writer, -1);
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
                    writer.commit();
                }
            }
        }

        Misc.freeObjList(writers);
        drainWalQueue(applyRnd, tableName);
    }

    public void assertRandomIndexes(String tableNameNoWal, String tableNameWal, Rnd rnd) throws SqlException {
        try (TableReader reader = getReader(tableNameNoWal)) {
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

    @Before
    public void clearSeeds() {
        s0 = 0;
        s1 = 0;
    }

    public TableToken createInitialTable(String tableName, boolean isWal) throws SqlException {
        return createInitialTable(tableName, isWal, initialRowCount);
    }

    public TableToken createInitialTable(String tableName1, boolean isWal, int rowCount) throws SqlException {
        SharedRandom.RANDOM.set(new Rnd());
        if (engine.getTableTokenIfExists(tableName1) == null) {
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
        }
        return engine.verifyTableName(tableName1);
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
                (int) metadata.getMetadataVersion(),
                tableDropProb
        );
    }

    public ObjList<FuzzTransaction> generateTransactions(String tableName, Rnd rnd) throws NumericException {
        long start = IntervalUtils.parseFloorPartialTimestamp("2022-02-24T17");
        long end = start + partitionCount * Timestamps.DAY_MICROS;
        return generateTransactions(tableName, rnd, start, end);
    }

    public ObjList<FuzzTransaction> generateTransactions(String tableName, Rnd rnd, long start, long end) {
        TableToken tableToken = engine.verifyTableName(tableName);
        try (TableMetadata metadata = engine.getMetadata(tableToken)) {
            return generateSet(rnd, metadata, start, end, tableName);
        }
    }

    public void setFuzzCounts(boolean isO3, int fuzzRowCount, int transactionCount, int strLen, int symbolStrLenMax, int symbolCountMax, int initialRowCount, int partitionCount) {
        this.isO3 = isO3;
        this.fuzzRowCount = fuzzRowCount;
        this.transactionCount = transactionCount;
        this.strLen = strLen;
        this.symbolStrLenMax = symbolStrLenMax;
        this.symbolCountMax = symbolCountMax;
        this.initialRowCount = initialRowCount;
        this.partitionCount = partitionCount;
    }

    public void setFuzzProbabilities(double cancelRowsProb, double notSetProb, double nullSetProb, double rollbackProb, double collAddProb, double collRemoveProb, double colRenameProb, double dataAddProb, double truncateProb, double equalTsRowsProb, double tableDropProb) {
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
        this.tableDropProb = tableDropProb;
    }

    public void withDb(CairoEngine engine, SqlExecutionContext sqlExecutionContext) {
        this.engine = engine;
        this.sqlExecutionContext = sqlExecutionContext;
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

    private void applyWalParallel(ObjList<FuzzTransaction> transactions, String tableName, Rnd applyRnd) {
        ObjList<ObjList<FuzzTransaction>> tablesTransactions = new ObjList<>();
        tablesTransactions.add(transactions);
        applyManyWalParallel(tablesTransactions, applyRnd, tableName, false, true);
    }

    private void checkIndexRandomValueScan(String expectedTableName, String actualTableName, Rnd rnd, long recordCount, String columnName) throws SqlException {
        long randomRow = rnd.nextLong(recordCount);
        sink.clear();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.printSql(compiler, sqlExecutionContext, "select \"" + columnName + "\" as a from " + expectedTableName + " limit " + randomRow + ", 1", sink);
            String prefix = "a\n";
            String randomValue = sink.length() > prefix.length() + 2 ? sink.subSequence(prefix.length(), sink.length() - 1).toString() : null;
            String indexedWhereClause = " where \"" + columnName + "\" = " + (randomValue == null ? "null" : "'" + randomValue + "'");
            LOG.info().$("checking random index with filter: ").$(indexedWhereClause).I$();
            String limit = ""; // For debugging
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, expectedTableName + indexedWhereClause + limit, actualTableName + indexedWhereClause + limit, LOG);
        }
    }

    private void checkNoSuspendedTables(int tableId, TableToken tableName, long lastTxn) {
        Assert.assertFalse(tableName.getTableName(), engine.getTableSequencerAPI().isSuspended(tableName));
    }

    private void checkNoSuspendedTables(ObjHashSet<TableToken> tableTokenBucket) {
        engine.getTableSequencerAPI().forAllWalTables(tableTokenBucket, false, checkNoSuspendedTablesRef);
    }

    private void compile(String sql) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.compile(sql, sqlExecutionContext).execute(null).await();
        }
    }

    @NotNull
    private ObjList<FuzzTransaction> createTransactions(Rnd rnd, String tableNameBase) throws SqlException, NumericException {
        String tableNameNoWal = tableNameBase + "_nonwal";

        createInitialTable(tableNameNoWal, false, initialRowCount);
        createInitialTable(tableNameBase, true, initialRowCount);

        ObjList<FuzzTransaction> transactions = generateTransactions(tableNameNoWal, rnd);
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
        writers.add((WalWriter) engine.getTableWriterAPI(tableName, "apply trans test"));
        final int writerIndex = writers.size() - 1;

        return new Thread(() -> {
            int opIndex;

            try {
                Rnd tempRnd = new Rnd();
                while ((opIndex = nextOperation.incrementAndGet()) < transactions.size() && errors.isEmpty()) {
                    FuzzTransaction transaction = transactions.getQuick(opIndex);

                    // wait until structure version, truncate is applied
                    while (waitBarrierVersion.get() < transaction.waitBarrierVersion && errors.isEmpty()) {
                        Os.sleep(1);
                    }

                    if (transaction.waitAllDone) {
                        while (doneCount.get() != opIndex) {
                            Os.sleep(1);
                        }
                    }

                    WalWriter walWriter = writers.get(writerIndex);

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
                        increment |= operation.apply(tempRnd, engine, walWriter, -1);
                    }

                    if (transaction.reopenTable) {
                        // Table is dropped, reload all writers
                        for (int ii = 0; ii < writers.size(); ii++) {
                            if (writers.get(ii).getTableToken().getTableName().equals(tableName)) {
                                writers.get(ii).close();
                                writers.setQuick(ii, (WalWriter) engine.getTableWriterAPI(tableName, "apply trans test"));
                            }
                        }
                        forceReaderReload.incrementAndGet();
                        engine.releaseInactive();
                    } else {
                        if (transaction.rollback) {
                            walWriter.rollback();
                        } else {
                            walWriter.commit();
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
                errors.add(e);
            } finally {
                Path.clearThreadLocals();
            }
        });
    }

    private void drainWalQueue(Rnd applyRnd, String tableName) {
        try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 1, 1);
             O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine.getMessageBus(), engine.getSnapshotAgent(), 1);
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
                        || Chars.contains(e.getFlyweightMessage(), "table name is reserved")) {
                    LOG.error().$((Throwable) e).$();
                    Os.sleep(10);
                } else {
                    throw e;
                }
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
            try (ApplyWal2TableJob job = new ApplyWal2TableJob(engine, 1, 1)) {
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
            try (O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine.getMessageBus(), engine.getSnapshotAgent(), 1)) {
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
                if (ColumnType.isVariableLength(columnType)) {
                    for (int partitionIndex = 0; partitionIndex < reader.getPartitionCount(); partitionIndex++) {
                        reader.openPartition(partitionIndex);
                        int columnBase = reader.getColumnBase(partitionIndex);
                        MemoryR dCol = reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, i));
                        MemoryR iCol = reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, i) + 1);

                        long colTop = reader.getColumnTop(columnBase, i);
                        long rowCount = reader.getPartitionRowCount(partitionIndex) - colTop;
                        if (DebugUtils.isSparseVarCol(rowCount, iCol.getPageAddress(0), dCol.getPageAddress(0), columnType)) {
                            Assert.fail("var column " + reader.getMetadata().getColumnName(i)
                                    + " is not dense, .i file record size is different from .d file record size");
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

        createInitialTable(tableNameWal, true, initialRowCount);
        createInitialTable(tableNameWal2, true, initialRowCount);
        TableToken nonWalTt = createInitialTable(tableNameNoWal, false, initialRowCount);

        ObjList<FuzzTransaction> transactions;
        transactions = generateTransactions(tableNameNoWal, rnd);
        String timestampColumnName;
        try (TableMetadata meta = engine.getMetadata(nonWalTt)) {
            timestampColumnName = meta.getColumnName(meta.getTimestampIndex());
        }

        long startMicro = System.nanoTime() / 1000;
        applyNonWal(transactions, tableNameNoWal, rnd);
        long endNonWalMicro = System.nanoTime() / 1000;
        long nonWalTotal = endNonWalMicro - startMicro;

        applyWal(transactions, tableNameWal, getRndParallelWalCount(rnd), rnd);

        long endWalMicro = System.nanoTime() / 1000;
        long walTotal = endWalMicro - endNonWalMicro;

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
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
        }

        assertCounts(tableNameWal, timestampColumnName);
        assertCounts(tableNameNoWal, timestampColumnName);
        assertStringColDensity(tableNameWal);
    }

    protected void runFuzz(Rnd rnd, String tableNameBase, int tableCount, boolean randomiseProbs, boolean randomiseCounts) throws Exception {
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
                        0.1 * rnd.nextDouble(), 0.01,
                        rnd.nextDouble()
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
            }
        }
    }
}

