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

package io.questdb.cairo;

import io.questdb.MessageBus;
import io.questdb.MessageBusImpl;
import io.questdb.Metrics;
import io.questdb.Telemetry;
import io.questdb.cairo.mig.EngineMigration;
import io.questdb.cairo.pool.AbstractMultiTenantPool;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.pool.ReaderPool;
import io.questdb.cairo.pool.SequencerMetadataPool;
import io.questdb.cairo.pool.SqlCompilerPool;
import io.questdb.cairo.pool.TableMetadataPool;
import io.questdb.cairo.pool.WalWriterPool;
import io.questdb.cairo.pool.WriterPool;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.DefaultWalDirectoryPolicy;
import io.questdb.cairo.wal.DefaultWalListener;
import io.questdb.cairo.wal.WalDirectoryPolicy;
import io.questdb.cairo.wal.WalListener;
import io.questdb.cairo.wal.WalReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SequencerMetadata;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cutlass.text.CopyContext;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerFactory;
import io.questdb.griffin.SqlCompilerFactoryImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.SCSequence;
import io.questdb.mp.Sequence;
import io.questdb.mp.SimpleWaitingLock;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.MutableCharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.tasks.TelemetryTask;
import io.questdb.tasks.TelemetryWalTask;
import io.questdb.tasks.WalTxnNotificationTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.sql.OperationFuture.QUERY_COMPLETE;
import static io.questdb.griffin.CompiledQuery.*;

public class CairoEngine implements Closeable, WriterSource {
    public static final String REASON_BUSY_READER = "busyReader";
    public static final String REASON_BUSY_SEQUENCER_METADATA_POOL = "busySequencerMetaPool";
    public static final String REASON_BUSY_TABLE_READER_METADATA_POOL = "busyTableReaderMetaPool";
    public static final String REASON_CHECKPOINT_IN_PROGRESS = "checkpointInProgress";
    private static final Log LOG = LogFactory.getLog(CairoEngine.class);
    private static final int MAX_SLEEP_MILLIS = 250;
    protected final CairoConfiguration configuration;
    private final AtomicLong asyncCommandCorrelationId = new AtomicLong();
    private final DatabaseCheckpointAgent checkpointAgent;
    private final CopyContext copyContext;
    private final ConcurrentHashMap<TableToken> createTableLock = new ConcurrentHashMap<>();
    private final EngineMaintenanceJob engineMaintenanceJob;
    private final FunctionFactoryCache ffCache;
    private final MessageBusImpl messageBus;
    private final MetadataCache metadataCache;
    private final Metrics metrics;
    private final PartitionOverwriteControl partitionOverwriteControl = new PartitionOverwriteControl();
    private final QueryRegistry queryRegistry;
    private final ReaderPool readerPool;
    private final SqlExecutionContext rootExecutionContext;
    private final SequencerMetadataPool sequencerMetadataPool;
    private final SqlCompilerPool sqlCompilerPool;
    private final TableFlagResolver tableFlagResolver;
    private final IDGenerator tableIdGenerator;
    private final TableMetadataPool tableMetadataPool;
    private final TableNameRegistry tableNameRegistry;
    private final TableSequencerAPI tableSequencerAPI;
    private final Telemetry<TelemetryTask> telemetry;
    private final Telemetry<TelemetryWalTask> telemetryWal;
    // initial value of unpublishedWalTxnCount is 1 because we want to scan for non-applied WAL transactions on startup
    private final AtomicLong unpublishedWalTxnCount = new AtomicLong(1);
    private final WalWriterPool walWriterPool;
    private final WriterPool writerPool;
    private @NotNull DdlListener ddlListener = DefaultDdlListener.INSTANCE;
    private @NotNull WalDirectoryPolicy walDirectoryPolicy = DefaultWalDirectoryPolicy.INSTANCE;
    private @NotNull WalListener walListener = DefaultWalListener.INSTANCE;

    // Kept for embedded API purposes. The second constructor (the one with metrics)
    // should be preferred for internal use.
    public CairoEngine(CairoConfiguration configuration) {
        this(configuration, Metrics.disabled());
    }

    public CairoEngine(CairoConfiguration configuration, Metrics metrics) {
        try {
            ffCache = new FunctionFactoryCache(
                    configuration,
                    ServiceLoader.load(FunctionFactory.class, FunctionFactory.class.getClassLoader())
            );
            this.tableFlagResolver = newTableFlagResolver(configuration);
            this.configuration = configuration;
            this.copyContext = new CopyContext(configuration);
            this.tableSequencerAPI = new TableSequencerAPI(this, configuration);
            this.messageBus = new MessageBusImpl(configuration);
            this.metrics = metrics;
            // Message bus and metrics must be initialized before the pools.
            this.writerPool = new WriterPool(configuration, this);
            this.readerPool = new ReaderPool(configuration, messageBus, partitionOverwriteControl);
            this.sequencerMetadataPool = new SequencerMetadataPool(configuration, this);
            this.tableMetadataPool = new TableMetadataPool(configuration);
            this.walWriterPool = new WalWriterPool(configuration, this);
            this.engineMaintenanceJob = new EngineMaintenanceJob(configuration);
            this.telemetry = new Telemetry<>(TelemetryTask.TELEMETRY, configuration);
            this.telemetryWal = new Telemetry<>(TelemetryWalTask.WAL_TELEMETRY, configuration);
            this.tableIdGenerator = new IDGenerator(configuration, TableUtils.TAB_INDEX_FILE_NAME);
            this.checkpointAgent = new DatabaseCheckpointAgent(this);
            this.queryRegistry = new QueryRegistry(configuration);
            this.rootExecutionContext = new SqlExecutionContextImpl(this, 1)
                    .with(AllowAllSecurityContext.INSTANCE);

            tableIdGenerator.open();
            checkpointRecover();

            // Migrate database files.
            EngineMigration.migrateEngineTo(this, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);
            tableNameRegistry = configuration.isReadOnlyInstance()
                    ? new TableNameRegistryRO(configuration, tableFlagResolver)
                    : new TableNameRegistryRW(configuration, tableFlagResolver);
            tableNameRegistry.reload();

            this.sqlCompilerPool = new SqlCompilerPool(this);
            if (configuration.getPartitionO3OverwriteControlEnabled()) {
                enablePartitionOverwriteControl();
            }
            this.metadataCache = new MetadataCache(this);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public static void compile(SqlCompiler compiler, CharSequence sql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
        switch (cq.getType()) {
            case INSERT:
            case INSERT_AS_SELECT:
                final InsertOperation insertOperation = cq.getInsertOperation();
                if (insertOperation != null) {
                    // for insert as select the operation is null
                    try (InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)) {
                        insertMethod.execute();
                        insertMethod.commit();
                    }
                }
                break;
            case DROP:
                drop0(null, cq);
                break;
            case SELECT:
                throw SqlException.$(0, "use select()");
            default:
                try (OperationFuture future = cq.execute(null)) {
                    future.await();
                }
                break;
        }
    }

    public static void ddl(
            SqlCompiler compiler,
            CharSequence ddl,
            SqlExecutionContext sqlExecutionContext,
            @Nullable SCSequence eventSubSeq
    ) throws SqlException {
        CompiledQuery cc = compiler.compile(ddl, sqlExecutionContext);
        switch (cc.getType()) {
            case INSERT:
                throw SqlException.$(0, "use insert()");
            case DROP:
                throw SqlException.$(0, "use drop()");
            case SELECT:
                throw SqlException.$(0, "use select()");
            default:
                try (OperationFuture future = cc.execute(eventSubSeq)) {
                    future.await();
                }
                break;
        }
    }

    public static void insert(SqlCompiler compiler, CharSequence insertSql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        CompiledQuery cq = compiler.compile(insertSql, sqlExecutionContext);
        switch (cq.getType()) {
            case INSERT:
            case INSERT_AS_SELECT:
                final InsertOperation insertOperation = cq.getInsertOperation();
                if (insertOperation != null) {
                    // for insert as select the operation is null
                    try (InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)) {
                        insertMethod.execute();
                        insertMethod.commit();
                    }
                }
                break;
            case SELECT:
                throw SqlException.$(0, "use select()");
            case DROP:
                throw SqlException.$(0, "use drop()");
            default:
                throw SqlException.$(0, "use ddl()");
        }
    }

    public static RecordCursorFactory select(SqlCompiler compiler, CharSequence selectSql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return compiler.compile(selectSql, sqlExecutionContext).getRecordCursorFactory();
    }

    public void applyTableRename(TableToken token, TableToken updatedTableToken) {
        tableNameRegistry.rename(token.getTableName(), updatedTableToken.getTableName(), token);
        if (token.isWal()) {
            tableSequencerAPI.applyRename(updatedTableToken);
        }
    }

    public void awaitTable(String tableName, long timeout, TimeUnit timeoutUnit) {
        awaitTxn(tableName, -1, timeout, timeoutUnit);
    }

    public void awaitTxn(String tableName, long txn, long timeout, TimeUnit timeoutUnit) {
        final long startTime = configuration.getMillisecondClock().getTicks();
        long maxWait = timeoutUnit.toMillis(timeout);
        int sleep = 10;

        TableToken tableToken = null;
        long seqTxn = txn;
        long writerTxn = -1;
        while (configuration.getMillisecondClock().getTicks() - startTime < maxWait) {
            if (tableToken == null) {
                try {
                    tableToken = verifyTableName(tableName);
                } catch (CairoException ex) {
                    Os.sleep(sleep);
                    sleep = Math.min(MAX_SLEEP_MILLIS, sleep * 2);
                    continue;
                }
            }

            if (tableToken != null) {
                seqTxn = seqTxn > -1 ? seqTxn : getTableSequencerAPI().getTxnTracker(tableToken).getSeqTxn();
                writerTxn = getTableSequencerAPI().getTxnTracker(tableToken).getWriterTxn();
                if (seqTxn <= writerTxn) {
                    return;
                }

                boolean isSuspended = getTableSequencerAPI().isSuspended(tableToken);
                if (isSuspended) {
                    throw CairoException.nonCritical().put("table is suspended [tableName=").put(tableName).put(']');
                }
                Os.sleep(sleep);
                sleep = Math.min(MAX_SLEEP_MILLIS, sleep * 2);
            }
        }
        throw CairoException.nonCritical()
                .put("txn timed out [table=").put(tableName)
                .put(", expectedTxn=").put(seqTxn)
                .put(", writerTxn=").put(writerTxn);
    }

    public void checkpointCreate(SqlExecutionContext executionContext) throws SqlException {
        checkpointAgent.checkpointCreate(executionContext, false);
    }

    public void snapshotCreate(SqlExecutionContext executionContext) throws SqlException {
        checkpointAgent.checkpointCreate(executionContext, true);
    }

    /**
     * Recovers database from checkpoint after restoring data from a snapshot.
     */
    public final void checkpointRecover() {
        checkpointAgent.recover();
    }

    public void checkpointRelease() throws SqlException {
        checkpointAgent.checkpointRelease();
    }

    @TestOnly
    public boolean clear() {
        checkpointAgent.clear();
        messageBus.clear();
        boolean b1 = readerPool.releaseAll();
        boolean b2 = writerPool.releaseAll();
        boolean b3 = tableSequencerAPI.releaseAll();
        boolean b4 = sequencerMetadataPool.releaseAll();
        boolean b5 = walWriterPool.releaseAll();
        boolean b6 = tableMetadataPool.releaseAll();
        partitionOverwriteControl.clear();
        return b1 & b2 & b3 & b4 & b5 & b6;
    }

    @Override
    public void close() {
        Misc.free(sqlCompilerPool);
        Misc.free(writerPool);
        Misc.free(readerPool);
        Misc.free(sequencerMetadataPool);
        Misc.free(tableMetadataPool);
        Misc.free(walWriterPool);
        Misc.free(tableIdGenerator);
        Misc.free(messageBus);
        Misc.free(tableSequencerAPI);
        Misc.free(telemetry);
        Misc.free(telemetryWal);
        Misc.free(tableNameRegistry);
        Misc.free(checkpointAgent);
        Misc.free(metadataCache);
    }

    @TestOnly
    public void closeNameRegistry() {
        tableNameRegistry.close();
    }

    public void compile(CharSequence sql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        try (SqlCompiler compiler = getSqlCompiler()) {
            compile(compiler, sql, sqlExecutionContext);
        }
    }

    public void compile(CharSequence sql) throws SqlException {
        compile(sql, rootExecutionContext);
    }

    public @NotNull TableToken createTable(
            SecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            boolean ifNotExists,
            TableStructure struct,
            boolean keepLock
    ) {
        securityContext.authorizeTableCreate();
        return createTableUnsecure(securityContext, mem, path, ifNotExists, struct, keepLock, false);
    }

    public @NotNull TableToken createTableInVolume(
            SecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            boolean ifNotExists,
            TableStructure struct,
            boolean keepLock
    ) {
        securityContext.authorizeTableCreate();
        return createTableUnsecure(securityContext, mem, path, ifNotExists, struct, keepLock, true);
    }

    public void ddl(CharSequence ddl, SqlExecutionContext executionContext) throws SqlException {
        ddl(ddl, executionContext, null);
    }

    public void ddl(CharSequence ddl, SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        try (SqlCompiler compiler = getSqlCompiler()) {
            ddl(compiler, ddl, sqlExecutionContext, eventSubSeq);
        }
    }

    public void drop(Path path, TableToken tableToken) {
        verifyTableToken(tableToken);
        if (tableToken.isWal()) {
            if (tableNameRegistry.dropTable(tableToken)) {
                tableSequencerAPI.dropTable(tableToken, false);
            } else {
                LOG.info().$("table is already dropped [table=").$(tableToken)
                        .$(", dirName=").$(tableToken.getDirName()).I$();
            }
        } else {
            CharSequence lockedReason = lockAll(tableToken, "removeTable", false);
            if (lockedReason == null) {
                try {
                    path.of(configuration.getRoot()).concat(tableToken).$();
                    if (!configuration.getFilesFacade().unlinkOrRemove(path, LOG)) {
                        throw CairoException.critical(configuration.getFilesFacade().errno()).put("could not remove table [name=").put(tableToken.getTableName())
                                .put(", dirName=").put(tableToken.getDirName()).put(']');
                    }
                } finally {
                    unlockTableUnsafe(tableToken, null, false);
                    try (MetadataCacheWriter metadataRW = getMetadataCache().writeLock()) {
                        metadataRW.dropTable(tableToken);
                    }
                }

                tableNameRegistry.dropTable(tableToken);
                return;
            }
            throw CairoException.nonCritical().put("could not lock '").put(tableToken.getTableName()).put("' [reason='").put(lockedReason).put("']");
        }
    }

    public void drop(CharSequence dropSql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        drop(dropSql, sqlExecutionContext, null);
    }

    public void drop(CharSequence dropSql, SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        try (SqlCompiler compiler = getSqlCompiler()) {
            final CompiledQuery cq = compiler.compile(dropSql, sqlExecutionContext);
            switch (cq.getType()) {
                case UPDATE:
                case DROP:
                    drop0(eventSubSeq, cq);
                    break;
                case INSERT:
                case INSERT_AS_SELECT:
                    throw SqlException.$(0, "use insert()");
                case SELECT:
                    throw SqlException.$(0, "use select()");
                default:
                    throw SqlException.$(0, "use ddl()");
            }
        } catch (SqlException | CairoException ex) {
            if (!Chars.contains(ex.getFlyweightMessage(), "table does not exist")) {
                throw ex;
            }
        } catch (TableReferenceOutOfDateException e) {
            // ignore
        }
    }

    public void enablePartitionOverwriteControl() {
        LOG.info().$("partition overwrite control is enabled").$();
        partitionOverwriteControl.enable();
    }

    public TableWriter getBackupWriter(TableToken tableToken, CharSequence backupDirName) {
        verifyTableToken(tableToken);
        // There is no point in pooling/caching these writers since they are only used once, backups are not incremental
        return new TableWriter(
                configuration,
                tableToken,
                messageBus,
                null,
                true,
                DefaultLifecycleManager.INSTANCE,
                backupDirName,
                getDdlListener(tableToken),
                checkpointAgent,
                Metrics.disabled(),
                this
        );
    }

    @TestOnly
    public int getBusyReaderCount() {
        return readerPool.getBusyCount();
    }

    @TestOnly
    public int getBusyWriterCount() {
        return writerPool.getBusyCount();
    }

    public DatabaseCheckpointStatus getCheckpointStatus() {
        return checkpointAgent;
    }

    public long getCommandCorrelationId() {
        return asyncCommandCorrelationId.incrementAndGet();
    }

    public CairoConfiguration getConfiguration() {
        return configuration;
    }

    public CopyContext getCopyContext() {
        return copyContext;
    }

    public @NotNull DdlListener getDdlListener(TableToken tableToken) {
        return tableFlagResolver.isSystem(tableToken.getTableName()) ? DefaultDdlListener.INSTANCE : ddlListener;
    }

    public Job getEngineMaintenanceJob() {
        return engineMaintenanceJob;
    }

    public FunctionFactoryCache getFunctionFactoryCache() {
        return ffCache;
    }

    public TableMetadata getLegacyMetadata(TableToken tableToken) {
        return getLegacyMetadata(tableToken, TableUtils.ANY_TABLE_VERSION);
    }

    /**
     * Retrieves up-to-date table metadata regardless of table type.
     *
     * @param tableToken     table token
     * @param desiredVersion version of table metadata used previously if consistent metadata reads are required
     * @return returns {@link SequencerMetadata} for WAL tables and {@link TableMetadata}
     * for non-WAL, which would be metadata of the {@link TableReader}
     */
    public TableMetadata getLegacyMetadata(TableToken tableToken, long desiredVersion) {
        if (!tableToken.isWal()) {
            return getTableMetadata(tableToken, desiredVersion);
        }
        return getSequencerMetadata(tableToken, desiredVersion);
    }

    public MessageBus getMessageBus() {
        return messageBus;
    }

    public MetadataCache getMetadataCache() {
        return metadataCache;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public PartitionOverwriteControl getPartitionOverwriteControl() {
        return partitionOverwriteControl;
    }

    @TestOnly
    public PoolListener getPoolListener() {
        return this.writerPool.getPoolListener();
    }

    public QueryRegistry getQueryRegistry() {
        return queryRegistry;
    }

    public TableReader getReader(CharSequence tableName) {
        TableToken tableToken = verifyTableNameForRead(tableName);
        // Do not call getReader(TableToken tableToken), it will do unnecessary token verification
        return readerPool.get(tableToken);
    }

    public TableReader getReader(TableToken tableToken) {
        verifyTableToken(tableToken);
        return readerPool.get(tableToken);
    }

    public TableReader getReader(TableToken tableToken, long metadataVersion) {
        verifyTableToken(tableToken);
        final int tableId = tableToken.getTableId();
        TableReader reader = readerPool.get(tableToken);
        if ((metadataVersion > -1 && reader.getMetadataVersion() != metadataVersion)
                || tableId > -1 && reader.getMetadata().getTableId() != tableId) {
            TableReferenceOutOfDateException ex = TableReferenceOutOfDateException.of(
                    tableToken,
                    tableId,
                    reader.getMetadata().getTableId(),
                    metadataVersion,
                    reader.getMetadataVersion()
            );
            reader.close();
            throw ex;
        }
        return reader;
    }

    public Map<CharSequence, AbstractMultiTenantPool.Entry<ReaderPool.R>> getReaderPoolEntries() {
        return readerPool.entries();
    }

    public TableReader getReaderWithRepair(TableToken tableToken) {
        // todo: untested verification
        verifyTableToken(tableToken);
        try {
            return getReader(tableToken);
        } catch (CairoException e) {
            // Cannot open reader on existing table is pretty bad.
            // In some messed states, for example after _meta file swap failure Reader cannot be opened
            // but writer can be. Opening writer fixes the table mess.
            tryRepairTable(tableToken, e);
        }
        try {
            return getReader(tableToken);
        } catch (CairoException e) {
            LOG.critical()
                    .$("could not open reader [table=").$(tableToken)
                    .$(", errno=").$(e.getErrno())
                    .$(", error=").$(e.getFlyweightMessage())
                    .I$();
            throw e;
        }
    }

    public TableMetadata getSequencerMetadata(TableToken tableToken) {
        return getSequencerMetadata(tableToken, TableUtils.ANY_TABLE_VERSION);
    }

    /**
     * Table metadata as seen by the table sequencer. This is the most up-to-date table
     * metadata, and it can be used to positively confirm column metadata changes immediately after
     * making them.
     * <p>
     * However, this metadata cannot confirm all the changes, one of which is "dedup" flag on a table.
     * This is a shortcoming and to confirm the "dedup" flag {{@link #getTableMetadata(TableToken, long)}} should
     * be polled instead. We expect to fix issues like this one in the near future.
     *
     * @param tableToken     table token
     * @param desiredVersion version of table metadata used previously if consistent metadata reads are required
     * @return sequence metadata instance
     */
    public TableMetadata getSequencerMetadata(TableToken tableToken, long desiredVersion) {
        assert tableToken.isWal();
        verifyTableToken(tableToken);
        return validateDesiredMetadataVersion(
                tableToken,
                sequencerMetadataPool.get(tableToken),
                desiredVersion
        );
    }

    public SqlCompiler getSqlCompiler() {
        return sqlCompilerPool.get();
    }

    public SqlCompilerFactory getSqlCompilerFactory() {
        return SqlCompilerFactoryImpl.INSTANCE;
    }

    public TableFlagResolver getTableFlagResolver() {
        return tableFlagResolver;
    }

    public IDGenerator getTableIdGenerator() {
        return tableIdGenerator;
    }

    /**
     * Same as {{@link #getTableMetadata(TableToken, long)}} but it will provide the most
     * up-to-date version of the metadata without correlating it with anything else.
     *
     * @param tableToken table token
     * @return pooled metadata instance
     */
    public TableMetadata getTableMetadata(TableToken tableToken) {
        return getTableMetadata(tableToken, TableUtils.ANY_TABLE_VERSION);
    }

    /**
     * This is explicitly "table" metadata. For legacy (non-WAL) tables, this metadata
     * is the same as writer metadata. For new WAL tables, table metadata could be "old",
     * as in not all WAL transactions has reached the table yet. In scenarios where
     * table modification is made and positively confirmed immediately after via metadata, {@link #getSequencerMetadata(TableToken, long)}
     * must be used instead.
     * <p>
     * Metadata provided by this method is good enough for the read-only queries.
     *
     * @param tableToken     table token
     * @param desiredVersion version of table metadata used previously if consistent metadata reads are required
     * @return pooled metadata instance
     */
    public TableMetadata getTableMetadata(TableToken tableToken, long desiredVersion) {
        verifyTableToken(tableToken);
        try {
            return validateDesiredMetadataVersion(tableToken, tableMetadataPool.get(tableToken), desiredVersion);
        } catch (CairoException e) {
            if (tableToken.isWal()) {
                throw e;
            } else {
                tryRepairTable(tableToken, e);
            }
        }
        return validateDesiredMetadataVersion(tableToken, tableMetadataPool.get(tableToken), desiredVersion);
    }

    public TableSequencerAPI getTableSequencerAPI() {
        return tableSequencerAPI;
    }

    public int getTableStatus(Path path, TableToken tableToken) {
        if (tableToken == TableNameRegistry.LOCKED_TOKEN) {
            return TableUtils.TABLE_RESERVED;
        }
        if (tableToken == null || !tableToken.equals(tableNameRegistry.getTableToken(tableToken.getTableName()))) {
            return TableUtils.TABLE_DOES_NOT_EXIST;
        }
        return TableUtils.exists(configuration.getFilesFacade(), path, configuration.getRoot(), tableToken.getDirName());
    }

    public int getTableStatus(Path path, CharSequence tableName) {
        final TableToken tableToken = tableNameRegistry.getTableToken(tableName);
        if (tableToken == null) {
            return TableUtils.TABLE_DOES_NOT_EXIST;
        }
        return getTableStatus(path, tableToken);
    }

    @TestOnly
    public int getTableStatus(CharSequence tableName) {
        return getTableStatus(Path.getThreadLocal(configuration.getRoot()), tableName);
    }

    public TableToken getTableTokenByDirName(String dirName) {
        return tableNameRegistry.getTableTokenByDirName(dirName);
    }

    public int getTableTokenCount(boolean includeDropped) {
        return tableNameRegistry.getTableTokenCount(includeDropped);
    }

    public TableToken getTableTokenIfExists(CharSequence tableName) {
        final TableToken token = tableNameRegistry.getTableToken(tableName);
        if (token == TableNameRegistry.LOCKED_TOKEN) {
            return null;
        }
        return token;
    }

    public TableToken getTableTokenIfExists(CharSequence tableName, int lo, int hi) {
        final StringSink sink = Misc.getThreadLocalSink();
        sink.put(tableName, lo, hi);
        return getTableTokenIfExists(sink);
    }

    public void getTableTokens(ObjHashSet<TableToken> bucket, boolean includeDropped) {
        tableNameRegistry.getTableTokens(bucket, includeDropped);
    }

    @Override
    public TableWriterAPI getTableWriterAPI(TableToken tableToken, @NotNull String lockReason) {
        verifyTableToken(tableToken);
        if (!tableToken.isWal()) {
            return writerPool.get(tableToken, lockReason);
        }
        return walWriterPool.get(tableToken);
    }

    @Override
    public TableWriterAPI getTableWriterAPI(CharSequence tableName, @NotNull String lockReason) {
        TableToken tableToken = verifyTableNameForRead(tableName);
        // Do not call getTableWriterAPI(TableToken tableToken, String lockReason),
        // it will do unnecessary token verification
        if (!tableToken.isWal()) {
            return writerPool.get(tableToken, lockReason);
        }
        return walWriterPool.get(tableToken);
    }

    public Telemetry<TelemetryTask> getTelemetry() {
        return telemetry;
    }

    public Telemetry<TelemetryWalTask> getTelemetryWal() {
        return telemetryWal;
    }

    public long getUnpublishedWalTxnCount() {
        return unpublishedWalTxnCount.get();
    }

    public TableToken getUpdatedTableToken(TableToken tableToken) {
        return tableNameRegistry.getTokenByDirName(tableToken.getDirName());
    }

    public @NotNull WalDirectoryPolicy getWalDirectoryPolicy() {
        return walDirectoryPolicy;
    }

    public @NotNull WalListener getWalListener() {
        return walListener;
    }

    // For testing only
    @TestOnly
    public WalReader getWalReader(
            @SuppressWarnings("unused") SecurityContext securityContext,
            TableToken tableToken,
            CharSequence walName,
            int segmentId,
            long walRowCount
    ) {
        if (tableToken.isWal()) {
            return new WalReader(configuration, tableToken, walName, segmentId, walRowCount);
        }
        throw CairoException.nonCritical().put("WAL reader is not supported for table ").put(tableToken.getTableName());
    }

    public @NotNull WalWriter getWalWriter(TableToken tableToken) {
        verifyTableToken(tableToken);
        return walWriterPool.get(tableToken);
    }

    public TableWriter getWriter(TableToken tableToken, @NotNull String lockReason) {
        verifyTableToken(tableToken);
        return writerPool.get(tableToken, lockReason);
    }

    public TableWriter getWriterOrPublishCommand(TableToken tableToken, @NotNull AsyncWriterCommand asyncWriterCommand) {
        verifyTableToken(tableToken);
        return writerPool.getWriterOrPublishCommand(tableToken, asyncWriterCommand.getCommandName(), asyncWriterCommand);
    }

    public Map<CharSequence, WriterPool.Entry> getWriterPoolEntries() {
        return writerPool.entries();
    }

    public TableWriter getWriterUnsafe(TableToken tableToken, @NotNull String lockReason) {
        return writerPool.get(tableToken, lockReason);
    }

    public void insert(CharSequence insertSql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        try (SqlCompiler compiler = getSqlCompiler()) {
            insert(compiler, insertSql, sqlExecutionContext);
        }
    }

    public boolean isTableDropped(TableToken tableToken) {
        return isTableDropped(tableToken.getDirName());
    }

    public boolean isTableDropped(CharSequence dirName) {
        return tableNameRegistry.isTableDropped(dirName);
    }

    public boolean isWalTable(TableToken tableToken) {
        return tableToken.isWal();
    }

    public void load() {
        // Convert tables to WAL/non-WAL, if necessary.
        final ObjList<TableToken> convertedTables = TableConverter.convertTables(this, tableSequencerAPI, tableFlagResolver);
        tableNameRegistry.reload(convertedTables);
    }

    public String lockAll(TableToken tableToken, String lockReason, boolean ignoreInProgressCheckpoint) {
        assert null != lockReason;
        if (!ignoreInProgressCheckpoint && checkpointAgent.isInProgress()) {
            // prevent reader locking before checkpoint is released
            return REASON_CHECKPOINT_IN_PROGRESS;
        }
        // busy metadata is same as busy reader from user perspective
        String lockedReason;
        if (tableMetadataPool.lock(tableToken)) {
            if (sequencerMetadataPool.lock(tableToken)) {
                lockedReason = writerPool.lock(tableToken, lockReason);
                if (lockedReason == null) {
                    // not locked
                    if (readerPool.lock(tableToken)) {
                        LOG.info().$("locked [table=`").utf8(tableToken.getDirName())
                                .$("`, thread=").$(Thread.currentThread().getId())
                                .I$();
                        return null;
                    }
                    writerPool.unlock(tableToken);
                    lockedReason = REASON_BUSY_READER;
                }
                sequencerMetadataPool.unlock(tableToken);
            } else {
                lockedReason = REASON_BUSY_SEQUENCER_METADATA_POOL;
            }
            tableMetadataPool.unlock(tableToken);
        } else {
            lockedReason = REASON_BUSY_TABLE_READER_METADATA_POOL;
        }
        return lockedReason;
    }

    public boolean lockReaders(TableToken tableToken) {
        verifyTableToken(tableToken);
        return lockReadersByTableToken(tableToken);
    }

    public boolean lockReadersAndMetadata(TableToken tableToken) {
        if (checkpointAgent.isInProgress()) {
            // prevent reader locking before checkpoint is released
            return false;
        }
        if (readerPool.lock(tableToken)) {
            if (tableMetadataPool.lock(tableToken)) {
                return true;
            } else {
                readerPool.unlock(tableToken);
            }
        }
        return false;
    }

    public boolean lockReadersByTableToken(TableToken tableToken) {
        if (checkpointAgent.isInProgress()) {
            // prevent reader locking before checkpoint is released
            return false;
        }
        return readerPool.lock(tableToken);
    }

    public boolean lockTableCreate(TableToken tableToken) {
        return createTableLock.putIfAbsent(tableToken.getTableName(), tableToken) == null;
    }

    public TableToken lockTableName(CharSequence tableName, boolean isWal) {
        int tableId = (int) getTableIdGenerator().getNextId();
        return lockTableName(tableName, tableId, isWal);
    }

    @Nullable
    public TableToken lockTableName(CharSequence tableName, int tableId, boolean isWal) {
        String tableNameStr = Chars.toString(tableName);
        final String dirName = TableUtils.getTableDir(configuration.mangleTableDirNames(), tableNameStr, tableId, isWal);
        return lockTableName(tableNameStr, dirName, tableId, isWal);
    }

    @SuppressWarnings("unused")
    @Nullable
    public TableToken lockTableName(CharSequence tableName, String dirName, int tableId, boolean isWal) {
        validNameOrThrow(tableName);
        String tableNameStr = Chars.toString(tableName);
        return tableNameRegistry.lockTableName(tableNameStr, dirName, tableId, isWal);
    }

    public void notifyDropped(TableToken tableToken) {
        tableNameRegistry.dropTable(tableToken);
    }

    /**
     * Publishes notification of table transaction to the queue. The intent is to notify Apply2WalJob that
     * there are WAL files to be merged into the table. Notification can fail if the queue is full, in
     * which case it will have to be republished from a persisted storage. However, this method does not
     * care about that.
     *
     * @param tableToken table token of the table that has to be processed by the Apply2WalJob
     * @return true if the message was successfully put on the queue and false otherwise.
     */
    public boolean notifyWalTxnCommitted(@NotNull TableToken tableToken) {
        final Sequence pubSeq = messageBus.getWalTxnNotificationPubSequence();
        while (true) {
            long cursor = pubSeq.next();
            if (cursor > -1L) {
                WalTxnNotificationTask task = messageBus.getWalTxnNotificationQueue().get(cursor);
                task.of(tableToken);
                pubSeq.done(cursor);
                return true;
            } else if (cursor == -1L) {
                LOG.info().$("cannot publish WAL notifications, queue is full [current=").$(pubSeq.current())
                        .$(", table=").utf8(tableToken.getDirName())
                        .I$();
                // queue overflow, throw away notification and notify a job to rescan all tables
                notifyWalTxnRepublisher(tableToken);
                return false;
            }
        }
    }

    public void notifyWalTxnRepublisher(TableToken tableToken) {
        tableSequencerAPI.notifyCommitReadable(tableToken, -1);
        unpublishedWalTxnCount.incrementAndGet();
    }

    public void print(CharSequence sql, MutableCharSink<?> sink) throws SqlException {
        print(sql, sink, rootExecutionContext);
    }

    public void print(CharSequence sql, MutableCharSink<?> sink, SqlExecutionContext executionContext) throws SqlException {
        sink.clear();
        try (
                RecordCursorFactory factory = select(sql, executionContext);
                RecordCursor cursor = factory.getCursor(executionContext)
        ) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink);
        }
    }

    public void reconcileTableNameRegistryState() {
        tableNameRegistry.reconcile();
    }

    public void registerTableToken(TableToken tableToken) {
        tableNameRegistry.registerName(tableToken);
    }

    @TestOnly
    public boolean releaseAllReaders() {
        boolean b1 = sequencerMetadataPool.releaseAll();
        boolean b2 = tableMetadataPool.releaseAll();
        return readerPool.releaseAll() & b1 & b2;
    }

    @TestOnly
    public void releaseAllWalWriters() {
        walWriterPool.releaseAll();
    }

    @TestOnly
    public void releaseAllWriters() {
        writerPool.releaseAll();
    }

    public boolean releaseInactive() {
        boolean useful = writerPool.releaseInactive();
        useful |= readerPool.releaseInactive();
        useful |= tableSequencerAPI.releaseInactive();
        useful |= sequencerMetadataPool.releaseInactive();
        useful |= tableMetadataPool.releaseInactive();
        useful |= walWriterPool.releaseInactive();
        return useful;
    }

    @TestOnly
    public void releaseInactiveTableSequencers() {
        walWriterPool.releaseInactive();
        tableSequencerAPI.releaseInactive();
    }

    @TestOnly
    public void reloadTableNames() {
        reloadTableNames(null);
    }

    @TestOnly
    public void reloadTableNames(@Nullable ObjList<TableToken> convertedTables) {
        tableNameRegistry.reload(convertedTables);
        try (MetadataCacheWriter metadataRW = getMetadataCache().writeLock()) {
            metadataRW.hydrateAllTables();
        }
    }

    public void removeTableToken(TableToken tableToken) {
        tableNameRegistry.purgeToken(tableToken);
        tableSequencerAPI.purgeTxnTracker(tableToken.getDirName());
        PoolListener listener = getPoolListener();
        if (listener != null) {
            listener.onEvent(
                    PoolListener.SRC_TABLE_REGISTRY,
                    Thread.currentThread().getId(),
                    tableToken,
                    PoolListener.EV_REMOVE_TOKEN,
                    (short) 0,
                    (short) 0
            );
        }
    }

    public TableToken rename(
            SecurityContext securityContext,
            Path fromPath,
            MemoryMARW memory,
            CharSequence fromTableName,
            Path toPath,
            CharSequence toTableName
    ) {
        validNameOrThrow(fromTableName);
        validNameOrThrow(toTableName);

        final TableToken fromTableToken = verifyTableName(fromTableName);
        if (Chars.equalsIgnoreCaseNc(fromTableName, toTableName)) {
            return fromTableToken;
        }

        securityContext.authorizeTableRename(fromTableToken);
        TableToken toTableToken;
        if (fromTableToken != null) {
            if (fromTableToken.isWal()) {
                String toTableNameStr = Chars.toString(toTableName);
                toTableToken = tableNameRegistry.addTableAlias(toTableNameStr, fromTableToken);
                if (toTableToken != null) {
                    boolean renamed = false;
                    try {
                        try (WalWriter walWriter = getWalWriter(fromTableToken)) {
                            long seqTxn = walWriter.renameTable(fromTableName, toTableNameStr);
                            LOG.info().$("renaming table [from='").utf8(fromTableName)
                                    .$("', to='").utf8(toTableName)
                                    .$("', wal=").$(walWriter.getWalId())
                                    .$("', seqTxn=").$(seqTxn)
                                    .I$();
                            renamed = true;
                        }
                        TableUtils.overwriteTableNameFile(
                                fromPath.of(configuration.getRoot()).concat(toTableToken),
                                memory,
                                configuration.getFilesFacade(),
                                toTableToken.getTableName()
                        );
                    } finally {
                        if (renamed) {
                            tableNameRegistry.rename(fromTableToken, toTableToken);
                            try (MetadataCacheWriter metadataRW = getMetadataCache().writeLock()) {
                                metadataRW.renameTable(fromTableToken, toTableToken);
                            }
                        } else {
                            LOG.info()
                                    .$("failed to rename table [from=").utf8(fromTableName)
                                    .$(", to=").utf8(toTableName)
                                    .I$();
                            tableNameRegistry.removeAlias(toTableToken);
                        }
                    }
                } else {
                    throw CairoException.nonCritical()
                            .put("cannot rename table, new name is already in use" +
                                    " [table=").put(fromTableName)
                            .put(", toTableName=").put(toTableName)
                            .put(']');
                }
            } else {
                String lockedReason = lockAll(fromTableToken, "renameTable", false);
                if (lockedReason == null) {
                    try {
                        toTableToken = rename0(fromPath, fromTableToken, toPath, toTableName);
                        TableUtils.overwriteTableNameFile(
                                fromPath.of(configuration.getRoot()).concat(toTableToken),
                                memory,
                                configuration.getFilesFacade(),
                                toTableToken.getTableName()
                        );
                        try (MetadataCacheWriter metadataRW = getMetadataCache().writeLock()) {
                            metadataRW.renameTable(fromTableToken, toTableToken);
                        }
                    } finally {
                        unlock(securityContext, fromTableToken, null, false);
                    }
                    tableNameRegistry.dropTable(fromTableToken);
                } else {
                    LOG.error()
                            .$("could not lock and rename [from=").utf8(fromTableName)
                            .$("', to=").utf8(toTableName)
                            .$("', reason=").$(lockedReason)
                            .I$();
                    throw EntryUnavailableException.instance(lockedReason);
                }
            }

            getDdlListener(fromTableToken).onTableRenamed(securityContext, fromTableToken, toTableToken);

            return toTableToken;
        } else {
            LOG.error().$("cannot rename, table does not exist [table=").utf8(fromTableName).I$();
            throw CairoException.nonCritical().put("cannot rename, table does not exist [table=").put(fromTableName).put(']');
        }
    }

    @TestOnly
    public void resetNameRegistryMemory() {
        tableNameRegistry.resetMemory();
    }

    public RecordCursorFactory select(CharSequence selectSql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        try (SqlCompiler compiler = getSqlCompiler()) {
            return select(compiler, selectSql, sqlExecutionContext);
        }
    }

    @SuppressWarnings("unused")
    public void setDdlListener(@NotNull DdlListener ddlListener) {
        this.ddlListener = ddlListener;
    }

    @TestOnly
    public void setPoolListener(PoolListener poolListener) {
        this.tableMetadataPool.setPoolListener(poolListener);
        this.sequencerMetadataPool.setPoolListener(poolListener);
        this.writerPool.setPoolListener(poolListener);
        this.readerPool.setPoolListener(poolListener);
        this.walWriterPool.setPoolListener(poolListener);
    }

    @TestOnly
    public void setReaderListener(ReaderPool.ReaderListener readerListener) {
        readerPool.setTableReaderListener(readerListener);
    }

    @TestOnly
    public void setUp() {
    }

    public void setWalDirectoryPolicy(@NotNull WalDirectoryPolicy walDirectoryPolicy) {
        this.walDirectoryPolicy = walDirectoryPolicy;
    }

    public void setWalListener(@NotNull WalListener walListener) {
        this.walListener = walListener;
    }

    public void setWalPurgeJobRunLock(@Nullable SimpleWaitingLock walPurgeJobRunLock) {
        this.checkpointAgent.setWalPurgeJobRunLock(walPurgeJobRunLock);
    }

    public void unlock(
            @SuppressWarnings("unused") SecurityContext securityContext,
            TableToken tableToken,
            @Nullable TableWriter writer,
            boolean newTable
    ) {
        verifyTableToken(tableToken);
        unlockTableUnsafe(tableToken, writer, newTable);
        LOG.info().$("unlocked [table=`").$(tableToken).$("`]").$();
    }

    public void unlockReaders(TableToken tableToken) {
        verifyTableToken(tableToken);
        readerPool.unlock(tableToken);
    }

    public void unlockReadersAndMetadata(TableToken tableToken) {
        readerPool.unlock(tableToken);
        tableMetadataPool.unlock(tableToken);
    }

    public void unlockTableCreate(TableToken tableToken) {
        createTableLock.remove(tableToken.getTableName(), tableToken);
    }

    public void unlockTableName(TableToken tableToken) {
        tableNameRegistry.unlockTableName(tableToken);
    }

    public long update(CharSequence updateSql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return update(updateSql, sqlExecutionContext, null);
    }

    public long update(CharSequence updateSql, SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        try (SqlCompiler compiler = getSqlCompiler()) {
            while (true) {
                try {
                    CompiledQuery cc = compiler.compile(updateSql, sqlExecutionContext);
                    switch (cc.getType()) {
                        case UPDATE:
                            try (
                                    // update operation is stashed in the compiled query,
                                    // and it has to be released to avoid memory leak
                                    UpdateOperation ignore = cc.getUpdateOperation();
                                    OperationFuture future = cc.execute(eventSubSeq)
                            ) {
                                future.await();
                                return future.getAffectedRowsCount();
                            }
                        case INSERT:
                            throw SqlException.$(0, "use insert()");
                        case DROP:
                            throw SqlException.$(0, "use drop()");
                        case SELECT:
                            throw SqlException.$(0, "use select()");
                    }
                } catch (TableReferenceOutOfDateException ex) {
                    // retry, e.g. continue
                } catch (SqlException ex) {
                    if (Chars.contains(ex.getFlyweightMessage(), "cached query plan cannot be used because table schema has changed")) {
                        continue;
                    }
                    throw ex;
                }
            }
        }
    }

    public TableToken verifyTableName(final CharSequence tableName) {
        TableToken tableToken = tableNameRegistry.getTableToken(tableName);
        if (tableToken == null) {
            throw CairoException.tableDoesNotExist(tableName);
        }
        if (tableToken == TableNameRegistry.LOCKED_TOKEN) {
            throw CairoException.nonCritical().put("table name is reserved [table=").put(tableName).put("]");
        }
        return tableToken;
    }

    public TableToken verifyTableName(final CharSequence tableName, int lo, int hi) {
        StringSink sink = Misc.getThreadLocalSink();
        sink.put(tableName, lo, hi);
        return verifyTableName(sink);
    }

    public void verifyTableToken(TableToken tableToken) {
        TableToken tt = tableNameRegistry.getTableToken(tableToken.getTableName());
        if (tt == null || tt == TableNameRegistry.LOCKED_TOKEN) {
            throw CairoException.tableDoesNotExist(tableToken.getTableName());
        }
        if (!tt.equals(tableToken)) {
            throw TableReferenceOutOfDateException.of(tableToken, tableToken.getTableId(), tt.getTableId(), tt.getTableId(), -1);
        }
    }

    private static void drop0(@Nullable SCSequence eventSubSeq, CompiledQuery cq) throws SqlException {
        try (OperationFuture fut = cq.execute(eventSubSeq)) {
            if (fut.await(30 * Timestamps.SECOND_MILLIS) != QUERY_COMPLETE) {
                throw SqlException.$(0, "drop table timeout");
            }
        }
    }

    // caller has to acquire the lock before this method is called and release the lock after the call
    private void createTableInVolumeUnsafe(MemoryMARW mem, Path path, TableStructure struct, TableToken tableToken) {
        if (TableUtils.TABLE_DOES_NOT_EXIST != TableUtils.existsInVolume(configuration.getFilesFacade(), path, tableToken.getDirName())) {
            throw CairoException.nonCritical().put("name is reserved [table=").put(tableToken.getTableName()).put(']');
        }

        // only create the table after it has been registered
        TableUtils.createTableInVolume(
                configuration.getFilesFacade(),
                configuration.getRoot(),
                configuration.getMkDirMode(),
                mem,
                path,
                tableToken.getDirName(),
                struct,
                ColumnType.VERSION,
                tableToken.getTableId()
        );
    }

    // caller has to acquire the lock before this method is called and release the lock after the call
    private void createTableUnsafe(MemoryMARW mem, Path path, TableStructure struct, TableToken tableToken) {
        if (TableUtils.TABLE_DOES_NOT_EXIST != TableUtils.exists(configuration.getFilesFacade(), path, configuration.getRoot(), tableToken.getDirName())) {
            throw CairoException.nonCritical().put("name is reserved [table=").put(tableToken.getTableName()).put(']');
        }

        // only create the table after it has been registered
        TableUtils.createTable(
                configuration.getFilesFacade(),
                configuration.getRoot(),
                configuration.getMkDirMode(),
                mem,
                path,
                tableToken.getDirName(),
                struct,
                ColumnType.VERSION,
                tableToken.getTableId()
        );
    }

    private @NotNull TableToken createTableUnsecure(
            SecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            boolean ifNotExists,
            TableStructure struct,
            boolean keepLock,
            boolean inVolume
    ) {
        assert !struct.isWalEnabled() || PartitionBy.isPartitioned(struct.getPartitionBy()) : "WAL is only supported for partitioned tables";
        final CharSequence tableName = struct.getTableName();
        validNameOrThrow(tableName);

        final int tableId = (int) tableIdGenerator.getNextId();

        while (true) {
            TableToken tableToken = lockTableName(tableName, tableId, struct.isWalEnabled());
            if (tableToken == null) {
                if (ifNotExists) {
                    tableToken = getTableTokenIfExists(tableName);
                    if (tableToken != null) {
                        return tableToken;
                    }
                    Os.pause();
                    continue;
                }
                throw EntryUnavailableException.instance("table exists");
            }
            while (!lockTableCreate(tableToken)) {
                Os.pause();
            }
            try {
                String lockedReason = lockAll(tableToken, "createTable", true);
                if (lockedReason == null) {
                    boolean tableCreated = false;
                    try {
                        if (inVolume) {
                            createTableInVolumeUnsafe(mem, path, struct, tableToken);
                        } else {
                            createTableUnsafe(mem, path, struct, tableToken);
                        }

                        if (struct.isWalEnabled()) {
                            tableSequencerAPI.registerTable(tableToken.getTableId(), struct, tableToken);
                        }
                        tableCreated = true;
                    } finally {
                        if (!keepLock) {
                            unlockTableUnsafe(tableToken, null, tableCreated);
                            LOG.info().$("unlocked [table=`").$(tableToken).$("`]").$();
                        }
                    }
                    tableNameRegistry.registerName(tableToken);
                } else {
                    if (!ifNotExists) {
                        throw EntryUnavailableException.instance(lockedReason);
                    }
                }
            } catch (Throwable th) {
                if (struct.isWalEnabled()) {
                    // tableToken.getLoggingName() === tableName, table cannot be renamed while creation hasn't finished
                    tableSequencerAPI.dropTable(tableToken, true);
                }
                throw th;
            } finally {
                tableNameRegistry.unlockTableName(tableToken);
                unlockTableCreate(tableToken);
            }

            getDdlListener(tableToken).onTableCreated(securityContext, tableToken);

            try (MetadataCacheWriter metadataRW = getMetadataCache().writeLock()) {
                metadataRW.hydrateTable(tableToken);
            }

            return tableToken;
        }
    }


    private TableToken rename0(Path fromPath, TableToken fromTableToken, Path toPath, CharSequence toTableName) {

        // !!! we do not care what is inside the path1 & path2, we will reset them anyway
        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence root = configuration.getRoot();

        fromPath.of(root).concat(fromTableToken).$();

        TableToken toTableToken = lockTableName(toTableName, fromTableToken.getTableId(), false);

        if (toTableToken == null) {
            LOG.error()
                    .$("rename target exists [from='").utf8(fromTableToken.getTableName())
                    .$("', to='").utf8(toTableName)
                    .I$();
            throw CairoException.nonCritical().put("Rename target exists");
        }
        while (!lockTableCreate(toTableToken)) {
            Os.pause();
        }

        if (ff.exists(toPath.of(root).concat(toTableToken).$())) {
            tableNameRegistry.unlockTableName(toTableToken);
        }

        try {
            if (ff.rename(fromPath.$(), toPath.$()) != Files.FILES_RENAME_OK) {
                final int error = ff.errno();
                LOG.error()
                        .$("could not rename [from='").$(fromPath)
                        .$("', to='").$(toPath)
                        .$("', error=").$(error)
                        .I$();
                throw CairoException.critical(error)
                        .put("could not rename [from='").put(fromPath)
                        .put("', to='").put(toPath)
                        .put(']');
            }
            tableNameRegistry.registerName(toTableToken);
            return toTableToken;
        } finally {
            tableNameRegistry.unlockTableName(toTableToken);
            unlockTableCreate(toTableToken);
        }
    }

    private void tryRepairTable(TableToken tableToken, CairoException rethrow) {
        LOG.info()
                .$("starting table repair [table=").$(tableToken)
                .$(", dirName=").utf8(tableToken.getDirName())
                .$(", cause=").$(rethrow.getFlyweightMessage())
                .I$();
        try {
            writerPool.get(tableToken, "repair").close();
            LOG.info().$("table repair succeeded [table=").$(tableToken).I$();
        } catch (EntryUnavailableException e) {
            // This is fine, writer is busy. Throw back origin error.
            LOG.info().$("writer is busy, skipping repair [table=").$(tableToken).I$();
            throw rethrow;
        } catch (Throwable th) {
            LOG.critical()
                    .$("table repair failed [dirName=").utf8(tableToken.getDirName())
                    .$(", error=").$(th.getMessage())
                    .I$();
            throw rethrow;
        }
    }

    private void unlockTableUnsafe(TableToken tableToken, TableWriter writer, boolean newTable) {
        readerPool.unlock(tableToken);
        writerPool.unlock(tableToken, writer, newTable);
        sequencerMetadataPool.unlock(tableToken);
        tableMetadataPool.unlock(tableToken);
    }

    private void validNameOrThrow(CharSequence tableName) {
        if (!TableUtils.isValidTableName(tableName, configuration.getMaxFileNameLength())) {
            throw CairoException.nonCritical()
                    .put("invalid table name [table=").putAsPrintable(tableName)
                    .put(']');
        }
    }

    private TableMetadata validateDesiredMetadataVersion(TableToken tableToken, TableMetadata metadata, long desiredVersion) {
        if (desiredVersion != TableUtils.ANY_TABLE_VERSION && metadata.getMetadataVersion() != desiredVersion) {
            final TableReferenceOutOfDateException ex = TableReferenceOutOfDateException.of(
                    tableToken,
                    tableToken.getTableId(),
                    metadata.getTableId(),
                    desiredVersion,
                    metadata.getMetadataVersion()
            );
            metadata.close();
            throw ex;
        }
        return metadata;
    }

    @NotNull
    private TableToken verifyTableNameForRead(CharSequence tableName) {
        TableToken token = getTableTokenIfExists(tableName);
        if (token == null || token == TableNameRegistry.LOCKED_TOKEN) {
            throw CairoException.tableDoesNotExist(tableName);
        }
        return token;
    }

    protected TableFlagResolver newTableFlagResolver(CairoConfiguration configuration) {
        return new TableFlagResolverImpl(configuration.getSystemTableNamePrefix().toString());
    }

    private class EngineMaintenanceJob extends SynchronizedJob {

        private final long checkInterval;
        private final MicrosecondClock clock;
        private long last = 0;

        public EngineMaintenanceJob(CairoConfiguration configuration) {
            this.clock = configuration.getMicrosecondClock();
            this.checkInterval = configuration.getIdleCheckInterval() * 1000;
        }

        @Override
        protected boolean runSerially() {
            long t = clock.getTicks();
            if (last + checkInterval < t) {
                last = t;
                return releaseInactive();
            }
            return false;
        }
    }

}
