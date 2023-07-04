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

package io.questdb.cairo;

import io.questdb.MessageBus;
import io.questdb.MessageBusImpl;
import io.questdb.Metrics;
import io.questdb.Telemetry;
import io.questdb.cairo.mig.EngineMigration;
import io.questdb.cairo.pool.*;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.WalListener;
import io.questdb.cairo.wal.WalReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cutlass.text.CopyContext;
import io.questdb.griffin.DatabaseSnapshotAgent;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.Sequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
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
import java.util.concurrent.atomic.AtomicLong;

public class CairoEngine implements Closeable, WriterSource {
    public static final String BUSY_READER = "busyReader";
    private static final Log LOG = LogFactory.getLog(CairoEngine.class);
    private final AtomicLong asyncCommandCorrelationId = new AtomicLong();
    private final CairoConfiguration configuration;
    private final CopyContext copyContext;
    private final EngineMaintenanceJob engineMaintenanceJob;
    private final FunctionFactoryCache ffCache;
    private final MessageBusImpl messageBus;
    private final MetadataPool metadataPool;
    private final Metrics metrics;
    private final ReaderPool readerPool;
    private final IDGenerator tableIdGenerator;
    private final TableNameRegistry tableNameRegistry;
    private final TableSequencerAPI tableSequencerAPI;
    private final Telemetry<TelemetryTask> telemetry;
    private final Telemetry<TelemetryWalTask> telemetryWal;
    // initial value of unpublishedWalTxnCount is 1 because we want to scan for non-applied WAL transactions on startup
    private final AtomicLong unpublishedWalTxnCount = new AtomicLong(1);
    private final WalWriterPool walWriterPool;
    private final WriterPool writerPool;
    private @NotNull WalListener walListener;

    // Kept for embedded API purposes. The second constructor (the one with metrics)
    // should be preferred for internal use.
    public CairoEngine(CairoConfiguration configuration) {
        this(configuration, Metrics.disabled());
    }

    public CairoEngine(CairoConfiguration configuration, Metrics metrics) {
        ffCache = new FunctionFactoryCache(
                configuration,
                ServiceLoader.load(FunctionFactory.class, FunctionFactory.class.getClassLoader())
        );
        this.configuration = configuration;
        this.copyContext = new CopyContext(configuration);
        this.metrics = metrics;
        this.tableSequencerAPI = new TableSequencerAPI(this, configuration);
        this.messageBus = new MessageBusImpl(configuration);
        this.writerPool = new WriterPool(configuration, messageBus, metrics);
        this.readerPool = new ReaderPool(configuration, messageBus);
        this.metadataPool = new MetadataPool(configuration, this);
        this.walWriterPool = new WalWriterPool(configuration, this);
        this.engineMaintenanceJob = new EngineMaintenanceJob(configuration);
        this.telemetry = new Telemetry<>(TelemetryTask.TELEMETRY, configuration);
        this.telemetryWal = new Telemetry<>(TelemetryWalTask.WAL_TELEMETRY, configuration);
        this.tableIdGenerator = new IDGenerator(configuration, TableUtils.TAB_INDEX_FILE_NAME);
        this.walListener = WalListener.DEFAULT;
        try {
            this.tableIdGenerator.open();
        } catch (Throwable e) {
            close();
            throw e;
        }
        // Recover snapshot, if necessary.
        try {
            DatabaseSnapshotAgent.recoverSnapshot(this);
        } catch (Throwable e) {
            close();
            throw e;
        }
        // Migrate database files.
        try {
            EngineMigration.migrateEngineTo(this, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, false);
        } catch (Throwable e) {
            close();
            throw e;
        }

        // Convert tables to WAL/non-WAL, if necessary.
        final ObjList<TableToken> convertedTables;
        try {
            convertedTables = TableConverter.convertTables(configuration, tableSequencerAPI);
        } catch (Throwable e) {
            close();
            throw e;
        }

        try {
            tableNameRegistry = configuration.isReadOnlyInstance() ?
                    new TableNameRegistryRO(configuration) : new TableNameRegistryRW(configuration);
            tableNameRegistry.reloadTableNameCache(convertedTables);
        } catch (Throwable e) {
            close();
            throw e;
        }

        if (convertedTables != null) {
            for (int i = 0, n = convertedTables.size(); i < n; i++) {
                final TableToken token = convertedTables.get(i);
                try (TableWriter writer = getWriter(token, "tableTypeConversion")) {
                    writer.commitSeqTxn(0);
                }
            }
        }
    }

    public void applyTableRename(TableToken token, TableToken updatedTableToken) {
        tableNameRegistry.rename(token.getTableName(), updatedTableToken.getTableName(), token);
    }

    @TestOnly
    public boolean clear() {
        boolean b1 = readerPool.releaseAll();
        boolean b2 = writerPool.releaseAll();
        boolean b3 = tableSequencerAPI.releaseAll();
        boolean b4 = metadataPool.releaseAll();
        boolean b5 = walWriterPool.releaseAll();
        messageBus.reset();
        return b1 & b2 & b3 & b4 & b5;
    }

    @Override
    public void close() {
        Misc.free(writerPool);
        Misc.free(readerPool);
        Misc.free(metadataPool);
        Misc.free(walWriterPool);
        Misc.free(tableIdGenerator);
        Misc.free(messageBus);
        Misc.free(tableSequencerAPI);
        Misc.free(telemetry);
        Misc.free(telemetryWal);
        Misc.free(tableNameRegistry);
    }

    @TestOnly
    public void closeNameRegistry() {
        tableNameRegistry.close();
    }

    public @NotNull TableToken createTable(
            SecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            boolean ifNotExists,
            TableStructure struct,
            boolean keepLock
    ) {
        return createTable(securityContext, mem, path, ifNotExists, struct, keepLock, false);
    }

    public @NotNull TableToken createTableInVolume(
            SecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            boolean ifNotExists,
            TableStructure struct,
            boolean keepLock
    ) {
        return createTable(securityContext, mem, path, ifNotExists, struct, keepLock, true);
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
            CharSequence lockedReason = lock(tableToken, "removeTable");
            if (lockedReason == null) {
                try {
                    path.of(configuration.getRoot()).concat(tableToken).$();
                    int errno;
                    if ((errno = configuration.getFilesFacade().unlinkOrRemove(path, LOG)) != 0) {
                        throw CairoException.critical(errno).put("could not remove table [name=").put(tableToken)
                                .put(", dirName=").put(tableToken.getDirName()).put(']');
                    }
                } finally {
                    unlockTableUnsafe(tableToken, null, false);
                }

                tableNameRegistry.dropTable(tableToken);
                return;
            }
            throw CairoException.nonCritical().put("Could not lock '").put(tableToken).put("' [reason='").put(lockedReason).put("']");
        }
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
                Metrics.disabled()
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

    public long getCommandCorrelationId() {
        return asyncCommandCorrelationId.incrementAndGet();
    }

    public CairoConfiguration getConfiguration() {
        return configuration;
    }

    public CopyContext getCopyContext() {
        return copyContext;
    }

    public Job getEngineMaintenanceJob() {
        return engineMaintenanceJob;
    }

    public FunctionFactoryCache getFunctionFactoryCache() {
        return ffCache;
    }

    public MessageBus getMessageBus() {
        return messageBus;
    }

    public TableRecordMetadata getMetadata(TableToken tableToken) {
        verifyTableToken(tableToken);
        try {
            return metadataPool.get(tableToken);
        } catch (CairoException e) {
            tryRepairTable(tableToken, e);
        }
        return metadataPool.get(tableToken);
    }

    public TableRecordMetadata getMetadata(TableToken tableToken, long metadataVersion) {
        verifyTableToken(tableToken);
        try {
            final TableRecordMetadata metadata = metadataPool.get(tableToken);
            if (metadataVersion != TableUtils.ANY_TABLE_VERSION && metadata.getMetadataVersion() != metadataVersion) {
                final TableReferenceOutOfDateException ex = TableReferenceOutOfDateException.of(tableToken, metadata.getTableId(), metadata.getTableId(), metadataVersion, metadata.getMetadataVersion());
                metadata.close();
                throw ex;
            }
            return metadata;
        } catch (CairoException e) {
            tryRepairTable(tableToken, e);
        }
        return metadataPool.get(tableToken);
    }

    public Metrics getMetrics() {
        return metrics;
    }

    @TestOnly
    public PoolListener getPoolListener() {
        return this.writerPool.getPoolListener();
    }

    public TableReader getReader(CharSequence tableName) {
        return getReader(verifyTableNameForRead(tableName));
    }

    public TableReader getReader(TableToken tableToken) {
        verifyTableToken(tableToken);
        return readerPool.get(tableToken);
    }

    public TableReader getReader(TableToken tableToken, long version) {
        verifyTableToken(tableToken);
        final int tableId = tableToken.getTableId();
        TableReader reader = readerPool.get(tableToken);
        if ((version > -1 && reader.getVersion() != version)
                || tableId > -1 && reader.getMetadata().getTableId() != tableId) {
            TableReferenceOutOfDateException ex = TableReferenceOutOfDateException.of(tableToken, tableId, reader.getMetadata().getTableId(), version, reader.getVersion());
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
                    .$(", error=").$(e.getMessage()).I$();
            throw e;
        }
    }

    public IDGenerator getTableIdGenerator() {
        return tableIdGenerator;
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

    public int getTableStatus(CharSequence tableName) {
        TableToken tableToken = getTableTokenIfExists(tableName);
        if (tableToken == null) {
            return TableUtils.TABLE_DOES_NOT_EXIST;
        }
        return getTableStatus(Path.getThreadLocal(configuration.getRoot()), tableToken);
    }

    public TableToken getTableTokenByDirName(String dirName) {
        return tableNameRegistry.getTableTokenByDirName(dirName);
    }

    public int getTableTokenCount(boolean includeDropped) {
        return tableNameRegistry.getTableTokenCount(includeDropped);
    }

    public TableToken getTableTokenIfExists(CharSequence tableName) {
        return tableNameRegistry.getTableToken(tableName);
    }

    public TableToken getTableTokenIfExists(CharSequence tableName, int lo, int hi) {
        StringSink sink = Misc.getThreadLocalBuilder();
        sink.put(tableName, lo, hi);
        return tableNameRegistry.getTableToken(sink);
    }

    public void getTableTokens(ObjHashSet<TableToken> bucket, boolean includeDropped) {
        tableNameRegistry.getTableTokens(bucket, includeDropped);
    }

    @Override
    public TableWriterAPI getTableWriterAPI(TableToken tableToken, @Nullable String lockReason) {
        verifyTableToken(tableToken);
        if (!tableToken.isWal()) {
            return writerPool.get(tableToken, lockReason);
        }
        return walWriterPool.get(tableToken);
    }

    @Override
    public TableWriterAPI getTableWriterAPI(CharSequence tableName, String lockReason) {
        return getTableWriterAPI(verifyTableNameForRead(tableName), lockReason);
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

        throw CairoException.nonCritical().put("WAL reader is not supported for table ").put(tableToken);
    }

    @TestOnly
    public @NotNull WalWriter getWalWriter(TableToken tableToken) {
        verifyTableToken(tableToken);
        return walWriterPool.get(tableToken);
    }

    public TableWriter getWriter(TableToken tableToken, String lockReason) {
        verifyTableToken(tableToken);
        return writerPool.get(tableToken, lockReason);
    }

    public TableWriter getWriterOrPublishCommand(TableToken tableToken, @NotNull AsyncWriterCommand asyncWriterCommand) {
        verifyTableToken(tableToken);
        return writerPool.getWriterOrPublishCommand(tableToken, asyncWriterCommand.getCommandName(), asyncWriterCommand);
    }

    public TableWriter getWriterUnsafe(TableToken tableToken, String lockReason) {
        return writerPool.get(tableToken, lockReason);
    }

    public boolean isTableDropped(TableToken tableToken) {
        return tableNameRegistry.isTableDropped(tableToken);
    }

    public boolean isWalTable(TableToken tableToken) {
        return tableToken.isWal();
    }

    public String lock(TableToken tableToken, String lockReason) {
        assert null != lockReason;
        // busy metadata is same as busy reader from user perspective
        String lockedReason = BUSY_READER;
        if (metadataPool.lock(tableToken)) {
            lockedReason = writerPool.lock(tableToken, lockReason);
            if (lockedReason == null) {
                // not locked
                if (readerPool.lock(tableToken)) {
                    LOG.info().$("locked [table=`").utf8(tableToken.getDirName()).$("`, thread=").$(Thread.currentThread().getId()).I$();
                    return null;
                }
                writerPool.unlock(tableToken);
                lockedReason = BUSY_READER;
            }
            metadataPool.unlock(tableToken);
        }
        return lockedReason;
    }

    public boolean lockReaders(TableToken tableToken) {
        verifyTableToken(tableToken);
        return readerPool.lock(tableToken);
    }

    public boolean lockReadersByTableToken(TableToken tableToken) {
        return readerPool.lock(tableToken);
    }

    public TableToken lockTableName(CharSequence tableName, boolean isWal) {
        validNameOrThrow(tableName);
        int tableId = (int) getTableIdGenerator().getNextId();
        return lockTableName(tableName, tableId, isWal);
    }

    @Nullable
    public TableToken lockTableName(CharSequence tableName, int tableId, boolean isWal) {
        String tableNameStr = Chars.toString(tableName);
        final String dirName = TableUtils.getTableDir(configuration.mangleTableDirNames(), tableNameStr, tableId, isWal);
        return tableNameRegistry.lockTableName(tableNameStr, dirName, tableId, isWal);
    }

    public void notifyDropped(TableToken tableToken) {
        tableNameRegistry.dropTable(tableToken);
    }

    public void notifyWalTxnCommitted(TableToken tableToken, long txn) {
        final Sequence pubSeq = messageBus.getWalTxnNotificationPubSequence();
        while (true) {
            long cursor = pubSeq.next();
            if (cursor > -1L) {
                WalTxnNotificationTask task = messageBus.getWalTxnNotificationQueue().get(cursor);
                task.of(tableToken, txn);
                pubSeq.done(cursor);
                return;
            } else if (cursor == -1L) {
                LOG.debug().$("cannot publish WAL notifications, queue is full [current=")
                        .$(pubSeq.current()).$(", table=").utf8(tableToken.getDirName())
                        .I$();
                // queue overflow, throw away notification and notify a job to rescan all tables
                notifyWalTxnRepublisher();
                return;
            }
        }
    }

    public void notifyWalTxnRepublisher() {
        unpublishedWalTxnCount.incrementAndGet();
    }

    public void registerTableToken(TableToken tableToken) {
        tableNameRegistry.registerName(tableToken);
    }

    @TestOnly
    public boolean releaseAllReaders() {
        boolean b1 = metadataPool.releaseAll();
        return readerPool.releaseAll() & b1;
    }

    @TestOnly
    public void releaseAllWriters() {
        writerPool.releaseAll();
    }

    public boolean releaseInactive() {
        boolean useful = writerPool.releaseInactive();
        useful |= readerPool.releaseInactive();
        useful |= tableSequencerAPI.releaseInactive();
        useful |= metadataPool.releaseInactive();
        useful |= walWriterPool.releaseInactive();
        return useful;
    }

    @TestOnly
    public void releaseInactiveTableSequencers() {
        walWriterPool.releaseInactive();
        tableSequencerAPI.releaseInactive();
    }

    public void releaseReadersByTableToken(TableToken tableToken) {
        readerPool.unlock(tableToken);
    }

    @TestOnly
    public void reloadTableNames() {
        reloadTableNames(null);
    }

    @TestOnly
    public void reloadTableNames(ObjList<TableToken> convertedTables) {
        tableNameRegistry.reloadTableNameCache(convertedTables);
    }

    public void removeTableToken(TableToken tableToken) {
        tableNameRegistry.purgeToken(tableToken);
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
        final TableToken toTableToken;
        if (fromTableToken != null) {
            if (fromTableToken.isWal()) {
                String toTableNameStr = Chars.toString(toTableName);
                toTableToken = tableNameRegistry.addTableAlias(toTableNameStr, fromTableToken);
                if (toTableToken != null) {
                    boolean renamed = false;
                    try {
                        try (WalWriter walWriter = getWalWriter(fromTableToken)) {
                            long seqTxn = walWriter.renameTable(fromTableName, toTableNameStr);
                            LOG.info().$("renamed table [from='").utf8(fromTableName)
                                    .$("', to='").utf8(toTableName)
                                    .$("', wal=").$(walWriter.getWalId())
                                    .$("', seqTxn=").$(seqTxn)
                                    .I$();
                            renamed = true;
                        }
                        TableUtils.overwriteTableNameFile(
                                fromPath.of(configuration.getRoot()).concat(toTableToken),
                                memory, configuration.getFilesFacade(),
                                toTableToken.getTableName()
                        );
                    } finally {
                        if (renamed) {
                            tableNameRegistry.replaceAlias(fromTableToken, toTableToken);
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
                            .put("cannot rename table, new name is already in use [table=").put(fromTableName)
                            .put(", toTableName=").put(toTableName)
                            .put(']');
                }
            } else {
                String lockedReason = lock(fromTableToken, "renameTable");
                if (null == lockedReason) {
                    try {
                        toTableToken = rename0(fromPath, fromTableToken, toPath, toTableName);
                        TableUtils.overwriteTableNameFile(
                                fromPath.of(configuration.getRoot()).concat(toTableToken),
                                memory,
                                configuration.getFilesFacade(),
                                toTableToken.getTableName()
                        );
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

    @TestOnly
    public void setPoolListener(PoolListener poolListener) {
        this.metadataPool.setPoolListener(poolListener);
        this.writerPool.setPoolListener(poolListener);
        this.readerPool.setPoolListener(poolListener);
        this.walWriterPool.setPoolListener(poolListener);
    }

    @TestOnly
    public void setReaderListener(ReaderPool.ReaderListener readerListener) {
        readerPool.setTableReaderListener(readerListener);
    }

    public void setWalListener(@NotNull WalListener walListener) {
        this.walListener = walListener;
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

    public void unlockTableName(TableToken tableToken) {
        tableNameRegistry.unlockTableName(tableToken);
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
        StringSink sink = Misc.getThreadLocalBuilder();
        sink.put(tableName, lo, hi);
        return verifyTableName(sink);
    }

    public void verifyTableToken(TableToken tableToken) {
        TableToken tt = tableNameRegistry.getTableToken(tableToken.getTableName());
        if (tt == null) {
            throw CairoException.tableDoesNotExist(tableToken.getTableName());
        }
        if (!tt.equals(tableToken)) {
            throw TableReferenceOutOfDateException.of(tableToken, tableToken.getTableId(), tt.getTableId(), tt.getTableId(), -1);
        }
    }

    private @NotNull TableToken createTable(
            SecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            boolean ifNotExists,
            TableStructure struct,
            boolean keepLock,
            boolean inVolume
    ) {
        assert !struct.isWalEnabled() || PartitionBy.isPartitioned(struct.getPartitionBy()) : "WAL is only supported for partitioned tables";
        securityContext.authorizeTableCreate();
        final CharSequence tableName = struct.getTableName();
        validNameOrThrow(tableName);

        int tableId = (int) tableIdGenerator.getNextId();
        TableToken tableToken = lockTableName(tableName, tableId, struct.isWalEnabled());
        if (tableToken == null) {
            if (ifNotExists) {
                return getTableTokenIfExists(tableName);
            }
            throw EntryUnavailableException.instance("table exists");
        }

        try {
            String lockedReason = lock(tableToken, "createTable");
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
        }

        securityContext.onTableCreated(tableToken);
        return tableToken;
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

        if (ff.exists(toPath.of(root).concat(toTableToken).$())) {
            tableNameRegistry.unlockTableName(toTableToken);
        }

        try {
            if (ff.rename(fromPath, toPath) != Files.FILES_RENAME_OK) {
                final int error = ff.errno();
                LOG.error()
                        .$("could not rename [from='").utf8(fromPath)
                        .$("', to='").utf8(toPath)
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
        }
    }

    private void tryRepairTable(TableToken tableToken, RuntimeException rethrow) {
        try {
            writerPool.get(tableToken, "repair").close();
        } catch (EntryUnavailableException e) {
            // This is fine, writer is busy. Throw back origin error.
            throw rethrow;
        } catch (Throwable th) {
            LOG.critical()
                    .$("could not repair before reading [dirName=").utf8(tableToken.getDirName())
                    .$(" ,error=").$(th.getMessage()).I$();
            throw rethrow;
        }
    }

    private void unlockTableUnsafe(TableToken tableToken, TableWriter writer, boolean newTable) {
        readerPool.unlock(tableToken);
        writerPool.unlock(tableToken, writer, newTable);
        metadataPool.unlock(tableToken);
    }

    private void validNameOrThrow(CharSequence tableName) {
        if (!TableUtils.isValidTableName(tableName, configuration.getMaxFileNameLength())) {
            throw CairoException.nonCritical()
                    .put("invalid table name [table=").putAsPrintable(tableName)
                    .put(']');
        }
    }

    @NotNull
    private TableToken verifyTableNameForRead(CharSequence tableName) {
        TableToken token = getTableTokenIfExists(tableName);
        if (token == null || token == TableNameRegistry.LOCKED_TOKEN) {
            throw CairoException.tableDoesNotExist(tableName);
        }
        return token;
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
