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

package io.questdb.cairo;

import io.questdb.MessageBus;
import io.questdb.MessageBusImpl;
import io.questdb.Metrics;
import io.questdb.cairo.mig.EngineMigration;
import io.questdb.cairo.pool.*;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.sql.ReaderOutOfDateException;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.WalReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cutlass.text.TextImportExecutionContext;
import io.questdb.griffin.DatabaseSnapshotAgent;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.tasks.TelemetryTask;
import io.questdb.tasks.WalTxnNotificationTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CairoEngine implements Closeable, WriterSource {
    public static final String BUSY_READER = "busyReader";
    private static final Log LOG = LogFactory.getLog(CairoEngine.class);
    private final AtomicLong asyncCommandCorrelationId = new AtomicLong();
    private final CairoConfiguration configuration;
    private final EngineMaintenanceJob engineMaintenanceJob;
    private final MessageBusImpl messageBus;
    private final MetadataPool metadataPool;
    private final Metrics metrics;
    private final ReaderPool readerPool;
    private final IDGenerator tableIdGenerator;
    private final TableNameRegistry tableNameRegistry;
    private final TableSequencerAPI tableSequencerAPI;
    private final MPSequence telemetryPubSeq;
    private final RingQueue<TelemetryTask> telemetryQueue;
    private final SCSequence telemetrySubSeq;
    private final TextImportExecutionContext textImportExecutionContext;
    // initial value of unpublishedWalTxnCount is 1 because we want to scan for unapplied WAL transactions on startup
    private final AtomicLong unpublishedWalTxnCount = new AtomicLong(1);
    private final WalWriterPool walWriterPool;
    private final WriterPool writerPool;


    // Kept for embedded API purposes. The second constructor (the one with metrics)
    // should be preferred for internal use.
    public CairoEngine(CairoConfiguration configuration) {
        this(configuration, Metrics.disabled());
    }

    public CairoEngine(CairoConfiguration configuration, Metrics metrics) {
        this.configuration = configuration;
        this.textImportExecutionContext = new TextImportExecutionContext(configuration);
        this.metrics = metrics;
        this.tableSequencerAPI = new TableSequencerAPI(this, configuration);
        this.messageBus = new MessageBusImpl(configuration);
        this.writerPool = new WriterPool(this.getConfiguration(), this.getMessageBus(), metrics);
        this.readerPool = new ReaderPool(configuration, messageBus);
        this.metadataPool = new MetadataPool(configuration, this);
        this.walWriterPool = new WalWriterPool(configuration, this);
        this.engineMaintenanceJob = new EngineMaintenanceJob(configuration);
        if (configuration.getTelemetryConfiguration().getEnabled()) {
            this.telemetryQueue = new RingQueue<>(TelemetryTask::new, configuration.getTelemetryConfiguration().getQueueCapacity());
            this.telemetryPubSeq = new MPSequence(telemetryQueue.getCycle());
            this.telemetrySubSeq = new SCSequence();
            telemetryPubSeq.then(telemetrySubSeq).then(telemetryPubSeq);
        } else {
            this.telemetryQueue = null;
            this.telemetryPubSeq = null;
            this.telemetrySubSeq = null;
        }
        tableIdGenerator = new IDGenerator(configuration, TableUtils.TAB_INDEX_FILE_NAME);
        try {
            tableIdGenerator.open();
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
            EngineMigration.migrateEngineTo(this, ColumnType.VERSION, false);
        } catch (Throwable e) {
            close();
            throw e;
        }

        try {
            this.tableNameRegistry = configuration.isReadOnlyInstance() ?
                    new TableNameRegistryRO(configuration) : new TableNameRegistryRW(configuration);
            this.tableNameRegistry.reloadTableNameCache();
        } catch (Throwable e) {
            close();
            throw e;
        }
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
        Misc.free(telemetryQueue);
        Misc.free(tableNameRegistry);
    }

    @TestOnly
    public void closeNameRegistry() {
        tableNameRegistry.close();
    }

    public void createTable(
            CairoSecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            boolean ifNotExists,
            TableStructure struct,
            boolean keepLock
    ) {
        securityContext.checkWritePermission();
        CharSequence tableName = struct.getTableName();
        validNameOrThrow(tableName);

        int tableId = (int) tableIdGenerator.getNextId();
        TableToken tableToken = lockTableName(tableName, tableId, struct.isWalEnabled());
        if (tableToken == null) {
            if (ifNotExists) {
                return;
            }
            throw EntryUnavailableException.instance("table exists");
        }

        try {
            String lockedReason = lock(securityContext, tableToken, "createTable");
            if (null == lockedReason) {
                boolean newTable = false;
                try {
                    int status = getStatus(securityContext, path, tableToken);
                    if (status == TableUtils.TABLE_RESERVED) {
                        throw CairoException.nonCritical().put("name is reserved [table=").put(tableName).put(']');
                    }
                    createTableUnsafe(
                            securityContext,
                            mem,
                            path,
                            struct,
                            tableToken,
                            tableId
                    );
                    newTable = true;
                    tableNameRegistry.registerName(tableToken);
                } finally {
                    if (!keepLock) {
                        readerPool.unlock(tableToken);
                        writerPool.unlock(tableToken, null, newTable);
                        metadataPool.unlock(tableToken);
                        LOG.info().$("unlocked [privateTableName=`").utf8(tableToken.getDirName()).$("`]").$();
                    }
                }
            } else {
                if (!ifNotExists) {
                    throw EntryUnavailableException.instance(lockedReason);
                }
            }
        } catch (Throwable th) {
            if (struct.isWalEnabled()) {
                // tableToken.getLoggingName() === tableName, table cannot be renamed while creation hasn't finished
                tableSequencerAPI.dropTable(tableToken.getTableName(), tableToken, true);
            }
            throw th;
        } finally {
            tableNameRegistry.unlockTableName(tableToken);
        }
    }

    // caller has to acquire the lock before this method is called and release the lock after the call
    public void createTableUnsafe(
            CairoSecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            TableStructure struct,
            TableToken tableToken,
            int tableId
    ) {
        securityContext.checkWritePermission();

        // only create the table after it has been registered
        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence root = configuration.getRoot();
        final int mkDirMode = configuration.getMkDirMode();
        TableUtils.createTable(
                ff,
                root,
                mkDirMode,
                mem,
                path,
                tableToken.getDirName(),
                struct,
                ColumnType.VERSION,
                tableId
        );
        if (struct.isWalEnabled()) {
            tableSequencerAPI.registerTable(tableId, struct, tableToken);
        }
    }

    public void drop(
            CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName
    ) {
        securityContext.checkWritePermission();
        validNameOrThrow(tableName);
        TableToken tableToken = tableNameRegistry.getTableToken(tableName);
        if (tableToken == null || tableToken == TableNameRegistry.LOCKED_TOKEN) {
            throw CairoException.nonCritical().put("table does not exist [table=").put(tableName).put("]");
        }

        if (tableToken.isWal()) {
            if (tableNameRegistry.dropTable(tableName, tableToken)) {
                tableSequencerAPI.dropTable(tableName, tableToken, false);
            }
            // todo: log that this drop table was unsuccessful, e.g. someone else beat us to it
        } else {
            CharSequence lockedReason = lock(securityContext, tableToken, "removeTable");
            if (null == lockedReason) {
                try {
                    path.of(configuration.getRoot()).concat(tableToken).$();
                    int errno;
                    if ((errno = configuration.getFilesFacade().rmdir(path)) != 0) {
                        LOG.error().$("drop failed [tableName='").utf8(tableName).$("', error=").$(errno).$(']').$();
                        throw CairoException.critical(errno).put("could not remove table [name=").put(tableName)
                                .put(", privateTableName=").put(tableToken.getDirName()).put(']');
                    }
                } finally {
                    readerPool.unlock(tableToken);
                    writerPool.unlock(tableToken, null, false);
                    metadataPool.unlock(tableToken);
                }

                tableNameRegistry.dropTable(tableName, tableToken);
                return;
            }
            throw CairoException.nonCritical().put("Could not lock '").put(tableName).put("' [reason='").put(lockedReason).put("']");
        }
    }

    public TableWriter getBackupWriter(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            CharSequence backupDirName
    ) {
        securityContext.checkWritePermission();
        // There is no point in pooling/caching these writers since they are only used once, backups are not incremental
        TableToken tableToken = getTableToken(tableName);
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

    public Job getEngineMaintenanceJob() {
        return engineMaintenanceJob;
    }

    public MessageBus getMessageBus() {
        return messageBus;
    }

    public TableRecordMetadata getMetadata(CairoSecurityContext securityContext, TableToken tableToken) {
        try {
            return metadataPool.get(tableToken);
        } catch (CairoException e) {
            tryRepairTable(securityContext, tableToken, e);
        }
        return metadataPool.get(tableToken);
    }

    public TableRecordMetadata getMetadata(CairoSecurityContext securityContext, TableToken tableToken, long structureVersion) {
        try {
            final TableRecordMetadata metadata = metadataPool.get(tableToken);
            if (structureVersion != TableUtils.ANY_TABLE_VERSION && metadata.getStructureVersion() != structureVersion) {
                // rename to StructureVersionException?
                final ReaderOutOfDateException ex = ReaderOutOfDateException.of(tableToken.getTableName(), metadata.getTableId(), metadata.getTableId(), structureVersion, metadata.getStructureVersion());
                metadata.close();
                throw ex;
            }
            return metadata;
        } catch (CairoException e) {
            tryRepairTable(securityContext, tableToken, e);
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

    public TableReader getReader(CairoSecurityContext securityContext, CharSequence tableName) {
        return getReader(securityContext, tableName, TableUtils.ANY_TABLE_ID, TableUtils.ANY_TABLE_VERSION);
    }

    public TableReader getReader(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            int tableId,
            long version
    ) {
        validNameOrThrow(tableName);
        TableToken tableToken = getTableToken(tableName);
        TableReader reader = readerPool.get(tableToken);
        if ((version > -1 && reader.getVersion() != version)
                || tableId > -1 && reader.getMetadata().getTableId() != tableId) {
            ReaderOutOfDateException ex = ReaderOutOfDateException.of(tableName, tableId, reader.getMetadata().getTableId(), version, reader.getVersion());
            reader.close();
            throw ex;
        }
        return reader;
    }

    public TableReader getReaderByTableToken(@SuppressWarnings("unused") CairoSecurityContext securityContext, TableToken tableToken) {
        return readerPool.get(tableToken);
    }

    public Map<TableToken, AbstractMultiTenantPool.Entry<ReaderPool.R>> getReaderPoolEntries() {
        return readerPool.entries();
    }

    public TableReader getReaderWithRepair(CairoSecurityContext securityContext, CharSequence tableName) {

        validNameOrThrow(tableName);

        try {
            return getReader(securityContext, tableName);
        } catch (CairoException e) {
            // Cannot open reader on existing table is pretty bad.
            // In some messed states, for example after _meta file swap failure Reader cannot be opened
            // but writer can be. Opening writer fixes the table mess.
            TableToken tableToken = getTableToken(tableName);
            tryRepairTable(securityContext, tableToken, e);
        }
        try {
            return getReader(securityContext, tableName);
        } catch (CairoException e) {
            LOG.critical()
                    .$("could not open reader [table=").$(tableName)
                    .$(", errno=").$(e.getErrno())
                    .$(", error=").$(e.getMessage()).I$();
            throw e;
        }
    }

    public int getStatus(
            @SuppressWarnings("unused") CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName,
            int lo,
            int hi
    ) {
        StringSink sink = Misc.getThreadLocalBuilder();
        sink.put(tableName, lo, hi);
        return getStatus(securityContext, path, sink);
    }

    public int getStatus(
            @SuppressWarnings("unused") CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName
    ) {
        TableToken tableToken = tableNameRegistry.getTableToken(tableName);
        return getStatus(securityContext, path, tableToken);
    }

    public int getStatus(
            CairoSecurityContext securityContext,
            Path path,
            TableToken tableToken
    ) {
        if (tableToken == null) {
            return TableUtils.TABLE_DOES_NOT_EXIST;
        }
        if (tableToken == TableNameRegistry.LOCKED_TOKEN) {
            return TableUtils.TABLE_RESERVED;
        }
        return TableUtils.exists(configuration.getFilesFacade(), path, configuration.getRoot(), tableToken.getDirName());
    }

    public IDGenerator getTableIdGenerator() {
        return tableIdGenerator;
    }

    public String getTableNameAsString(CharSequence tableName) {
        // In Table Name Registry TableToken has up to date tableName === TableToken.getLoggingName(),
        // even after rename, Table Name Registry values are updated, that's why it's safe to use
        // TableToken.getLoggingName() as table name here.
        return getTableToken(tableName).getTableName();
    }

    public String getTableName(TableToken token) {
        return tableNameRegistry.getTableName(token);
    }

    public TableSequencerAPI getTableSequencerAPI() {
        return tableSequencerAPI;
    }

    public TableToken getTableToken(final CharSequence tableName) {
        TableToken tableToken = tableNameRegistry.getTableToken(tableName);
        if (tableToken == null) {
            throw CairoException.nonCritical().put("table does not exist [table=").put(tableName).put("]");
        }
        if (tableToken == TableNameRegistry.LOCKED_TOKEN) {
            throw CairoException.nonCritical().put("table name is reserved [table=").put(tableName).put("]");
        }
        return tableToken;
    }

    public TableToken getTableTokenByPrivateTableName(String tableName, int tableId) {
        return tableNameRegistry.getTableToken(tableName, tableId);
    }

    public Iterable<TableToken> getTableTokens() {
        return tableNameRegistry.getTableTokens();
    }

    @Override
    public TableWriterAPI getTableWriterAPI(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            @Nullable String lockReason
    ) {
        securityContext.checkWritePermission();
        validNameOrThrow(tableName);
        TableToken tableNameRecord = getTableToken(tableName);
        if (tableNameRecord == null) {
            throw CairoException.nonCritical().put("table does not exist [table=").put(tableName).put("]");
        }

        if (!tableNameRecord.isWal()) {
            return writerPool.get(tableNameRecord, lockReason);

        }
        return walWriterPool.get(tableNameRecord);
    }

    public Sequence getTelemetryPubSequence() {
        return telemetryPubSeq;
    }

    public RingQueue<TelemetryTask> getTelemetryQueue() {
        return telemetryQueue;
    }

    public SCSequence getTelemetrySubSequence() {
        return telemetrySubSeq;
    }

    public TextImportExecutionContext getTextImportExecutionContext() {
        return textImportExecutionContext;
    }

    public long getUnpublishedWalTxnCount() {
        return unpublishedWalTxnCount.get();
    }

    // For testing only
    @TestOnly
    public WalReader getWalReader(
            @SuppressWarnings("unused") CairoSecurityContext securityContext,
            CharSequence tableName,
            CharSequence walName,
            int segmentId,
            long walRowCount
    ) {
        TableToken tableNameRecord = getTableToken(tableName);
        if (tableNameRecord != null && tableNameRecord.isWal()) {
            // This is WAL table because sequencer exists
            return new WalReader(configuration, tableNameRecord, walName, segmentId, walRowCount);
        }

        throw CairoException.nonCritical().put("WAL reader is not supported for table ").put(tableName);
    }

    @TestOnly
    public @NotNull WalWriter getWalWriter(CairoSecurityContext securityContext, CharSequence tableName) {
        securityContext.checkWritePermission();
        return walWriterPool.get(getTableToken(tableName));
    }

    public TableWriter getWriter(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            String lockReason
    ) {
        securityContext.checkWritePermission();
        validNameOrThrow(tableName);
        TableToken tableToken = getTableToken(tableName);
        return writerPool.get(tableToken, lockReason);
    }

    public TableWriter getWriterByTableToken(
            CairoSecurityContext securityContext,
            TableToken tableToken,
            String lockReason
    ) {
        securityContext.checkWritePermission();
        return writerPool.get(tableToken, lockReason);
    }

    public TableWriter getWriterOrPublishCommand(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            @NotNull AsyncWriterCommand asyncWriterCommand
    ) {
        securityContext.checkWritePermission();
        validNameOrThrow(tableName);
        TableToken tableToken = getTableToken(tableName);
        return writerPool.getWriterOrPublishCommand(tableToken, asyncWriterCommand.getCommandName(), asyncWriterCommand);
    }

    public boolean isLiveTable(TableToken tableToken) {
        return tableToken != TableNameRegistry.LOCKED_TOKEN && !tableNameRegistry.isTableDropped(tableToken);
    }

    public boolean isTableDropped(TableToken tableToken) {
        return tableNameRegistry.isTableDropped(tableToken);
    }

    public boolean isWalTableName(CharSequence tableName) {
        TableToken tableToken = getTableToken(tableName);
        return tableToken != null && tableToken.isWal();
    }

    public String lock(
            CairoSecurityContext securityContext,
            TableToken tableToken,
            String lockReason
    ) {
        assert null != lockReason;
        securityContext.checkWritePermission();

        String lockedReason = writerPool.lock(tableToken, lockReason);
        if (lockedReason == null) { // not locked
            if (readerPool.lock(tableToken)) {
                if (metadataPool.lock(tableToken)) {
                    LOG.info().$("locked [table=`").utf8(tableToken.getDirName()).$("`, thread=").$(Thread.currentThread().getId()).I$();
                    return null;
                }
                readerPool.unlock(tableToken);
            }
            writerPool.unlock(tableToken);
            return BUSY_READER;
        }
        return lockedReason;

    }

    public boolean lockReaders(CharSequence tableName) {
        validNameOrThrow(tableName);
        return readerPool.lock(getTableToken(tableName));
    }

    public boolean lockReadersByTableToken(TableToken tableToken) {
        return readerPool.lock(tableToken);
    }

    public TableToken lockTableName(CharSequence tableName, boolean isWal) {
        int tableId = (int) getTableIdGenerator().getNextId();
        return lockTableName(tableName, tableId, isWal);
    }

    @Nullable
    public TableToken lockTableName(CharSequence tableName, int tableId, boolean isWal) {
        String publicTableName = Chars.toString(tableName);
        String privateTableName = Chars.toString(tableName);
        if (isWal) {
            privateTableName += TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
            privateTableName += tableId;
        } else if (configuration.manglePrivateTableNames()) {
            privateTableName += TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
        }
        return tableNameRegistry.lockTableName(publicTableName, privateTableName, tableId, isWal);
    }

    public CharSequence lockWriter(CairoSecurityContext securityContext, CharSequence tableName, String lockReason) {
        securityContext.checkWritePermission();
        validNameOrThrow(tableName);
        return writerPool.lock(getTableToken(tableName), lockReason);
    }

    public void notifyWalTxnCommitted(int tableId, TableToken tableToken, long txn) {
        final Sequence pubSeq = messageBus.getWalTxnNotificationPubSequence();
        while (true) {
            long cursor = pubSeq.next();
            if (cursor > -1L) {
                WalTxnNotificationTask task = messageBus.getWalTxnNotificationQueue().get(cursor);
                task.of(tableToken, tableId, txn);
                pubSeq.done(cursor);
                return;
            } else if (cursor == -1L) {
                LOG.info().$("cannot publish WAL notifications, queue is full [current=")
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

    public TableToken refreshTableToken(TableToken tableToken) {
        return tableNameRegistry.refreshTableToken(tableToken);
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
        tableNameRegistry.reloadTableNameCache();
    }

    public int removeDirectory(@Transient Path path, CharSequence dir) {
        path.of(configuration.getRoot()).concat(dir);
        final FilesFacade ff = configuration.getFilesFacade();
        return ff.rmdir(path.slash$());
    }

    public void removeTableToken(TableToken tableName) {
        tableNameRegistry.purgeToken(tableName);
    }

    public void rename(
            CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName,
            Path otherPath,
            CharSequence newName
    ) {
        securityContext.checkWritePermission();

        validNameOrThrow(tableName);
        validNameOrThrow(newName);

        TableToken tableToken = getTableToken(tableName);
        if (tableToken != null) {
            if (tableToken.isWal()) {
                TableToken newTableToken = tableNameRegistry.rename(tableName, newName, tableToken);
                tableSequencerAPI.renameWalTable(tableToken, newTableToken);
            } else {
                String lockedReason = lock(securityContext, tableToken, "renameTable");
                if (null == lockedReason) {
                    try {
                        rename0(path, tableToken, tableName, otherPath, newName);
                    } finally {
                        unlock(securityContext, tableName, null, false);
                    }
                    tableNameRegistry.dropTable(tableName, tableToken);
                } else {
                    LOG.error().$("cannot lock and rename [from='").$(tableName).$("', to='").$(newName).$("', reason='").$(lockedReason).$("']").$();
                    throw EntryUnavailableException.instance(lockedReason);
                }
            }
        } else {
            LOG.error().$('\'').utf8(tableName).$("' does not exist. Rename failed.").$();
            throw CairoException.nonCritical().put("Rename failed. Table '").put(tableName).put("' does not exist");
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

    public void unlock(
            @SuppressWarnings("unused") CairoSecurityContext securityContext,
            CharSequence tableName,
            @Nullable TableWriter writer,
            boolean newTable
    ) {
        validNameOrThrow(tableName);
        TableToken tableToken = getTableToken(tableName);
        readerPool.unlock(tableToken);
        writerPool.unlock(tableToken, writer, newTable);
        metadataPool.unlock(tableToken);
        LOG.info().$("unlocked [table=`").utf8(tableName).$("`]").$();
    }

    public void unlockReaders(CharSequence tableName) {
        validNameOrThrow(tableName);
        readerPool.unlock(getTableToken(tableName));
    }

    public void unlockTableName(TableToken tableToken) {
        tableNameRegistry.unlockTableName(tableToken);
    }

    public void unlockWriter(CairoSecurityContext securityContext, CharSequence tableName) {
        securityContext.checkWritePermission();
        validNameOrThrow(tableName);
        writerPool.unlock(getTableToken(tableName));
    }

    private void rename0(Path path, TableToken srcTableToken, CharSequence tableName, Path otherPath, CharSequence to) {
        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence root = configuration.getRoot();

        path.of(root).concat(srcTableToken).$();
        TableToken dstTableToken = lockTableName(to, srcTableToken.getTableId(), false);

        if (dstTableToken == null || ff.exists(otherPath.of(root).concat(dstTableToken).$())) {
            if (dstTableToken != null) {
                unlockTableName(dstTableToken);
            }
            LOG.error().$("rename target exists [from='").utf8(tableName).$("', to='").utf8(otherPath.chop$()).I$();
            throw CairoException.nonCritical().put("Rename target exists");
        }

        try {
            if (ff.rename(path, otherPath) != Files.FILES_RENAME_OK) {
                int error = ff.errno();
                LOG.error().$("could not rename [from='").$(path).$("', to='").utf8(otherPath).$("', error=").$(error).I$();
                throw CairoException.critical(error)
                        .put("could not rename [from='").put(path)
                        .put("', to='").put(otherPath)
                        .put("', error=").put(error);
            }
            tableNameRegistry.registerName(dstTableToken);
        } finally {
            tableNameRegistry.unlockTableName(dstTableToken);
        }
    }

    private void tryRepairTable(
            CairoSecurityContext securityContext,
            TableToken tableToken,
            RuntimeException rethrow
    ) {
        try {
            securityContext.checkWritePermission();
            writerPool.get(tableToken, "repair").close();
        } catch (EntryUnavailableException e) {
            // This is fine, writer is busy. Throw back origin error.
            throw rethrow;
        } catch (Throwable th) {
            LOG.critical()
                    .$("could not repair before reading [privateTableName=").utf8(tableToken.getDirName())
                    .$(" ,error=").$(th.getMessage()).I$();
            throw rethrow;
        }
    }

    private void validNameOrThrow(CharSequence tableName) {
        if (!TableUtils.isValidTableName(tableName, configuration.getMaxFileNameLength())) {
            throw CairoException.nonCritical()
                    .put("invalid table name [table=").putAsPrintable(tableName)
                    .put(']');
        }
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
