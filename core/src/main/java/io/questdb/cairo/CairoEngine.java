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
import io.questdb.cairo.wal.*;
import io.questdb.cutlass.text.TextImportExecutionContext;
import io.questdb.griffin.DatabaseSnapshotAgent;
import io.questdb.griffin.SqlCompiler;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.tasks.TelemetryTask;
import io.questdb.tasks.WalTxnNotificationTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.pool.WriterPool.OWNERSHIP_REASON_NONE;

public class CairoEngine implements Closeable, WriterSource, WalWriterSource {
    public static final String BUSY_READER = "busyReader";
    private static final Log LOG = LogFactory.getLog(CairoEngine.class);
    private final WriterPool writerPool;
    private final ReaderPool readerPool;
    private final CairoConfiguration configuration;
    private final Metrics metrics;
    private final EngineMaintenanceJob engineMaintenanceJob;
    private final MessageBus messageBus;
    private final RingQueue<TelemetryTask> telemetryQueue;
    private final MPSequence telemetryPubSeq;
    private final SCSequence telemetrySubSeq;
    private final AtomicLong asyncCommandCorrelationId = new AtomicLong();
    private final IDGenerator tableIdGenerator;
    private final TableRegistry tableRegistry;
    private final TextImportExecutionContext textImportExecutionContext;
    private final ThreadSafeObjectPool<SqlCompiler> sqlCompilerPool;
    private final AtomicLong failedWalTxnCount = new AtomicLong(1);

    // Kept for embedded API purposes. The second constructor (the one with metrics)
    // should be preferred for internal use.
    public CairoEngine(CairoConfiguration configuration) {
        this(configuration, Metrics.disabled(), 5);
    }

    public CairoEngine(CairoConfiguration configuration, Metrics metrics, int totalIoThreads) {
        this.configuration = configuration;
        this.textImportExecutionContext = new TextImportExecutionContext(configuration);
        this.metrics = metrics;
        this.tableRegistry = new TableRegistry(this, configuration);
        this.messageBus = new MessageBusImpl(configuration);
        this.writerPool = new WriterPool(this, metrics);
        this.readerPool = new ReaderPool(this);
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

        this.sqlCompilerPool = new ThreadSafeObjectPool<>(() -> new SqlCompiler(this), totalIoThreads);
    }

    public long getFailedWalTxnCount() {
        return failedWalTxnCount.get();
    }

    public void checkTableName(CharSequence tableName) {
        if (!TableUtils.isValidTableName(tableName, configuration.getMaxFileNameLength())) {
            throw CairoException.nonCritical()
                    .put("invalid table name [table=").putAsPrintable(tableName)
                    .put(']');
        }
    }

    @TestOnly
    public boolean clear() {
        boolean b1 = readerPool.releaseAll();
        boolean b2 = writerPool.releaseAll();
        boolean b3 = tableRegistry.releaseAll();
        return b1 & b2 & b3;
    }

    @Override
    public void close() {
        Misc.free(writerPool);
        Misc.free(readerPool);
        Misc.free(tableIdGenerator);
        Misc.free(messageBus);
        Misc.free(tableRegistry);
        Misc.free(telemetryQueue);
    }

    public void createTable(
            CairoSecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            TableStructure struct,
            boolean keepLock
    ) {
        securityContext.checkWritePermission();
        // Helps to not create objects down the line
        String tableName = Chars.toString(struct.getTableName());
        checkTableName(tableName);

        CharSequence systemTableName;
        int tableId = (int) tableIdGenerator.getNextId();
        if (struct.isWalEnabled()) {
            systemTableName = tableRegistry.registerTableName(tableName, tableId);
            if (systemTableName == null) {
                throw EntryUnavailableException.instance("table exists");
            }
        } else {
            systemTableName = getSystemTableName(tableName);
        }

        try {
            String lockedReason = lock(securityContext, systemTableName, "createTable");
            if (null == lockedReason) {
                boolean newTable = false;
                try {
                    if (getStatus(securityContext, path, tableName) != TableUtils.TABLE_DOES_NOT_EXIST) {
                        // RESERVE is the same as if exists
                        throw EntryUnavailableException.instance("table exists");
                    }
                    createTableUnsafe(
                            securityContext,
                            mem,
                            path,
                            struct,
                            systemTableName,
                            tableId
                    );
                    newTable = true;
                } finally {
                    if (!keepLock) {
                        unlock(securityContext, tableName, null, newTable);
                    }
                }
            } else {
                throw EntryUnavailableException.instance(lockedReason);
            }
        } catch (Throwable th) {
            if (struct.isWalEnabled()) {
                tableRegistry.dropTable(tableName, Chars.toString(systemTableName));
            }
            throw th;
        }
    }

    public void releaseReadersBySystemName(CharSequence systemTableName) {
        // TODO: release readers at the same time
        readerPool.unlock(systemTableName);
    }

    ClosableInstance<SqlCompiler> getAdhocSqlCompiler() {
        return sqlCompilerPool.get();
    }

    public TableRegistry getTableRegistry() {
        return tableRegistry;
    }

    public CharSequence getSystemTableName(final CharSequence tableName) {
        return getTableRegistry().getSystemTableNameOrDefault(tableName);
    }

    public String getTableNameBySystemName(CharSequence systemTableName) {
        String tableName = tableRegistry.getTableNameBySystemName(systemTableName);
        if (tableName != null) {
            return tableName;
        }

        return TableUtils.toTableNameFromSystemName(Chars.toString(systemTableName));
    }

    public boolean isWalTable(final CharSequence tableName) {
        return getTableRegistry().getSystemTableName(tableName) != null;
    }

    public TableRecordMetadata getMetadata(CairoSecurityContext securityContext, CharSequence tableName, MetadataFactory metadataFactory) {
        securityContext.checkWritePermission();
        CharSequence walSystemTableName = tableRegistry.getSystemTableName(tableName);
        if (walSystemTableName != null) {
            // This is WAL table because sequencer exists
            final SequencerMetadata sequencerMetadata = metadataFactory.getSequencerMetadata();
            tableRegistry.copyMetadataTo(Chars.toString(tableName), walSystemTableName, sequencerMetadata);
            return sequencerMetadata;
        }

        try {
            return metadataFactory.openTableReaderMetadata(tableName);
        } catch (CairoException e) {
            try (TableReader reader = tryGetReaderRepairWithWriter(securityContext, tableName, e)) {
                return metadataFactory.openTableReaderMetadata(reader);
            }
        }
    }

    public TableWriter getBackupWriter(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            CharSequence backupDirName
    ) {
        securityContext.checkWritePermission();
        // There is no point in pooling/caching these writers since they are only used once, backups are not incremental
        CharSequence systemTableName = getSystemTableName(tableName);
        return new TableWriter(
                configuration,
                tableName,
                systemTableName,
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

    public TableReader getReader(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            int tableId,
            long version
    ) {
        checkTableName(tableName);
        CharSequence systemTableName = getSystemTableName(tableName);
        TableReader reader = readerPool.get(systemTableName);
        if ((version > -1 && reader.getVersion() != version)
                || tableId > -1 && reader.getMetadata().getId() != tableId) {
            ReaderOutOfDateException ex = ReaderOutOfDateException.of(tableName, tableId, reader.getMetadata().getId(), version, reader.getVersion());
            reader.close();
            throw ex;
        }
        return reader;
    }

    public Map<CharSequence, ReaderPool.Entry> getReaderPoolEntries() {
        return readerPool.entries();
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

    public Metrics getMetrics() {
        return metrics;
    }

    public PoolListener getPoolListener() {
        return this.writerPool.getPoolListener();
    }

    public IDGenerator getTableIdGenerator() {
        return tableIdGenerator;
    }

    @Override
    public TableWriterFrontend getTableWriterFrontEnd(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            @Nullable String lockReason
    ) {
        securityContext.checkWritePermission();
        checkTableName(tableName);
        CharSequence systemTableName = tableRegistry.getSystemTableName(tableName);
        if (systemTableName != null) {
            return tableRegistry.getWalWriter(systemTableName);
        }

        return writerPool.get(tableRegistry.getDefaultTableName(tableName), lockReason);
    }

    public void notifyWalTxnFailed() {
        failedWalTxnCount.incrementAndGet();
    }

    public void setPoolListener(PoolListener poolListener) {
        this.writerPool.setPoolListener(poolListener);
        this.readerPool.setPoolListener(poolListener);
    }

    public TableReader getReader(
            CairoSecurityContext securityContext,
            CharSequence tableName
    ) {
        return getReader(securityContext, tableName, TableUtils.ANY_TABLE_ID, TableUtils.ANY_TABLE_VERSION);
    }

    // For testing only
    @TestOnly
    public WalReader getWalReader(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            CharSequence walName,
            int segmentId,
            long walRowCount
    ) {
        securityContext.checkWritePermission();
        CharSequence systemTableName = tableRegistry.getSystemTableName(tableName);
        if (systemTableName != null) {
            // This is WAL table because sequencer exists
            return new WalReader(configuration, tableName, systemTableName, walName, segmentId, walRowCount);
        }

        throw CairoException.nonCritical().put("WAL reader is not supported for table ").put(tableName);
    }

    @Override
    public @NotNull WalWriter getWalWriter(CairoSecurityContext securityContext, CharSequence tableName) {
        securityContext.checkWritePermission();
        return tableRegistry.getWalWriter(getSystemTableName(tableName));
    }

    public TableReader getReaderWithRepair(CairoSecurityContext securityContext, CharSequence tableName) {
        checkTableName(tableName);
        try {
            return getReader(securityContext, tableName);
        } catch (CairoException ex) {
            // Cannot open reader on existing table is pretty bad.
            LOG.critical().$("error opening reader [table=").$(tableName)
                    .$(",errno=").$(ex.getErrno())
                    .$(",error=").$(ex.getMessage()).I$();
            // In some messed states, for example after _meta file swap failure Reader cannot be opened
            // but writer can be. Opening writer fixes the table mess.
            return tryGetReaderRepairWithWriter(securityContext, tableName, ex);
        }
    }

    private TableReader tryGetReaderRepairWithWriter(CairoSecurityContext securityContext, CharSequence tableName, RuntimeException originException) {
        try (TableWriter ignored = getWriter(securityContext, tableName, "repair")) {
            return getReader(securityContext, tableName);
        } catch (EntryUnavailableException wrOpEx) {
            // This is fine, writer is busy. Throw back origin error.
            throw originException;
        } catch (Throwable th) {
            LOG.error().$("error preliminary opening writer for [table=").$(tableName)
                    .$(",error=").$(th.getMessage()).I$();
            throw originException;
        }
    }

    public int getStatus(
            CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName,
            int lo,
            int hi
    ) {
        CharSequence systemTableName = getSystemTableName(tableName.subSequence(lo, hi));
        return TableUtils.exists(configuration.getFilesFacade(), path, configuration.getRoot(), systemTableName);
    }

    public int getStatus(
            CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName
    ) {
        CharSequence systemTableName = getSystemTableName(tableName);
        return TableUtils.exists(configuration.getFilesFacade(), path, configuration.getRoot(), systemTableName);
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

    public TableWriter getWriter(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            String lockReason
    ) {
        securityContext.checkWritePermission();
        checkTableName(tableName);
        return writerPool.get(getSystemTableName(tableName), lockReason);
    }

    public TableWriter getWriterBySystemName(
            CairoSecurityContext securityContext,
            CharSequence tableSystemName,
            String lockReason
    ) {
        securityContext.checkWritePermission();
        return writerPool.get(tableSystemName, lockReason);
    }

    public TableWriter getWriterOrPublishCommand(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            @NotNull AsyncWriterCommand asyncWriterCommand
    ) {
        securityContext.checkWritePermission();
        checkTableName(tableName);
        CharSequence systemTableName = getSystemTableName(tableName);
        return writerPool.getWriterOrPublishCommand(systemTableName, asyncWriterCommand.getCommandName(), asyncWriterCommand);
    }

    public String lock(
            CairoSecurityContext securityContext,
            CharSequence systemTableName,
            String lockReason
    ) {
        assert null != lockReason;
        securityContext.checkWritePermission();

        String lockedReason = writerPool.lock(systemTableName, lockReason);
        if (lockedReason == OWNERSHIP_REASON_NONE) {
            boolean locked = readerPool.lock(systemTableName);
            if (locked) {
                LOG.info().$("locked [table=`").utf8(systemTableName).$("`, thread=").$(Thread.currentThread().getId()).I$();
                return null;
            }
            writerPool.unlock(systemTableName);
            return BUSY_READER;
        }
        return lockedReason;

    }

    public void notifyWalTxnCommitted(int tableId, String systemTableName, long txn) {
        Sequence pubSeq = messageBus.getWalTxnNotificationPubSequence();
        while (true) {
            long cursor = pubSeq.next();
            if (cursor > -1L) {
                WalTxnNotificationTask task = messageBus.getWalTxnNotificationQueue().get(cursor);
                task.of(systemTableName, tableId, txn);
                pubSeq.done(cursor);
                return;
            } else if (cursor == -1L) {
                LOG.info().$("cannot publish WAL notifications, queue is full [current=")
                        .$(pubSeq.current()).$(", table=").$(systemTableName)
                        .$();
                // Oh, no queue overflow! Throw away notification and trigger a job to rescan all the tables
                notifyWalTxnFailed();
                return;
            }
        }
    }

    public boolean lockReaders(CharSequence tableName) {
        checkTableName(tableName);
        return readerPool.lock(getSystemTableName(tableName));
    }

    public boolean lockReadersBySystemName(CharSequence systemTableName) {
        return readerPool.lock(systemTableName);
    }

    public CharSequence lockWriter(CairoSecurityContext securityContext, CharSequence tableName, String lockReason) {
        securityContext.checkWritePermission();
        checkTableName(tableName);
        return writerPool.lock(getSystemTableName(tableName), lockReason);
    }

    @TestOnly
    public boolean releaseAllReaders() {
        return readerPool.releaseAll();
    }

    @TestOnly
    public void releaseAllWriters() {
        writerPool.releaseAll();
    }

    public boolean releaseInactive() {
        boolean useful = writerPool.releaseInactive();
        useful |= readerPool.releaseInactive();
        useful |= tableRegistry.releaseInactive();
        return useful;
    }

    @TestOnly
    public void clearPools() {
        sqlCompilerPool.releaseInactive();
    }

    public void remove(
            CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName
    ) {
        securityContext.checkWritePermission();
        checkTableName(tableName);
        CharSequence systemTableName = getSystemTableName(tableName);

        if (isWalTable(tableName)) {
            tableRegistry.dropTable(Chars.toString(tableName), Chars.toString(systemTableName));
        } else {
            CharSequence lockedReason = lock(securityContext, systemTableName, "removeTable");
            if (null == lockedReason) {
                try {
                    path.of(configuration.getRoot()).concat(getSystemTableName(tableName)).$();
                    int errno;
                    if ((errno = configuration.getFilesFacade().rmdir(path)) != 0) {
                        LOG.error().$("remove failed [tableName='").utf8(tableName).$("', error=").$(errno).$(']').$();
                        throw CairoException.critical(errno).put("Table remove failed");
                    }
                    return;
                } finally {
                    unlock(securityContext, tableName, null, false);
                }
            }
            throw CairoException.nonCritical().put("Could not lock '").put(tableName).put("' [reason='").put(lockedReason).put("']");
        }
    }

    public int removeDirectory(@Transient Path path, CharSequence dir) {
        path.of(configuration.getRoot()).concat(dir);
        final FilesFacade ff = configuration.getFilesFacade();
        return ff.rmdir(path.slash$());
    }

    public void rename(
            CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName,
            Path otherPath,
            CharSequence newName
    ) {
        securityContext.checkWritePermission();

        checkTableName(tableName);
        checkTableName(newName);

        CharSequence systemTableName = getSystemTableName(tableName);
        String lockedReason = lock(securityContext, systemTableName, "renameTable");
        if (null == lockedReason) {
            try {
                rename0(path, tableName, otherPath, newName);
            } finally {
                unlock(securityContext, tableName, null, false);
            }
        } else {
            LOG.error().$("cannot lock and rename [from='").$(tableName).$("', to='").$(newName).$("', reason='").$(lockedReason).$("']").$();
            throw EntryUnavailableException.instance(lockedReason);
        }
    }

    public void unlock(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            @Nullable TableWriter writer,
            boolean newTable
    ) {
        checkTableName(tableName);
        CharSequence systemTableName = getSystemTableName(tableName);
        readerPool.unlock(systemTableName);
        writerPool.unlock(systemTableName, writer, newTable);
        LOG.info().$("unlocked [table=`").utf8(tableName).$("`]").$();
    }

    public void unlockReaders(CharSequence tableName) {
        checkTableName(tableName);
        readerPool.unlock(getSystemTableName(tableName));
    }

    public void unlockWriter(CairoSecurityContext securityContext, CharSequence tableName) {
        securityContext.checkWritePermission();
        checkTableName(tableName);
        writerPool.unlock(getSystemTableName(tableName));
    }

    public TextImportExecutionContext getTextImportExecutionContext() {
        return textImportExecutionContext;
    }

    // caller has to acquire the lock before this method is called and release the lock after the call
    private void createTableUnsafe(
            CairoSecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            TableStructure struct,
            CharSequence systemTableName,
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
                systemTableName,
                struct,
                ColumnType.VERSION,
                tableId
        );
        if (struct.isWalEnabled()) {
            tableRegistry.registerTable(tableId, struct);
        }
    }

    private void rename0(Path path, CharSequence tableName, Path otherPath, CharSequence to) {
        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence root = configuration.getRoot();

        CharSequence systemTableName = getSystemTableName(tableName);
        CharSequence dstFileName = getSystemTableName(to);
        if (TableUtils.exists(ff, path, root, systemTableName) != TableUtils.TABLE_EXISTS) {
            LOG.error().$('\'').utf8(tableName).$("' does not exist. Rename failed.").$();
            throw CairoException.nonCritical().put("Rename failed. Table '").put(tableName).put("' does not exist");
        }

        path.of(root).concat(systemTableName).$();
        otherPath.of(root).concat(dstFileName).$();

        if (ff.exists(otherPath)) {
            LOG.error().$("rename target exists [from='").$(tableName).$("', to='").$(otherPath).I$();
            throw CairoException.nonCritical().put("Rename target exists");
        }

        if (ff.rename(path, otherPath) != Files.FILES_RENAME_OK) {
            int error = ff.errno();
            LOG.error().$("rename failed [from='").$(path).$("', to='").$(otherPath).$("', error=").$(error).I$();
            throw CairoException.critical(error).put("Rename failed");
        }
    }

    private class EngineMaintenanceJob extends SynchronizedJob {

        private final MicrosecondClock clock;
        private final long checkInterval;
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
