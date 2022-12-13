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

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class CairoEngine implements Closeable, WriterSource, WalWriterSource {
    public static final String BUSY_READER = "busyReader";
    private static final Log LOG = LogFactory.getLog(CairoEngine.class);
    private final AtomicLong asyncCommandCorrelationId = new AtomicLong();
    private final CairoConfiguration configuration;
    private final EngineMaintenanceJob engineMaintenanceJob;
    private final MessageBusImpl messageBus;
    private final MetadataPool metadataPool;
    private final Metrics metrics;
    private final ReaderPool readerPool;
    private final ThreadSafeObjectPool<SqlCompiler> sqlCompilerPool;
    private final IDGenerator tableIdGenerator;
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
    // Defaults WAL Apply threads set to 2, this is the upper limit of number of parallel compilations when applying WAL segments.
    public CairoEngine(CairoConfiguration configuration) {
        this(configuration, Metrics.disabled(), 2);
    }

    public CairoEngine(CairoConfiguration configuration, Metrics metrics, int totalWALApplyThreads) {
        this.configuration = configuration;
        this.textImportExecutionContext = new TextImportExecutionContext(configuration);
        this.metrics = metrics;
        this.tableSequencerAPI = new TableSequencerAPI(this, configuration);
        this.messageBus = new MessageBusImpl(configuration);
        this.writerPool = new WriterPool(configuration, messageBus, metrics);
        this.readerPool = new ReaderPool(configuration, messageBus);
        this.metadataPool = new MetadataPool(configuration, tableSequencerAPI);
        this.walWriterPool = new WalWriterPool(configuration, tableSequencerAPI);
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

        this.sqlCompilerPool = new ThreadSafeObjectPool<>(() -> new SqlCompiler(this), totalWALApplyThreads);
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
        if (sqlCompilerPool != null) {
            sqlCompilerPool.releaseAll();
        }
    }

    public void createTable(
            CairoSecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            TableStructure struct
    ) {
        validNameOrThrow(struct.getTableName());
        String lockedReason = lock(securityContext, struct.getTableName(), "createTable");
        if (null == lockedReason) {
            boolean newTable = false;
            try {
                if (getStatus(securityContext, path, struct.getTableName()) != TableUtils.TABLE_DOES_NOT_EXIST) {
                    // RESERVE is the same as if exists
                    throw EntryUnavailableException.instance("table exists");
                }
                createTableUnsafe(
                        securityContext,
                        mem,
                        path,
                        struct
                );
                newTable = true;
            } finally {
                unlock(securityContext, struct.getTableName(), null, newTable);
            }
        } else {
            throw EntryUnavailableException.instance(lockedReason);
        }
    }

    // caller has to acquire the lock before this method is called and release the lock after the call
    public void createTableUnsafe(
            CairoSecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            TableStructure struct
    ) {
        securityContext.checkWritePermission();
        int tableId = (int) tableIdGenerator.getNextId();
        if (struct.isWalEnabled()) {
            tableSequencerAPI.registerTable(tableId, struct);
        }

        // only create the table after it has been registered
        TableUtils.createTable(
                configuration,
                mem,
                path,
                struct,
                tableId
        );
    }

    public TableWriter getBackupWriter(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            CharSequence backupDirName
    ) {
        securityContext.checkWritePermission();
        // There is no point in pooling/caching these writers since they are only used once, backups are not incremental
        return new TableWriter(
                configuration,
                tableName,
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

    public TableRecordMetadata getMetadata(CairoSecurityContext securityContext, CharSequence tableName) {
        return getMetadata(securityContext, tableName, TableUtils.ANY_TABLE_VERSION);
    }

    public TableRecordMetadata getMetadata(CairoSecurityContext securityContext, CharSequence tableName, long structureVersion) {
        try {
            final TableRecordMetadata metadata = metadataPool.get(tableName);
            if (structureVersion != TableUtils.ANY_TABLE_VERSION && metadata.getStructureVersion() != structureVersion) {
                // rename to StructureVersionException?
                final ReaderOutOfDateException ex = ReaderOutOfDateException.of(tableName, metadata.getTableId(), metadata.getTableId(), structureVersion, metadata.getStructureVersion());
                metadata.close();
                throw ex;
            }
            return metadata;
        } catch (CairoException e) {
            tryRepairTable(securityContext, tableName, e);
        }
        return metadataPool.get(tableName);
    }

    public Metrics getMetrics() {
        return metrics;
    }

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
        TableReader reader = readerPool.get(tableName);
        if ((version > -1 && reader.getVersion() != version)
                || tableId > -1 && reader.getMetadata().getTableId() != tableId) {
            ReaderOutOfDateException ex = ReaderOutOfDateException.of(tableName, tableId, reader.getMetadata().getTableId(), version, reader.getVersion());
            reader.close();
            throw ex;
        }
        return reader;
    }

    public Map<CharSequence, AbstractMultiTenantPool.Entry<ReaderPool.R>> getReaderPoolEntries() {
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
            tryRepairTable(securityContext, tableName, e);
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
            CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName,
            int lo,
            int hi
    ) {
        return TableUtils.exists(configuration.getFilesFacade(), path, configuration.getRoot(), tableName, lo, hi);
    }

    public int getStatus(
            CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName
    ) {
        return getStatus(securityContext, path, tableName, 0, tableName.length());
    }

    public IDGenerator getTableIdGenerator() {
        return tableIdGenerator;
    }

    public TableSequencerAPI getTableSequencerAPI() {
        return tableSequencerAPI;
    }

    @Override
    public TableWriterAPI getTableWriterAPI(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            @Nullable String lockReason
    ) {
        securityContext.checkWritePermission();
        if (tableSequencerAPI.hasSequencer(tableName)) {
            return walWriterPool.get(tableName);
        }
        return getWriter(securityContext, tableName, lockReason);
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
            CairoSecurityContext securityContext,
            CharSequence tableName,
            CharSequence walName,
            int segmentId,
            long walRowCount
    ) {
        if (tableSequencerAPI.hasSequencer(tableName)) {
            // This is WAL table because sequencer exists
            return new WalReader(configuration, tableName, walName, segmentId, walRowCount);
        }

        throw CairoException.nonCritical().put("WAL reader is not supported for table ").put(tableName);
    }

    @Override
    public @NotNull WalWriter getWalWriter(CairoSecurityContext securityContext, CharSequence tableName) {
        securityContext.checkWritePermission();
        return walWriterPool.get(tableName);
    }

    public TableWriter getWriter(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            String lockReason
    ) {
        securityContext.checkWritePermission();
        validNameOrThrow(tableName);
        return writerPool.get(tableName, lockReason);
    }

    public TableWriter getWriterOrPublishCommand(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            @NotNull AsyncWriterCommand asyncWriterCommand
    ) {
        securityContext.checkWritePermission();
        validNameOrThrow(tableName);
        return writerPool.getWriterOrPublishCommand(tableName, asyncWriterCommand.getCommandName(), asyncWriterCommand);
    }

    public String lock(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            String lockReason
    ) {
        assert null != lockReason;
        securityContext.checkWritePermission();

        validNameOrThrow(tableName);
        String lockedReason = writerPool.lock(tableName, lockReason);
        if (lockedReason == null) { // not locked
            if (readerPool.lock(tableName)) {
                if (metadataPool.lock(tableName)) {
                    tableSequencerAPI.releaseInactive();
                    LOG.info().$("locked [table=`").utf8(tableName).$("`, thread=").$(Thread.currentThread().getId()).I$();
                    return null;
                }
                readerPool.unlock(tableName);
            }
            writerPool.unlock(tableName);
            return BUSY_READER;
        }
        return lockedReason;
    }

    public boolean lockReaders(CharSequence tableName) {
        validNameOrThrow(tableName);
        return readerPool.lock(tableName);
    }

    public void lockReaders(CharSequence tableName, Path path) {
        // this method must be invoked only when thread has lock on writer pool
        path.of(configuration.getRoot()).concat(tableName);
        int plen = path.length();

        final FilesFacade ff = configuration.getFilesFacade();
        try (TxnScoreboard txnScoreboard = new TxnScoreboard(configuration).ofRW(path)) {
            path.trimTo(plen).concat(TXN_FILE_NAME).$();
            int txnFd = TableUtils.openRO(configuration.getFilesFacade(), path, LOG);
            long txn;
            try {
                long mem = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    txn = TableUtils.readLongOrFail(configuration.getFilesFacade(), txnFd, TableUtils.TX_OFFSET_TXN_64, mem, path);
                } finally {
                    Unsafe.free(mem, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                ff.close(txnFd);
            }

            // make sure we can get a slot
            if (txnScoreboard.acquireTxn(txn)) {
                txnScoreboard.releaseTxn(txn);

                // lock the current txn if scoreboard is at min
                if (txnScoreboard.getMin() == txn && txnScoreboard.releaseTxn(txn) == -1) {
                    return;
                }
                throw CairoException.nonCritical().put("could not lock out the readers [table=").put(tableName).put(']');
            }
            throw CairoException.nonCritical().put("oversubscribed scoreboard [table=").put(tableName).put(']');
        }
    }

    public void notifyWalTxnCommitted(int tableId, String tableName, long txn) {
        final Sequence pubSeq = messageBus.getWalTxnNotificationPubSequence();
        while (true) {
            long cursor = pubSeq.next();
            if (cursor > -1L) {
                WalTxnNotificationTask task = messageBus.getWalTxnNotificationQueue().get(cursor);
                task.of(tableName, tableId, txn);
                pubSeq.done(cursor);
                return;
            } else if (cursor == -1L) {
                LOG.info().$("cannot publish WAL notifications, queue is full [current=")
                        .$(pubSeq.current()).$(", table=").$(tableName)
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
    public void releaseInactiveCompilers() {
        sqlCompilerPool.releaseInactive();
    }

    @TestOnly
    public void releaseInactiveTableSequencers() {
        tableSequencerAPI.releaseInactive();
    }

    public void remove(
            CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName
    ) {
        securityContext.checkWritePermission();
        validNameOrThrow(tableName);
        CharSequence lockedReason = lock(securityContext, tableName, "removeTable");
        if (null == lockedReason) {
            try {
                path.of(configuration.getRoot()).concat(tableName).$();
                int errno;
                if ((errno = configuration.getFilesFacade().rmdir(path)) != 0) {
                    LOG.error().$("could not remove table [tableName='").utf8(tableName).$("', error=").$(errno).I$();
                    throw CairoException.critical(errno).put("could not remove table [tableName=").put(tableName).put(']');
                }
                return;
            } finally {
                unlock(securityContext, tableName, null, false);
            }
        }
        throw CairoException.nonCritical().put("Could not lock '").put(tableName).put("' [reason='").put(lockedReason).put("']");
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

        validNameOrThrow(tableName);
        validNameOrThrow(newName);

        String lockedReason = lock(securityContext, tableName, "renameTable");
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

    public void setPoolListener(PoolListener poolListener) {
        this.metadataPool.setPoolListener(poolListener);
        this.writerPool.setPoolListener(poolListener);
        this.readerPool.setPoolListener(poolListener);
        this.walWriterPool.setPoolListener(poolListener);
    }

    public void unlock(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            @Nullable TableWriter writer,
            boolean newTable
    ) {
        securityContext.checkWritePermission();
        validNameOrThrow(tableName);
        readerPool.unlock(tableName);
        writerPool.unlock(tableName, writer, newTable);
        metadataPool.unlock(tableName);
        LOG.info().$("unlocked [table=`").utf8(tableName).$("`]").$();
    }

    public void unlockReaders(CharSequence tableName) {
        validNameOrThrow(tableName);
        readerPool.unlock(tableName);
    }

    private void rename0(Path path, CharSequence tableName, Path otherPath, CharSequence to) {
        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence root = configuration.getRoot();

        if (TableUtils.exists(ff, path, root, tableName) != TableUtils.TABLE_EXISTS) {
            LOG.error().$('\'').utf8(tableName).$("' does not exist. Rename failed.").$();
            throw CairoException.nonCritical().put("Rename failed. Table '").put(tableName).put("' does not exist");
        }

        path.of(root).concat(tableName).$();
        otherPath.of(root).concat(to).$();

        if (ff.exists(otherPath)) {
            LOG.error().$("rename target exists [from='").$(tableName).$("', to='").$(otherPath).I$();
            throw CairoException.nonCritical().put("Rename target exists");
        }

        if (ff.rename(path, otherPath) != Files.FILES_RENAME_OK) {
            int error = ff.errno();
            LOG.error().$("could not rename [from='").$(path).$("', to='").$(otherPath).$("', error=").$(error).I$();
            throw CairoException.critical(error)
                    .put("could not rename [from='").put(path)
                    .put("', to='").put(otherPath)
                    .put("', error=").put(error);
        }
    }

    private void tryRepairTable(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            RuntimeException rethrow
    ) {
        try {
            getWriter(securityContext, tableName, "repair").close();
        } catch (EntryUnavailableException e) {
            // This is fine, writer is busy. Throw back origin error.
            throw rethrow;
        } catch (Throwable th) {
            LOG.critical()
                    .$("could not repair before reading [table=").$(tableName)
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

    ClosableInstance<SqlCompiler> getAdhocSqlCompiler() {
        return sqlCompilerPool.get();
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
