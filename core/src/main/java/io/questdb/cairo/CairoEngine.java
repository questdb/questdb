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
import io.questdb.griffin.SqlToOperation;
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
    private final Path rootPath;
    private final int rootPathLen;

    private final TextImportExecutionContext textImportExecutionContext;
    private TxReader tempTxReader;

    // Kept for embedded API purposes. The second constructor (the one with metrics)
    // should be preferred for internal use.
    public CairoEngine(CairoConfiguration configuration) {
        this(configuration, Metrics.disabled());
    }

    public CairoEngine(CairoConfiguration configuration, Metrics metrics) {
        this.configuration = configuration;
        this.textImportExecutionContext = new TextImportExecutionContext(configuration);
        this.metrics = metrics;
        this.tableRegistry = new TableRegistry(this, configuration);
        this.messageBus = new MessageBusImpl(configuration);
        this.writerPool = new WriterPool(configuration, messageBus, metrics);
        this.readerPool = new ReaderPool(configuration, messageBus);
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

        this.rootPath = new Path().of(configuration.getRoot());
        this.rootPathLen = rootPath.length();
    }

    public void checkMissingWalTransactions() {
        try (TxReader txReader = new TxReader(configuration.getFilesFacade())) {
            tempTxReader = txReader;
            tableRegistry.forAllWalTables(this::checkNotifyOutstandingTxnInWal);
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
        Misc.free(rootPath);
        Misc.free(tableRegistry);
        Misc.free(telemetryQueue);
    }

    public void createTable(
            CairoSecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            TableStructure struct
    ) {
        checkTableName(struct.getTableName());
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

    public TableRegistry getTableRegistry() {
        return tableRegistry;
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
            tableRegistry.registerTable(tableId, struct);
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

    public TableRecordMetadata getMetadata(CairoSecurityContext securityContext, CharSequence tableName, MetadataFactory metadataFactory) {
        securityContext.checkWritePermission();
        final String tableNameStr = Chars.toString(tableName);
        if (tableRegistry.hasSequencer(tableNameStr)) {
            // This is WAL table because sequencer exists
            final SequencerMetadata sequencerMetadata = metadataFactory.getSequencerMetadata();
            tableRegistry.copyMetadataTo(tableName, sequencerMetadata);
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
        if (tableRegistry.hasSequencer(tableName)) {
            return tableRegistry.getWalWriter(tableName);
        }
        return getWriter(securityContext, tableName, lockReason);
    }

    public void checkNotifyOutstandingTxnInWal(int tableId, CharSequence tableName, long txn) {
        rootPath.trimTo(rootPathLen).concat(tableName).concat(TableUtils.TXN_FILE_NAME).$();
        try (TxReader txReader = tempTxReader.ofRO(rootPath, PartitionBy.NONE)) {
            if (txReader.unsafeReadTxn()  < txn) {
                // table name should be immutable when in the notification message
                String tableNameStr = Chars.toString(tableName);
                notifyWalTxnCommitted(tableId, tableNameStr, txn);
            }
        }
    }

    public void notifyWalTxnCommitted(int tableId, String tableName, long txn) {
        Sequence pubSeq = messageBus.getWalTxnNotificationPubSequence();
        int steelingAttempts = 100;
        SqlToOperation sqlToOperation = null;
        IntLongHashMap localCommittedTransactions = new IntLongHashMap();

        try {
            while (true) {
                long cursor = pubSeq.next();
                if (cursor > -1L) {
                    WalTxnNotificationTask task = messageBus.getWalTxnNotificationQueue().get(cursor);
                    task.of(tableName, tableId, txn);
                    pubSeq.done(cursor);
                    return;
                } else if (cursor == -1L) {
                    // Oh, no queue overflow!
                    // Steel the work!
                    if (steelingAttempts-- > 0) {
                        Sequence subSeq = messageBus.getWalTxnNotificationSubSequence();
                        if (sqlToOperation == null) {
                            sqlToOperation = new SqlToOperation(this);
                        }
                        try {
                            while ((cursor = subSeq.next()) > -1L || cursor == -2L) {
                                WalTxnNotificationTask task = messageBus.getWalTxnNotificationQueue().get(cursor);
                                String taskTableName = task.getTableName();
                                int taskTableId = task.getTableId();
                                // We can release queue obj now, all data copied. If writing fails another commit or async job will re-trigger it
                                subSeq.done(cursor);

                                if (localCommittedTransactions.get(taskTableId) < task.getTxn()) {
                                    long lastCommittedTxn = ApplyWal2TableJob.processWalTxnNotification(taskTableName, taskTableId, this, sqlToOperation);
                                    if (lastCommittedTxn > -1) {
                                        localCommittedTransactions.put(taskTableId, lastCommittedTxn);
                                    }
                                }

                                // If this is the same table we want to notify about
                                // then we don't to notify about it anymore, whoever processes the WAL
                                // for the table will apply the transaction we want to notify about too
                                if (tableName.equals(taskTableName) && tableId == taskTableId) {
                                    return;
                                }
                            }
                        } catch (Throwable throwable) {
                            LOG.criticalW()
                                    .$("error in steeling and processing WAL notifications. Attempts left: ").$(steelingAttempts)
                                    .$(throwable).$();
                        }
                    } else {
                        LOG.criticalW().$("error publishing WAL notifications, queue is full").$();
                        // WAL is committed and can eventually be picked up and applied to the table.
                        // Error is critical but throwing exception will make client assume
                        // that commit failed but in fact the data is written.
                        return;
                    }
                }
            }
        } finally {
            if (sqlToOperation != null) {
                Misc.free(sqlToOperation);
            }
        }
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

    public TableReader getReader(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            int tableId,
            long version
    ) {
        checkTableName(tableName);
        TableReader reader = readerPool.get(tableName);
        if ((version > -1 && reader.getVersion() != version)
                || tableId > -1 && reader.getMetadata().getId() != tableId) {
            ReaderOutOfDateException ex = ReaderOutOfDateException.of(tableName, tableId, reader.getMetadata().getId(), version, reader.getVersion());
            reader.close();
            throw ex;
        }
        return reader;
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
        if (tableRegistry.hasSequencer(tableName)) {
            // This is WAL table because sequencer exists
            return new WalReader(configuration, tableName, walName, segmentId, walRowCount);
        }

        throw CairoException.nonCritical().put("WAL reader is not supported for table ").put(tableName);
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
        return TableUtils.exists(configuration.getFilesFacade(), path, configuration.getRoot(), tableName, lo, hi);
    }

    public int getStatus(
            CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName
    ) {
        return getStatus(securityContext, path, tableName, 0, tableName.length());
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
        return writerPool.get(tableName, lockReason);
    }

    public TableWriter getWriterOrPublishCommand(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            @NotNull AsyncWriterCommand asyncWriterCommand
    ) {
        securityContext.checkWritePermission();
        checkTableName(tableName);
        return writerPool.getWriterOrPublishCommand(tableName, asyncWriterCommand.getCommandName(), asyncWriterCommand);
    }

    @Override
    public @NotNull WalWriter getWalWriter(CairoSecurityContext securityContext, CharSequence tableName) {
        securityContext.checkWritePermission();
        return tableRegistry.getWalWriter(tableName);
    }

    public String lock(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            String lockReason
    ) {
        assert null != lockReason;
        securityContext.checkWritePermission();

        checkTableName(tableName);
        String lockedReason = writerPool.lock(tableName, lockReason);
        if (lockedReason == OWNERSHIP_REASON_NONE) {
            boolean locked = readerPool.lock(tableName);
            if (locked) {
                LOG.info().$("locked [table=`").utf8(tableName).$("`, thread=").$(Thread.currentThread().getId()).I$();
                return null;
            }
            writerPool.unlock(tableName);
            return BUSY_READER;
        }
        return lockedReason;
    }

    public boolean lockReaders(CharSequence tableName) {
        checkTableName(tableName);
        return readerPool.lock(tableName);
    }

    public CharSequence lockWriter(CairoSecurityContext securityContext, CharSequence tableName, String lockReason) {
        securityContext.checkWritePermission();
        checkTableName(tableName);
        return writerPool.lock(tableName, lockReason);
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

    public void remove(
            CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName
    ) {
        securityContext.checkWritePermission();
        checkTableName(tableName);
        CharSequence lockedReason = lock(securityContext, tableName, "removeTable");
        if (null == lockedReason) {
            try {
                path.of(configuration.getRoot()).concat(tableName).$();
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

    public void unlock(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            @Nullable TableWriter writer,
            boolean newTable
    ) {
        checkTableName(tableName);
        readerPool.unlock(tableName);
        writerPool.unlock(tableName, writer, newTable);
        LOG.info().$("unlocked [table=`").utf8(tableName).$("`]").$();
    }

    public void unlockReaders(CharSequence tableName) {
        checkTableName(tableName);
        readerPool.unlock(tableName);
    }

    public void unlockWriter(CairoSecurityContext securityContext, CharSequence tableName) {
        securityContext.checkWritePermission();
        checkTableName(tableName);
        writerPool.unlock(tableName);
    }

    public TextImportExecutionContext getTextImportExecutionContext() {
        return textImportExecutionContext;
    }

    private void checkTableName(CharSequence tableName) {
        if (!TableUtils.isValidTableName(tableName, configuration.getMaxFileNameLength())) {
            throw CairoException.nonCritical()
                    .put("invalid table name [table=").putAsPrintable(tableName)
                    .put(']');
        }
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
