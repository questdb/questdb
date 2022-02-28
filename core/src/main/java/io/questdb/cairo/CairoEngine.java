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
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.pool.ReaderPool;
import io.questdb.cairo.pool.WriterPool;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.sql.ReaderOutOfDateException;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.AlterStatement;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.tasks.TableWriterTask;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.pool.WriterPool.OWNERSHIP_REASON_NONE;

public class CairoEngine implements Closeable, WriterSource {
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
    private final RingQueue<TableWriterTask> tableWriterCmdQueue;
    private final MCSequence tableWriterCmdSubSeq;
    private final long tableIdMemSize;
    private final AtomicLong alterCommandCommandCorrelationId = new AtomicLong();
    private long tableIdFd = -1;
    private long tableIdMem = 0;
    // List of readers kept around to lock partitions while a database snapshot is being made.
    private final AtomicReference<ObjList<TableReader>> snapshotReadersRef = new AtomicReference<>();

    // Kept for embedded API purposes. The second constructor (the one with metrics)
    // should be preferred for internal use.
    public CairoEngine(CairoConfiguration configuration) {
        this(configuration, Metrics.disabled());
    }

    public CairoEngine(CairoConfiguration configuration, Metrics metrics) {
        this.configuration = configuration;
        this.metrics = metrics;
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
        this.tableIdMemSize = Files.PAGE_SIZE;
        // subscribe to table writer commands to provide cold command handling
        this.tableWriterCmdQueue = messageBus.getTableWriterCommandQueue();
        final FanOut fanOut = messageBus.getTableWriterCommandFanOut();
        fanOut.and(tableWriterCmdSubSeq = new MCSequence(fanOut.current(), tableWriterCmdQueue.getCycle()));
        openTableId();
        try {
            EngineMigration.migrateEngineTo(this, ColumnType.VERSION, false);
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    @TestOnly
    public boolean clear() {
        releaseSnapshotReaders();
        boolean b1 = readerPool.releaseAll();
        boolean b2 = writerPool.releaseAll();
        return b1 & b2;
    }

    @Override
    public void close() {
        Misc.free(writerPool);
        releaseSnapshotReaders();
        Misc.free(readerPool);
        freeTableId();
        Misc.free(messageBus);
    }

    public void createTable(
            CairoSecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            TableStructure struct
    ) {
        CharSequence lockedReason = lock(securityContext, struct.getTableName(), "createTable");
        if (null == lockedReason) {
            if (writerPool.exists(struct.getTableName())) {
                throw EntryUnavailableException.instance("table exists");
            }
            boolean newTable = false;
            try {
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

    public void createTableUnsafe(
            CairoSecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            TableStructure struct
    ) {
        securityContext.checkWritePermission();
        TableUtils.createTable(
                configuration,
                mem,
                path,
                struct,
                (int) getNextTableId()
        );
    }

    public void freeTableId() {
        if (tableIdMem != 0) {
            configuration.getFilesFacade().munmap(tableIdMem, tableIdMemSize, MemoryTag.MMAP_DEFAULT);
            tableIdMem = 0;
        }
        if (tableIdFd != -1) {
            configuration.getFilesFacade().close(tableIdFd);
            tableIdFd = -1;
        }
    }

    public TableWriter getBackupWriter(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            CharSequence backupDirName
    ) {
        securityContext.checkWritePermission();
        // There is no point in pooling/caching these writers since they are only used once, backups are not incremental
        return new TableWriter(configuration, tableName, messageBus, null, true, DefaultLifecycleManager.INSTANCE, backupDirName, Metrics.disabled());
    }

    public int getBusyReaderCount() {
        return readerPool.getBusyCount();
    }

    public int getBusyWriterCount() {
        return writerPool.getBusyCount();
    }

    public CairoConfiguration getConfiguration() {
        return configuration;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public Job getEngineMaintenanceJob() {
        return engineMaintenanceJob;
    }

    public MessageBus getMessageBus() {
        return messageBus;
    }

    public long getNextTableId() {
        long next;
        long x = Unsafe.getUnsafe().getLong(tableIdMem);
        do {
            next = x;
            x = Os.compareAndSwap(tableIdMem, next, next + 1);
        } while (next != x);
        return next + 1;
    }

    public PoolListener getPoolListener() {
        return this.writerPool.getPoolListener();
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
        TableReader reader = readerPool.get(tableName);
        if ((version > -1 && reader.getVersion() != version)
                || tableId > -1 && reader.getMetadata().getId() != tableId) {
            reader.close();
            throw ReaderOutOfDateException.of(tableName);
        }
        return reader;
    }

    public int getStatus(
            CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName,
            int lo,
            int hi
    ) {
        if (writerPool.exists(tableName)) {
            return TableUtils.TABLE_EXISTS;
        }
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

    @Override
    public TableWriter getWriter(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            CharSequence lockReason
    ) {
        securityContext.checkWritePermission();
        return writerPool.get(tableName, lockReason);
    }

    public CharSequence lock(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            CharSequence lockReason
    ) {
        assert null != lockReason;
        securityContext.checkWritePermission();

        CharSequence lockedReason = writerPool.lock(tableName, lockReason);
        if (lockedReason == OWNERSHIP_REASON_NONE) {
            boolean locked = readerPool.lock(tableName);
            if (locked) {
                LOG.info().$("locked [table=`").utf8(tableName).$("`, thread=").$(Thread.currentThread().getId()).$(']').$();
                return null;
            }
            writerPool.unlock(tableName);
            return BUSY_READER;
        }
        return lockedReason;
    }

    public boolean lockReaders(CharSequence tableName) {
        return readerPool.lock(tableName);
    }

    public CharSequence lockWriter(CharSequence tableName, CharSequence lockReason) {
        return writerPool.lock(tableName, lockReason);
    }

    public void openTableId() {
        freeTableId();
        FilesFacade ff = configuration.getFilesFacade();
        Path path = Path.getThreadLocal(configuration.getRoot()).concat(TableUtils.TAB_INDEX_FILE_NAME).$();
        try {
            tableIdFd = TableUtils.openFileRWOrFail(ff, path, configuration.getWriterFileOpenOpts());
            this.tableIdMem = TableUtils.mapRW(ff, tableIdFd, tableIdMemSize, MemoryTag.MMAP_DEFAULT);
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    public long publishTableWriterCommand(AlterStatement alterTableStatement) {
        CharSequence tableName = alterTableStatement.getTableName();
        final MPSequence commandPubSeq = messageBus.getTableWriterCommandPubSeq();

        while (true) {
            long pubCursor = commandPubSeq.next();
            long correlationId = alterCommandCommandCorrelationId.incrementAndGet();
            if (pubCursor > -1) {
                final TableWriterTask command = tableWriterCmdQueue.get(pubCursor);
                alterTableStatement.serialize(command);
                command.setInstance(correlationId);
                commandPubSeq.done(pubCursor);
                LOG.info()
                        .$("published ASYNC writer ALTER TABLE task [table=").$(tableName)
                        .$(",instance=").$(correlationId)
                        .I$();
                return correlationId;
            } else if (pubCursor == -1) {
                // Queue is full
                LOG.error()
                        .$("could not publish writer task [table=").$(tableName)
                        .$(",instance").$(correlationId)
                        .$(",seqCursor=").$(pubCursor)
                        .I$();
                throw CairoException.instance(0).put("Could not publish writer ALTER TABLE task [table=").put(tableName).put(']');
            }
        }
    }

    public boolean releaseAllReaders() {
        return readerPool.releaseAll();
    }

    public void releaseAllWriters() {
        writerPool.releaseAll();
    }

    public boolean releaseInactive() {
        boolean useful = writerPool.releaseInactive();
        useful |= readerPool.releaseInactive();
        return useful;
    }

    public void remove(
            CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName
    ) {
        securityContext.checkWritePermission();
        CharSequence lockedReason = lock(securityContext, tableName, "removeTable");
        if (null == lockedReason) {
            try {
                path.of(configuration.getRoot()).concat(tableName).$();
                int errno;
                if ((errno = configuration.getFilesFacade().rmdir(path)) != 0) {
                    LOG.error().$("remove failed [tableName='").utf8(tableName).$("', error=").$(errno).$(']').$();
                    throw CairoException.instance(errno).put("Table remove failed");
                }
                return;
            } finally {
                unlock(securityContext, tableName, null, false);
            }
        }
        throw CairoException.instance(configuration.getFilesFacade().errno()).put("Could not lock '").put(tableName).put("' [reason='").put(lockedReason).put("']");
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
        CharSequence lockedReason = lock(securityContext, tableName, "renameTable");
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

    // This is not thread safe way to reset table ID back to 0
    // It is useful for testing only
    public void resetTableId() {
        Unsafe.getUnsafe().putLong(tableIdMem, 0);
    }

    public boolean tick() {
        final long cursor = tableWriterCmdSubSeq.next();
        if (cursor > -1) {
            final TableWriterTask cmd = tableWriterCmdQueue.get(cursor);
            final String tableName = cmd.getTableName();
            boolean done = false;
            LOG.info().$("received table command cmd [tableName=").$(tableName)
                    .$(", type=").$(cmd.getType())
                    .$(", instance=").$(cmd.getInstance())
                    .$(", ip=").$ip(cmd.getIp())
                    .I$();

            if (tableName != null) {
                try (TableWriter writer = writerPool.get(tableName, "async writer cmd")) {
                    done = true; // next line must call done() on the sequence
                    writer.processCommandQueue(cmd, tableWriterCmdSubSeq, cursor, true);
                } catch (EntryUnavailableException e) {
                    // ignore command, writer is busy
                    // it will tick on its way back to pool or earlier
                } catch (Throwable e) {
                    LogRecord record = LOG.error()
                            .$("could not create table writer or execute writer command [tableName=").$(tableName)
                            .$(", tableId=").$(cmd.getTableId()).$(", ex=`");
                    if (e instanceof Sinkable) {
                        record.$((Sinkable) e).$('`').I$();
                    } else {
                        record.$(e).$('`').I$();
                    }
                } finally {
                    if (!done) {
                        tableWriterCmdSubSeq.done(cursor);
                    }
                }
            } else {
                tableWriterCmdSubSeq.done(cursor);
            }
            return true;
        }
        return false;
    }

    public void unlock(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            @Nullable TableWriter writer,
            boolean newTable
    ) {
        readerPool.unlock(tableName);
        writerPool.unlock(tableName, writer, newTable);
        LOG.info().$("unlocked [table=`").utf8(tableName).$("`]").$();
    }

    public void unlockReaders(CharSequence tableName) {
        readerPool.unlock(tableName);
    }

    public void unlockWriter(CharSequence tableName) {
        writerPool.unlock(tableName);
    }

    public boolean isSnapshotInProgress() {
        return snapshotReadersRef.get() != null;
    }

    public boolean setSnapshotReaders(ObjList<TableReader> readers) {
        return snapshotReadersRef.compareAndSet(null, readers);
    }

    public boolean releaseSnapshotReaders() {
        ObjList<TableReader> readers = snapshotReadersRef.getAndSet(null);
        if (readers != null) {
            Misc.freeObjList(readers);
            readers.clear();
            return true;
        }
        return false;
    }

    private void rename0(Path path, CharSequence tableName, Path otherPath, CharSequence to) {
        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence root = configuration.getRoot();

        if (TableUtils.exists(ff, path, root, tableName) != TableUtils.TABLE_EXISTS) {
            LOG.error().$('\'').utf8(tableName).$("' does not exist. Rename failed.").$();
            throw CairoException.instance(0).put("Rename failed. Table '").put(tableName).put("' does not exist");
        }

        path.of(root).concat(tableName).$();
        otherPath.of(root).concat(to).$();

        if (ff.exists(otherPath)) {
            LOG.error().$("rename target exists [from='").$(tableName).$("', to='").$(otherPath).$("']").$();
            throw CairoException.instance(0).put("Rename target exists");
        }

        if (!ff.rename(path, otherPath)) {
            int error = ff.errno();
            LOG.error().$("rename failed [from='").$(path).$("', to='").$(otherPath).$("', error=").$(error).$(']').$();
            throw CairoException.instance(error).put("Rename failed");
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
            boolean useful = false;
            while (tick()) {
                // process and drain cmd queue
                useful = true;
            }
            if (last + checkInterval < t) {
                last = t;
                return useful | releaseInactive();
            }
            return useful;
        }
    }
}
