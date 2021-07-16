/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.pool.ReaderPool;
import io.questdb.cairo.pool.WriterPool;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.sql.ReaderOutOfDateException;
import io.questdb.cairo.vm.AppendOnlyVirtualMemory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.cairo.ColumnType.SYMBOL;

public class CairoEngine implements Closeable, WriterSource {
    private static final Log LOG = LogFactory.getLog(CairoEngine.class);
    public static final String BUSY_READER = "busyReader";

    private final WriterPool writerPool;
    private final ReaderPool readerPool;
    private final CairoConfiguration configuration;
    private final WriterMaintenanceJob writerMaintenanceJob;
    private final MessageBus messageBus;
    private final RingQueue<TelemetryTask> telemetryQueue;
    private final MPSequence telemetryPubSeq;
    private final SCSequence telemetrySubSeq;
    private final long tableIdMemSize;
    private long tableIdFd = -1;
    private long tableIdMem = 0;

    public CairoEngine(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.messageBus = new MessageBusImpl(configuration);
        this.writerPool = new WriterPool(configuration, messageBus);
        this.readerPool = new ReaderPool(configuration);
        this.writerMaintenanceJob = new WriterMaintenanceJob(configuration);
        if (configuration.getTelemetryConfiguration().getEnabled()) {
            this.telemetryQueue = new RingQueue<>(TelemetryTask::new, configuration.getTelemetryConfiguration().getQueueCapacity());
            this.telemetryPubSeq = new MPSequence(telemetryQueue.getCapacity());
            this.telemetrySubSeq = new SCSequence();
            telemetryPubSeq.then(telemetrySubSeq).then(telemetryPubSeq);
        } else {
            this.telemetryQueue = null;
            this.telemetryPubSeq = null;
            this.telemetrySubSeq = null;
        }
        this.tableIdMemSize = Files.PAGE_SIZE;
        openTableId();
        try {
            new EngineMigration(this, configuration).migrateEngineTo(ColumnType.VERSION);
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    public void openTableId() {
        freeTableId();
        FilesFacade ff = configuration.getFilesFacade();
        Path path = Path.getThreadLocal(configuration.getRoot()).concat(TableUtils.TAB_INDEX_FILE_NAME).$();
        tableIdFd = TableUtils.openFileRWOrFail(ff, path);
        final long fileSize = ff.length(tableIdFd);
        if (fileSize < Long.BYTES) {
            if (!ff.allocate(tableIdFd, Files.PAGE_SIZE)) {
                ff.close(tableIdFd);
                throw CairoException.instance(ff.errno()).put("Could not allocate [file=").put(path).put(", actual=").put(fileSize).put(", desired=").put(this.tableIdMemSize).put(']');
            }
        }

        this.tableIdMem = ff.mmap(tableIdFd, tableIdMemSize, 0, Files.MAP_RW);
        if (tableIdMem == -1) {
            ff.close(tableIdFd);
            throw CairoException.instance(ff.errno()).put("Could not mmap [file=").put(path).put(']');
        }
    }

    public boolean clear() {
        boolean b1 = readerPool.releaseAll();
        boolean b2 = writerPool.releaseAll();
        return b1 & b2;
    }

    @Override
    public void close() {
        Misc.free(writerPool);
        Misc.free(readerPool);
        freeTableId();
        Misc.free(messageBus);
    }

    public void createTable(
            CairoSecurityContext securityContext,
            AppendOnlyVirtualMemory mem,
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
            AppendOnlyVirtualMemory mem,
            Path path,
            TableStructure struct
    ) {
        securityContext.checkWritePermission();
        TableUtils.createTable(
                configuration.getFilesFacade(),
                mem,
                path,
                configuration.getRoot(),
                struct,
                configuration.getMkDirMode(),
                (int) getNextTableId()
        );
    }

    public void freeTableId() {
        if (tableIdMem != 0) {
            configuration.getFilesFacade().munmap(tableIdMem, tableIdMemSize);
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
        return new TableWriter(configuration, tableName, messageBus, true, DefaultLifecycleManager.INSTANCE, backupDirName);
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
        return getReader(securityContext, tableName,  TableUtils.ANY_TABLE_ID, TableUtils.ANY_TABLE_VERSION);
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

    public Job getWriterMaintenanceJob() {
        return writerMaintenanceJob;
    }

    public CharSequence lock(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            CharSequence lockReason
    ) {
        assert null != lockReason;
        securityContext.checkWritePermission();

        CharSequence lockedReason = writerPool.lock(tableName, lockReason);
        if (null == lockedReason) {
            boolean locked = readerPool.lock(tableName);
            if (locked) {
                LOG.info().$("locked [table=`").$(tableName).$("`, thread=").$(Thread.currentThread().getId()).$(']').$();
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

    public boolean migrateNullFlag(CairoSecurityContext cairoSecurityContext, CharSequence tableName) {
        try (
                TableWriter writer = getWriter(cairoSecurityContext, tableName, "migrateNullFlag");
                TableReader reader = getReader(cairoSecurityContext, tableName)
        ) {
            TableReaderMetadata readerMetadata = reader.getMetadata();
            if (readerMetadata.getVersion() < 416) {
                LOG.info().$("migrating null flag for symbols [table=").utf8(tableName).$(']').$();
                for (int i = 0, count = reader.getColumnCount(); i < count; i++) {
                    if (readerMetadata.getColumnType(i) == SYMBOL) {
                        LOG.info().$("updating null flag [column=").utf8(readerMetadata.getColumnName(i)).$(']').$();
                        writer.getSymbolMapWriter(i).updateNullFlag(reader.hasNull(i));
                    }
                }
                writer.updateMetadataVersion();
                LOG.info().$("migrated null flag for symbols [table=").utf8(tableName).$(", tableVersion=").$(ColumnType.VERSION).$(']').$();
                return true;
            }
        }
        return false;
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

    public void unlock(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            @Nullable TableWriter writer,
            boolean newTable
    ) {
        readerPool.unlock(tableName);
        writerPool.unlock(tableName, writer, newTable);
        LOG.info().$("unlocked [table=`").$(tableName).$("`]").$();
    }

    public void unlockReaders(CharSequence tableName) {
        readerPool.unlock(tableName);
    }

    public void unlockWriter(CharSequence tableName) {
        writerPool.unlock(tableName);
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

    private class WriterMaintenanceJob extends SynchronizedJob {

        private final MicrosecondClock clock;
        private final long checkInterval;
        private long last = 0;

        public WriterMaintenanceJob(CairoConfiguration configuration) {
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
