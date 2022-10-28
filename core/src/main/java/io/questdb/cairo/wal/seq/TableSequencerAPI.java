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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.*;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.cairo.wal.TableNameRegistry;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class TableSequencerAPI implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableSequencerAPI.class);
    private final CairoConfiguration configuration;
    private final Function<CharSequence, TableSequencerEntry> createSequencerInstanceLambda;
    private final Function<CharSequence, WalWriterPool> createWalPoolLambda;
    private final CairoEngine engine;
    private final long inactiveTtlUs;
    private final boolean mangleTableSystemNames;
    private final int recreateDistressedSequencerAttempts;
    private final ConcurrentHashMap<TableSequencerEntry> seqRegistry = new ConcurrentHashMap<>();
    private final TableNameRegistry tableNameRegistry = new TableNameRegistry();
    private final ConcurrentHashMap<WalWriterPool> walRegistry = new ConcurrentHashMap<>();
    private volatile boolean closed;

    public TableSequencerAPI(CairoEngine engine, CairoConfiguration configuration) {
        this.configuration = configuration;
        this.engine = engine;
        this.mangleTableSystemNames = configuration.mangleTableSystemNames();
        this.createSequencerInstanceLambda = this::createSequencerInstance;
        this.createWalPoolLambda = this::createWalPool;
        this.inactiveTtlUs = configuration.getInactiveWalWriterTTL() * 1000;
        this.tableNameRegistry.reloadTableNameCache(configuration);
        this.recreateDistressedSequencerAttempts = configuration.getWalRecreateDistressedSequencerAttempts();
    }

    @Override
    public void close() {
        closed = true;
        releaseAll();
        Misc.free(tableNameRegistry);
    }

    public void copyMetadataTo(final String tableName, final CharSequence systemTableName, final SequencerMetadata metadata) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(systemTableName, SequencerLockType.READ)) {
            try {
                tableSequencer.copyMetadataTo(metadata, tableName);
            } finally {
                tableSequencer.unlockRead();
            }
        }
    }

    public void dropTable(String tableName, String systemTableName, boolean failedCreate) {
        if (tableNameRegistry.removeName(tableName, systemTableName)) {
            try (TableSequencerImpl seq = openSequencerLocked(systemTableName, SequencerLockType.WRITE)) {
                try {
                    seq.dropTable();
                } finally {
                    seq.unlockWrite();
                }
            } catch (CairoException e) {
                if (!failedCreate) {
                    throw e;
                }
            }

            final WalWriterPool pool = walRegistry.get(systemTableName);
            if (pool != null) {
                pool.releaseAll(Long.MAX_VALUE);
                walRegistry.remove(systemTableName);
            }
        }
    }

    public void forAllWalTables(final RegisteredTable callback) {
        for (CharSequence systemTableName : tableNameRegistry.getTableSystemNames()) {
            long lastTxn;
            int tableId;
            try {
                try (TableSequencerImpl sequencer = openSequencerLocked(systemTableName, SequencerLockType.NONE)) {
                    lastTxn = sequencer.lastTxn();
                    tableId = sequencer.getTableId();
                } catch (CairoException e) {
                    LOG.critical().$("could not open sequencer for table [name=").utf8(systemTableName)
                            .$(", errno=").$(e.getErrno())
                            .$(", error=").$(e.getFlyweightMessage()).I$();
                    continue;
                }
                callback.onTable(tableId, Chars.toString(systemTableName), lastTxn);
            } catch (CairoException e) {
                LOG.error().$("failed process WAL table [name=").utf8(systemTableName)
                        .$(", errno=").$(e.getErrno())
                        .$(", error=").$(e.getFlyweightMessage()).I$();
            }
        }
    }

    public @NotNull TransactionLogCursor getCursor(final CharSequence tableName, long seqTxn) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableName, SequencerLockType.READ)) {
            TransactionLogCursor cursor;
            try {
                cursor = tableSequencer.getTransactionLogCursor(seqTxn);
            } finally {
                tableSequencer.unlockRead();
            }
            return cursor;
        }
    }

    @NotNull
    public String getDefaultTableName(CharSequence tableName) {
        if (!mangleTableSystemNames) {
            return Chars.toString(tableName);
        }
        return tableName.toString() + TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
    }

    public @NotNull TableMetadataChangeLog getMetadataChangeLogCursor(final CharSequence tableName, long structureVersionLo) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableName, SequencerLockType.READ)) {
            TableMetadataChangeLog metadataChangeLog;
            try {
                metadataChangeLog = tableSequencer.getMetadataChangeLogCursor(structureVersionLo);
            } finally {
                tableSequencer.unlockRead();
            }
            return metadataChangeLog;
        }
    }

    public int getNextWalId(final CharSequence tableName) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableName, SequencerLockType.READ)) {
            int walId;
            try {
                walId = tableSequencer.getNextWalId();
            } finally {
                tableSequencer.unlockRead();
            }
            return walId;
        }
    }

    public String getSystemTableName(CharSequence tableName) {
        return tableNameRegistry.getTableSystemName(tableName);
    }

    public String getSystemTableNameOrDefault(final CharSequence tableName) {
        final String systemName = tableNameRegistry.getSystemName(tableName);
        if (systemName != null) {
            return systemName;
        }

        return getDefaultTableName(tableName);
    }

    public String getTableNameBySystemName(CharSequence systemTableName) {
        return tableNameRegistry.getTableNameBySystemName(systemTableName);
    }

    public @NotNull WalWriter getWalWriter(final CharSequence tableName) {
        return getWalPool(tableName).get();
    }

    public boolean isSuspended(final CharSequence tableName) {
        try (TableSequencerImpl sequencer = openSequencerLocked(tableName, SequencerLockType.READ)) {
            boolean isSuspended;
            try {
                isSuspended = sequencer.isSuspended();
            } finally {
                sequencer.unlockRead();
            }
            return isSuspended;
        }
    }

    public boolean isTableDropped(CharSequence systemTableName) {
        return tableNameRegistry.isTableDropped(systemTableName);
    }

    public long lastTxn(final CharSequence tableName) {
        try (TableSequencerImpl sequencer = openSequencerLocked(tableName, SequencerLockType.READ)) {
            long lastTxn;
            try {
                lastTxn = sequencer.lastTxn();
            } finally {
                sequencer.unlockRead();
            }
            return lastTxn;
        }
    }

    public long nextStructureTxn(final CharSequence tableName, long structureVersion, AlterOperation operation) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableName, SequencerLockType.WRITE)) {
            long txn;
            try {
                txn = tableSequencer.nextStructureTxn(structureVersion, operation);
            } finally {
                tableSequencer.unlockWrite();
            }
            return txn;
        }
    }

    public long nextTxn(final CharSequence tableName, int walId, long expectedSchemaVersion, int segmentId, long segmentTxn) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableName, SequencerLockType.WRITE)) {
            long txn;
            try {
                txn = tableSequencer.nextTxn(expectedSchemaVersion, walId, segmentId, segmentTxn);
            } finally {
                tableSequencer.unlockWrite();
            }
            return txn;
        }
    }

    public void registerTable(int tableId, final TableStructure tableStructure, String systemTableName) {
        //noinspection EmptyTryBlock
        try (TableSequencerImpl ignore = createSequencer(tableId, tableStructure, systemTableName)) {
        }
    }

    @Nullable
    public String registerTableName(String tableName, int tableId) {
        String str = tableNameRegistry.getSystemName(tableName);
        if (str != null) {
            return str;
        }

        StringSink sink = Misc.getThreadLocalBuilder();
        sink.put(tableName).put(TableUtils.SYSTEM_TABLE_NAME_SUFFIX).put(tableId);
        return tableNameRegistry.registerName(Chars.toString(tableName), sink.toString());
    }

    public boolean releaseAll() {
        return releaseAll(Long.MAX_VALUE);
    }

    public boolean releaseInactive() {
        return releaseAll(configuration.getMicrosecondClock().getTicks() - inactiveTtlUs);
    }

    public void removeTableSystemName(CharSequence tableSystemName) {
        tableNameRegistry.removeTableSystemName(tableSystemName);
    }

    public void rename(CharSequence tableName, CharSequence newName, String toString) {
        tableNameRegistry.rename(tableName, newName, toString);
    }

    public void reopen() {
        tableNameRegistry.reloadTableNameCache(configuration);
        closed = false;
    }

    @TestOnly
    public void setDistressed(String tableName) {
        try (TableSequencerImpl sequencer = openSequencerLocked(tableName, SequencerLockType.WRITE)) {
            try {
                sequencer.setDistressed();
            } finally {
                sequencer.unlockWrite();
            }
        }
    }

    public void suspendTable(final CharSequence tableName) {
        try (TableSequencerImpl sequencer = openSequencerLocked(tableName, SequencerLockType.WRITE)) {
            try {
                sequencer.suspendTable();
            } finally {
                sequencer.unlockWrite();
            }
        }
    }

    private @NotNull TableSequencerImpl createSequencer(int tableId, final TableStructure tableStructure, String tableSystemName) {
        throwIfClosed();
        return seqRegistry.compute(tableSystemName, (key, value) -> {
            if (value == null) {
                TableSequencerEntry sequencer = new TableSequencerEntry(this, this.engine, tableSystemName);
                sequencer.create(tableId, tableStructure);
                sequencer.open();
                return sequencer;
            }
            return value;
        });
    }

    private TableSequencerEntry createSequencerInstance(CharSequence tableNameStr) {
        TableSequencerEntry sequencer = new TableSequencerEntry(this, this.engine, (String) tableNameStr);
        sequencer.open();
        return sequencer;
    }

    private WalWriterPool createWalPool(CharSequence charSequence) {
        return new WalWriterPool((String) charSequence, this, engine.getConfiguration());
    }

    @NotNull
    private TableSequencerEntry getTableSequencerEntryLocked(CharSequence tableName, SequencerLockType lock) {
        String tableNameStr = Chars.toString(tableName);
        int attempt = 0;
        TableSequencerEntry entry;

        while (attempt < recreateDistressedSequencerAttempts) {
            throwIfClosed();
            entry = seqRegistry.computeIfAbsent(tableNameStr, this.createSequencerInstanceLambda);
            if (lock == SequencerLockType.READ) {
                entry.readLock();
            }
            if (lock == SequencerLockType.WRITE) {
                entry.writeLock();
            }

            if (!entry.isDistressed()) {
                return entry;
            } else {
                if (lock == SequencerLockType.READ) {
                    entry.unlockRead();
                } else if (lock == SequencerLockType.WRITE) {
                    entry.unlockWrite();
                }
            }
            attempt++;
        }

        throw CairoException.critical(0).put("sequencer is distressed [table=").put(tableName).put(']');
    }

    private @NotNull WalWriterPool getWalPool(final CharSequence tableName) {
        throwIfClosed();
        return walRegistry.computeIfAbsent(Chars.toString(tableName), this.createWalPoolLambda);
    }

    private @NotNull TableSequencerImpl openSequencerLocked(final CharSequence tableName, SequencerLockType lock) {
        throwIfClosed();

        TableSequencerEntry entry = seqRegistry.get(tableName);
        if (entry != null) {
            if (lock == SequencerLockType.READ) {
                entry.readLock();
            } else if (lock == SequencerLockType.WRITE) {
                entry.writeLock();
            }

            if (!entry.isDistressed()) {
                return entry;
            } else {
                if (lock == SequencerLockType.READ) {
                    entry.unlockRead();
                } else if (lock == SequencerLockType.WRITE) {
                    entry.unlockWrite();
                }
            }
        }

        return getTableSequencerEntryLocked(tableName, lock);
    }

    protected boolean releaseAll(long deadline) {
        boolean r0 = releaseWalWriters(deadline);
        boolean r1 = releaseEntries(deadline);
        return r0 || r1;
    }

    private boolean releaseEntries(long deadline) {
        if (seqRegistry.size() == 0) {
            // nothing to release
            return true;
        }
        boolean removed = false;
        final Iterator<TableSequencerEntry> iterator = seqRegistry.values().iterator();
        while (iterator.hasNext()) {
            final TableSequencerEntry sequencer = iterator.next();
            if (deadline >= sequencer.releaseTime) {
                sequencer.pool = null;
                sequencer.close();
                removed = true;
                iterator.remove();
            }
        }
        return removed;
    }

    private boolean releaseWalWriters(long deadline) {
        if (walRegistry.size() == 0) {
            // nothing to release
            return true;
        }
        boolean removed = false;
        final Iterator<WalWriterPool> iterator = walRegistry.values().iterator();
        while (iterator.hasNext()) {
            final WalWriterPool pool = iterator.next();
            removed = pool.releaseAll(deadline);
            if (deadline == Long.MAX_VALUE && pool.size() == 0) {
                iterator.remove();
            }
        }
        return removed;
    }

    private boolean returnToPool(final TableSequencerEntry entry) {
        if (closed) {
            return false;
        }
        entry.releaseTime = configuration.getMicrosecondClock().getTicks();
        return true;
    }

    private void throwIfClosed() {
        if (closed) {
            LOG.info().$("is closed").$();
            throw PoolClosedException.INSTANCE;
        }
    }

    enum SequencerLockType {
        WRITE,
        READ,
        NONE
    }

    @FunctionalInterface
    public interface RegisteredTable {
        void onTable(int tableId, final String tableName, long lastTxn);
    }

    private static class TableSequencerEntry extends TableSequencerImpl {
        private TableSequencerAPI pool;
        private volatile long releaseTime = Long.MAX_VALUE;

        TableSequencerEntry(TableSequencerAPI pool, CairoEngine engine, String tableName) {
            super(engine, tableName);
            this.pool = pool;
        }

        @Override
        public void close() {
            if (pool != null && !pool.closed) {
                if (!isDistressed() && pool != null) {
                    if (pool.returnToPool(this)) {
                        return;
                    }
                }

                // Sequencer is distressed, close before removing from the pool.
                super.close();
                if (pool != null) {
                    pool.seqRegistry.remove(getTableName(), this);
                }
            } else {
                super.close();
            }
        }
    }

    public static class WalWriterPool {
        private final ArrayDeque<Entry> cache = new ArrayDeque<>();
        private final CairoConfiguration configuration;
        private final ReentrantLock lock = new ReentrantLock();
        private final String tableName;
        private final TableSequencerAPI tableSequencerAPI;
        private volatile boolean closed;

        public WalWriterPool(String tableName, TableSequencerAPI tableSequencerAPI, CairoConfiguration configuration) {
            this.tableName = tableName;
            this.tableSequencerAPI = tableSequencerAPI;
            this.configuration = configuration;
        }

        public Entry get() {
            lock.lock();
            try {
                Entry obj;
                do {
                    obj = cache.poll();

                    if (obj == null) {
                        obj = new Entry(tableName, tableSequencerAPI, configuration, this);
                    } else {
                        if (!obj.goActive()) {
                            obj = Misc.free(obj);
                        } else {
                            obj.reset();
                        }
                    }
                } while (obj == null);

                return obj;
            } finally {
                lock.unlock();
            }
        }

        public boolean returnToPool(Entry obj) {
            assert obj != null;
            lock.lock();
            try {
                if (closed) {
                    return false;
                } else {
                    try {
                        obj.rollback();
                    } catch (Throwable e) {
                        LOG.error().$("could not rollback WAL writer [table=").$(obj.getTableName())
                                .$(", walId=").$(obj.getWalId())
                                .$(", error=").$(e).$();
                        obj.close();
                        throw e;
                    }
                    cache.push(obj);
                    obj.releaseTime = configuration.getMicrosecondClock().getTicks();
                    return true;
                }
            } finally {
                lock.unlock();
            }
        }

        public int size() {
            lock.lock();
            try {
                return cache.size();
            } finally {
                lock.unlock();
            }
        }

        protected boolean releaseAll(long deadline) {
            boolean removed = false;
            lock.lock();
            try {
                Iterator<Entry> iterator = cache.iterator();
                while (iterator.hasNext()) {
                    final Entry e = iterator.next();
                    if (deadline >= e.releaseTime) {
                        removed = true;
                        e.doClose(true);
                        e.pool = null;
                        iterator.remove();
                    }
                }
                if (deadline == Long.MAX_VALUE) {
                    this.closed = true;
                }
            } finally {
                lock.unlock();
            }
            return removed;
        }

        public static class Entry extends WalWriter {
            private WalWriterPool pool;
            private volatile long releaseTime = Long.MAX_VALUE;

            public Entry(String systemTableName, TableSequencerAPI tableSequencerAPI, CairoConfiguration configuration, WalWriterPool pool) {
                super(systemTableName, tableSequencerAPI.getTableNameBySystemName(systemTableName), tableSequencerAPI, configuration);
                this.pool = pool;
            }

            @Override
            public void close() {
                if (isOpen()) {
                    if (!isDistressed() && pool != null) {
                        if (pool.returnToPool(this)) {
                            return;
                        }
                    }
                    super.close();
                }
            }

            public void reset() {
                this.releaseTime = Long.MAX_VALUE;
            }
        }
    }
}
