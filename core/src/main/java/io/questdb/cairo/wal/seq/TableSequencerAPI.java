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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static io.questdb.cairo.wal.WalUtils.SEQ_DIR;

public class TableSequencerAPI implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(TableSequencerAPI.class);
    private final ConcurrentHashMap<TableSequencerEntry> seqRegistry = new ConcurrentHashMap<>();
    private final CairoEngine engine;
    private final Function<CharSequence, TableSequencerEntry> createSequencerInstanceLambda;
    private final CairoConfiguration configuration;
    private final long inactiveTtlUs;
    private final int recreateDistressedSequencerAttempts;
    private volatile boolean closed;

    public TableSequencerAPI(CairoEngine engine, CairoConfiguration configuration) {
        this.configuration = configuration;
        this.engine = engine;
        this.createSequencerInstanceLambda = this::createSequencerInstance;
        this.inactiveTtlUs = configuration.getInactiveWalWriterTTL() * 1000;
        this.recreateDistressedSequencerAttempts = configuration.getWalRecreateDistressedSequencerAttempts();
    }

    @Override
    public void close() {
        closed = true;
        releaseAll();
    }

    public void getTableMetadata(final CharSequence tableName, final TableRecordMetadataSink sink, boolean compress) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableName, SequencerLockType.READ)) {
            tableSequencer.getTableMetadata(sink, compress);
        }
    }

    public void forAllWalTables(final RegisteredTable callback) {
        final CharSequence root = configuration.getRoot();
        final FilesFacade ff = configuration.getFilesFacade();

        // todo: too much GC here
        // this will be replaced with table name registry when drop WAL table implemented
        try (Path path = new Path().of(root).slash$()) {
            final StringSink nameSink = new StringSink();
            int rootLen = path.length();
            ff.iterateDir(path, (name, type) -> {
                if (Files.isDir(name, type, nameSink)) {
                    path.trimTo(rootLen);
                    if (isWalTable(nameSink, path, ff)) {
                        long lastTxn;
                        int tableId;
                        try (TableSequencer tableSequencer = openSequencerLocked(nameSink, SequencerLockType.NONE)) {
                            lastTxn = tableSequencer.lastTxn();
                            tableId = tableSequencer.getTableId();
                        } catch (CairoException ex) {
                            LOG.critical().$("could not open table sequencer [table=").$(nameSink).$(", errno=").$(ex.getErrno())
                                    .$(", error=").$((Throwable) ex).I$();
                            return;
                        }
                        try {
                            callback.onTable(tableId, nameSink, lastTxn);
                        } catch (CairoException ex) {
                            LOG.critical().$("could not process table sequencer [table=").$(nameSink).$(", errno=").$(ex.getErrno())
                                    .$(", error=").$((Throwable) ex).I$();
                        }
                    }
                }
            });
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

    public boolean hasSequencer(final CharSequence tableName) {
        TableSequencer tableSequencer = seqRegistry.get(tableName);
        if (tableSequencer != null) {
            return true;
        }
        return isWalTable(tableName, configuration.getRoot(), configuration.getFilesFacade());
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

    public boolean releaseAll() {
        return releaseAll(Long.MAX_VALUE);
    }

    public void reloadMetadataConditionally(
            final CharSequence tableName,
            long expectedStructureVersion,
            TableRecordMetadataSink sink,
            boolean compress
    ) {
        try (TableSequencer tableSequencer = openSequencerLocked(tableName, SequencerLockType.READ)) {
            if (tableSequencer.getStructureVersion() != expectedStructureVersion) {
                tableSequencer.getTableMetadata(sink, compress);
            }
        }
    }

    public void registerTable(int tableId, final TableStructure tableStructure) {
        //noinspection EmptyTryBlock
        try (TableSequencerImpl ignore = createSequencer(tableId, tableStructure)) {
        }
    }

    public boolean releaseInactive() {
        return releaseAll(configuration.getMicrosecondClock().getTicks() - inactiveTtlUs);
    }

    public void reopen() {
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

    // Check if sequencer files exist, e.g. is it WAL table sequencer must exist
    private static boolean isWalTable(final CharSequence tableName, final CharSequence root, final FilesFacade ff) {
        Path path = Path.getThreadLocal2(root);
        return isWalTable(tableName, path, ff);
    }

    private static boolean isWalTable(final CharSequence tableName, final Path root, final FilesFacade ff) {
        root.concat(tableName).concat(SEQ_DIR);
        return ff.exists(root.$());
    }

    private @NotNull TableSequencerImpl createSequencer(int tableId, final TableStructure tableStructure) {
        throwIfClosed();
        final String tableName = Chars.toString(tableStructure.getTableName());
        return seqRegistry.compute(tableName, (key, value) -> {
            if (value == null) {
                TableSequencerEntry sequencer = new TableSequencerEntry(this, this.engine, tableName);
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

    private boolean returnToPool(final TableSequencerEntry entry) {
        if (closed) {
            return false;
        }
        entry.releaseTime = configuration.getMicrosecondClock().getTicks();
        return true;
    }

    protected boolean releaseAll(long deadline) {
        return releaseEntries(deadline);
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
        void onTable(int tableId, final CharSequence tableName, long lastTxn);
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
}
