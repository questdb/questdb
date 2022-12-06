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
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.function.Function;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;
import static io.questdb.cairo.TableUtils.TXNLOG_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.SEQ_DIR;
import static io.questdb.cairo.wal.WalUtils.SEQ_META_TABLE_ID;
import static io.questdb.cairo.wal.seq.TableTransactionLog.MAX_TXN_OFFSET;

public class TableSequencerAPI implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(TableSequencerAPI.class);
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final long inactiveTtlUs;
    private final Function<CharSequence, TableSequencerEntry> openSequencerInstanceLambda;
    private final int recreateDistressedSequencerAttempts;
    private final ConcurrentHashMap<TableSequencerEntry> seqRegistry = new ConcurrentHashMap<>();
    private volatile boolean closed;

    public TableSequencerAPI(CairoEngine engine, CairoConfiguration configuration) {
        this.configuration = configuration;
        this.engine = engine;
        this.openSequencerInstanceLambda = this::openSequencerInstance;
        this.inactiveTtlUs = configuration.getInactiveWalWriterTTL() * 1000;
        this.recreateDistressedSequencerAttempts = configuration.getWalRecreateDistressedSequencerAttempts();
    }

    // kept visible for tests
    public static boolean isWalTable(final CharSequence tableName, final Path root, final FilesFacade ff) {
        root.concat(tableName).concat(SEQ_DIR);
        return ff.exists(root.$());
    }

    @Override
    public void close() {
        closed = true;
        releaseAll();
    }

    public void forAllWalTables(RegisteredTable callback) {
        final CharSequence root = configuration.getRoot();
        final FilesFacade ff = configuration.getFilesFacade();

        // this will be replaced with table name registry when drop WAL table implemented
        try (Path path = new Path().of(root).slash$()) {
            final StringSink nameSink = new StringSink();
            int rootLen = path.length();
            ff.iterateDir(path, (name, type) -> {
                if (Files.isDir(name, type, nameSink)) {
                    path.trimTo(rootLen);
                    if (isWalTable(nameSink, path, ff)) {
                        path.trimTo(rootLen);
                        int tableId;
                        long lastTxn;
                        try {
                            if (!seqRegistry.containsKey(nameSink)) {
                                // Fast path.
                                // The following calls are racy, i.e. there might be a sequencer modifying both
                                // metadata and log concurrently as we read the values. It's ok since we iterate
                                // through the WAL tables periodically, so eventually we should see the updates.
                                path.concat(nameSink).concat(SEQ_DIR);
                                long fdMeta = -1;
                                long fdTxn = -1;
                                try {
                                    fdMeta = openFileRO(ff, path, META_FILE_NAME);
                                    fdTxn = openFileRO(ff, path, TXNLOG_FILE_NAME);
                                    tableId = ff.readNonNegativeInt(fdMeta, SEQ_META_TABLE_ID);
                                    lastTxn = ff.readNonNegativeLong(fdTxn, MAX_TXN_OFFSET);
                                } finally {
                                    if (fdMeta > -1) {
                                        ff.close(fdMeta);
                                    }
                                    if (fdTxn > -1) {
                                        ff.close(fdTxn);
                                    }
                                }
                            } else {
                                // Slow path.
                                try (TableSequencer tableSequencer = openSequencerLocked(nameSink, SequencerLockType.NONE)) {
                                    lastTxn = tableSequencer.lastTxn();
                                    tableId = tableSequencer.getTableId();
                                }
                            }
                        } catch (CairoException ex) {
                            LOG.critical().$("could not read WAL table metadata [table=").utf8(nameSink).$(", errno=").$(ex.getErrno())
                                    .$(", error=").$((Throwable) ex).I$();
                            return;
                        }
                        if (tableId < 0 || lastTxn < 0) {
                            LOG.critical().$("could not read WAL table metadata [table=").utf8(nameSink).$(", tableId=").$(tableId)
                                    .$(", lastTxn=").$(lastTxn).I$();
                            return;
                        }
                        try {
                            callback.onTable(tableId, nameSink, lastTxn);
                        } catch (CairoException ex) {
                            LOG.critical().$("could not process table sequencer [table=").utf8(nameSink).$(", errno=").$(ex.getErrno())
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

    public long getTableMetadata(final CharSequence tableName, final TableRecordMetadataSink sink) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableName, SequencerLockType.READ)) {
            try {
                return tableSequencer.getTableMetadata(sink);
            } finally {
                tableSequencer.unlockRead();
            }
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

    public long nextTxn(final CharSequence tableName, int walId, long expectedSchemaVersion, int segmentId, int segmentTxn) {
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

    public void registerTable(int tableId, final TableStructure tableStructure) {
        String tableNameStr = Chars.toString(tableStructure.getTableName());
        try (
                TableSequencerImpl tableSequencer = getTableSequencerEntry(tableNameStr, SequencerLockType.WRITE, (tableName) -> {
                    TableSequencerEntry sequencer = new TableSequencerEntry(this, this.engine, tableNameStr);
                    sequencer.create(tableId, tableStructure);
                    sequencer.open();
                    return sequencer;
                })
        ) {
            tableSequencer.unlockWrite();
        }
    }

    public boolean releaseAll() {
        return releaseAll(Long.MAX_VALUE);
    }

    public boolean releaseInactive() {
        return releaseAll(configuration.getMicrosecondClock().getTicks() - inactiveTtlUs);
    }

    public void reloadMetadataConditionally(
            final CharSequence tableName,
            long expectedStructureVersion,
            TableRecordMetadataSink sink
    ) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableName, SequencerLockType.READ)) {
            try {
                if (tableSequencer.getStructureVersion() != expectedStructureVersion) {
                    tableSequencer.getTableMetadata(sink);
                }
            } finally {
                tableSequencer.unlockRead();
            }
        }
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

    private static long openFileRO(FilesFacade ff, Path path, CharSequence fileName) {
        final int rootLen = path.length();
        path.concat(fileName).$();
        try {
            return TableUtils.openRO(ff, path, LOG);
        } finally {
            path.trimTo(rootLen);
        }
    }

    @NotNull
    private TableSequencerEntry getTableSequencerEntry(String tableName, SequencerLockType lock, Function<CharSequence, TableSequencerEntry> getSequencerLambda) {
        TableSequencerEntry entry;
        int attempt = 0;
        while (attempt < recreateDistressedSequencerAttempts) {
            throwIfClosed();
            entry = seqRegistry.computeIfAbsent(tableName, getSequencerLambda);
            if (lock == SequencerLockType.READ) {
                entry.readLock();
            } else if (lock == SequencerLockType.WRITE) {
                entry.writeLock();
            }

            boolean isDistressed = entry.isDistressed();
            if (!isDistressed && !entry.isClosed()) {
                return entry;
            } else {
                if (lock == SequencerLockType.READ) {
                    entry.unlockRead();
                } else if (lock == SequencerLockType.WRITE) {
                    entry.unlockWrite();
                }
            }
            if (isDistressed) {
                attempt++;
            }
        }

        throw CairoException.critical(0).put("sequencer is distressed [table=").put(tableName).put(']');
    }

    private TableSequencerEntry openSequencerInstance(CharSequence tableNameStr) {
        TableSequencerEntry sequencer = new TableSequencerEntry(this, this.engine, Chars.toString(tableNameStr));
        sequencer.open();
        return sequencer;
    }

    @NotNull
    private TableSequencerEntry openSequencerLocked(CharSequence tableName, SequencerLockType lock) {
        return getTableSequencerEntry(Chars.toString(tableName), lock, this.openSequencerInstanceLambda);
    }

    private boolean releaseEntries(long deadline) {
        if (seqRegistry.size() == 0) {
            // nothing to release
            return true;
        }
        boolean removed = false;
        for (CharSequence tableSystemName : seqRegistry.keySet()) {
            String tableNameStr = (String) tableSystemName;
            final TableSequencerEntry sequencer = seqRegistry.get(tableNameStr);
            if (sequencer != null && deadline >= sequencer.releaseTime && !sequencer.isClosed()) {
                assert tableNameStr.equals(sequencer.getTableName());
                // Remove from registry only if this thread closed the instance
                if (sequencer.checkClose()) {
                    LOG.info().$("releasing idle table sequencer [table=").$(tableSystemName).I$();
                    seqRegistry.remove(tableNameStr, sequencer);
                    removed = true;
                }
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

    protected boolean releaseAll(long deadline) {
        return releaseEntries(deadline);
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
        private final TableSequencerAPI pool;
        private volatile long releaseTime = Long.MAX_VALUE;

        TableSequencerEntry(TableSequencerAPI pool, CairoEngine engine, String tableName) {
            super(engine, tableName);
            this.pool = pool;
        }

        @Override
        public void close() {
            if (!pool.closed) {
                if (!isDistressed()) {
                    releaseTime = pool.configuration.getMicrosecondClock().getTicks();
                } else {
                    // Sequencer is distressed, close before removing from the pool.
                    // Remove from registry only if this thread closed the instance.
                    if (checkClose()) {
                        LOG.info().$("closed distressed table sequencer [table=").$(getTableName()).I$();
                        pool.seqRegistry.remove(getTableName(), this);
                    }
                }
            } else {
                super.close();
            }
        }
    }
}
