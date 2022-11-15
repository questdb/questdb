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
import io.questdb.cairo.wal.TableNameRecord;
import io.questdb.cairo.wal.TableNameRegistry;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.function.Function;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.*;
import static io.questdb.cairo.wal.seq.TableTransactionLog.MAX_TXN_OFFSET;

public class TableSequencerAPI implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(TableSequencerAPI.class);
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final long inactiveTtlUs;
    private final Function<CharSequence, TableSequencerEntry> openSequencerInstanceLambda;
    private final int recreateDistressedSequencerAttempts;
    private final ConcurrentHashMap<TableSequencerEntry> seqRegistry = new ConcurrentHashMap<>();
    private final TableNameRegistry tableNameRegistry;
    private volatile boolean closed;

    public TableSequencerAPI(CairoEngine engine, CairoConfiguration configuration) {
        this.configuration = configuration;
        this.engine = engine;
        this.openSequencerInstanceLambda = this::openSequencerInstance;
        this.inactiveTtlUs = configuration.getInactiveWalWriterTTL() * 1000;
        this.recreateDistressedSequencerAttempts = configuration.getWalRecreateDistressedSequencerAttempts();
        this.tableNameRegistry = new TableNameRegistry();
        this.tableNameRegistry.reloadTableNameCache(configuration);
    }

    @Override
    public void close() {
        closed = true;
        releaseAll();
        Misc.free(tableNameRegistry);
    }

    public void deleteNonWalName(CharSequence tableName, String systemTableName) {
        tableNameRegistry.deleteNonWalName(tableName, systemTableName);
    }

    public void deregisterTableName(CharSequence tableName, String systemTableName) {
        tableNameRegistry.removeName(tableName, systemTableName);
    }

    public void dropTable(CharSequence tableName, String systemTableName, boolean failedCreate) {
        if (tableNameRegistry.removeName(tableName, systemTableName)) {
            LOG.info().$("dropped wal table [name=").utf8(tableName).$(", systemTableName=").utf8(systemTableName).I$();
            try (TableSequencerImpl seq = openSequencerLocked(systemTableName, SequencerLockType.WRITE)) {
                try {
                    seq.dropTable();
                } finally {
                    seq.unlockWrite();
                }
            } catch (CairoException e) {
                LOG.info().$("failed to drop wal table [name=").utf8(tableName).$(", systemTableName=").utf8(systemTableName).I$();
                if (!failedCreate) {
                    throw e;
                }
            }
        }
    }

    public void forAllWalTables(final RegisteredTable callback) {
        final CharSequence root = configuration.getRoot();
        final FilesFacade ff = configuration.getFilesFacade();
        Path path = Path.PATH.get();

        for (CharSequence systemTableName : getTableSystemNames()) {
            if (tableNameRegistry.isWalSystemTableName(systemTableName) || tableNameRegistry.isWalTableDropped(systemTableName)) {
                long lastTxn;
                int tableId;

                try {
                    if (!seqRegistry.containsKey(systemTableName)) {
                        // Fast path.
                        // The following calls are racy, i.e. there might be a sequencer modifying both
                        // metadata and log concurrently as we read the values. It's ok since we iterate
                        // through the WAL tables periodically, so eventually we should see the updates.
                        path.of(root).concat(systemTableName).concat(SEQ_DIR);
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
                        try (TableSequencer tableSequencer = openSequencerLocked(systemTableName, SequencerLockType.NONE)) {
                            lastTxn = tableSequencer.lastTxn();
                            tableId = tableSequencer.getTableId();
                        }
                    }
                } catch (CairoException ex) {
                    LOG.critical().$("could not read WAL table metadata [table=").utf8(systemTableName).$(", errno=").$(ex.getErrno())
                            .$(", error=").$((Throwable) ex).I$();
                    continue;
                }

                if (tableId < 0 || lastTxn < 0) {
                    LOG.critical().$("could not read WAL table metadata [table=").utf8(systemTableName).$(", tableId=").$(tableId)
                            .$(", lastTxn=").$(lastTxn).I$();
                    continue;
                }

                try {
                    callback.onTable(tableId, Chars.toString(systemTableName), lastTxn);
                } catch (CairoException ex) {
                    LOG.critical().$("could not process table sequencer [table=").utf8(systemTableName).$(", errno=").$(ex.getErrno())
                            .$(", error=").$((Throwable) ex).I$();
                }
            }
        }
    }

    public @NotNull TransactionLogCursor getCursor(final String systemTableName, long seqTxn) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(systemTableName, SequencerLockType.READ)) {
            TransactionLogCursor cursor;
            try {
                cursor = tableSequencer.getTransactionLogCursor(seqTxn);
            } finally {
                tableSequencer.unlockRead();
            }
            return cursor;
        }
    }

    public @NotNull TableMetadataChangeLog getMetadataChangeLogCursor(final String systemTableName, long structureVersionLo) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(systemTableName, SequencerLockType.READ)) {
            TableMetadataChangeLog metadataChangeLog;
            try {
                metadataChangeLog = tableSequencer.getMetadataChangeLogCursor(structureVersionLo);
            } finally {
                tableSequencer.unlockRead();
            }
            return metadataChangeLog;
        }
    }

    public int getNextWalId(final String systemTableName) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(systemTableName, SequencerLockType.READ)) {
            int walId;
            try {
                walId = tableSequencer.getNextWalId();
            } finally {
                tableSequencer.unlockRead();
            }
            return walId;
        }
    }

    public String getSystemName(final CharSequence tableName) {
        return tableNameRegistry.getSystemName(tableName);
    }

    public void getTableMetadata(final String systemTableName, final TableRecordMetadataSink sink) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(systemTableName, SequencerLockType.READ)) {
            try {
                tableSequencer.getTableMetadata(sink);
            } finally {
                tableSequencer.unlockRead();
            }
        }
    }

    public String getTableNameBySystemName(CharSequence systemTableName) {
        return tableNameRegistry.getTableNameBySystemName(systemTableName);
    }

    public TableNameRecord getTableNameRecord(final CharSequence tableName) {
        return tableNameRegistry.getTableNameRecord(tableName);
    }

    public Iterable<CharSequence> getTableSystemNames() {
        return tableNameRegistry.getTableSystemNames();
    }

    @TestOnly
    public boolean isSuspended(final String systemTableName) {
        try (TableSequencerImpl sequencer = openSequencerLocked(systemTableName, SequencerLockType.READ)) {
            boolean isSuspended;
            try {
                isSuspended = sequencer.isSuspended();
            } finally {
                sequencer.unlockRead();
            }
            return isSuspended;
        }
    }

    public boolean isWalSystemName(String systemTableName) {
        return tableNameRegistry.isWalSystemTableName(systemTableName);
    }

    public boolean isWalTableDropped(String systemTableName) {
        return tableNameRegistry.isWalTableDropped(systemTableName);
    }

    public boolean isWalTableName(CharSequence tableName) {
        return tableNameRegistry.isWalTableName(tableName);
    }

    public long lastTxn(final String tableName) {
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

    public long nextStructureTxn(final String systemTableName, long structureVersion, AlterOperation operation) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(systemTableName, SequencerLockType.WRITE)) {
            long txn;
            try {
                txn = tableSequencer.nextStructureTxn(structureVersion, operation);
            } finally {
                tableSequencer.unlockWrite();
            }
            return txn;
        }
    }

    public long nextTxn(final String systemTableName, int walId, long expectedSchemaVersion, int segmentId, long segmentTxn) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(systemTableName, SequencerLockType.WRITE)) {
            long txn;
            try {
                txn = tableSequencer.nextTxn(expectedSchemaVersion, walId, segmentId, segmentTxn);
            } finally {
                tableSequencer.unlockWrite();
            }
            return txn;
        }
    }

    public void registerTable(int tableId, final TableStructure tableStructure, final String systemTableName) {
        try (
                TableSequencerImpl tableSequencer = getTableSequencerEntry(systemTableName, SequencerLockType.WRITE, (key) -> {
                    String tableName = tableNameRegistry.getTableNameBySystemName(systemTableName);
                    TableSequencerEntry sequencer = new TableSequencerEntry(this, this.engine, systemTableName, tableName);
                    sequencer.create(tableId, tableStructure);
                    sequencer.open();
                    return sequencer;
                })
        ) {
            tableSequencer.unlockWrite();
        }
    }

    @Nullable
    public String registerTableName(CharSequence tableName, int tableId, boolean isWal) {
        TableNameRecord nameRecord = tableNameRegistry.getTableNameRecord(tableName);
        if (nameRecord != null) {
            return null;
        }

        String tableNameStr = Chars.toString(tableName);
        String systemTableName = tableNameStr;
        if (isWal) {
            systemTableName += TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
            systemTableName += tableId;
        } else if (configuration.mangleTableSystemNames()) {
            systemTableName += TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
        }
        return tableNameRegistry.registerName(tableNameStr, systemTableName, isWal);
    }

    public boolean releaseAll() {
        return releaseAll(Long.MAX_VALUE);
    }

    public boolean releaseInactive() {
        return releaseAll(configuration.getMicrosecondClock().getTicks() - inactiveTtlUs);
    }

    public void reloadMetadataConditionally(
            final String systemTableName,
            long expectedStructureVersion,
            TableRecordMetadataSink sink
    ) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(systemTableName, SequencerLockType.READ)) {
            try {
                if (tableSequencer.getStructureVersion() != expectedStructureVersion) {
                    tableSequencer.getTableMetadata(sink);
                }
            } finally {
                tableSequencer.unlockRead();
            }
        }
    }

    public void removeTableSystemName(CharSequence systemTableName) {
        tableNameRegistry.removeTableSystemName(systemTableName);
    }

    public void renameWalTable(CharSequence tableName, CharSequence newTableName, String systemTableName) {
        String newTableNameStr = tableNameRegistry.rename(tableName, newTableName, systemTableName);
        try (TableSequencerImpl tableSequencer = openSequencerLocked(systemTableName, SequencerLockType.WRITE)) {
            try {
                tableSequencer.rename(newTableNameStr);
            } finally {
                tableSequencer.unlockWrite();
            }
        }
        LOG.advisory().$("renamed wal table [table=")
                .utf8(tableName).$(", newName=").utf8(newTableName).$(", systemTableName=").utf8(systemTableName).I$();
    }

    public void reopen() {
        tableNameRegistry.reloadTableNameCache(configuration);
        closed = false;
    }

    @TestOnly
    public void resetNameRegistryMemory() {
        tableNameRegistry.resetMemory(configuration);
    }

    @TestOnly
    public void setDistressed(String systemTableName) {
        try (TableSequencerImpl sequencer = openSequencerLocked(systemTableName, SequencerLockType.WRITE)) {
            try {
                sequencer.setDistressed();
            } finally {
                sequencer.unlockWrite();
            }
        }
    }

    public void suspendTable(final String systemTableName) {
        try (TableSequencerImpl sequencer = openSequencerLocked(systemTableName, SequencerLockType.WRITE)) {
            try {
                sequencer.suspendTable();
            } finally {
                sequencer.unlockWrite();
            }
        }
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
    private TableSequencerEntry getTableSequencerEntry(String systemTableName, SequencerLockType lock, Function<CharSequence, TableSequencerEntry> getSequencerLambda) {
        TableSequencerEntry entry;
        int attempt = 0;
        while (attempt < recreateDistressedSequencerAttempts) {
            throwIfClosed();
            entry = seqRegistry.computeIfAbsent(systemTableName, getSequencerLambda);
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

        throw CairoException.critical(0).put("sequencer is distressed [table=").put(systemTableName).put(']');
    }

    private TableSequencerEntry openSequencerInstance(CharSequence systemTableName) {
        String tableName = tableNameRegistry.getTableNameBySystemName(systemTableName);
        TableSequencerEntry sequencer = new TableSequencerEntry(this, this.engine, (String) systemTableName, tableName);
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
                    LOG.info().$("releasing idle table sequencer [table=").utf8(tableSystemName).I$();
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
        void onTable(int tableId, final String tableName, long lastTxn);
    }

    private static class TableSequencerEntry extends TableSequencerImpl {
        private final TableSequencerAPI pool;
        private volatile long releaseTime = Long.MAX_VALUE;

        TableSequencerEntry(TableSequencerAPI pool, CairoEngine engine, String systemTableName, String tableName) {
            super(engine, systemTableName, tableName);
            this.pool = pool;
        }

        @Override
        public void close() {
            if (!pool.closed) {
                if (!isDistressed() && !isDropped()) {
                    releaseTime = pool.configuration.getMicrosecondClock().getTicks();
                } else {
                    // Sequencer is distressed or dropped, close before removing from the pool.
                    // Remove from registry only if this thread closed the instance.
                    if (checkClose()) {
                        LOG.info().$("closed distressed table sequencer [table=").$(getTableName()).$();
                        pool.seqRegistry.remove(getTableName(), this);
                    }
                }
            } else {
                super.close();
            }
        }
    }
}
