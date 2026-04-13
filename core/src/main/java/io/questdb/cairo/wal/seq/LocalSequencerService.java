/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjHashSet;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.questdb.cairo.wal.WalUtils.SEQ_DIR;
import static io.questdb.cairo.wal.WalUtils.TXNLOG_FILE_NAME;

/**
 * Local, file-based implementation of {@link SequencerService}.
 * Manages a pool of {@link TableSequencerImpl} instances, one per WAL table,
 * with read/write locking for concurrent access.
 */
public class LocalSequencerService implements SequencerService {
    private static final Log LOG = LogFactory.getLog(LocalSequencerService.class);
    private final CairoConfiguration configuration;
    private final AtomicLong databaseVersion = new AtomicLong(0);
    private final CairoEngine engine;
    private final long inactiveTtlUs;
    private final BiFunction<CharSequence, Object, TableSequencerImpl> openSequencerInstanceLambda = this::openSequencerInstance;
    private final int recreateDistressedSequencerAttempts;
    private final ConcurrentHashMap<TableSequencerImpl> seqRegistry = new ConcurrentHashMap<>(false);
    private final Function<CharSequence, SeqTxnTracker> createTxnTracker;
    private final ConcurrentHashMap<SeqTxnTracker> seqTxnTrackers;
    private volatile SequencerServiceListener listener;
    volatile boolean closed;

    public LocalSequencerService(
            CairoEngine engine,
            CairoConfiguration configuration,
            ConcurrentHashMap<SeqTxnTracker> seqTxnTrackers,
            Function<CharSequence, SeqTxnTracker> createTxnTracker
    ) {
        this.configuration = configuration;
        this.engine = engine;
        this.inactiveTtlUs = configuration.getInactiveWalWriterTTL() * 1000;
        this.recreateDistressedSequencerAttempts = configuration.getWalRecreateDistressedSequencerAttempts();
        this.seqTxnTrackers = seqTxnTrackers;
        this.createTxnTracker = createTxnTracker;
    }

    @Override
    public void applyRename(TableToken tableToken) {
        TableToken oldToken;
        try (TableSequencerImpl sequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            try {
                oldToken = sequencer.getTableToken();
                sequencer.notifyRename(tableToken);
            } finally {
                sequencer.unlockWrite();
            }
        }
        SequencerServiceListener l = listener;
        if (l != null) {
            long version = databaseVersion.incrementAndGet();
            l.onTableRenamed(oldToken, tableToken, version);
        }
    }

    @Override
    public void close() {
        closed = true;
        releaseAll();
    }

    @Override
    public long dropTable(TableToken tableToken) {
        LOG.info().$("dropping wal table [table=").$(tableToken).I$();
        try (TableSequencerImpl seq = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            try {
                seq.dropTable();
            } finally {
                seq.unlockWrite();
            }
        }
        long version = databaseVersion.incrementAndGet();
        SequencerServiceListener l = listener;
        if (l != null) {
            l.onTableDropped(tableToken, version);
        }
        return version;
    }

    @Override
    public void forAllWalTables(ObjHashSet<TableToken> tableTokenBucket, boolean includeDropped, TableSequencerCallback callback) {
        final CharSequence root = configuration.getDbRoot();
        final FilesFacade ff = configuration.getFilesFacade();
        Path path = Path.PATH.get();

        engine.getTableTokens(tableTokenBucket, includeDropped);
        for (int i = 0, n = tableTokenBucket.size(); i < n; i++) {
            TableToken tableToken = tableTokenBucket.get(i);

            boolean isDropped = includeDropped && engine.isTableDropped(tableToken);
            if (engine.isWalTable(tableToken) && !isDropped) {
                long lastTxn;
                int tableId = tableToken.getTableId();

                try {
                    if (!seqRegistry.containsKey(tableToken.getDirName())) {
                        path.of(root).concat(tableToken.getDirName()).concat(SEQ_DIR);
                        long fdTxn = TableUtils.openRO(ff, path, TXNLOG_FILE_NAME, LOG);
                        lastTxn = ff.readNonNegativeLong(fdTxn, TableTransactionLogFile.MAX_TXN_OFFSET_64);
                        ff.close(fdTxn);
                    } else {
                        try (TableSequencer tableSequencer = openSequencerLocked(tableToken, SequencerLockType.NONE)) {
                            lastTxn = tableSequencer.lastTxn();
                        }
                    }
                } catch (CairoException ex) {
                    if (ex.isFileCannotRead() || ex.isTableDropped()) {
                        lastTxn = -1;
                    } else {
                        LOG.critical().$("could not read WAL table transaction file [table=").$(tableToken)
                                .$(", errno=").$(ex.getErrno())
                                .$(", error=").$((Throwable) ex).I$();
                        continue;
                    }
                }

                try {
                    if (includeDropped || lastTxn > -1) {
                        callback.onTable(tableId, tableToken, lastTxn);
                    }
                } catch (CairoException ex) {
                    LOG.critical().$("could not process table sequencer [table=").$(tableToken)
                            .$(", errno=").$(ex.getErrno())
                            .$(", error=").$((Throwable) ex).I$();
                }
            } else if (isDropped) {
                try {
                    callback.onTable(tableToken.getTableId(), tableToken, -1);
                } catch (CairoException ex) {
                    LOG.critical().$("could not process table sequencer [table=").$(tableToken)
                            .$(", errno=").$(ex.getErrno())
                            .$(", error=").$((Throwable) ex).I$();
                }
            }
        }
    }

    public CairoConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public @NotNull TransactionLogCursor getCursor(TableToken tableToken, long seqTxn) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.READ)) {
            TransactionLogCursor cursor;
            try {
                cursor = tableSequencer.getTransactionLogCursor(seqTxn);
            } finally {
                tableSequencer.unlockRead();
            }
            return cursor;
        }
    }

    int getCurrentWalId(TableToken tableToken) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.READ)) {
            try {
                return tableSequencer.getCurrentWalId();
            } finally {
                tableSequencer.unlockRead();
            }
        }
    }

    @Override
    public long getDatabaseVersion() {
        return databaseVersion.get();
    }

    @Override
    public @NotNull TableMetadataChangeLog getMetadataChangeLog(TableToken tableToken, long structureVersionLo) {
        try (TableSequencerImpl tableSequencer = getOrOpenSequencer(tableToken, this.openSequencerInstanceLambda)) {
            if (tableSequencer.metadataMatches(structureVersionLo)) {
                return EmptyOperationCursor.INSTANCE;
            }
        }
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.READ)) {
            try {
                return tableSequencer.getMetadataChangeLog(structureVersionLo);
            } finally {
                tableSequencer.unlockRead();
            }
        }
    }

    TableMetadataChangeLog getMetadataChangeLogSlow(TableToken tableToken, long structureVersionLo) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.READ)) {
            TableMetadataChangeLog metadataChangeLog;
            try {
                metadataChangeLog = tableSequencer.getMetadataChangeLogSlow(structureVersionLo);
            } finally {
                tableSequencer.unlockRead();
            }
            return metadataChangeLog;
        }
    }

    @Override
    public int getNextWalId(TableToken tableToken) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.READ)) {
            int walId;
            try {
                walId = tableSequencer.getNextWalId();
            } finally {
                tableSequencer.unlockRead();
            }
            return walId;
        }
    }

    ConcurrentHashMap<TableSequencerImpl> getSeqRegistry() {
        return seqRegistry;
    }

    @Override
    public long getTableMetadata(TableToken tableToken, TableRecordMetadataSink sink) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.READ)) {
            try {
                return tableSequencer.getTableMetadata(sink);
            } finally {
                tableSequencer.unlockRead();
            }
        }
    }

    boolean isClosed() {
        return closed;
    }

    @Override
    public long lastTxn(TableToken tableToken) {
        try (TableSequencerImpl sequencer = openSequencerLocked(tableToken, SequencerLockType.READ)) {
            long lastTxn;
            try {
                lastTxn = sequencer.lastTxn();
            } finally {
                sequencer.unlockRead();
            }
            return lastTxn;
        }
    }

    @Override
    public long nextStructureTxn(TableToken tableToken, long structureVersion, AlterOperation alterOp) {
        long txn;
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            try {
                txn = tableSequencer.nextStructureTxn(structureVersion, alterOp);
            } finally {
                tableSequencer.unlockWrite();
            }
        }
        if (txn != NO_TXN) {
            SequencerServiceListener l = listener;
            if (l != null) {
                l.onTransactionsAvailable(tableToken, txn);
            }
        }
        return txn;
    }

    @Override
    public long nextTxn(TableToken tableToken, int walId, long expectedSchemaVersion, int segmentId, int segmentTxn, long txnMinTimestamp, long txnMaxTimestamp, long txnRowCount) {
        long txn;
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            try {
                if (!tableSequencer.getTableToken().equals(tableToken)) {
                    throw TableReferenceOutOfDateException.of(tableToken);
                }
                txn = tableSequencer.nextTxn(expectedSchemaVersion, walId, segmentId, segmentTxn, txnMinTimestamp, txnMaxTimestamp, txnRowCount);
            } finally {
                tableSequencer.unlockWrite();
            }
        }
        if (txn != NO_TXN) {
            SequencerServiceListener l = listener;
            if (l != null) {
                l.onTransactionsAvailable(tableToken, txn);
            }
        }
        return txn;
    }

    @TestOnly
    void openSequencer(TableToken tableToken) {
        try (TableSequencerImpl sequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            sequencer.unlockWrite();
        }
    }

    boolean prepareToConvertToNonWal(TableToken tableToken) {
        boolean isDropped;
        try (TableSequencerImpl seq = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            isDropped = seq.isDropped();
            seq.unlockWrite();
        } catch (CairoException e) {
            LOG.info().$("cannot open sequencer files, assumed table converted to non-wal [table=")
                    .$(tableToken).I$();
            return true;
        }

        final TableSequencerImpl tableSequencer = seqRegistry.get(tableToken.getDirName());
        if (tableSequencer != null && tableSequencer.checkClose()) {
            LOG.info().$("table is converted to non-WAL, closed table sequencer [table=").$(tableToken).I$();
            seqRegistry.remove(tableToken.getDirName(), tableSequencer);
        }
        return !isDropped;
    }

    /**
     * Initialize sequencer files on disk for a newly created table.
     * Called AFTER the main table files have been created.
     * This is the same code path used by both local creation and the
     * onTableRegistered callback for remote creates.
     */
    @Override
    public void initSequencerFiles(int tableId, TableStructure structure, TableToken tableToken) {
        try (
                TableSequencerImpl tableSequencer = getTableSequencerEntry(
                        tableToken,
                        SequencerLockType.WRITE,
                        (key, tt) -> new TableSequencerImpl(
                                this,
                                engine,
                                tableToken,
                                getSeqTxnTracker((TableToken) tt),
                                tableId,
                                structure
                        )
                )
        ) {
            tableSequencer.unlockWrite();
        }
    }

    @Override
    public long registerTable(int tableId, TableStructure structure, TableToken tableToken, long callerDatabaseVersion) {
        // Logical registration only — no physical files created here.
        // Physical sequencer files are created by initSequencerFiles() after
        // the main table files are created on disk.
        long version = databaseVersion.incrementAndGet();
        SequencerServiceListener l = listener;
        if (l != null) {
            l.onTableRegistered(tableToken, version);
        }
        return version;
    }

    @Override
    public boolean releaseAll() {
        return releaseAll(Long.MAX_VALUE);
    }

    @Override
    public boolean releaseInactive() {
        return releaseAll(configuration.getMicrosecondClock().getTicks() - inactiveTtlUs);
    }

    @Override
    public TableToken reload(TableToken tableToken) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            try {
                return tableSequencer.reload();
            } finally {
                tableSequencer.unlockWrite();
            }
        }
    }

    void reloadMetadataConditionally(TableToken tableToken, long expectedStructureVersion, TableRecordMetadataSink sink) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.READ)) {
            try {
                if (tableSequencer.getStructureVersion() != expectedStructureVersion) {
                    tableSequencer.getTableMetadata(sink);
                }
            } finally {
                tableSequencer.unlockRead();
            }
        }
    }

    @Override
    public void setListener(SequencerServiceListener listener) {
        this.listener = listener;
    }

    @TestOnly
    void closeSequencer(TableToken tableToken) {
        try (TableSequencerImpl sequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            try {
                sequencer.close();
            } finally {
                sequencer.unlockWrite();
            }
        }
    }

    @TestOnly
    void setDistressed(TableToken tableToken) {
        try (TableSequencerImpl sequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            try {
                sequencer.setDistressed();
            } finally {
                sequencer.unlockWrite();
            }
        }
    }

    private @NotNull TableSequencerImpl getOrOpenSequencer(
            TableToken tableToken,
            BiFunction<CharSequence, Object, TableSequencerImpl> lambda
    ) {
        int attempt = 0;
        while (attempt < recreateDistressedSequencerAttempts) {
            throwIfClosed();
            TableSequencerImpl entry = seqRegistry.computeIfAbsent(tableToken.getDirName(), tableToken, lambda);
            boolean isDistressed = entry.isDistressed();
            if (!isDistressed && !entry.isClosed()) {
                return entry;
            }

            if (isDistressed) {
                attempt++;
            }
        }

        throw CairoException.critical(0).put("sequencer is distressed [table=").put(tableToken.getDirName()).put(']');
    }

    @NotNull
    private SeqTxnTracker getSeqTxnTracker(TableToken tt) {
        return seqTxnTrackers.computeIfAbsent(tt.getDirName(), createTxnTracker);
    }

    @NotNull
    private TableSequencerImpl getTableSequencerEntry(
            TableToken tableToken,
            SequencerLockType lock,
            BiFunction<CharSequence, Object, TableSequencerImpl> getSequencerLambda
    ) {
        TableSequencerImpl entry;
        int attempt = 0;
        while (attempt < recreateDistressedSequencerAttempts) {
            throwIfClosed();
            entry = seqRegistry.computeIfAbsent(tableToken.getDirName(), tableToken, getSequencerLambda);
            if (lock == SequencerLockType.READ) {
                entry.readLock();
            } else if (lock == SequencerLockType.WRITE) {
                entry.writeLock();
            }
            boolean isDistressed = entry.isDistressed();
            if (!isDistressed && !entry.isClosed()) {
                return entry;
            } else if (lock == SequencerLockType.READ) {
                entry.unlockRead();
            } else if (lock == SequencerLockType.WRITE) {
                entry.unlockWrite();
            }
            if (isDistressed) {
                attempt++;
            }
        }
        throw CairoException.critical(0).put("sequencer is distressed [table=").put(tableToken.getDirName()).put(']');
    }

    private TableSequencerImpl openSequencerInstance(CharSequence tableDir, Object tableToken) {
        return new TableSequencerImpl(
                this,
                this.engine,
                (TableToken) tableToken,
                getSeqTxnTracker((TableToken) tableToken),
                0,
                null
        );
    }

    @NotNull
    private TableSequencerImpl openSequencerLocked(TableToken tableToken, SequencerLockType lock) {
        return getTableSequencerEntry(tableToken, lock, this.openSequencerInstanceLambda);
    }

    private boolean releaseAll(long deadline) {
        return releaseEntries(deadline);
    }

    private boolean releaseEntries(long deadline) {
        if (seqRegistry.isEmpty()) {
            return true;
        }
        boolean removed = false;
        final Iterator<CharSequence> iterator = seqRegistry.keySet().iterator();
        while (iterator.hasNext()) {
            final CharSequence tableDir = iterator.next();
            final TableSequencerImpl sequencer = seqRegistry.get(tableDir);
            if (sequencer != null && deadline >= sequencer.releaseTime && !sequencer.isClosed()) {
                if (sequencer.checkClose()) {
                    LOG.debug().$("releasing idle table sequencer [tableDir=").$safe(tableDir).I$();
                    iterator.remove();
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

    enum SequencerLockType {
        WRITE,
        READ,
        NONE
    }
}
