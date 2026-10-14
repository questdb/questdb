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
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.questdb.cairo.TableUtils.WAL_2_TABLE_RESUME_REASON;
import static io.questdb.cairo.wal.WalUtils.SEQ_DIR;
import static io.questdb.cairo.wal.WalUtils.TXNLOG_FILE_NAME;

public class TableSequencerAPI implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(TableSequencerAPI.class);
    // The header bytes the WAL table scan reads: the format version, the max txn and the flushed-through txn, at
    // the same offsets in both log formats.
    private static final int SCAN_HEADER_SIZE = (int) (TableTransactionLogV1.UNSYNCED_FROM_TXN_OFFSET_64 + Long.BYTES);
    final CairoConfiguration configuration;
    final ConcurrentHashMap<TableSequencerImpl> seqRegistry = new ConcurrentHashMap<>(false);
    private final Function<CharSequence, SeqTxnTracker> createTxnTracker;
    private final CairoEngine engine;
    private final long inactiveTtlUs;
    private final int recreateDistressedSequencerAttempts;
    private final ConcurrentHashMap<SeqTxnTracker> seqTxnTrackers = new ConcurrentHashMap<>(false);
    private final BiFunction<CharSequence, Object, TableSequencerImpl> openSequencerInstanceLambda = this::openSequencerInstance;
    volatile boolean closed;

    public TableSequencerAPI(CairoEngine engine, CairoConfiguration configuration) {
        this.configuration = configuration;
        this.engine = engine;
        this.inactiveTtlUs = configuration.getInactiveWalWriterTTL() * 1000;
        this.recreateDistressedSequencerAttempts = configuration.getWalRecreateDistressedSequencerAttempts();
        this.createTxnTracker = dir -> new SeqTxnTracker(configuration);
    }

    public void applyRename(TableToken tableToken) {
        try (TableSequencerImpl sequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            try {
                sequencer.notifyRename(tableToken);
            } finally {
                sequencer.unlockWrite();
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        releaseAll();
    }

    @TestOnly
    public void closeSequencer(TableToken tableToken) {
        try (TableSequencerImpl sequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            sequencer.unlockWrite();
        }
    }

    public void dropTable(TableToken tableToken, boolean failedCreate) {
        LOG.info().$("dropping wal table [table=").$(tableToken).I$();
        try (TableSequencerImpl seq = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            try {
                seq.dropTable();
            } finally {
                seq.unlockWrite();
            }
            getSeqTxnTracker(tableToken).notifyOnDrop();
        } catch (CairoException e) {
            LOG.info().$("failed to drop wal table [table=").$(tableToken).I$();
            if (!failedCreate) {
                throw e;
            }
        }
    }

    public void forAllWalTables(ObjHashSet<TableToken> tableTokenBucket, boolean includeDropped, TableSequencerCallback callback) {
        final CharSequence root = configuration.getDbRoot();
        final FilesFacade ff = configuration.getFilesFacade();
        Path path = Path.PATH.get();

        engine.getTableTokens(tableTokenBucket, includeDropped);
        for (int i = 0, n = tableTokenBucket.size(); i < n; i++) {
            TableToken tableToken = tableTokenBucket.get(i);

            // Exclude locked entries.
            // Use includeDropped argument to decide whether to include dropped tables.
            boolean isDropped = includeDropped && engine.isTableDropped(tableToken);
            if (engine.isWalTable(tableToken) && !isDropped) {
                long lastTxn;
                int tableId = tableToken.getTableId();

                try {
                    if (!seqRegistry.containsKey(tableToken.getDirName())) {
                        // Fast path.
                        // The following calls are racy, i.e. there might be a sequencer modifying both
                        // metadata and log concurrently as we read the values. It's ok since we iterate
                        // through the WAL tables periodically, so eventually we should see the updates.
                        path.of(root).concat(tableToken.getDirName()).concat(SEQ_DIR);
                        lastTxn = readCertifiedLastTxn(ff, path, tableToken);
                    } else {
                        // Slow path.
                        try (TableSequencer tableSequencer = openSequencerLocked(tableToken, SequencerLockType.NONE)) {
                            lastTxn = tableSequencer.lastTxn();
                        }
                    }
                } catch (CairoException ex) {
                    if (ex.isFileCannotRead() || ex.isTableDropped()) {
                        // Table is partially dropped, but not fully.
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

    public int getCurrentWalId(final TableToken tableToken) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.READ)) {
            try {
                return tableSequencer.getCurrentWalId();
            } finally {
                tableSequencer.unlockRead();
            }
        }
    }

    public @NotNull TransactionLogCursor getCursor(final TableToken tableToken, long seqTxn) {
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

    public @NotNull TransactionLogCursor getCursor(
            final TableToken tableToken,
            long seqTxn,
            @NotNull TableSequencerCursorHolder cursorHolder
    ) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.READ)) {
            TransactionLogCursor cursor;
            try {
                cursor = tableSequencer.getTransactionLogCursor(seqTxn, cursorHolder);
            } finally {
                tableSequencer.unlockRead();
            }
            return cursor;
        }
    }

    public @NotNull TableMetadataChangeLog getMetadataChangeLog(final TableToken tableToken, long structureVersionLo) {
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

    public TableMetadataChangeLog getMetadataChangeLogSlow(final TableToken tableToken, long structureVersionLo) {
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

    public TableMetadataChangeLog getMetadataChangeLogSlow(
            final TableToken tableToken,
            long structureVersionLo,
            @NotNull TableSequencerCursorHolder cursorHolder
    ) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.READ)) {
            TableMetadataChangeLog metadataChangeLog;
            try {
                metadataChangeLog = tableSequencer.getMetadataChangeLogSlow(structureVersionLo, cursorHolder);
            } finally {
                tableSequencer.unlockRead();
            }
            return metadataChangeLog;
        }
    }

    public int getNextWalId(final TableToken tableToken) {
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

    public long getTableMetadata(final TableToken tableToken, final TableRecordMetadataSink sink) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.READ)) {
            try {
                return tableSequencer.getTableMetadata(sink);
            } finally {
                tableSequencer.unlockRead();
            }
        }
    }

    @NotNull
    public SeqTxnTracker getTxnTracker(TableToken tableToken) {
        return getSeqTxnTracker(tableToken);
    }

    /**
     * The engine's set of WAL writers with a pending adaptive group-commit flush. A
     * {@link io.questdb.cairo.wal.WalWriter}, which holds the sequencer API and not the engine, registers itself
     * here when it defers a device flush.
     */
    @NotNull
    public io.questdb.cairo.wal.WalGroupCommitFlushQueue getWalGroupCommitFlushQueue() {
        return engine.getWalGroupCommitFlushQueue();
    }

    public void handleDataSyncFailure(Throwable failure) {
        engine.handleDataSyncFailure(failure);
    }

    public boolean isDataSyncFailureFatal(Throwable failure) {
        return engine.isDataSyncFailureFatal(failure);
    }

    public boolean isDurabilityFailed() {
        return engine.isDurabilityFailed();
    }

    /**
     * Non-creating counterpart to {@link #getTxnTracker(TableToken)}: returns the tracker only if
     * one has already been installed, and never allocates or installs one on a miss. A caller that
     * only needs to inspect suspension state, and for which "no tracker yet" means "never
     * suspended", should prefer this on a hot read path (e.g. a metrics scrape) over the creating
     * accessor.
     */
    @Nullable
    public SeqTxnTracker getTxnTrackerIfExists(TableToken tableToken) {
        return getTxnTrackerIfExists(tableToken.getDirName());
    }

    /**
     * {@link #getTxnTrackerIfExists(TableToken)} by the table's directory name, which is what keys the
     * trackers. For callers that hold only the name, such as the scoreboard pool.
     */
    @Nullable
    public SeqTxnTracker getTxnTrackerIfExists(CharSequence tableDirName) {
        return seqTxnTrackers.get(tableDirName);
    }

    public boolean initTxnTracker(TableToken tableToken, long writerTxn, long seqTxn) {
        SeqTxnTracker seqTxnTracker = getSeqTxnTracker(tableToken);
        final boolean isSuspended = isSuspended(tableToken);
        return seqTxnTracker.initTxns(writerTxn, seqTxn, isSuspended);
    }

    public boolean isSuspended(final TableToken tableToken) {
        return getSeqTxnTracker(tableToken).isSuspended();
    }

    public boolean isTxnTrackerInitialised(final TableToken tableToken) {
        return getSeqTxnTracker(tableToken).isInitialised();
    }

    public long lastTxn(final TableToken tableToken) {
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

    public long nextStructureTxn(final TableToken tableToken, long structureVersion, AlterOperation alterOp) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            long txn;
            try {
                txn = tableSequencer.nextStructureTxn(structureVersion, alterOp);
            } finally {
                tableSequencer.unlockWrite();
            }
            return txn;
        }
    }

    public long nextTxn(final TableToken tableToken, int walId, long expectedSchemaVersion, int segmentId, int segmentTxn, long txnMinTimestamp, long txnMaxTimestamp, long txnRowCount) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            long txn;
            try {
                if (!tableSequencer.getTableToken().equals(tableToken)) {
                    throw TableReferenceOutOfDateException.of(tableToken);
                }
                txn = tableSequencer.nextTxn(expectedSchemaVersion, walId, segmentId, segmentTxn, txnMinTimestamp, txnMaxTimestamp, txnRowCount);
            } finally {
                tableSequencer.unlockWrite();
            }
            return txn;
        }
    }

    /**
     * Flushes the sequencer transaction log of {@code tableToken} to the device, the last step of the adaptive
     * group commit's batched flush, after the data and the events. Opens the sequencer under the write lock, the
     * lock {@code nextTxn} takes, so the flush cannot race an append or a rotation. Callers invoke it only for
     * adaptive tables with at least one deferred sequencer commit.
     *
     * @return the highest seqTxn the flush covered; see {@link TableSequencerImpl#fdatasyncTxnLog()}.
     */
    public long fdatasyncTxnLog(final TableToken tableToken) {
        return fdatasyncTxnLog(tableToken, Long.MAX_VALUE);
    }

    /**
     * Flushes like {@link #fdatasyncTxnLog(TableToken)}, and also the older part files of a V2 log that hold txns
     * from {@code fromTxn} on, for a flush that must cover records a run in a mode that does not flush the log
     * left; see {@link TableTransactionLogFile#fdatasyncTxnLog(long)}.
     */
    public long fdatasyncTxnLog(final TableToken tableToken, long fromTxn) {
        assert TableSequencerImpl.assertNoWriteLocksHeld();
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            try {
                return tableSequencer.fdatasyncTxnLog(fromTxn);
            } finally {
                tableSequencer.unlockWrite();
            }
        }
    }

    public boolean notifyOnCheck(TableToken tableToken, long seqTxn) {
        // Updates seqTxn and returns true if CheckWalTransactionsJob should post notification
        // to run ApplyWal2TableJob for the table
        return getSeqTxnTracker(tableToken).notifyOnCheck(seqTxn);
    }

    public void notifySegmentClosed(TableToken tableToken, long txn, int walId, int segmentId) {
        engine.getWalListener().segmentClosed(tableToken, txn, walId, segmentId);
    }

    public void notifyWalClosed(TableToken tableToken, long txn, int walId) {
        engine.getWalListener().walClosed(tableToken, txn, walId);
    }

    @TestOnly
    public void openSequencer(TableToken tableToken) {
        try (TableSequencerImpl sequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            sequencer.unlockWrite();
        }
    }

    public @Nullable CairoError poisonOnDataSyncFailure(Throwable failure) {
        return engine.poisonOnDataSyncFailure(failure);
    }

    public boolean prepareToConvertToNonWal(final TableToken tableToken) {
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

    public void purgeTxnTracker(String dirName) {
        final SeqTxnTracker removed = seqTxnTrackers.remove(dirName);
        if (removed != null) {
            // Clear the adaptive group-commit contiguous-prefix pins and zero this tracker's contribution to
            // the engine-wide durable-frontier gauge before it is discarded (table drop / reboot reset), so a
            // fresh tracker for the same dir does not inherit stale pins and the gauge does not leak.
            removed.resetDurableFrontier();
        }
    }

    /**
     * Closes the table's cached sequencer and drops its {@link SeqTxnTracker} and scoreboard. Recovery must run
     * before replay to rebuild the save-point pins, as on a freshly started engine. A simulated crash can roll back a txn
     * that the open sequencer still counts in its in-memory {@code lastTxn}; through that sequencer,
     * {@link #forAllWalTables} would seed the tracker above the durable writer txn, and the apply job would wait
     * forever for a txn the WAL no longer has.
     * <p>
     * Closes only an idle sequencer ({@link TableSequencerImpl#checkClose()} takes the schema write lock); the
     * caller has already released every reader and writer of the table.
     */
    @TestOnly
    public void resetForReboot(TableToken tableToken) {
        final String dirName = tableToken.getDirName();
        final TableSequencerImpl sequencer = seqRegistry.get(dirName);
        // An entry closed earlier stays registered otherwise, and getTableSequencerEntry() would spin on it: it
        // retries a closed entry without counting the attempt.
        if (sequencer != null && (sequencer.checkClose() || sequencer.isClosed())) {
            seqRegistry.remove(dirName, sequencer);
        }
        purgeTxnTracker(dirName);
        // A reboot also discards the old epoch reservations and the scoreboard's monotonic reader floor.
        engine.getTxnScoreboardPool().remove(tableToken);
    }

    public void registerTable(int tableId, final TableStructure tableDescriptor, final TableToken tableToken) {
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
                                tableDescriptor
                        )
                )
        ) {
            SeqTxnTracker seqTxnTracker = getSeqTxnTracker(tableToken);
            seqTxnTracker.initTxns(0, 0, false);
            tableSequencer.unlockWrite();
        }
    }

    public boolean releaseAll() {
        seqTxnTrackers.clear();
        return releaseAll(Long.MAX_VALUE);
    }

    public boolean releaseInactive() {
        return releaseAll(configuration.getMicrosecondClock().getTicks() - inactiveTtlUs);
    }

    public TableToken reload(TableToken tableToken) {
        try (TableSequencerImpl tableSequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            try {
                return tableSequencer.reload();
            } finally {
                tableSequencer.unlockWrite();
            }
        }
    }

    public void reloadMetadataConditionally(
            final TableToken tableToken,
            long expectedStructureVersion,
            TableRecordMetadataSink sink
    ) {
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

    public void resumeTable(TableToken tableToken, long resumeFromTxn) {
        final SeqTxnTracker tracker = getTxnTracker(tableToken);
        try (TableSequencerImpl sequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            try {
                // A replica keeps no save point, so the boot arm owes nothing there.
                final boolean isCompletionOutstanding = tracker.isDurableEpochCompletionRequired()
                        && engine.getResolvedCommitMode() == CommitMode.ADAPTIVE;
                // Re-notify an already resumed table too. Ordinary RESUME must not contend with its apply writer.
                if (!isCompletionOutstanding && (!isSuspended(tableToken) || resumeFromTxn <= 0)) {
                    // Lift SUSPEND WAL only once the resume is certain, and before the notification, which
                    // the apply job drops for a table still hard-suspended.
                    engine.removeWalApplySuspended(tableToken);
                    sequencer.resumeTable();
                    return;
                }
                final long nextTxn = sequencer.lastTxn() + 1;
                if (resumeFromTxn > nextTxn) {
                    throw CairoException.nonCritical().put("resume txn is higher than next available transaction [resumeFromTxn=").put(resumeFromTxn).put(", nextTxn=").put(nextTxn).put(']');
                }
            } finally {
                sequencer.unlockWrite();
            }
        }
        // Constructor enrolment, required publication and pooled return may all take the sequencer lock.
        try (TableWriter writer = engine.getWriter(tableToken, WAL_2_TABLE_RESUME_REASON)) {
            try (TableSequencerImpl sequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
                try {
                    if (resumeFromTxn > 0 && isSuspended(tableToken) && resumeFromTxn - 1 > writer.getAppliedSeqTxn()) {
                        if (writer.isEnrolledAdaptive() && engine.getResolvedCommitMode() == CommitMode.ADAPTIVE) {
                            tracker.setDurableEpochCompletionRequired(true);
                        }
                        // This _txn-only commit is serialized with other RESUMEs; it does not publish an epoch.
                        writer.commitSeqTxn(resumeFromTxn - 1);
                    }
                } finally {
                    sequencer.unlockWrite();
                }
            }
            writer.completeDurableEpochAfter("resume wal");
            try (TableSequencerImpl sequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
                try {
                    engine.removeWalApplySuspended(tableToken);
                    sequencer.resumeTable();
                } finally {
                    sequencer.unlockWrite();
                }
            }
        } catch (EntryUnavailableException busy) {
            // Another thread holds the writer and nothing was attempted: the suspension, its reason and
            // its write denial stay as they were, and the caller retries.
            throw busy;
        } catch (CairoException e) {
            if (engine.isDataSyncFailureFatal(e)) {
                engine.handleDataSyncFailure(e);
            }
            final String reason = e.getFlyweightMessage().toString();
            suspendTable(tableToken, ErrorTag.NONE, reason);
            throw CairoException.nonCritical()
                    .put("could not complete the durable epoch; the table stays suspended; retry RESUME WAL [table=")
                    .put(tableToken.getTableName()).put(", reason=").put(reason).put(']');
        }
    }

    @TestOnly
    public void setDistressed(TableToken tableToken) {
        try (TableSequencerImpl sequencer = openSequencerLocked(tableToken, SequencerLockType.WRITE)) {
            try {
                sequencer.setDistressed();
            } finally {
                sequencer.unlockWrite();
            }
        }
    }

    public void setHardSuspended(final TableToken tableToken, boolean hardSuspended) {
        getSeqTxnTracker(tableToken).setHardSuspended(hardSuspended);
    }

    public void suspendTable(final TableToken tableToken, ErrorTag errorTag, String errorMessage) {
        getSeqTxnTracker(tableToken).setSuspended(errorTag, errorMessage);
    }

    /**
     * @see SeqTxnTracker#updateWriterTxns(long, long)
     */
    public boolean updateWriterTxns(final TableToken tableToken, long writerTxn, long dirtyWriterTxn) {
        return getSeqTxnTracker(tableToken).updateWriterTxns(writerTxn, dirtyWriterTxn);
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

    /**
     * The last txn of a sequencer log this process has not opened, as its open would leave it, or -1 when the header
     * cannot be read. A power loss can leave the header on the disk ahead of its newest records, and the first open
     * in a process cuts them, so the header's max txn is returned only when it is certain: the table's tracker holds
     * as much, and every value the tracker holds was certified, or an open now would cut no record. A log that is not
     * certain is opened. Its open fails with the same error at the table's first use when it fails here, so the
     * header's value is returned then, as without the check.
     */
    private long readCertifiedLastTxn(FilesFacade ff, Path path, TableToken tableToken) {
        long lastTxn = -1;
        boolean isCertain = true;
        final long header = Unsafe.malloc(SCAN_HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
        long fdTxn = -1;
        try {
            fdTxn = TableUtils.openRO(ff, path, TXNLOG_FILE_NAME, LOG);
            if (ff.read(fdTxn, header, SCAN_HEADER_SIZE, 0) == SCAN_HEADER_SIZE) {
                lastTxn = Math.max(Unsafe.getLong(header + TableTransactionLogFile.MAX_TXN_OFFSET_64), -1);
                if (lastTxn > 0) {
                    final SeqTxnTracker tracker = getTxnTrackerIfExists(tableToken);
                    if (tracker == null || lastTxn > tracker.getSeqTxn()) {
                        isCertain = TableTransactionLog.isTailCertain(
                                ff,
                                fdTxn,
                                Unsafe.getInt(header),
                                lastTxn,
                                Unsafe.getLong(header + TableTransactionLogV1.UNSYNCED_FROM_TXN_OFFSET_64),
                                configuration.getCommitMode(),
                                engine.getLocalDurabilityPolicy()
                        );
                    }
                }
            }
        } finally {
            if (fdTxn != -1) {
                ff.close(fdTxn);
            }
            Unsafe.free(header, SCAN_HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
        if (!isCertain) {
            try (TableSequencer tableSequencer = openSequencerLocked(tableToken, SequencerLockType.NONE)) {
                return tableSequencer.lastTxn();
            } catch (CairoException ex) {
                if (ex.isFileCannotRead() || ex.isTableDropped()) {
                    throw ex;
                }
            }
        }
        return lastTxn;
    }

    private boolean releaseEntries(long deadline) {
        if (seqRegistry.isEmpty()) {
            // nothing to release
            return true;
        }
        boolean removed = false;
        final Iterator<CharSequence> iterator = seqRegistry.keySet().iterator();
        while (iterator.hasNext()) {
            final CharSequence tableDir = iterator.next();
            final TableSequencerImpl sequencer = seqRegistry.get(tableDir);
            if (sequencer != null && deadline >= sequencer.releaseTime && !sequencer.isClosed()) {
                // Remove from registry only if this thread closed the instance
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

    protected boolean releaseAll(long deadline) {
        return releaseEntries(deadline);
    }

    enum SequencerLockType {
        WRITE,
        READ,
        NONE
    }

    @FunctionalInterface
    public interface TableSequencerCallback {
        void onTable(int tableId, final TableToken tableName, long lastTxn);
    }
}
