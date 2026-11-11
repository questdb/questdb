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

import io.questdb.cairo.BinaryAlterSerializer;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.IDGenerator;
import io.questdb.cairo.IDGeneratorFactory;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.WalDirectoryPolicy;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.locks.ReadWriteLock;

import static io.questdb.cairo.wal.WalUtils.SEQ_DIR;
import static io.questdb.cairo.wal.WalUtils.WAL_INDEX_FILE_NAME;
import static io.questdb.cairo.wal.seq.TableTransactionLogFile.STRUCTURAL_CHANGE_WAL_ID;

public class TableSequencerImpl implements TableSequencer {
    private static final Log LOG = LogFactory.getLog(TableSequencerImpl.class);
    private static final ThreadLocal<ObjHashSet<TableSequencerImpl>> WRITE_LOCKS = ThreadLocal.withInitial(ObjHashSet::new);
    private final static BinaryAlterSerializer alterCommandWalFormatter = new BinaryAlterSerializer();
    private final CairoEngine engine;
    private final SequencerMetadata metadata;
    private final SequencerMetadataService metadataSvc;
    private final MicrosecondClock microClock;
    private final Path path;
    private final TableSequencerAPI pool;
    private final int rootLen;
    private final ReadWriteLock schemaLock = new SimpleReadWriteLock();
    private final SeqTxnTracker seqTxnTracker;
    private final TableTransactionLog tableTransactionLog;
    private final WalDirectoryPolicy walDirectoryPolicy;
    private final IDGenerator walIdGenerator;
    volatile long releaseTime = Long.MAX_VALUE;
    private volatile boolean closed = false;
    private boolean distressed = false;
    private TableToken tableToken;

    TableSequencerImpl(
            TableSequencerAPI pool,
            CairoEngine engine,
            TableToken tableToken,
            SeqTxnTracker txnTracker,
            int tableId,
            @Nullable TableStructure tableStruct
    ) {
        this.pool = pool;
        this.engine = engine;
        this.tableToken = tableToken;
        this.seqTxnTracker = txnTracker;
        final CairoConfiguration configuration = engine.getConfiguration();
        this.walDirectoryPolicy = engine.getWalDirectoryPolicy();
        try {
            path = new Path();
            path.of(configuration.getDbRoot());
            path.concat(tableToken.getDirName()).concat(SEQ_DIR);
            rootLen = path.size();

            metadata = new SequencerMetadata(configuration);
            metadataSvc = new SequencerMetadataService(metadata, tableToken);
            walIdGenerator = IDGeneratorFactory.newIDGenerator(
                    configuration,
                    WAL_INDEX_FILE_NAME,
                    configuration.getIdGenerateBatchStep() < 0 ? 512 : configuration.getIdGenerateBatchStep()
            );
            // The engine swaps its policy when the node changes role; the log reads the current one. An open that
            // finds a torn record keeps what the table has committed, and the table's schema with it.
            tableTransactionLog = new TableTransactionLog(
                    configuration,
                    walDirectoryPolicy,
                    () -> engine.getLocalDurabilityPolicy().isLocalDurabilityEnabled(),
                    this::readCommittedTableTxn
            );
            microClock = configuration.getMicrosecondClock();
            if (tableStruct != null) {
                writeLock();
                try {
                    final long timestamp = microClock.getTicks();
                    createSequencerFiles(configuration, walDirectoryPolicy, metadata, tableTransactionLog, path, rootLen, tableStruct, tableToken, tableId, timestamp);
                    engine.getWalListener().tableCreated(tableToken, timestamp);
                } finally {
                    unlockWrite();
                }
            }
        } catch (Throwable th) {
            LOG.critical().$("could not create sequencer [name=").$(tableToken)
                    .$(", error=").$safe(th.getMessage())
                    .I$();
            closeLocked();
            throw th;
        }
        try {
            walIdGenerator.open(path);
            // The txn log is the recovery authority; open it before _meta so a damaged _meta can be rebuilt.
            tableTransactionLog.open(path);
            // The open may have cut torn records that a scan already published to the tracker.
            seqTxnTracker.lowerSeqTxn(tableTransactionLog.lastTxn());
            openOrRecoverMetadata(tableTransactionLog.getMaxMetadataVersion());
            // For a newly created table and an existing one alike.
            seqTxnTracker.setStructureVersion(metadata.getMetadataVersion());
        } catch (CairoException ex) {
            closeLocked();
            if (ex.isTableDropped()) {
                throw ex;
            }
            final boolean droppedTableMetadataMissing = (ex.isFileCannotRead() || ex.isSequencerMetadataOpenFailed())
                    && engine.getTableTokenByDirName(tableToken.getDirName()) == null;
            if (droppedTableMetadataMissing) {
                LOG.info().$("could not open sequencer, table is dropped [table=").$(tableToken)
                        .$(", path=").$(path)
                        .$(", error=").$safe(ex.getMessage())
                        .I$();
                throw CairoException.tableDropped(tableToken);
            }
            LOG.critical().$("could not open sequencer [table=").$(tableToken)
                    .$(", path=").$(path)
                    .$(", errno=").$(ex.getErrno())
                    .$(", error=").$safe(ex.getMessage())
                    .I$();
            throw ex;
        } catch (Throwable th) {
            LOG.critical().$("could not open sequencer [table=").$(tableToken)
                    .$(", path=").$(path)
                    .$(", error=").$safe(th.getMessage())
                    .I$();
            closeLocked();
            throw th;
        }
    }

    public static boolean assertNoWriteLocksHeld() {
        assert WRITE_LOCKS.get().size() == 0 : "writer lifecycle or sequencer flush under sequencer write lock";
        return true;
    }

    /**
     * Creates the on-disk sequencer files - the {@code txn_seq} directory, the initial sequencer metadata
     * and the transaction log - for a brand-new table directly under {@code path}
     */
    public static void createSequencerFiles(
            CairoConfiguration configuration,
            WalDirectoryPolicy walDirectoryPolicy,
            Path path,
            TableStructure tableStruct,
            TableToken tableToken,
            int tableId
    ) {
        final int tableDirLen = path.size();
        final int rootLen = path.concat(SEQ_DIR).size();
        try (
                SequencerMetadata metadata = new SequencerMetadata(configuration);
                // Only creates the log, which writes no field and cuts nothing, so the policy and the floor play no part.
                TableTransactionLog tableTransactionLog = new TableTransactionLog(
                        configuration,
                        walDirectoryPolicy,
                        LocalDurabilityPolicy.ALWAYS_ON,
                        () -> null
                )
        ) {
            createSequencerFiles(
                    configuration,
                    walDirectoryPolicy,
                    metadata,
                    tableTransactionLog,
                    path,
                    rootLen,
                    tableStruct,
                    tableToken,
                    tableId,
                    configuration.getMicrosecondClock().getTicks()
            );
        } finally {
            path.trimTo(tableDirLen);
        }
    }

    public boolean checkClose() {
        if (closed) {
            return false;
        }
        writeLock();
        try {
            return closeLocked();
        } finally {
            unlockWrite();
        }
    }

    @Override
    public void close() {
        if (pool.closed) {
            checkClose();
        } else if (!isDistressed() && !isDropped()) {
            releaseTime = pool.configuration.getMicrosecondClock().getTicks();
        } else if (checkClose()) {
            LOG.info()
                    .$("closed table sequencer [table=").$(getTableToken())
                    .$(", distressed=").$(isDistressed())
                    .$(", dropped=").$(isDropped())
                    .I$();
            // Remove from registry only if this thread closed the instance.
            pool.seqRegistry.remove(getTableToken().getDirName(), this);
        }
    }

    @Override
    public void dropTable() {
        checkDropped();
        final long timestamp = microClock.getTicks();
        final long txn;
        try {
            txn = tableTransactionLog.addEntry(
                    getStructureVersion(), WalUtils.DROP_TABLE_WAL_ID,
                    0, 0, timestamp, 0, 0, 0
            );
            // Group commit leaves the log's flush to the next batch, and no writer flushes a dropped table again.
            final CairoConfiguration configuration = engine.getConfiguration();
            // Local commits keep the configured grade, including on a replica.
            if (configuration.getCommitMode() == CommitMode.ADAPTIVE && configuration.getAdaptiveCommitGroupWindowUs() > 0) {
                fdatasyncTxnLog();
            }
        } catch (CairoException e) {
            // The log is flushed in the append or right after it. A failed flush stops the engine, as on the commit path.
            engine.handleDataSyncFailure(e);
            throw e;
        }
        metadata.dropTable();
        notifyTxnCommitted(Long.MAX_VALUE);
        engine.getWalListener().tableDropped(tableToken, txn, timestamp);
    }

    public int getCurrentWalId() {
        return (int) walIdGenerator.getCurrentId();
    }

    @Override
    public TableMetadataChangeLog getMetadataChangeLog(long structureVersionLo) {
        checkDropped();
        if (metadata.getMetadataVersion() == structureVersionLo) {
            // Nothing to do.
            return EmptyOperationCursor.INSTANCE;
        }
        return tableTransactionLog.getTableMetadataChangeLog(structureVersionLo, alterCommandWalFormatter);
    }

    @Override
    public TableMetadataChangeLog getMetadataChangeLogSlow(long structureVersionLo) {
        checkDropped();
        // Do not check cached metadata version.
        return tableTransactionLog.getTableMetadataChangeLog(structureVersionLo, alterCommandWalFormatter);
    }

    public TableMetadataChangeLog getMetadataChangeLogSlow(
            long structureVersionLo,
            @NotNull TableSequencerCursorHolder cursorHolder
    ) {
        checkDropped();
        return tableTransactionLog.getTableMetadataChangeLog(
                structureVersionLo,
                alterCommandWalFormatter,
                cursorHolder
        );
    }

    @Override
    public int getNextWalId() {
        return (int) walIdGenerator.getNextId();
    }

    @Override
    public long getStructureVersion() {
        return metadata.getMetadataVersion();
    }

    @Override
    public int getTableId() {
        return metadata.getTableId();
    }

    @Override
    public long getTableMetadata(@NotNull TableRecordMetadataSink sink) {
        int columnCount = metadata.getColumnCount();
        int timestampIndex = metadata.getTimestampIndex();
        int compressedTimestampIndex = -1;
        sink.clear();

        int compressedColumnCount = 0;
        boolean reorderNeeded = false;
        int lastOrder = -1;
        for (int i = 0; i < columnCount; i++) {
            int columnType = metadata.getColumnType(i);
            int columnOrder = metadata.getReadColumnOrder().getQuick(i);
            IntList coveringColumnIndices = metadata.getColumnMetadata(i).getCoveringColumnIndices();
            sink.addColumn(
                    metadata.getColumnName(i),
                    columnType,
                    metadata.getColumnIndexType(i),
                    metadata.getIndexValueBlockCapacity(i),
                    metadata.isSymbolTableStatic(i),
                    i,
                    metadata.isDedupKey(i),
                    metadata.getColumnMetadata(i).isSymbolCacheFlag(),
                    metadata.getColumnMetadata(i).getSymbolCapacity(),
                    coveringColumnIndices
            );
            if (columnType > -1) {
                reorderNeeded |= lastOrder > columnOrder;
                lastOrder = columnOrder;
                if (i == timestampIndex) {
                    compressedTimestampIndex = compressedColumnCount;
                }
                compressedColumnCount++;
            }
        }

        sink.of(
                tableToken,
                metadata.getTableId(),
                timestampIndex,
                compressedTimestampIndex,
                metadata.getMetadataVersion(),
                compressedColumnCount,
                reorderNeeded || sink.requiresFullReadColumnOrder() ? metadata.getReadColumnOrder() : null
        );

        return tableTransactionLog.lastTxn();
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public TransactionLogCursor getTransactionLogCursor(long seqTxn) {
        checkDropped();
        return tableTransactionLog.getCursor(seqTxn);
    }

    public TransactionLogCursor getTransactionLogCursor(
            long seqTxn,
            @NotNull TableSequencerCursorHolder cursorHolder
    ) {
        checkDropped();
        return tableTransactionLog.getCursor(seqTxn, cursorHolder);
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * Flushes the sequencer transaction log to the device, parts before the header, for the adaptive group
     * commit. The caller holds the sequencer write lock, the lock {@code nextTxn} takes, so no append or rotation
     * of the log's mappings runs concurrently.
     *
     * @return the highest seqTxn this flush covers: the log's last txn, read under the write lock, where no append
     * is in flight, so every txn at or below it is in the log before the fdatasync; a txn sequenced after the lock is
     * released is not covered. Taken from the log, not from the tracker, which a reader of an unopened log can seed.
     * The caller passes it to {@link SeqTxnTracker#markWriterDurable(int, long, long)}.
     */
    public long fdatasyncTxnLog() {
        return fdatasyncTxnLog(Long.MAX_VALUE);
    }

    /**
     * Flushes like {@link #fdatasyncTxnLog()}, and also the older part files of a V2 log that hold txns from
     * {@code fromTxn} on; see {@link TableTransactionLogFile#fdatasyncTxnLog(long)}.
     */
    public long fdatasyncTxnLog(long fromTxn) {
        assert !closed;
        final long coveredSeqTxn = tableTransactionLog.lastTxn();
        try {
            tableTransactionLog.fdatasyncTxnLog(fromTxn);
        } catch (Throwable th) {
            // Fence while the caller still holds the write lock: the next writer to take it would fdatasync the
            // same file, and that call can return 0 because the kernel has already reported this error.
            seqTxnTracker.fenceDurableFrontier(th);
            throw th;
        }
        return coveredSeqTxn;
    }

    public boolean isDistressed() {
        return distressed;
    }

    public boolean isDropped() {
        return metadata.isDropped();
    }

    @Override
    public long lastTxn() {
        return tableTransactionLog.lastTxn();
    }

    public boolean metadataMatches(long structureVersion) {
        return metadata.getMetadataVersion() == structureVersion;
    }

    @Override
    public long nextStructureTxn(long expectedStructureVersion, TableMetadataChange change) {
        // Writing to TableSequencer can happen from multiple threads, so we need to protect against concurrent writes.
        assert !closed;
        checkDropped();
        checkHardSuspended();
        long txn;
        try {
            // From sequencer perspective metadata version is the same as column structure version
            if (metadata.getMetadataVersion() == expectedStructureVersion) {
                final long timestamp = microClock.getTicks();
                // Local commits keep the configured grade, including on a replica.
                final int commitMode = engine.getConfiguration().getCommitMode();
                final boolean isDurableMode = commitMode == CommitMode.ADAPTIVE || commitMode == CommitMode.SYNC;
                // Orphaned pins registered by now are covered by the flushes of this change.
                final long orphanSweepMark = isDurableMode ? seqTxnTracker.snapshotOrphanSweepMark() : 0;
                tableTransactionLog.beginMetadataChangeEntry(
                        expectedStructureVersion + 1, alterCommandWalFormatter, change, timestamp);

                final TableToken oldTableToken = tableToken;

                // Re-read serialised change to ensure it can be read.
                AlterOperation deserializedAlter = tableTransactionLog.readTableMetadataChangeLog(
                        expectedStructureVersion, alterCommandWalFormatter);

                applyToMetadata(deserializedAlter);
                if (metadata.getMetadataVersion() != expectedStructureVersion + 1) {
                    throw CairoException.critical(0)
                            .put("applying structure change to WAL table failed [table=").put(tableToken)
                            .put(", oldVersion: ").put(expectedStructureVersion)
                            .put(", newVersion: ").put(metadata.getMetadataVersion())
                            .put(']');
                }
                metadata.sync();
                // TableToken can become updated as a result of alter.
                tableToken = metadata.getTableToken();
                txn = tableTransactionLog.endMetadataChangeEntry();
                // The change flushed the whole log; under group commit the frontier still waits for earlier pins.
                if (isDurableMode && !seqTxnTracker.isDurableFrontierFenced()) {
                    seqTxnTracker.markWriterDurable(STRUCTURAL_CHANGE_WAL_ID, orphanSweepMark, txn);
                }
                // Published before notifyOnCommit() below advances the tracker's
                // seqTxn. CheckWalTransactionsJob reads the log without this lock and
                // can advance seqTxn first; no reader pairs the two values.
                seqTxnTracker.setStructureVersion(metadata.getMetadataVersion());

                // The tracker follows the log for every txn, suspended or not, as nextTxn()
                // does for data txns; QwpTudCache.advanceStructureCheckGate relies on it.
                // notifyOnCommit() withholds the apply-job wakeup while suspended.
                if (seqTxnTracker.notifyOnCommit(txn)) {
                    engine.notifyWalTxnCommitted(tableToken);
                }
                if (!seqTxnTracker.isSuspended()) {
                    engine.notifyLiveViewBaseTableCommit(tableToken, txn);
                    if (!tableToken.equals(oldTableToken)) {
                        engine.getWalListener().tableRenamed(tableToken, txn, timestamp, oldTableToken);
                    } else {
                        engine.getWalListener().nonDataTxnCommitted(tableToken, txn, timestamp);
                    }
                }
            } else {
                return NO_TXN;
            }
        } catch (Throwable th) {
            distressed = true;
            seqTxnTracker.fenceDurableFrontier(th);
            LOG.critical().$("could not apply structure change to WAL table sequencer [table=").$(tableToken)
                    .$(", error=").$safe(th.getMessage())
                    .I$();
            throw th;
        }
        return txn;
    }

    @Override
    public long nextTxn(
            long expectedStructureVersion,
            int walId,
            int segmentId,
            int segmentTxn,
            long txnMinTimestamp,
            long txnMaxTimestamp,
            long txnRowCount
    ) {
        return nextTxnIfLastTxn(
                Long.MAX_VALUE,
                expectedStructureVersion,
                walId,
                segmentId,
                segmentTxn,
                txnMinTimestamp,
                txnMaxTimestamp,
                txnRowCount
        );
    }

    @Override
    public long nextTxnIfLastTxn(
            long expectedLastTxn,
            long expectedStructureVersion,
            int walId,
            int segmentId,
            int segmentTxn,
            long txnMinTimestamp,
            long txnMaxTimestamp,
            long txnRowCount
    ) {
        // TableSequencerAPI holds the sequencer write lock across this check and transaction-log append.
        assert !closed;
        checkDropped();
        checkHardSuspended();
        if (expectedLastTxn != Long.MAX_VALUE && lastTxn() != expectedLastTxn) {
            return FENCE_REJECTED;
        }
        long txn;
        final long timestamp = microClock.getTicks();
        try {
            // From sequencer perspective metadata version is the same as column structure version.
            if (metadata.getMetadataVersion() == expectedStructureVersion) {
                txn = nextTxn(walId, segmentId, segmentTxn, timestamp, txnMinTimestamp, txnMaxTimestamp, txnRowCount);
            } else {
                return NO_TXN;
            }
        } catch (Throwable th) {
            // Under ADAPTIVE W=0 the txn log is fdatasync'd inline, so this is the same barrier failure that
            // fdatasyncTxnLog fences, and the reopened sequencer's fresh fd would not report it again.
            distressed = true;
            seqTxnTracker.fenceDurableFrontier(th);
            LOG.critical().$("could not apply transaction to WAL table sequencer [table=").$(tableToken)
                    .$(", error=").$safe(th.getMessage())
                    .I$();
            throw th;
        }

        notifyTxnCommitted(txn);
        engine.getWalListener().dataTxnCommitted(tableToken, txn, timestamp, walId, segmentId, segmentTxn);
        return txn;
    }

    public void notifyRename(TableToken tableToken) {
        this.tableToken = tableToken;
        this.metadata.notifyRenameTable(tableToken);
    }

    @Override
    public TableToken reload() {
        tableTransactionLog.reload(path);
        if (tableTransactionLog.isDropped()) {
            return null;
        }
        // The reload may have cut torn records the tracker already holds: a promote rescans under the durable policy.
        seqTxnTracker.lowerSeqTxn(tableTransactionLog.lastTxn());

        reconcileMetadataWithCommittedLog(tableTransactionLog.getMaxMetadataVersion());
        seqTxnTracker.setStructureVersion(metadata.getMetadataVersion());
        long lastTxn = tableTransactionLog.lastTxn();
        LOG.info()
                .$("reloaded table sequencer [table=").$(tableToken)
                .$(", lastTxn=").$(lastTxn)
                .I$();
        seqTxnTracker.notifyOnCommit(lastTxn);
        return tableToken = metadata.getTableToken();
    }

    @Override
    public void resumeTable() {
        notifyTxnCommitted(Long.MAX_VALUE);
        seqTxnTracker.setUnsuspended();
    }

    @TestOnly
    public void setDistressed() {
        this.distressed = true;
    }

    private static void createSequencerFiles(
            CairoConfiguration configuration,
            WalDirectoryPolicy walDirectoryPolicy,
            SequencerMetadata metadata,
            TableTransactionLog tableTransactionLog,
            Path path,
            int rootLen,
            TableStructure tableStruct,
            TableToken tableToken,
            int tableId,
            long timestamp
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        if (ff.mkdirs(path.slash(), configuration.getMkDirMode()) != 0) {
            throw CairoException.critical(ff.errno()).put("Cannot create sequencer directory: ").put(path);
        }
        walDirectoryPolicy.initDirectory(path, tableToken);
        path.trimTo(rootLen);
        metadata.create(tableStruct, tableToken, path, rootLen, tableId);
        tableTransactionLog.create(path, timestamp);
        // The WAL id counter, created and sized as its generator's first open would, so the flush below covers its
        // entry: a counter lost to a power loss starts again from 0 and hands out WAL ids the log already names.
        try {
            final long fd = TableUtils.openFileRWOrFail(ff, path.trimTo(rootLen).concat(WAL_INDEX_FILE_NAME).$(), configuration.getWriterFileOpenOpts());
            try {
                TableUtils.allocateDiskSpaceAligned(ff, fd, Files.PAGE_SIZE);
            } finally {
                ff.close(fd);
            }
        } finally {
            path.trimTo(rootLen);
        }
        // SYNC and ADAPTIVE acknowledge a commit once its record is flushed, which presumes the log's files and
        // their directory entries outlive a power loss. Flushing content never makes a new entry durable.
        if (CommitMode.isPowerLossDurable(configuration.getCommitMode())) {
            final int tableDirLen = path.trimTo(rootLen).parent().size();
            try {
                DurableEpochManifest.fsyncSequencer(configuration, path, tableDirLen);
                // The device barrier for the flushes above, as syncfs through _txn is for a table.
                path.trimTo(tableDirLen).concat(SEQ_DIR).concat(WalUtils.TXNLOG_FILE_NAME);
                final long fd = ff.openRWExisting(path.$());
                if (fd == -1) {
                    throw CairoException.critical(ff.errno()).put("could not open sequencer log for the device flush [path=").put(path).put(']');
                }
                try {
                    ff.fsyncDurable(fd);
                } finally {
                    ff.close(fd);
                }
            } finally {
                // The flushes wrote over the path past the table directory: rebuild it rather than trim back to it.
                path.trimTo(tableDirLen).concat(SEQ_DIR);
            }
        }
    }

    private void applyToMetadata(TableMetadataChange change) {
        change.apply(metadataSvc, true);
        metadata.syncToMetaFile();
    }

    private void checkDropped() {
        if (metadata.isDropped()) {
            throw CairoException.tableDropped(tableToken);
        }
    }

    private void checkHardSuspended() {
        // A hard-suspended table denies commits like a dropped table, but with a distinct
        // exception. Gated by cairo.wal.apply.suspended.write.denied so suspension can instead
        // keep buffering WAL writes for later apply. Mirrors the writer-pool gate in
        // CairoEngine.getWalWriter()/getTableWriterAPI(): consult isWalApplySuspended() so the
        // runtime SUSPEND WAL flag and the cairo.wal.apply.suspended.tables config list are both
        // honoured here, otherwise a config-listed table is denied a writer at the pool yet still
        // commits at the sequencer.
        if (engine.getConfiguration().isWalApplySuspendedWriteDenied() && engine.isWalApplySuspended(tableToken)) {
            throw CairoException.tableSuspended(tableToken);
        }
    }

    private boolean closeLocked() {
        if (closed) {
            return false;
        }
        try {
            // Distress and the fence are what a failed append or log flush leaves behind; the flush is skipped then.
            if (tableTransactionLog != null && !distressed && !seqTxnTracker.isDurableFrontierFenced()) {
                try {
                    tableTransactionLog.flushOnCleanClose();
                } catch (CairoException e) {
                    // As in fdatasyncTxnLog(): a later flush of this log can return 0 without the pages this one lost.
                    seqTxnTracker.fenceDurableFrontier(e);
                    LOG.error().$("could not flush sequencer txnlog on close [table=").$(tableToken)
                            .$(", errno=").$(e.getErrno())
                            .$(", error=").$safe(e.getFlyweightMessage())
                            .I$();
                }
            }
        } finally {
            // checkClose() holds the write lock, which keeps appends out during the flush; the flush changes neither
            // the last txn nor the metadata. Marking the sequencer closed only now lets a lock-free lookup keep
            // reading it meanwhile: a closed entry that is still registered makes that lookup spin until the close
            // returns.
            closed = true;
            Misc.free(metadata);
            Misc.free(tableTransactionLog);
            Misc.free(walIdGenerator);
            Misc.free(path);
        }
        return true;
    }

    private long nextTxn(
            int walId,
            int segmentId,
            int segmentTxn,
            long timestamp,
            long txnMinTimestamp,
            long txnMaxTimestamp,
            long txnRowCount
    ) {
        // Local commits keep the configured grade, including on a replica.
        final int effectiveMode = engine.getConfiguration().getCommitMode();
        final long txn = tableTransactionLog.addEntry(
                getStructureVersion(),
                walId,
                segmentId,
                segmentTxn,
                timestamp,
                txnMinTimestamp,
                txnMaxTimestamp,
                txnRowCount
        );
        // Under adaptive group commit, pin this writer's txn under the sequencer write lock, before the caller's
        // notifyTxnCommitted publishes it to the tracker. Otherwise a peer writer's markWriterDurable could see
        // the new seqTxn with no pin for it and advance the durable frontier over a txn that is not yet durable.
        // registerWriterPending is putIfAbsent, so later commits in the same batch keep the writer's first pin;
        // the writer's own markWriterDurable after its flush, or resetDurableFrontier at start-up, drops it.
        if (effectiveMode == io.questdb.cairo.CommitMode.ADAPTIVE
                && engine.getConfiguration().getAdaptiveCommitGroupWindowUs() > 0) {
            seqTxnTracker.registerWriterPending(walId, txn);
        } else if (effectiveMode == CommitMode.ADAPTIVE || effectiveMode == CommitMode.SYNC) {
            // The append flushed the log itself, and a writer's private files are flushed before it sequences.
            if (!seqTxnTracker.isDurableFrontierFenced()) {
                seqTxnTracker.setLocalDurableSeqTxn(txn);
            }
        }
        return txn;
    }

    private void notifyTxnCommitted(long txn) {
        if (txn == Long.MAX_VALUE || seqTxnTracker.notifyOnCommit(txn)) {
            engine.notifyWalTxnCommitted(tableToken);
        }
        // Live views consume WAL segments directly, so notify the refresh job as soon as
        // the sequencer has the commit visible, independently of the apply job's progress.
        if (txn != Long.MAX_VALUE) {
            engine.notifyLiveViewBaseTableCommit(tableToken, txn);
        }
    }

    private void openOrRecoverMetadata(long committedStructureVersion) {
        try {
            metadata.openTableSequencerMetadata(path, rootLen, tableToken);
        } catch (CairoException ex) {
            if (tableTransactionLog.isDropped()) {
                // The txn log is the recovery authority: if it records the drop, surface that
                // even when the registry has not yet caught up with the drop.
                throw CairoException.tableDropped(tableToken);
            }
            if (!ex.isSequencerMetadataOpenFailed()) {
                throw ex;
            }
            LOG.critical().$("could not open sequencer metadata, rebuilding from transaction log [table=").$(tableToken)
                    .$(", committedStructureVersion=").$(committedStructureVersion)
                    .$(", error=").$safe(ex.getMessage())
                    .I$();
            rebuildMetadataFromCommittedLog(committedStructureVersion);
            return;
        }
        reconcileMetadataWithCommittedLog(committedStructureVersion);
    }

    /**
     * One read of the table's {@code _txn}, taken only when an open of the log finds a torn record: its committed
     * seqTxn, which the apply never reads from the log again, and its column structure version. Not its saved lag
     * txns: every writer open drops them and applies them again from the log. Null when {@code _txn} does not load,
     * or when its live area does not lie whole inside the file, as after a lost file extension, which maps as zeros:
     * an unknown answer is never taken as "nothing committed".
     */
    private long[] readCommittedTableTxn() {
        final CairoConfiguration configuration = engine.getConfiguration();
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path txnPath = new Path(); TxReader txReader = new TxReader(ff)) {
            txnPath.of(configuration.getDbRoot()).concat(tableToken.getDirName()).concat(TableUtils.TXN_FILE_NAME);
            // Neither value depends on the partitioning or the timestamp type, so this needs no metadata.
            txReader.ofRO(txnPath.$(), ColumnType.TIMESTAMP, PartitionBy.NONE);
            // A table writer can commit while a promote reloads the log.
            TableUtils.safeReadTxn(txReader, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());
            if (txReader.getBaseOffset() + txReader.getRecordSize() > ff.length(txnPath.$())) {
                return null;
            }
            return new long[]{txReader.getSeqTxn(), txReader.getColumnStructureVersion()};
        } catch (CairoException | CairoError e) {
            return null;
        }
    }

    private void rebuildMetadataFromCommittedLog(long committedStructureVersion) {
        metadata.openFromInitialMetadata(path, rootLen, tableToken);
        replayCommittedMetadataChanges(0, committedStructureVersion, false);
    }

    private void reconcileMetadataWithCommittedLog(long committedStructureVersion) {
        if (tableTransactionLog.isDropped()) {
            return;
        }

        long metaVersion = metadata.getMetadataVersion();
        if (metaVersion == committedStructureVersion) {
            return;
        }

        if (metaVersion < committedStructureVersion) {
            // Normal catch-up: preserve the old reload() behavior by applying only the missing sidecars.
            replayCommittedMetadataChanges(metaVersion, committedStructureVersion, true);
            return;
        }

        // _meta is ahead of the committed txn log. Discard it and rebuild from _meta.0.
        rebuildMetadataFromCommittedLog(committedStructureVersion);
    }

    private void replayCommittedMetadataChange(TableMetadataChange change, boolean applyRenameSidecars) {
        if (!applyRenameSidecars && change instanceof AlterOperation alter) {
            if (alter.getCommand() == AlterOperation.RENAME_TABLE) {
                replayCommittedRename();
                return;
            }
        }
        change.apply(metadataSvc, true);
    }

    private void replayCommittedMetadataChanges(long metaVersion, long committedStructureVersion, boolean applyRenameSidecars) {
        try (TableMetadataChangeLog metaChangeCursor = tableTransactionLog.getTableMetadataChangeLog(
                metaVersion, alterCommandWalFormatter)
        ) {
            while (metaChangeCursor.hasNext()) {
                replayCommittedMetadataChange(metaChangeCursor.next(), applyRenameSidecars);
            }
        }

        if (metadata.getMetadataVersion() != committedStructureVersion) {
            throw CairoException.critical(0)
                    .put("could not recover sequencer metadata [table=").put(tableToken)
                    .put(", metaVersion=").put(metadata.getMetadataVersion())
                    .put(", committedStructureVersion=").put(committedStructureVersion)
                    .put(']');
        }

        metadata.syncToMetaFile();
        tableToken = metadata.getTableToken();
    }

    private void replayCommittedRename() {
        // Full rebuild starts from the registry token, the durable table-name authority. Historical
        // rename sidecars can otherwise move metadata away from the registry after abandoned renames or rename chains.
        // skipTableRename() still bumps structureVersion so the replay matches the committed log's version count.
        metadata.skipTableRename();
    }

    void readLock() {
        assert !WRITE_LOCKS.get().contains(this) : "sequencer write lock re-entry";
        schemaLock.readLock().lock();
    }

    void unlockRead() {
        schemaLock.readLock().unlock();
    }

    void unlockWrite() {
        assert WRITE_LOCKS.get().remove(this) : "sequencer write lock not owned";
        schemaLock.writeLock().unlock();
    }

    void writeLock() {
        assert !WRITE_LOCKS.get().contains(this) : "sequencer write lock re-entry";
        schemaLock.writeLock().lock();
        assert WRITE_LOCKS.get().add(this);
    }
}
