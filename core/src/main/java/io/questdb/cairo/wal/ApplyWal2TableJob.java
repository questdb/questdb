/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.wal;

import io.questdb.Telemetry;
import io.questdb.TelemetryOrigin;
import io.questdb.TelemetrySystemEvent;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoKeywords;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.mv.MatViewRefreshTask;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TableMetadataChange;
import io.questdb.cairo.wal.seq.TableMetadataChangeLog;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.Job;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.TelemetryTask;
import io.questdb.tasks.TelemetryWalTask;
import io.questdb.tasks.WalTxnNotificationTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.TelemetrySystemEvent.*;
import static io.questdb.cairo.ErrorTag.OUT_OF_MEMORY;
import static io.questdb.cairo.ErrorTag.resolveTag;
import static io.questdb.cairo.TableUtils.TABLE_EXISTS;
import static io.questdb.cairo.pool.AbstractMultiTenantPool.NO_LOCK_REASON;
import static io.questdb.cairo.wal.WalTxnType.MAT_VIEW_INVALIDATE;
import static io.questdb.cairo.wal.WalTxnType.*;
import static io.questdb.cairo.wal.WalUtils.*;
import static io.questdb.tasks.TableWriterTask.CMD_ALTER_TABLE;
import static io.questdb.tasks.TableWriterTask.CMD_UPDATE_TABLE;

public class ApplyWal2TableJob extends AbstractQueueConsumerJob<WalTxnNotificationTask> implements Closeable {
    public static final String WAL_2_TABLE_RESUME_REASON = "Resume WAL Data Application";
    private static final Log LOG = LogFactory.getLog(ApplyWal2TableJob.class);
    private static final String WAL_2_TABLE_WRITE_REASON = "WAL Data Application";
    private final CairoConfiguration config;
    private final CairoEngine engine;
    private final WalMetrics metrics;
    private final MicrosecondClock microClock;
    private final MatViewRefreshTask mvRefreshTask = new MatViewRefreshTask();
    private final BlockFileWriter mvStateWriter;
    private final OperationExecutor operationExecutor;
    private final long tableTimeQuotaMicros;
    private final Telemetry<TelemetryTask> telemetry;
    private final TelemetryFacade telemetryFacade;
    private final WalEventReader walEventReader;
    private final Telemetry<TelemetryWalTask> walTelemetry;
    private final WalTelemetryFacade walTelemetryFacade;
    private long lastAttemptSeqTxn;
    private long lastCommittedRows;

    public ApplyWal2TableJob(CairoEngine engine, int sharedQueryWorkerCount) {
        super(engine.getMessageBus().getWalTxnNotificationQueue(), engine.getMessageBus().getWalTxnNotificationSubSequence());
        this.engine = engine;
        walTelemetry = engine.getTelemetryWal();
        walTelemetryFacade = walTelemetry.isEnabled() ? this::doStoreWalTelemetry : this::storeWalTelemetryNoop;
        telemetry = engine.getTelemetry();
        telemetryFacade = telemetry.isEnabled() ? this::doStoreTelemetry : this::storeTelemetryNoOp;
        operationExecutor = new OperationExecutor(engine, sharedQueryWorkerCount);
        CairoConfiguration configuration = engine.getConfiguration();
        microClock = configuration.getMicrosecondClock();
        walEventReader = new WalEventReader(configuration.getFilesFacade());
        metrics = engine.getMetrics().walMetrics();
        tableTimeQuotaMicros = configuration.getWalApplyTableTimeQuota() >= 0 ? configuration.getWalApplyTableTimeQuota() * 1000L : Micros.DAY_MICROS;
        config = engine.getConfiguration();
        mvStateWriter = new BlockFileWriter(config.getFilesFacade(), config.getCommitMode());
    }

    @Override
    public void close() {
        Misc.free(operationExecutor);
        Misc.free(walEventReader);
        Misc.free(mvStateWriter);
    }

    private static void cleanDroppedTableDirectory(CairoEngine engine, Path tempPath, TableToken tableToken) {
        // Clean all the files inside table folder name except WAL directories and SEQ_DIR directory
        boolean allClean = true;
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        tempPath.of(engine.getConfiguration().getDbRoot()).concat(tableToken);
        int rootLen = tempPath.size();

        long p = ff.findFirst(tempPath.$());
        if (p > 0) {
            try {
                do {
                    long pUtf8NameZ = ff.findName(p);
                    int type = ff.findType(p);
                    if (ff.isDirOrSoftLinkDirNoDots(tempPath, rootLen, pUtf8NameZ, type)) {
                        if (!CairoKeywords.isTxnSeq(pUtf8NameZ) && !CairoKeywords.isWal(pUtf8NameZ)) {
                            if (!ff.unlinkOrRemove(tempPath, LOG)) {
                                allClean = false;
                            }
                        }
                    } else if (type == Files.DT_FILE) {
                        tempPath.trimTo(rootLen);
                        tempPath.concat(pUtf8NameZ);

                        if (CairoKeywords.isTxn(pUtf8NameZ) || CairoKeywords.isMeta(pUtf8NameZ) || matchesWalLock(tempPath)) {
                            continue;
                        }

                        allClean &= removeOrLog(tempPath, ff);
                    }
                } while (ff.findNext(p) > 0);

                if (allClean) {
                    // Remove _txn and _meta files when all other files are removed
                    if (!removeOrLog(tempPath.trimTo(rootLen).concat(TableUtils.TXN_FILE_NAME), ff)) {
                        return;
                    }
                    ff.removeQuiet(tempPath.trimTo(rootLen).concat(TableUtils.META_FILE_NAME).$());
                }
            } finally {
                ff.findClose(p);
            }
        }
    }

    private static boolean matchesWalLock(Utf8Sequence name) {
        if (Utf8s.endsWithAscii(name, ".lock")) {
            for (int i = name.size() - ".lock".length() - 1; i > 0; i--) {
                byte b = name.byteAt(i);
                if (b < '0' || b > '9') {
                    return Utf8s.equalsAscii(WAL_NAME_BASE, 0, WAL_NAME_BASE.length(), name, i - WAL_NAME_BASE.length() + 1, i + 1);
                }
            }
        }

        for (int i = 0, n = name.size(); i < n; i++) {
            byte b = name.byteAt(i);
            if (b < '0' || b > '9') {
                return false;
            }
        }
        return true;
    }

    /**
     * Attempts to remove writer from the pool and delete the files. In that order,
     * specifically for windows. Sometimes there is writer in the context of this call.
     * In any case, this method will try to lock the writer and hold the lock until
     * table's files are removed.
     *
     * @param tableToken table token of the table to be purged
     * @param writer     writer instance if we have one
     * @param engine     the engine, used for its writer pool
     * @param tempPath   path used to check table dir existence
     */
    private static void purgeTableFiles(
            TableToken tableToken,
            @Nullable TableWriter writer,
            CairoEngine engine,
            @Transient Path tempPath
    ) {
        if (engine.lockReadersAndMetadata(tableToken)) {
            TableWriter writerToClose = null;
            try {
                final CairoConfiguration configuration = engine.getConfiguration();
                if (writer == null && TableUtils.exists(configuration.getFilesFacade(), tempPath, configuration.getDbRoot(), tableToken.getDirName()) == TABLE_EXISTS) {
                    try {
                        writer = writerToClose = engine.getWriterUnsafe(tableToken, WAL_2_TABLE_WRITE_REASON);
                    } catch (EntryUnavailableException ex) {
                        // Table is being written to, we cannot destroy it at the moment
                        return;
                    } catch (CairoException ex) {
                        // Ignore it, table can be half deleted.
                    }
                }
                // we want to release files, so that we can remove them, but we do not want
                // the writer to go back to the pool, in case someone else is about to use it.
                if (writer != null) {
                    // Force writer to close all the files.
                    writer.destroy();
                }

                // while holding the writer and essentially the lock on the table,
                // we can remove the files.
                cleanDroppedTableDirectory(engine, tempPath, tableToken);
            } finally {
                Misc.free(writerToClose);
                engine.unlockReadersAndMetadata(tableToken);
            }
        } else {
            LOG.info().$("table is dropped, waiting to acquire Table Readers lock to delete the table files [table=")
                    .$(tableToken).I$();
        }
    }

    private static boolean removeOrLog(Path path, FilesFacade ff) {
        if (!ff.removeQuiet(path.$())) {
            LOG.info().$("could not remove, will retry [path=").$(path).$(", errno=").$(ff.errno()).I$();
            return false;
        }
        return true;
    }

    /**
     * Iterates over all outstanding transactions in the WAL.
     * Interrupts if time limit is reached or termination is requested.
     * Returns true if it applied all the transactions, and false if it was terminated early
     *
     * @param tableToken        table token
     * @param writer            table writer
     * @param engine            cairo engine
     * @param operationExecutor operation executor
     * @param tempPath          temporary path
     * @param runStatus         run status
     */
    private void applyOutstandingWalTransactions(
            TableToken tableToken,
            TableWriter writer,
            CairoEngine engine,
            OperationExecutor operationExecutor,
            Path tempPath,
            RunStatus runStatus,
            TableWriterPressureControl pressureControl
    ) {
        final TableSequencerAPI tableSequencerAPI = engine.getTableSequencerAPI();
        boolean isTerminating;
        boolean finishedAll = true;
        long initialSeqTxn = writer.getSeqTxn();
        // Default to incremental mat view refresh.
        mvRefreshTask.clear();
        mvRefreshTask.operation = MatViewRefreshTask.INCREMENTAL_REFRESH;
        mvRefreshTask.baseTableToken = writer.getTableToken();

        try (TransactionLogCursor transactionLogCursor = tableSequencerAPI.getCursor(tableToken, writer.getAppliedSeqTxn())) {
            TableMetadataChangeLog structuralChangeCursor = null;
            try {
                int iTransaction = 0;
                int totalTransactionCount = 0;
                long rowsAdded = 0;
                long physicalRowsAdded = 0;
                long insertTimespan = 0;

                tempPath.of(engine.getConfiguration().getDbRoot()).concat(tableToken).slash();

                // Populate transactionMeta with timestamps of future transactions
                // to avoid O3 commits by pre-calculating safe to commit timestamp for every commit.
                writer.readWalTxnDetails(transactionLogCursor);
                transactionLogCursor.toTop();
                isTerminating = runStatus.isTerminating();
                final long timeLimit = microClock.getTicks() + tableTimeQuotaMicros;
                boolean firstRun = true;

                try {

                    WHILE_TRANSACTION_CURSOR:
                    while (!isTerminating && ((finishedAll = microClock.getTicks() <= timeLimit) || firstRun) && transactionLogCursor.hasNext()) {
                        firstRun = false;
                        final int walId = transactionLogCursor.getWalId();
                        final int segmentId = transactionLogCursor.getSegmentId();
                        final long segmentTxn = transactionLogCursor.getSegmentTxn();
                        final long commitTimestamp = transactionLogCursor.getCommitTimestamp();
                        final long seqTxn = transactionLogCursor.getTxn();

                        lastAttemptSeqTxn = seqTxn;
                        if (seqTxn != writer.getAppliedSeqTxn() + 1) {
                            throw CairoException.critical(0)
                                    .put("unexpected sequencer transaction, expected ").put(writer.getAppliedSeqTxn() + 1)
                                    .put(" but was ").put(seqTxn);
                        }

                        switch (walId) {
                            case METADATA_WALID:
                                // This is metadata change
                                // to be taken from Sequencer directly
                                final long newStructureVersion = transactionLogCursor.getStructureVersion();
                                if (writer.getColumnStructureVersion() != newStructureVersion - 1) {
                                    throw CairoException.critical(0)
                                            .put("unexpected new WAL structure version [walStructure=").put(newStructureVersion)
                                            .put(", tableStructureVersion=").put(writer.getColumnStructureVersion())
                                            .put(']');
                                }

                                boolean hasNext;
                                if (structuralChangeCursor == null || !(hasNext = structuralChangeCursor.hasNext())) {
                                    Misc.free(structuralChangeCursor);
                                    // Re-read the sequencer files to get the metadata change cursor.
                                    structuralChangeCursor = tableSequencerAPI.getMetadataChangeLogSlow(tableToken, newStructureVersion - 1);
                                    hasNext = structuralChangeCursor.hasNext();
                                }

                                if (hasNext) {
                                    final long start = microClock.getTicks();
                                    walTelemetryFacade.store(WAL_TXN_APPLY_START, tableToken, walId, seqTxn, -1L, -1L, start - commitTimestamp);
                                    writer.setSeqTxn(seqTxn);
                                    try {
                                        final TableMetadataChange metadataChangeOp = structuralChangeCursor.next();
                                        metadataChangeOp.apply(writer, true);
                                        final String matViewInvalidationReason = metadataChangeOp.matViewInvalidationReason();
                                        if (matViewInvalidationReason != null) {
                                            mvRefreshTask.operation = MatViewRefreshTask.INVALIDATE;
                                            mvRefreshTask.invalidationReason = matViewInvalidationReason;
                                        }
                                    } catch (Throwable th) {
                                        // Don't mark transaction as applied if exception occurred
                                        writer.setSeqTxn(seqTxn - 1);
                                        throw th;
                                    }
                                    walTelemetryFacade.store(WAL_TXN_STRUCTURE_CHANGE_APPLIED, tableToken, walId, seqTxn, -1L, -1L, microClock.getTicks() - start);
                                } else {
                                    // Something messed up in sequencer.
                                    // There is a transaction in WAL but no structure change record.
                                    throw CairoException.critical(0)
                                            .put("could not apply structure change from WAL to table. WAL metadata change does not exist [structureVersion=")
                                            .put(newStructureVersion)
                                            .put(']');
                                }
                                break;

                            case DROP_TABLE_WAL_ID:
                                engine.notifyDropped(tableToken);
                                purgeTableFiles(tableToken, writer, engine, tempPath);
                                return;

                            case 0:
                                throw CairoException.critical(0)
                                        .put("broken table transaction record in sequencer log, walId cannot be 0 [table=")
                                        .put(tableToken.getTableName()).put(", seqTxn=").put(seqTxn).put(']');

                            default:
                                // Always set full path when using thread static path
                                operationExecutor.setNowAndFixClock(commitTimestamp, writer.getTimestampType());
                                tempPath.of(engine.getConfiguration().getDbRoot()).concat(tableToken).slash().putAscii(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                                final long start = microClock.getTicks();

                                long lastLoadedTxnDetails = writer.getWalTnxDetails().getLastSeqTxn();
                                if (seqTxn > lastLoadedTxnDetails || (lastLoadedTxnDetails < seqTxn + config.getWalApplyLookAheadTransactionCount()
                                        && (transactionLogCursor.getMaxTxn() > lastLoadedTxnDetails || transactionLogCursor.extend()))
                                ) {
                                    // Last few transactions left to process from the list
                                    // of observed transactions built upfront in the beginning of the loop.
                                    // Read more transactions from the sequencer into readWalTxnDetails to continue
                                    writer.readWalTxnDetails(transactionLogCursor);
                                    transactionLogCursor.setPosition(seqTxn);
                                }

                                assert walId == writer.getWalTnxDetails().getWalId(seqTxn);
                                assert segmentId == writer.getWalTnxDetails().getSegmentId(seqTxn);

                                final int txnCommitted = processWalCommit(
                                        writer,
                                        walId,
                                        segmentId,
                                        tempPath,
                                        segmentTxn,
                                        operationExecutor,
                                        seqTxn,
                                        commitTimestamp,
                                        pressureControl
                                );
                                assert txnCommitted != 0;

                                if (txnCommitted > 0) {
                                    insertTimespan += microClock.getTicks() - start;
                                    rowsAdded += lastCommittedRows;
                                    iTransaction += txnCommitted;
                                    physicalRowsAdded += writer.getPhysicallyWrittenRowsSinceLastCommit();
                                    if (txnCommitted > 1) {
                                        transactionLogCursor.setPosition(writer.getAppliedSeqTxn());
                                    }
                                }

                                isTerminating = runStatus.isTerminating();
                                if (isTerminating) {
                                    // transaction cursor goes beyond prepared transactionMeta or termination requested. Re-run the loop.
                                    break WHILE_TRANSACTION_CURSOR;
                                }
                        }
                    }
                    totalTransactionCount += iTransaction;

                    if (!finishedAll || isTerminating) {
                        writer.commitSeqTxn();
                    }
                } catch (EjectApplyWalException ex) {
                    finishedAll = false;
                }

                finishedAll = finishedAll || (writer.getAppliedSeqTxn() == transactionLogCursor.getMaxTxn() && !transactionLogCursor.hasNext());
                if (totalTransactionCount > 0) {
                    LOG.info().$("job ")
                            .$(finishedAll ? "finished" : "ejected")
                            .$(" [table=").$(writer.getTableToken())
                            .$(", seqTxn=").$(writer.getAppliedSeqTxn())
                            .$(", transactions=").$(totalTransactionCount)
                            .$(", rows=").$(rowsAdded)
                            .$(", time=").$(insertTimespan / 1000)
                            .$("ms, rate=").$(rowsAdded * 1000000L / Math.max(1, insertTimespan))
                            .$("rows/s, ampl=").$(Math.round(100.0 * physicalRowsAdded / rowsAdded) / 100.0)
                            .I$();
                }

                if (initialSeqTxn < writer.getSeqTxn()) {
                    engine.notifyMatViewBaseTableCommit(mvRefreshTask, writer.getSeqTxn());
                }
            } catch (Throwable th) {
                // We could have been applying multiple txns, and we failed somewhere in the middle. The writer will
                // be returned to the pool and dirty writes will be rolled back. We have to update the sequencer
                // on the state of the writer and revert any dirty txns that might have advanced. We do that
                // by equalizing writerTxn and dirtyWriterTxn.
                engine.getTableSequencerAPI().updateWriterTxns(tableToken, writer.getSeqTxn(), writer.getSeqTxn());
                throw th;
            } finally {
                Misc.free(structuralChangeCursor);
            }
        }
    }

    private void doStoreTelemetry(short event, short origin) {
        TelemetryTask.store(telemetry, origin, event);
    }

    private void doStoreWalTelemetry(short event, TableToken tableToken, int walId, long seqTxn, long rowCount, long physicalRowCount, long latencyUs) {
        TelemetryWalTask.store(walTelemetry, event, tableToken.getTableId(), walId, seqTxn, rowCount, physicalRowCount, latencyUs);
    }

    private void handleWalApplyFailure(TableToken tableToken, Throwable throwable, SeqTxnTracker txnTracker) {
        ErrorTag errorTag;
        String errorMessage;

        if (engine.isTableDropped(tableToken)) {
            // Sometimes we can have SQL exceptions when re-compiling ALTER or UPDATE statements
            // that the table we work on is already dropped. In this case, we can ignore the exception.
            // WARNING: do not treat "table does not exist" same as "table is dropped"
            // table can be renamed, not dropped and deleting table files is not the right thing to do.
            purgeTableFiles(tableToken, null, engine, Path.PATH.get());
            return;
        }

        if (throwable instanceof CairoException) {
            CairoException cairoException = (CairoException) throwable;
            if (cairoException.isOutOfMemory()) {
                if (txnTracker != null) {
                    txnTracker.getMemPressureControl().onOutOfMemory();
                    if (txnTracker.getMemPressureControl().isReadyToProcess()) {
                        engine.notifyWalTxnRepublisher(tableToken);
                        return;
                    } else {
                        LOG.info().$("high memory pressure, table is backed off from processing WAL transactions [table=")
                                .$(tableToken).I$();
                    }
                }
                errorTag = OUT_OF_MEMORY;
            } else {
                errorTag = resolveTag(cairoException.getErrno());
            }
            errorMessage = cairoException.getFlyweightMessage().toString();
        } else {
            errorTag = ErrorTag.NONE;
            errorMessage = throwable.getMessage();
        }

        try {
            telemetryFacade.store(TelemetrySystemEvent.WAL_APPLY_SUSPEND, TelemetryOrigin.WAL_APPLY);
            LogRecord logRecord = LOG.critical().$("job failed, table suspended [table=").$(tableToken);
            if (lastAttemptSeqTxn > -1) {
                logRecord.$(", seqTxn=").$(lastAttemptSeqTxn);
            }
            // These errors are important, so we want to always log the stacktrace (if we have it).
            logRecord.$(", error=").$(throwable).I$();
            engine.getTableSequencerAPI().suspendTable(tableToken, errorTag, errorMessage);
        } catch (CairoException e) {
            LOG.critical().$("could not suspend table [table=").$(tableToken)
                    .$(", error=").$safe(e.getFlyweightMessage())
                    .I$();
        }
    }

    private int processWalCommit(
            TableWriter writer,
            int walId,
            int segmentId,
            @Transient Path walPath,
            long segmentTxn,
            OperationExecutor operationExecutor,
            long seqTxn,
            long commitTimestamp,
            TableWriterPressureControl pressureControl
    ) {
        WalTxnDetails txnDetails = writer.getWalTnxDetails();
        final byte walTxnType = txnDetails.getWalTxnType(seqTxn);
        final long start = microClock.getTicks();

        switch (walTxnType) {
            case DATA:
            case MAT_VIEW_DATA:
                walTelemetryFacade.store(WAL_TXN_APPLY_START, writer.getTableToken(), walId, seqTxn, -1L, -1L, start - commitTimestamp);
                writer.commitWalInsertTransactions(
                        walPath,
                        seqTxn,
                        pressureControl
                );
                final long latency = microClock.getTicks() - start;
                long totalPhysicalRowCount = writer.getPhysicallyWrittenRowsSinceLastCommit();
                long lastCommittedSeqTxn = writer.getAppliedSeqTxn();
                lastCommittedRows = 0;
                for (long s = seqTxn; s <= lastCommittedSeqTxn; s++) {
                    long walRowCount = txnDetails.getSegmentRowHi(s) - txnDetails.getSegmentRowLo(s);
                    long commitPhRowCount = s == lastCommittedSeqTxn ? totalPhysicalRowCount : 0;
                    metrics.addApplyRowsWritten(walRowCount, commitPhRowCount, latency);
                    walTelemetryFacade.store(WAL_TXN_DATA_APPLIED, writer.getTableToken(), walId, s, walRowCount, commitPhRowCount, latency);
                    lastCommittedRows += walRowCount;
                }

                if (writer.getTableToken().isMatView()) {
                    for (long s = lastCommittedSeqTxn; s >= seqTxn; s--) {
                        byte txnType = txnDetails.getWalTxnType(s);
                        if (txnType == MAT_VIEW_DATA) {
                            try {
                                final Path path = Path.PATH2.get();
                                final TableToken token = writer.getTableToken();
                                path.of(engine.getConfiguration().getDbRoot()).concat(token);
                                updateMatViewRefreshState(
                                        path,
                                        txnDetails.getMatViewRefreshTxn(s),
                                        txnDetails.getMatViewRefreshTimestamp(s),
                                        false,
                                        null,
                                        txnDetails.getMatViewPeriodHi(s),
                                        // Mat view data commit means that cached intervals were applied and should be evicted.
                                        null,
                                        -1
                                );
                            } catch (CairoException e) {
                                LOG.error().$("could not update state for materialized view [view=").$(writer.getTableToken())
                                        .$(", msg=").$safe(e.getFlyweightMessage())
                                        .$(", errno=").$(e.getErrno())
                                        .I$();
                            }
                            break; // we've found the latest mat view state, not need to check earlier transactions
                        }
                    }
                }

                return (int) (writer.getAppliedSeqTxn() - seqTxn + 1);
            case SQL:
                try (WalEventReader eventReader = walEventReader) {
                    final WalEventCursor walEventCursor = eventReader.of(walPath, segmentTxn);
                    final WalEventCursor.SqlInfo sqlInfo = walEventCursor.getSqlInfo();
                    walTelemetryFacade.store(WAL_TXN_APPLY_START, writer.getTableToken(), walId, seqTxn, -1L, -1L, start - commitTimestamp);
                    processWalSql(writer, sqlInfo, operationExecutor, seqTxn);
                    walTelemetryFacade.store(WAL_TXN_SQL_APPLIED, writer.getTableToken(), walId, seqTxn, -1L, -1L, microClock.getTicks() - start);
                    lastCommittedRows = 0;
                    return 1;
                }
            case TRUNCATE:
                long txn = writer.getTxn();
                writer.setSeqTxn(seqTxn);
                writer.removeAllPartitions();
                if (writer.getTxn() == txn) {
                    // force mark the transaction as applied
                    writer.markSeqTxnCommitted(seqTxn);
                }
                lastCommittedRows = 0;
                // Invalidate dependent mat views on truncate.
                mvRefreshTask.operation = MatViewRefreshTask.INVALIDATE;
                mvRefreshTask.invalidationReason = "truncate operation";
                return 1;
            case MAT_VIEW_INVALIDATE:
                try (WalEventReader eventReader = walEventReader) {
                    final Path path = Path.PATH2.get();
                    final TableToken token = writer.getTableToken();
                    path.of(engine.getConfiguration().getDbRoot()).concat(token);
                    int tablePathLen = path.size();
                    path.slash().putAscii(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                    final WalEventCursor walEventCursor = eventReader.of(path, segmentTxn);
                    final WalEventCursor.MatViewInvalidationInfo info = walEventCursor.getMatViewInvalidationInfo();
                    updateMatViewRefreshState(
                            path.trimTo(tablePathLen),
                            info.getLastRefreshBaseTableTxn(),
                            info.getLastRefreshTimestampUs(),
                            info.isInvalid(),
                            info.getInvalidationReason(),
                            info.getLastPeriodHi(),
                            info.getRefreshIntervals(),
                            info.getRefreshIntervalsBaseTxn()
                    );
                } catch (CairoException e) {
                    LOG.error().$("could not update state for materialized view [view=").$(writer.getTableToken())
                            .$(", msg=").$safe(e.getFlyweightMessage())
                            .$(", errno=").$(e.getErrno())
                            .I$();
                }
                // WAL-E files can be deleted by the purge job after a commit.
                // Update the materialized view state before committing the transaction.
                writer.setSeqTxn(seqTxn);
                writer.markSeqTxnCommitted(seqTxn);
                lastCommittedRows = 0;
                return 1;
            default:
                throw new UnsupportedOperationException("Unsupported WAL txn type: " + walTxnType);
        }
    }

    private void processWalSql(TableWriter tableWriter, WalEventCursor.SqlInfo sqlInfo, OperationExecutor operationExecutor, long seqTxn) {
        final int cmdType = sqlInfo.getCmdType();
        final CharSequence sql = sqlInfo.getSql();
        operationExecutor.resetRnd(sqlInfo.getRndSeed0(), sqlInfo.getRndSeed1());
        sqlInfo.populateBindVariableService(operationExecutor.getBindVariableService());
        try {
            while (true) {
                try {
                    switch (cmdType) {
                        case CMD_ALTER_TABLE:
                            final String matViewInvalidationReason = operationExecutor.executeAlter(tableWriter, sql, seqTxn);
                            if (matViewInvalidationReason != null) {
                                mvRefreshTask.operation = MatViewRefreshTask.INVALIDATE;
                                mvRefreshTask.invalidationReason = matViewInvalidationReason;
                            }
                            return;
                        case CMD_UPDATE_TABLE:
                            final long rowsAffected = operationExecutor.executeUpdate(tableWriter, sql, seqTxn);
                            if (rowsAffected > 0) {
                                mvRefreshTask.operation = MatViewRefreshTask.INVALIDATE;
                                mvRefreshTask.invalidationReason = UpdateOperation.MAT_VIEW_INVALIDATION_REASON;
                            }
                            return;
                        default:
                            throw new UnsupportedOperationException("Unsupported command type: " + cmdType);
                    }
                } catch (SqlException ex) {
                    if (ex.isWalRecoverable()) {
                        LOG.info().$("recoverable error applying SQL to wal table [table=").$(tableWriter.getTableToken())
                                .$(", sql=").$(sql)
                                .$(", position=").$(ex.getPosition())
                                .$(", error=").$safe(ex.getFlyweightMessage())
                                .I$();

                        return;
                    }
                    if (!ex.isTableDoesNotExist()) {
                        throw ex;
                    }
                } catch (CairoException ex) {
                    if (!ex.isTableDoesNotExist()) {
                        throw ex;
                    }
                }

                TableToken tableToken = tableWriter.getTableToken();

                // Getting to here means we got Table Does Not Exist SQL or Cairo Exception.
                // Table may be renamed or dropped while processing the WAL transaction.
                // Need to refresh the table token and retry.
                TableToken updatedToken = engine.getUpdatedTableToken(tableToken);
                if (updatedToken == null || tableToken.equals(updatedToken)) {
                    if (engine.isTableDropped(tableToken)) {
                        // This is definitely dropped table.
                        throw CairoException.tableDropped(tableToken);
                    }
                    // No progress, same token or no token, and it's not dropped.
                    // Stop processing WAL transactions for this table, switch to the next table.
                    LOG.info().$("failed to compile SQL, table rename not fully applied, will retry [table=")
                            .$(tableToken).I$();
                    throw EjectApplyWalException.INSTANCE;
                }
                tableWriter.updateTableToken(updatedToken);
            }
        } catch (SqlException ex) {
            throw CairoException.nonCritical().put("error applying SQL to wal table [table=")
                    .put(tableWriter.getTableToken().getTableName()).put(", sql=").put(sql)
                    .put(", position=").put(ex.getPosition())
                    .put(", error=").put(ex.getFlyweightMessage());
        } catch (CairoException e) {
            if (e.isTableDropped()) {
                throw e;
            }
            LogRecord log = !e.isWALTolerable() ? LOG.error() : LOG.info();
            log.$("error applying SQL to wal table [table=").$(tableWriter.getTableToken())
                    .$(", sql=").$(sql)
                    .$(", msg=").$safe(e.getFlyweightMessage())
                    .$(", errno=").$(e.getErrno())
                    .I$();

            if (!e.isWALTolerable()) {
                throw e;
            } else {
                // Mark as applied.
                tableWriter.commitSeqTxn(seqTxn);
            }
        }
    }

    private void storeTelemetryNoOp(short event, short origin) {
    }

    private void storeWalTelemetryNoop(short event, TableToken tableToken, int walId, long seqTxn, long rowCount, long physicalRowCount, long latencyUs) {
    }

    private void updateMatViewRefreshState(
            Path tablePath,
            long lastRefreshBaseTxn,
            long lastRefreshTimestamp,
            boolean invalid,
            @Nullable CharSequence invalidationReason,
            long lastPeriodHi,
            @Nullable LongList refreshIntervals,
            long refreshIntervalsBaseTxn
    ) {
        try (BlockFileWriter stateWriter = mvStateWriter) {
            stateWriter.of(tablePath.concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
            MatViewState.append(
                    lastRefreshTimestamp,
                    lastRefreshBaseTxn,
                    invalid,
                    invalidationReason,
                    lastPeriodHi,
                    refreshIntervals,
                    refreshIntervalsBaseTxn,
                    stateWriter
            );
        }
    }

    /**
     * Returns transaction number, which is always > -1. Negative values are used as status code.
     */
    void applyWal(
            @NotNull TableToken tableToken,
            CairoEngine engine,
            OperationExecutor operationExecutor,
            Job.RunStatus runStatus
    ) {
        final Path tempPath = Path.PATH.get();
        SeqTxnTracker txnTracker = null;
        this.lastAttemptSeqTxn = -1;
        try {
            // security context is checked on writing to the WAL and can be ignored here
            final TableToken updatedToken = engine.getUpdatedTableToken(tableToken);
            if (engine.isTableDropped(tableToken) || updatedToken == null) {
                if (engine.isTableDropped(tableToken)) {
                    purgeTableFiles(tableToken, null, engine, tempPath);
                }
                // else: table is dropped and fully cleaned, this is late notification.
            } else {
                long writerTxn, dirtyWriterTxn;
                txnTracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);
                TableWriterPressureControl pressureControl = txnTracker.getMemPressureControl();
                TableWriter writer = null;
                try {
                    writer = engine.getWriterUnsafe(updatedToken, WAL_2_TABLE_WRITE_REASON);
                    assert writer.getMetadata().getTableId() == tableToken.getTableId();
                    if (!pressureControl.isReadyToProcess()) {
                        // rely on CheckWalTransactionsJob to notify us when to apply transactions
                        return;
                    }
                    applyOutstandingWalTransactions(tableToken, writer, engine, operationExecutor, tempPath, runStatus, pressureControl);
                    if (pressureControl.onEnoughMemory()) {
                        LOG.info().$("table writing memory pressure is easing up [table=").$(tableToken)
                                .$(", parallelMemoryLimit=").$(pressureControl.getMemoryPressureRegulationValue())
                                .I$();
                    }
                    writerTxn = writer.getSeqTxn();
                    dirtyWriterTxn = writer.getAppliedSeqTxn();
                } catch (EntryUnavailableException tableBusy) {
                    //noinspection StringEquality
                    if (tableBusy.getReason() != NO_LOCK_REASON
                            && !WAL_2_TABLE_WRITE_REASON.equals(tableBusy.getReason())
                            && !WAL_2_TABLE_RESUME_REASON.equals(tableBusy.getReason())) {
                        LOG.critical().$("unsolicited table lock [table=").$(tableToken)
                                .$(", lockReason=").$(tableBusy.getReason())
                                .I$();
                        // This is abnormal termination but table is not set to suspended state.
                        // Reset state of SeqTxnTracker so that next CheckWalTransactionJob run will send job notification if necessary.
                        engine.notifyWalTxnRepublisher(tableToken);
                    }
                    // Do not suspend table. Perhaps writer will be unlocked with no transaction applied.
                    // We do not suspend table because of having initial value on writerTxn. It will either be
                    // "ignore" or last txn we applied.
                    return;
                } catch (Throwable th) {
                    // There is some unexpected error and table will likely to be suspended.
                    // It is safer to create new TableWriter after exceptions.
                    if (writer != null) {
                        writer.markDistressed();
                    }
                    throw th;
                } finally {
                    Misc.free(writer);
                }
                if (engine.getTableSequencerAPI().updateWriterTxns(tableToken, writerTxn, dirtyWriterTxn)) {
                    engine.notifyWalTxnCommitted(tableToken);
                }
            }
        } catch (Throwable ex) {
            handleWalApplyFailure(tableToken, ex, txnTracker);
        }
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        final TableToken tableToken;
        try {
            final WalTxnNotificationTask task = queue.get(cursor);
            tableToken = task.getTableToken();
        } finally {
            // Do not hold the queue while transactions are applied to the table
            subSeq.done(cursor);
        }

        applyWal(tableToken, engine, operationExecutor, runStatus);
        return true;
    }

    @FunctionalInterface
    private interface TelemetryFacade {
        void store(short event, short origin);
    }

    @FunctionalInterface
    private interface WalTelemetryFacade {
        void store(short event, TableToken tableToken, int walId, long seqTxn, long rowCount, long physicalRowCount, long latencyUs);
    }

    private static class EjectApplyWalException extends RuntimeException {
        public static final EjectApplyWalException INSTANCE = new EjectApplyWalException();
    }
}
