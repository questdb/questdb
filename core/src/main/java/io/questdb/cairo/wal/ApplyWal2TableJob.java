/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.*;
import io.questdb.cairo.wal.seq.TableMetadataChangeLog;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.Job;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.TelemetryTask;
import io.questdb.tasks.TelemetryWalTask;
import io.questdb.tasks.WalTxnNotificationTask;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

import static io.questdb.TelemetrySystemEvent.*;
import static io.questdb.cairo.TableUtils.TABLE_EXISTS;
import static io.questdb.cairo.pool.AbstractMultiTenantPool.NO_LOCK_REASON;
import static io.questdb.cairo.wal.WalTxnType.*;
import static io.questdb.cairo.wal.WalUtils.*;
import static io.questdb.tasks.TableWriterTask.CMD_ALTER_TABLE;
import static io.questdb.tasks.TableWriterTask.CMD_UPDATE_TABLE;

public class ApplyWal2TableJob extends AbstractQueueConsumerJob<WalTxnNotificationTask> implements Closeable {
    public static final String WAL_2_TABLE_RESUME_REASON = "Resume WAL Data Application";
    private static final Log LOG = LogFactory.getLog(ApplyWal2TableJob.class);
    private static final String WAL_2_TABLE_WRITE_REASON = "WAL Data Application";
    private static final int WAL_APPLY_FAILED = -2;
    private static final int WAL_APPLY_IGNORE_ERROR = -1;
    private final CairoEngine engine;
    private final int lookAheadTransactionCount;
    private final WalMetrics metrics;
    private final MicrosecondClock microClock;
    private final OperationExecutor operationExecutor;
    private final long tableTimeQuotaMicros;
    private final Telemetry<TelemetryTask> telemetry;
    private final TelemetryFacade telemetryFacade;
    private final WalEventReader walEventReader;
    private final Telemetry<TelemetryWalTask> walTelemetry;
    private final WalTelemetryFacade walTelemetryFacade;

    public ApplyWal2TableJob(CairoEngine engine, int workerCount, int sharedWorkerCount) {
        super(engine.getMessageBus().getWalTxnNotificationQueue(), engine.getMessageBus().getWalTxnNotificationSubSequence());
        this.engine = engine;
        walTelemetry = engine.getTelemetryWal();
        walTelemetryFacade = walTelemetry.isEnabled() ? this::doStoreWalTelemetry : this::storeWalTelemetryNoop;
        telemetry = engine.getTelemetry();
        telemetryFacade = telemetry.isEnabled() ? this::doStoreTelemetry : this::storeTelemetryNoop;
        operationExecutor = new OperationExecutor(engine, workerCount, sharedWorkerCount);
        CairoConfiguration configuration = engine.getConfiguration();
        microClock = configuration.getMicrosecondClock();
        walEventReader = new WalEventReader(configuration.getFilesFacade());
        metrics = engine.getMetrics().walMetrics();
        lookAheadTransactionCount = configuration.getWalApplyLookAheadTransactionCount();
        tableTimeQuotaMicros = configuration.getWalApplyTableTimeQuota() >= 0 ? configuration.getWalApplyTableTimeQuota() * 1000L : Timestamps.DAY_MICROS;
    }

    @Override
    public void close() {
        Misc.free(operationExecutor);
        Misc.free(walEventReader);
    }

    private static boolean cleanDroppedTableDirectory(CairoEngine engine, Path tempPath, TableToken tableToken) {
        // Clean all the files inside table folder name except WAL directories and SEQ_DIR directory
        boolean allClean = true;
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        tempPath.of(engine.getConfiguration().getRoot()).concat(tableToken);
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
                        return false;
                    }
                    return ff.removeQuiet(tempPath.trimTo(rootLen).concat(TableUtils.META_FILE_NAME));
                }
            } finally {
                ff.findClose(p);
            }
        }
        return false;
    }

    private static boolean removeOrLog(Path path, FilesFacade ff) {
        if (!ff.removeQuiet(path.$())) {
            LOG.info().$("could not remove, will retry [path=").utf8(", errno=").$(ff.errno()).I$();
            return false;
        }
        return true;
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

    private static boolean tryDestroyDroppedTable(TableToken tableToken, TableWriter writer, CairoEngine engine, Path tempPath) {
        if (engine.lockReadersAndMetadata(tableToken)) {
            TableWriter writerToClose = null;
            try {
                final CairoConfiguration configuration = engine.getConfiguration();
                if (writer == null && TableUtils.exists(configuration.getFilesFacade(), tempPath, configuration.getRoot(), tableToken.getDirName()) == TABLE_EXISTS) {
                    try {
                        writer = writerToClose = engine.getWriterUnsafe(tableToken, WAL_2_TABLE_WRITE_REASON);
                    } catch (EntryUnavailableException ex) {
                        // Table is being written to, we cannot destroy it at the moment
                        return false;
                    } catch (CairoException ex) {
                        // Ignore it, table can be half deleted.
                    }
                }
                if (writer != null) {
                    // Force writer to close all the files.
                    writer.destroy();
                }
                return cleanDroppedTableDirectory(engine, tempPath, tableToken);
            } finally {
                if (writerToClose != null) {
                    writerToClose.close();
                }
                engine.unlockReadersAndMetadata(tableToken);
            }
        } else {
            LOG.info().$("table '").utf8(tableToken.getDirName())
                    .$("' is dropped, waiting to acquire Table Readers lock to delete the table files").$();
        }
        return false;
    }

    // Returns true if the application is finished and false if it's early terminated
    private void applyOutstandingWalTransactions(
            TableToken tableToken,
            TableWriter writer,
            CairoEngine engine,
            OperationExecutor operationExecutor,
            Path tempPath,
            RunStatus runStatus
    ) {
        final TableSequencerAPI tableSequencerAPI = engine.getTableSequencerAPI();
        boolean isTerminating;
        boolean finishedAll = true;

        try (TransactionLogCursor transactionLogCursor = tableSequencerAPI.getCursor(tableToken, writer.getAppliedSeqTxn())) {
            TableMetadataChangeLog structuralChangeCursor = null;
            try {
                int iTransaction = 0;
                int totalTransactionCount = 0;
                long rowsAdded = 0;
                long physicalRowsAdded = 0;
                long insertTimespan = 0;

                tempPath.of(engine.getConfiguration().getRoot()).concat(tableToken).slash();

                // Populate transactionMeta with timestamps of future transactions
                // to avoid O3 commits by pre-calculating safe to commit timestamp for every commit.
                writer.readWalTxnDetails(transactionLogCursor);
                transactionLogCursor.toTop();

                isTerminating = runStatus.isTerminating();
                final long timeLimit = microClock.getTicks() + tableTimeQuotaMicros;
                boolean firstRun = true;
                WHILE_TRANSACTION_CURSOR:
                while (!isTerminating && ((finishedAll = microClock.getTicks() <= timeLimit) || firstRun) && transactionLogCursor.hasNext()) {
                    firstRun = false;
                    final int walId = transactionLogCursor.getWalId();
                    final int segmentId = transactionLogCursor.getSegmentId();
                    final long segmentTxn = transactionLogCursor.getSegmentTxn();
                    final long commitTimestamp = transactionLogCursor.getCommitTimestamp();
                    final long seqTxn = transactionLogCursor.getTxn();

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
                                structuralChangeCursor.next().apply(writer, true);
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

                        case DROP_TABLE_WALID:
                            engine.notifyDropped(tableToken);
                            tryDestroyDroppedTable(tableToken, writer, engine, tempPath);
                            return;

                        case 0:
                            throw CairoException.critical(0)
                                    .put("broken table transaction record in sequencer log, walId cannot be 0 [table=")
                                    .put(tableToken.getTableName()).put(", seqTxn=").put(seqTxn).put(']');

                        default:
                            // Always set full path when using thread static path
                            operationExecutor.setNowAndFixClock(commitTimestamp);
                            tempPath.of(engine.getConfiguration().getRoot()).concat(tableToken).slash().putAscii(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                            final long start = microClock.getTicks();

                            long lastLoadedTxnDetails = writer.getWalTnxDetails().getLastSeqTxn();
                            if (seqTxn > lastLoadedTxnDetails
                                    || (lastLoadedTxnDetails < seqTxn + lookAheadTransactionCount && (transactionLogCursor.getMaxTxn() > lastLoadedTxnDetails || transactionLogCursor.extend()))
                            ) {
                                // Last few transactions left to process from the list
                                // of observed transactions built upfront in the beginning of the loop.
                                // Read more transactions from the sequencer into readWalTxnDetails to continue
                                writer.readWalTxnDetails(transactionLogCursor);
                                transactionLogCursor.setPosition(seqTxn);
                            }

                            long walSegment = writer.getWalTnxDetails().getWalSegmentId(seqTxn);
                            assert walId == Numbers.decodeHighInt(walSegment);
                            assert segmentId == Numbers.decodeLowInt(walSegment);

                            isTerminating = runStatus.isTerminating();
                            final long added = processWalCommit(
                                    writer,
                                    walId,
                                    tempPath,
                                    segmentTxn,
                                    operationExecutor,
                                    seqTxn,
                                    commitTimestamp
                            );

                            if (added > -1L) {
                                insertTimespan += microClock.getTicks() - start;
                                rowsAdded += added;
                                iTransaction++;
                                physicalRowsAdded += writer.getPhysicallyWrittenRowsSinceLastCommit();
                            }
                            if (added == -2L || isTerminating) {
                                // transaction cursor goes beyond prepared transactionMeta or termination requested. Re-run the loop.
                                break WHILE_TRANSACTION_CURSOR;
                            }
                    }
                }
                totalTransactionCount += iTransaction;

                if (!finishedAll || isTerminating) {
                    writer.commitSeqTxn();
                }

                if (totalTransactionCount > 0) {
                    LOG.info().$("job ")
                            .$(finishedAll ? "finished" : "ejected")
                            .$(" [table=").utf8(writer.getTableToken().getDirName())
                            .$(", seqTxn=").$(writer.getAppliedSeqTxn())
                            .$(", transactions=").$(totalTransactionCount)
                            .$(", rows=").$(rowsAdded)
                            .$(", time=").$(insertTimespan / 1000)
                            .$("ms, rate=").$(rowsAdded * 1000000L / Math.max(1, insertTimespan))
                            .$("rows/s, physicalWrittenRowsMultiplier=").$(Math.round(100.0 * physicalRowsAdded / rowsAdded) / 100.0)
                            .I$();
                }
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

    private long processWalCommit(
            TableWriter writer,
            int walId,
            @Transient Path walPath,
            long segmentTxn,
            OperationExecutor operationExecutor,
            long seqTxn,
            long commitTimestamp
    ) {
        try (WalEventReader eventReader = walEventReader) {
            final WalEventCursor walEventCursor = eventReader.of(walPath, WAL_FORMAT_VERSION, segmentTxn);
            final byte walTxnType = walEventCursor.getType();
            switch (walTxnType) {
                case DATA:
                    final WalEventCursor.DataInfo dataInfo = walEventCursor.getDataInfo();
                    if (writer.getWalTnxDetails().hasRecord(seqTxn)) {
                        long rowCount = dataInfo.getEndRowID() - dataInfo.getStartRowID();
                        final long start = microClock.getTicks();
                        walTelemetryFacade.store(WAL_TXN_APPLY_START, writer.getTableToken(), walId, seqTxn, -1L, -1L, start - commitTimestamp);
                        final long rowsAdded = writer.commitWalTransaction(
                                walPath,
                                !dataInfo.isOutOfOrder(),
                                dataInfo.getStartRowID(),
                                dataInfo.getEndRowID(),
                                dataInfo.getMinTimestamp(),
                                dataInfo.getMaxTimestamp(),
                                dataInfo,
                                seqTxn
                        );
                        final long latency = microClock.getTicks() - start;
                        long physicalRowCount = writer.getPhysicallyWrittenRowsSinceLastCommit();
                        metrics.addApplyRowsWritten(rowCount, physicalRowCount, latency);
                        walTelemetryFacade.store(WAL_TXN_DATA_APPLIED, writer.getTableToken(), walId, seqTxn, rowsAdded, physicalRowCount, latency);
                        return rowCount;
                    } else {
                        // re-build wal transaction details
                        return -2L;
                    }

                case SQL:
                    final WalEventCursor.SqlInfo sqlInfo = walEventCursor.getSqlInfo();
                    final long start = microClock.getTicks();
                    walTelemetryFacade.store(WAL_TXN_APPLY_START, writer.getTableToken(), walId, seqTxn, -1L, -1L, start - commitTimestamp);
                    final long rowsAffected = processWalSql(writer, sqlInfo, operationExecutor, seqTxn);
                    walTelemetryFacade.store(WAL_TXN_SQL_APPLIED, writer.getTableToken(), walId, seqTxn, -1L, -1L, microClock.getTicks() - start);
                    return rowsAffected;
                case TRUNCATE:
                    long txn = writer.getTxn();
                    writer.setSeqTxn(seqTxn);
                    writer.removeAllPartitions();
                    if (writer.getTxn() == txn) {
                        // force mark the transaction as applied
                        writer.markSeqTxnCommitted(seqTxn);
                    }
                    return -1L;
                default:
                    throw new UnsupportedOperationException("Unsupported WAL txn type: " + walTxnType);
            }
        }
    }

    private long processWalSql(TableWriter tableWriter, WalEventCursor.SqlInfo sqlInfo, OperationExecutor operationExecutor, long seqTxn) {
        final int cmdType = sqlInfo.getCmdType();
        final CharSequence sql = sqlInfo.getSql();
        operationExecutor.resetRnd(sqlInfo.getRndSeed0(), sqlInfo.getRndSeed1());
        sqlInfo.populateBindVariableService(operationExecutor.getBindVariableService());
        try {
            switch (cmdType) {
                case CMD_ALTER_TABLE:
                    operationExecutor.executeAlter(tableWriter, sql, seqTxn);
                    return -1;
                case CMD_UPDATE_TABLE:
                    return operationExecutor.executeUpdate(tableWriter, sql, seqTxn);
                default:
                    throw new UnsupportedOperationException("Unsupported command type: " + cmdType);
            }
        } catch (SqlException ex) {
            throw CairoException.nonCritical().put("error applying SQL to wal table [table=")
                    .put(tableWriter.getTableToken().getTableName()).put(", sql=").put(sql)
                    .put(", position=").put(ex.getPosition())
                    .put(", error=").put(ex.getFlyweightMessage());
        } catch (CairoException e) {
            LogRecord log = !e.isWALTolerable() ? LOG.error() : LOG.info();
            log.$("error applying SQL to wal table [table=")
                    .utf8(tableWriter.getTableToken().getTableName()).$(", sql=").$(sql)
                    .$(", error=").$(e.getFlyweightMessage())
                    .$(", errno=").$(e.getErrno()).I$();

            if (!e.isWALTolerable()) {
                throw e;
            } else {
                // Mark as applied.
                tableWriter.commitSeqTxn(seqTxn);
                return -1;
            }
        }
    }

    private void storeTelemetryNoop(short event, short origin) {
    }

    private void storeWalTelemetryNoop(short event, TableToken tableToken, int walId, long seqTxn, long rowCount, long physicalRowCount, long latencyUs) {
    }

    /**
     * Returns transaction number, which is always > -1. Negative values are used as status code.
     */
    long applyWal(
            @NotNull TableToken tableToken,
            CairoEngine engine,
            OperationExecutor operationCompiler,
            Job.RunStatus runStatus
    ) {
        long lastWriterTxn = WAL_APPLY_IGNORE_ERROR;
        Path tempPath = Path.PATH.get();

        try {
            // security context is checked on writing to the WAL and can be ignored here
            TableToken updatedToken = engine.getUpdatedTableToken(tableToken);
            if (engine.isTableDropped(tableToken) || updatedToken == null) {
                if (engine.isTableDropped(tableToken)) {
                    return tryDestroyDroppedTable(tableToken, null, engine, tempPath) ? Long.MAX_VALUE : -1;
                }
                // else: table is dropped and fully cleaned, this is late notification.
                return Long.MAX_VALUE;
            }

            try (TableWriter writer = engine.getWriterUnsafe(updatedToken, WAL_2_TABLE_WRITE_REASON)) {
                assert writer.getMetadata().getTableId() == tableToken.getTableId();
                applyOutstandingWalTransactions(tableToken, writer, engine, operationCompiler, tempPath, runStatus);
                lastWriterTxn = writer.getSeqTxn();
            } catch (EntryUnavailableException tableBusy) {
                //noinspection StringEquality
                if (tableBusy.getReason() != NO_LOCK_REASON
                        && !WAL_2_TABLE_WRITE_REASON.equals(tableBusy.getReason())
                        && !WAL_2_TABLE_RESUME_REASON.equals(tableBusy.getReason())) {
                    LOG.critical().$("unsolicited table lock [table=").utf8(tableToken.getDirName()).$(", lockReason=").$(tableBusy.getReason()).I$();
                    // This is abnormal termination but table is not set to suspended state.
                    // Reset state of SeqTxnTracker so that next CheckWalTransactionJob run will send job notification if necessary.
                    engine.notifyWalTxnRepublisher(tableToken);
                }
                // Don't suspend table. Perhaps writer will be unlocked with no transaction applied.
                // We don't suspend table by virtue of having initial value on lastWriterTxn. It will either be
                // "ignore" or last txn we applied.
                return lastWriterTxn;
            }

            if (engine.getTableSequencerAPI().notifyCommitReadable(tableToken, lastWriterTxn)) {
                engine.notifyWalTxnCommitted(tableToken);
            }
        } catch (CairoException ex) {
            if (ex.isTableDropped() || engine.isTableDropped(tableToken)) {
                engine.notifyDropped(tableToken);
                // Table is dropped, and we received cairo exception in the middle of apply
                return tryDestroyDroppedTable(tableToken, null, engine, tempPath) ? Long.MAX_VALUE : WAL_APPLY_IGNORE_ERROR;
            }
            telemetryFacade.store(TelemetrySystemEvent.WAL_APPLY_SUSPEND, TelemetryOrigin.WAL_APPLY);
            LOG.critical().$("job failed, table suspended [table=").utf8(tableToken.getDirName())
                    .$(", error=").$(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
            return WAL_APPLY_FAILED;
        } catch (Throwable ex) {
            telemetryFacade.store(TelemetrySystemEvent.WAL_APPLY_SUSPEND, TelemetryOrigin.WAL_APPLY);
            LOG.critical().$("job failed, table suspended [table=").utf8(tableToken.getDirName())
                    .$(", error=").$(ex)
                    .I$();
            return WAL_APPLY_FAILED;
        }
        return lastWriterTxn;
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        final TableToken tableToken;

        try {
            WalTxnNotificationTask task = queue.get(cursor);
            tableToken = task.getTableToken();
        } finally {
            // Don't hold the queue until the all the transactions applied to the table
            subSeq.done(cursor);
        }

        final long txn = applyWal(tableToken, engine, operationExecutor, runStatus);
        if (txn == WAL_APPLY_FAILED) {
            try {
                engine.getTableSequencerAPI().suspendTable(tableToken);
            } catch (CairoException e) {
                LOG.critical().$("could not suspend table [table=").$(tableToken.getTableName()).$(", error=").$(e.getFlyweightMessage()).I$();
            }
        }
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
}
