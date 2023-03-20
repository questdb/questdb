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
import io.questdb.cairo.*;
import io.questdb.cairo.wal.seq.TableMetadataChangeLog;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.Job;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.tasks.TelemetryTask;
import io.questdb.tasks.TelemetryWalTask;
import io.questdb.tasks.WalTxnNotificationTask;
import org.jetbrains.annotations.Nullable;

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
    private static final int FORCE_FULL_COMMIT = -2;
    private static final Log LOG = LogFactory.getLog(ApplyWal2TableJob.class);
    private static final int TXN_METADATA_LONGS_SIZE = 3;
    private static final String WAL_2_TABLE_WRITE_REASON = "WAL Data Application";
    private static final int WAL_APPLY_FAILED = -2;
    private static final int WAL_APPLY_IGNORE_ERROR = -1;
    private final long commitSquashRowLimit;
    private final CairoEngine engine;
    private final IntLongHashMap lastAppliedSeqTxns = new IntLongHashMap();
    private final int lookAheadTransactionCount;
    private final WalMetrics metrics;
    private final MicrosecondClock microClock;
    private final OperationCompiler operationCompiler;
    private final TableSequencerAPI tableSequencerAPI;
    private final Telemetry<TelemetryTask> telemetry;
    private final TelemetryFacade telemetryFacade;
    private final LongList transactionMeta = new LongList();
    private final WalEventReader walEventReader;
    private final Telemetry<TelemetryWalTask> walTelemetry;
    private final WalTelemetryFacade walTelemetryFacade;
    private long rowsSinceLastCommit;

    public ApplyWal2TableJob(CairoEngine engine, int workerCount, int sharedWorkerCount, @Nullable FunctionFactoryCache ffCache) {
        super(engine.getMessageBus().getWalTxnNotificationQueue(), engine.getMessageBus().getWalTxnNotificationSubSequence());
        this.engine = engine;
        tableSequencerAPI = engine.getTableSequencerAPI();
        walTelemetry = engine.getTelemetryWal();
        walTelemetryFacade = walTelemetry.isEnabled() ? this::doStoreWalTelemetry : this::storeWalTelemetryNoop;
        telemetry = engine.getTelemetry();
        telemetryFacade = telemetry.isEnabled() ? this::doStoreTelemetry : this::storeTelemetryNoop;
        operationCompiler = new OperationCompiler(engine, workerCount, sharedWorkerCount, ffCache);
        CairoConfiguration configuration = engine.getConfiguration();
        microClock = configuration.getMicrosecondClock();
        walEventReader = new WalEventReader(configuration.getFilesFacade());
        commitSquashRowLimit = configuration.getWalCommitSquashRowLimit();
        metrics = engine.getMetrics().getWalMetrics();
        lookAheadTransactionCount = configuration.getWalApplyLookAheadTransactionCount();
    }

    // returns transaction number, which is always > -1. Negative values are used as status code.
    public long applyWAL(
            TableToken tableToken,
            CairoEngine engine,
            OperationCompiler operationCompiler,
            Job.RunStatus runStatus
    ) {
        long lastSequencerTxn;
        long lastWriterTxn = WAL_APPLY_IGNORE_ERROR;
        Path tempPath = Path.PATH.get();

        try {
            do {
                // security context is checked on writing to the WAL and can be ignored here
                TableToken updatedToken = engine.getUpdatedTableToken(tableToken);
                if (engine.isTableDropped(tableToken) || updatedToken == null) {
                    if (engine.isTableDropped(tableToken)) {
                        return tryDestroyDroppedTable(tableToken, null, engine, tempPath) ? Long.MAX_VALUE : -1;
                    }
                    // else: table is dropped and fully cleaned, this is late notification.
                    return Long.MAX_VALUE;
                }

                rowsSinceLastCommit = 0;
                try (TableWriter writer = engine.getWriterUnsafe(updatedToken, WAL_2_TABLE_WRITE_REASON, false)) {
                    assert writer.getMetadata().getTableId() == tableToken.getTableId();
                    applyOutstandingWalTransactions(tableToken, writer, engine, operationCompiler, tempPath, runStatus);
                    lastWriterTxn = writer.getSeqTxn();
                } catch (EntryUnavailableException tableBusy) {
                    //noinspection StringEquality
                    if (tableBusy.getReason() != NO_LOCK_REASON
                            && !WAL_2_TABLE_WRITE_REASON.equals(tableBusy.getReason())
                            && !WAL_2_TABLE_RESUME_REASON.equals(tableBusy.getReason())) {
                        LOG.critical().$("unsolicited table lock [table=").utf8(tableToken.getDirName()).$(", lock_reason=").$(tableBusy.getReason()).I$();
                    }
                    // Don't suspend table. Perhaps writer will be unlocked with no transaction applied.
                    // We don't suspend table by virtue of having initial value on lastWriterTxn. It will either be
                    // "ignore" or last txn we applied.
                    break;
                }

                lastSequencerTxn = engine.getTableSequencerAPI().lastTxn(tableToken);
            } while (lastWriterTxn < lastSequencerTxn && !runStatus.isTerminating());
        } catch (CairoException ex) {
            if (ex.isTableDropped() || engine.isTableDropped(tableToken)) {
                // Table is dropped, and we received cairo exception in the middle of apply
                return tryDestroyDroppedTable(tableToken, null, engine, tempPath) ? Long.MAX_VALUE : WAL_APPLY_IGNORE_ERROR;
            }
            telemetryFacade.store(TelemetryOrigin.WAL_APPLY, WAL_APPLY_SUSPEND);
            LOG.critical().$("WAL apply job failed, table suspended [table=").utf8(tableToken.getDirName())
                    .$(", error=").$(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
            return WAL_APPLY_FAILED;
        }
        return lastWriterTxn;
    }

    @Override
    public void close() {
        Misc.free(operationCompiler);
        Misc.free(walEventReader);
    }

    private static boolean cleanDroppedTableDirectory(CairoEngine engine, Path tempPath, TableToken tableToken) {
        // Clean all the files inside table folder name except WAL directories and SEQ_DIR directory
        boolean allClean = true;
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        tempPath.of(engine.getConfiguration().getRoot()).concat(tableToken);
        int rootLen = tempPath.length();

        long p = ff.findFirst(tempPath.$());
        if (p > 0) {
            try {
                do {
                    long pUtf8NameZ = ff.findName(p);
                    int type = ff.findType(p);
                    if (ff.isDirOrSoftLinkDirNoDots(tempPath, rootLen, pUtf8NameZ, type)) {
                        if (!Chars.endsWith(tempPath, SEQ_DIR) && !Chars.equals(tempPath, rootLen + 1, rootLen + 1 + WAL_NAME_BASE.length(), WAL_NAME_BASE, 0, WAL_NAME_BASE.length())) {
                            if (ff.unlinkOrRemove(tempPath, LOG) != 0) {
                                allClean = false;
                            }
                        }

                    } else if (type == Files.DT_FILE) {
                        tempPath.trimTo(rootLen);
                        tempPath.concat(pUtf8NameZ);

                        if (Chars.endsWith(tempPath, TableUtils.TXN_FILE_NAME) || Chars.endsWith(tempPath, TableUtils.META_FILE_NAME) || matchesWalLock(tempPath)) {
                            continue;
                        }

                        if (!ff.remove(tempPath.$())) {
                            allClean = false;
                            LOG.info().$("could not remove [tempPath=").utf8(tempPath).$(", errno=").$(ff.errno()).I$();
                        }
                    }
                } while (ff.findNext(p) > 0);

                if (allClean) {
                    // Remove _txn and _meta files when all other files are removed
                    ff.remove(tempPath.trimTo(rootLen).concat(TableUtils.TXN_FILE_NAME).$());
                    ff.remove(tempPath.trimTo(rootLen).concat(TableUtils.META_FILE_NAME).$());
                    return true;
                }
            } finally {
                ff.findClose(p);
            }
        }
        return false;
    }

    private static AlterOperation compileAlter(TableWriter tableWriter, OperationCompiler compiler, CharSequence sql, long seqTxn) throws SqlException {
        try {
            return compiler.compileAlterSql(sql, tableWriter.getTableToken());
        } catch (SqlException ex) {
            tableWriter.markSeqTxnCommitted(seqTxn);
            throw ex;
        }
    }

    private static UpdateOperation compileUpdate(TableWriter tableWriter, OperationCompiler compiler, CharSequence sql, long seqTxn) throws SqlException {
        try {
            return compiler.compileUpdateSql(sql, tableWriter.getTableToken());
        } catch (SqlException ex) {
            tableWriter.markSeqTxnCommitted(seqTxn);
            throw ex;
        }
    }

    private static boolean matchesWalLock(CharSequence name) {
        if (Chars.endsWith(name, ".lock")) {
            for (int i = name.length() - ".lock".length() - 1; i > 0; i--) {
                char c = name.charAt(i);
                if (c < '0' || c > '9') {
                    return Chars.equals(name, i - WAL_NAME_BASE.length() + 1, i + 1, WAL_NAME_BASE, 0, WAL_NAME_BASE.length());
                }
            }
        }

        for (int i = 0, n = name.length(); i < n; i++) {
            char c = name.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }

    private static boolean tryDestroyDroppedTable(TableToken tableToken, TableWriter writer, CairoEngine engine, Path tempPath) {
        if (engine.lockReadersByTableToken(tableToken)) {
            TableWriter writerToClose = null;
            try {
                final CairoConfiguration configuration = engine.getConfiguration();
                if (writer == null && TableUtils.exists(configuration.getFilesFacade(), tempPath, configuration.getRoot(), tableToken.getDirName()) == TABLE_EXISTS) {
                    try {
                        writer = writerToClose = engine.getWriterUnsafe(tableToken, WAL_2_TABLE_WRITE_REASON, false);
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
                engine.releaseReadersByTableToken(tableToken);
            }
        } else {
            LOG.info().$("table '").utf8(tableToken.getDirName())
                    .$("' is dropped, waiting to acquire Table Readers lock to delete the table files").$();
        }
        return false;
    }

    private void applyOutstandingWalTransactions(
            TableToken tableToken,
            TableWriter writer,
            CairoEngine engine,
            OperationCompiler operationCompiler,
            Path tempPath,
            Job.RunStatus runStatus
    ) {
        boolean isTerminating;
        try (TransactionLogCursor transactionLogCursor = tableSequencerAPI.getCursor(tableToken, writer.getSeqTxn())) {
            TableMetadataChangeLog structuralChangeCursor = null;

            try {

                int iTransaction = 0;
                int totalTransactionCount = 0;
                long rowsAdded = 0;
                long physicalRowsAdded = 0;
                long insertTimespan = 0;

                tempPath.of(engine.getConfiguration().getRoot()).concat(tableToken).slash();
                int rootLen = tempPath.length();

                // Populate transactionMeta with timestamps of future transactions
                // to avoid O3 commits by pre-calculating safe to commit timestamp for every commit.
                LongList transactionMeta = readObservableTxnMeta(tempPath, transactionLogCursor, rootLen, writer.getMaxTimestamp());
                transactionLogCursor.toTop();

                isTerminating = runStatus.isTerminating();
                WHILE_TRANSACTION_CURSOR:
                while (transactionLogCursor.hasNext() && !isTerminating) {
                    final int walId = transactionLogCursor.getWalId();
                    final int segmentId = transactionLogCursor.getSegmentId();
                    final long segmentTxn = transactionLogCursor.getSegmentTxn();
                    final long commitTimestamp = transactionLogCursor.getCommitTimestamp();
                    final long seqTxn = transactionLogCursor.getTxn();

                    if (seqTxn != writer.getSeqTxn() + 1) {
                        throw CairoException.critical(0)
                                .put("unexpected sequencer transaction, expected ").put(writer.getSeqTxn() + 1)
                                .put(" but was ").put(seqTxn);
                    }

                    switch (walId) {
                        case METADATA_WALID:
                            // This is metadata change
                            // to be taken from Sequencer directly
                            final long newStructureVersion = transactionLogCursor.getStructureVersion();
                            if (writer.getStructureVersion() != newStructureVersion - 1) {
                                throw CairoException.critical(0)
                                        .put("unexpected new WAL structure version [walStructure=").put(newStructureVersion)
                                        .put(", tableStructureVersion=").put(writer.getStructureVersion())
                                        .put(']');
                            }

                            boolean hasNext;
                            if (structuralChangeCursor == null || !(hasNext = structuralChangeCursor.hasNext())) {
                                Misc.free(structuralChangeCursor);
                                structuralChangeCursor = tableSequencerAPI.getMetadataChangeLog(tableToken, newStructureVersion - 1);
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
                            tryDestroyDroppedTable(tableToken, writer, engine, tempPath);
                            return;

                        case 0:
                            throw CairoException.critical(0)
                                    .put("broken table transaction record in sequencer log, walId cannot be 0 [table=")
                                    .put(tableToken.getTableName()).put(", seqTxn=").put(seqTxn).put(']');

                        default:
                            // Always set full path when using thread static path
                            operationCompiler.setNowAndFixClock(commitTimestamp);
                            tempPath.of(engine.getConfiguration().getRoot()).concat(tableToken).slash().put(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                            final long start = microClock.getTicks();

                            if (iTransaction > 0 && transactionMeta.size() < (iTransaction + lookAheadTransactionCount) * TXN_METADATA_LONGS_SIZE) {
                                // Last few transactions left to process from the list
                                // of observed transactions built upfront in the beginning of the loop.
                                // Check if more transaction exist, exit restart the loop to have better picture
                                // of the future transactions and optimise the application.
                                if (transactionLogCursor.reset()) {
                                    transactionMeta = readObservableTxnMeta(tempPath, transactionLogCursor, rootLen, writer.getMaxTimestamp());
                                    transactionLogCursor.toTop();
                                    totalTransactionCount += iTransaction;
                                    iTransaction = 0;
                                    continue;
                                }
                            }

                            isTerminating = runStatus.isTerminating();
                            final long added = processWalCommit(
                                    writer,
                                    walId,
                                    tempPath,
                                    segmentTxn,
                                    operationCompiler,
                                    seqTxn,
                                    commitTimestamp,
                                    isTerminating,
                                    transactionMeta,
                                    iTransaction * TXN_METADATA_LONGS_SIZE
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
                if (totalTransactionCount > 0) {
                    LOG.info().$("WAL apply job finished [table=").$(writer.getTableToken())
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
            OperationCompiler operationCompiler,
            long seqTxn,
            long commitTimestamp,
            boolean isTerminating,
            LongList minTimestamps,
            int minTimestampsIndex
    ) {
        try (WalEventReader eventReader = walEventReader) {
            final WalEventCursor walEventCursor = eventReader.of(walPath, WAL_FORMAT_VERSION, segmentTxn);
            final byte walTxnType = walEventCursor.getType();
            switch (walTxnType) {
                case DATA:
                    final WalEventCursor.DataInfo dataInfo = walEventCursor.getDataInfo();
                    if (minTimestampsIndex < minTimestamps.size()) {
                        long commitToTimestamp = minTimestamps.getQuick(minTimestampsIndex);
                        long rowCount = dataInfo.getEndRowID() - dataInfo.getStartRowID();
                        if (commitToTimestamp < 0 || isTerminating) {
                            // commit everything, do not store data in memory LAG buffer
                            commitToTimestamp = Long.MAX_VALUE;
                            rowsSinceLastCommit = 0;
                        } else {
                            rowsSinceLastCommit += rowCount;
                            if (rowsSinceLastCommit < commitSquashRowLimit) {
                                // This is an optimisation to apply small commits.
                                // We want to store data in memory LAG buffer and commit it later when the buffer size is reasonably big
                                // or when there is no more data available in WAL.
                                // Do not commit yet, copy to LAG memory buffer and wait for more rows
                                commitToTimestamp = -1;
                            }
                        }
                        final long start = microClock.getTicks();
                        walTelemetryFacade.store(WAL_TXN_APPLY_START, writer.getTableToken(), walId, seqTxn, -1L, -1L, start - commitTimestamp);
                        final long rowsAdded = writer.processWalData(
                                walPath,
                                !dataInfo.isOutOfOrder(),
                                dataInfo.getStartRowID(),
                                dataInfo.getEndRowID(),
                                dataInfo.getMinTimestamp(),
                                dataInfo.getMaxTimestamp(),
                                dataInfo,
                                seqTxn,
                                commitToTimestamp
                        );
                        rowsSinceLastCommit -= rowsAdded;
                        final long latency = microClock.getTicks() - start;
                        long physicalRowCount = writer.getPhysicallyWrittenRowsSinceLastCommit();
                        metrics.addApplyRowsWritten(rowCount, physicalRowCount, latency);
                        walTelemetryFacade.store(WAL_TXN_DATA_APPLIED, writer.getTableToken(), walId, seqTxn, rowsAdded, physicalRowCount, latency);
                        return rowCount;
                    } else {
                        return -2L;
                    }

                case SQL:
                    final WalEventCursor.SqlInfo sqlInfo = walEventCursor.getSqlInfo();
                    final long start = microClock.getTicks();
                    walTelemetryFacade.store(WAL_TXN_APPLY_START, writer.getTableToken(), walId, seqTxn, -1L, -1L, start - commitTimestamp);
                    processWalSql(writer, sqlInfo, operationCompiler, seqTxn);
                    walTelemetryFacade.store(WAL_TXN_SQL_APPLIED, writer.getTableToken(), walId, seqTxn, -1L, -1L, microClock.getTicks() - start);
                    return -1L;
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

    private void processWalSql(TableWriter tableWriter, WalEventCursor.SqlInfo sqlInfo, OperationCompiler operationCompiler, long seqTxn) {
        final int cmdType = sqlInfo.getCmdType();
        final CharSequence sql = sqlInfo.getSql();
        operationCompiler.resetRnd(sqlInfo.getRndSeed0(), sqlInfo.getRndSeed1());
        sqlInfo.populateBindVariableService(operationCompiler.getBindVariableService());
        try {
            switch (cmdType) {
                case CMD_ALTER_TABLE:
                    AlterOperation alterOperation = compileAlter(tableWriter, operationCompiler, sql, seqTxn);
                    try {
                        tableWriter.apply(alterOperation, seqTxn);
                    } finally {
                        Misc.free(alterOperation);
                    }
                    break;
                case CMD_UPDATE_TABLE:
                    UpdateOperation updateOperation = compileUpdate(tableWriter, operationCompiler, sql, seqTxn);
                    try {
                        tableWriter.apply(updateOperation, seqTxn);
                    } finally {
                        Misc.free(updateOperation);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported command type: " + cmdType);
            }
        } catch (SqlException ex) {
            // This is fine, some syntax error, we should not block WAL processing if SQL is not valid
            LOG.error().$("error applying SQL to wal table [table=")
                    .utf8(tableWriter.getTableToken().getTableName()).$(", sql=").$(sql).$(", error=").$(ex.getFlyweightMessage()).I$();
        } catch (CairoException e) {
            if (e.isWALTolerable()) {
                // This is fine, some syntax error, we should not block WAL processing if SQL is not valid
                LOG.error().$("error applying SQL to wal table [table=")
                        .utf8(tableWriter.getTableToken().getTableName()).$(", sql=").$(sql).$(", error=").$(e.getFlyweightMessage()).I$();
            } else {
                throw e;
            }
        }
    }

    private LongList readObservableTxnMeta(Path tempPath, TransactionLogCursor transactionLogCursor, int rootLen, long maxCommittedTimestamp) {
        try (WalEventReader eventReader = walEventReader) {
            transactionMeta.clear();
            int prevWalId = Integer.MIN_VALUE;
            int prevSegmentId = Integer.MIN_VALUE;
            int prevSegmentTxn = Integer.MIN_VALUE;
            WalEventCursor walEventCursor = null;

            while (transactionLogCursor.hasNext()) {

                final int walId = transactionLogCursor.getWalId();
                final int segmentId = transactionLogCursor.getSegmentId();
                final int segmentTxn = transactionLogCursor.getSegmentTxn();

                boolean recordAdded = false;
                if (walId > 0) {
                    tempPath.trimTo(rootLen).put(WAL_NAME_BASE).put(walId).slash().put(segmentId);

                    if (prevWalId != walId || prevSegmentId != segmentId || prevSegmentTxn + 1 != segmentTxn) {
                        walEventCursor = eventReader.of(tempPath, WAL_FORMAT_VERSION, segmentTxn);
                    } else {
                        // This is same WALE file, just read next txn transaction.
                        if (!walEventCursor.hasNext()) {
                            walEventCursor = eventReader.of(tempPath, WAL_FORMAT_VERSION, segmentTxn);
                        }
                    }

                    final byte walTxnType = walEventCursor.getType();
                    if (walTxnType == DATA) {
                        recordAdded = true;
                        WalEventCursor.DataInfo commitInfo = walEventCursor.getDataInfo();
                        transactionMeta.add(-1); // commit to timestamp
                        transactionMeta.add(commitInfo.getMaxTimestamp());
                        transactionMeta.add(commitInfo.getMinTimestamp());
                    }
                }
                prevWalId = walId;
                prevSegmentId = segmentId;
                prevSegmentTxn = segmentTxn;

                // This is a structural change, UPDATE or non-structural ALTER
                // when it happens in between the transactions, we want to commit everything before it.
                if (!recordAdded && transactionMeta.size() > 0) {
                    transactionMeta.setQuick(transactionMeta.size() - 3, FORCE_FULL_COMMIT); // commit to timestamp of prev record
                }
            }
        }

        // find min timestamp after every transaction
        long runningMinTimestamp = Long.MAX_VALUE;
        long maxLag = 0;
        for (int n = transactionMeta.size(), i = n - 1; i > -1; i -= TXN_METADATA_LONGS_SIZE) {

            long currentMinTimestamp = transactionMeta.getQuick(i);
            long currentMaxTimestamp = transactionMeta.getQuick(i - 1);

            long nextMinTimestamp = Math.min(currentMaxTimestamp, runningMinTimestamp);
            long lag = currentMaxTimestamp - nextMinTimestamp;
            if (lag > maxLag) {
                maxLag = lag;
            }

            runningMinTimestamp = Math.min(runningMinTimestamp, currentMinTimestamp);
            // No point to hold data in lag buffer if it's already intersects with committed data
            transactionMeta.setQuick(i, Math.max(maxCommittedTimestamp, runningMinTimestamp));

            // Leave last commitToTimestamp as -1 so everything is committed at the end
            if (i < n - 1) {
                long commitToTimestamp = transactionMeta.getQuick(i - 2);
                if (commitToTimestamp != FORCE_FULL_COMMIT) {
                    // set commitToTimestamp to be nextMinTimestamp
                    // so that O3 does not happen
                    transactionMeta.setQuick(i - 2, nextMinTimestamp);
                } else {
                    // This is a flag that the commit has to be done in full
                    // because of following UPDATE or ALTER.
                    // Everything will be committed at this point, so it's safe to reset runningMinTimestamp
                    runningMinTimestamp = Long.MAX_VALUE;
                }
            }
        }
        return transactionMeta;
    }

    private void storeTelemetryNoop(short event, short origin) {
    }

    private void storeWalTelemetryNoop(short event, TableToken tableToken, int walId, long seqTxn, long rowCount, long physicalRowCount, long latencyUs) {
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        final TableToken tableToken;
        final long seqTxn;

        try {
            WalTxnNotificationTask task = queue.get(cursor);
            tableToken = task.getTableToken();
            seqTxn = task.getTxn();
        } finally {
            // Don't hold the queue until the all the transactions applied to the table
            subSeq.done(cursor);
        }

        final int tableId = tableToken.getTableId();
        if (lastAppliedSeqTxns.get(tableId) < seqTxn) {
            // Check, maybe we already processed this table to higher txn.
            final long txn = applyWAL(tableToken, engine, operationCompiler, runStatus);
            if (txn > -1L) {
                lastAppliedSeqTxns.put(tableId, txn);
            } else if (txn == WAL_APPLY_FAILED) {
                // Set processed transaction marker as Long.MAX_VALUE - 1
                // so that when the table is unsuspended it's notified with transaction Long.MAX_VALUE
                // and got picked up for processing in this apply job.
                lastAppliedSeqTxns.put(tableId, Long.MAX_VALUE - 1);
                try {
                    engine.getTableSequencerAPI().suspendTable(tableToken);
                } catch (CairoException e) {
                    LOG.critical().$("could not suspend table [table=").$(tableToken.getTableName()).$(", error=").$(e.getFlyweightMessage()).I$();
                }
            }
        } else {
            LOG.debug().$("Skipping WAL processing for table, already processed [table=").$(tableToken).$(", txn=").$(seqTxn).I$();
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
