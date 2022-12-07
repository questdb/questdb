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

package io.questdb.cairo.wal;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.wal.seq.TableMetadataChangeLog;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cairo.wal.seq.TableTransactionLog;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.engine.ops.AbstractOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.std.IntLongHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import io.questdb.tasks.WalTxnNotificationTask;

import java.io.Closeable;

import static io.questdb.cairo.wal.WalTxnType.*;
import static io.questdb.cairo.wal.WalUtils.WAL_FORMAT_VERSION;
import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;
import static io.questdb.tasks.TableWriterTask.CMD_ALTER_TABLE;
import static io.questdb.tasks.TableWriterTask.CMD_UPDATE_TABLE;

public class ApplyWal2TableJob extends AbstractQueueConsumerJob<WalTxnNotificationTask> implements Closeable {
    private static final Log LOG = LogFactory.getLog(ApplyWal2TableJob.class);
    private static final String WAL_2_TABLE_WRITE_REASON = "WAL Data Application";
    private static final int WAL_APPLY_FAILED = -2;
    private final CairoEngine engine;
    private final IntLongHashMap lastAppliedSeqTxns = new IntLongHashMap();
    private final SqlToOperation sqlToOperation;
    private final WalEventReader walEventReader;

    public ApplyWal2TableJob(CairoEngine engine, int workerCount, int sharedWorkerCount) {
        super(engine.getMessageBus().getWalTxnNotificationQueue(), engine.getMessageBus().getWalTxnNotificationSubSequence());
        this.engine = engine;
        this.sqlToOperation = new SqlToOperation(engine, workerCount, sharedWorkerCount);
        walEventReader = new WalEventReader(engine.getConfiguration().getFilesFacade());
    }

    @Override
    public void close() {
        Misc.free(sqlToOperation);
        Misc.free(walEventReader);
    }

    public long processWalTxnNotification(
            CharSequence tableName,
            int tableId,
            CairoEngine engine,
            SqlToOperation sqlToOperation
    ) {
        long lastSeqTxn = -1;
        long lastAppliedSeqTxn = -1;
        do {
            // security context is checked on writing to the WAL and can be ignored here
            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, WAL_2_TABLE_WRITE_REASON)) {
                assert writer.getMetadata().getTableId() == tableId;
                applyOutstandingWalTransactions(writer, engine, sqlToOperation);
                lastAppliedSeqTxn = writer.getSeqTxn();
            } catch (EntryUnavailableException tableBusy) {
                if (!WAL_2_TABLE_WRITE_REASON.equals(tableBusy.getReason())) {
                    LOG.critical().$("unsolicited table lock [table=").$(tableName).$(", lock_reason=").$(tableBusy.getReason()).I$();
                    return WAL_APPLY_FAILED;
                }
                // This is good, someone else will apply the data
                break;
            } catch (CairoException ex) {
                LOG.critical().$("WAL apply job failed, table suspended [table=").$(tableName)
                        .$(", error=").$(ex.getFlyweightMessage())
                        .$(", errno=").$(ex.getErrno())
                        .I$();
                return WAL_APPLY_FAILED;
            }

            lastSeqTxn = engine.getTableSequencerAPI().lastTxn(tableName);
        } while (lastAppliedSeqTxn < lastSeqTxn);
        assert lastAppliedSeqTxn == lastSeqTxn;

        return lastAppliedSeqTxn;
    }

    @Override
    public boolean run(int workerId) {
        long cursor;
        boolean useful = false;

        while ((cursor = subSeq.next()) > -1 && doRun(workerId, cursor)) {
            useful = true;
        }
        return useful;
    }

    private void applyOutstandingWalTransactions(
            TableWriter writer,
            CairoEngine engine,
            SqlToOperation sqlToOperation
    ) {
        final TableSequencerAPI tableSequencerAPI = engine.getTableSequencerAPI();
        try (TransactionLogCursor transactionLogCursor = tableSequencerAPI.getCursor(writer.getTableName(), writer.getSeqTxn())) {
            final Path tempPath = Path.PATH.get();

            TableMetadataChangeLog structuralChangeCursor = null;
            try {
                while (transactionLogCursor.hasNext()) {
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

                    if (walId == 0) {
                        throw CairoException.critical(0)
                                .put("broken table transaction record in sequencer log, walId cannot be 0 [table=")
                                .put(writer.getTableName()).put(", seqTxn=").put(seqTxn).put(']');
                    }

                    if (walId != TableTransactionLog.STRUCTURAL_CHANGE_WAL_ID) {
                        // Always set full path when using thread static path
                        tempPath.of(engine.getConfiguration().getRoot()).concat(writer.getTableName()).slash().put(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                        // fix the clock for WAL SQL events so all nodes will be in sync when now() or systimestamp() used
                        sqlToOperation.setNowAndFixClock(commitTimestamp);
                        processWalCommit(writer, tempPath, segmentTxn, sqlToOperation, seqTxn);
                    } else {
                        // This is metadata change
                        // to be taken from TableSequencer directly
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
                            structuralChangeCursor = tableSequencerAPI.getMetadataChangeLogCursor(writer.getTableName(), newStructureVersion - 1);
                            hasNext = structuralChangeCursor.hasNext();
                        }

                        if (hasNext) {
                            structuralChangeCursor.next().apply(writer, true);
                            writer.setSeqTxn(seqTxn);
                        } else {
                            // Something messed up in sequencer.
                            // There is a transaction in WAL but no structure change record.
                            // TODO: make sequencer distressed and try to reconcile on sequencer opening
                            //  or skip the transaction?
                            throw CairoException.critical(0)
                                    .put("could not apply structure change from WAL to table. WAL metadata change does not exist [structureVersion=")
                                    .put(newStructureVersion)
                                    .put(']');
                        }
                    }
                }
            } finally {
                Misc.free(structuralChangeCursor);
            }
        }
    }

    private void processWalCommit(TableWriter writer, @Transient Path walPath, long segmentTxn, SqlToOperation sqlToOperation, long seqTxn) {
        try (WalEventReader eventReader = walEventReader) {
            final WalEventCursor walEventCursor = eventReader.of(walPath, WAL_FORMAT_VERSION, segmentTxn);
            final byte walTxnType = walEventCursor.getType();
            switch (walTxnType) {
                case DATA:
                    final WalEventCursor.DataInfo dataInfo = walEventCursor.getDataInfo();
                    writer.processWalData(
                            walPath,
                            !dataInfo.isOutOfOrder(),
                            dataInfo.getStartRowID(),
                            dataInfo.getEndRowID(),
                            dataInfo.getMinTimestamp(),
                            dataInfo.getMaxTimestamp(),
                            dataInfo,
                            seqTxn
                    );
                    break;
                case SQL:
                    final WalEventCursor.SqlInfo sqlInfo = walEventCursor.getSqlInfo();
                    processWalSql(writer, sqlInfo, sqlToOperation, seqTxn);
                    break;
                case TRUNCATE:
                    // TODO(puzpuzpuz): this implementation is broken because of ILP I/O threads' symbol cache
                    //                  and also concurrent table readers
                    writer.setSeqTxn(seqTxn);
                    writer.truncate();
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported WAL txn type: " + walTxnType);
            }
        }
    }

    private void processWalSql(TableWriter tableWriter, WalEventCursor.SqlInfo sqlInfo, SqlToOperation sqlToOperation, long seqTxn) {
        final int cmdType = sqlInfo.getCmdType();
        final CharSequence sql = sqlInfo.getSql();
        sqlToOperation.resetRnd(sqlInfo.getRndSeed0(), sqlInfo.getRndSeed1());
        sqlInfo.populateBindVariableService(sqlToOperation.getBindVariableService());
        AbstractOperation operation = null;
        try {
            switch (cmdType) {
                case CMD_ALTER_TABLE:
                    operation = sqlToOperation.toAlterOperation(sql);
                    break;
                case CMD_UPDATE_TABLE:
                    operation = sqlToOperation.toUpdateOperation(sql);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported command type: " + cmdType);
            }
            tableWriter.apply(operation, seqTxn);
        } catch (CairoException e) {
            if (e.isWALTolerable()) {
                // This is fine, some syntax error, we should block WAL processing if SQL is not valid
                LOG.error().$("error applying UPDATE SQL to wal table [table=")
                        .$(tableWriter.getTableName()).$(", sql=").$(sql).$(", error=").$(e.getFlyweightMessage()).I$();
            } else {
                throw e;
            }
        } finally {
            if (operation != null) {
                operation.close();
            }
        }
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        final CharSequence tableName;
        final int tableId;
        final long seqTxn;

        try {
            WalTxnNotificationTask walTxnNotificationTask = queue.get(cursor);
            tableId = walTxnNotificationTask.getTableId();
            tableName = walTxnNotificationTask.getTableName();
            seqTxn = walTxnNotificationTask.getTxn();
        } finally {
            // Don't hold the queue until the all the transactions applied to the table
            subSeq.done(cursor);
        }

        if (lastAppliedSeqTxns.get(tableId) < seqTxn) {
            // Check, maybe we already processed this table to higher txn.
            final long lastAppliedSeqTxn = processWalTxnNotification(tableName, tableId, engine, sqlToOperation);
            if (lastAppliedSeqTxn > -1L) {
                lastAppliedSeqTxns.put(tableId, lastAppliedSeqTxn);
            } else if (lastAppliedSeqTxn == WAL_APPLY_FAILED) {
                lastAppliedSeqTxns.put(tableId, Long.MAX_VALUE);
                engine.getTableSequencerAPI().suspendTable(tableName);
            }
        } else {
            LOG.debug().$("Skipping WAL processing for table, already processed [table=").$(tableName).$(", txn=").$(seqTxn).I$();
        }
        return true;
    }
}
