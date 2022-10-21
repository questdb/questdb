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
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cairo.wal.seq.TableTransactionLog;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.cairo.wal.seq.TableMetadataChangeLog;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.std.IntLongHashMap;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.tasks.WalTxnNotificationTask;

import java.io.Closeable;

import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;

public class ApplyWal2TableJob extends AbstractQueueConsumerJob<WalTxnNotificationTask> implements Closeable {
    private static final String WAL_2_TABLE_WRITE_REASON = "WAL Data Application";
    private static final Log LOG = LogFactory.getLog(ApplyWal2TableJob.class);
    private final CairoEngine engine;
    private final SqlToOperation sqlToOperation;
    private final IntLongHashMap localCommittedTransactions = new IntLongHashMap();

    public ApplyWal2TableJob(CairoEngine engine) {
        super(engine.getMessageBus().getWalTxnNotificationQueue(), engine.getMessageBus().getWalTxnNotificationSubSequence());
        this.engine = engine;
        this.sqlToOperation = new SqlToOperation(engine);
    }

    public static long processWalTxnNotification(
            CharSequence tableName,
            int tableId,
            CairoEngine engine,
            SqlToOperation sqlToOperation
    ) {
        long lastSeqTxn = -1;
        long lastCommittedSeqTxn = -1;
        do {
            // This is work steeling, security context is checked on writing to the WAL
            // and can be ignored here
            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, WAL_2_TABLE_WRITE_REASON)) {
                assert writer.getMetadata().getId() == tableId;
                applyOutstandingWalTransactions(writer, engine, sqlToOperation);
                lastCommittedSeqTxn = writer.getSeqTxn();
            } catch (EntryUnavailableException tableBusy) {
                // This is all good, someone else will apply the data
                if (!WAL_2_TABLE_WRITE_REASON.equals(tableBusy.getReason())) {
                    // Oh, no, rogue writer
                    LOG.critical().$("Rogue TableWriter. Table with WAL writing is out or writer pool [table=").$(tableName)
                            .$(", lock_reason=").$(tableBusy.getReason()).I$();
                }
                break;
            } catch (CairoException ex) {
                engine.notifyWalTxnFailed();
                LOG.critical().$("failed to apply WAL data to table [table=").$(tableName)
                        .$(", error=").$(ex.getMessage())
                        .$(", errno=").$(ex.getErrno())
                        .I$();
                break;
            }

            lastSeqTxn = engine.getTableRegistry().lastTxn(tableName);
        } while (lastCommittedSeqTxn < lastSeqTxn);
        assert lastCommittedSeqTxn == lastSeqTxn;

        return lastCommittedSeqTxn;
    }

    @Override
    public void close() {
        sqlToOperation.close();
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

    @Override
    protected boolean doRun(int workerId, long cursor) {
        CharSequence tableName;
        int tableId;
        long txn;

        try {
            WalTxnNotificationTask walTxnNotificationTask = queue.get(cursor);
            tableId = walTxnNotificationTask.getTableId();
            tableName = walTxnNotificationTask.getTableName();
            txn = walTxnNotificationTask.getTxn();
        } finally {
            // Don't hold the queue until the all the transactions applied to the table
            subSeq.done(cursor);
        }

        if (localCommittedTransactions.get(tableId) < txn) {
            // Check, maybe we already processed this table to higher txn.
            long committedTxn = processWalTxnNotification(tableName, tableId, engine, sqlToOperation);
            if (committedTxn > -1) {
                localCommittedTransactions.put(tableId, committedTxn);
            }
        } else {
            LOG.debug().$("Skipping WAL processing for table, already processed [table=").$(tableName).$(", txn=").$(txn).I$();
        }
        return true;
    }

    private static void applyOutstandingWalTransactions(
            TableWriter writer,
            CairoEngine engine,
            SqlToOperation sqlToOperation
    ) {
        final TableSequencerAPI tableSequencerAPI = engine.getTableRegistry();
        try (TransactionLogCursor transactionLogCursor = tableSequencerAPI.getCursor(writer.getTableName(), writer.getSeqTxn())) {
            final Path tempPath = Path.PATH.get();

            TableMetadataChangeLog structuralChangeCursor = null;
            try {
                while (transactionLogCursor.hasNext()) {
                    final int walId = transactionLogCursor.getWalId();
                    final int segmentId = transactionLogCursor.getSegmentId();
                    final long segmentTxn = transactionLogCursor.getSegmentTxn();
                    final long seqTxn = transactionLogCursor.getTxn();

                    if (seqTxn != writer.getSeqTxn() + 1) {
                        throw CairoException.critical(0).put("Unexpected sequencer transaction ").put(seqTxn).put(" expected ").put((writer.getSeqTxn() + 1));
                    }
                    writer.setSeqTxn(seqTxn);

                    if (walId != TableTransactionLog.STRUCTURAL_CHANGE_WAL_ID) {
                        // Always set full path when using thread static path
                        tempPath.of(engine.getConfiguration().getRoot()).concat(writer.getTableName()).slash().put(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                        writer.processWalCommit(tempPath, segmentTxn, sqlToOperation);
                    } else {
                        // This is metadata change
                        // to be taken from TableSequencer directly
                        // This may look odd, but on metadata change record, segment ID means structure version.
                        @SuppressWarnings("UnnecessaryLocalVariable") final int newStructureVersion = segmentId;
                        if (writer.getStructureVersion() != newStructureVersion - 1) {
                            throw CairoException.critical(0)
                                    .put("Unexpected new WAL structure version [walStructure=").put(newStructureVersion)
                                    .put(", tableStructureVersion=").put(writer.getStructureVersion())
                                    .put(']');
                        }

                        boolean hasNext;
                        if (structuralChangeCursor == null || !(hasNext = structuralChangeCursor.hasNext())) {
                            Misc.free(structuralChangeCursor);
                            structuralChangeCursor = tableSequencerAPI.getMetadataChangeLogCursor(writer.getTableName(), newStructureVersion - 1);
                            hasNext = structuralChangeCursor != null && structuralChangeCursor.hasNext();
                        }

                        if (hasNext) {
                            try {
                                structuralChangeCursor.next().apply(writer, true);
                            } catch (SqlException e) {
                                throw CairoException.critical(0)
                                        .put("cannot apply structure change from WAL to table [error=")
                                        .put(e.getFlyweightMessage()).put(']');
                            }
                        } else {
                            // Something messed up in sequencer.
                            // There is a transaction in WAL but no structure change record.
                            // TODO: make sequencer distressed and try to reconcile on sequencer opening
                            //  or skip the transaction?
                            throw CairoException.critical(0)
                                    .put("cannot apply structure change from WAL to table. WAL metadata change does not exist [structureVersion=")
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
}
