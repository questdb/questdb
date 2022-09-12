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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlToOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.std.IntLongHashMap;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.tasks.WalTxnNotificationTask;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;

public class ApplyWal2TableJob extends AbstractQueueConsumerJob<WalTxnNotificationTask> implements Closeable {
    private static final String WAL_2_TABLE_WRITE_REASON = "WAL Data Application";
    private static final Log LOG = LogFactory.getLog(ApplyWal2TableJob.class);
    private static final ThreadLocal<SequencerStructureChangeCursor> reusableStructureChangeCursor = new ThreadLocal<>();
    public static final Closeable CLEANER = ApplyWal2TableJob::clearThreadLocals;
    private final CairoEngine engine;
    private final SqlToOperation sqlToOperation;
    private final IntLongHashMap localCommittedTransactions = new IntLongHashMap();

    public ApplyWal2TableJob(CairoEngine engine) {
        super(engine.getMessageBus().getWalTxnNotificationQueue(), engine.getMessageBus().getWalTxnNotificationSubSequence());
        this.engine = engine;
        this.sqlToOperation = new SqlToOperation(engine);
    }

    public static void clearThreadLocals() {
        Misc.free(reusableStructureChangeCursor);
    }

    public static long processWalTxnNotification(
            CharSequence tableName,
            int tableId,
            CairoEngine engine,
            SqlToOperation sqlToOperation
    ) {
        SequencerStructureChangeCursor cursor = reusableStructureChangeCursor.get();
        long lastTxn = -1;
        long lastCommittedTxn;
        do {
            // This is work steeling, security context is checked on writing to the WAL
            // and can be ignored here
            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, WAL_2_TABLE_WRITE_REASON)) {
                assert writer.getMetadata().getId() == tableId;
                cursor = applyOutstandingWalTransactions(writer, engine, sqlToOperation, cursor);
                lastCommittedTxn = writer.getTxn();
            } catch (EntryUnavailableException tableBusy) {
                // This is all good, someone else will apply the data
                if (!WAL_2_TABLE_WRITE_REASON.equals(tableBusy.getReason())) {
                    // Oh, no, rogue writer
                    LOG.critical().$("Rogue TableWriter. Table with WAL writing is out or writer pool [table=").$(tableName)
                            .$(", lock_reason=").$(tableBusy.getReason()).I$();
                }
                break;
            } catch (CairoException ex) {
                LOG.critical().$("Failed to apply WAL data to table [table=").$(tableName)
                        .$(", error=").$(ex.getMessage())
                        .$(", errno=").$(ex.getErrno())
                        .I$();
                break;
            }

            lastTxn = engine.getTableRegistry().lastTxn(tableName);
        } while (lastCommittedTxn < lastTxn);

        reusableStructureChangeCursor.set(cursor);
        return lastTxn;
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

    private static SequencerStructureChangeCursor applyOutstandingWalTransactions(
            TableWriter writer, CairoEngine engine,
            SqlToOperation sqlToOperation,
            @Nullable SequencerStructureChangeCursor reusableStructureChangeCursor
    ) {
        TableRegistry tableRegistry = engine.getTableRegistry();
        long lastCommitted = writer.getTxn();

        try (SequencerCursor sequencerCursor = tableRegistry.getCursor(writer.getTableName(), lastCommitted)) {
            Path tempPath = Path.PATH.get();
            tempPath.of(engine.getConfiguration().getRoot()).concat(writer.getTableName());
            int rootLen = tempPath.length();

            while (sequencerCursor.hasNext()) {
                int walId = sequencerCursor.getWalId();
                int segmentId = sequencerCursor.getSegmentId();
                long segmentTxn = sequencerCursor.getSegmentTxn();
                long nextTableTxn = sequencerCursor.getTxn();

                if (nextTableTxn != writer.getTxn() + 1) {
                    throw CairoException.critical(0).put("Unexpected WAL segment transaction ").put(nextTableTxn).put(" expected ").put((writer.getTxn() + 1));
                }
                tempPath.trimTo(rootLen).slash().put(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                if (walId != TxnCatalog.METADATA_WALID) {
                    writer.processWalCommit(tempPath, segmentTxn, sqlToOperation);
                } else {
                    // This is metadata change
                    // to be taken from Sequencer directly
                    // This may look odd, but on metadata change record, segment ID means structure version.
                    final int newStructureVersion = segmentId;
                    if (writer.getStructureVersion() != newStructureVersion - 1) {
                        throw CairoException.critical(0)
                                .put("Unexpected new WAL structure version [walStructure=").put(newStructureVersion)
                                .put(", tableStructureVersion=").put(writer.getStructureVersion())
                                .put(']');
                    }
                    boolean hasNext;
                    if (reusableStructureChangeCursor == null || !(hasNext = reusableStructureChangeCursor.hasNext())) {
                        reusableStructureChangeCursor = tableRegistry.getStructureChangeCursor(writer.getTableName(), reusableStructureChangeCursor, newStructureVersion - 1);
                        hasNext = reusableStructureChangeCursor.hasNext();
                    }

                    if (hasNext) {
                        try {
                            reusableStructureChangeCursor.next().apply(writer, true);
                        } catch (SqlException e) {
                            throw CairoException.critical(0)
                                    .put("cannot apply structure change from WAL to table. ").put(e.getFlyweightMessage());
                        }
                    } else {
                        throw CairoException.critical(0)
                                .put("cannot apply structure change from WAL to table. WAL metadata change does not exist [structureVersion=")
                                .put(newStructureVersion)
                                .put(']');
                    }
                }
            }
        } finally {
            if (reusableStructureChangeCursor != null) {
                reusableStructureChangeCursor.reset();
            }
        }
        return reusableStructureChangeCursor;
    }
}
