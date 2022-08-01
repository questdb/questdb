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

package io.questdb.cairo;

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.std.str.Path;
import io.questdb.tasks.WalTxnNotificationTask;

import static io.questdb.cairo.WalWriter.WAL_NAME_BASE;

public class ApplyWal2TableJob extends AbstractQueueConsumerJob<WalTxnNotificationTask> {
    public final static String WAL_2_TABLE_WRITE_REASON = "WAL Data Application";
    private static final Log LOG = LogFactory.getLog(ApplyWal2TableJob.class);
    private final CairoEngine engine;

    public ApplyWal2TableJob(CairoEngine engine) {
        super(engine.getMessageBus().getWalTxnNotificationQueue(), engine.getMessageBus().getWalTxnNotificationSubSequence());
        this.engine = engine;
    }

    public static void processWalTxnNotification(
            CharSequence tableName,
            int tableId,
            long txn,
            CairoEngine engine
    ) {
        // This is work steeling, security context is checked on writing to the WAL
        // and can be ignored here
        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, WAL_2_TABLE_WRITE_REASON)) {
            assert writer.getMetadata().getId() == tableId;
            applyOutstandingWalTransactions(writer, engine);
        } catch (EntryUnavailableException tableBusy) {
            // This is all good, someone else will apply the data
            if (!WAL_2_TABLE_WRITE_REASON.equals(tableBusy.getReason())) {
                // Oh, no, rogue writer
                LOG.error().$("Rogue TableWriter. Table with WAL writing is out or writer pool [table=").$(tableName)
                        .$(", lock_reason=").$(tableBusy.getReason()).I$();
            }
        }
    }

    private static void applyOutstandingWalTransactions(TableWriter writer, CairoEngine engine) {
        Sequencer sequencer = engine.getSequencer(writer.getTableName());
        long lastCommitted = writer.getTxn();

        try (SequencerCursor sequencerCursor = sequencer.getCursor(lastCommitted)) {
            Path tempPath = Path.PATH.get();
            tempPath.of(engine.getConfiguration().getRoot()).concat(writer.getTableName());
            int rootLen = tempPath.length();

            while (sequencerCursor.hasNext()) {
                int walid = sequencerCursor.getWalId();
                int segmentId = sequencerCursor.getSegmentId();
                long segmentTxn = sequencerCursor.getSegmentTxn();
                long nextTableTxn = sequencerCursor.getTxn();

                if (nextTableTxn != writer.getTxn() + 1) {
                    throw CairoException.instance(0).put("Unexpected WAL segment transaction ").put(nextTableTxn).put(" expected ").put((writer.getTxn() + 1));
                }
                tempPath.trimTo(rootLen).slash().put(WAL_NAME_BASE).put(walid).slash().put(segmentId);
                writer.processWalCommit(tempPath, segmentTxn);
            }
        }
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        try {
            WalTxnNotificationTask walTxnNotificationTask = queue.get(cursor);
            int tableId = walTxnNotificationTask.getTableId();
            CharSequence tableName = walTxnNotificationTask.getTableName();
            long txn = walTxnNotificationTask.getTxn();
            processWalTxnNotification(tableName, tableId, txn, engine);
        } finally {
            subSeq.done(cursor);
        }
        return true;
    }
}
