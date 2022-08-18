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
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.std.str.Path;
import io.questdb.tasks.WalTxnNotificationTask;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.WalWriter.WAL_NAME_BASE;

public class ApplyWal2TableJob extends AbstractQueueConsumerJob<WalTxnNotificationTask> {
    public final static String WAL_2_TABLE_WRITE_REASON = "WAL Data Application";
    private static final Log LOG = LogFactory.getLog(ApplyWal2TableJob.class);
    private final CairoEngine engine;
    private SequencerStructureChangeCursor reusableStructureChangeCursor;

    public ApplyWal2TableJob(CairoEngine engine) {
        super(engine.getMessageBus().getWalTxnNotificationQueue(), engine.getMessageBus().getWalTxnNotificationSubSequence());
        this.engine = engine;
    }

    public static SequencerStructureChangeCursor processWalTxnNotification(
            CharSequence tableName,
            int tableId,
            long txn,
            CairoEngine engine,
            @Nullable SequencerStructureChangeCursor reusableStructureChangeCursor
    ) {
        // This is work steeling, security context is checked on writing to the WAL
        // and can be ignored here
        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, WAL_2_TABLE_WRITE_REASON)) {
            assert writer.getMetadata().getId() == tableId;
            return applyOutstandingWalTransactions(writer, engine, reusableStructureChangeCursor);
        } catch (EntryUnavailableException tableBusy) {
            // This is all good, someone else will apply the data
            if (!WAL_2_TABLE_WRITE_REASON.equals(tableBusy.getReason())) {
                // Oh, no, rogue writer
                LOG.error().$("Rogue TableWriter. Table with WAL writing is out or writer pool [table=").$(tableName)
                        .$(", lock_reason=").$(tableBusy.getReason()).I$();
            }
        } catch (CairoException ex) {
            LOG.critical().$("Failed to apply WAL data to table [table=").$(tableName)
                    .$(", error=").$(ex.getMessage())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
        }
        return null;
    }


    private static SequencerStructureChangeCursor applyOutstandingWalTransactions(TableWriter writer, CairoEngine engine, @Nullable SequencerStructureChangeCursor reusableStructureChangeCursor) {
        TableRegistry tableRegistry = engine.getTableRegistry();
        long lastCommitted = writer.getTxn();

        try (SequencerCursor sequencerCursor = tableRegistry.getCursor(writer.getTableName(), lastCommitted)) {
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
                if (walid != TxnCatalog.METADATA_WALID) {
                    writer.processWalCommit(tempPath, segmentTxn);
                } else {
                    // This is metadata change
                    // to be taken from Sequencer directly
                    final int newStructureVersion = segmentId;
                    if (writer.getStructureVersion() != newStructureVersion - 1) {
                        throw CairoException.instance(0)
                                .put("Unexpected new WAL structure version [walStructure=").put(newStructureVersion)
                                .put(", tableStructureVersion=").put(writer.getStructureVersion())
                                .put(']');
                    }
                    reusableStructureChangeCursor = tableRegistry.getStructureChangeCursor(writer.getTableName(), reusableStructureChangeCursor, newStructureVersion - 1);
                    if (reusableStructureChangeCursor.hasNext()) {
                        try {
                            reusableStructureChangeCursor.next().apply(writer, true);
                        } catch (SqlException e) {
                            throw CairoException.instance(0)
                                    .put("cannot apply structure change from WAL to table. ").put(e.getFlyweightMessage());
                        }
                    } else {
                        throw CairoException.instance(0)
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

    @Override
    protected boolean doRun(int workerId, long cursor) {
        try {
            WalTxnNotificationTask walTxnNotificationTask = queue.get(cursor);
            int tableId = walTxnNotificationTask.getTableId();
            CharSequence tableName = walTxnNotificationTask.getTableName();
            long txn = walTxnNotificationTask.getTxn();
            reusableStructureChangeCursor = processWalTxnNotification(tableName, tableId, txn, engine, reusableStructureChangeCursor);
        } finally {
            subSeq.done(cursor);
        }
        return true;
    }
}
