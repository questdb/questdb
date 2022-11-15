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
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.tasks.WalTxnNotificationTask;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.TABLE_EXISTS;
import static io.questdb.cairo.wal.WalTxnType.*;
import static io.questdb.cairo.wal.WalUtils.*;
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
            String systemTableName,
            int tableId,
            CairoEngine engine,
            SqlToOperation sqlToOperation
    ) {
        long lastSeqTxn = -1;
        long lastAppliedSeqTxn = -1;
        Path tempPath = Path.PATH.get();

        try {
            do {
                // security context is checked on writing to the WAL and can be ignored here
                if (engine.getTableSequencerAPI().isWalTableDropped(systemTableName)) {
                    // table was dropped, clean up the table directory.
                    tryDestroyDroppedTable(systemTableName, null, engine, tempPath);
                    return Long.MAX_VALUE;
                }

                if (!engine.getTableSequencerAPI().isWalSystemName(systemTableName)) {
                    LOG.info().$("table '").utf8(systemTableName).$("' does not exist, skipping WAL application").$();
                    return 0;
                }

                try (TableWriter writer = engine.getWriterBySystemName(AllowAllCairoSecurityContext.INSTANCE, systemTableName, WAL_2_TABLE_WRITE_REASON)) {
                    assert writer.getMetadata().getTableId() == tableId;
                    applyOutstandingWalTransactions(systemTableName, writer, engine, sqlToOperation, tempPath);
                    lastAppliedSeqTxn = writer.getSeqTxn();
                } catch (EntryUnavailableException tableBusy) {
                    if (!WAL_2_TABLE_WRITE_REASON.equals(tableBusy.getReason())) {
                        LOG.critical().$("unsolicited table lock [table=").utf8(systemTableName).$(", lock_reason=").$(tableBusy.getReason()).I$();
                        // Don't suspend table. Perhaps writer will be unlocked with no transaction applied.
                    }
                    // This is good, someone else will apply the data
                    break;
                }

                lastSeqTxn = engine.getTableSequencerAPI().lastTxn(systemTableName);
            } while (lastAppliedSeqTxn < lastSeqTxn);
        } catch (CairoException ex) {
            LOG.critical().$("WAL apply job failed, table suspended [table=").utf8(systemTableName)
                    .$(", error=").$(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
            return WAL_APPLY_FAILED;
        }
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

    private static boolean cleanDroppedTableDirectory(CairoEngine engine, Path tempPath, CharSequence systemTableName) {
        // Clean all the files inside table folder name except WAL directories and SEQ_DIR directory
        boolean allClean = true;
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        tempPath.of(engine.getConfiguration().getRoot()).concat(systemTableName);
        int rootLen = tempPath.length();

        long p = ff.findFirst(tempPath.$());
        if (p > 0) {
            try {
                do {
                    long pUtf8NameZ = ff.findName(p);
                    int type = ff.findType(p);
                    if (Files.isDir(pUtf8NameZ, type)) {
                        tempPath.trimTo(rootLen);
                        tempPath.concat(pUtf8NameZ).$();

                        if (!Chars.endsWith(tempPath, SEQ_DIR) && !Chars.equals(tempPath, rootLen + 1, rootLen + 1 + WAL_NAME_BASE.length(), WAL_NAME_BASE, 0, WAL_NAME_BASE.length())) {
                            if (ff.rmdir(tempPath) != 0) {
                                allClean = false;
                                LOG.info().$("could not remove [tempPath=").$(tempPath).$(", errno=").$(ff.errno()).I$();
                            }
                        }

                    } else if (type == Files.DT_FILE) {
                        tempPath.trimTo(rootLen);
                        tempPath.concat(pUtf8NameZ).$();

                        if (Chars.endsWith(tempPath, TableUtils.TXN_FILE_NAME) || Chars.endsWith(tempPath, TableUtils.META_FILE_NAME)) {
                            continue;
                        }

                        if (!ff.remove(tempPath)) {
                            allClean = false;
                            LOG.info().$("could not remove [tempPath=").$(tempPath).$(", errno=").$(ff.errno()).I$();
                        }
                    }
                } while (ff.findNext(p) > 0);

                if (allClean) {
                    // Remove _txn and _meta files when all other files are removed
                    allClean = ff.remove(tempPath.trimTo(rootLen).concat(TableUtils.TXN_FILE_NAME).$());
                    allClean &= ff.remove(tempPath.trimTo(rootLen).concat(TableUtils.META_FILE_NAME).$());
                }
            } finally {
                ff.findClose(p);
            }
        }

        return allClean;
    }

    private static AlterOperation compileAlter(TableWriter tableWriter, SqlToOperation sqlToOperation, CharSequence sql, long seqTxn) throws SqlException {
        try {
            return sqlToOperation.toAlterOperation(sql, tableWriter.getSystemTableName());
        } catch (SqlException ex) {
            tableWriter.markSeqTxnCommitted(seqTxn);
            throw ex;
        }
    }

    private static UpdateOperation compileUpdate(TableWriter tableWriter, SqlToOperation sqlToOperation, CharSequence sql, long seqTxn) throws SqlException {
        try {
            return sqlToOperation.toUpdateOperation(sql, tableWriter.getSystemTableName());
        } catch (SqlException ex) {
            tableWriter.markSeqTxnCommitted(seqTxn);
            throw ex;
        }
    }

    private static void tryDestroyDroppedTable(String systemTableName, TableWriter writer, CairoEngine engine, Path tempPath) {
        if (engine.lockReadersBySystemName(systemTableName)) {
            TableWriter writerToClose = null;
            try {
                final CairoConfiguration configuration = engine.getConfiguration();
                if (writer == null && TableUtils.exists(configuration.getFilesFacade(), tempPath, configuration.getRoot(), systemTableName) == TABLE_EXISTS) {
                    try {
                        writer = writerToClose = engine.getWriterBySystemName(AllowAllCairoSecurityContext.INSTANCE, systemTableName, WAL_2_TABLE_WRITE_REASON);
                    } catch (CairoException ex) {
                        // Ignore it, table can be half deleted.
                    }
                }
                if (writer != null) {
                    // Force writer to close all the files.
                    writer.destroy();
                }
                if (!cleanDroppedTableDirectory(engine, tempPath, systemTableName)) {
                    engine.notifyWalTxnRepublisher();
                }
            } finally {
                if (writerToClose != null) {
                    writerToClose.close();
                }
                engine.releaseReadersBySystemName(systemTableName);
            }
        } else {
            LOG.info().$("table '").utf8(systemTableName)
                    .$("' is dropped, waiting to acquire Table Readers lock to delete the table files").$();
            engine.notifyWalTxnRepublisher();
        }
    }

    private void applyOutstandingWalTransactions(
            String systemTableName,
            TableWriter writer,
            CairoEngine engine,
            SqlToOperation sqlToOperation,
            Path tempPath
    ) {
        final TableSequencerAPI tableSequencerAPI = engine.getTableSequencerAPI();

        try (TransactionLogCursor transactionLogCursor = tableSequencerAPI.getCursor(systemTableName, writer.getSeqTxn())) {
            TableMetadataChangeLog structuralChangeCursor = null;
            try {
                while (transactionLogCursor.hasNext()) {
                    final int walId = transactionLogCursor.getWalId();
                    final int segmentId = transactionLogCursor.getSegmentId();
                    final long segmentTxn = transactionLogCursor.getSegmentTxn();
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
                            // This may look odd, but on metadata change record, segment ID means structure version.
                            @SuppressWarnings("UnnecessaryLocalVariable") final int newStructureVersion = segmentId;
                            if (writer.getStructureVersion() != newStructureVersion - 1) {
                                throw CairoException.critical(0)
                                        .put("unexpected new WAL structure version [walStructure=").put(newStructureVersion)
                                        .put(", tableStructureVersion=").put(writer.getStructureVersion())
                                        .put(']');
                            }

                            boolean hasNext;
                            if (structuralChangeCursor == null || !(hasNext = structuralChangeCursor.hasNext())) {
                                Misc.free(structuralChangeCursor);
                                structuralChangeCursor = tableSequencerAPI.getMetadataChangeLogCursor(writer.getSystemTableName(), newStructureVersion - 1);
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
                            break;

                        case DROP_TABLE_WALID:
                            tryDestroyDroppedTable(systemTableName, writer, engine, tempPath);
                            return;

                        case RENAME_TABLE_WALID:
                            writer.changeTableName(seqTxn, transactionLogCursor.getTableName());
                            break;

                        case 0:
                            throw CairoException.critical(0)
                                    .put("broken table transaction record in sequencer log, walId cannot be 0 [table=")
                                    .put(writer.getTableName()).put(", seqTxn=").put(seqTxn).put(']');

                        default:
                            // Always set full path when using thread static path
                            tempPath.of(engine.getConfiguration().getRoot()).concat(systemTableName).slash().put(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                            processWalCommit(writer, tempPath, segmentTxn, sqlToOperation, seqTxn);
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
        sqlInfo.populateBindVariableService(sqlToOperation.getBindVariableService());
        try {
            switch (cmdType) {
                case CMD_ALTER_TABLE:
                    AlterOperation alterOperation = compileAlter(tableWriter, sqlToOperation, sql, seqTxn);
                    try {
                        tableWriter.apply(alterOperation, seqTxn);
                    } finally {
                        Misc.free(alterOperation);
                    }
                    break;
                case CMD_UPDATE_TABLE:
                    UpdateOperation updateOperation = compileUpdate(tableWriter, sqlToOperation, sql, seqTxn);
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
                    .$(tableWriter.getTableName()).$(", sql=").$(sql).$(", error=").$(ex.getFlyweightMessage()).I$();
        } catch (CairoException e) {
            if (e.isWALTolerable()) {
                // This is fine, some syntax error, we should not block WAL processing if SQL is not valid
                LOG.error().$("error applying SQL to wal table [table=")
                        .$(tableWriter.getTableName()).$(", sql=").$(sql).$(", error=").$(e.getFlyweightMessage()).I$();
            } else {
                throw e;
            }
        }
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        final String tableName;
        final int tableId;
        final long seqTxn;

        try {
            WalTxnNotificationTask walTxnNotificationTask = queue.get(cursor);
            tableId = walTxnNotificationTask.getTableId();
            tableName = walTxnNotificationTask.getSystemTableName();
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
