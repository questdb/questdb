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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.seq.MetadataServiceStub;
import io.questdb.cairo.wal.seq.TableMetadataChange;
import io.questdb.cairo.wal.seq.TableMetadataChangeLog;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjHashSet;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

public class CheckWalTransactionsJob extends SynchronizedJob {
    private static final Log LOG = LogFactory.getLog(CheckWalTransactionsJob.class);
    private final TableSequencerAPI.TableSequencerCallback checkNotifyOutstandingTxnInWalRef;
    private final CharSequence dbRoot;
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final MillisecondClock millisecondClock;
    private final long spinLockTimeout;
    private final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
    // Empty list means that all tables should be checked.
    private final ObjHashSet<TableToken> tablesToCheck = new ObjHashSet<>();
    private final TxReader txReader;
    private long lastProcessedCount = 0;
    private final RenameTrackingMetadataService renameTrackingMetadataService = new RenameTrackingMetadataService();
    private Path threadLocalPath;

    public CheckWalTransactionsJob(CairoEngine engine) {
        this.engine = engine;
        this.ff = engine.getConfiguration().getFilesFacade();
        txReader = new TxReader(engine.getConfiguration().getFilesFacade());
        dbRoot = engine.getConfiguration().getRoot();
        millisecondClock = engine.getConfiguration().getMillisecondClock();
        spinLockTimeout = engine.getConfiguration().getSpinLockTimeout();
        checkNotifyOutstandingTxnInWalRef = (tableToken, txn, txn2) -> checkNotifyOutstandingTxnInWal(txn, txn2);
    }

    public void addTableToCheck(TableToken tableToken) {
        tablesToCheck.add(tableToken);
    }

    public void checkMissingWalTransactions() {
        threadLocalPath = Path.PATH.get().of(dbRoot);
        engine.getTableSequencerAPI().forAllWalTables(tableTokenBucket, true, checkNotifyOutstandingTxnInWalRef);
    }

    public void checkNotifyOutstandingTxnInWal(@NotNull TableToken tableToken, long seqTxn) {
        if (!tablesToCheck.isEmpty() && !tablesToCheck.contains(tableToken)) {
            return;
        }
        if (
                seqTxn < 0 && TableUtils.exists(
                        ff,
                        threadLocalPath,
                        dbRoot,
                        tableToken.getDirName()
                ) == TableUtils.TABLE_EXISTS
        ) {
            // Dropped table
            engine.notifyWalTxnCommitted(tableToken);
        } else {
            if (TableUtils.isPendingRenameTempTableName(tableToken.getTableName(), engine.getConfiguration().getTempRenamePendingTablePrefix())) {
                tableToken = renameToExpectedTableName(tableToken);
            }

            if (engine.getTableSequencerAPI().isTxnTrackerInitialised(tableToken)) {
                if (engine.getTableSequencerAPI().notifyOnCheck(tableToken, seqTxn)) {
                    engine.notifyWalTxnCommitted(tableToken);
                }
            } else {
                threadLocalPath.trimTo(dbRoot.length()).concat(tableToken).concat(TableUtils.META_FILE_NAME).$();
                if (ff.exists(threadLocalPath)) {
                    threadLocalPath.trimTo(dbRoot.length()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$();
                    try (TxReader txReader = this.txReader.ofRO(threadLocalPath, PartitionBy.NONE)) {
                        TableUtils.safeReadTxn(this.txReader, millisecondClock, spinLockTimeout);
                        if (engine.getTableSequencerAPI().initTxnTracker(tableToken, txReader.getSeqTxn(), seqTxn)) {
                            engine.notifyWalTxnCommitted(tableToken);
                        }
                    }
                } else {
                    // table is dropped, notify the JOB to delete the data
                    engine.notifyWalTxnCommitted(tableToken);
                }
            }
        }
    }

    @Override
    public boolean runSerially() {
        long unpublishedWalTxnCount = engine.getUnpublishedWalTxnCount();
        if (unpublishedWalTxnCount == lastProcessedCount) {
            return false;
        }
        checkMissingWalTransactions();
        lastProcessedCount = unpublishedWalTxnCount;
        return true;
    }

    @NotNull
    private TableToken renameToExpectedTableName(TableToken tableToken) {
        try {
            LOG.info().$("attempting to apply deferred table rename [tempName=").utf8(tableToken.getTableName()).I$();

            // Table name is temporary, because the real table name was occupied by another one at the point of creation
            // Rename the table to the correct name.
            renameTrackingMetadataService.tableName = TableUtils.getTableNameFromDirName(tableToken.getDirName());
            try (TableMetadataChangeLog metaChangeCursor = engine.getTableSequencerAPI().getMetadataChangeLog(tableToken, 0)) {
                while (metaChangeCursor.hasNext()) {
                    TableMetadataChange change = metaChangeCursor.next();
                    change.apply(renameTrackingMetadataService, true);
                }
            }

            TableToken updatedTableToken = tableToken.renamed(Chars.toString(renameTrackingMetadataService.tableName));
            try {
                engine.applyTableRename(tableToken, updatedTableToken);
                LOG.info().$("successfully applied deferred table rename [tempName=").utf8(tableToken.getTableName())
                        .$(", to=").utf8(updatedTableToken.getTableName())
                        .$(", tableDir=").utf8(tableToken.getDirName())
                        .I$();
                tableToken = updatedTableToken;
            } catch (CairoException e) {
                // In most cases it's expected, the table name can be still occupied by another table
                LOG.info().$("could not apply deferred table rename [tempName=").utf8(tableToken.getTableName())
                        .$(", to=").utf8(updatedTableToken.getTableName())
                        .$(", error=").$(e.getFlyweightMessage())
                        .I$();
            }
            return tableToken;
        } catch (CairoException ex) {
            LOG.info().$("failed to rename to final table name [table=").$(tableToken).$(", error=").$(ex.getFlyweightMessage()).$(", errno=").$(ex.getErrno()).I$();
            return tableToken;
        }
    }

    private static class RenameTrackingMetadataService implements MetadataServiceStub {
        private CharSequence tableName;

        @Override
        public void addColumn(CharSequence name, int type, int symbolCapacity, boolean symbolCacheFlag, boolean isIndexed, int indexValueBlockCapacity, boolean isSequential, SecurityContext securityContext) {
        }

        @Override
        public TableRecordMetadata getMetadata() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableToken getTableToken() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeColumn(@NotNull CharSequence columnName) {
        }

        @Override
        public void renameColumn(@NotNull CharSequence columnName, @NotNull CharSequence newName, SecurityContext securityContext) {
        }

        @Override
        public void renameTable(@NotNull CharSequence fromNameTable, @NotNull CharSequence toTableName) {
            tableName = Chars.toString(toTableName);
        }
    }
}
