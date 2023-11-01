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
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjHashSet;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

public class CheckWalTransactionsJob extends SynchronizedJob {
    private final TableSequencerAPI.TableSequencerCallback checkNotifyOutstandingTxnInWalRef;
    private final CharSequence dbRoot;
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final MillisecondClock millisecondClock;
    private final long spinLockTimeout;
    private final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
    // Empty list means that all tables should be checked.
    private final TxReader txReader;
    private long lastProcessedCount = 0;
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


    public void checkMissingWalTransactions() {
        threadLocalPath = Path.PATH.get().of(dbRoot);
        engine.getTableSequencerAPI().forAllWalTables(tableTokenBucket, true, checkNotifyOutstandingTxnInWalRef);
    }

    public void checkNotifyOutstandingTxnInWal(@NotNull TableToken tableToken, long seqTxn) {
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
}
