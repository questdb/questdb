/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjHashSet;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

public class CheckWalTransactionsJob extends SynchronizedJob {
    private final long checkInterval;
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
    private long lastRunMs;
    private boolean notificationQueueIsFull = false;
    private Path threadLocalPath;


    public CheckWalTransactionsJob(CairoEngine engine) {
        this.engine = engine;
        this.ff = engine.getConfiguration().getFilesFacade();
        txReader = new TxReader(engine.getConfiguration().getFilesFacade());
        dbRoot = engine.getConfiguration().getDbRoot();
        millisecondClock = engine.getConfiguration().getMillisecondClock();
        spinLockTimeout = engine.getConfiguration().getSpinLockTimeout();
        checkNotifyOutstandingTxnInWalRef = (tableId, token, txn) -> checkNotifyOutstandingTxnInWal(token, txn);
        checkInterval = engine.getConfiguration().getSequencerCheckInterval();
        lastRunMs = millisecondClock.getTicks();
    }

    public void checkMissingWalTransactions() {
        threadLocalPath = Path.PATH.get().of(dbRoot);
        engine.getTableSequencerAPI().forAllWalTables(tableTokenBucket, true, checkNotifyOutstandingTxnInWalRef);
    }

    public void checkNotifyOutstandingTxnInWal(@NotNull TableToken tableToken, long seqTxn) {
        if (notificationQueueIsFull) {
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
            notificationQueueIsFull = !engine.notifyWalTxnCommitted(tableToken);
        } else {
            if (engine.getTableSequencerAPI().isTxnTrackerInitialised(tableToken)) {
                if (engine.getTableSequencerAPI().notifyOnCheck(tableToken, seqTxn)) {
                    notificationQueueIsFull = !engine.notifyWalTxnCommitted(tableToken);
                }
            } else {
                LPSZ txnPath = threadLocalPath.trimTo(dbRoot.length()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$();
                if (ff.exists(txnPath)) {
                    try (
                            TableMetadata tableMetadata = engine.getTableMetadata(tableToken);
                            TxReader txReader = this.txReader.ofRO(txnPath, tableMetadata.getTimestampType(), tableMetadata.getPartitionBy())
                    ) {
                        TableUtils.safeReadTxn(this.txReader, millisecondClock, spinLockTimeout);
                        if (engine.getTableSequencerAPI().initTxnTracker(tableToken, txReader.getSeqTxn(), seqTxn)) {
                            notificationQueueIsFull = !engine.notifyWalTxnCommitted(tableToken);
                        }
                    } catch (CairoException e) {
                        if (!e.isFileCannotRead()) {
                            throw e;
                        } // race, table is dropped, ApplyWal2TableJob is already deleting the files
                    }
                } // else table is dropped, ApplyWal2TableJob already is deleting the files
            }
        }
    }

    @Override
    public boolean runSerially() {
        long unpublishedWalTxnCount = engine.getUnpublishedWalTxnCount();
        if (unpublishedWalTxnCount == lastProcessedCount || notificationQueueIsFull) {
            // when notification queue was full last run, re-evaluate tables after a timeout
            final long t = millisecondClock.getTicks();
            if (lastRunMs + checkInterval < t) {
                lastRunMs = t;
                notificationQueueIsFull = !republishNotificationsFromTrackers();
            }
            return false;
        }
        checkMissingWalTransactions();
        lastProcessedCount = unpublishedWalTxnCount;
        return !notificationQueueIsFull;
    }

    private boolean republishNotificationsFromTrackers() {
        engine.getTableTokens(tableTokenBucket, false);
        for (int i = 0, n = tableTokenBucket.size(); i < n; i++) {
            TableToken tableToken = tableTokenBucket.get(i);
            SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);
            if (!tracker.isSuspended() && tracker.getWriterTxn() < tracker.getSeqTxn()) {
                if (!engine.notifyWalTxnCommitted(tableToken)) {
                    return false;
                }
            }
        }
        return true;
    }
}
