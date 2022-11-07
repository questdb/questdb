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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;

public class CheckWalTransactionsJob extends SynchronizedJob {
    private final TableSequencerAPI.RegisteredTable checkNotifyOutstandingTxnInWal;
    private final CharSequence dbRoot;
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final MillisecondClock millisecondClock;
    private final long spinLockTimeout;
    private final TxReader txReader;
    private long lastProcessedCount = 0;
    private Path threadLocalPath;

    public CheckWalTransactionsJob(CairoEngine engine) {
        this.engine = engine;
        this.ff = engine.getConfiguration().getFilesFacade();
        this.txReader = new TxReader(engine.getConfiguration().getFilesFacade());
        this.dbRoot = engine.getConfiguration().getRoot();
        this.millisecondClock = engine.getConfiguration().getMillisecondClock();
        this.spinLockTimeout = engine.getConfiguration().getSpinLockTimeout();
        checkNotifyOutstandingTxnInWal = this::checkNotifyOutstandingTxnInWal;
    }

    public void checkMissingWalTransactions() {
        threadLocalPath = Path.PATH.get().of(dbRoot);
        engine.getTableSequencerAPI().forAllWalTables(checkNotifyOutstandingTxnInWal);
    }

    public void checkNotifyOutstandingTxnInWal(int tableId, String systemTableName, long txn) {
        threadLocalPath.trimTo(dbRoot.length()).concat(systemTableName).concat(TableUtils.META_FILE_NAME).$();
        if (ff.exists(threadLocalPath)) {
            threadLocalPath.trimTo(dbRoot.length()).concat(systemTableName).concat(TableUtils.TXN_FILE_NAME).$();
            try (TxReader txReader2 = txReader.ofRO(threadLocalPath, PartitionBy.NONE)) {
                TableUtils.safeReadTxn(txReader, millisecondClock, spinLockTimeout);
                if (txReader2.getSeqTxn() < txn) {
                    // table name should be immutable when in the notification message
                    engine.notifyWalTxnCommitted(tableId, systemTableName, txn);
                }
            }
        } else {
            // table is dropped, notify the JOB to delete the data
            engine.notifyWalTxnCommitted(tableId, systemTableName, Long.MAX_VALUE);
        }
    }

    @Override
    protected boolean runSerially() {
        long unpublishedWalTxnCount = engine.getUnpublishedWalTxnCount();
        if (unpublishedWalTxnCount == lastProcessedCount) {
            return false;
        }
        checkMissingWalTransactions();
        lastProcessedCount = unpublishedWalTxnCount;
        return true;
    }
}
