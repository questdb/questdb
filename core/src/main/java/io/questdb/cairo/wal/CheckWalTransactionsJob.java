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
import io.questdb.std.Chars;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;

public class CheckWalTransactionsJob extends SynchronizedJob {
    private final CairoEngine engine;
    private final TxReader txReader;
    private final CharSequence dbRoot;
    private final MillisecondClock milliseconClock;
    private final long spinLockTimeout;
    private long lastProcessed = 0;
    private final TableSequencerAPI.RegisteredTable callback = this::checkNotifyOutstandingTxnInWal;

    public CheckWalTransactionsJob(CairoEngine engine) {
        this.engine = engine;
        txReader = new TxReader(engine.getConfiguration().getFilesFacade());
        dbRoot = engine.getConfiguration().getRoot();
        milliseconClock = engine.getConfiguration().getMillisecondClock();
        spinLockTimeout = engine.getConfiguration().getSpinLockTimeout();
    }

    public void checkMissingWalTransactions() {
        engine.getTableSequencerAPI().forAllWalTables(callback);
    }

    public void checkNotifyOutstandingTxnInWal(int tableId, CharSequence tableName, long txn) {
        // todo: too much GC
        final Path rootPath = Path.PATH.get().of(dbRoot);
        rootPath.concat(tableName).concat(TableUtils.TXN_FILE_NAME).$();
        try (TxReader txReader2 = txReader.ofRO(rootPath, PartitionBy.NONE)) {
            TableUtils.safeReadTxn(txReader2, milliseconClock, spinLockTimeout);
            if (txReader2.getSeqTxn() < txn && !engine.getTableSequencerAPI().isSuspended(tableName)) {
                // table name should be immutable when in the notification message
                final String tableNameStr = Chars.toString(tableName);
                engine.notifyWalTxnCommitted(tableId, tableNameStr, txn);
            }
        }
    }

    @Override
    protected boolean runSerially() {
        final long unpublishedWalTxnCount = engine.getUnpublishedWalTxnCount();
        if (unpublishedWalTxnCount == lastProcessed) {
            return false;
        }
        checkMissingWalTransactions();
        lastProcessed = unpublishedWalTxnCount;
        return true;
    }
}
