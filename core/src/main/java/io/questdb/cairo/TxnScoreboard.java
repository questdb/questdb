/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.std.QuietCloseable;

/**
 * Per-table transaction scoreboard. Used as a MVCC building block:
 * table readers acquire a transaction number lock in the scoreboard when
 * going active. This way table writers and purge jobs (GC) can check
 * if files associated with an older transaction can be deleted.
 */
public interface TxnScoreboard extends QuietCloseable {
    int CHECKPOINT_ID = -1;
    long UNKNOWN_SEQ_TXN = -1;

    boolean acquireTxn(int id, long tableTxn, long seqTxn);

    int getEntryCount();

    /**
     * Returns the minimum sequencer transaction needed by active table snapshots.
     * The caller must read {@code currentTableTxn} and {@code currentSeqTxn} from one stable {@code _txn} view.
     * The caller must not use the result for reclamation while a checkpoint is in progress.
     */
    default long getMinSeqTxn(long currentTableTxn, long currentSeqTxn) {
        return UNKNOWN_SEQ_TXN;
    }

    TableToken getTableToken();

    boolean hasEarlierTxnLocks(long maxTxn);

    /**
     * Copies an active source {@link TableReader}'s transaction lock into another scoreboard slot.
     * This method does not check the maximum table transaction.
     * <p>
     * A reader-pool copy must keep its source reader active at the same {@code tableTxn} and {@code seqTxn}.
     * The source reader must stay at that snapshot until the copied reader releases its lock.
     * The source reader must not close, reload, or go passive sooner.
     * <p>
     * {@link DatabaseCheckpointAgent} is the only exception.
     * It may close the source reader after it acquires {@link #CHECKPOINT_ID}.
     * The checkpoint-in-progress state blocks reclamation while the checkpoint uses the copied snapshot.
     */
    boolean incrementTxn(int id, long tableTxn, long seqTxn);

    boolean isOutdated(long txn);

    boolean isRangeAvailable(long fromTxn, long toTxn);

    boolean isTxnAvailable(long txn);

    long releaseTxn(int id, long txn);
}
