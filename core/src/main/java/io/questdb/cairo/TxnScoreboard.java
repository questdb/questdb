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
    // Two virtual pins that stop partition purge from reclaiming the partition versions named by the table's
    // two save point generations (an O3 rewrite leaves the old version on disk). Each id is a single slot
    // holding one txn, so advance() alternates between EPOCH_ID_A and EPOCH_ID_B: the slot of the generation
    // being overwritten takes the new txn, and the prior txn stays pinned in the other slot, because recovery
    // may fall back to it. A concurrent O3PartitionPurgeJob therefore never finds either save point unprotected.
    // Both ids are independent of CHECKPOINT_ID and of each other; TxnScoreboardV2.toInternalId() maps them
    // to slots.
    int EPOCH_ID_A = -2;
    int EPOCH_ID_B = -3;
    // seqTxn of a slot that sets no floor for getMinSeqTxn(): the save point pins.
    long NO_SEQ_TXN_FLOOR = Long.MAX_VALUE;
    /**
     * {@link #scanRangeHolders(long, long)}: nothing holds the range.
     */
    int RANGE_FREE = 0;
    /**
     * {@link #scanRangeHolders(long, long)}: a durable-epoch pin ({@link #EPOCH_ID_A} /
     * {@link #EPOCH_ID_B}) holds the range. Blocks an in-place overwrite, not a copy.
     */
    int RANGE_HELD_BY_EPOCH = 1;
    /**
     * {@link #scanRangeHolders(long, long)}: a reader or a checkpoint holds the range. Blocks a copy
     * too: something is reading those files.
     */
    int RANGE_HELD_BY_OTHER = 2;
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

    /**
     * Reports in one pass who holds {@code [fromTxn, toTxn)}: a save point pin, anything else (a reader
     * or a checkpoint), both, or nothing. Returns a mask of {@link #RANGE_HELD_BY_EPOCH} /
     * {@link #RANGE_HELD_BY_OTHER}, or {@link #RANGE_FREE}.
     * <p>
     * The squash gate needs both answers: may it overwrite (nothing holds the range), and failing that may
     * it copy (no reader or checkpoint holds it). Under adaptive a save point pin is always present, so one
     * scan replaces two.
     * <p>
     * Purge callers use {@link #isRangeAvailable(long, long)} instead: they delete bytes a recovery rewind
     * may still need, so a save point pin and a reader pin mean the same to them. Only a copy squash can act
     * on the difference, because it deletes nothing: it writes a new partition version and queues the old
     * one, whose removal goes back through the purge check.
     */
    int scanRangeHolders(long fromTxn, long toTxn);

    long releaseTxn(int id, long txn);
}
