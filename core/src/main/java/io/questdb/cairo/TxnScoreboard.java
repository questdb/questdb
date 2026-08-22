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
    // Two PING-PONG virtual pins used by the adaptive durable-epoch path (Plan 3B) to hold the
    // epoch's partition-version set so partition purge cannot reclaim partitions the epoch
    // references (needed for O3-rewritten partitions). Each virtual id is a SINGLE slot holding one
    // txn, so to honour INV-5's pin-before-release ordering across epochs (the new epoch's
    // partition-versions must be protected from purge BEFORE the prior epoch's protection is
    // dropped, leaving no unprotected window for a concurrent O3PartitionPurgeJob), advance()
    // alternates between EPOCH_ID_A and EPOCH_ID_B: pin the NEW txn into the free slot, then release
    // the PRIOR txn from the other slot. Both are INDEPENDENT of CHECKPOINT_ID and of each other.
    // See TxnScoreboardV2.toInternalId() for the id->slot mapping.
    int EPOCH_ID_A = -2;
    int EPOCH_ID_B = -3;
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
     * too -- something is reading those files right now.
     */
    int RANGE_HELD_BY_OTHER = 2;

    boolean acquireTxn(int id, long txn);

    int getEntryCount();

    TableToken getTableToken();

    boolean hasEarlierTxnLocks(long maxTxn);

    /**
     * Ignores min/max txn values and increments the counter. Must be called only when there is
     * an active reader that already acquired this txn.
     * <p>
     * Used by io.questdb.cairo.pool.ReaderPool#getCopyOf()
     */
    boolean incrementTxn(int id, long txn);

    boolean isOutdated(long txn);

    boolean isRangeAvailable(long fromTxn, long toTxn);

    boolean isTxnAvailable(long txn);

    /**
     * One pass over the scoreboard reporting WHO holds {@code [fromTxn, toTxn)}: a durable-epoch
     * pin, anything else (a reader or a checkpoint), both, or nothing. Returns a mask of
     * {@link #RANGE_HELD_BY_EPOCH} / {@link #RANGE_HELD_BY_OTHER}, or {@link #RANGE_FREE}.
     * <p>
     * The squash gate needs BOTH answers -- may it overwrite (nothing holds the range), and failing
     * that may it copy (no reader or checkpoint holds it) -- and asking them as two predicates meant
     * two full scans on exactly the path that always takes both, since under ADAPTIVE the epoch pin
     * is always present. One scan answers both.
     * <p>
     * Deliberately NOT offered to the purge callers of {@link #isRangeAvailable(long, long)}. Each of
     * those is about to DELETE bytes a recovery rewind may still need, so for them an epoch pin and a
     * reader pin are the same answer: no. Only a copy squash can act on the distinction, because it
     * deletes nothing -- it writes a new partition version and merely queues the old one, whose real
     * removal goes back through the epoch-protected predicate.
     */
    int scanRangeHolders(long fromTxn, long toTxn);

    long releaseTxn(int id, long txn);
}
