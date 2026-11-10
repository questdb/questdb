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

package io.questdb.cairo.mv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.LongList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.locks.Lock;

/**
 * The role-switch commit fence of a materialized view's own WAL. Every write that externalizes a replicated seqTxn
 * on a view's WAL goes through it: the refresh job's data commits and view state events, and the view state event
 * that {@code REFRESH MATERIALIZED VIEW ... RESUME} writes.
 * <p>
 * A primary-to-replica demote flips the node read-only in place, without halting the refresh workers or the
 * statements that already hold a view's {@link WalWriter}. A mint that lands after the flip would leave a
 * local-only seqTxn on the replicated view table that the closing uploader never ships. The fence holds the
 * role-switch READ lock across an authoritative in-lock read-only re-check and the mint, so the mint is atomic
 * against the role flip: either the flip ran first and the fence refuses with a read-only error, or the mint lands
 * fully as primary while the flip's WRITE acquire waits for this read hold. For a deployment that does not
 * replicate, the read lock is uncontended and the read-only flag static, so the fence costs nothing.
 */
public final class MatViewCommitFence {

    private MatViewCommitFence() {
    }

    /**
     * Takes the fence. The caller mints its WAL transaction and then unlocks the returned lock.
     *
     * @throws CairoException a read-only refusal when the node is, or just became, read-only
     */
    public static Lock acquire(@NotNull CairoEngine engine) {
        if (engine.isReadOnlyMode()) {
            throw CairoException.readOnlyAccess();
        }
        final Lock lock = engine.getRoleSwitchReadLock();
        lock.lock();
        try {
            if (engine.isReadOnlyMode()) {
                throw CairoException.readOnlyAccess();
            }
            engine.fireRoleSwitchMintObserver();
            return lock;
        } catch (Throwable th) {
            lock.unlock();
            throw th;
        }
    }

    /**
     * Writes a view state event through {@link WalWriter#resetMatViewState} under the fence. Every view state event
     * carries the base table id and the backlog-lost marker, see {@link MatViewBacklog}.
     */
    public static void resetMatViewState(
            @NotNull CairoEngine engine,
            @NotNull WalWriter walWriter,
            long lastRefreshBaseTxn,
            long lastRefreshTimestamp,
            boolean isInvalid,
            @Nullable CharSequence invalidationReason,
            long lastPeriodHi,
            @Nullable LongList refreshIntervals,
            long refreshIntervalsBaseTxn,
            int baseTableId,
            @Nullable CharSequence backlogLostReason
    ) {
        final Lock lock = acquire(engine);
        try {
            walWriter.resetMatViewState(
                    lastRefreshBaseTxn,
                    lastRefreshTimestamp,
                    isInvalid,
                    invalidationReason,
                    lastPeriodHi,
                    refreshIntervals,
                    refreshIntervalsBaseTxn,
                    baseTableId,
                    backlogLostReason
            );
        } finally {
            lock.unlock();
        }
    }
}
