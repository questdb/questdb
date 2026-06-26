/*******************************************************************************
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

package io.questdb.cairo.wal;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Engine-wide registry of {@link WalWriter}s that have a PENDING (not-yet-device-flushed) adaptive
 * group-commit (Deferred 2, {@code cairo.adaptive.commit.group.window.us} {@code > 0}).
 *
 * <p>The hard durability requirement is that an idle writer's last commit becomes device-durable within
 * {@code <= W} even when commits STOP. A committing thread advances durability on its own subsequent
 * commits (the commit-driven trigger in {@code WalWriter.commit0}); but if commits cease, nothing on the
 * commit path will ever flush the tail. This registry lets a BACKGROUND flusher (driven by
 * {@code WalPurgeJob}) sweep every writer with a pending commit older than {@code W} and force its deferred
 * fdatasync.
 *
 * <p>A {@link WalWriter} adds itself on the transition into a pending state and removes itself once its
 * pending backlog is flushed (or on close). The set is a concurrent, weakly-consistent snapshot: a writer
 * added or removed during a sweep is handled on the next sweep, which is exactly the desired bound (the
 * sweep cadence is {@code <= W}). {@link WalWriter#forceDurableIfPending(long, long)} is itself thread-safe
 * (it synchronizes on the writer), so the flusher never corrupts a writer that is concurrently committing —
 * the two simply contend on the writer's monitor and at most one performs the flush.
 */
public class WalGroupCommitFlushQueue {
    private static final Log LOG = LogFactory.getLog(WalGroupCommitFlushQueue.class);
    // Identity set of writers with a pending durable commit. ConcurrentHashMap.newKeySet gives lock-free
    // add/remove and a weakly-consistent iterator — safe to traverse while writers register/deregister.
    private final Set<WalWriter> pending = ConcurrentHashMap.newKeySet();
    // Count of background flushes that threw (and were dropped). Stays 0 in healthy operation; a non-zero
    // value flags a writer that became distressed / a table mid-drop / a flush hitting a stale fd. Used by
    // the concurrency stress test to detect a flusher-vs-segment-roll race, and useful operator telemetry.
    private final java.util.concurrent.atomic.AtomicLong failedFlushes = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Number of background flushes that failed (threw) and were dropped from the queue. 0 in healthy
     * operation.
     */
    public long getFailedFlushCount() {
        return failedFlushes.get();
    }

    /**
     * Register {@code writer} as having a pending (deferred) device flush. Idempotent.
     */
    public void register(WalWriter writer) {
        pending.add(writer);
    }

    /**
     * Force the deferred fdatasync of every registered writer whose oldest pending commit is at least
     * {@code windowUs} old relative to {@code nowMicros}. A writer whose backlog the flush drains (or that
     * is found to have no pending work) removes itself via {@link #unregister(WalWriter)}.
     *
     * <p>Best-effort and non-throwing: a writer that is busy committing (or mid-close) is skipped and
     * retried on the next sweep — its own committing thread will flush it, or it will still be pending next
     * time. The flusher must never let one distressed writer abort the whole sweep.
     *
     * @return {@code true} if at least one writer was flushed (useful work done)
     */
    public boolean sweep(long nowMicros, long windowUs) {
        if (pending.isEmpty()) {
            return false;
        }
        boolean did = false;
        for (WalWriter w : pending) {
            try {
                if (w.forceDurableIfPending(nowMicros, windowUs)) {
                    did = true;
                }
            } catch (Throwable th) {
                // A writer flush can fail (e.g. it became distressed, or the table is mid-drop). Drop it from
                // the queue so a permanently broken writer cannot wedge the sweep; the writer itself surfaces
                // the error to its own commit path. Durability is unaffected: localDurableSeqTxn only advances
                // on a SUCCESSFUL flush, so a failed flush leaves the durable frontier honestly behind.
                LOG.error().$("group-commit background flush failed [table=").$(w.getTableToken())
                        .$(", error=").$(th).I$();
                failedFlushes.incrementAndGet();
                pending.remove(w);
            }
        }
        return did;
    }

    /**
     * Deregister {@code writer} (its backlog is flushed, or it is closing). Idempotent.
     */
    public void unregister(WalWriter writer) {
        pending.remove(writer);
    }
}
