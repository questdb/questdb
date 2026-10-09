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

import io.questdb.cairo.CairoException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Engine-wide set of {@link WalWriter}s whose adaptive group commit is not yet flushed to the device.
 * <p>
 * A committing thread flushes on its own later commits, but once commits stop nothing on the commit path flushes
 * the tail. The background flusher, driven by {@code WalPurgeJob}, sweeps this set and forces the deferred
 * fdatasync of every writer whose pending commit is older than the group window, so an idle writer's last commit
 * becomes durable within the window. A writer adds itself when it enters the pending state and removes itself once
 * its backlog is flushed, or on close; one added or removed during a sweep is handled by the next sweep.
 * {@link WalWriter#forceDurableIfPending(long, long)} synchronizes on the writer, so at most one of the flusher and
 * a committing thread flushes it.
 */
public class WalGroupCommitFlushQueue {
    private static final Log LOG = LogFactory.getLog(WalGroupCommitFlushQueue.class);
    // Identity set of writers with a pending durable commit. Its iterator is weakly consistent, so a sweep can
    // traverse it while writers register and deregister.
    private final Set<WalWriter> pending = ConcurrentHashMap.newKeySet();
    // A non-zero count flags a distressed writer, a table being dropped, or a flush on a stale fd.
    private final java.util.concurrent.atomic.AtomicLong failedFlushes = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Number of background flushes that threw and dropped their writer from the set. Zero in healthy operation.
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
     * Forces the deferred fdatasync of every registered writer whose oldest pending commit is at least
     * {@code windowUs} old at {@code nowMicros}. A writer whose backlog the flush drains, or that has no pending
     * work, removes itself through {@link #unregister(WalWriter)}.
     * <p>
     * A failure other than a data sync failure is logged and drops the writer from the set, so one distressed
     * writer cannot abort the sweep. A data sync failure is rethrown, because the writer has already stopped the
     * engine.
     *
     * @return {@code true} if at least one writer was flushed
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
                if (CairoException.isDataSyncFailure(th)) {
                    // A failed writeback is never retried: the writer has already stopped the engine, and the
                    // worker loop must see the fatal error.
                    CairoException.rethrowCleanupFailure(th);
                }
                // A non-durability failure (e.g. table mid-drop) remains isolated to this writer.
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
