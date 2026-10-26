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
import io.questdb.std.datetime.MicrosecondClock;
import org.jetbrains.annotations.TestOnly;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Engine-wide set of {@link WalWriter}s whose adaptive group commit is not yet flushed to the device.
 * <p>
 * A committing thread flushes on its own later commits, but once commits stop nothing on the commit path flushes
 * the tail. The background flusher, driven by {@code WalPurgeJob}, sweeps this set and forces the deferred
 * fdatasync of every writer whose pending commit is older than the group window, so an idle writer's last commit
 * becomes durable within the window. A writer adds itself when it enters the pending state and removes itself once
 * its backlog is flushed, or on close; one added or removed during a sweep is handled by the next sweep.
 * {@link WalWriter#forceDurableIfPending(long)} synchronizes on the writer, so at most one of the flusher and a
 * committing thread flushes it.
 * <p>
 * The queue keeps a lower and an upper bound on the batch start of every pending writer, so that
 * {@link #sweepIfDue(long, MicrosecondClock)} enters no writer's monitor while no batch can be due. Three sites keep
 * the bounds: {@link #register(WalWriter, long)} widens them after adding the writer; a walk resets them before it
 * creates its iterator; and the walk widens them, when it ends, by the batch starts that
 * {@link WalWriter#forceDurableIfPending(long)} reports for the writers it left pending. A batch start dropped by a
 * reset therefore belongs to a writer whose add preceded the walk's iterator, and that walk reports it. Bounds
 * left by a flushed or closed writer are only wider than needed, which costs one more walk.
 */
public class WalGroupCommitFlushQueue {
    // Returned by WalWriter.forceDurableIfPending when it flushed the writer's batch.
    static final long BATCH_FLUSHED = Long.MIN_VALUE;
    // Returned by WalWriter.forceDurableIfPending when the writer had no pending batch.
    static final long NO_PENDING_BATCH = Long.MAX_VALUE;
    private static final Log LOG = LogFactory.getLog(WalGroupCommitFlushQueue.class);
    private static final long NO_EARLIEST_BATCH_START = Long.MAX_VALUE;
    private static final long NO_LATEST_BATCH_START = Long.MIN_VALUE;
    // Lower bound on the batch start (writer's clock, micros) of every pending writer; see the class comment.
    private final AtomicLong earliestBatchStartMicros = new AtomicLong(NO_EARLIEST_BATCH_START);
    // A non-zero count flags a distressed writer, a table being dropped, or a flush on a stale fd.
    @TestOnly
    private final AtomicLong failedFlushes = new AtomicLong();
    // Upper bound on the batch start of every pending writer; a clock reading below it is a backwards step.
    private final AtomicLong latestBatchStartMicros = new AtomicLong(NO_LATEST_BATCH_START);
    // Identity set of writers with a pending durable commit. Its iterator is weakly consistent, so a sweep can
    // traverse it while writers register and deregister.
    private final Set<WalWriter> pending = ConcurrentHashMap.newKeySet();

    /**
     * Number of background flushes that threw and dropped their writer from the set. Zero in healthy operation.
     */
    @TestOnly
    public long getFailedFlushCount() {
        return failedFlushes.get();
    }

    /**
     * Register {@code writer} as having a pending (deferred) device flush whose batch started at
     * {@code batchStartMicros} on the writer's clock. Idempotent. The caller holds the writer monitor.
     */
    public void register(WalWriter writer, long batchStartMicros) {
        // Add before widening the bounds: a walk that resets them after this point creates its iterator later
        // and so reaches this writer.
        pending.add(writer);
        lowerEarliestBatchStart(batchStartMicros);
        raiseLatestBatchStart(batchStartMicros);
    }

    /**
     * Forces the deferred fdatasync of every registered writer whose oldest pending commit is at least
     * {@code windowUs} old, or whose age is negative because the clock stepped back. Each writer reads the clock under
     * its own monitor, which orders the read after its batch start. A writer whose backlog the flush drains, or that
     * has no pending work, removes itself through {@link #unregister(WalWriter)}.
     * <p>
     * A failure other than a data sync failure is logged and drops the writer from the set, so one distressed
     * writer cannot abort the sweep. A data sync failure is rethrown, because the writer has already stopped the
     * engine.
     *
     * @return {@code true} if at least one writer was flushed
     */
    public boolean sweep(long windowUs) {
        if (pending.isEmpty()) {
            return false;
        }
        return walk(windowUs);
    }

    /**
     * Runs {@link #sweep(long)} only if a writer may be due at {@code clock}'s reading: some batch is
     * {@code windowUs} old, or the reading is below a batch start (the clock stepped back). Otherwise no writer
     * would flush, and the walk is skipped. {@code clock} must be the clock the writers read for their batch starts.
     */
    public void sweepIfDue(long windowUs, MicrosecondClock clock) {
        if (pending.isEmpty()) {
            return;
        }
        // Read the bounds before the clock: every batch start in them was read before this reading, so a reading
        // below the latest one is a backwards step, not a batch that started after it.
        final long latest = latestBatchStartMicros.get();
        final long earliest = earliestBatchStartMicros.get();
        final long nowMicros = clock.getTicks();
        if (nowMicros >= latest && (earliest == NO_EARLIEST_BATCH_START || nowMicros - earliest < windowUs)) {
            return;
        }
        walk(windowUs);
    }

    /**
     * Deregister {@code writer} (its backlog is flushed, or it is closing). Idempotent. The bounds are left as
     * they are: they may only be wider than the pending set needs.
     */
    public void unregister(WalWriter writer) {
        pending.remove(writer);
    }

    private void lowerEarliestBatchStart(long batchStartMicros) {
        long current;
        while (batchStartMicros < (current = earliestBatchStartMicros.get())) {
            if (earliestBatchStartMicros.compareAndSet(current, batchStartMicros)) {
                return;
            }
        }
    }

    private void raiseLatestBatchStart(long batchStartMicros) {
        long current;
        while (batchStartMicros > (current = latestBatchStartMicros.get())) {
            if (latestBatchStartMicros.compareAndSet(current, batchStartMicros)) {
                return;
            }
        }
    }

    private boolean walk(long windowUs) {
        // Reset before the iterator is created; see the class comment.
        final long earliestBefore = earliestBatchStartMicros.getAndSet(NO_EARLIEST_BATCH_START);
        final long latestBefore = latestBatchStartMicros.getAndSet(NO_LATEST_BATCH_START);
        long earliest = NO_EARLIEST_BATCH_START;
        long latest = NO_LATEST_BATCH_START;
        boolean flushed = false;
        boolean walked = false;
        try {
            for (WalWriter w : pending) {
                try {
                    final long batchStartMicros = w.forceDurableIfPending(windowUs);
                    if (batchStartMicros == BATCH_FLUSHED) {
                        flushed = true;
                    } else if (batchStartMicros != NO_PENDING_BATCH) {
                        earliest = Math.min(earliest, batchStartMicros);
                        latest = Math.max(latest, batchStartMicros);
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
            walked = true;
        } finally {
            if (!walked) {
                // The walk stopped early. The writers it did not reach had their batch starts within the bounds it
                // reset, so put those bounds back.
                earliest = Math.min(earliest, earliestBefore);
                latest = Math.max(latest, latestBefore);
            }
            lowerEarliestBatchStart(earliest);
            raiseLatestBatchStart(latest);
        }
        return flushed;
    }
}
