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

package io.questdb.cairo.wal.seq;

import io.questdb.mp.SynchronizedJob;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

/**
 * Sweep that cancels {@link TxnWaiter} instances whose deadline has passed. On each
 * invocation it walks every known {@link SeqTxnTracker} and calls
 * {@link SeqTxnTracker#cancelExpiredWaiters(long)} on it. The base
 * {@link SynchronizedJob} CAS lock ensures at most one worker performs the sweep at a
 * time when the same instance is assigned to every worker via {@code WorkerPool.assign(Job)};
 * losers exit immediately.
 *
 * <p>The tracker enumeration is supplied as a {@link Consumer}-accepting callback so the
 * job does not need to know whether trackers live inside a sequencer API, a test
 * harness, or a mock.
 *
 * <p>{@link #shutdown()} is the engine-shutdown hook: it walks every tracker and forces
 * every parked waiter to wake with the shutdown flag set, so worker pools can halt
 * without blocking on continuations that nothing else will ever fire.
 */
public final class WaiterTimeoutJob extends SynchronizedJob {
    private final MillisecondClock clock;
    private final Consumer<Consumer<SeqTxnTracker>> trackerEnumerator;

    public WaiterTimeoutJob(
            @NotNull MillisecondClock clock,
            @NotNull Consumer<Consumer<SeqTxnTracker>> trackerEnumerator
    ) {
        this.clock = clock;
        this.trackerEnumerator = trackerEnumerator;
    }

    /**
     * Releases every parked waiter across all trackers: marks each waiter's continuation
     * as shutting down and schedules a resume so the body wakes, observes the flag, and
     * unwinds. Must run while worker pools are still RUNNING -- otherwise the resumed
     * conts have no carrier left to remount them. Idempotent: late waiters that get
     * registered after this call still observe the engine's closing state through the
     * SQL circuit breaker before they reach {@code suspend()}.
     */
    public void shutdown() {
        trackerEnumerator.accept(SeqTxnTracker::shutdownAllWaiters);
    }

    @Override
    protected boolean runSerially() {
        long now = clock.getTicks();
        trackerEnumerator.accept(tracker -> tracker.cancelExpiredWaiters(now));
        return false;
    }
}