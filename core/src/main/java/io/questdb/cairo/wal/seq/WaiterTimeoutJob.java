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

import io.questdb.mp.Job;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

/**
 * Periodic sweep that cancels {@link TxnWaiter} instances whose deadline has passed.
 * Runs on the shared worker pool; on each invocation it walks every known
 * {@link SeqTxnTracker} at most once per {@code sweepIntervalMillis} and invokes
 * {@link SeqTxnTracker#cancelExpiredWaiters(long)} on it.
 *
 * <p>The tracker enumeration is supplied as a {@link Consumer}-accepting callback so the
 * job does not need to know whether trackers live inside a sequencer API, a test
 * harness, or a mock.
 *
 * <p>The same instance may be assigned to every worker via {@code WorkerPool.assign(Job)};
 * a CAS on {@link #nextSweepMillis} ensures that at most one worker performs the sweep
 * per interval, regardless of how many workers race into {@link #run}.
 */
public final class WaiterTimeoutJob implements Job {
    private static final long NEXT_SWEEP_OFFSET = Unsafe.getFieldOffset(WaiterTimeoutJob.class, "nextSweepMillis");
    private final MillisecondClock clock;
    private final long sweepIntervalMillis;
    private final Consumer<Consumer<SeqTxnTracker>> trackerEnumerator;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long nextSweepMillis;

    public WaiterTimeoutJob(
            @NotNull MillisecondClock clock,
            long sweepIntervalMillis,
            @NotNull Consumer<Consumer<SeqTxnTracker>> trackerEnumerator
    ) {
        this.clock = clock;
        this.sweepIntervalMillis = sweepIntervalMillis;
        this.trackerEnumerator = trackerEnumerator;
    }

    @Override
    public boolean run(int workerId, @NotNull RunStatus runStatus) {
        long now = clock.getTicks();
        long expected = nextSweepMillis;
        if (now < expected) {
            return false;
        }
        // CAS prevents multiple workers (when assigned via WorkerPool.assign(Job)) from
        // double-sweeping in the same interval. The loser exits without sweeping.
        if (!Unsafe.cas(this, NEXT_SWEEP_OFFSET, expected, now + sweepIntervalMillis)) {
            return false;
        }
        trackerEnumerator.accept(tracker -> tracker.cancelExpiredWaiters(now));
        return false;
    }
}
