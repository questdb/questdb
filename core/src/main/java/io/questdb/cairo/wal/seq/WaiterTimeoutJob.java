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
import io.questdb.std.datetime.NanosecondClock;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

/**
 * Periodic sweep that cancels {@link TxnWaiter} instances whose deadline has passed.
 * Runs on the shared worker pool; on each invocation it walks every known
 * {@link SeqTxnTracker} at most once per {@code sweepIntervalNanos} and invokes
 * {@link SeqTxnTracker#cancelExpiredWaiters(long)} on it.
 *
 * <p>The tracker enumeration is supplied as a {@link Consumer}-accepting callback so the
 * job does not need to know whether trackers live inside a sequencer API, a test
 * harness, or a mock.
 */
public final class WaiterTimeoutJob implements Job {
    private final NanosecondClock clock;
    private final long sweepIntervalNanos;
    private final Consumer<Consumer<SeqTxnTracker>> trackerEnumerator;
    private long nextSweepNanos;

    public WaiterTimeoutJob(
            @NotNull NanosecondClock clock,
            long sweepIntervalNanos,
            @NotNull Consumer<Consumer<SeqTxnTracker>> trackerEnumerator
    ) {
        this.clock = clock;
        this.sweepIntervalNanos = sweepIntervalNanos;
        this.trackerEnumerator = trackerEnumerator;
    }

    @Override
    public boolean run(int workerId, @NotNull RunStatus runStatus) {
        long now = clock.getTicks();
        if (now < nextSweepNanos) {
            return false;
        }
        nextSweepNanos = now + sweepIntervalNanos;
        trackerEnumerator.accept(tracker -> tracker.cancelExpiredWaiters(now));
        return false;
    }
}
