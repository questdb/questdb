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

package io.questdb.mp;

import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public abstract class SynchronizedJob implements Job {
    // PROTOTYPE knob. 0 keeps today's free-for-all CAS exactly, so one binary
    // serves both arms of an A/B. Non-zero is the lease in microseconds.
    private static final long AFFINITY_LEASE_MICROS =
            Long.getLong("questdb.experimental.job.affinity.lease.micros", 0L);
    private static final long LOCKED_OFFSET = Unsafe.getFieldOffset(SynchronizedJob.class, "locked");
    private static final long OWNER_OFFSET = Unsafe.getFieldOffset(SynchronizedJob.class, "owner");

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private volatile int locked = 0;
    // PROTOTYPE: carrier entitled to run this job, and when it last did.
    private volatile int owner = -1;
    private volatile long ownerSeenMicros = 0L;

    @Override
    public boolean run(@NotNull WorkerContext workerContext) {
        // PROTOTYPE: Leader/Followers instead of a free-for-all.
        //
        // This job is already mutually exclusive -- the CAS below means exactly
        // one worker runs it at a time no matter how many ask. So rotating the
        // winner buys no parallelism; it only smears the wakeups. Measured, one
        // busy connection hands every worker in the pool a scrap of work every
        // ~1,400 loop passes, and a scrap resets the worker's back-off ticker,
        // which needs 6,990 consecutive idle passes to reach a nap. Every worker
        // therefore stays permanently a few times short of backing off.
        //
        // Pinning the job to one carrier lets the others get a plain false and
        // climb the ladder. The lease exists only for liveness: if the owner
        // stops calling (parked in another job, halted), anyone may take over
        // once it goes stale.
        // Affinity must never apply to a caller driving this job from outside a
        // Worker loop -- drains, tests, shutdown paths -- which is exactly what
        // ImmutableWorkerContext marks. LogFactory's shutdown drain
        // (`while (job.run(TERMINATING_STATUS))`) runs on the closing thread,
        // which owns nothing; refusing it there exits the drain loop on the
        // first iteration and the final log line is never flushed.
        // LogFactoryTest.testLogAutoDeleteByFileAge6months catches precisely
        // that: it asserts the log file ends with '!'.
        if (AFFINITY_LEASE_MICROS > 0 && !(workerContext instanceof Job.ImmutableWorkerContext)) {
            final int me = workerContext.carrierId();
            final int currentOwner = owner;
            if (currentOwner != me && currentOwner != -1) {
                final long now = Worker.CLOCK_MICROS.getTicks();
                if (now - ownerSeenMicros < AFFINITY_LEASE_MICROS) {
                    return false;
                }
                // Lease expired -- try to take it over. Losing is fine, the
                // winner is whoever gets here first and the rest back off.
                if (!Unsafe.cas(this, OWNER_OFFSET, currentOwner, me)) {
                    return false;
                }
            } else if (currentOwner == -1) {
                Unsafe.cas(this, OWNER_OFFSET, -1, me);
            }
        }
        if (Unsafe.cas(this, LOCKED_OFFSET, 0, 1)) {
            try {
                if (AFFINITY_LEASE_MICROS > 0 && !(workerContext instanceof Job.ImmutableWorkerContext) && owner == workerContext.carrierId()) {
                    ownerSeenMicros = Worker.CLOCK_MICROS.getTicks();
                }
                return runSerially();
            } finally {
                locked = 0;
            }
        }
        return false;
    }

    protected abstract boolean runSerially();
}
