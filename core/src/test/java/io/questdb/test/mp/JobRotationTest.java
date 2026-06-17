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

package io.questdb.test.mp;

import io.questdb.mp.Job;
import io.questdb.mp.continuation.WorkerContinuation;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class JobRotationTest {

    /**
     * Regression test for the rotation recycle pool being write-only at halt.
     * <p>
     * A single suspending "session" re-parks many times during RUNNING (the way
     * {@code sleep(N)} / {@code wait_wal_table} re-park once per wake interval).
     * Each re-park makes the worker's outer driver abandon the continuation it
     * suspended to surrender the resumed session cont. That abandoned cont holds
     * a freshly-minted generation, including the stateful {@link SelectorJob}
     * clone (the analog of an HTTP selector + handler set).
     * <p>
     * Before the fix, {@code Worker.recycleJobList()} ran only when a cont became
     * done -- i.e. only at halt -- so every re-park cold-minted a fresh clone and
     * pinned it until shutdown: {@code clonesCreated} grew with the number of
     * re-parks. After the fix, a handoff-suspended (spent) cont is recycled
     * during RUNNING, so the generation -- and its stateful clone -- returns to
     * the pool and is reused: {@code clonesCreated} stays bounded by the
     * concurrent-suspend high-water mark (here 1), no matter how many times the
     * session re-parks.
     */
    @Test
    public void testStatefulCloneReusedAcrossReparksDuringRunning() throws InterruptedException {
        final int reparks = 60;
        final SelectorJob selectorJob = new SelectorJob();
        final SleeperJob sleeperJob = new SleeperJob(reparks);

        TestWorkerPool pool = new TestWorkerPool("job-rotation-test", 1);
        // selectorJob.run() returns true so the worker loop stays hot and drives
        // the re-park dance without napping; it does no real work.
        pool.assign(selectorJob);
        pool.assign(sleeperJob);

        try {
            pool.start();
            Assert.assertTrue(
                    "session did not complete its re-parks in time",
                    sleeperJob.done.await(30, TimeUnit.SECONDS)
            );

            int created = selectorJob.clonesCreated.get();
            int recycled = selectorJob.clonesRecycled.get();

            // Fix: exactly one cold clone is ever minted, then reused from the
            // pool every cycle. The old code minted one clone per re-park and
            // never recycled during RUNNING, so created would be ~= reparks.
            Assert.assertTrue(
                    "stateful clones grew with re-park count -- recycle pool not "
                            + "replenished during RUNNING (created=" + created
                            + ", reparks=" + reparks + ")",
                    created <= 5
            );
            // The recycle path must actually run during RUNNING (it was 0 before
            // the fix). Allow slack for scheduling, but it must clearly engage.
            Assert.assertTrue(
                    "recycle path never ran during RUNNING (recycled=" + recycled + ")",
                    recycled >= reparks / 2
            );
        } finally {
            pool.halt();
        }
    }

    /**
     * Stateful job standing in for HttpServer's per-clone selector + handler set.
     * Each {@link #cloneInstance()} mints a fresh instance (clone != this, so the
     * pool tracks it in ownedJobClones); {@link #recycleInstance()} returns it.
     */
    private static final class SelectorJob implements Job {
        final AtomicInteger clonesCreated = new AtomicInteger();
        final AtomicInteger clonesRecycled = new AtomicInteger();

        @Override
        public Job cloneInstance() {
            clonesCreated.incrementAndGet();
            // Share the counters with the root so the test observes the totals
            // through clones that the worker recycles/closes during rotation.
            return new CountingSelectorJob(clonesCreated, clonesRecycled);
        }

        @Override
        public boolean run(@NotNull WorkerContext workerContext) {
            // Keep the worker loop hot so re-parks drive quickly; no real work.
            return true;
        }
    }

    /**
     * The instance returned by {@link SelectorJob#cloneInstance()} -- carries the
     * shared counters so {@link #recycleInstance()} / {@link #closeInstance()} on a
     * clone is observable by the test through the root job.
     */
    private static final class CountingSelectorJob implements Job {
        private final AtomicInteger created;
        private final AtomicInteger recycled;

        CountingSelectorJob(AtomicInteger created, AtomicInteger recycled) {
            this.created = created;
            this.recycled = recycled;
        }

        @Override
        public Job cloneInstance() {
            created.incrementAndGet();
            return new CountingSelectorJob(created, recycled);
        }

        @Override
        public void recycleInstance() {
            recycled.incrementAndGet();
        }

        @Override
        public boolean run(@NotNull WorkerContext workerContext) {
            return true;
        }
    }

    /**
     * Drives a single suspending session that re-parks {@code reparks} times. The
     * session is keyed on the continuation that started it ({@link #sleeperCont}),
     * so only that cont re-parks; fresh conts minted by rotation just drain the
     * resume queue and hand the session cont back to the outer driver.
     */
    private static final class SleeperJob implements Job {
        final CountDownLatch done = new CountDownLatch(1);
        private final AtomicInteger remaining;
        private final AtomicBoolean started = new AtomicBoolean();
        private volatile WorkerContinuation sleeperCont;

        SleeperJob(int reparks) {
            this.remaining = new AtomicInteger(reparks);
        }

        @Override
        public boolean run(@NotNull WorkerContext workerContext) {
            WorkerContinuation cur = WorkerContinuation.current();
            if (cur == null) {
                return false;
            }
            if (cur == sleeperCont) {
                // The session cont resuming from a previous park.
                if (remaining.get() > 0) {
                    remaining.decrementAndGet();
                    cur.scheduleResume();
                    WorkerContinuation.suspend();
                } else {
                    sleeperCont = null;
                    done.countDown();
                }
                return false;
            }
            // A fresh (non-session) cont: start the one and only session, pinning
            // it to this cont, then park. Subsequent fresh conts skip this.
            if (sleeperCont == null && started.compareAndSet(false, true)) {
                sleeperCont = cur;
                cur.scheduleResume();
                WorkerContinuation.suspend();
            }
            return false;
        }
    }
}
