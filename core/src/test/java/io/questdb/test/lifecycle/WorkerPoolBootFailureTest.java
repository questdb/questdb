package io.questdb.test.lifecycle;

import io.questdb.Metrics;
import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.LifecycleOrchestrator;
import io.questdb.lifecycle.LifecycleStartupException;
import io.questdb.lifecycle.State;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.ObjList;
import io.questdb.test.lifecycle.fakes.ProbeComponent;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.Closeable;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Guards boot-failure surfacing for stage-2 stable-below callbacks.
 * <p>
 * A stage-2 callback (registered via onStableBelow) that throws must cause
 * run() to throw LifecycleStartupException, not return quietly with the
 * component stuck in DEGRADED state (a thread-less zombie that passes readiness
 * probes but cannot serve requests).
 */
public class WorkerPoolBootFailureTest {

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(30, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    /**
     * A SIGTERM-during-boot drives {@code halt(long)} concurrently with a {@code start()} that is still
     * mid-way through its per-worker spawn loop. {@code halt(long)} sets {@code closed} (a plain CAS,
     * outside the monitor) and frees {@code freeOnExit}. If {@code start()} only checks {@code closed}
     * at the top of the method (before the loop), it keeps spawning the remaining workers AFTER the
     * concurrent halt set {@code closed} -- each one loops on the {@code freeOnExit} resources that
     * {@code halt} then frees, a use-after-free plus an orphan-thread leak.
     *
     * <p>The witness uses the {@code beforeWorkerAddedForTesting} seam to park {@code start()} inside
     * the add critical section after worker 0 has been spawned. A second thread then calls
     * {@code halt(long)}; it flips {@code closed} immediately and blocks on the monitor the parked add
     * holds. Releasing the park lets {@code start()} resume INSIDE the same monitor with {@code closed}
     * already set.
     *
     * <p>On the un-fixed tree {@code start()} has no in-lock {@code closed} re-check, so it spawns every
     * remaining worker (1..N-1) against resources {@code halt} is about to free: RED. With the in-lock
     * {@code closed.get()} re-check it breaks the loop, so the workers after the one observed-closed are
     * never spawned: GREEN. The witness asserts the late workers never ran.
     */
    @Test
    public void testConcurrentHaltStopsStartFromSpawningAgainstFreedResources() throws Exception {
        final int workerCount = 4;
        final WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public String getPoolName() {
                return "start-halt-race";
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public boolean isDaemonPool() {
                return true;
            }
        });

        // Every worker that actually starts records its worker id when its assigned job first runs. On
        // the fixed tree only the workers spawned before the in-lock closed re-check observed closed run;
        // on the un-fixed tree all four run despite the concurrent halt.
        final Set<Integer> startedWorkerIds = Collections.newSetFromMap(new ConcurrentHashMap<>());
        pool.assign((workerId, runStatus) -> {
            startedWorkerIds.add(workerId);
            return false;
        });

        // freeOnExit is what a late-spawned worker would loop on after halt frees it.
        final AtomicBoolean resourceFreed = new AtomicBoolean(false);
        pool.freeOnExit((Closeable) () -> resourceFreed.set(true));

        // Park start() inside the add-loop on the SECOND iteration: worker 0 has already been added and
        // started, and the monitor is held open while worker 1's add is pending. The concurrent halt
        // arrives in this window.
        final CountDownLatch startParkedInAdd = new CountDownLatch(1);
        final CountDownLatch releaseStartPark = new CountDownLatch(1);
        final AtomicLong seamInvocations = new AtomicLong();
        pool.setBeforeWorkerAddedForTesting(() -> {
            if (seamInvocations.getAndIncrement() == 1) {
                startParkedInAdd.countDown();
                try {
                    if (!releaseStartPark.await(20, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("add-loop park timed out waiting for release");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        final AtomicReference<Throwable> startError = new AtomicReference<>();
        final AtomicReference<Throwable> haltError = new AtomicReference<>();
        final CountDownLatch haltSetClosed = new CountDownLatch(1);
        final CountDownLatch haltReturned = new CountDownLatch(1);

        final Thread starter = new Thread(() -> {
            try {
                pool.start();
            } catch (Throwable t) {
                startError.set(t);
            }
        }, "start-halt-starter");
        starter.setDaemon(true);

        final Thread halter = new Thread(() -> {
            haltSetClosed.countDown();
            try {
                // halt(long) flips closed via a plain CAS (outside the monitor) immediately, then blocks
                // on the monitor the parked add holds. Once the park releases it proceeds to free
                // freeOnExit.
                pool.halt(TimeUnit.SECONDS.toNanos(10));
            } catch (Throwable t) {
                haltError.set(t);
            } finally {
                haltReturned.countDown();
            }
        }, "start-halt-halter");
        halter.setDaemon(true);

        try {
            starter.start();
            Assert.assertTrue("start() must park inside the add-loop holding the monitor",
                    startParkedInAdd.await(15, TimeUnit.SECONDS));

            // Wait until worker 0 (spawned before the park) is actually ticking, so the test exercises a
            // real running worker, not just an entry in the list.
            final long tickDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (startedWorkerIds.isEmpty() && System.nanoTime() < tickDeadline) {
                Thread.sleep(1);
            }
            Assert.assertTrue("worker 0 (spawned before the park) must be running", startedWorkerIds.contains(0));

            // Fire the concurrent halt while start() is parked mid-add. It sets closed immediately, then
            // blocks on the monitor held by the parked add.
            halter.start();
            Assert.assertTrue("halt thread must start", haltSetClosed.await(10, TimeUnit.SECONDS));
            // Give the halter time to flip closed and reach the monitor it must wait on.
            Thread.sleep(200);

            // Release the parked add: start() resumes INSIDE the monitor with closed already set by the
            // concurrent halt. With the in-lock re-check it breaks; without it, it spawns the remaining
            // workers against soon-to-be-freed resources.
            releaseStartPark.countDown();

            starter.join(TimeUnit.SECONDS.toMillis(15));
            Assert.assertTrue("halt() must return after the add critical section releases",
                    haltReturned.await(20, TimeUnit.SECONDS));
        } finally {
            releaseStartPark.countDown();
            starter.join(TimeUnit.SECONDS.toMillis(10));
            halter.join(TimeUnit.SECONDS.toMillis(10));
            pool.setBeforeWorkerAddedForTesting(null);
            pool.halt();
        }

        if (startError.get() != null) {
            throw new AssertionError("start() threw: "
                    + startError.get().getClass().getSimpleName() + ": " + startError.get().getMessage(),
                    startError.get());
        }
        if (haltError.get() != null) {
            throw new AssertionError("halt() threw: "
                    + haltError.get().getClass().getSimpleName() + ": " + haltError.get().getMessage(),
                    haltError.get());
        }

        Assert.assertTrue("halt() must free freeOnExit", resourceFreed.get());

        // The core safety assertion: once the concurrent halt has set closed, start()'s loop must break
        // before spawning any further worker. The remaining workers (1..N-1) would loop on the freeOnExit
        // resources halt frees -- a use-after-free plus orphan threads. On the fixed tree they are never
        // spawned; on the un-fixed tree start() spawns them all regardless of closed.
        for (int i = 1; i < workerCount; i++) {
            Assert.assertFalse(
                    "worker " + i + " must NOT have been spawned after the concurrent halt set closed "
                            + "(start() spawned it against soon-to-be-freed resources -- a use-after-free "
                            + "plus an orphan thread; the in-lock closed re-check is missing)",
                    startedWorkerIds.contains(i));
        }
    }

    /**
     * A stage-2 callback (onStableBelow) that throws must surface the failure
     * so run() throws LifecycleStartupException instead of succeeding with a
     * DEGRADED component.
     */
    @Test
    public void testStage2CallbackFailureSurfacedAsBootFailure() {
        // "wpm" mimics the worker-pool-manager envelope: publishes DEGRADED in start(),
        // then registers an onStableBelow callback that throws when all its dependents
        // are stable. "dep" mimics a protocol envelope that hard-deps on "wpm".
        final String marker = "worker-pool-start-failure";
        LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        orch.register(new ThrowingStage2Component("wpm", marker));
        ObjList<String> wpmDep = new ObjList<>();
        wpmDep.add("wpm");
        orch.register(new ProbeComponent("dep", wpmDep, new ObjList<>()));
        try {
            orch.run();
            Assert.fail("expected LifecycleStartupException; boot should fail when stage-2 callback throws");
        } catch (LifecycleStartupException e) {
            // Boot must fail loudly, not return a DEGRADED zombie.
            // The exception message or its cause should contain the marker.
            boolean causeMatches = e.getCause() != null
                    && e.getCause().getMessage() != null
                    && e.getCause().getMessage().contains(marker);
            boolean messageMatches = e.getMessage() != null && e.getMessage().contains(marker);
            Assert.assertTrue(
                    "LifecycleStartupException or its cause must reference the stage-2 failure marker; got: "
                            + e.getMessage() + ", cause: " + (e.getCause() != null ? e.getCause().getMessage() : "null"),
                    causeMatches || messageMatches
            );
        } finally {
            orch.close();
        }
    }

    /**
     * A healthy stage-2 callback (no throw) must still result in a successful boot.
     * The fix must not break the normal path.
     */
    @Test
    public void testHealthyStage2CallbackBootsSuccessfully() {
        LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        orch.register(new HealthyStage2Component("wpm"));
        ObjList<String> wpmDep = new ObjList<>();
        wpmDep.add("wpm");
        orch.register(new ProbeComponent("dep", wpmDep, new ObjList<>()));
        try {
            orch.run();
            // No exception -- healthy stage-2 callback should produce READY.
            Assert.assertEquals(State.READY, orch.stateOf("wpm"));
            Assert.assertEquals(State.READY, orch.stateOf("dep"));
        } finally {
            orch.close();
        }
    }

    /**
     * When start() stalls between running=true and started.countDown() (realistic on an OOM
     * mid-launch: a worker thread is already spawned and looping, but the start latch never
     * counts down), halt(long) must STILL signal worker.halt() for every worker before it clears
     * and frees freeOnExit. Otherwise a worker keeps looping on RUNNING against freed resources --
     * a use-after-free plus an orphan thread leak.
     */
    @Test
    public void testStartLatchTimeoutStillHaltsEveryWorker() throws Exception {
        final int workerCount = 2;
        final WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public String getPoolName() {
                return "halt-on-start-timeout";
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public boolean isDaemonPool() {
                return true;
            }
        });

        final AtomicLong jobTicks = new AtomicLong();
        pool.assign((workerId, runStatus) -> {
            jobTicks.incrementAndGet();
            return false;
        });

        // Track that freeOnExit is released by halt(): a worker still looping after halt against a
        // freed resource is the use-after-free this guards.
        final AtomicBoolean resourceFreed = new AtomicBoolean(false);
        pool.freeOnExit((Closeable) () -> resourceFreed.set(true));

        // Stall start() in the running=true / started-not-counted-down window: the workers are
        // already spawned and looping, but the start latch is held open until we release it.
        final CountDownLatch releaseStart = new CountDownLatch(1);
        final CountDownLatch startEntered = new CountDownLatch(1);
        pool.setBeforeStartedSignalForTesting(() -> {
            startEntered.countDown();
            try {
                releaseStart.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        final Thread starter = new Thread(pool::start, "pool-starter");
        starter.setDaemon(true);
        starter.start();
        try {
            // start() has entered the stall window; the worker threads are looping.
            Assert.assertTrue("start() must reach the pre-countDown stall window",
                    startEntered.await(10, TimeUnit.SECONDS));
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (jobTicks.get() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
            Assert.assertTrue("the workers must be running their assigned job", jobTicks.get() > 0);

            // halt(long) takes the start-latch-timeout branch (started never counted down). It must
            // still signal every worker, so the loops exit promptly.
            pool.halt(TimeUnit.MILLISECONDS.toNanos(200));

            Assert.assertTrue("halt() must free freeOnExit", resourceFreed.get());

            // After halt the workers must STOP ticking. Sample, wait well past the worker sleep
            // cadence, sample again: a halted worker leaves the count stable; an un-halted worker
            // keeps incrementing (the bug).
            final long afterHalt = jobTicks.get();
            Thread.sleep(300);
            final long settled = jobTicks.get();
            Assert.assertEquals(
                    "every worker must be halted on the start-latch-timeout branch; a still-ticking "
                            + "count means a worker is looping on RUNNING against freed resources",
                    afterHalt, settled);
        } finally {
            releaseStart.countDown();
            starter.join(TimeUnit.SECONDS.toMillis(10));
            pool.halt();
        }
    }

    /**
     * A SIGTERM-during-boot drives {@code halt()} concurrently with a still-running {@code start()}
     * that is mid-way through populating the workers list. {@code halt()}'s unconditional first pass
     * iterates that list ({@code size()}/{@code getQuick(i)}); if the list is read torn while
     * {@code start()}'s {@code workers.add(worker)} is mutating its non-volatile pos/buffer, the read
     * surfaces (under {@code -ea}) as an {@code AssertionError} ({@code assert index < pos}) or an NPE
     * ({@code getQuick} returns a null slot whose {@code halt()} is then called). That error escapes
     * {@code halt()} and {@code close()}; the JVM shutdown hook's {@code catch (Error)} does not catch
     * the NPE, so {@code freeOnExit.close()} is skipped and native handles leak.
     *
     * <p>The existing {@code beforeStartedSignalForTesting} seam fires AFTER the whole add-loop
     * (outside the monitor), so it cannot open this mid-add window. This test uses the
     * {@code beforeWorkerAddedForTesting} seam, which fires INSIDE the add-loop while the workersLock
     * is held, to hold the add critical section open and drive a concurrent {@code halt()}.
     *
     * <p>The witness is deterministic via an observable proxy for the torn read. The seam parks
     * {@code start()} mid-add-loop (holding the monitor on the fixed tree) AFTER worker 0 has been
     * added, started and is ticking; a second thread then calls {@code halt()}. {@code halt()}'s
     * unconditional first pass would signal worker 0 (stopping its ticks). On the fixed tree the first
     * pass must take the same monitor the parked add holds, so it is HELD OFF -- worker 0 keeps ticking
     * while {@code start()} is parked. On the un-fixed tree there is no monitor, so the first pass reads
     * the (partial) list immediately and signals worker 0 -- its ticks FREEZE while {@code start()} is
     * still parked. The witness asserts worker 0 keeps ticking while parked: GREEN on the fixed tree,
     * RED on the un-fixed tree (the un-guarded first pass read+signalled the half-built list).
     *
     * <p>After the parked add releases, the witness also asserts {@code halt()} threw nothing, the
     * worker added before the park ends up halted (the unconditional first-pass halt signal ran), and
     * {@code freeOnExit} was closed. The bounded halt is preserved -- the fix changes only the
     * publication of the list.
     */
    @Test
    public void testHaltDuringStartAddLoopIsHeldOffNotReadTorn() throws Exception {
        final int workerCount = 4;
        final WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public String getPoolName() {
                return "halt-during-add";
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public boolean isDaemonPool() {
                return true;
            }
        });
        // A simple ticking job lets the test confirm the worker added before the park was actually
        // halted (the unconditional first-pass halt signal ran), not merely cleared from the list.
        final AtomicLong jobTicks = new AtomicLong();
        pool.assign((workerId, runStatus) -> {
            jobTicks.incrementAndGet();
            return false;
        });

        final AtomicBoolean resourceFreed = new AtomicBoolean(false);
        pool.freeOnExit((Closeable) () -> resourceFreed.set(true));

        // Park start() inside the add-loop on the SECOND iteration, holding workersLock (fixed tree).
        // Parking on the second iteration means worker 0 has already been added, started and is ticking
        // -- so the halt-first-pass signal has something live to halt, and the monitor is held open for
        // worker 1's pending add when the concurrent halt() arrives.
        final CountDownLatch startParkedInAdd = new CountDownLatch(1);
        final CountDownLatch releaseStartPark = new CountDownLatch(1);
        final AtomicLong seamInvocations = new AtomicLong();
        pool.setBeforeWorkerAddedForTesting(() -> {
            if (seamInvocations.getAndIncrement() == 1) {
                startParkedInAdd.countDown();
                try {
                    if (!releaseStartPark.await(30, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("add-loop park timed out waiting for release");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        final AtomicReference<Throwable> startError = new AtomicReference<>();
        final AtomicReference<Throwable> haltError = new AtomicReference<>();
        final CountDownLatch haltStarted = new CountDownLatch(1);
        final CountDownLatch haltReturned = new CountDownLatch(1);

        final Thread starter = new Thread(() -> {
            try {
                pool.start();
            } catch (Throwable t) {
                startError.set(t);
            }
        }, "add-loop-starter");
        starter.setDaemon(true);

        final Thread halter = new Thread(() -> {
            haltStarted.countDown();
            try {
                pool.halt(TimeUnit.SECONDS.toNanos(10));
            } catch (Throwable t) {
                haltError.set(t);
            } finally {
                haltReturned.countDown();
            }
        }, "add-loop-halter");
        halter.setDaemon(true);

        boolean worker0KeptTickingWhileParked = false;
        try {
            starter.start();
            Assert.assertTrue("start() must park inside the add-loop holding the monitor",
                    startParkedInAdd.await(15, TimeUnit.SECONDS));

            // Worker 0 was added + started before the park; wait until it is actually ticking so the
            // "kept ticking while parked" signal is meaningful.
            final long tickDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (jobTicks.get() == 0 && System.nanoTime() < tickDeadline) {
                Thread.sleep(1);
            }
            Assert.assertTrue("worker 0 (added before the park) must be ticking", jobTicks.get() > 0);

            // Launch the concurrent halt() while start() is parked inside the add critical section.
            halter.start();
            Assert.assertTrue("halt thread must start", haltStarted.await(10, TimeUnit.SECONDS));

            // Observable proxy for the torn read: halt()'s unconditional first pass would signal worker 0
            // and stop its ticks. On the fixed tree that first pass blocks on workersLock (held by the
            // parked add), so worker 0 stays RUNNING and free-runs its no-work job loop (thousands of
            // ticks per 500ms). On the un-fixed tree the first pass reads the partial list immediately and
            // signals worker 0 -- it stops after at most one in-flight tick. A large delta means halt was
            // held off; a near-zero delta (<= a couple of in-flight ticks) means it signalled worker 0.
            final long ticksBefore = jobTicks.get();
            Thread.sleep(500);
            final long tickDelta = jobTicks.get() - ticksBefore;
            worker0KeptTickingWhileParked = tickDelta > 100;

            // Release the parked add: start() finishes the loop, the monitor frees, halt() proceeds.
            releaseStartPark.countDown();

            starter.join(TimeUnit.SECONDS.toMillis(15));
            Assert.assertTrue("halt() must return after the add critical section releases",
                    haltReturned.await(30, TimeUnit.SECONDS));
        } finally {
            releaseStartPark.countDown();
            starter.join(TimeUnit.SECONDS.toMillis(10));
            halter.join(TimeUnit.SECONDS.toMillis(10));
            pool.setBeforeWorkerAddedForTesting(null);
            pool.halt();
        }

        if (startError.get() != null) {
            throw new AssertionError("start() threw (torn workers list): "
                    + startError.get().getClass().getSimpleName() + ": " + startError.get().getMessage(),
                    startError.get());
        }
        if (haltError.get() != null) {
            throw new AssertionError("halt() threw reading the workers list torn while start() was "
                    + "mid-add (AssertionError/NPE escapes close() -> freeOnExit skipped -> native handle "
                    + "leak): " + haltError.get().getClass().getSimpleName() + ": "
                    + haltError.get().getMessage(), haltError.get());
        }

        // Core safe-publish assertion (deterministic): halt()'s first pass must be held off by the add
        // critical section's monitor while start() is parked mid-add -- it cannot read+signal the
        // half-built list, so worker 0 keeps ticking. On the un-fixed tree the first pass reads the
        // partial list and signals worker 0, freezing its ticks while still parked: RED.
        Assert.assertTrue("halt()'s first pass must be held off by the add critical section's monitor "
                        + "while start() is parked mid-add (worker 0's ticks froze -- the un-guarded first pass "
                        + "read+signalled the half-built workers list, the safe-publish is missing)",
                worker0KeptTickingWhileParked);

        // freeOnExit must be closed: an escaped torn-read error would have skipped it (native leak).
        Assert.assertTrue("halt() must free freeOnExit (an escaped torn-read error would skip it)",
                resourceFreed.get());

        // The first-pass signal ran: after the full halt the worker added before the park is
        // halted, so its tick count stays stable rather than climbing forever.
        final long afterHalt = jobTicks.get();
        Thread.sleep(200);
        Assert.assertEquals("the worker added before the park must have been halted (the unconditional "
                        + "first-pass halt signal ran before started.await); a climbing tick count means it was not",
                afterHalt, jobTicks.get());
    }

    /**
     * The {@code /metrics} scrape calls {@code updateWorkerMetrics()} on its own thread, unserialized
     * against {@code start()}'s add-loop and {@code halt()}'s clear(). With the workers-list iteration
     * left unguarded, a scrape that lands while {@code start()} is mid-add reads the list torn -- a null
     * slot ({@code getQuick(i)} returns null, then {@code getJobStartMicros()} NPEs) or a half-published
     * non-volatile pos/buffer.
     *
     * <p>The witness is deterministic via the same observable proxy the add-loop halt test uses. The
     * {@code beforeWorkerAddedForTesting} seam parks {@code start()} mid-add-loop holding {@code workersLock}
     * (worker 0 added, worker 1's add pending). A second thread then calls {@code updateWorkerMetrics()}.
     * On the fixed tree the scrape's iteration must take the same monitor the parked add holds, so it is
     * HELD OFF and does not return while {@code start()} is parked. On the un-fixed tree there is no
     * monitor, so the scrape reads the partial list and returns immediately. The witness asserts the scrape
     * is held off (does not return) while parked: GREEN on the fixed tree, RED on the un-fixed tree (the
     * scrape read the half-built workers list).
     */
    @Test
    public void testMetricsScrapeIsHeldOffNotReadTornDuringStartAddLoop() throws Exception {
        final int workerCount = 4;
        final WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public Metrics getMetrics() {
                return Metrics.ENABLED;
            }

            @Override
            public String getPoolName() {
                return "scrape-during-add";
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public boolean isDaemonPool() {
                return true;
            }
        });
        pool.assign((workerId, runStatus) -> false);

        // Park start() inside the add-loop on the SECOND iteration, holding workersLock, so worker 0 is
        // already added and worker 1's add is pending.
        final CountDownLatch startParkedInAdd = new CountDownLatch(1);
        final CountDownLatch releaseStartPark = new CountDownLatch(1);
        final AtomicLong seamInvocations = new AtomicLong();
        pool.setBeforeWorkerAddedForTesting(() -> {
            if (seamInvocations.getAndIncrement() == 1) {
                startParkedInAdd.countDown();
                try {
                    if (!releaseStartPark.await(30, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("add-loop park timed out waiting for release");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        final AtomicReference<Throwable> startError = new AtomicReference<>();
        final AtomicReference<Throwable> scrapeError = new AtomicReference<>();
        final CountDownLatch scrapeStarted = new CountDownLatch(1);
        final CountDownLatch scrapeReturned = new CountDownLatch(1);

        final Thread starter = new Thread(() -> {
            try {
                pool.start();
            } catch (Throwable t) {
                startError.set(t);
            }
        }, "scrape-add-starter");
        starter.setDaemon(true);

        final Thread scraper = new Thread(() -> {
            scrapeStarted.countDown();
            try {
                pool.updateWorkerMetrics(System.nanoTime() / 1000);
            } catch (Throwable t) {
                scrapeError.set(t);
            } finally {
                scrapeReturned.countDown();
            }
        }, "scrape-add-scraper");
        scraper.setDaemon(true);

        boolean scrapeHeldOffWhileParked = false;
        try {
            starter.start();
            Assert.assertTrue("start() must park inside the add-loop holding the monitor",
                    startParkedInAdd.await(15, TimeUnit.SECONDS));

            scraper.start();
            Assert.assertTrue("scrape thread must start", scrapeStarted.await(10, TimeUnit.SECONDS));

            // The scrape must not complete while start() holds the add critical section open: a return
            // means it iterated the half-built list (the un-fixed tree). Wait well past any plausible
            // uncontended scrape latency.
            scrapeHeldOffWhileParked = !scrapeReturned.await(500, TimeUnit.MILLISECONDS);

            releaseStartPark.countDown();
            starter.join(TimeUnit.SECONDS.toMillis(15));
            Assert.assertTrue("the scrape must complete once the add critical section releases",
                    scrapeReturned.await(15, TimeUnit.SECONDS));
        } finally {
            releaseStartPark.countDown();
            starter.join(TimeUnit.SECONDS.toMillis(10));
            scraper.join(TimeUnit.SECONDS.toMillis(10));
            pool.setBeforeWorkerAddedForTesting(null);
            pool.halt();
        }

        if (startError.get() != null) {
            throw new AssertionError("start() threw (torn workers list): "
                    + startError.get().getClass().getSimpleName() + ": " + startError.get().getMessage(),
                    startError.get());
        }
        if (scrapeError.get() != null) {
            throw new AssertionError("updateWorkerMetrics() threw reading the workers list torn while "
                    + "start() was mid-add (a null slot NPEs on getJobStartMicros): "
                    + scrapeError.get().getClass().getSimpleName() + ": " + scrapeError.get().getMessage(),
                    scrapeError.get());
        }

        Assert.assertTrue("the metrics scrape must be held off by the add critical section's monitor while "
                        + "start() is parked mid-add (it returned -- the un-guarded iteration read the "
                        + "half-built workers list)",
                scrapeHeldOffWhileParked);
    }

    // Component that mimics a two-stage start: publishes DEGRADED in start(),
    // registers an onStableBelow callback that throws the marker exception.
    private static final class ThrowingStage2Component implements Component {

        private final ObjList<String> hardDeps = new ObjList<>();
        private final String marker;
        private final String myName;
        private final ObjList<String> softDeps = new ObjList<>();

        ThrowingStage2Component(String name, String marker) {
            this.myName = name;
            this.marker = marker;
        }

        @Override
        public ObjList<String> hardRequiredDependencies() {
            return hardDeps;
        }

        @Override
        public String name() {
            return myName;
        }

        @Override
        public ObjList<String> softDependencies() {
            return softDeps;
        }

        @Override
        public void start(LifecycleContext ctx) {
            ctx.publish(State.DEGRADED);
            final String m = marker;
            ctx.onStableBelow(myName, () -> {
                throw new RuntimeException(m);
            });
        }

        @Override
        public void stop() {
        }
    }

    // Component that mimics a healthy two-stage start: publishes DEGRADED, then
    // registers a callback that publishes READY on success.
    private static final class HealthyStage2Component implements Component {

        private final ObjList<String> hardDeps = new ObjList<>();
        private final String myName;
        private final ObjList<String> softDeps = new ObjList<>();

        HealthyStage2Component(String name) {
            this.myName = name;
        }

        @Override
        public ObjList<String> hardRequiredDependencies() {
            return hardDeps;
        }

        @Override
        public String name() {
            return myName;
        }

        @Override
        public ObjList<String> softDependencies() {
            return softDeps;
        }

        @Override
        public void start(LifecycleContext ctx) {
            ctx.publish(State.DEGRADED);
            ctx.onStableBelow(myName, () -> ctx.publish(State.READY));
        }

        @Override
        public void stop() {
        }
    }
}
