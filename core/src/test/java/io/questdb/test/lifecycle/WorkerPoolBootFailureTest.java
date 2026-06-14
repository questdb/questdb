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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
