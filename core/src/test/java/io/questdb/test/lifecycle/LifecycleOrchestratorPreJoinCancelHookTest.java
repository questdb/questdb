package io.questdb.test.lifecycle;

import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.LifecycleOrchestrator;
import io.questdb.lifecycle.State;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class LifecycleOrchestratorPreJoinCancelHookTest {

    @Rule
    public Timeout timeout = Timeout.builder().withTimeout(60, TimeUnit.SECONDS).withLookingForStuckThread(true).build();

    @Test
    public void preJoinCancelHookUnblocksInFlightStartBeforeBootJoin() throws Exception {
        // Models a SIGTERM during a long PITR restore: the boot thread is parked inside a
        // component's start() whose only shutdown observer is its own cancel flag. Without the
        // pre-join hook, close() burns the full 30s boot-join budget first, because the stop
        // loop (the only other cancel signaller) runs AFTER the join.
        final LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        final CountDownLatch startParked = new CountDownLatch(1);
        final CountDownLatch cancelSignalled = new CountDownLatch(1);
        final AtomicLong hookNanos = new AtomicLong(-1);
        final AtomicLong stopNanos = new AtomicLong(-1);

        orch.register(new Component() {
            @Override
            public ObjList<String> hardRequiredDependencies() {
                return new ObjList<>();
            }

            @Override
            public String name() {
                return "slow-restore";
            }

            @Override
            public ObjList<String> softDependencies() {
                return new ObjList<>();
            }

            @Override
            public void start(LifecycleContext ctx) {
                ctx.publish(State.STARTING);
                startParked.countDown();
                try {
                    if (!cancelSignalled.await(50, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("cancel was never signalled");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                ctx.publish(State.READY);
            }

            @Override
            public void stop() {
                stopNanos.compareAndSet(-1, System.nanoTime());
                // The legacy (too-late) signal path; must not be the one that unblocks start().
                cancelSignalled.countDown();
            }
        });

        orch.setPreJoinCancelHook(() -> {
            hookNanos.compareAndSet(-1, System.nanoTime());
            cancelSignalled.countDown();
        });

        final AtomicReference<Throwable> bootFailure = new AtomicReference<>();
        final Thread boot = new Thread(() -> {
            try {
                orch.run();
            } catch (Throwable t) {
                // A close() racing run() may legitimately surface a startup exception, but an
                // unexpected boot-thread failure must still fail the test loudly rather than
                // vanish -- capture it and rethrow after the joins below instead of discarding it.
                bootFailure.set(t);
            }
        }, "boot");
        boot.start();
        try {
            Assert.assertTrue("start() must be in flight before close()",
                    startParked.await(10, TimeUnit.SECONDS));

            final long closeStartNanos = System.nanoTime();
            orch.close();
            final long closeElapsedMs = (System.nanoTime() - closeStartNanos) / 1_000_000L;
            boot.join(TimeUnit.SECONDS.toMillis(10));
            Assert.assertFalse("boot thread must unwind", boot.isAlive());

            Assert.assertTrue("the pre-join cancel hook must run during close()", hookNanos.get() != -1);
            if (stopNanos.get() != -1) {
                Assert.assertTrue(
                        "the hook must fire before the reverse-topo stop loop reaches the component",
                        hookNanos.get() < stopNanos.get());
            }
            Assert.assertTrue(
                    "close() must not burn the 30s boot-join budget once the hook released the "
                            + "in-flight start() [elapsedMs=" + closeElapsedMs + ']',
                    closeElapsedMs < 15_000);
        } finally {
            // Failure hygiene: if an assertion above fired early, unblock and unwind the
            // non-daemon boot thread so it cannot stay parked in start() for its full 50s
            // await. All three calls are no-ops on the happy path: the latch countDown is
            // idempotent, close() is CAS-guarded, and joining a dead thread returns at once.
            cancelSignalled.countDown();
            orch.close();
            boot.join(TimeUnit.SECONDS.toMillis(10));
        }
        if (bootFailure.get() != null) {
            throw new AssertionError("boot thread failed: " + bootFailure.get(), bootFailure.get());
        }
    }

    @Test
    public void setPreJoinCancelHookNullUninstalls() {
        final LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        final AtomicInteger hookRuns = new AtomicInteger();
        orch.setPreJoinCancelHook(hookRuns::incrementAndGet);
        orch.setPreJoinCancelHook(null);
        Assert.assertNull(
                "a null hook must uninstall the previously installed one",
                orch.getPreJoinCancelHookForTest());
        orch.close();
        Assert.assertEquals(
                "the uninstalled hook must not run during close()",
                0, hookRuns.get());
    }
}
