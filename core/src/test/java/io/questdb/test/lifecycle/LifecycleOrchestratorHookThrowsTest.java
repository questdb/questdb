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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Witnesses that {@code LifecycleOrchestrator.close()} survives a THROWING shutdown hook. Both
 * hook sites (the pre-join cancel hook, which fires before the boot-thread join, and the pre-stop
 * hook, which fires before the reverse-topo stop loop) wrap the callback in a suppressing
 * catch-all; without either catch a throwing hook aborts the whole shutdown at that point -- the
 * stop loop never runs, no component's stop() is called, and listening sockets / engine resources
 * leak to process exit.
 */
public class LifecycleOrchestratorHookThrowsTest {

    @Rule
    public Timeout timeout = Timeout.builder().withTimeout(60, TimeUnit.SECONDS).withLookingForStuckThread(true).build();

    @Test
    public void throwingShutdownHooksDoNotAbortTheStopLoop() throws Exception {
        final LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        final CountDownLatch started = new CountDownLatch(1);
        final AtomicBoolean stopCalled = new AtomicBoolean();
        final AtomicInteger hookRuns = new AtomicInteger();

        orch.register(new Component() {
            @Override
            public ObjList<String> hardRequiredDependencies() {
                return new ObjList<>();
            }

            @Override
            public String name() {
                return "hook-throws-probe";
            }

            @Override
            public ObjList<String> softDependencies() {
                return new ObjList<>();
            }

            @Override
            public void start(LifecycleContext ctx) {
                ctx.publish(State.READY);
                started.countDown();
            }

            @Override
            public void stop() {
                stopCalled.set(true);
            }
        });

        orch.setPreJoinCancelHook(() -> {
            hookRuns.incrementAndGet();
            throw new IllegalStateException("pre-join cancel hook deliberately throws");
        });
        orch.setPreStopHook(() -> {
            hookRuns.incrementAndGet();
            throw new IllegalStateException("pre-stop hook deliberately throws");
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
            Assert.assertTrue("component must reach READY before close()",
                    started.await(10, TimeUnit.SECONDS));

            // Contract under test: close() absorbs both hook throws and still runs the full
            // shutdown -- boot join, reverse-topo stop loop, executor shutdown.
            orch.close();

            boot.join(TimeUnit.SECONDS.toMillis(10));
            Assert.assertFalse("boot thread must unwind", boot.isAlive());
            Assert.assertEquals("both throwing hooks must have run", 2, hookRuns.get());
            Assert.assertTrue(
                    "the stop loop must still reach the component's stop() after the hooks threw",
                    stopCalled.get());
        } finally {
            // Failure hygiene: close() is CAS-guarded, joining a dead thread returns at once.
            orch.close();
            boot.join(TimeUnit.SECONDS.toMillis(10));
        }
        if (bootFailure.get() != null) {
            throw new AssertionError("boot thread failed: " + bootFailure.get(), bootFailure.get());
        }
    }
}
