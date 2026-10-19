package io.questdb.test.lifecycle;

import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.LifecycleOrchestrator;
import io.questdb.lifecycle.LifecycleStartupException;
import io.questdb.lifecycle.State;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Regression test for #084 (Phase 9 Cycle 2 High; R3H5 + R4M2 corroborated).
 * <p>
 * Pre-fix HEAD: {@code LifecycleOrchestrator.startAllInTopologicalOrder} walks the
 * topo order without consulting the {@code closed} flag. A {@code close()} call
 * concurrent with an in-flight start would continue starting components in the loop
 * even after close had begun, racing the reverse-topo stop loop and leaving native
 * handles touched by an executor thread after the owning component had been freed.
 * <p>
 * After fix: the per-iteration {@code if (closed.get()) return;} short-circuit at the
 * top of {@code startAllInTopologicalOrder}'s loop body causes the loop to exit as
 * soon as {@code close()} flips the flag, so the component that was just inside
 * {@code start()} can finish but no further component is touched.
 */
public class LifecycleOrchestratorCloseRaceTest {

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(30, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Test
    public void closeShortCircuitsStartAllInTopologicalOrder() throws Exception {
        // Topology: A (no deps), B (hard-dep A).
        // A's start() blocks on a CountDownLatch until the test releases it AFTER calling close()
        // on another thread. Once A returns, the loop's next iteration MUST observe closed=true
        // and skip B entirely.
        final CountDownLatch aStartedEntered = new CountDownLatch(1);
        final CountDownLatch aReleaseLatch = new CountDownLatch(1);
        final AtomicBoolean bStartCalled = new AtomicBoolean(false);

        final Component a = new Component() {
            private final ObjList<String> empty = new ObjList<>();

            @Override
            public ObjList<String> hardRequiredDependencies() {
                return empty;
            }

            @Override
            public String name() {
                return "a";
            }

            @Override
            public ObjList<String> softDependencies() {
                return empty;
            }

            @Override
            public void start(LifecycleContext ctx) {
                aStartedEntered.countDown();
                try {
                    aReleaseLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void stop() {
            }
        };

        final ObjList<String> bHardDeps = new ObjList<>();
        bHardDeps.add("a");
        final Component b = new Component() {
            private final ObjList<String> empty = new ObjList<>();

            @Override
            public ObjList<String> hardRequiredDependencies() {
                return bHardDeps;
            }

            @Override
            public String name() {
                return "b";
            }

            @Override
            public ObjList<String> softDependencies() {
                return empty;
            }

            @Override
            public void start(LifecycleContext ctx) {
                bStartCalled.set(true);
            }

            @Override
            public void stop() {
            }
        };

        final ObservableOrchestrator orch = new ObservableOrchestrator();
        orch.register(a);
        orch.register(b);

        final Thread runner = new Thread(() -> {
            try {
                orch.run();
            } catch (LifecycleStartupException ignore) {
                // close() before completion means boot-essential anyFailedReachable may flip;
                // either way the close-short-circuit assertion below carries the test.
            }
        }, "orch-runner");
        runner.start();
        try {
            Assert.assertTrue(
                    "A.start() did not enter within 10s -- runner did not schedule",
                    aStartedEntered.await(10, TimeUnit.SECONDS)
            );
            Assert.assertEquals(State.STARTING, orch.stateOf("a"));
            // A must not unblock before closed=true is observable, or the race window is missed
            final Thread closer = new Thread(orch::close, "orch-closer");
            closer.start();
            final long closedDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!orch.isClosed() && System.nanoTime() < closedDeadline) {
                Thread.sleep(1);
            }
            Assert.assertTrue("close() did not flip the closed flag", orch.isClosed());
            aReleaseLatch.countDown();
            runner.join(10_000L);
            closer.join(10_000L);
            Assert.assertFalse(
                    "B.start() must NOT be called once close() races startAllInTopologicalOrder",
                    bStartCalled.get()
            );
        } finally {
            aReleaseLatch.countDown();
            orch.close();
        }
    }

    private static class ObservableOrchestrator extends LifecycleOrchestrator {
        ObservableOrchestrator() {
            super(null, null, null);
        }

        @Override
        public boolean isClosed() {
            return super.isClosed();
        }
    }
}
