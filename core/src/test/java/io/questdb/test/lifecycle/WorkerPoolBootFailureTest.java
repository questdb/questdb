package io.questdb.test.lifecycle;

import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.LifecycleOrchestrator;
import io.questdb.lifecycle.LifecycleStartupException;
import io.questdb.lifecycle.State;
import io.questdb.std.ObjList;
import io.questdb.test.lifecycle.fakes.ProbeComponent;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;

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
