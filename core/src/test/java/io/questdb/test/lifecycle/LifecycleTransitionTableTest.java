package io.questdb.test.lifecycle;

import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.LifecycleOrchestrator;
import io.questdb.lifecycle.State;
import io.questdb.test.lifecycle.fakes.ProbeComponent;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * W1 fix -- table-driven coverage of the LIFE-05 transition matrix from
 * RESEARCH.md Pattern F. Cases live in {@link #data()} as a {@code Object[][]}
 * data set (NOT enumerated inline). Each row is {fromState, toState, expectedFinalState}.
 * If the orchestrator rejects the transition, expectedFinalState equals fromState.
 * If accepted, expectedFinalState equals toState.
 */
@RunWith(Parameterized.class)
public class LifecycleTransitionTableTest {

    @Rule
    public Timeout timeout = Timeout.builder().withTimeout(30, TimeUnit.SECONDS).build();

    private final State expectedFinal;
    private final State from;
    private final State to;

    public LifecycleTransitionTableTest(State from, State to, State expectedFinal) {
        this.from = from;
        this.to = to;
        this.expectedFinal = expectedFinal;
    }

    @Parameterized.Parameters(name = "{0} -> {1} expects {2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                // Valid transitions -- the key positive paths from Pattern F.
                {State.INIT, State.STARTING, State.STARTING},
                {State.STARTING, State.READY, State.READY},
                {State.STARTING, State.DEGRADED, State.DEGRADED},
                {State.STARTING, State.FAILED, State.FAILED},
                // CR-03: STARTING -> STOPPING is valid so close() can cancel an in-flight start
                // (e.g. SIGTERM during a long-running PITR restore). The component's stop()
                // path is the only place that runs signalRestoreCancel + awaitRestoreCancel.
                {State.STARTING, State.STOPPING, State.STOPPING},
                {State.DEGRADED, State.READY, State.READY},
                {State.DEGRADED, State.SWITCHING, State.SWITCHING},
                {State.DEGRADED, State.STOPPING, State.STOPPING},
                {State.READY, State.DEGRADED, State.DEGRADED},
                {State.READY, State.SWITCHING, State.SWITCHING},
                {State.READY, State.STOPPING, State.STOPPING},
                {State.READY, State.FAILED, State.FAILED},
                {State.SWITCHING, State.READY, State.READY},
                {State.SWITCHING, State.DEGRADED, State.DEGRADED},
                {State.SWITCHING, State.STOPPING, State.STOPPING},
                {State.STOPPING, State.STOPPED, State.STOPPED},

                // Invalid transitions -- the key negative paths. STOPPED and FAILED are terminal.
                {State.INIT, State.READY, State.INIT},        // INIT -> READY rejected (must go via STARTING)
                {State.INIT, State.DEGRADED, State.INIT},
                {State.INIT, State.STOPPING, State.INIT},
                {State.INIT, State.STOPPED, State.INIT},
                {State.INIT, State.SWITCHING, State.INIT},
                {State.STARTING, State.STOPPED, State.STARTING},    // STOPPED only reachable from STOPPING
                {State.STARTING, State.SWITCHING, State.STARTING},
                {State.READY, State.STOPPED, State.READY},
                {State.READY, State.STARTING, State.READY},       // no rewind
                {State.READY, State.INIT, State.READY},
                {State.DEGRADED, State.STARTING, State.DEGRADED},
                {State.DEGRADED, State.STOPPED, State.DEGRADED},
                {State.STOPPING, State.READY, State.STOPPING},
                {State.STOPPING, State.DEGRADED, State.STOPPING},
                {State.STOPPING, State.SWITCHING, State.STOPPING},
                {State.STOPPED, State.STARTING, State.STOPPED},     // STOPPED is terminal
                {State.STOPPED, State.READY, State.STOPPED},
                {State.STOPPED, State.FAILED, State.STOPPED},     // even FAILED rejected from STOPPED
                {State.FAILED, State.READY, State.FAILED},      // FAILED is terminal (D-11)
                {State.FAILED, State.STARTING, State.FAILED},
                {State.FAILED, State.STOPPING, State.FAILED},
                {State.FAILED, State.STOPPED, State.FAILED},
                {State.SWITCHING, State.STARTING, State.SWITCHING},
                {State.SWITCHING, State.INIT, State.SWITCHING},
        });
    }

    @Test
    public void testTransition() {
        LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        ProbeComponent p = new ProbeComponent("a");
        orch.register(p);
        LifecycleContext ctx = orch.contextFor("a");
        // Drive the component into the FROM state via the legal path.
        driveToState(ctx, from);
        Assert.assertEquals("setup: failed to drive to FROM=" + from, from, orch.stateOf("a"));
        // Attempt the TO transition; the orchestrator either accepts (state becomes TO) or rejects (state stays FROM).
        ctx.publish(to);
        Assert.assertEquals(expectedFinal, orch.stateOf("a"));
        orch.close();
    }

    private static void driveToState(LifecycleContext ctx, State target) {
        // INIT is the natural starting state; everything else needs a legal walk.
        switch (target) {
            case INIT:
                return;
            case STARTING:
                ctx.publish(State.STARTING);
                return;
            case READY:
                ctx.publish(State.STARTING);
                ctx.publish(State.READY);
                return;
            case DEGRADED:
                ctx.publish(State.STARTING);
                ctx.publish(State.DEGRADED);
                return;
            case SWITCHING:
                ctx.publish(State.STARTING);
                ctx.publish(State.READY);
                ctx.publish(State.SWITCHING);
                return;
            case STOPPING:
                ctx.publish(State.STARTING);
                ctx.publish(State.READY);
                ctx.publish(State.STOPPING);
                return;
            case STOPPED:
                ctx.publish(State.STARTING);
                ctx.publish(State.READY);
                ctx.publish(State.STOPPING);
                ctx.publish(State.STOPPED);
                return;
            case FAILED:
                ctx.publish(State.STARTING);
                ctx.publish(State.FAILED, "test-drive-to-failed");
                return;
        }
    }
}
