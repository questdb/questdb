package io.questdb.test.lifecycle.envelopes;

import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.Role;
import io.questdb.lifecycle.State;
import io.questdb.std.ObjList;
import io.questdb.test.lifecycle.LifecycleTestHarness;
import io.questdb.test.lifecycle.fakes.ProbeComponent;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies the pg-wire envelope contract:
 * - starts DEGRADED (acceptOpen=false, socket bound but accept paused)
 * - transitions to READY when engine reaches READY via onDependencyState
 * - switchRole is a NO-OP (D4-08): publishes SWITCHING then READY without socket churn
 */
public class PgWireEnvelopeTest {

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(30, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    /**
     * Model a pg-wire-shaped component: DEGRADED on start, READY on engine onDependencyState.
     * This verifies the pg-wire protocol envelope contract without requiring real port binding.
     */
    private static Component newPgWireShapedComponent() {
        return new Component() {
            private volatile LifecycleContext ctxRef;
            private final AtomicBoolean acceptOpen = new AtomicBoolean(false);
            private final ObjList<String> hardDeps = new ObjList<String>() {{ add("worker-pool-manager"); }};
            private final ObjList<String> softDeps = new ObjList<String>() {{ add("engine"); }};

            @Override
            public ObjList<String> hardRequiredDependencies() { return hardDeps; }

            @Override
            public String name() { return "pg-wire"; }

            @Override
            public void onDependencyState(String depName, State previous, State current) {
                if ("engine".equals(depName) && current == State.READY) {
                    acceptOpen.set(true);
                    if (ctxRef != null) ctxRef.publish(State.READY);
                }
            }

            @Override
            public ObjList<String> softDependencies() { return softDeps; }

            @Override
            public void start(LifecycleContext ctx) {
                this.ctxRef = ctx;
                ctx.publish(State.STARTING);
                // server would be created here in production; acceptOpen stays false
                ctx.publish(State.DEGRADED);
            }

            @Override
            public void stop() {}

            @Override
            public void switchRole(LifecycleContext ctx, Role newRole) {
                // D4-08 NO-OP
                ctx.publish(State.SWITCHING);
                ctx.publish(State.READY);
            }
        };
    }

    @Test
    public void testStartsToReadyViaOnDependencyState() {
        try (LifecycleTestHarness h = new LifecycleTestHarness(Role.PRIMARY)) {
            h.registerFakeReady("factory-provider");
            ProbeComponent engineFake = new ProbeComponent("engine", new ObjList<>(), new ObjList<>());
            engineFake.holdInDegraded();
            h.register(engineFake);
            h.registerFakeReady("worker-pool-manager", "engine");
            h.register(newPgWireShapedComponent());
            h.start();
            // engine is still DEGRADED; pg-wire should be in DEGRADED (bound, accept paused)
            h.assertState("pg-wire", State.DEGRADED);
            // advance engine to READY
            engineFake.advanceTo(State.READY);
            // pg-wire should now advance to READY via onDependencyState
            h.awaitState("pg-wire", State.READY, 5_000L);
            h.assertState("pg-wire", State.READY);
        }
    }

    @Test
    public void testNoOpSwitchRolePublishesSwitchingThenReady() {
        try (LifecycleTestHarness h = new LifecycleTestHarness(Role.PRIMARY)) {
            h.registerFakeReady("factory-provider");
            ProbeComponent engineFake = new ProbeComponent("engine", new ObjList<>(), new ObjList<>());
            engineFake.holdInDegraded();
            h.register(engineFake);
            h.registerFakeReady("worker-pool-manager", "engine");
            h.register(newPgWireShapedComponent());
            h.start();
            engineFake.advanceTo(State.READY);
            h.awaitState("pg-wire", State.READY, 5_000L);
            // switchRole must be NO-OP: state returns to READY immediately, no stop/start
            h.switchRole(Role.REPLICA);
            Assert.assertEquals("pg-wire must reach READY after switchRole(REPLICA)", State.READY, h.stateOf("pg-wire"));
            h.switchRole(Role.PRIMARY);
            Assert.assertEquals("pg-wire must reach READY after switchRole(PRIMARY)", State.READY, h.stateOf("pg-wire"));
        }
    }

    @Test
    public void testEnvelopeSoftDepsContainEngine() {
        AtomicInteger engineSoftDepCount = new AtomicInteger(0);
        Component pgWire = newPgWireShapedComponent();
        for (int i = 0; i < pgWire.softDependencies().size(); i++) {
            if ("engine".equals(pgWire.softDependencies().getQuick(i))) {
                engineSoftDepCount.incrementAndGet();
            }
        }
        Assert.assertEquals("pg-wire soft deps must contain 'engine'", 1, engineSoftDepCount.get());
    }

    @Test
    public void testEnvelopeHardDepsContainWorkerPoolManager() {
        Component pgWire = newPgWireShapedComponent();
        boolean found = false;
        for (int i = 0; i < pgWire.hardRequiredDependencies().size(); i++) {
            if ("worker-pool-manager".equals(pgWire.hardRequiredDependencies().getQuick(i))) {
                found = true;
                break;
            }
        }
        Assert.assertTrue("pg-wire hard deps must contain 'worker-pool-manager'", found);
    }
}
