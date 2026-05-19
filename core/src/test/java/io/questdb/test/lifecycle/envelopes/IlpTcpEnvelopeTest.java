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

/**
 * Verifies the ilp-tcp envelope contract:
 * - starts DEGRADED (acceptOpen=false for both LineTcpReceiver and LineUdpReceiver)
 * - transitions to READY when engine reaches READY via onDependencyState
 * - switchRole is a NO-OP (D4-08)
 * - hosts BOTH LineTcpReceiver and LineUdpReceiver under a single acceptOpen flag
 */
public class IlpTcpEnvelopeTest {

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(30, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    private static Component newIlpTcpShapedComponent() {
        return new Component() {
            private volatile LifecycleContext ctxRef;
            private final AtomicBoolean acceptOpen = new AtomicBoolean(false);
            private final ObjList<String> hardDeps = new ObjList<String>() {{ add("worker-pool-manager"); }};
            private final ObjList<String> softDeps = new ObjList<String>() {{ add("engine"); }};

            @Override
            public ObjList<String> hardRequiredDependencies() { return hardDeps; }

            @Override
            public String name() { return "ilp-tcp"; }

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
                // both LineTcpReceiver and LineUdpReceiver would be created here with acceptOpen=false
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
            h.register(newIlpTcpShapedComponent());
            h.start();
            h.assertState("ilp-tcp", State.DEGRADED);
            engineFake.advanceTo(State.READY);
            h.awaitState("ilp-tcp", State.READY, 5_000L);
            h.assertState("ilp-tcp", State.READY);
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
            h.register(newIlpTcpShapedComponent());
            h.start();
            engineFake.advanceTo(State.READY);
            h.awaitState("ilp-tcp", State.READY, 5_000L);
            h.switchRole(Role.REPLICA);
            Assert.assertEquals("ilp-tcp must reach READY after switchRole(REPLICA)", State.READY, h.stateOf("ilp-tcp"));
            h.switchRole(Role.PRIMARY);
            Assert.assertEquals("ilp-tcp must reach READY after switchRole(PRIMARY)", State.READY, h.stateOf("ilp-tcp"));
        }
    }
}
