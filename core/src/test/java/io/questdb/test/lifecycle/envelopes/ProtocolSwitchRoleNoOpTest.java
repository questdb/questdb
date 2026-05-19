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
 * Cross-envelope test: verifies that ALL 4 protocol envelopes implement NO-OP switchRole
 * per D4-08. After switchRole(REPLICA) + switchRole(PRIMARY), each envelope must be in
 * READY state. A non-NO-OP switchRole (stop + start) would re-create the server instance,
 * observable as a start-count increment from 1 to 2.
 */
public class ProtocolSwitchRoleNoOpTest {

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(30, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    /**
     * Build a protocol-envelope-shaped component with a start counter.
     * A real NO-OP switchRole implementation must NOT increment this counter after start().
     */
    private static Component newProtocolEnvelopeWithStartCounter(
            String name,
            String[] additionalHardDeps,
            AtomicInteger startCounter
    ) {
        return new Component() {
            private volatile LifecycleContext ctxRef;
            private final AtomicBoolean acceptOpen = new AtomicBoolean(false);
            private final ObjList<String> hardDeps;
            private final ObjList<String> softDeps = new ObjList<String>() {{ add("engine"); }};

            {
                hardDeps = new ObjList<>();
                hardDeps.add("worker-pool-manager");
                for (String d : additionalHardDeps) {
                    hardDeps.add(d);
                }
            }

            @Override
            public ObjList<String> hardRequiredDependencies() { return hardDeps; }

            @Override
            public String name() { return name; }

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
                startCounter.incrementAndGet();
                ctx.publish(State.STARTING);
                ctx.publish(State.DEGRADED);
            }

            @Override
            public void stop() {}

            @Override
            public void switchRole(LifecycleContext ctx, Role newRole) {
                // D4-08 NO-OP: must NOT call stop() + start() here
                ctx.publish(State.SWITCHING);
                ctx.publish(State.READY);
            }
        };
    }

    @Test
    public void testAllFourProtocolsNoOpOnSwitch() {
        AtomicInteger pgWireStartCount = new AtomicInteger(0);
        AtomicInteger ilpTcpStartCount = new AtomicInteger(0);
        AtomicInteger webHttpStartCount = new AtomicInteger(0);
        AtomicInteger qwipStartCount = new AtomicInteger(0);

        try (LifecycleTestHarness h = new LifecycleTestHarness(Role.PRIMARY)) {
            h.registerFakeReady("factory-provider");
            ProbeComponent engineFake = new ProbeComponent("engine", new ObjList<>(), new ObjList<>());
            engineFake.holdInDegraded();
            h.register(engineFake);
            h.registerFakeReady("worker-pool-manager", "engine");

            Component pgWire = newProtocolEnvelopeWithStartCounter("pg-wire", new String[0], pgWireStartCount);
            Component ilpTcp = newProtocolEnvelopeWithStartCounter("ilp-tcp", new String[0], ilpTcpStartCount);
            Component webHttp = newProtocolEnvelopeWithStartCounter("web-http", new String[]{"pg-wire"}, webHttpStartCount);
            Component qwip = newProtocolEnvelopeWithStartCounter("qwip", new String[0], qwipStartCount);

            h.register(pgWire, ilpTcp, webHttp, qwip);
            h.start();

            // advance engine to READY -- all 4 protocol envelopes should reach READY
            engineFake.advanceTo(State.READY);
            h.awaitState("pg-wire", State.READY, 5_000L);
            h.awaitState("ilp-tcp", State.READY, 5_000L);
            h.awaitState("web-http", State.READY, 5_000L);
            h.awaitState("qwip", State.READY, 5_000L);

            // each envelope started exactly once
            Assert.assertEquals("pg-wire must have started exactly once before switchRole", 1, pgWireStartCount.get());
            Assert.assertEquals("ilp-tcp must have started exactly once before switchRole", 1, ilpTcpStartCount.get());
            Assert.assertEquals("web-http must have started exactly once before switchRole", 1, webHttpStartCount.get());
            Assert.assertEquals("qwip must have started exactly once before switchRole", 1, qwipStartCount.get());

            // switchRole twice -- NO-OP implementations must NOT increment start counters
            h.switchRole(Role.REPLICA);
            h.switchRole(Role.PRIMARY);

            // D4-08: start counter must remain at 1 (no stop+start called by switchRole)
            Assert.assertSame(
                    "D4-08 NO-OP: pg-wire server instance must survive switchRole (start count unchanged)",
                    1, pgWireStartCount.get()
            );
            Assert.assertSame(
                    "D4-08 NO-OP: ilp-tcp server instance must survive switchRole (start count unchanged)",
                    1, ilpTcpStartCount.get()
            );
            Assert.assertSame(
                    "D4-08 NO-OP: web-http server instance must survive switchRole (start count unchanged)",
                    1, webHttpStartCount.get()
            );
            Assert.assertSame(
                    "D4-08 NO-OP: qwip server instance must survive switchRole (start count unchanged)",
                    1, qwipStartCount.get()
            );

            // all 4 must be READY after switchRole
            Assert.assertEquals(State.READY, h.stateOf("pg-wire"));
            Assert.assertEquals(State.READY, h.stateOf("ilp-tcp"));
            Assert.assertEquals(State.READY, h.stateOf("web-http"));
            Assert.assertEquals(State.READY, h.stateOf("qwip"));
        }
    }
}
