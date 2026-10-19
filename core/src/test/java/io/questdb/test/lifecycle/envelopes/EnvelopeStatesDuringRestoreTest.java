package io.questdb.test.lifecycle.envelopes;

import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleContext;
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
 * OBS-03 regression test: verifies that protocol envelopes reach DEGRADED (socket bound,
 * accept paused) BEFORE the engine reaches READY. This models the scenario where the engine
 * is held in DEGRADED (e.g., during a PITR restore) and the protocol sockets should already
 * be listening.
 * <p>
 * This is the in-process test analog of the smoke harness's OBS-03 verification
 * (ss -tlnp shows listening sockets before the engine is READY).
 */
public class EnvelopeStatesDuringRestoreTest {

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(30, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    private static Component newProtocolEnvelopeContract(String name, String[] additionalHardDeps) {
        return new Component() {
            private volatile LifecycleContext ctxRef;
            private final AtomicBoolean acceptOpen = new AtomicBoolean(false);
            private final ObjList<String> hardDeps;
            private final ObjList<String> softDeps = new ObjList<String>() {{
                add("engine");
            }};

            {
                hardDeps = new ObjList<>();
                hardDeps.add("worker-pool-manager");
                for (String d : additionalHardDeps) {
                    hardDeps.add(d);
                }
            }

            @Override
            public ObjList<String> hardRequiredDependencies() {
                return hardDeps;
            }

            @Override
            public String name() {
                return name;
            }

            @Override
            public void onDependencyState(String depName, State previous, State current) {
                if ("engine".equals(depName) && current == State.READY) {
                    acceptOpen.set(true);
                    if (ctxRef != null) ctxRef.publish(State.READY);
                }
            }

            @Override
            public ObjList<String> softDependencies() {
                return softDeps;
            }

            @Override
            public void start(LifecycleContext ctx) {
                this.ctxRef = ctx;
                ctx.publish(State.STARTING);
                // D4-05: socket is bound here (before engine READY)
                // acceptOpen stays false until onDependencyState("engine", READY) fires
                ctx.publish(State.DEGRADED);
            }

            @Override
            public void stop() {
            }
        };
    }

    @Test
    public void testSocketBoundDuringDegraded() {
        // Model: engine held in DEGRADED (simulating PITR restore).
        // All 4 protocol envelopes should reach DEGRADED (socket bound) while engine is still DEGRADED.
        try (LifecycleTestHarness h = new LifecycleTestHarness()) {
            h.registerFakeReady("factory-provider");

            // Engine is held in DEGRADED -- simulates a long-running PITR restore.
            ProbeComponent engineFake = new ProbeComponent("engine", new ObjList<>(), new ObjList<>());
            engineFake.holdInDegraded();
            h.register(engineFake);

            h.registerFakeReady("worker-pool-manager", "engine");

            Component pgWire = newProtocolEnvelopeContract("pg-wire", new String[0]);
            Component ilpTcp = newProtocolEnvelopeContract("ilp-tcp", new String[0]);
            Component webHttp = newProtocolEnvelopeContract("web-http", new String[]{"pg-wire"});
            Component qwip = newProtocolEnvelopeContract("qwip", new String[0]);
            h.register(pgWire, ilpTcp, webHttp, qwip);

            h.start();

            // OBS-03: all 4 protocol envelopes must be DEGRADED (socket bound, accept paused)
            // while engine is still in DEGRADED (simulating active restore).
            Assert.assertEquals(State.DEGRADED, h.stateOf("engine"));
            h.assertState("pg-wire", State.DEGRADED);
            h.assertState("ilp-tcp", State.DEGRADED);
            h.assertState("web-http", State.DEGRADED);
            h.assertState("qwip", State.DEGRADED);

            // Now advance engine to READY (restore completes).
            engineFake.advanceTo(State.READY);

            // All protocol envelopes must now advance to READY.
            h.awaitState("pg-wire", State.READY, 5_000L);
            h.awaitState("ilp-tcp", State.READY, 5_000L);
            h.awaitState("web-http", State.READY, 5_000L);
            h.awaitState("qwip", State.READY, 5_000L);
        }
    }

    @Test
    public void testEngineReachesReadyAfterAllProtocolsDegraded() {
        // Verify ordering: all protocol envelopes reach DEGRADED (startSeq recorded)
        // before engine.advanceTo(READY) is called. This proves the early-bind contract (D4-05).
        try (LifecycleTestHarness h = new LifecycleTestHarness()) {
            h.registerFakeReady("factory-provider");

            ProbeComponent engineFake = new ProbeComponent("engine", new ObjList<>(), new ObjList<>());
            engineFake.holdInDegraded();
            h.register(engineFake);

            ProbeComponent wpmFake = new ProbeComponent("worker-pool-manager",
                    new ObjList<String>() {{
                        add("engine");
                    }}, new ObjList<>());
            h.register(wpmFake);

            Component pgWire = newProtocolEnvelopeContract("pg-wire", new String[0]);
            Component ilpTcp = newProtocolEnvelopeContract("ilp-tcp", new String[0]);
            Component webHttp = newProtocolEnvelopeContract("web-http", new String[]{"pg-wire"});
            Component qwip = newProtocolEnvelopeContract("qwip", new String[0]);
            h.register(pgWire, ilpTcp, webHttp, qwip);

            h.start();

            // All protocol envelopes are DEGRADED (D4-05: sockets bound) while engine is still DEGRADED.
            Assert.assertEquals(State.DEGRADED, h.stateOf("engine"));
            Assert.assertEquals(State.DEGRADED, h.stateOf("pg-wire"));
            Assert.assertEquals(State.DEGRADED, h.stateOf("ilp-tcp"));
            Assert.assertEquals(State.DEGRADED, h.stateOf("web-http"));
            Assert.assertEquals(State.DEGRADED, h.stateOf("qwip"));

            // Engine advances to READY only after all protocol envelopes have started.
            engineFake.advanceTo(State.READY);

            // All protocol envelopes transition DEGRADED -> READY.
            h.awaitState("pg-wire", State.READY, 5_000L);
            h.awaitState("ilp-tcp", State.READY, 5_000L);
            h.awaitState("web-http", State.READY, 5_000L);
            h.awaitState("qwip", State.READY, 5_000L);
            h.awaitState("engine", State.READY, 5_000L);
        }
    }
}
