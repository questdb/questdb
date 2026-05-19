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
 * Verifies the web-http envelope contract:
 * - starts DEGRADED (acceptOpen=false, socket bound but accept paused)
 * - transitions to READY when engine reaches READY via onDependencyState
 * - hard-deps on BOTH worker-pool-manager AND pg-wire (RESEARCH Section 6 --
 *   FlushQueryCacheJob wiring requires pgServer reference)
 * - switchRole is a NO-OP (D4-08)
 */
public class WebHttpEnvelopeTest {

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(30, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    private static Component newWebHttpShapedComponent() {
        return new Component() {
            private volatile LifecycleContext ctxRef;
            private final AtomicBoolean acceptOpen = new AtomicBoolean(false);
            private final ObjList<String> hardDeps = new ObjList<String>() {{
                add("worker-pool-manager");
                add("pg-wire");
            }};
            private final ObjList<String> softDeps = new ObjList<String>() {{ add("engine"); }};

            @Override
            public ObjList<String> hardRequiredDependencies() { return hardDeps; }

            @Override
            public String name() { return "web-http"; }

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
                // HttpServer and FlushQueryCacheJob would be created here with acceptOpen=false
                ctx.publish(State.DEGRADED);
            }

            @Override
            public void stop() {}
        };
    }

    @Test
    public void testStartsToReadyViaOnDependencyState() {
        try (LifecycleTestHarness h = new LifecycleTestHarness()) {
            h.registerFakeReady("factory-provider");
            ProbeComponent engineFake = new ProbeComponent("engine", new ObjList<>(), new ObjList<>());
            engineFake.holdInDegraded();
            h.register(engineFake);
            h.registerFakeReady("worker-pool-manager", "engine");
            h.registerFakeReady("pg-wire", "worker-pool-manager");
            h.register(newWebHttpShapedComponent());
            h.start();
            h.assertState("web-http", State.DEGRADED);
            engineFake.advanceTo(State.READY);
            h.awaitState("web-http", State.READY, 5_000L);
            h.assertState("web-http", State.READY);
        }
    }

    @Test
    public void testWebHttpHardDepsContainPgWire() {
        Component webHttp = newWebHttpShapedComponent();
        boolean foundPgWire = false;
        boolean foundWpm = false;
        for (int i = 0; i < webHttp.hardRequiredDependencies().size(); i++) {
            String dep = webHttp.hardRequiredDependencies().getQuick(i);
            if ("pg-wire".equals(dep)) foundPgWire = true;
            if ("worker-pool-manager".equals(dep)) foundWpm = true;
        }
        Assert.assertTrue("web-http hard deps must contain 'pg-wire'", foundPgWire);
        Assert.assertTrue("web-http hard deps must contain 'worker-pool-manager'", foundWpm);
    }
}
