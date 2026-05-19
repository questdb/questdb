package io.questdb.test.lifecycle.envelopes;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.State;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.lifecycle.LifecycleTestHarness;
import io.questdb.test.lifecycle.fakes.SynchronousReadyEngine;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * CR-01 regression test: each of the four OSS protocol envelopes (pg-wire, ilp-tcp,
 * web-http, qwip) must reach {@link State#READY} when the engine publishes READY
 * synchronously inside its own start() body. This is the production timing of
 * {@code ServerMain.EngineEnvelope.start()} (lines 758-769): the orchestrator
 * dispatches the engine READY transition immediately, but dependent envelopes have
 * not yet captured their {@link io.questdb.lifecycle.LifecycleContext}, so their
 * {@code onDependencyState} callback misses the transition.
 * <p>
 * Without the D1 replay block at each protocol envelope's {@code start()} tail the
 * envelope stays at DEGRADED. With D1 the catch-up check
 * {@code ctx.state("engine") == READY} fires and the envelope reaches READY.
 * <p>
 * The tests drive the PRODUCTION envelopes via these factories (NOT shaped components)
 * so the regression catches removal of the replay block:
 * <ul>
 *   <li>{@code ServerMain.testNewIlpTcpEnvelope}</li>
 *   <li>{@code ServerMain.testNewPgWireEnvelope}</li>
 *   <li>{@code ServerMain.testNewQwipEnvelope}</li>
 *   <li>{@code ServerMain.testNewWebHttpEnvelope}</li>
 * </ul>
 * Earlier per-envelope tests use {@code ProbeComponent.holdInDegraded()} which masks the
 * race by deferring engine READY until after the envelope's start() returned -- this
 * test deliberately avoids that path.
 */
public class ProtocolEnvelopeEngineReadyRaceTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        // All four protocol envelopes (pg-wire, ilp-tcp, web-http, qwip) need their
        // server factories to short-circuit (createXxxServer returns null) so the envelope's
        // start() does not attempt to bind any real port. The disabled-by-extras switches
        // override the earlier enabled=true defaults written by createDummyConfiguration.
        TestUtils.unchecked(() -> createDummyConfiguration(
                PropertyKey.HTTP_ENABLED + "=false",
                PropertyKey.HTTP_MIN_ENABLED + "=false",
                PropertyKey.PG_ENABLED + "=false",
                PropertyKey.LINE_TCP_ENABLED + "=false",
                PropertyKey.QWP_UDP_ENABLED + "=false"
        ));
        dbPath.parent().$();
    }

    @Test
    public void ilpTcpReachesReadyWhenEnginePublishesReadySynchronously() throws Exception {
        try (final ServerMain server = new ServerMain(getServerMainArgs())) {
            server.testInitForEnvelopeTests();
            try (LifecycleTestHarness h = new LifecycleTestHarness()) {
                h.register(new SynchronousReadyEngine());
                h.registerFakeReady("worker-pool-manager", "engine");
                h.register(server.testNewIlpTcpEnvelope());
                h.start();
                h.assertState("ilp-tcp", State.READY);
            }
        }
    }

    @Test
    public void pgWireReachesReadyWhenEnginePublishesReadySynchronously() throws Exception {
        try (final ServerMain server = new ServerMain(getServerMainArgs())) {
            server.testInitForEnvelopeTests();
            try (LifecycleTestHarness h = new LifecycleTestHarness()) {
                h.register(new SynchronousReadyEngine());
                h.registerFakeReady("worker-pool-manager", "engine");
                h.register(server.testNewPgWireEnvelope());
                h.start();
                h.assertState("pg-wire", State.READY);
            }
        }
    }

    @Test
    public void qwipReachesReadyWhenEnginePublishesReadySynchronously() throws Exception {
        try (final ServerMain server = new ServerMain(getServerMainArgs())) {
            server.testInitForEnvelopeTests();
            try (LifecycleTestHarness h = new LifecycleTestHarness()) {
                h.register(new SynchronousReadyEngine());
                h.registerFakeReady("worker-pool-manager", "engine");
                final Component qwip = server.testNewQwipEnvelope();
                h.register(qwip);
                h.start();
                h.assertState("qwip", State.READY);
                // D4-10 primary-only semantics: on PRIMARY the accept loop opens on engine READY
                // catch-up. We assert the envelope reached READY here; the acceptOpen flag is an
                // internal field exercised by QwipEnvelopeTest / QwipEnvelopeSwitchRoleTest.
            }
        }
    }

    @Test
    public void webHttpReachesReadyWhenEnginePublishesReadySynchronously() throws Exception {
        try (final ServerMain server = new ServerMain(getServerMainArgs())) {
            server.testInitForEnvelopeTests();
            try (LifecycleTestHarness h = new LifecycleTestHarness()) {
                h.register(new SynchronousReadyEngine());
                h.registerFakeReady("worker-pool-manager", "engine");
                // web-http hard-deps on pg-wire (RESEARCH Section 6: FlushQueryCacheJob needs the
                // PGServer from PgWireEnvelope). The hard-dep is satisfied by a fake-ready stub in
                // the test orchestrator; the cross-envelope getPgServer() lookup goes through
                // ServerMain.findEnvelope() which consults the internal orchestrator stood up by
                // testInitForEnvelopeTests().
                h.registerFakeReady("pg-wire", "worker-pool-manager");
                h.register(server.testNewWebHttpEnvelope());
                h.start();
                h.assertState("web-http", State.READY);
            }
        }
    }
}
