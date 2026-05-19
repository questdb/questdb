package io.questdb.test.lifecycle.envelopes;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.Role;
import io.questdb.lifecycle.State;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.lifecycle.LifecycleTestHarness;
import io.questdb.test.lifecycle.fakes.SynchronousReadyEngine;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * D-07 / verification gap must-have #2: production protocol envelopes must tolerate a
 * stop() followed by a fresh start() on the same instance and reach
 * {@link State#READY} the second time. The orchestrator's
 * {@link io.questdb.lifecycle.Component#switchRole(LifecycleContext, io.questdb.lifecycle.Role)}
 * default contract is {@code stop(); start(ctx);}, so this round-trip is exercised on
 * every role transition.
 * <p>
 * Each test drives the PRODUCTION envelope via the {@code ServerMain.testNewXxxEnvelope}
 * factory (NOT a shaped component). After the first start() reaches READY against a
 * {@link SynchronousReadyEngine}, the test invokes {@code envelope.stop()} directly and
 * then {@code envelope.start(h.contextFor(name))} -- the new
 * {@link LifecycleTestHarness#contextFor(String)} seam mints a fresh
 * {@link io.questdb.lifecycle.LifecycleContext} bound to the same component name.
 */
public class ProtocolEnvelopeStopStartRoundTripTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
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
    public void ilpTcpStopStartRoundTripReachesReady() throws Exception {
        try (final ServerMain server = new ServerMain(getServerMainArgs())) {
            server.testInitForEnvelopeTests();
            try (LifecycleTestHarness h = new LifecycleTestHarness(Role.PRIMARY)) {
                h.register(new SynchronousReadyEngine());
                h.registerFakeReady("worker-pool-manager", "engine");
                Component ilpTcp = server.testNewIlpTcpEnvelope();
                h.register(ilpTcp);
                h.start();
                h.assertState("ilp-tcp", State.READY);
                ilpTcp.stop();
                LifecycleContext ctx = h.contextFor("ilp-tcp");
                ilpTcp.start(ctx);
                h.assertState("ilp-tcp", State.READY);
            }
        }
    }

    @Test
    public void pgWireStopStartRoundTripReachesReady() throws Exception {
        try (final ServerMain server = new ServerMain(getServerMainArgs())) {
            server.testInitForEnvelopeTests();
            try (LifecycleTestHarness h = new LifecycleTestHarness(Role.PRIMARY)) {
                h.register(new SynchronousReadyEngine());
                h.registerFakeReady("worker-pool-manager", "engine");
                Component pgWire = server.testNewPgWireEnvelope();
                h.register(pgWire);
                h.start();
                h.assertState("pg-wire", State.READY);
                pgWire.stop();
                LifecycleContext ctx = h.contextFor("pg-wire");
                pgWire.start(ctx);
                h.assertState("pg-wire", State.READY);
            }
        }
    }

    @Test
    public void qwipStopStartRoundTripReachesReady() throws Exception {
        try (final ServerMain server = new ServerMain(getServerMainArgs())) {
            server.testInitForEnvelopeTests();
            try (LifecycleTestHarness h = new LifecycleTestHarness(Role.PRIMARY)) {
                h.register(new SynchronousReadyEngine());
                h.registerFakeReady("worker-pool-manager", "engine");
                Component qwip = server.testNewQwipEnvelope();
                h.register(qwip);
                h.start();
                h.assertState("qwip", State.READY);
                qwip.stop();
                LifecycleContext ctx = h.contextFor("qwip");
                qwip.start(ctx);
                h.assertState("qwip", State.READY);
            }
        }
    }

    @Test
    public void webHttpStopStartRoundTripReachesReady() throws Exception {
        try (final ServerMain server = new ServerMain(getServerMainArgs())) {
            server.testInitForEnvelopeTests();
            try (LifecycleTestHarness h = new LifecycleTestHarness(Role.PRIMARY)) {
                h.register(new SynchronousReadyEngine());
                h.registerFakeReady("worker-pool-manager", "engine");
                h.registerFakeReady("pg-wire", "worker-pool-manager");
                Component webHttp = server.testNewWebHttpEnvelope();
                h.register(webHttp);
                h.start();
                h.assertState("web-http", State.READY);
                webHttp.stop();
                LifecycleContext ctx = h.contextFor("web-http");
                webHttp.start(ctx);
                h.assertState("web-http", State.READY);
            }
        }
    }
}
