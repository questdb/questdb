package io.questdb.test.lifecycle.envelopes;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.Role;
import io.questdb.lifecycle.State;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.lifecycle.LifecycleTestHarness;
import io.questdb.test.lifecycle.fakes.SynchronousReadyEngine;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;

/**
 * Drives the PRODUCTION {@code ServerMain.QwipEnvelope} (via {@link ServerMain#testNewQwipEnvelope})
 * through {@link LifecycleTestHarness} to verify D4-10 pause-the-job semantics and the D1 engine-READY
 * replay path. Supersedes the prior shaped-component implementation (04-C) which mirrored the
 * production switchRole body and therefore could not catch drift (CR-01 regressions in particular).
 * <p>
 * Coverage:
 * <ul>
 *   <li>{@code primaryBootCatchUpReachesReady} -- PRIMARY boot against {@link SynchronousReadyEngine}
 *       reaches READY and opens the accept loop via the D1 replay block (catch-up path).</li>
 *   <li>{@code qwipPausesOnPrimaryToReplica} -- PRIMARY -&gt; REPLICA flips {@code acceptOpen=false}.</li>
 *   <li>{@code qwipResumesOnReplicaToPrimary} -- REPLICA -&gt; PRIMARY flips {@code acceptOpen=true}.</li>
 *   <li>{@code receiverInstancePreservedAcrossSwitch} -- switchRole does not rebind {@code receiver}.</li>
 *   <li>{@code replicaBootCatchUpStaysAcceptPaused} -- REPLICA boot publishes READY but
 *       {@code acceptOpen} stays false (primary-only D4-10 semantics).</li>
 * </ul>
 */
public class QwipEnvelopeSwitchRoleTest extends AbstractBootstrapTest {

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
    public void primaryBootCatchUpReachesReady() throws Exception {
        try (final ServerMain server = new ServerMain(getServerMainArgs())) {
            server.testInitForEnvelopeTests();
            try (LifecycleTestHarness h = new LifecycleTestHarness(Role.PRIMARY)) {
                h.register(new SynchronousReadyEngine());
                h.registerFakeReady("worker-pool-manager", "engine");
                final Component qwip = server.testNewQwipEnvelope();
                h.register(qwip);
                h.start();
                h.assertState("qwip", State.READY);
                Assert.assertTrue(
                        "PRIMARY boot + engine READY catch-up must open the accept loop (D1 replay + D4-10)",
                        isAcceptOpen(qwip)
                );
            }
        }
    }

    @Test
    public void qwipPausesOnPrimaryToReplica() throws Exception {
        try (final ServerMain server = new ServerMain(getServerMainArgs())) {
            server.testInitForEnvelopeTests();
            try (LifecycleTestHarness h = new LifecycleTestHarness(Role.PRIMARY)) {
                h.register(new SynchronousReadyEngine());
                h.registerFakeReady("worker-pool-manager", "engine");
                final Component qwip = server.testNewQwipEnvelope();
                h.register(qwip);
                h.start();
                h.assertState("qwip", State.READY);
                Assert.assertTrue("accept loop must be open on PRIMARY boot", isAcceptOpen(qwip));

                final Object receiverBefore = getReceiver(qwip);

                h.switchRole(Role.REPLICA);

                Assert.assertEquals(State.READY, h.stateOf("qwip"));
                Assert.assertFalse(
                        "acceptOpen must be false after PRIMARY->REPLICA (D4-10)",
                        isAcceptOpen(qwip)
                );
                Assert.assertSame(
                        "switchRole must not rebind receiver",
                        receiverBefore,
                        getReceiver(qwip)
                );
            }
        }
    }

    @Test
    public void qwipResumesOnReplicaToPrimary() throws Exception {
        try (final ServerMain server = new ServerMain(getServerMainArgs())) {
            server.testInitForEnvelopeTests();
            try (LifecycleTestHarness h = new LifecycleTestHarness(Role.REPLICA)) {
                h.register(new SynchronousReadyEngine());
                h.registerFakeReady("worker-pool-manager", "engine");
                final Component qwip = server.testNewQwipEnvelope();
                h.register(qwip);
                h.start();
                h.assertState("qwip", State.READY);
                Assert.assertFalse(
                        "accept loop must stay paused on REPLICA boot (primary-only, D4-10)",
                        isAcceptOpen(qwip)
                );

                final Object receiverBefore = getReceiver(qwip);

                h.switchRole(Role.PRIMARY);

                Assert.assertEquals(State.READY, h.stateOf("qwip"));
                Assert.assertTrue(
                        "acceptOpen must be true after REPLICA->PRIMARY (D4-10)",
                        isAcceptOpen(qwip)
                );
                Assert.assertSame(
                        "switchRole must not rebind receiver",
                        receiverBefore,
                        getReceiver(qwip)
                );
            }
        }
    }

    @Test
    public void receiverInstancePreservedAcrossSwitch() throws Exception {
        try (final ServerMain server = new ServerMain(getServerMainArgs())) {
            server.testInitForEnvelopeTests();
            try (LifecycleTestHarness h = new LifecycleTestHarness(Role.PRIMARY)) {
                h.register(new SynchronousReadyEngine());
                h.registerFakeReady("worker-pool-manager", "engine");
                final Component qwip = server.testNewQwipEnvelope();
                h.register(qwip);
                h.start();
                h.assertState("qwip", State.READY);

                final Object receiverInitial = getReceiver(qwip);

                h.switchRole(Role.REPLICA);
                Assert.assertSame("receiver must survive PRIMARY->REPLICA", receiverInitial, getReceiver(qwip));

                h.switchRole(Role.PRIMARY);
                Assert.assertSame("receiver must survive REPLICA->PRIMARY", receiverInitial, getReceiver(qwip));

                h.switchRole(Role.REPLICA);
                Assert.assertSame("receiver must survive second PRIMARY->REPLICA", receiverInitial, getReceiver(qwip));
            }
        }
    }

    @Test
    public void replicaBootCatchUpStaysAcceptPaused() throws Exception {
        try (final ServerMain server = new ServerMain(getServerMainArgs())) {
            server.testInitForEnvelopeTests();
            try (LifecycleTestHarness h = new LifecycleTestHarness(Role.REPLICA)) {
                h.register(new SynchronousReadyEngine());
                h.registerFakeReady("worker-pool-manager", "engine");
                final Component qwip = server.testNewQwipEnvelope();
                h.register(qwip);
                h.start();
                h.assertState("qwip", State.READY);
                Assert.assertFalse(
                        "REPLICA + engine READY catch-up must publish READY but keep accept paused (D4-10)",
                        isAcceptOpen(qwip)
                );
            }
        }
    }

    /**
     * Reflective accessor for the private inner production class
     * {@code ServerMain$QwipEnvelope}. The class is private; the method is public and
     * {@code @TestOnly}. Reflection keeps this test from forcing the production class
     * to widen its visibility.
     */
    private static Object getReceiver(Component qwip) throws Exception {
        Method m = qwip.getClass().getDeclaredMethod("getReceiver");
        m.setAccessible(true);
        return m.invoke(qwip);
    }

    private static boolean isAcceptOpen(Component qwip) throws Exception {
        Method m = qwip.getClass().getDeclaredMethod("isAcceptOpen");
        m.setAccessible(true);
        return (boolean) m.invoke(qwip);
    }
}
