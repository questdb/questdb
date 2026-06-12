/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.WorkerPoolManager;
import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.Services;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpReceiver;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.udp.AbstractLineProtoUdpReceiver;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleContext;
import io.questdb.mp.WorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Regression test for #090 (Phase 9 Cycle 2 Moderate).
 * <p>
 * Pre-fix HEAD: {@code IlpTcpEnvelope.start}, {@code MinHttpEnvelope.start}, and
 * {@code WebHttpEnvelope.start} bodies allocate multiple fields in sequence without
 * a wrapping try/catch. A mid-body throw leaves partially-allocated resources
 * (receivers, servers, worker pools) referenced by the envelope's private fields.
 * The orchestrator's close-loop then skips FAILED components, so those resources
 * leak.
 * <p>
 * After fix: each envelope's {@code start()} body wraps its multi-step allocation
 * sequence in a try/catch that frees partially-allocated resources before rethrowing,
 * mirroring the WR-09 addSuppressed pattern from {@code PrimaryRoleState.openLoops}.
 * <p>
 * The tests inject a {@link Services} subclass whose factory methods throw after the
 * first sibling has been constructed, then drive {@code envelope.start(ctx)} directly
 * and assert via reflection that the partially-allocated field is null (proof that
 * the catch path freed it).
 */
public class ServerMainProtocolEnvelopePartialInitTest extends AbstractBootstrapTest {

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(30, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration(
                PropertyKey.HTTP_ENABLED + "=true",
                PropertyKey.HTTP_MIN_ENABLED + "=true",
                PropertyKey.PG_ENABLED + "=false",
                PropertyKey.LINE_TCP_ENABLED + "=true",
                PropertyKey.LINE_UDP_ENABLED + "=true"
        ));
        dbPath.parent().$();
    }

    @Test
    public void partialInitFreesAllocatedResources_IlpTcp() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain server = new ServerMain(getServerMainArgs()) {
                @Override
                protected Services services() {
                    return new Services() {
                        @Override
                        public LineTcpReceiver createLineTcpReceiver(
                                LineTcpReceiverConfiguration config,
                                CairoEngine cairoEngine,
                                WorkerPoolManager workerPoolManager,
                                AtomicBoolean acceptOpen
                        ) {
                            // First step: succeed via real factory.
                            return Services.INSTANCE.createLineTcpReceiver(config, cairoEngine, workerPoolManager, acceptOpen);
                        }

                        @Override
                        public AbstractLineProtoUdpReceiver createLineUdpReceiver(
                                LineUdpReceiverConfiguration config,
                                CairoEngine cairoEngine,
                                WorkerPoolManager workerPoolManager,
                                AtomicBoolean acceptOpen
                        ) {
                            throw new RuntimeException("forced-mid-init-throw [ilp-tcp partial-init test]");
                        }
                    };
                }
            }) {
                server.testInitForEnvelopeTests();
                Component envelope = server.testNewIlpTcpEnvelope();
                LifecycleContext ctx = newDetachedContext(server, envelope.name());
                boolean threw = false;
                try {
                    envelope.start(ctx);
                } catch (Throwable t) {
                    threw = true;
                }
                Assert.assertTrue("ilp-tcp envelope.start() must propagate the forced throw", threw);
                Object leakedTcp = readField(envelope, "lineTcpReceiver");
                Assert.assertNull(
                        "lineTcpReceiver must be freed and nulled by the partial-init catch-rethrow "
                                + "(actual value: " + leakedTcp + ")",
                        leakedTcp
                );
            }
        });
    }

    @Test
    public void partialInitFreesAllocatedResources_MinHttp() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain server = new ServerMain(getServerMainArgs()) {
                @Override
                protected Services services() {
                    return new Services() {
                        @Override
                        public HttpServer createMinHttpServer(HttpServerConfiguration configuration, WorkerPool workerPool) {
                            // The MinHttpEnvelope.start() body has already allocated `pool` and
                            // assigned it to the envelope field. Throw here so the catch-rethrow
                            // path is exercised.
                            throw new RuntimeException("forced-mid-init-throw [min-http partial-init test]");
                        }
                    };
                }
            }) {
                server.testInitForEnvelopeTests();
                // The MinHttpEnvelope.start() driven via the orchestrator path normally consults
                // ServerMain.this.orchestrator via testInitForEnvelopeTests(); register a fresh
                // MinHttpEnvelope and drive start() directly so the throw fires.
                Component envelope = newMinHttpEnvelopeViaReflection(server);
                LifecycleContext ctx = newDetachedContext(server, envelope.name());
                boolean threw = false;
                try {
                    envelope.start(ctx);
                } catch (Throwable t) {
                    threw = true;
                }
                Assert.assertTrue("min-http envelope.start() must propagate the forced throw", threw);
                Object leakedPool = readField(envelope, "pool");
                Assert.assertNull(
                        "pool must be halted and nulled by the partial-init catch-rethrow "
                                + "(actual value: " + leakedPool + ")",
                        leakedPool
                );
            }
        });
    }

    @Test
    public void partialInitFreesAllocatedResources_WebHttp() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain server = new ServerMain(getServerMainArgs()) {
                @Override
                protected <T extends Component> T findEnvelope(String name, Class<T> type) {
                    // The WebHttpEnvelope.start() body has already allocated `server` via
                    // services().createHttpServer(...). Throw here at the cross-envelope lookup
                    // step so the catch-rethrow path is exercised AFTER server is non-null.
                    throw new RuntimeException("forced-mid-init-throw [web-http partial-init test]");
                }
            }) {
                server.testInitForEnvelopeTests();
                Component envelope = server.testNewWebHttpEnvelope();
                LifecycleContext ctx = newDetachedContext(server, envelope.name());
                boolean threw = false;
                try {
                    envelope.start(ctx);
                } catch (Throwable t) {
                    threw = true;
                }
                Assert.assertTrue("web-http envelope.start() must propagate the forced throw", threw);
                Object leakedServer = readField(envelope, "server");
                Assert.assertNull(
                        "server must be freed and nulled by the partial-init catch-rethrow "
                                + "(actual value: " + leakedServer + ")",
                        leakedServer
                );
            }
        });
    }

    /**
     * Build a fresh {@code MinHttpEnvelope} instance via reflection. There is no
     * {@code testNewMinHttpEnvelope()} factory on {@link ServerMain}; we construct
     * one directly through the inner class's hosting reference. The envelope captures
     * {@code ServerMain.this} so the overridden {@link Services#createMinHttpServer}
     * in the test subclass fires on start().
     */
    private static Component newMinHttpEnvelopeViaReflection(ServerMain server) throws Exception {
        // The inner class MinHttpEnvelope lives on ServerMain; use the published factory if it
        // exists, otherwise reflect through the inner-class ctor.
        try {
            // Prefer a published factory if one is added in a future plan; today we know none
            // exists for MinHttpEnvelope, so we go through the reflective path.
            return (Component) ServerMain.class.getMethod("testNewMinHttpEnvelope").invoke(server);
        } catch (NoSuchMethodException ignore) {
            Class<?> inner = Class.forName("io.questdb.ServerMain$MinHttpEnvelope");
            java.lang.reflect.Constructor<?> ctor = inner.getDeclaredConstructor(ServerMain.class, io.questdb.log.Log.class);
            ctor.setAccessible(true);
            return (Component) ctor.newInstance(server, io.questdb.log.LogFactory.getLog(ServerMainProtocolEnvelopePartialInitTest.class));
        }
    }

    /**
     * Mint a fresh {@link LifecycleContext} bound to {@code componentName} against the
     * test server's internal orchestrator (set up by {@code testInitForEnvelopeTests()}).
     */
    private static LifecycleContext newDetachedContext(ServerMain server, String componentName) {
        // Use the internal orchestrator's contextFor method. The component need not be
        // registered for publish() calls to silently no-op (unknown-component publishInternal
        // path).
        return server.getOrchestrator().contextFor(componentName);
    }

    private static Object readField(Object instance, String fieldName) throws Exception {
        Class<?> cls = instance.getClass();
        while (cls != null) {
            try {
                Field f = cls.getDeclaredField(fieldName);
                f.setAccessible(true);
                return f.get(instance);
            } catch (NoSuchFieldException ignored) {
                cls = cls.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName + " not found on " + instance.getClass().getName() + " or any superclass");
    }
}
