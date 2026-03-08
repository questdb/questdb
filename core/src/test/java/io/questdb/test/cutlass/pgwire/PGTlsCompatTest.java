/*******************************************************************************
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

package io.questdb.test.cutlass.pgwire;

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.log.Log;
import io.questdb.mp.WorkerPool;
import io.questdb.network.NetworkFacade;
import io.questdb.network.PlainSocket;
import io.questdb.network.Socket;
import io.questdb.network.SocketFactory;
import io.questdb.network.TlsSessionInitFailedException;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class PGTlsCompatTest extends BasePGTest {

    @Test
    public void testTlsSessionGetsCreatedWhenSocketSupportsTls() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger createTlsSessionCalls = new AtomicInteger();
            final AtomicInteger tlsIOCalls = new AtomicInteger();

            final PGConfiguration conf = new Port0PGConfiguration() {
                @Override
                public FactoryProvider getFactoryProvider() {
                    return new DefaultFactoryProvider() {
                        @Override
                        public @NotNull SocketFactory getPGWireSocketFactory() {
                            return new FakeTlsSocketFactory(true, createTlsSessionCalls, tlsIOCalls);
                        }
                    };
                }
            };

            final WorkerPool workerPool = new TestWorkerPool(1, conf.getMetrics());
            try (final PGServer server = createPGWireServer(conf, engine, workerPool)) {
                Assert.assertNotNull(server);

                workerPool.start(LOG);

                Properties properties = newPGProperties();
                properties.setProperty("sslmode", "require");

                final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", server.getPort());
                try (Connection ignore = DriverManager.getConnection(url, properties)) {
                    Assert.fail();
                } catch (PSQLException ignore) {
                } finally {
                    workerPool.halt();
                }

                Assert.assertTrue("Some create TLS session calls expected: " + createTlsSessionCalls.get(), createTlsSessionCalls.get() > 0);
                Assert.assertTrue("Some TLS I/O calls expected: " + tlsIOCalls.get(), tlsIOCalls.get() > 0);
            }
        });
    }

    @Test
    public void testTlsSessionIsNotCreatedWhenSocketDoesNotSupportTls() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger createTlsSessionCalls = new AtomicInteger();
            final AtomicInteger tlsIOCalls = new AtomicInteger();

            final PGConfiguration conf = new Port0PGConfiguration() {
                @Override
                public FactoryProvider getFactoryProvider() {
                    return new DefaultFactoryProvider() {
                        @Override
                        public @NotNull SocketFactory getPGWireSocketFactory() {
                            return new FakeTlsSocketFactory(false, createTlsSessionCalls, tlsIOCalls);
                        }
                    };
                }
            };

            final WorkerPool workerPool = new TestWorkerPool(1, conf.getMetrics());
            try (final PGServer server = createPGWireServer(conf, engine, workerPool)) {
                Assert.assertNotNull(server);

                workerPool.start(LOG);

                Properties properties = newPGProperties();

                final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", server.getPort());
                try (Connection ignore = DriverManager.getConnection(url, properties)) {
                    Assert.fail();
                } catch (PSQLException ignore) {
                } finally {
                    workerPool.halt();
                }

                Assert.assertEquals("No create TLS session calls expected: " + createTlsSessionCalls.get(), 0, createTlsSessionCalls.get());
                Assert.assertEquals("No TLS I/O calls expected: " + tlsIOCalls.get(), 0, tlsIOCalls.get());
            }
        });
    }

    @Test
    public void testTlsSessionRequestErrors() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger createTlsSessionCalls = new AtomicInteger();
            final AtomicInteger tlsIOCalls = new AtomicInteger();

            final PGConfiguration conf = new Port0PGConfiguration() {
                @Override
                public FactoryProvider getFactoryProvider() {
                    return new DefaultFactoryProvider() {
                        @Override
                        public @NotNull SocketFactory getPGWireSocketFactory() {
                            return new FakeTlsSocketFactory(true, createTlsSessionCalls, tlsIOCalls);
                        }
                    };
                }

                @Override
                public int getForceSendFragmentationChunkSize() {
                    return 2; // force fragmentation and PeerIsSlowToReadException
                }
            };

            final int N = 10;
            final WorkerPool workerPool = new TestWorkerPool(1, conf.getMetrics());
            try (final PGServer server = createPGWireServer(conf, engine, workerPool)) {
                Assert.assertNotNull(server);
                final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", server.getPort());

                workerPool.start(LOG);

                Properties properties = newPGProperties();
                properties.setProperty("sslmode", "allow");
                // unencrypted connection
                try {
                    for (int i = 0; i < N; i++) {
                        try (Connection ignore = DriverManager.getConnection(url, properties)) {
                            Assert.fail();
                        } catch (PSQLException ex) {
                            TestUtils.assertContains(ex.getMessage(), "ERROR: request SSL message expected");
                        }

                        Assert.assertEquals("No create TLS session calls expected: " + createTlsSessionCalls.get(), 0, createTlsSessionCalls.get());
                        Assert.assertEquals("No TLS I/O calls expected: " + tlsIOCalls.get(), 0, tlsIOCalls.get());
                    }
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    @NotNull
    private static Properties newPGProperties() {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "require");
        properties.setProperty("binaryTransfer", "true");
        return properties;
    }

    private static class FakeTlsSocket extends PlainSocket {
        private final AtomicInteger createTlsSessionCalls;
        private final AtomicInteger tlsIOCalls;
        private final boolean tlsSupported;
        private boolean tlsSessionStarted;

        public FakeTlsSocket(NetworkFacade nf, Log log, boolean tlsSupported, AtomicInteger createTlsSessionCalls, AtomicInteger tlsIOCalls) {
            super(nf, log);
            this.tlsSupported = tlsSupported;
            this.createTlsSessionCalls = createTlsSessionCalls;
            this.tlsIOCalls = tlsIOCalls;
        }

        @Override
        public void close() {
            super.close();
            tlsSessionStarted = false;
        }

        @Override
        public boolean isTlsSessionStarted() {
            return tlsSessionStarted;
        }

        @Override
        public void startTlsSession(CharSequence peerName) throws TlsSessionInitFailedException {
            if (!tlsSessionStarted) {
                createTlsSessionCalls.incrementAndGet();
                tlsSessionStarted = true;
                return;
            }
            throw TlsSessionInitFailedException.instance("TLS session has been started already");
        }

        @Override
        public boolean supportsTls() {
            return tlsSupported;
        }

        @Override
        public int tlsIO(int readinessFlags) {
            if (tlsSessionStarted) {
                tlsIOCalls.incrementAndGet();
                return -1; // return error code to force close the connection
            }
            return 0;
        }

        @Override
        public boolean wantsTlsRead() {
            return tlsSessionStarted;
        }

        @Override
        public boolean wantsTlsWrite() {
            return tlsSessionStarted;
        }
    }

    private static class FakeTlsSocketFactory implements SocketFactory {
        private final AtomicInteger createTlsSessionCalls;
        private final AtomicInteger tlsIOCalls;
        private final boolean tlsSupported;

        private FakeTlsSocketFactory(boolean tlsSupported, AtomicInteger createTlsSessionCalls, AtomicInteger tlsIOCalls) {
            this.tlsSupported = tlsSupported;
            this.createTlsSessionCalls = createTlsSessionCalls;
            this.tlsIOCalls = tlsIOCalls;
        }

        @Override
        public Socket newInstance(NetworkFacade nf, Log log) {
            return new FakeTlsSocket(nf, log, tlsSupported, createTlsSessionCalls, tlsIOCalls);
        }
    }
}
