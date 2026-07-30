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

package io.questdb.test.cutlass.http;

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.http.DefaultHttpContextConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpAuthenticator;
import io.questdb.cutlass.http.HttpAuthenticatorFactory;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpContextConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.RejectProcessorFactory;
import io.questdb.log.Log;
import io.questdb.network.NetworkFacade;
import io.questdb.network.PlainSocket;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.Socket;
import io.questdb.network.SocketFactory;
import io.questdb.test.AbstractOomSweepTest;
import org.junit.Assert;
import org.junit.Test;

public class HttpConnectionContextTest extends AbstractOomSweepTest {

    @Test
    public void testConstructorFailureFreesNativeAllocations() throws Exception {
        // The constructor takes both header parsers, the response sink and - when the server
        // pre-allocates - the receive buffer, in that order. A ceiling tripped at any of those
        // points used to strand everything acquired before it: the half-built context never
        // reaches the connection pool, so nothing ever calls close() on it.
        //
        // Sweeping the ceiling covers every allocation point rather than one hand-picked value,
        // and assertOomSweep's own brackets fail loudly if the range stops short of the
        // failing-to-succeeding transition. assertMemoryLeak is the assertion that matters: each
        // point either builds a context and closes it, or throws and must leave nothing behind.
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            // The default context allocates roughly 4.8 KiB here - a 4096-byte request header
            // buffer and a 512-byte multipart one, each with its own 64-byte boundary augmenter
            // and 32-byte sink - so the step has to be finer than the smallest of those to land
            // between allocation points rather than skipping the whole span in one stride.
            assertOomSweep(16 * 1024, 64, null, () -> {
                //noinspection EmptyTryBlock
                try (HttpConnectionContext ignore = new HttpConnectionContext(httpConfig, PlainSocketFactory.INSTANCE)) {
                    // built without tripping the ceiling; close() releases it
                }
            });
        });
    }

    @Test
    public void testClearDisarmsBreakerOnPoolReturnWhileProtocolSwitched() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            try (HttpConnectionContext context = new HttpConnectionContext(httpConfig, PlainSocketFactory.INSTANCE)) {
                NetworkSqlExecutionCircuitBreaker breaker = context.getOrCreateCircuitBreaker(engine);
                breaker.of(42);
                breaker.resetTimer();
                Assert.assertEquals(42, breaker.getFd());
                Assert.assertTrue(breaker.isTimerSet());

                // A protocol-switched (WebSocket/QWP) request boundary must not disarm the breaker:
                // a parked credit-suspended egress stream still needs it.
                context.switchProtocol();
                context.reset();
                Assert.assertEquals("reset() must preserve the breaker while the protocol is switched", 42, breaker.getFd());
                Assert.assertTrue(breaker.isTimerSet());

                // Pool return unconditionally disarms it, even while switched.
                context.clear();
                Assert.assertEquals("clear() must disarm the breaker on pool return", -1, breaker.getFd());
                Assert.assertFalse(breaker.isTimerSet());
            }
        });
    }

    @Test
    public void testConstructorFailureAtAuthenticatorFactoryFreesPreallocatedBuffers() throws Exception {
        // The authenticator factory is the first fallible step after the pre-allocated receive
        // buffer and the response sink's send buffer, so this is the point where those two are
        // live and only the constructor's own catch can hand them back. Nothing has taken the
        // authenticator yet, so the rollback must not touch it either.
        final CountingAuthenticator authenticator = new CountingAuthenticator();
        final HttpFullFatServerConfiguration httpConfig = preallocatingConfiguration(
                () -> {
                    throw new InjectedFailure();
                },
                context -> {
                    throw new IllegalStateException("unreachable: the authenticator factory throws first");
                }
        );

        assertMemoryLeak(() -> {
            final CountingSocketFactory socketFactory = new CountingSocketFactory();
            assertConstructorRollback(httpConfig, socketFactory);
            Assert.assertEquals("nothing acquired the authenticator, so nothing must close it", 0, authenticator.closeCount);
        });
    }

    @Test
    public void testConstructorFailureAtCookieHandlerClosesSocket() throws Exception {
        // The cookie handler is the first extension hook the constructor reaches once IOContext has
        // taken the socket, and the session store, the context configuration and the network facade
        // follow it before any native allocation happens. Nothing native is live at that point, so
        // the socket is the only thing a throw can strand and the socket count is the only oracle:
        // a half-built context never reaches the connection pool, so nothing else ever closes it.
        final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration) {
            @Override
            public FactoryProvider getFactoryProvider() {
                throw new InjectedFailure();
            }
        };

        assertMemoryLeak(() -> assertConstructorRollback(httpConfig, new CountingSocketFactory()));
    }

    @Test
    public void testConstructorFailureAtRejectFactoryClosesSocketAndAuthenticator() throws Exception {
        // The reject processor is created immediately after the authenticator, so a throw there is
        // the narrowest window in which the authenticator is live and unreachable. The socket is
        // live from IOContext's constructor onwards, i.e. across every failure point.
        final CountingAuthenticator authenticator = new CountingAuthenticator();
        final HttpFullFatServerConfiguration httpConfig = preallocatingConfiguration(
                () -> authenticator,
                context -> {
                    throw new InjectedFailure();
                }
        );

        assertMemoryLeak(() -> {
            final CountingSocketFactory socketFactory = new CountingSocketFactory();
            try {
                // Closing here keeps the leak check honest on the path where the constructor
                // unexpectedly succeeds: dropping a built context would leak on top of the
                // failures below and bury them.
                new HttpConnectionContext(httpConfig, socketFactory).close();
                Assert.fail("expected InjectedFailure");
            } catch (InjectedFailure ignore) {
                // the injected reject factory threw
            }
            // The authenticator is asserted first so its own regression shows up rather than
            // hiding behind the socket assertion, which the sibling test above pins on its own.
            Assert.assertEquals("the constructor must close the authenticator it took", 1, authenticator.closeCount);
            Assert.assertEquals("the constructor must close the socket it took", 1, socketFactory.closeCount);
        });
    }

    @Test
    public void testConstructorFailureFreesPreallocatedNativeAllocations() throws Exception {
        // Same sweep as testConstructorFailureFreesNativeAllocations, but with preAllocateBuffers()
        // on, which is the only way the receive-buffer malloc and responseSink.open() run at all -
        // DefaultHttpServerConfiguration returns false, so both are dead in that test.
        //
        // The two buffers are shrunk from their 128 KiB defaults so the sweep can step finely
        // enough to land between allocation points without running for thousands of iterations.
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration) {
                @Override
                public int getRecvBufferSize() {
                    return 1024;
                }

                @Override
                public int getSendBufferSize() {
                    return 2048;
                }

                @Override
                public boolean preAllocateBuffers() {
                    return true;
                }
            };
            assertOomSweep(16 * 1024, 64, null, () -> {
                //noinspection EmptyTryBlock
                try (HttpConnectionContext ignore = new HttpConnectionContext(httpConfig, PlainSocketFactory.INSTANCE)) {
                    // built without tripping the ceiling; close() releases it
                }
            });
        });
    }

    @Test
    public void testResetDisarmsBreakerForPlainHttpKeepAlive() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            try (HttpConnectionContext context = new HttpConnectionContext(httpConfig, PlainSocketFactory.INSTANCE)) {
                NetworkSqlExecutionCircuitBreaker breaker = context.getOrCreateCircuitBreaker(engine);
                breaker.of(42);
                breaker.resetTimer();

                // A plain HTTP request boundary (not protocol-switched) must disarm the breaker so a
                // per-statement timeout cannot leak into the next keep-alive request on this connection.
                context.reset();
                Assert.assertEquals("reset() must disarm the breaker between plain HTTP requests", -1, breaker.getFd());
                Assert.assertFalse(breaker.isTimerSet());
            }
        });
    }

    private static void assertConstructorRollback(HttpFullFatServerConfiguration httpConfig, CountingSocketFactory socketFactory) {
        try {
            // Closing here keeps the leak check honest on the path where the constructor
            // unexpectedly succeeds: dropping a built context would leak on top of the failure
            // below and bury it.
            new HttpConnectionContext(httpConfig, socketFactory).close();
            Assert.fail("expected InjectedFailure");
        } catch (InjectedFailure ignore) {
            // the injected factory threw
        }
        Assert.assertEquals("the constructor must close the socket it took", 1, socketFactory.closeCount);
    }

    /**
     * Builds a configuration that pre-allocates its buffers - so the receive buffer and the response
     * sink's send buffer are live by the time the authenticator and the reject processor are created
     * - and takes both of those from the given factories. Both buffers are shrunk from their 128 KiB
     * defaults purely to keep the test cheap.
     */
    private static HttpFullFatServerConfiguration preallocatingConfiguration(
            HttpAuthenticatorFactory authenticatorFactory,
            RejectProcessorFactory rejectProcessorFactory
    ) {
        final FactoryProvider factoryProvider = new DefaultFactoryProvider() {
            @Override
            public HttpAuthenticatorFactory getHttpAuthenticatorFactory() {
                return authenticatorFactory;
            }

            @Override
            public RejectProcessorFactory getRejectProcessorFactory() {
                return rejectProcessorFactory;
            }
        };
        // The context configuration, not the server one: HttpConnectionContext reads the
        // authenticator and reject factories off getHttpContextConfiguration().getFactoryProvider().
        final HttpContextConfiguration contextConfiguration = new DefaultHttpContextConfiguration() {
            @Override
            public FactoryProvider getFactoryProvider() {
                return factoryProvider;
            }
        };
        return new DefaultHttpServerConfiguration(configuration, contextConfiguration) {
            @Override
            public int getRecvBufferSize() {
                return 1024;
            }

            @Override
            public int getSendBufferSize() {
                return 2048;
            }

            @Override
            public boolean preAllocateBuffers() {
                return true;
            }
        };
    }

    private static class CountingAuthenticator implements HttpAuthenticator {
        int closeCount;

        @Override
        public boolean authenticate(HttpRequestHeader headers) {
            return true;
        }

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public CharSequence getPrincipal() {
            return null;
        }
    }

    private static class CountingSocketFactory implements SocketFactory {
        int closeCount;

        @Override
        public Socket newInstance(NetworkFacade nf, Log log) {
            return new PlainSocket(nf, log) {
                @Override
                public void close() {
                    closeCount++;
                    super.close();
                }
            };
        }
    }

    private static class InjectedFailure extends RuntimeException {
        InjectedFailure() {
            super("injected factory failure", null, false, false);
        }
    }
}
