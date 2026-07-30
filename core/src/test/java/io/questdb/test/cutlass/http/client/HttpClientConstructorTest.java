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

package io.questdb.test.cutlass.http.client;

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.HttpClientConfiguration;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientCookieHandlerFactory;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.HttpClientLinux;
import io.questdb.cutlass.http.client.HttpClientOsx;
import io.questdb.cutlass.http.client.HttpClientWindows;
import io.questdb.log.Log;
import io.questdb.network.EpollFacade;
import io.questdb.network.KqueueFacade;
import io.questdb.network.NetworkFacade;
import io.questdb.network.PlainSocket;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.SelectFacade;
import io.questdb.network.Socket;
import io.questdb.network.SocketFactory;
import io.questdb.test.AbstractOomSweepTest;
import org.junit.Assert;
import org.junit.Test;

public class HttpClientConstructorTest extends AbstractOomSweepTest {

    @Test
    public void testConstructorFailureAtCookieHandlerFactoryClosesSocket() throws Exception {
        // The cookie-handler factory is the first extension point the constructor reaches after it
        // takes the socket - only getTimeout() precedes it - and it still runs before the first
        // native allocation, so the socket is the only resource live at the throw. The sweep below
        // cannot pin that branch: it builds a PlainSocket whose fd is still -1, whose close() is
        // then a no-op, so dropping the socket rollback keeps the leak check green. Counting the
        // closes is the only oracle for it.
        final HttpClientConfiguration configuration = new DefaultHttpClientConfiguration() {
            @Override
            public HttpClientCookieHandlerFactory getCookieHandlerFactory() {
                throw new InjectedFailure();
            }
        };

        assertMemoryLeak(() -> {
            final CountingSocketFactory socketFactory = new CountingSocketFactory();
            try {
                // Closing here keeps the leak check honest on the path where the constructor
                // unexpectedly succeeds: dropping a built client would leak on top of the assertion
                // below and bury it.
                HttpClientFactory.newInstance(configuration, socketFactory).close();
                Assert.fail("expected InjectedFailure");
            } catch (InjectedFailure ignore) {
                // the injected configuration getter threw
            }
            Assert.assertEquals("the constructor must close the socket it took", 1, socketFactory.closeCount);
        });
    }

    @Test
    public void testConstructorFailureFreesNativeAllocations() throws Exception {
        // The constructor takes the socket, then the request buffer, then the response-parser
        // buffer, then a ResponseHeaders whose own parser allocates again. A ceiling tripped at
        // any point after the first used to strand everything acquired before it: the caller
        // never receives the client, so close() never runs.
        //
        // The buffers are shrunk from their 64 KiB defaults so the sweep can step finely enough
        // to land between allocation points without running for thousands of iterations.
        final HttpClientConfiguration configuration = new DefaultHttpClientConfiguration() {
            @Override
            public int getInitialRequestBufferSize() {
                return 1024;
            }

            @Override
            public int getResponseBufferSize() {
                return 2048;
            }
        };

        assertMemoryLeak(() -> assertOomSweep(16 * 1024, 64, null, () -> {
            //noinspection EmptyTryBlock
            try (HttpClient ignore = HttpClientFactory.newInstance(configuration, PlainSocketFactory.INSTANCE)) {
                // built without tripping the ceiling; close() releases it
            }
        }));
    }

    @Test
    public void testLinuxConstructorFailureClosesBaseClient() throws Exception {
        assertSubclassConstructorRollback(new DefaultHttpClientConfiguration() {
            @Override
            public EpollFacade getEpollFacade() {
                throw new InjectedFailure();
            }
        }, HttpClientLinux::new);
    }

    @Test
    public void testOsxConstructorFailureClosesBaseClient() throws Exception {
        assertSubclassConstructorRollback(new DefaultHttpClientConfiguration() {
            @Override
            public KqueueFacade getKQueueFacade() {
                throw new InjectedFailure();
            }
        }, HttpClientOsx::new);
    }

    @Test
    public void testWindowsConstructorFailureClosesBaseClient() throws Exception {
        assertSubclassConstructorRollback(new DefaultHttpClientConfiguration() {
            @Override
            public SelectFacade getSelectFacade() {
                throw new InjectedFailure();
            }
        }, HttpClientWindows::new);
    }

    /**
     * Drives one platform subclass's constructor rollback on any host. {@code HttpClientFactory}
     * picks a single implementation from {@code Os.type}, so the sweep above can only ever reach the
     * subclass the test host runs on; these tests instantiate each one directly instead.
     * <p>
     * The failure comes from the platform facade getter rather than from the poller it feeds. That
     * keeps the test host-independent in both directions: the getter is evaluated as a constructor
     * argument, so {@code new Kqueue(...)} / {@code new FDSet(...)} is never invoked and neither
     * {@code KqueueAccessor} nor {@code SelectAccessor} - whose natives ship only in the macOS and
     * Windows builds - is ever initialised. By that point {@code super()} has taken the socket, both
     * client buffers and the response parser, so the subclass catch has to hand all of them back.
     */
    private static void assertSubclassConstructorRollback(
            HttpClientConfiguration configuration,
            ClientFactory clientFactory
    ) throws Exception {
        assertMemoryLeak(() -> {
            final CountingSocketFactory socketFactory = new CountingSocketFactory();
            try {
                // Closing here keeps the leak check honest on the path where the constructor
                // unexpectedly succeeds: dropping a built client would leak on top of the assertion
                // below and bury it.
                clientFactory.newInstance(configuration, socketFactory).close();
                Assert.fail("expected InjectedFailure");
            } catch (InjectedFailure ignore) {
                // the injected facade getter threw
            }
            Assert.assertEquals("the constructor must close the socket it took", 1, socketFactory.closeCount);
        });
    }

    @FunctionalInterface
    private interface ClientFactory {
        HttpClient newInstance(HttpClientConfiguration configuration, SocketFactory socketFactory);
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
            super("injected configuration failure", null, false, false);
        }
    }
}
