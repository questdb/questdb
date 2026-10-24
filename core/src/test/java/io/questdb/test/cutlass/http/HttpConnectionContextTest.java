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
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpContextConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRawSocket;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.RejectProcessorFactory;
import io.questdb.log.Log;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PlainSocket;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.Socket;
import io.questdb.network.SocketFactory;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractOomSweepTest;
import org.junit.Assert;
import org.junit.Test;

public class HttpConnectionContextTest extends AbstractOomSweepTest {

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
        // live and only the constructor's own catch can hand them back. The factory throws instead
        // of returning, so no authenticator exists to assert about here - the constructor closing
        // the one it did take is what testConstructorFailureAtRejectFactoryClosesSocketAndAuthenticator
        // covers.
        final HttpFullFatServerConfiguration httpConfig = preallocatingConfiguration(
                () -> {
                    throw new InjectedFailure();
                },
                context -> {
                    throw new IllegalStateException("unreachable: the authenticator factory throws first");
                }
        );

        assertMemoryLeak(() -> assertConstructorRollback(httpConfig, new CountingSocketFactory()));
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
    public void testEscapeJsonStrExhaustionAndExactFit() throws Exception {
        assertMemoryLeak(() -> {
            assertNativeHttpOverflow("abcdef", "preabcde");
            assertNativeHttpOverflow("ab\u0000", "preab\\u0");
            assertNativeHttpOverflow("abcd\u0000", "preabcd\\");
            try (DirectUtf16Sink nativeInput = new DirectUtf16Sink(64); HttpConnectionContext http = newHttp(8)) {
                nativeInput.put("abcde");
                DirectString direct = new DirectString().of(nativeInput.ptr(), nativeInput.length());
                HttpChunkedResponse response = http.getChunkedResponse();
                HttpRawSocket raw = http.getRawResponseSocket();
                long address = raw.getBufferAddress();
                response.putAscii("pre");
                response.bookmark();
                Assert.assertSame(response, response.escapeJsonStr(direct));
                Assert.assertEquals(0, raw.getBufferSize());
                assertNativeEquals(new Utf8String("preabcde"), address, 8);
                Assert.assertThrows(NoSpaceLeftInResponseBufferException.class, () -> response.putQuote());
                Assert.assertSame(response, response.escapeJsonStr(direct, 4, 2));
                Assert.assertSame(response, response.escapeJsonStr(direct, 0, 0));
                Assert.assertSame(response, response.escapeJsonStr(null, 0, 0));
                Assert.assertSame(response, response.escapeJsonStr(null, 3, 1));
                Assert.assertThrows(NullPointerException.class, () -> response.escapeJsonStr(null, 0, 1));
                Assert.assertThrows(NullPointerException.class, () -> response.escapeJsonStr(null, 0, 4));
                Assert.assertEquals(0, raw.getBufferSize());
                assertNativeEquals(new Utf8String("preabcde"), address, 8);
                Assert.assertTrue(response.resetToBookmark());
                assertNativeEquals(new Utf8String("pre"), address, 3);
                response.escapeJsonStr(direct, 2, 5);
                assertNativeEquals(new Utf8String("precde"), address, 6);
            }
        });
    }

    @Test
    public void testEscapeJsonStrFourCharWords() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectUtf16Sink input = new DirectUtf16Sink(32);
                 DirectUtf8Sink expected = new DirectUtf8Sink(32);
                 HttpConnectionContext http = newHttp(64)) {
                DirectString direct = new DirectString();
                HttpChunkedResponse response = http.getChunkedResponse();
                HttpRawSocket raw = http.getRawResponseSocket();
                long address = raw.getBufferAddress();
                response.bookmark();
                for (int lane = 0; lane < 4; lane++) {
                    for (int value = 0; value <= Character.MAX_VALUE; value++) {
                        input.clear();
                        for (int i = 0; i < 4; i++) {
                            input.put(i == lane ? (char) value : (char) ('a' + i));
                        }
                        direct.of(input.ptr(), input.length());
                        expected.clear();
                        expected.escapeJsonStr(input);
                        Assert.assertFalse(response.resetToBookmark());
                        response.escapeJsonStr(direct);
                        assertNativeEquals(expected, address, 64 - raw.getBufferSize());
                    }
                }
            }
        });
    }

    @Test
    public void testEscapeJsonStrRanges() throws Exception {
        assertMemoryLeak(() -> {
            ObjList<String> cases = new ObjList<>();
            cases.add("");
            cases.add("ASCII prefix ~\u007f");
            cases.add("a\"b\\c");
            cases.add("prefix-\u00e9\u4e2d\ud83d\ude00-tail");
            cases.add("prefix-\ud800x\udc00-tail");
            StringBuilder controls = new StringBuilder();
            for (char c = 0; c < 32; c++) {
                controls.append(c);
            }
            cases.add(controls.toString());
            try (DirectUtf8Sink expected = new DirectUtf8Sink(32);
                 DirectUtf16Sink nativeInput = new DirectUtf16Sink(64);
                 HttpConnectionContext http = newHttp(256)) {
                HttpChunkedResponse response = http.getChunkedResponse();
                HttpRawSocket raw = http.getRawResponseSocket();
                long address = raw.getBufferAddress();
                response.bookmark();
                for (int c = 0; c < cases.size(); c++) {
                    String input = cases.getQuick(c);
                    nativeInput.clear();
                    nativeInput.put(input);
                    DirectString direct = new DirectString().of(nativeInput.ptr(), input.length());
                    for (int lo = 0; lo <= input.length(); lo++) {
                        for (int hi = lo; hi <= input.length(); hi++) {
                            for (int prefixType = 0; prefixType < 2; prefixType++) {
                                String prefix = prefixType == 0 ? "pre:" : "\u00e9:";
                                expected.clear();
                                expected.put(prefix).escapeJsonStr(input, lo, hi);
                                Assert.assertFalse(response.resetToBookmark());
                                response.put(prefix);
                                Assert.assertSame(response, response.escapeJsonStr(direct, lo, hi));
                                assertNativeEquals(expected, address, 256 - raw.getBufferSize());
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testEscapeJsonStrSubclassUsesCharAt() throws Exception {
        assertMemoryLeak(() -> {
            String input = "ab\"\\\u0000\u00e9\ud83d\ude00";
            DirectString direct = new DirectString() {
                @Override
                public char charAt(int index) {
                    return input.charAt(index);
                }

                @Override
                public long lo() {
                    throw new AssertionError("subclass pointer must not replace charAt()");
                }
            }.of(0, input.length());
            try (DirectUtf8Sink expected = new DirectUtf8Sink(64); HttpConnectionContext http = newHttp(128)) {
                HttpChunkedResponse response = http.getChunkedResponse();
                HttpRawSocket raw = http.getRawResponseSocket();
                long address = raw.getBufferAddress();
                expected.escapeJsonStr(input);
                response.escapeJsonStr(direct);
                assertNativeEquals(expected, address, 128 - raw.getBufferSize());
            }
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

    private static void assertNativeEquals(Utf8Sequence expected, long address, int size) {
        Assert.assertEquals(expected.size(), size);
        for (int i = 0; i < size; i++) {
            Assert.assertEquals("byte " + i, expected.byteAt(i), Unsafe.getByte(address + i));
        }
    }

    private static void assertNativeHttpOverflow(String input, String partial) {
        try (DirectUtf16Sink nativeInput = new DirectUtf16Sink(64); HttpConnectionContext http = newHttp(8)) {
            nativeInput.put(input);
            DirectString direct = new DirectString().of(nativeInput.ptr(), input.length());
            HttpChunkedResponse response = http.getChunkedResponse();
            HttpRawSocket raw = http.getRawResponseSocket();
            long address = raw.getBufferAddress();
            response.putAscii("pre");
            response.bookmark();
            NoSpaceLeftInResponseBufferException exception = Assert.assertThrows(
                    NoSpaceLeftInResponseBufferException.class,
                    () -> response.escapeJsonStr(direct)
            );
            Assert.assertEquals(1, exception.getBytesRequired());
            Assert.assertEquals(0, raw.getBufferSize());
            assertNativeEquals(new Utf8String(partial), address, 8 - raw.getBufferSize());
            Assert.assertTrue(response.resetToBookmark());
            assertNativeEquals(new Utf8String("pre"), address, 8 - raw.getBufferSize());
            response.escapeJsonStr("ok");
            assertNativeEquals(new Utf8String("preok"), address, 8 - raw.getBufferSize());
        }
    }

    private static HttpConnectionContext newHttp(int capacity) {
        return new HttpConnectionContext(new DefaultHttpServerConfiguration(configuration) {
            @Override
            public int getSendBufferSize() {
                return capacity;
            }

            @Override
            public boolean preAllocateBuffers() {
                return true;
            }
        }, PlainSocketFactory.INSTANCE);
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
