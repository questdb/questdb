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

import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.network.PlainSocketFactory;
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
}
