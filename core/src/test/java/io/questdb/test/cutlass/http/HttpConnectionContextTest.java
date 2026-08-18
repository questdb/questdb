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
import io.questdb.cutlass.http.HttpHeaderParser;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.test.AbstractOomSweepTest;
import org.junit.Assert;
import org.junit.Test;

public class HttpConnectionContextTest extends AbstractOomSweepTest {
    // The context's construction allocates roughly 4.7 KiB across its two header parsers and the
    // response sink, so the ceiling has to climb past that to reach the point where construction
    // survives - the far end of the transition assertOomSweep insists the sweep crosses.
    private static final int CONSTRUCTION_SLACK_MAX = 16 * 1024;
    // The smallest native allocation on the construction path is the 64-byte BoundaryAugmenter, one
    // of the buffers the pre-fix code stranded alongside the parser's DirectUtf8Sink. A coarser step
    // can jump the window between "the augmenter allocated" and "the header buffer failed", which is
    // the only window the leak shows in, and the sweep then passes blind.
    private static final int CONSTRUCTION_SLACK_STEP = 8;
    private static final int HEADER_BUFFER_SIZE = 4096;

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
    public void testConnectionContextConstructionOomLeavesNothingBehind() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            // The constructor allocates several native buffers in sequence. A ceiling that lets an
            // earlier one through and trips a later one strands the earlier one unless the constructor
            // closes what it built, so the sweep walks the ceiling across every one of those points.
            assertOomSweep(CONSTRUCTION_SLACK_MAX, CONSTRUCTION_SLACK_STEP, null, () -> {
                HttpConnectionContext context = new HttpConnectionContext(httpConfig, PlainSocketFactory.INSTANCE);
                // Only reached when construction survived its ceiling; a thrown constructor has
                // already cleaned up after itself, which is what this test asserts.
                context.close();
            });
        });
    }

    @Test
    public void testHeaderParserConstructionOomLeavesNothingBehind() throws Exception {
        assertMemoryLeak(() -> {
            ObjectPool<DirectUtf8String> csPool = new ObjectPool<>(DirectUtf8String.FACTORY, 64);
            // The parser mallocs its header buffer and then its BoundaryAugmenter. The augmenter used
            // to be a field initializer, so it ran first and leaked whenever the header buffer malloc
            // failed behind it - the NATIVE_HTTP_CONN leak this sweep guards.
            assertOomSweep(CONSTRUCTION_SLACK_MAX, CONSTRUCTION_SLACK_STEP, null, () -> {
                HttpHeaderParser parser = new HttpHeaderParser(HEADER_BUFFER_SIZE, csPool);
                parser.close();
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
}
