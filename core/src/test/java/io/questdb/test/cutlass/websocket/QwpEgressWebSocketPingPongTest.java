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

package io.questdb.test.cutlass.websocket;

import io.questdb.PropertyKey;
import io.questdb.test.TestServerMain;
import io.questdb.test.cutlass.qwp.AbstractQwpBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Regression coverage for the egress /read/v1 endpoint's PING / PONG handling.
 * The egress upgrade processor used to swallow PeerIsSlowToReadException
 * thrown from its inline pong send, leaving the partially-written pong
 * frame parked in the response sink with no one to drain it; clients
 * timed out forever. Pinning the send fragmentation chunk to 4 bytes
 * forces every pong frame to park on PISR, making the failure deterministic.
 */
public class QwpEgressWebSocketPingPongTest extends AbstractQwpBootstrapTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testEgressPingPongUnderTinySendChunk() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "4",
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                URI uri = new URI("ws://localhost:" + httpPort + "/read/v1");

                CountDownLatch openLatch = new CountDownLatch(1);
                CountDownLatch pongLatch = new CountDownLatch(1);
                AtomicBoolean pongReceived = new AtomicBoolean(false);
                AtomicReference<Throwable> error = new AtomicReference<>();

                HttpClient client = HttpClient.newHttpClient();
                WebSocket.Listener listener = new WebSocket.Listener() {
                    @Override
                    public void onError(WebSocket webSocket, Throwable err) {
                        error.set(err);
                        openLatch.countDown();
                        pongLatch.countDown();
                    }

                    @Override
                    public void onOpen(WebSocket webSocket) {
                        openLatch.countDown();
                        webSocket.request(1);
                    }

                    @Override
                    public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
                        pongReceived.set(true);
                        pongLatch.countDown();
                        webSocket.request(1);
                        return CompletableFuture.completedFuture(null);
                    }
                };

                WebSocket webSocket = client.newWebSocketBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .buildAsync(uri, listener)
                        .get(10, TimeUnit.SECONDS);

                Assert.assertTrue("WebSocket should open", openLatch.await(5, TimeUnit.SECONDS));
                Assert.assertNull("No error should occur during handshake", error.get());

                ByteBuffer pingData = ByteBuffer.wrap("ping-test".getBytes(StandardCharsets.UTF_8));
                webSocket.sendPing(pingData).get(5, TimeUnit.SECONDS);

                Assert.assertTrue("Should receive pong under tiny send chunk", pongLatch.await(5, TimeUnit.SECONDS));
                Assert.assertTrue("Pong should be received", pongReceived.get());

                webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "test complete")
                        .get(5, TimeUnit.SECONDS);
            }
        });
    }
}
