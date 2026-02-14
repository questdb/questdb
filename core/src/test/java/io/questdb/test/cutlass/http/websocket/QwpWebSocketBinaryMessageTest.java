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

package io.questdb.test.cutlass.http.websocket;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration tests for WebSocket binary message flow on the /write/v4 endpoint.
 * Uses JDK's java.net.http.WebSocket client.
 */
public class QwpWebSocketBinaryMessageTest extends AbstractBootstrapTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testWebSocketConnectAndSendBinaryMessage() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                URI uri = new URI("ws://localhost:" + httpPort + "/write/v4");

                CountDownLatch openLatch = new CountDownLatch(1);
                AtomicBoolean opened = new AtomicBoolean(false);
                AtomicReference<Throwable> error = new AtomicReference<>();

                HttpClient client = HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .build();

                WebSocket.Listener listener = new WebSocket.Listener() {
                    @Override
                    public void onOpen(WebSocket webSocket) {
                        opened.set(true);
                        openLatch.countDown();
                        webSocket.request(1);
                    }

                    @Override
                    public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
                        webSocket.request(1);
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public void onError(WebSocket webSocket, Throwable err) {
                        error.set(err);
                        openLatch.countDown();
                    }
                };

                WebSocket webSocket = client.newWebSocketBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .buildAsync(uri, listener)
                        .get(10, TimeUnit.SECONDS);

                Assert.assertTrue("WebSocket should open", openLatch.await(5, TimeUnit.SECONDS));
                Assert.assertTrue("WebSocket should be opened", opened.get());
                Assert.assertNull("No error should occur", error.get());

                // Send a binary message (ILP-like data)
                String ilpLine = "cpu,host=server01 usage=95.5 1234567890\n";
                byte[] data = ilpLine.getBytes(StandardCharsets.UTF_8);
                ByteBuffer buffer = ByteBuffer.wrap(data);

                CompletableFuture<WebSocket> sendFuture = webSocket.sendBinary(buffer, true);
                sendFuture.get(5, TimeUnit.SECONDS);

                // Give the server time to process
                Thread.sleep(100);

                // Close the connection
                webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "test complete")
                        .get(5, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    public void testWebSocketMultipleBinaryMessages() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                URI uri = new URI("ws://localhost:" + httpPort + "/write/v4");

                CountDownLatch openLatch = new CountDownLatch(1);
                AtomicBoolean opened = new AtomicBoolean(false);
                AtomicReference<Throwable> error = new AtomicReference<>();

                HttpClient client = HttpClient.newHttpClient();

                WebSocket.Listener listener = new WebSocket.Listener() {
                    @Override
                    public void onOpen(WebSocket webSocket) {
                        opened.set(true);
                        openLatch.countDown();
                        webSocket.request(1);
                    }

                    @Override
                    public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
                        webSocket.request(1);
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public void onError(WebSocket webSocket, Throwable err) {
                        error.set(err);
                        openLatch.countDown();
                    }
                };

                WebSocket webSocket = client.newWebSocketBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .buildAsync(uri, listener)
                        .get(10, TimeUnit.SECONDS);

                Assert.assertTrue("WebSocket should open", openLatch.await(5, TimeUnit.SECONDS));
                Assert.assertTrue("WebSocket should be opened", opened.get());
                Assert.assertNull("No error should occur", error.get());

                // Send multiple binary messages
                int numMessages = 10;
                for (int i = 0; i < numMessages; i++) {
                    String ilpLine = "cpu,host=server" + i + " usage=" + (50.0 + i) + " " + (1234567890L + i) + "\n";
                    byte[] data = ilpLine.getBytes(StandardCharsets.UTF_8);
                    ByteBuffer buffer = ByteBuffer.wrap(data);

                    CompletableFuture<WebSocket> sendFuture = webSocket.sendBinary(buffer, true);
                    sendFuture.get(5, TimeUnit.SECONDS);
                }

                // Give the server time to process
                Thread.sleep(200);

                // Close the connection
                webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "test complete")
                        .get(5, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    public void testWebSocketLargeBinaryMessage() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                URI uri = new URI("ws://localhost:" + httpPort + "/write/v4");

                CountDownLatch openLatch = new CountDownLatch(1);
                AtomicBoolean opened = new AtomicBoolean(false);
                AtomicReference<Throwable> error = new AtomicReference<>();

                HttpClient client = HttpClient.newHttpClient();

                WebSocket.Listener listener = new WebSocket.Listener() {
                    @Override
                    public void onOpen(WebSocket webSocket) {
                        opened.set(true);
                        openLatch.countDown();
                        webSocket.request(1);
                    }

                    @Override
                    public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
                        webSocket.request(1);
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public void onError(WebSocket webSocket, Throwable err) {
                        error.set(err);
                        openLatch.countDown();
                    }
                };

                WebSocket webSocket = client.newWebSocketBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .buildAsync(uri, listener)
                        .get(10, TimeUnit.SECONDS);

                Assert.assertTrue("WebSocket should open", openLatch.await(5, TimeUnit.SECONDS));
                Assert.assertTrue("WebSocket should be opened", opened.get());
                Assert.assertNull("No error should occur", error.get());

                // Build a large message (many ILP lines)
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 1000; i++) {
                    sb.append("cpu,host=server").append(i).append(" usage=").append(50.0 + (i % 50)).append(" ").append(1234567890L + i).append("\n");
                }
                byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
                ByteBuffer buffer = ByteBuffer.wrap(data);

                CompletableFuture<WebSocket> sendFuture = webSocket.sendBinary(buffer, true);
                sendFuture.get(10, TimeUnit.SECONDS);

                // Give the server time to process
                Thread.sleep(500);

                // Close the connection
                webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "test complete")
                        .get(5, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    public void testWebSocketPingPong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                URI uri = new URI("ws://localhost:" + httpPort + "/write/v4");

                CountDownLatch openLatch = new CountDownLatch(1);
                CountDownLatch pongLatch = new CountDownLatch(1);
                AtomicBoolean opened = new AtomicBoolean(false);
                AtomicBoolean pongReceived = new AtomicBoolean(false);
                AtomicReference<Throwable> error = new AtomicReference<>();

                HttpClient client = HttpClient.newHttpClient();

                WebSocket.Listener listener = new WebSocket.Listener() {
                    @Override
                    public void onOpen(WebSocket webSocket) {
                        opened.set(true);
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

                    @Override
                    public void onError(WebSocket webSocket, Throwable err) {
                        error.set(err);
                        openLatch.countDown();
                        pongLatch.countDown();
                    }
                };

                WebSocket webSocket = client.newWebSocketBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .buildAsync(uri, listener)
                        .get(10, TimeUnit.SECONDS);

                Assert.assertTrue("WebSocket should open", openLatch.await(5, TimeUnit.SECONDS));
                Assert.assertTrue("WebSocket should be opened", opened.get());
                Assert.assertNull("No error should occur", error.get());

                // Send a ping
                ByteBuffer pingData = ByteBuffer.wrap("ping-test".getBytes(StandardCharsets.UTF_8));
                webSocket.sendPing(pingData).get(5, TimeUnit.SECONDS);

                // Wait for pong response
                Assert.assertTrue("Should receive pong", pongLatch.await(5, TimeUnit.SECONDS));
                Assert.assertTrue("Pong should be received", pongReceived.get());

                // Close the connection
                webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "test complete")
                        .get(5, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    public void testWebSocketCloseHandshake() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                URI uri = new URI("ws://localhost:" + httpPort + "/write/v4");

                CountDownLatch openLatch = new CountDownLatch(1);
                CountDownLatch closeLatch = new CountDownLatch(1);
                AtomicBoolean opened = new AtomicBoolean(false);
                AtomicInteger closeCode = new AtomicInteger(-1);
                AtomicReference<String> closeReason = new AtomicReference<>();
                AtomicReference<Throwable> error = new AtomicReference<>();

                HttpClient client = HttpClient.newHttpClient();

                WebSocket.Listener listener = new WebSocket.Listener() {
                    @Override
                    public void onOpen(WebSocket webSocket) {
                        opened.set(true);
                        openLatch.countDown();
                        webSocket.request(1);
                    }

                    @Override
                    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                        closeCode.set(statusCode);
                        closeReason.set(reason);
                        closeLatch.countDown();
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public void onError(WebSocket webSocket, Throwable err) {
                        error.set(err);
                        openLatch.countDown();
                        closeLatch.countDown();
                    }
                };

                WebSocket webSocket = client.newWebSocketBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .buildAsync(uri, listener)
                        .get(10, TimeUnit.SECONDS);

                Assert.assertTrue("WebSocket should open", openLatch.await(5, TimeUnit.SECONDS));
                Assert.assertTrue("WebSocket should be opened", opened.get());
                Assert.assertNull("No error should occur", error.get());

                // Send close frame
                webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "client closing")
                        .get(5, TimeUnit.SECONDS);

                // Server should respond with close frame
                Assert.assertTrue("Should receive close response", closeLatch.await(5, TimeUnit.SECONDS));
                Assert.assertEquals("Close code should be normal", WebSocket.NORMAL_CLOSURE, closeCode.get());
            }
        });
    }

    @Test
    public void testWebSocketReconnect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                URI uri = new URI("ws://localhost:" + httpPort + "/write/v4");

                HttpClient client = HttpClient.newHttpClient();

                // Connect, send, close - repeat multiple times
                for (int attempt = 0; attempt < 3; attempt++) {
                    CountDownLatch openLatch = new CountDownLatch(1);
                    AtomicBoolean opened = new AtomicBoolean(false);
                    AtomicReference<Throwable> error = new AtomicReference<>();

                    WebSocket.Listener listener = new WebSocket.Listener() {
                        @Override
                        public void onOpen(WebSocket webSocket) {
                            opened.set(true);
                            openLatch.countDown();
                            webSocket.request(1);
                        }

                        @Override
                        public void onError(WebSocket webSocket, Throwable err) {
                            error.set(err);
                            openLatch.countDown();
                        }
                    };

                    WebSocket webSocket = client.newWebSocketBuilder()
                            .connectTimeout(Duration.ofSeconds(5))
                            .buildAsync(uri, listener)
                            .get(10, TimeUnit.SECONDS);

                    Assert.assertTrue("WebSocket should open on attempt " + attempt,
                            openLatch.await(5, TimeUnit.SECONDS));
                    Assert.assertTrue("WebSocket should be opened on attempt " + attempt, opened.get());
                    Assert.assertNull("No error should occur on attempt " + attempt, error.get());

                    // Send a message
                    String ilpLine = "cpu,host=server01,attempt=" + attempt + " usage=95.5 1234567890\n";
                    byte[] data = ilpLine.getBytes(StandardCharsets.UTF_8);
                    ByteBuffer buffer = ByteBuffer.wrap(data);
                    webSocket.sendBinary(buffer, true).get(5, TimeUnit.SECONDS);

                    // Close
                    webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "attempt " + attempt + " complete")
                            .get(5, TimeUnit.SECONDS);

                    // Small delay between reconnects
                    Thread.sleep(100);
                }
            }
        });
    }
}
