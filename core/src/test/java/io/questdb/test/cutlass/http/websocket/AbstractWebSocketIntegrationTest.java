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

import io.questdb.test.AbstractBootstrapTest;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for WebSocket integration tests with real server.
 * Uses JDK's built-in WebSocket client (java.net.http.WebSocket).
 */
public abstract class AbstractWebSocketIntegrationTest extends AbstractBootstrapTest {

    /**
     * Creates and connects a WebSocket client to the specified endpoint.
     *
     * @param port the server port
     * @param path the WebSocket path (e.g., "/write/v4/ws")
     * @return the connected WebSocket
     * @throws Exception if connection fails
     */
    protected WebSocket connectClient(int port, String path) throws Exception {
        return connectClient(port, path, 5, TimeUnit.SECONDS);
    }

    /**
     * Creates and connects a WebSocket client with custom timeout.
     *
     * @param port    the server port
     * @param path    the WebSocket path
     * @param timeout the connection timeout
     * @param unit    the timeout unit
     * @return the connected WebSocket
     * @throws Exception if connection fails
     */
    protected WebSocket connectClient(int port, String path, long timeout, TimeUnit unit) throws Exception {
        URI uri = URI.create("ws://localhost:" + port + path);
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(unit.toMillis(timeout)))
                .build();

        CountDownLatch connectLatch = new CountDownLatch(1);
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        WebSocket.Listener listener = new WebSocket.Listener() {
            @Override
            public void onOpen(WebSocket webSocket) {
                connectLatch.countDown();
                webSocket.request(1);
            }

            @Override
            public void onError(WebSocket webSocket, Throwable error) {
                errorRef.set(error);
                connectLatch.countDown();
            }
        };

        CompletableFuture<WebSocket> future = client.newWebSocketBuilder()
                .buildAsync(uri, listener);

        WebSocket ws = future.get(timeout, unit);

        if (!connectLatch.await(timeout, unit)) {
            ws.abort();
            throw new RuntimeException("WebSocket connection timeout");
        }

        if (errorRef.get() != null) {
            throw new RuntimeException("WebSocket connection error", errorRef.get());
        }

        return ws;
    }

    /**
     * Creates a WebSocket client with custom handlers.
     *
     * @param port    the server port
     * @param path    the WebSocket path
     * @param handler the message handler
     * @return a CompletableFuture that completes with the WebSocket when connected
     * @throws Exception if URI is invalid
     */
    protected CompletableFuture<WebSocket> createClient(int port, String path, WebSocketMessageHandler handler) throws Exception {
        URI uri = URI.create("ws://localhost:" + port + path);
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        WebSocket.Listener listener = new WebSocket.Listener() {
            private StringBuilder textBuffer = new StringBuilder();
            private ByteBuffer binaryBuffer;

            @Override
            public void onOpen(WebSocket webSocket) {
                handler.onOpen();
                webSocket.request(1);
            }

            @Override
            public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                textBuffer.append(data);
                if (last) {
                    handler.onMessage(textBuffer.toString());
                    textBuffer = new StringBuilder();
                }
                webSocket.request(1);
                return null;
            }

            @Override
            public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
                if (binaryBuffer == null) {
                    binaryBuffer = ByteBuffer.allocate(data.remaining());
                }
                // Ensure capacity
                if (binaryBuffer.remaining() < data.remaining()) {
                    ByteBuffer newBuffer = ByteBuffer.allocate(binaryBuffer.position() + data.remaining());
                    binaryBuffer.flip();
                    newBuffer.put(binaryBuffer);
                    binaryBuffer = newBuffer;
                }
                binaryBuffer.put(data);

                if (last) {
                    binaryBuffer.flip();
                    handler.onBinaryMessage(binaryBuffer);
                    binaryBuffer = null;
                }
                webSocket.request(1);
                return null;
            }

            @Override
            public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                handler.onClose(statusCode, reason);
                return null;
            }

            @Override
            public void onError(WebSocket webSocket, Throwable error) {
                handler.onError(error);
            }
        };

        return client.newWebSocketBuilder().buildAsync(uri, listener);
    }

    /**
     * Interface for handling WebSocket messages in tests.
     */
    public interface WebSocketMessageHandler {
        void onOpen();

        void onMessage(String message);

        void onBinaryMessage(ByteBuffer bytes);

        void onClose(int code, String reason);

        void onError(Throwable error);
    }

    /**
     * Simple adapter for WebSocketMessageHandler with latches for synchronization.
     */
    public static class TestWebSocketHandler implements WebSocketMessageHandler {
        private final CountDownLatch openLatch = new CountDownLatch(1);
        private final CountDownLatch closeLatch = new CountDownLatch(1);
        private final AtomicReference<Throwable> errorRef = new AtomicReference<>();
        private volatile int closeCode;
        private volatile String closeReason;

        @Override
        public void onOpen() {
            openLatch.countDown();
        }

        @Override
        public void onMessage(String message) {
            // Override in subclass if needed
        }

        @Override
        public void onBinaryMessage(ByteBuffer bytes) {
            // Override in subclass if needed
        }

        @Override
        public void onClose(int code, String reason) {
            this.closeCode = code;
            this.closeReason = reason;
            closeLatch.countDown();
        }

        @Override
        public void onError(Throwable error) {
            errorRef.set(error);
            openLatch.countDown();
            closeLatch.countDown();
        }

        public boolean awaitOpen(long timeout, TimeUnit unit) throws InterruptedException {
            return openLatch.await(timeout, unit);
        }

        public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
            return closeLatch.await(timeout, unit);
        }

        public Throwable getError() {
            return errorRef.get();
        }

        public int getCloseCode() {
            return closeCode;
        }

        public String getCloseReason() {
            return closeReason;
        }
    }
}
