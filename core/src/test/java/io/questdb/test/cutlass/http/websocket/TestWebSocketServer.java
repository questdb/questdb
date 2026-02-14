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

import io.questdb.cutlass.qwp.server.WebSocketConnectionContext;
import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.cutlass.qwp.websocket.WebSocketProcessor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.QuietCloseable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple WebSocket server for integration testing.
 * Uses our WebSocketConnectionContext implementation to handle WebSocket frames.
 */
public class TestWebSocketServer implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(TestWebSocketServer.class);
    private static final String WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    private static final String DEFAULT_KEYSTORE = "/keystore/server.keystore";
    private static final char[] DEFAULT_KEYSTORE_PASSWORD = "questdb".toCharArray();

    private final int port;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<ClientHandler> clients = new CopyOnWriteArrayList<>();
    private final WebSocketServerHandler handler;
    private final AtomicInteger connectionCount = new AtomicInteger(0);
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final boolean tlsEnabled;
    private final String keystore;
    private final char[] keystorePassword;

    private ServerSocket serverSocket;
    private Thread acceptThread;

    public TestWebSocketServer(int port, WebSocketServerHandler handler) {
        this(port, handler, false, null, null);
    }

    public TestWebSocketServer(int port, WebSocketServerHandler handler, boolean tlsEnabled) {
        this(port, handler, tlsEnabled, DEFAULT_KEYSTORE, DEFAULT_KEYSTORE_PASSWORD);
    }

    public TestWebSocketServer(int port, WebSocketServerHandler handler, boolean tlsEnabled, String keystore, char[] keystorePassword) {
        this.port = port;
        this.handler = handler;
        this.tlsEnabled = tlsEnabled;
        this.keystore = keystore;
        this.keystorePassword = keystorePassword;
    }

    public void start() throws IOException {
        if (running.getAndSet(true)) {
            return;
        }

        serverSocket = createServerSocket();
        serverSocket.setSoTimeout(100);  // Allow checking running flag periodically

        acceptThread = new Thread(() -> {
            startLatch.countDown();
            while (running.get()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    ClientHandler clientHandler = new ClientHandler(clientSocket);
                    clients.add(clientHandler);
                    connectionCount.incrementAndGet();
                    clientHandler.start();
                } catch (SocketTimeoutException e) {
                    // Expected, check running flag
                } catch (IOException e) {
                    if (running.get()) {
                        LOG.error().$("Accept error: ").$(e).$();
                    }
                }
            }
        }, "WebSocket-Accept");
        acceptThread.start();
    }

    public boolean awaitStart(long timeout, TimeUnit unit) throws InterruptedException {
        return startLatch.await(timeout, unit);
    }

    public int getPort() {
        return port;
    }

    public int getConnectionCount() {
        return connectionCount.get();
    }

    public int getActiveConnectionCount() {
        return clients.size();
    }

    public boolean isTlsEnabled() {
        return tlsEnabled;
    }

    private ServerSocket createServerSocket() throws IOException {
        if (tlsEnabled) {
            try {
                SSLContext sslContext = SSLContext.getInstance("TLS");
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(KeyStore.getInstance(KeyStore.getDefaultType()));

                KeyStore myKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                myKeyStore.load(TestWebSocketServer.class.getResourceAsStream(keystore), keystorePassword);

                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(myKeyStore, keystorePassword);
                sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());

                return sslContext.getServerSocketFactory().createServerSocket(port);
            } catch (Exception e) {
                throw new IOException("Failed to create TLS server socket", e);
            }
        } else {
            return new ServerSocket(port);
        }
    }

    @Override
    public void close() {
        running.set(false);

        // Close all client handlers
        for (ClientHandler client : clients) {
            client.close();
        }
        clients.clear();

        // Close server socket
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                // Ignore
            }
        }

        // Wait for accept thread
        if (acceptThread != null) {
            try {
                acceptThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Interface for handling WebSocket server events.
     */
    public interface WebSocketServerHandler {
        default void onConnect(ClientHandler client) {}
        default void onDisconnect(ClientHandler client) {}
        default void onBinaryMessage(ClientHandler client, byte[] data) {}
        default void onTextMessage(ClientHandler client, String text) {}
        default void onPing(ClientHandler client, byte[] data) {}
        default void onPong(ClientHandler client, byte[] data) {}
        default void onClose(ClientHandler client, int code, String reason) {}
        default void onError(ClientHandler client, Exception e) {}
    }

    /**
     * Handles a single WebSocket client connection.
     */
    public class ClientHandler implements QuietCloseable {
        private final Socket socket;
        private final AtomicBoolean running = new AtomicBoolean(false);
        private final WebSocketConnectionContext context;
        private final List<byte[]> receivedBinaryMessages = new ArrayList<>();
        private final List<String> receivedTextMessages = new ArrayList<>();
        private final CountDownLatch closeLatch = new CountDownLatch(1);

        private Thread readThread;
        private InputStream in;
        private OutputStream out;
        private volatile int closeCode = -1;
        private volatile String closeReason;

        ClientHandler(Socket socket) {
            this.socket = socket;
            this.context = new WebSocketConnectionContext(65536);
        }

        void start() {
            if (running.getAndSet(true)) {
                return;
            }

            readThread = new Thread(() -> {
                try {
                    // Set socket read timeout to prevent blocking forever
                    // This allows the server to poll for data and handle close requests
                    socket.setSoTimeout(100);

                    in = socket.getInputStream();
                    out = socket.getOutputStream();

                    // Perform WebSocket handshake
                    if (!performHandshake()) {
                        LOG.error().$("Handshake failed").$();
                        return;
                    }

                    handler.onConnect(this);

                    // Read and process WebSocket frames
                    byte[] readBuffer = new byte[8192];
                    WebSocketProcessor processor = new ServerProcessor();

                    while (running.get() && !context.isClosed()) {
                        int read;
                        try {
                            read = in.read(readBuffer);
                        } catch (SocketTimeoutException e) {
                            // Timeout - continue polling
                            continue;
                        }
                        if (read <= 0) {
                            break;
                        }

                        context.getRecvBuffer().write(readBuffer, 0, read);
                        context.handleRead(processor);

                        // Flush send buffer (pongs are sent immediately during handleRead)
                        flushSendBuffer();
                    }
                } catch (IOException e) {
                    if (running.get()) {
                        handler.onError(this, e);
                    }
                } finally {
                    handler.onDisconnect(this);
                    clients.remove(this);
                    closeLatch.countDown();
                }
            }, "WebSocket-Client-" + socket.getPort());
            readThread.start();
        }

        private boolean performHandshake() throws IOException {
            // Read HTTP request
            StringBuilder request = new StringBuilder();
            byte[] buf = new byte[1];
            while (true) {
                int read = in.read(buf);
                if (read <= 0) {
                    return false;
                }
                request.append((char) buf[0]);
                if (request.toString().endsWith("\r\n\r\n")) {
                    break;
                }
                if (request.length() > 8192) {
                    return false;  // Request too large
                }
            }

            // Parse Sec-WebSocket-Key
            String key = null;
            for (String line : request.toString().split("\r\n")) {
                if (line.toLowerCase().startsWith("sec-websocket-key:")) {
                    key = line.substring(18).trim();
                    break;
                }
            }

            if (key == null) {
                return false;
            }

            // Compute accept key
            String acceptKey = computeAcceptKey(key);

            // Send handshake response
            String response = "HTTP/1.1 101 Switching Protocols\r\n" +
                    "Upgrade: websocket\r\n" +
                    "Connection: Upgrade\r\n" +
                    "Sec-WebSocket-Accept: " + acceptKey + "\r\n" +
                    "\r\n";
            out.write(response.getBytes(StandardCharsets.US_ASCII));
            out.flush();

            return true;
        }

        private String computeAcceptKey(String key) {
            try {
                MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
                sha1.update((key + WEBSOCKET_GUID).getBytes(StandardCharsets.US_ASCII));
                return Base64.getEncoder().encodeToString(sha1.digest());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private synchronized void flushSendBuffer() throws IOException {
            if (context.hasPendingSend()) {
                byte[] data = context.getSendBuffer().toByteArray();
                out.write(data);
                out.flush();
                context.getSendBuffer().clear();
            }
        }

        /**
         * Sends a binary message to the client.
         */
        public synchronized void sendBinary(byte[] data) throws IOException {
            context.sendBinaryFrame(data, 0, data.length);
            flushSendBuffer();
        }

        /**
         * Sends a text message to the client.
         */
        public synchronized void sendText(String text) throws IOException {
            byte[] data = text.getBytes(StandardCharsets.UTF_8);
            context.sendTextFrame(data, 0, data.length);
            flushSendBuffer();
        }

        /**
         * Sends a ping frame to the client.
         */
        public synchronized void sendPing(byte[] data) throws IOException {
            context.sendPingFrame(data, 0, data.length);
            flushSendBuffer();
        }

        /**
         * Sends a close frame to the client.
         */
        public synchronized void sendClose(int code, String reason) throws IOException {
            context.sendCloseFrame(code, reason);
            flushSendBuffer();
        }

        /**
         * Returns all received binary messages.
         */
        public List<byte[]> getReceivedBinaryMessages() {
            return new ArrayList<>(receivedBinaryMessages);
        }

        /**
         * Returns all received text messages.
         */
        public List<String> getReceivedTextMessages() {
            return new ArrayList<>(receivedTextMessages);
        }

        /**
         * Returns the close code received from the client.
         */
        public int getCloseCode() {
            return closeCode;
        }

        /**
         * Returns the close reason received from the client.
         */
        public String getCloseReason() {
            return closeReason;
        }

        /**
         * Waits for the connection to close.
         */
        public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
            return closeLatch.await(timeout, unit);
        }

        @Override
        public void close() {
            running.set(false);
            try {
                socket.close();
            } catch (IOException e) {
                // Ignore
            }
            if (readThread != null) {
                try {
                    readThread.join(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            context.close();
        }

        private class ServerProcessor implements WebSocketProcessor {
            @Override
            public void onBinaryMessage(long payload, int length) {
                byte[] data = new byte[length];
                for (int i = 0; i < length; i++) {
                    data[i] = io.questdb.std.Unsafe.getUnsafe().getByte(payload + i);
                }
                receivedBinaryMessages.add(data);
                handler.onBinaryMessage(ClientHandler.this, data);
            }

            @Override
            public void onTextMessage(long payload, int length) {
                byte[] data = new byte[length];
                for (int i = 0; i < length; i++) {
                    data[i] = io.questdb.std.Unsafe.getUnsafe().getByte(payload + i);
                }
                String text = new String(data, StandardCharsets.UTF_8);
                receivedTextMessages.add(text);
                handler.onTextMessage(ClientHandler.this, text);
            }

            @Override
            public void onPing(long payload, int length) {
                byte[] data = new byte[length];
                for (int i = 0; i < length; i++) {
                    data[i] = io.questdb.std.Unsafe.getUnsafe().getByte(payload + i);
                }
                handler.onPing(ClientHandler.this, data);
            }

            @Override
            public void onPong(long payload, int length) {
                byte[] data = new byte[length];
                for (int i = 0; i < length; i++) {
                    data[i] = io.questdb.std.Unsafe.getUnsafe().getByte(payload + i);
                }
                handler.onPong(ClientHandler.this, data);
            }

            @Override
            public void onClose(int code, long reason, int reasonLength) {
                closeCode = code;
                if (reasonLength > 0) {
                    byte[] data = new byte[reasonLength];
                    for (int i = 0; i < reasonLength; i++) {
                        data[i] = io.questdb.std.Unsafe.getUnsafe().getByte(reason + i);
                    }
                    closeReason = new String(data, StandardCharsets.UTF_8);
                }
                handler.onClose(ClientHandler.this, code, closeReason);
                // Send close response back to complete the handshake
                try {
                    context.sendCloseFrame(code >= 0 ? code : WebSocketCloseCode.NORMAL_CLOSURE, closeReason);
                    flushSendBuffer();
                } catch (IOException e) {
                    // Ignore - client may have already disconnected
                }
                ClientHandler.this.running.set(false);
            }

            @Override
            public void onError(int errorCode, CharSequence message) {
                handler.onError(ClientHandler.this, new RuntimeException(
                        "WebSocket error: " + errorCode + " - " + message));
            }
        }
    }
}
