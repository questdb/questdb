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

package io.questdb.test.cutlass.websocket;

import io.questdb.cutlass.qwp.server.WebSocketBuffer;
import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.cutlass.qwp.websocket.WebSocketFrameParser;
import io.questdb.cutlass.qwp.websocket.WebSocketFrameWriter;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple WebSocket server for integration testing.
 * Handles WebSocket frame parsing and writing inline.
 */
public class TestWebSocketServer implements QuietCloseable {
    private static final String DEFAULT_KEYSTORE = "/keystore/server.keystore";
    private static final char[] DEFAULT_KEYSTORE_PASSWORD = "questdb".toCharArray();
    private static final Log LOG = LogFactory.getLog(TestWebSocketServer.class);
    private static final String WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    private final List<ClientHandler> clients = new CopyOnWriteArrayList<>();
    private final AtomicInteger connectionCount = new AtomicInteger(0);
    private final WebSocketServerHandler handler;
    private final String keystore;
    private final char[] keystorePassword;
    private final int port;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final boolean tlsEnabled;
    private Thread acceptThread;
    private ServerSocket serverSocket;

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

    public boolean awaitStart(long timeout, TimeUnit unit) throws InterruptedException {
        return startLatch.await(timeout, unit);
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

    public int getActiveConnectionCount() {
        return clients.size();
    }

    public int getConnectionCount() {
        return connectionCount.get();
    }

    public int getPort() {
        return port;
    }

    public boolean isTlsEnabled() {
        return tlsEnabled;
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

    /**
     * Interface for handling WebSocket server events.
     */
    public interface WebSocketServerHandler {
        default void onBinaryMessage(ClientHandler client, byte[] data) {
        }

        default void onClose(ClientHandler client, int code, String reason) {
        }

        default void onConnect(ClientHandler client) {
        }

        default void onDisconnect(ClientHandler client) {
        }

        default void onError(ClientHandler client, Exception e) {
        }

        default void onPing(ClientHandler client, byte[] data) {
        }

        default void onPong(ClientHandler client, byte[] data) {
        }

        default void onTextMessage(ClientHandler client, String text) {
        }
    }

    /**
     * Handles a single WebSocket client connection.
     */
    public class ClientHandler implements QuietCloseable {
        private final CountDownLatch closeLatch = new CountDownLatch(1);
        private final WebSocketFrameParser parser = new WebSocketFrameParser();
        private final WebSocketBuffer recvBuffer;
        private final AtomicBoolean running = new AtomicBoolean(false);
        private final WebSocketBuffer sendBuffer;
        private final Socket socket;
        private volatile int closeCode = -1;
        private volatile String closeReason;
        private InputStream in;
        private boolean isClosed;
        private OutputStream out;
        private Thread readThread;

        ClientHandler(Socket socket) {
            this.socket = socket;
            this.recvBuffer = new WebSocketBuffer(65536);
            this.sendBuffer = new WebSocketBuffer(65536);
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
            recvBuffer.close();
            sendBuffer.close();
        }

        /**
         * Sends a binary message to the client.
         */
        public synchronized void sendBinary(byte[] data) throws IOException {
            writeDataFrame(WebSocketOpcode.BINARY, data, 0, data.length);
            flushSendBuffer();
        }

        /**
         * Sends a close frame to the client.
         */
        public synchronized void sendClose(int code, String reason) throws IOException {
            int maxPayloadLen = 2;
            if (reason != null && !reason.isEmpty()) {
                maxPayloadLen += reason.length() * 3;
            }
            int maxFrameSize = WebSocketFrameWriter.headerSize(maxPayloadLen, false) + maxPayloadLen;
            sendBuffer.ensureCapacity(maxFrameSize);

            long writePtr = sendBuffer.writeAddress();
            int written = WebSocketFrameWriter.writeCloseFrame(writePtr, code, reason);
            sendBuffer.advanceWrite(written);
            flushSendBuffer();
        }

        /**
         * Sends a ping frame to the client.
         */
        public synchronized void sendPing(byte[] data) throws IOException {
            int frameSize = WebSocketFrameWriter.headerSize(data.length, false) + data.length;
            sendBuffer.ensureCapacity(frameSize);

            long writePtr = sendBuffer.writeAddress();
            int written = WebSocketFrameWriter.writePingFrame(writePtr, data, 0, data.length);
            sendBuffer.advanceWrite(written);
            flushSendBuffer();
        }

        /**
         * Sends a text message to the client.
         */
        public synchronized void sendText(String text) throws IOException {
            byte[] data = text.getBytes(StandardCharsets.UTF_8);
            writeDataFrame(WebSocketOpcode.TEXT, data, 0, data.length);
            flushSendBuffer();
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
            if (sendBuffer.readableBytes() > 0) {
                byte[] data = sendBuffer.toByteArray();
                out.write(data);
                out.flush();
                sendBuffer.clear();
            }
        }

        private void handleCloseFrame(long payloadPtr, int payloadLength, WebSocketProcessor processor) {
            int code = -1;
            long reasonPtr = 0;
            int reasonLength = 0;

            if (payloadLength >= 2) {
                int high = Unsafe.getUnsafe().getByte(payloadPtr) & 0xFF;
                int low = Unsafe.getUnsafe().getByte(payloadPtr + 1) & 0xFF;
                code = (high << 8) | low;
                reasonPtr = payloadPtr + 2;
                reasonLength = payloadLength - 2;
            }

            processor.onClose(code, reasonPtr, reasonLength);
        }

        private void handleRead(WebSocketProcessor processor) {
            while (recvBuffer.readableBytes() > 0) {
                parser.reset();

                long bufStart = recvBuffer.readAddress();
                long bufEnd = bufStart + recvBuffer.readableBytes();

                int consumed = parser.parse(bufStart, bufEnd);

                if (parser.getState() == WebSocketFrameParser.STATE_ERROR) {
                    processor.onError(parser.getErrorCode(), "Protocol error");
                    return;
                }

                if (consumed == 0 || parser.getState() == WebSocketFrameParser.STATE_NEED_MORE) {
                    break;
                }

                if (parser.getState() == WebSocketFrameParser.STATE_NEED_PAYLOAD) {
                    break;
                }

                int opcode = parser.getOpcode();
                long payloadLength = parser.getPayloadLength();
                int headerSize = parser.getHeaderSize();
                long payloadPtr = bufStart + headerSize;

                if (parser.isMasked()) {
                    parser.unmaskPayload(payloadPtr, payloadLength);
                }

                switch (opcode) {
                    case WebSocketOpcode.BINARY -> processor.onBinaryMessage(payloadPtr, (int) payloadLength);
                    case WebSocketOpcode.TEXT -> processor.onTextMessage(payloadPtr, (int) payloadLength);
                    case WebSocketOpcode.PING -> {
                        processor.onPing(payloadPtr, (int) payloadLength);
                        writePongFrame(payloadPtr, (int) payloadLength);
                    }
                    case WebSocketOpcode.PONG -> processor.onPong(payloadPtr, (int) payloadLength);
                    case WebSocketOpcode.CLOSE -> {
                        handleCloseFrame(payloadPtr, (int) payloadLength, processor);
                        isClosed = true;
                    }
                    default -> processor.onError(WebSocketCloseCode.PROTOCOL_ERROR, "Unknown opcode");
                }

                recvBuffer.skip(consumed);
            }

            recvBuffer.compact();
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

        private void writeDataFrame(int opcode, byte[] data, int offset, int length) {
            int headerSize = WebSocketFrameWriter.headerSize(length, false);
            int frameSize = headerSize + length;
            sendBuffer.ensureCapacity(frameSize);

            long writePtr = sendBuffer.writeAddress();
            int written = WebSocketFrameWriter.writeHeader(writePtr, true, opcode, length, false);

            for (int i = 0; i < length; i++) {
                Unsafe.getUnsafe().putByte(writePtr + written + i, data[offset + i]);
            }
            sendBuffer.advanceWrite(written + length);
        }

        private void writePongFrame(long payloadPtr, int payloadLength) {
            int frameSize = WebSocketFrameWriter.headerSize(payloadLength, false) + payloadLength;
            sendBuffer.ensureCapacity(frameSize);

            long writePtr = sendBuffer.writeAddress();
            int written;
            if (payloadLength > 0) {
                written = WebSocketFrameWriter.writePongFrame(writePtr, payloadPtr, payloadLength);
            } else {
                written = WebSocketFrameWriter.writeHeader(writePtr, true, WebSocketOpcode.PONG, 0, false);
            }
            sendBuffer.advanceWrite(written);
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

                    while (running.get() && !isClosed) {
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

                        recvBuffer.write(readBuffer, 0, read);
                        handleRead(processor);

                        // Flush send buffer (pongs are sent immediately during handleRead)
                        flushSendBuffer();
                    }
                } catch (IOException e) {
                    if (running.get()) {
                        handler.onError(this, e);
                    }
                } finally {
                    recvBuffer.close();
                    sendBuffer.close();
                    handler.onDisconnect(this);
                    clients.remove(this);
                    closeLatch.countDown();
                }
            }, "WebSocket-Client-" + socket.getPort());
            readThread.start();
        }

        private class ServerProcessor implements WebSocketProcessor {
            @Override
            public void onBinaryMessage(long payload, int length) {
                byte[] data = new byte[length];
                for (int i = 0; i < length; i++) {
                    data[i] = Unsafe.getUnsafe().getByte(payload + i);
                }
                handler.onBinaryMessage(ClientHandler.this, data);
            }

            @Override
            public void onClose(int code, long reason, int reasonLength) {
                closeCode = code;
                if (reasonLength > 0) {
                    byte[] data = new byte[reasonLength];
                    for (int i = 0; i < reasonLength; i++) {
                        data[i] = Unsafe.getUnsafe().getByte(reason + i);
                    }
                    closeReason = new String(data, StandardCharsets.UTF_8);
                }
                handler.onClose(ClientHandler.this, code, closeReason);
                // Send close response back to complete the handshake
                try {
                    sendClose(code >= 0 ? code : WebSocketCloseCode.NORMAL_CLOSURE, closeReason);
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

            @Override
            public void onPing(long payload, int length) {
                byte[] data = new byte[length];
                for (int i = 0; i < length; i++) {
                    data[i] = Unsafe.getUnsafe().getByte(payload + i);
                }
                handler.onPing(ClientHandler.this, data);
            }

            @Override
            public void onPong(long payload, int length) {
                byte[] data = new byte[length];
                for (int i = 0; i < length; i++) {
                    data[i] = Unsafe.getUnsafe().getByte(payload + i);
                }
                handler.onPong(ClientHandler.this, data);
            }

            @Override
            public void onTextMessage(long payload, int length) {
                byte[] data = new byte[length];
                for (int i = 0; i < length; i++) {
                    data[i] = Unsafe.getUnsafe().getByte(payload + i);
                }
                String text = new String(data, StandardCharsets.UTF_8);
                handler.onTextMessage(ClientHandler.this, text);
            }
        }
    }
}
