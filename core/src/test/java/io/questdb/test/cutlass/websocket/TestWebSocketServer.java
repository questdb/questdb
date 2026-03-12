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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A simple WebSocket server for integration testing.
 * Handles WebSocket frame parsing and writing inline.
 */
public class TestWebSocketServer implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(TestWebSocketServer.class);
    private static final String WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    private final List<ClientHandler> clients = new CopyOnWriteArrayList<>();
    private final WebSocketServerHandler handler;
    private final int port;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private Thread acceptThread;
    private ServerSocket serverSocket;

    public TestWebSocketServer(int port, WebSocketServerHandler handler) {
        this.port = port;
        this.handler = handler;
    }

    public boolean awaitStart(long timeout, TimeUnit unit) throws InterruptedException {
        return startLatch.await(timeout, unit);
    }

    @Override
    public void close() {
        running.set(false);

        for (ClientHandler client : clients) {
            client.close();
        }
        clients.clear();

        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                // Ignore
            }
        }

        if (acceptThread != null) {
            try {
                acceptThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void start() throws IOException {
        if (running.getAndSet(true)) {
            return;
        }

        serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(100);  // Allow checking running flag periodically

        acceptThread = new Thread(() -> {
            startLatch.countDown();
            while (running.get()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    ClientHandler clientHandler = new ClientHandler(clientSocket);
                    clients.add(clientHandler);
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

    /**
     * Interface for handling WebSocket server events.
     */
    public interface WebSocketServerHandler {
        default void onBinaryMessage(ClientHandler client, byte[] data) {
        }
    }

    /**
     * Handles a single WebSocket client connection.
     */
    public class ClientHandler implements QuietCloseable {
        private final WebSocketFrameParser parser = new WebSocketFrameParser();
        private final WebSocketBuffer recvBuffer;
        private final AtomicBoolean running = new AtomicBoolean(false);
        private final WebSocketBuffer sendBuffer;
        private final Socket socket;
        private InputStream in;
        private boolean isClosed;
        private OutputStream out;
        private Thread readThread;

        ClientHandler(Socket socket) {
            this.socket = socket;
            this.recvBuffer = new WebSocketBuffer(65536);
            this.sendBuffer = new WebSocketBuffer(65536);
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

        public synchronized void sendBinary(byte[] data) throws IOException {
            writeDataFrame(data, data.length);
            flushSendBuffer();
        }

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

        public synchronized void sendPing(byte[] data) throws IOException {
            int frameSize = WebSocketFrameWriter.headerSize(data.length, false) + data.length;
            sendBuffer.ensureCapacity(frameSize);

            long writePtr = sendBuffer.writeAddress();
            int written = WebSocketFrameWriter.writePingFrame(writePtr, data, 0, data.length);
            sendBuffer.advanceWrite(written);
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

        private void handleRead() {
            while (recvBuffer.readableBytes() > 0) {
                parser.reset();

                long bufStart = recvBuffer.readAddress();
                long bufEnd = bufStart + recvBuffer.readableBytes();

                int consumed = parser.parse(bufStart, bufEnd);

                if (parser.getState() == WebSocketFrameParser.STATE_ERROR) {
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
                    case WebSocketOpcode.BINARY -> {
                        byte[] data = new byte[(int) payloadLength];
                        for (int i = 0; i < (int) payloadLength; i++) {
                            data[i] = Unsafe.getUnsafe().getByte(payloadPtr + i);
                        }
                        handler.onBinaryMessage(this, data);
                    }
                    case WebSocketOpcode.PING -> writePongFrame(payloadPtr, (int) payloadLength);
                    case WebSocketOpcode.CLOSE -> {
                        int code = WebSocketCloseCode.NORMAL_CLOSURE;
                        if (payloadLength >= 2) {
                            int high = Unsafe.getUnsafe().getByte(payloadPtr) & 0xFF;
                            int low = Unsafe.getUnsafe().getByte(payloadPtr + 1) & 0xFF;
                            code = (high << 8) | low;
                        }
                        try {
                            sendClose(code, null);
                        } catch (IOException e) {
                            // Ignore - client may have already disconnected
                        }
                        ClientHandler.this.running.set(false);
                        isClosed = true;
                    }
                }

                recvBuffer.skip(consumed);
            }

            recvBuffer.compact();
        }

        private boolean performHandshake() throws IOException {
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
                    return false;
                }
            }

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

            String acceptKey = computeAcceptKey(key);

            String response = "HTTP/1.1 101 Switching Protocols\r\n" +
                    "Upgrade: websocket\r\n" +
                    "Connection: Upgrade\r\n" +
                    "Sec-WebSocket-Accept: " + acceptKey + "\r\n" +
                    "\r\n";
            out.write(response.getBytes(StandardCharsets.US_ASCII));
            out.flush();

            return true;
        }

        private void writeDataFrame(byte[] data, int length) {
            int headerSize = WebSocketFrameWriter.headerSize(length, false);
            int frameSize = headerSize + length;
            sendBuffer.ensureCapacity(frameSize);

            long writePtr = sendBuffer.writeAddress();
            int written = WebSocketFrameWriter.writeHeader(writePtr, true, WebSocketOpcode.BINARY, length, false);

            for (int i = 0; i < length; i++) {
                Unsafe.getUnsafe().putByte(writePtr + written + i, data[i]);
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
                    socket.setSoTimeout(100);

                    in = socket.getInputStream();
                    out = socket.getOutputStream();

                    if (!performHandshake()) {
                        LOG.error().$("Handshake failed").$();
                        return;
                    }

                    byte[] readBuffer = new byte[8192];

                    while (running.get() && !isClosed) {
                        int read;
                        try {
                            read = in.read(readBuffer);
                        } catch (SocketTimeoutException e) {
                            continue;
                        }
                        if (read <= 0) {
                            break;
                        }

                        recvBuffer.write(readBuffer, 0, read);
                        handleRead();

                        flushSendBuffer();
                    }
                } catch (IOException e) {
                    if (running.get()) {
                        LOG.error().$("Client error: ").$(e).$();
                    }
                } finally {
                    recvBuffer.close();
                    sendBuffer.close();
                    clients.remove(this);
                }
            }, "WebSocket-Client-" + socket.getPort());
            readThread.start();
        }
    }
}
