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

package io.questdb.cutlass.http.client;

import io.questdb.HttpClientConfiguration;
import io.questdb.cutlass.ilpv4.websocket.WebSocketCloseCode;
import io.questdb.cutlass.ilpv4.websocket.WebSocketFrameParser;
import io.questdb.cutlass.ilpv4.websocket.WebSocketHandshake;
import io.questdb.cutlass.ilpv4.websocket.WebSocketOpcode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.IOOperation;
import io.questdb.network.NetworkFacade;
import io.questdb.network.Socket;
import io.questdb.network.SocketFactory;
import io.questdb.network.TlsSessionInitFailedException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Utf8String;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Zero-GC WebSocket client built on QuestDB's native socket infrastructure.
 * <p>
 * This client uses native memory buffers and non-blocking I/O with
 * platform-specific event notification (epoll/kqueue/select).
 * <p>
 * Features:
 * <ul>
 *   <li>Zero-copy send path using {@link WebSocketSendBuffer}</li>
 *   <li>Automatic ping/pong handling</li>
 *   <li>TLS support</li>
 *   <li>Connection keep-alive</li>
 * </ul>
 * <p>
 * Thread safety: This class is NOT thread-safe. Each connection should be
 * accessed from a single thread at a time.
 */
public abstract class WebSocketClient implements QuietCloseable {

    private static final Log LOG = LogFactory.getLog(WebSocketClient.class);

    private static final int DEFAULT_RECV_BUFFER_SIZE = 65536;
    private static final int DEFAULT_SEND_BUFFER_SIZE = 65536;

    protected final NetworkFacade nf;
    protected final Socket socket;

    private final WebSocketSendBuffer sendBuffer;
    private final WebSocketFrameParser frameParser;
    private final Rnd rnd;
    private final int defaultTimeout;

    // Receive buffer (native memory)
    private long recvBufPtr;
    private int recvBufSize;
    private int recvPos;      // Write position
    private int recvReadPos;  // Read position

    // Connection state
    private CharSequence host;
    private int port;
    private boolean upgraded;
    private boolean closed;

    // Handshake key for verification
    private String handshakeKey;

    public WebSocketClient(HttpClientConfiguration configuration, SocketFactory socketFactory) {
        this.nf = configuration.getNetworkFacade();
        this.socket = socketFactory.newInstance(nf, LOG);
        this.defaultTimeout = configuration.getTimeout();

        int sendBufSize = Math.max(configuration.getInitialRequestBufferSize(), DEFAULT_SEND_BUFFER_SIZE);
        int maxSendBufSize = Math.max(configuration.getMaximumRequestBufferSize(), sendBufSize);
        this.sendBuffer = new WebSocketSendBuffer(sendBufSize, maxSendBufSize);

        this.recvBufSize = Math.max(configuration.getResponseBufferSize(), DEFAULT_RECV_BUFFER_SIZE);
        this.recvBufPtr = Unsafe.malloc(recvBufSize, MemoryTag.NATIVE_DEFAULT);
        this.recvPos = 0;
        this.recvReadPos = 0;

        this.frameParser = new WebSocketFrameParser();
        this.rnd = new Rnd(System.nanoTime(), System.currentTimeMillis());
        this.upgraded = false;
        this.closed = false;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;

            // Try to send close frame
            if (upgraded && !socket.isClosed()) {
                try {
                    sendCloseFrame(WebSocketCloseCode.NORMAL_CLOSURE, null, 1000);
                } catch (Exception e) {
                    // Ignore errors during close
                }
            }

            disconnect();
            sendBuffer.close();

            if (recvBufPtr != 0) {
                Unsafe.free(recvBufPtr, recvBufSize, MemoryTag.NATIVE_DEFAULT);
                recvBufPtr = 0;
            }
        }
    }

    /**
     * Disconnects the socket without closing the client.
     * The client can be reconnected by calling connect() again.
     */
    public void disconnect() {
        Misc.free(socket);
        upgraded = false;
        host = null;
        port = 0;
        recvPos = 0;
        recvReadPos = 0;
    }

    /**
     * Connects to a WebSocket server.
     *
     * @param host    the server hostname
     * @param port    the server port
     * @param timeout connection timeout in milliseconds
     */
    public void connect(CharSequence host, int port, int timeout) {
        if (closed) {
            throw new HttpClientException("WebSocket client is closed");
        }

        // Close existing connection if connecting to different host:port
        if (this.host != null && (!this.host.equals(host) || this.port != port)) {
            disconnect();
        }

        if (socket.isClosed()) {
            doConnect(host, port, timeout);
        }

        this.host = host;
        this.port = port;
    }

    /**
     * Connects using default timeout.
     */
    public void connect(CharSequence host, int port) {
        connect(host, port, defaultTimeout);
    }

    private void doConnect(CharSequence host, int port, int timeout) {
        long fd = nf.socketTcp(true);
        if (fd < 0) {
            throw new HttpClientException("could not allocate a file descriptor [errno=").errno(nf.errno()).put(']');
        }

        if (nf.setTcpNoDelay(fd, true) < 0) {
            LOG.info().$("could not disable Nagle's algorithm [fd=").$(fd)
                    .$(", errno=").$(nf.errno()).I$();
        }

        socket.of(fd);
        nf.configureKeepAlive(fd);

        long addrInfo = nf.getAddrInfo(host, port);
        if (addrInfo == -1) {
            disconnect();
            throw new HttpClientException("could not resolve host [host=").put(host).put(']');
        }

        if (nf.connectAddrInfo(fd, addrInfo) != 0) {
            int errno = nf.errno();
            nf.freeAddrInfo(addrInfo);
            disconnect();
            throw new HttpClientException("could not connect [host=").put(host)
                    .put(", port=").put(port)
                    .put(", errno=").put(errno).put(']');
        }
        nf.freeAddrInfo(addrInfo);

        if (nf.configureNonBlocking(fd) < 0) {
            int errno = nf.errno();
            disconnect();
            throw new HttpClientException("could not configure non-blocking [fd=").put(fd)
                    .put(", errno=").put(errno).put(']');
        }

        if (socket.supportsTls()) {
            try {
                socket.startTlsSession(host);
            } catch (TlsSessionInitFailedException e) {
                int errno = nf.errno();
                disconnect();
                throw new HttpClientException("could not start TLS session [fd=").put(fd)
                        .put(", error=").put(e.getFlyweightMessage())
                        .put(", errno=").put(errno).put(']');
            }
        }

        setupIoWait();
        LOG.debug().$("Connected to [host=").$(host).$(", port=").$(port).I$();
    }

    /**
     * Performs WebSocket upgrade handshake.
     *
     * @param path    the WebSocket endpoint path (e.g., "/ws")
     * @param timeout timeout in milliseconds
     */
    public void upgrade(CharSequence path, int timeout) {
        if (closed) {
            throw new HttpClientException("WebSocket client is closed");
        }
        if (socket.isClosed()) {
            throw new HttpClientException("Not connected");
        }
        if (upgraded) {
            return; // Already upgraded
        }

        // Generate random key
        byte[] keyBytes = new byte[16];
        for (int i = 0; i < 16; i++) {
            keyBytes[i] = (byte) rnd.nextInt(256);
        }
        handshakeKey = Base64.getEncoder().encodeToString(keyBytes);

        // Build upgrade request
        sendBuffer.reset();
        sendBuffer.putAscii("GET ");
        sendBuffer.putAscii(path);
        sendBuffer.putAscii(" HTTP/1.1\r\n");
        sendBuffer.putAscii("Host: ");
        sendBuffer.putAscii(host);
        if ((socket.supportsTls() && port != 443) || (!socket.supportsTls() && port != 80)) {
            sendBuffer.putAscii(":");
            sendBuffer.putAscii(Integer.toString(port));
        }
        sendBuffer.putAscii("\r\n");
        sendBuffer.putAscii("Upgrade: websocket\r\n");
        sendBuffer.putAscii("Connection: Upgrade\r\n");
        sendBuffer.putAscii("Sec-WebSocket-Key: ");
        sendBuffer.putAscii(handshakeKey);
        sendBuffer.putAscii("\r\n");
        sendBuffer.putAscii("Sec-WebSocket-Version: 13\r\n");
        sendBuffer.putAscii("\r\n");

        // Send request
        long startTime = System.nanoTime();
        doSend(sendBuffer.getBufferPtr(), sendBuffer.getWritePos(), timeout);

        // Read response
        int remainingTimeout = remainingTime(timeout, startTime);
        readUpgradeResponse(remainingTimeout);

        upgraded = true;
        sendBuffer.reset();
        LOG.debug().$("WebSocket upgraded [path=").$(path).I$();
    }

    /**
     * Performs upgrade with default timeout.
     */
    public void upgrade(CharSequence path) {
        upgrade(path, defaultTimeout);
    }

    private void readUpgradeResponse(int timeout) {
        // Read HTTP response into receive buffer
        long startTime = System.nanoTime();

        while (true) {
            int remainingTimeout = remainingTime(timeout, startTime);
            int bytesRead = recvOrDie(recvBufPtr + recvPos, recvBufSize - recvPos, remainingTimeout);
            if (bytesRead > 0) {
                recvPos += bytesRead;
            }

            // Check for end of headers (\r\n\r\n)
            int headerEnd = findHeaderEnd();
            if (headerEnd > 0) {
                validateUpgradeResponse(headerEnd);
                // Compact buffer - move remaining data to start
                int remaining = recvPos - headerEnd;
                if (remaining > 0) {
                    Vect.memmove(recvBufPtr, recvBufPtr + headerEnd, remaining);
                }
                recvPos = remaining;
                recvReadPos = 0;
                return;
            }

            if (recvPos >= recvBufSize) {
                throw new HttpClientException("HTTP response too large");
            }
        }
    }

    private int findHeaderEnd() {
        // Look for \r\n\r\n
        for (int i = 0; i < recvPos - 3; i++) {
            if (Unsafe.getUnsafe().getByte(recvBufPtr + i) == '\r' &&
                Unsafe.getUnsafe().getByte(recvBufPtr + i + 1) == '\n' &&
                Unsafe.getUnsafe().getByte(recvBufPtr + i + 2) == '\r' &&
                Unsafe.getUnsafe().getByte(recvBufPtr + i + 3) == '\n') {
                return i + 4;
            }
        }
        return -1;
    }

    private void validateUpgradeResponse(int headerEnd) {
        // Extract response as string for parsing
        byte[] responseBytes = new byte[headerEnd];
        for (int i = 0; i < headerEnd; i++) {
            responseBytes[i] = Unsafe.getUnsafe().getByte(recvBufPtr + i);
        }
        String response = new String(responseBytes, StandardCharsets.US_ASCII);

        // Check status line
        if (!response.startsWith("HTTP/1.1 101")) {
            String statusLine = response.split("\r\n")[0];
            throw new HttpClientException("WebSocket upgrade failed: ").put(statusLine);
        }

        // Verify Sec-WebSocket-Accept
        String expectedAccept = WebSocketHandshake.computeAcceptKey(handshakeKey);
        if (!response.contains("Sec-WebSocket-Accept: " + expectedAccept)) {
            throw new HttpClientException("Invalid Sec-WebSocket-Accept header");
        }
    }

    // === Sending ===

    /**
     * Gets the send buffer for building WebSocket frames.
     * <p>
     * Usage:
     * <pre>
     * WebSocketSendBuffer buf = client.getSendBuffer();
     * buf.beginBinaryFrame();
     * buf.putLong(data);
     * WebSocketSendBuffer.FrameInfo frame = buf.endBinaryFrame();
     * client.sendFrame(frame, timeout);
     * buf.reset();
     * </pre>
     */
    public WebSocketSendBuffer getSendBuffer() {
        return sendBuffer;
    }

    /**
     * Sends a complete WebSocket frame.
     *
     * @param frame   frame info from endBinaryFrame()
     * @param timeout timeout in milliseconds
     */
    public void sendFrame(WebSocketSendBuffer.FrameInfo frame, int timeout) {
        checkConnected();
        doSend(sendBuffer.getBufferPtr() + frame.offset, frame.length, timeout);
    }

    /**
     * Sends a complete WebSocket frame with default timeout.
     */
    public void sendFrame(WebSocketSendBuffer.FrameInfo frame) {
        sendFrame(frame, defaultTimeout);
    }

    /**
     * Sends binary data as a WebSocket binary frame.
     *
     * @param dataPtr pointer to data
     * @param length  data length
     * @param timeout timeout in milliseconds
     */
    public void sendBinary(long dataPtr, int length, int timeout) {
        checkConnected();
        sendBuffer.reset();
        sendBuffer.beginBinaryFrame();
        sendBuffer.putBlockOfBytes(dataPtr, length);
        WebSocketSendBuffer.FrameInfo frame = sendBuffer.endBinaryFrame();
        doSend(sendBuffer.getBufferPtr() + frame.offset, frame.length, timeout);
        sendBuffer.reset();
    }

    /**
     * Sends binary data with default timeout.
     */
    public void sendBinary(long dataPtr, int length) {
        sendBinary(dataPtr, length, defaultTimeout);
    }

    /**
     * Sends a ping frame.
     */
    public void sendPing(int timeout) {
        checkConnected();
        sendBuffer.reset();
        WebSocketSendBuffer.FrameInfo frame = sendBuffer.writePingFrame();
        doSend(sendBuffer.getBufferPtr() + frame.offset, frame.length, timeout);
        sendBuffer.reset();
    }

    /**
     * Sends a close frame.
     */
    public void sendCloseFrame(int code, String reason, int timeout) {
        sendBuffer.reset();
        WebSocketSendBuffer.FrameInfo frame = sendBuffer.writeCloseFrame(code, reason);
        try {
            doSend(sendBuffer.getBufferPtr() + frame.offset, frame.length, timeout);
        } finally {
            sendBuffer.reset();
        }
    }

    private void doSend(long ptr, int len, int timeout) {
        long startTime = System.nanoTime();
        while (len > 0) {
            int remainingTimeout = remainingTime(timeout, startTime);
            ioWait(remainingTimeout, IOOperation.WRITE);
            int sent = dieIfNegative(socket.send(ptr, len));
            while (socket.wantsTlsWrite()) {
                remainingTimeout = remainingTime(timeout, startTime);
                ioWait(remainingTimeout, IOOperation.WRITE);
                dieIfNegative(socket.tlsIO(Socket.WRITE_FLAG));
            }
            if (sent > 0) {
                ptr += sent;
                len -= sent;
            }
        }
    }

    // === Receiving ===

    /**
     * Receives and processes WebSocket frames.
     *
     * @param handler frame handler callback
     * @param timeout timeout in milliseconds
     * @return true if a frame was received, false on timeout
     */
    public boolean receiveFrame(WebSocketFrameHandler handler, int timeout) {
        checkConnected();

        // First, try to parse any data already in buffer
        Boolean result = tryParseFrame(handler);
        if (result != null) {
            return result;
        }

        // Need more data
        long startTime = System.nanoTime();
        while (true) {
            int remainingTimeout = remainingTime(timeout, startTime);
            if (remainingTimeout <= 0) {
                return false; // Timeout
            }

            // Ensure buffer has space
            if (recvPos >= recvBufSize - 1024) {
                growRecvBuffer();
            }

            int bytesRead = recvOrTimeout(recvBufPtr + recvPos, recvBufSize - recvPos, remainingTimeout);
            if (bytesRead <= 0) {
                return false; // Timeout
            }
            recvPos += bytesRead;

            result = tryParseFrame(handler);
            if (result != null) {
                return result;
            }
        }
    }

    /**
     * Receives frame with default timeout.
     */
    public boolean receiveFrame(WebSocketFrameHandler handler) {
        return receiveFrame(handler, defaultTimeout);
    }

    /**
     * Non-blocking attempt to receive a WebSocket frame.
     * Returns immediately if no complete frame is available.
     *
     * @param handler frame handler callback
     * @return true if a frame was received, false if no data available
     */
    public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
        checkConnected();

        // First, try to parse any data already in buffer
        Boolean result = tryParseFrame(handler);
        if (result != null) {
            return result;
        }

        // Try one non-blocking recv
        if (recvPos >= recvBufSize - 1024) {
            growRecvBuffer();
        }

        int n = socket.recv(recvBufPtr + recvPos, recvBufSize - recvPos);
        if (n < 0) {
            throw new HttpClientException("peer disconnect [errno=").errno(nf.errno()).put(']');
        }
        if (n == 0) {
            return false; // No data available
        }
        recvPos += n;

        // Try to parse again
        result = tryParseFrame(handler);
        return result != null && result;
    }

    private Boolean tryParseFrame(WebSocketFrameHandler handler) {
        if (recvPos <= recvReadPos) {
            return null; // No data
        }

        frameParser.reset();
        int consumed = frameParser.parse(recvBufPtr + recvReadPos, recvBufPtr + recvPos);

        if (frameParser.getState() == WebSocketFrameParser.STATE_NEED_MORE ||
            frameParser.getState() == WebSocketFrameParser.STATE_NEED_PAYLOAD) {
            return null; // Need more data
        }

        if (frameParser.getState() == WebSocketFrameParser.STATE_ERROR) {
            throw new HttpClientException("WebSocket frame parse error: ")
                    .put(WebSocketCloseCode.describe(frameParser.getErrorCode()));
        }

        if (frameParser.getState() == WebSocketFrameParser.STATE_COMPLETE) {
            long payloadPtr = recvBufPtr + recvReadPos + frameParser.getHeaderSize();
            int payloadLen = (int) frameParser.getPayloadLength();

            // Unmask if needed (server frames should not be masked)
            if (frameParser.isMasked()) {
                frameParser.unmaskPayload(payloadPtr, payloadLen);
            }

            // Handle frame by opcode
            int opcode = frameParser.getOpcode();
            switch (opcode) {
                case WebSocketOpcode.PING:
                    // Auto-respond with pong
                    sendPongFrame(payloadPtr, payloadLen);
                    if (handler != null) {
                        handler.onPing(payloadPtr, payloadLen);
                    }
                    break;
                case WebSocketOpcode.PONG:
                    if (handler != null) {
                        handler.onPong(payloadPtr, payloadLen);
                    }
                    break;
                case WebSocketOpcode.CLOSE:
                    upgraded = false;
                    if (handler != null) {
                        int closeCode = 0;
                        String reason = null;
                        if (payloadLen >= 2) {
                            closeCode = ((Unsafe.getUnsafe().getByte(payloadPtr) & 0xFF) << 8)
                                    | (Unsafe.getUnsafe().getByte(payloadPtr + 1) & 0xFF);
                            if (payloadLen > 2) {
                                byte[] reasonBytes = new byte[payloadLen - 2];
                                for (int i = 0; i < reasonBytes.length; i++) {
                                    reasonBytes[i] = Unsafe.getUnsafe().getByte(payloadPtr + 2 + i);
                                }
                                reason = new String(reasonBytes, StandardCharsets.UTF_8);
                            }
                        }
                        handler.onClose(closeCode, reason);
                    }
                    break;
                case WebSocketOpcode.BINARY:
                    if (handler != null) {
                        handler.onBinaryMessage(payloadPtr, payloadLen);
                    }
                    break;
                case WebSocketOpcode.TEXT:
                    if (handler != null) {
                        handler.onTextMessage(payloadPtr, payloadLen);
                    }
                    break;
            }

            // Advance read position
            recvReadPos += consumed;

            // Compact buffer if needed
            compactRecvBuffer();

            return true;
        }

        return false;
    }

    private void sendPongFrame(long payloadPtr, int payloadLen) {
        try {
            sendBuffer.reset();
            WebSocketSendBuffer.FrameInfo frame = sendBuffer.writePongFrame(payloadPtr, payloadLen);
            doSend(sendBuffer.getBufferPtr() + frame.offset, frame.length, 1000); // Short timeout for pong
            sendBuffer.reset();
        } catch (Exception e) {
            LOG.error().$("Failed to send pong: ").$(e.getMessage()).I$();
        }
    }

    private void compactRecvBuffer() {
        if (recvReadPos > 0) {
            int remaining = recvPos - recvReadPos;
            if (remaining > 0) {
                Vect.memmove(recvBufPtr, recvBufPtr + recvReadPos, remaining);
            }
            recvPos = remaining;
            recvReadPos = 0;
        }
    }

    private void growRecvBuffer() {
        int newSize = recvBufSize * 2;
        recvBufPtr = Unsafe.realloc(recvBufPtr, recvBufSize, newSize, MemoryTag.NATIVE_DEFAULT);
        recvBufSize = newSize;
    }

    // === Socket I/O helpers ===

    private int recvOrDie(long ptr, int len, int timeout) {
        long startTime = System.nanoTime();
        int n = dieIfNegative(socket.recv(ptr, len));
        if (n == 0) {
            ioWait(remainingTime(timeout, startTime), IOOperation.READ);
            n = dieIfNegative(socket.recv(ptr, len));
        }
        return n;
    }

    private int recvOrTimeout(long ptr, int len, int timeout) {
        long startTime = System.nanoTime();
        int n = socket.recv(ptr, len);
        if (n < 0) {
            throw new HttpClientException("peer disconnect [errno=").errno(nf.errno()).put(']');
        }
        if (n == 0) {
            try {
                ioWait(timeout, IOOperation.READ);
            } catch (HttpClientException e) {
                // Timeout
                return 0;
            }
            n = socket.recv(ptr, len);
            if (n < 0) {
                throw new HttpClientException("peer disconnect [errno=").errno(nf.errno()).put(']');
            }
        }
        return n;
    }

    private int dieIfNegative(int byteCount) {
        if (byteCount < 0) {
            throw new HttpClientException("peer disconnect [errno=").errno(nf.errno()).put(']');
        }
        return byteCount;
    }

    private int remainingTime(int timeoutMillis, long startTimeNanos) {
        timeoutMillis -= (int) NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
        if (timeoutMillis <= 0) {
            throw new HttpClientException("timed out [errno=").errno(nf.errno()).put(']');
        }
        return timeoutMillis;
    }

    protected void dieWaiting(int n) {
        if (n == 1) {
            return;
        }
        if (n == 0) {
            throw new HttpClientException("timed out [errno=").put(nf.errno()).put(']');
        }
        throw new HttpClientException("queue error [errno=").put(nf.errno()).put(']');
    }

    private void checkConnected() {
        if (closed) {
            throw new HttpClientException("WebSocket client is closed");
        }
        if (!upgraded) {
            throw new HttpClientException("WebSocket not connected or upgraded");
        }
    }

    // === State ===

    /**
     * Returns whether the WebSocket is connected and upgraded.
     */
    public boolean isConnected() {
        return upgraded && !closed && !socket.isClosed();
    }

    /**
     * Returns the connected host.
     */
    public CharSequence getHost() {
        return host;
    }

    /**
     * Returns the connected port.
     */
    public int getPort() {
        return port;
    }

    // === Platform-specific I/O ===

    /**
     * Waits for I/O readiness using platform-specific mechanism.
     *
     * @param timeout timeout in milliseconds
     * @param op      I/O operation (READ or WRITE)
     */
    protected abstract void ioWait(int timeout, int op);

    /**
     * Sets up platform-specific I/O wait mechanism after connection.
     */
    protected abstract void setupIoWait();
}
