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

package io.questdb.cutlass.ilpv4.client;

import io.questdb.cutlass.ilpv4.websocket.WebSocketCloseCode;
import io.questdb.cutlass.ilpv4.websocket.WebSocketFrameParser;
import io.questdb.cutlass.ilpv4.websocket.WebSocketFrameWriter;
import io.questdb.cutlass.ilpv4.websocket.WebSocketHandshake;
import io.questdb.cutlass.ilpv4.websocket.WebSocketOpcode;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Base64;

/**
 * WebSocket client channel for ILP v4 binary streaming.
 * <p>
 * This class handles:
 * <ul>
 *   <li>HTTP upgrade handshake to establish WebSocket connection</li>
 *   <li>Binary frame encoding with client-side masking (RFC 6455)</li>
 *   <li>Ping/pong for connection keepalive</li>
 *   <li>Close handshake</li>
 * </ul>
 * <p>
 * Thread safety: This class is NOT thread-safe. It should only be accessed
 * from a single thread at a time.
 */
public class WebSocketChannel implements QuietCloseable {

    private static final int DEFAULT_BUFFER_SIZE = 65536;
    private static final int MAX_FRAME_HEADER_SIZE = 14; // 2 + 8 + 4 (header + extended len + mask)

    // Connection state
    private final String host;
    private final int port;
    private final String path;
    private final boolean tlsEnabled;
    private final boolean tlsValidationEnabled;

    // Socket I/O
    private Socket socket;
    private InputStream in;
    private OutputStream out;

    // Pre-allocated send buffer (native memory)
    private long sendBufferPtr;
    private int sendBufferSize;

    // Pre-allocated receive buffer (native memory)
    private long recvBufferPtr;
    private int recvBufferSize;
    private int recvBufferPos;      // Write position
    private int recvBufferReadPos;  // Read position

    // Frame parser (reused)
    private final WebSocketFrameParser frameParser;

    // Random for mask key generation
    private final Rnd rnd;

    // Timeouts
    private int connectTimeoutMs = 10_000;
    private int readTimeoutMs = 30_000;

    // State
    private boolean connected;
    private boolean closed;

    // Temporary byte array for handshake (allocated once)
    private final byte[] handshakeBuffer = new byte[4096];

    public WebSocketChannel(String url, boolean tlsEnabled) {
        this(url, tlsEnabled, true);
    }

    public WebSocketChannel(String url, boolean tlsEnabled, boolean tlsValidationEnabled) {
        // Parse URL: ws://host:port/path or wss://host:port/path
        String remaining = url;
        if (remaining.startsWith("wss://")) {
            remaining = remaining.substring(6);
            this.tlsEnabled = true;
        } else if (remaining.startsWith("ws://")) {
            remaining = remaining.substring(5);
            this.tlsEnabled = tlsEnabled;
        } else {
            this.tlsEnabled = tlsEnabled;
        }

        int slashIdx = remaining.indexOf('/');
        String hostPort;
        if (slashIdx >= 0) {
            hostPort = remaining.substring(0, slashIdx);
            this.path = remaining.substring(slashIdx);
        } else {
            hostPort = remaining;
            this.path = "/";
        }

        int colonIdx = hostPort.lastIndexOf(':');
        if (colonIdx >= 0) {
            this.host = hostPort.substring(0, colonIdx);
            this.port = Integer.parseInt(hostPort.substring(colonIdx + 1));
        } else {
            this.host = hostPort;
            this.port = this.tlsEnabled ? 443 : 80;
        }

        this.tlsValidationEnabled = tlsValidationEnabled;
        this.frameParser = new WebSocketFrameParser();
        this.rnd = new Rnd(System.nanoTime(), System.currentTimeMillis());

        // Allocate native buffers
        this.sendBufferSize = DEFAULT_BUFFER_SIZE;
        this.sendBufferPtr = Unsafe.malloc(sendBufferSize, MemoryTag.NATIVE_DEFAULT);

        this.recvBufferSize = DEFAULT_BUFFER_SIZE;
        this.recvBufferPtr = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_DEFAULT);
        this.recvBufferPos = 0;
        this.recvBufferReadPos = 0;

        this.connected = false;
        this.closed = false;
    }

    /**
     * Sets the connection timeout.
     */
    public WebSocketChannel setConnectTimeout(int timeoutMs) {
        this.connectTimeoutMs = timeoutMs;
        return this;
    }

    /**
     * Sets the read timeout.
     */
    public WebSocketChannel setReadTimeout(int timeoutMs) {
        this.readTimeoutMs = timeoutMs;
        return this;
    }

    /**
     * Connects to the WebSocket server.
     * Performs TCP connection and HTTP upgrade handshake.
     */
    public void connect() {
        if (connected) {
            return;
        }
        if (closed) {
            throw new LineSenderException("WebSocket channel is closed");
        }

        try {
            // Create socket
            SocketFactory socketFactory = tlsEnabled ? createSslSocketFactory() : SocketFactory.getDefault();
            socket = socketFactory.createSocket();
            socket.connect(new java.net.InetSocketAddress(host, port), connectTimeoutMs);
            socket.setSoTimeout(readTimeoutMs);
            socket.setTcpNoDelay(true);

            in = socket.getInputStream();
            out = socket.getOutputStream();

            // Perform WebSocket handshake
            performHandshake();

            connected = true;
        } catch (IOException e) {
            closeQuietly();
            throw new LineSenderException("Failed to connect to WebSocket server: " + e.getMessage(), e);
        }
    }

    /**
     * Sends binary data as a WebSocket binary frame.
     * The data is read from native memory at the given pointer.
     *
     * @param dataPtr pointer to the data
     * @param length  length of data in bytes
     */
    public void sendBinary(long dataPtr, int length) {
        ensureConnected();
        sendFrame(WebSocketOpcode.BINARY, dataPtr, length);
    }

    /**
     * Sends a ping frame.
     */
    public void sendPing() {
        ensureConnected();
        sendFrame(WebSocketOpcode.PING, 0, 0);
    }

    /**
     * Receives and processes incoming frames.
     * Handles ping/pong automatically.
     *
     * @param handler callback for received binary messages (may be null)
     * @param timeoutMs read timeout in milliseconds
     * @return true if a frame was received, false on timeout
     */
    public boolean receiveFrame(ResponseHandler handler, int timeoutMs) {
        ensureConnected();
        try {
            int oldTimeout = socket.getSoTimeout();
            socket.setSoTimeout(timeoutMs);
            try {
                return doReceiveFrame(handler);
            } finally {
                socket.setSoTimeout(oldTimeout);
            }
        } catch (SocketTimeoutException e) {
            return false;
        } catch (IOException e) {
            throw new LineSenderException("Failed to receive WebSocket frame: " + e.getMessage(), e);
        }
    }

    /**
     * Sends a close frame and closes the connection.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        try {
            if (connected) {
                // Send close frame
                sendCloseFrame(WebSocketCloseCode.NORMAL_CLOSURE, null);
            }
        } catch (Exception e) {
            // Ignore errors during close
        }

        closeQuietly();

        // Free native memory
        if (sendBufferPtr != 0) {
            Unsafe.free(sendBufferPtr, sendBufferSize, MemoryTag.NATIVE_DEFAULT);
            sendBufferPtr = 0;
        }
        if (recvBufferPtr != 0) {
            Unsafe.free(recvBufferPtr, recvBufferSize, MemoryTag.NATIVE_DEFAULT);
            recvBufferPtr = 0;
        }
    }

    public boolean isConnected() {
        return connected && !closed;
    }

    // ==================== Private methods ====================

    private void ensureConnected() {
        if (closed) {
            throw new LineSenderException("WebSocket channel is closed");
        }
        if (!connected) {
            throw new LineSenderException("WebSocket channel is not connected");
        }
    }

    private SocketFactory createSslSocketFactory() {
        try {
            if (!tlsValidationEnabled) {
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, new TrustManager[]{new X509TrustManager() {
                    public void checkClientTrusted(X509Certificate[] certs, String t) {}
                    public void checkServerTrusted(X509Certificate[] certs, String t) {}
                    public X509Certificate[] getAcceptedIssuers() { return null; }
                }}, new SecureRandom());
                return sslContext.getSocketFactory();
            }
            return SSLSocketFactory.getDefault();
        } catch (Exception e) {
            throw new LineSenderException("Failed to create SSL socket factory: " + e.getMessage(), e);
        }
    }

    private void performHandshake() throws IOException {
        // Generate random key (16 bytes, base64 encoded = 24 chars)
        byte[] keyBytes = new byte[16];
        for (int i = 0; i < 16; i++) {
            keyBytes[i] = (byte) rnd.nextInt(256);
        }
        String key = Base64.getEncoder().encodeToString(keyBytes);

        // Build HTTP upgrade request
        StringBuilder request = new StringBuilder();
        request.append("GET ").append(path).append(" HTTP/1.1\r\n");
        request.append("Host: ").append(host);
        if ((tlsEnabled && port != 443) || (!tlsEnabled && port != 80)) {
            request.append(":").append(port);
        }
        request.append("\r\n");
        request.append("Upgrade: websocket\r\n");
        request.append("Connection: Upgrade\r\n");
        request.append("Sec-WebSocket-Key: ").append(key).append("\r\n");
        request.append("Sec-WebSocket-Version: 13\r\n");
        request.append("\r\n");

        // Send request
        byte[] requestBytes = request.toString().getBytes(StandardCharsets.US_ASCII);
        out.write(requestBytes);
        out.flush();

        // Read response
        int responseLen = readHttpResponse();

        // Parse response
        String response = new String(handshakeBuffer, 0, responseLen, StandardCharsets.US_ASCII);

        // Check status line
        if (!response.startsWith("HTTP/1.1 101")) {
            throw new IOException("WebSocket handshake failed: " + response.split("\r\n")[0]);
        }

        // Verify Sec-WebSocket-Accept
        String expectedAccept = WebSocketHandshake.computeAcceptKey(key);
        if (!response.contains("Sec-WebSocket-Accept: " + expectedAccept)) {
            throw new IOException("Invalid Sec-WebSocket-Accept in handshake response");
        }
    }

    private int readHttpResponse() throws IOException {
        int pos = 0;
        int consecutiveCrLf = 0;

        while (pos < handshakeBuffer.length) {
            int b = in.read();
            if (b < 0) {
                throw new IOException("Connection closed during handshake");
            }
            handshakeBuffer[pos++] = (byte) b;

            // Look for \r\n\r\n
            if (b == '\r' || b == '\n') {
                if ((consecutiveCrLf == 0 && b == '\r') ||
                    (consecutiveCrLf == 1 && b == '\n') ||
                    (consecutiveCrLf == 2 && b == '\r') ||
                    (consecutiveCrLf == 3 && b == '\n')) {
                    consecutiveCrLf++;
                    if (consecutiveCrLf == 4) {
                        return pos;
                    }
                } else {
                    consecutiveCrLf = (b == '\r') ? 1 : 0;
                }
            } else {
                consecutiveCrLf = 0;
            }
        }
        throw new IOException("HTTP response too large");
    }

    private void sendFrame(int opcode, long payloadPtr, int payloadLen) {
        // Generate mask key
        int maskKey = rnd.nextInt();

        // Calculate required buffer size
        int headerSize = WebSocketFrameWriter.headerSize(payloadLen, true);
        int frameSize = headerSize + payloadLen;

        // Ensure buffer is large enough
        ensureSendBufferSize(frameSize);

        // Write frame header with mask
        int headerWritten = WebSocketFrameWriter.writeHeader(
                sendBufferPtr, true, opcode, payloadLen, maskKey);

        // Copy payload to buffer after header
        if (payloadLen > 0) {
            Unsafe.getUnsafe().copyMemory(payloadPtr, sendBufferPtr + headerWritten, payloadLen);
            // Mask the payload in place
            WebSocketFrameWriter.maskPayload(sendBufferPtr + headerWritten, payloadLen, maskKey);
        }

        // Send frame
        try {
            writeToSocket(sendBufferPtr, frameSize);
        } catch (IOException e) {
            throw new LineSenderException("Failed to send WebSocket frame: " + e.getMessage(), e);
        }
    }

    private void sendCloseFrame(int code, String reason) {
        int maskKey = rnd.nextInt();

        // Close payload: 2-byte code + optional reason
        int reasonLen = (reason != null) ? reason.length() : 0;
        int payloadLen = 2 + reasonLen;

        int headerSize = WebSocketFrameWriter.headerSize(payloadLen, true);
        int frameSize = headerSize + payloadLen;

        ensureSendBufferSize(frameSize);

        // Write header
        int headerWritten = WebSocketFrameWriter.writeHeader(
                sendBufferPtr, true, WebSocketOpcode.CLOSE, payloadLen, maskKey);

        // Write close code (big-endian)
        long payloadStart = sendBufferPtr + headerWritten;
        Unsafe.getUnsafe().putByte(payloadStart, (byte) ((code >> 8) & 0xFF));
        Unsafe.getUnsafe().putByte(payloadStart + 1, (byte) (code & 0xFF));

        // Write reason if present
        if (reason != null) {
            byte[] reasonBytes = reason.getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < reasonBytes.length; i++) {
                Unsafe.getUnsafe().putByte(payloadStart + 2 + i, reasonBytes[i]);
            }
        }

        // Mask payload
        WebSocketFrameWriter.maskPayload(payloadStart, payloadLen, maskKey);

        try {
            writeToSocket(sendBufferPtr, frameSize);
        } catch (IOException e) {
            // Ignore errors during close
        }
    }

    private boolean doReceiveFrame(ResponseHandler handler) throws IOException {
        // First, try to parse any data already in the buffer
        // This handles the case where multiple frames arrived in a single TCP read
        if (recvBufferPos > recvBufferReadPos) {
            Boolean result = tryParseFrame(handler);
            if (result != null) {
                return result;
            }
            // result == null means we need more data, continue to read
        }

        // Read more data into receive buffer
        int bytesRead = readFromSocket();
        if (bytesRead <= 0) {
            return false;
        }

        // Try parsing again with the new data
        Boolean result = tryParseFrame(handler);
        return result != null && result;
    }

    /**
     * Tries to parse a frame from the receive buffer.
     * @return true if frame processed, false if error, null if need more data
     */
    private Boolean tryParseFrame(ResponseHandler handler) throws IOException {
        frameParser.reset();
        int consumed = frameParser.parse(
                recvBufferPtr + recvBufferReadPos,
                recvBufferPtr + recvBufferPos);

        if (frameParser.getState() == WebSocketFrameParser.STATE_NEED_MORE) {
            return null; // Need more data
        }

        if (frameParser.getState() == WebSocketFrameParser.STATE_ERROR) {
            throw new IOException("WebSocket frame parse error: " + frameParser.getErrorCode());
        }

        if (frameParser.getState() == WebSocketFrameParser.STATE_COMPLETE) {
            long payloadPtr = recvBufferPtr + recvBufferReadPos + frameParser.getHeaderSize();
            int payloadLen = (int) frameParser.getPayloadLength();

            // Handle control frames
            int opcode = frameParser.getOpcode();
            switch (opcode) {
                case WebSocketOpcode.PING:
                    sendPongFrame(payloadPtr, payloadLen);
                    break;
                case WebSocketOpcode.PONG:
                    // Ignore pong
                    break;
                case WebSocketOpcode.CLOSE:
                    connected = false;
                    if (handler != null) {
                        int closeCode = 0;
                        if (payloadLen >= 2) {
                            closeCode = ((Unsafe.getUnsafe().getByte(payloadPtr) & 0xFF) << 8)
                                    | (Unsafe.getUnsafe().getByte(payloadPtr + 1) & 0xFF);
                        }
                        handler.onClose(closeCode, null);
                    }
                    break;
                case WebSocketOpcode.BINARY:
                    if (handler != null) {
                        handler.onBinaryMessage(payloadPtr, payloadLen);
                    }
                    break;
                case WebSocketOpcode.TEXT:
                    // Ignore text frames for now
                    break;
            }

            // Advance read position
            recvBufferReadPos += consumed;

            // Compact buffer if needed
            if (recvBufferReadPos > 0) {
                int remaining = recvBufferPos - recvBufferReadPos;
                if (remaining > 0) {
                    Unsafe.getUnsafe().copyMemory(
                            recvBufferPtr + recvBufferReadPos,
                            recvBufferPtr,
                            remaining);
                }
                recvBufferPos = remaining;
                recvBufferReadPos = 0;
            }

            return true;
        }

        return false;
    }

    private void sendPongFrame(long pingPayloadPtr, int pingPayloadLen) {
        int maskKey = rnd.nextInt();
        int headerSize = WebSocketFrameWriter.headerSize(pingPayloadLen, true);
        int frameSize = headerSize + pingPayloadLen;

        ensureSendBufferSize(frameSize);

        int headerWritten = WebSocketFrameWriter.writeHeader(
                sendBufferPtr, true, WebSocketOpcode.PONG, pingPayloadLen, maskKey);

        if (pingPayloadLen > 0) {
            Unsafe.getUnsafe().copyMemory(pingPayloadPtr, sendBufferPtr + headerWritten, pingPayloadLen);
            WebSocketFrameWriter.maskPayload(sendBufferPtr + headerWritten, pingPayloadLen, maskKey);
        }

        try {
            writeToSocket(sendBufferPtr, frameSize);
        } catch (IOException e) {
            // Ignore pong send errors
        }
    }

    private void ensureSendBufferSize(int required) {
        if (required > sendBufferSize) {
            int newSize = Math.max(required, sendBufferSize * 2);
            sendBufferPtr = Unsafe.realloc(sendBufferPtr, sendBufferSize, newSize, MemoryTag.NATIVE_DEFAULT);
            sendBufferSize = newSize;
        }
    }

    private void writeToSocket(long ptr, int len) throws IOException {
        // Copy to temp array for socket write (unavoidable with OutputStream)
        // Use separate write buffer to avoid race with read thread
        byte[] temp = getWriteTempBuffer(len);
        for (int i = 0; i < len; i++) {
            temp[i] = Unsafe.getUnsafe().getByte(ptr + i);
        }
        out.write(temp, 0, len);
        out.flush();
    }

    private int readFromSocket() throws IOException {
        // Ensure space in receive buffer
        int available = recvBufferSize - recvBufferPos;
        if (available < 1024) {
            // Grow buffer
            int newSize = recvBufferSize * 2;
            recvBufferPtr = Unsafe.realloc(recvBufferPtr, recvBufferSize, newSize, MemoryTag.NATIVE_DEFAULT);
            recvBufferSize = newSize;
            available = recvBufferSize - recvBufferPos;
        }

        // Read into temp array then copy to native buffer
        // Use separate read buffer to avoid race with write thread
        byte[] temp = getReadTempBuffer(available);
        int bytesRead = in.read(temp, 0, available);
        if (bytesRead > 0) {
            for (int i = 0; i < bytesRead; i++) {
                Unsafe.getUnsafe().putByte(recvBufferPtr + recvBufferPos + i, temp[i]);
            }
            recvBufferPos += bytesRead;
        }
        return bytesRead;
    }

    // Separate temp buffers for read and write to avoid race conditions
    // between send queue thread and response reader thread
    private byte[] writeTempBuffer;
    private byte[] readTempBuffer;

    private byte[] getWriteTempBuffer(int minSize) {
        if (writeTempBuffer == null || writeTempBuffer.length < minSize) {
            writeTempBuffer = new byte[Math.max(minSize, 8192)];
        }
        return writeTempBuffer;
    }

    private byte[] getReadTempBuffer(int minSize) {
        if (readTempBuffer == null || readTempBuffer.length < minSize) {
            readTempBuffer = new byte[Math.max(minSize, 8192)];
        }
        return readTempBuffer;
    }

    private void closeQuietly() {
        connected = false;
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                // Ignore
            }
            socket = null;
        }
        in = null;
        out = null;
    }

    /**
     * Callback interface for received WebSocket messages.
     */
    public interface ResponseHandler {
        void onBinaryMessage(long payload, int length);
        void onClose(int code, String reason);
    }
}
