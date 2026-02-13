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

package io.questdb.cutlass.ilpv4.server;

import io.questdb.cutlass.ilpv4.protocol.*;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.ilpv4.websocket.*;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRawSocket;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.network.Socket;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;

import java.nio.charset.StandardCharsets;

/**
 * HTTP request processor that handles WebSocket upgrade for ILP v4.
 * <p>
 * This processor:
 * 1. Validates the WebSocket handshake
 * 2. Sends the 101 Switching Protocols response
 * 3. Switches to WebSocket protocol for subsequent communication
 * 4. Parses WebSocket frames and processes ILP v4 messages
 * <p>
 * Per-connection state is stored in {@link IlpV4ProcessorState} via {@link LocalValue},
 * so a single processor instance can safely be shared across connections on the same worker.
 */
public class IlpV4WebSocketUpgradeProcessor implements HttpRequestProcessor {
    private static final Log LOG = LogFactory.getLog(IlpV4WebSocketUpgradeProcessor.class);
    private static final LocalValue<IlpV4ProcessorState> LV = new LocalValue<>();

    // HTTP response templates
    private static final byte[] BAD_REQUEST_PREFIX =
            "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: ".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HTTP_HEADER_END = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] UPGRADE_REQUIRED_RESPONSE =
            ("HTTP/1.1 426 Upgrade Required\r\n" +
                    "Upgrade: websocket\r\n" +
                    "Connection: Upgrade\r\n" +
                    "Sec-WebSocket-Version: 13\r\n" +
                    "Content-Length: 0\r\n" +
                    "\r\n").getBytes(StandardCharsets.US_ASCII);

    // Response status codes (match WebSocketResponse)
    private static final byte STATUS_OK = 0;
    private static final byte STATUS_PARSE_ERROR = 1;
    private static final byte STATUS_WRITE_ERROR = 3;
    private static final byte STATUS_SECURITY_ERROR = 4;
    private static final byte STATUS_INTERNAL_ERROR = (byte) 255;

    // Cumulative ACK batch size
    private static final int ACK_BATCH_SIZE = 8;

    // Dependencies for ILP processing (safe as instance fields — config only)
    private final CairoEngine engine;
    private final HttpFullFatServerConfiguration httpConfiguration;
    private final int maxResponseContentLength;
    private final int recvBufferSize;

    // WebSocket frame parser (scratchpad — fully reset within each processWebSocketFrames call)
    private final WebSocketFrameParser frameParser = new WebSocketFrameParser();

    public IlpV4WebSocketUpgradeProcessor(CairoEngine engine, HttpFullFatServerConfiguration httpConfiguration) {
        this.engine = engine;
        this.httpConfiguration = httpConfiguration;
        this.recvBufferSize = httpConfiguration.getRecvBufferSize();
        this.maxResponseContentLength = httpConfiguration.getSendBufferSize();
        this.frameParser.setServerMode(true);  // Expect masked frames from clients
    }

    @Override
    public void onHeadersReady(HttpConnectionContext context) {
        // Validate the WebSocket handshake (version, key, etc.) before allocating
        // any per-connection state. getProcessor() returns unconditionally (needed for
        // protocol-switched resume), so we validate here before sending the 101.
        // Rejecting early avoids allocating native buffers for malformed requests.
        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufferAddr = rawSocket.getBufferAddress();
        int bufferSize = rawSocket.getBufferSize();

        String validationError = IlpV4WebSocketHttpProcessor.validateHandshake(context.getRequestHeader());
        if (validationError != null) {
            LOG.error().$("WebSocket handshake validation failed [fd=").$(context.getFd())
                    .$(", error=").$(validationError).I$();
            int bytesWritten = validationError.contains("version")
                    ? writeUpgradeRequiredResponse(bufferAddr, bufferSize)
                    : writeBadRequestResponse(bufferAddr, bufferSize, validationError);
            if (bytesWritten > 0) {
                try {
                    rawSocket.send(bytesWritten);
                } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
                    // best-effort
                }
            }
            return;
        }

        // Initialize or get the ILP processor state for this connection
        IlpV4ProcessorState state = LV.get(context);
        if (state == null) {
            state = new IlpV4ProcessorState(
                    recvBufferSize,
                    maxResponseContentLength,
                    engine,
                    httpConfiguration.getLineHttpProcessorConfiguration()
            );
            LV.set(context, state);
        } else {
            state.clear();
        }
        state.of(context.getFd(), context.getSecurityContext());

        Utf8Sequence wsKey = IlpV4WebSocketHttpProcessor.getWebSocketKey(context.getRequestHeader());

        // Write the 101 Switching Protocols response
        int bytesWritten = writeHandshakeResponse(bufferAddr, bufferSize, wsKey);
        if (bytesWritten > 0) {
            try {
                rawSocket.send(bytesWritten);
            } catch (PeerDisconnectedException e) {
                LOG.info().$("WebSocket handshake failed, peer disconnected [fd=").$(context.getFd()).I$();
                return;
            } catch (PeerIsSlowToReadException e) {
                // Handshake blocked - this shouldn't happen on a fresh connection.
                // The buffer now has handshake data that we can't track with our ACK state machine.
                // Safest to disconnect rather than leave buffer in inconsistent state.
                LOG.error().$("WebSocket handshake blocked, disconnecting [fd=").$(context.getFd()).I$();
                return;
            }
            state.setWsHandshakeSent(true);
            LOG.info().$("WebSocket handshake sent [fd=").$(context.getFd()).I$();

            // Switch to WebSocket protocol - this tells the framework to bypass HTTP parsing
            context.switchProtocol();
        }
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        // For WebSocket, after the handshake is sent, we just return normally.
        // The framework will call reset() and then loop back to handleClientRecv().
        // Since we called switchProtocol() in onHeadersReady, the framework will
        // delegate to resumeRecv instead of parsing more HTTP requests.
        IlpV4ProcessorState state = LV.get(context);
        if (state != null && state.isWsHandshakeSent()) {
            LOG.debug().$("WebSocket handshake complete, ready for frames [fd=").$(context.getFd()).I$();
        }
    }

    @Override
    public void resumeRecv(HttpConnectionContext context) throws PeerIsSlowToWriteException, ServerDisconnectException, PeerIsSlowToReadException {
        // Ensure state is available
        IlpV4ProcessorState state = LV.get(context);
        if (state == null) {
            LOG.error().$("WebSocket resumeRecv but no state available [fd=").$(context.getFd()).I$();
            throw ServerDisconnectException.INSTANCE;
        }

        // This is called when there's data to read on a protocol-switched connection
        Socket socket = context.getSocket();
        long recvBuffer = context.getRecvBuffer();
        int recvBufferSize = context.getRecvBufferSize();

        try {
            // Read data from socket
            int recvBufferLen = state.getRecvBufferLen();
            int read = socket.recv(recvBuffer + recvBufferLen, recvBufferSize - recvBufferLen);

            if (read < 0) {
                // Connection closed
                LOG.info().$("WebSocket peer disconnected [fd=").$(context.getFd()).I$();
                throw ServerDisconnectException.INSTANCE;
            }

            if (read == 0) {
                // No data available, wait for more
                throw PeerIsSlowToWriteException.INSTANCE;
            }

            recvBufferLen += read;
            state.setRecvBufferLen(recvBufferLen);
            LOG.debug().$("WebSocket recv [fd=").$(context.getFd()).$(", bytes=").$(read).$(", total=").$(recvBufferLen).I$();

            // Parse WebSocket frames
            processWebSocketFrames(context, state, recvBuffer, recvBufferLen);

        } catch (ServerDisconnectException | PeerIsSlowToWriteException | PeerIsSlowToReadException e) {
            throw e;
        } catch (Throwable e) {
            LOG.error().$("WebSocket error [fd=").$(context.getFd()).$(", error=").$(e).I$();
            throw ServerDisconnectException.INSTANCE;
        }
    }

    private void processWebSocketFrames(HttpConnectionContext context, IlpV4ProcessorState state, long buffer, int bufferLen)
            throws ServerDisconnectException, PeerIsSlowToWriteException, PeerDisconnectedException, PeerIsSlowToReadException {
        long bufferEnd = buffer + bufferLen;
        long pos = buffer;

        while (pos < bufferEnd) {
            frameParser.reset();
            int consumed = frameParser.parse(pos, bufferEnd);

            if (frameParser.getState() == WebSocketFrameParser.STATE_ERROR) {
                LOG.error().$("WebSocket frame error [fd=").$(context.getFd()).$(", code=").$(frameParser.getErrorCode()).I$();
                throw ServerDisconnectException.INSTANCE;
            }

            if (consumed == 0 || frameParser.getState() == WebSocketFrameParser.STATE_NEED_MORE ||
                    frameParser.getState() == WebSocketFrameParser.STATE_NEED_PAYLOAD) {
                // Need more data - compact buffer and wait
                if (pos > buffer) {
                    int remaining = (int) (bufferEnd - pos);
                    if (remaining > 0) {
                        Unsafe.getUnsafe().copyMemory(pos, buffer, remaining);
                    }
                    state.setRecvBufferLen(remaining);
                }
                throw PeerIsSlowToWriteException.INSTANCE;
            }

            // Frame parsed successfully
            int opcode = frameParser.getOpcode();
            long payloadPtr = pos + frameParser.getHeaderSize();
            int payloadLen = (int) frameParser.getPayloadLength();

            // Unmask payload
            if (frameParser.isMasked()) {
                frameParser.unmaskPayload(payloadPtr, payloadLen);
            }

            // Handle frame
            handleWebSocketFrame(context, state, opcode, payloadPtr, payloadLen);

            pos += consumed;
        }

        // All data processed - flush any pending cumulative ACK
        flushPendingAck(context, state);
        state.setRecvBufferLen(0);
    }

    private void handleWebSocketFrame(HttpConnectionContext context, IlpV4ProcessorState state, int opcode, long payload, int length)
            throws ServerDisconnectException, PeerDisconnectedException, PeerIsSlowToReadException {
        switch (opcode) {
            case WebSocketOpcode.BINARY -> handleBinaryMessage(context, state, payload, length);
            case WebSocketOpcode.TEXT ->
                    LOG.debug().$("WebSocket text message ignored [fd=").$(context.getFd()).$(", len=").$(length).I$();
            case WebSocketOpcode.PING -> handlePing(context, state, payload, length);
            case WebSocketOpcode.PONG -> LOG.debug().$("WebSocket pong [fd=").$(context.getFd()).I$();
            case WebSocketOpcode.CLOSE -> {
                handleClose(context, state, payload, length);
                throw ServerDisconnectException.INSTANCE;
            }
            default ->
                    LOG.debug().$("WebSocket unknown opcode [fd=").$(context.getFd()).$(", opcode=").$(opcode).I$();
        }
    }

    private void handleBinaryMessage(HttpConnectionContext context, IlpV4ProcessorState state, long payload, int length)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        long seq = state.nextMessageSequence();
        LOG.debug().$("WebSocket binary message [fd=").$(context.getFd())
                .$(", len=").$(length)
                .$(", seq=").$(seq).I$();

        if (!state.isOk()) {
            LOG.debug().$("WebSocket ignoring message, state is in error [fd=").$(context.getFd()).I$();
            sendErrorResponse(context, state, seq, STATUS_INTERNAL_ERROR, "Previous message failed");
            return;
        }

        byte responseStatus = STATUS_OK;
        String errorMessage = null;

        try {
            // Add the binary data to the state buffer
            state.addData(payload, payload + length);

            // Process the ILP v4 message
            state.processMessage();

            if (state.isOk()) {
                // Commit the transaction
                state.commit();
                LOG.debug().$("WebSocket message committed [fd=").$(context.getFd())
                        .$(", seq=").$(seq).I$();
            } else {
                LOG.error().$("WebSocket message processing failed [fd=").$(context.getFd()).I$();
                responseStatus = STATUS_WRITE_ERROR;
                errorMessage = "Processing failed";
            }
        } catch (Throwable e) {
            LOG.error().$("WebSocket ILP processing error [fd=").$(context.getFd())
                    .$(", seq=").$(seq)
                    .$(", error=").$(e).I$();

            // Determine error type
            if (e.getMessage() != null && e.getMessage().contains("permission denied")) {
                responseStatus = STATUS_SECURITY_ERROR;
            } else if (e.getMessage() != null && e.getMessage().contains("parse")) {
                responseStatus = STATUS_PARSE_ERROR;
            } else {
                responseStatus = STATUS_INTERNAL_ERROR;
            }
            errorMessage = e.getMessage();
        } finally {
            // Reset state for next message (but preserve connectionSymbolDict for delta encoding)
            state.clear();
        }

        // Send response using cumulative ACK strategy
        if (responseStatus == STATUS_OK) {
            // Success - update tracking, send ACK if batch size reached
            state.setHighestProcessedSequence(seq);
            if (state.shouldSendAck(ACK_BATCH_SIZE)) {
                trySendAck(context, state);
            }
        } else {
            // Error - first ACK all successful messages (if in READY state), then send error
            if (state.hasPendingAck()) {
                trySendAck(context, state);
            }
            sendErrorResponse(context, state, seq, responseStatus, errorMessage);
        }
    }

    /**
     * Flushes any pending cumulative ACK.
     * Only attempts to send when in READY state (buffer is clear).
     */
    private void flushPendingAck(HttpConnectionContext context, IlpV4ProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (state.hasPendingAck()) {
            trySendAck(context, state);
        }
    }

    /**
     * Attempts to send a cumulative ACK for the highest processed sequence.
     * <p>
     * State transitions (managed by {@link IlpV4ProcessorState}):
     * <ul>
     *   <li>READY + success → stays READY, updates lastAckedSequence</li>
     *   <li>READY + PeerIsSlowToReadException → transitions to SENDING, throws</li>
     * </ul>
     *
     * @param context the HTTP connection context
     * @param state   the per-connection processor state
     * @throws PeerIsSlowToReadException if the client's receive buffer is full (transitions to SENDING)
     * @throws PeerDisconnectedException if the client disconnected
     */
    private void trySendAck(HttpConnectionContext context, IlpV4ProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        assert state.isSendReady() : "trySendAck called in wrong state";

        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufferAddr = rawSocket.getBufferAddress();
        int bufferSize = rawSocket.getBufferSize();

        // Response: status (1) + sequence (8) = 9 bytes
        int payloadLen = 9;
        int frameSize = WebSocketFrameWriter.headerSize(payloadLen, false) + payloadLen;

        if (frameSize > bufferSize) {
            // Buffer capacity too small for even a single ACK frame
            LOG.critical().$("Buffer too small for ACK response [fd=").$(context.getFd())
                    .$(", required=").$(frameSize)
                    .$(", bufferSize=").$(bufferSize).I$();
            throw PeerDisconnectedException.INSTANCE;
        }

        long sequence = state.getHighestProcessedSequence();
        int headerLen = WebSocketFrameWriter.writeBinaryFrameHeader(bufferAddr, payloadLen);
        // Write status
        Unsafe.getUnsafe().putByte(bufferAddr + headerLen, STATUS_OK);
        // Write sequence (little-endian)
        Unsafe.getUnsafe().putLong(bufferAddr + headerLen + 1, sequence);

        try {
            rawSocket.send(headerLen + payloadLen);
            state.onAckSent(sequence);
            LOG.debug().$("Sent cumulative ACK [fd=").$(context.getFd()).$(", upTo=").$(sequence).I$();
        } catch (PeerIsSlowToReadException e) {
            // OS buffer full - transition to SENDING state
            state.onAckBlocked(sequence);
            LOG.debug().$("ACK blocked, transitioning to SENDING [fd=").$(context.getFd())
                    .$(", seq=").$(sequence).I$();
            throw e;
        }
    }

    /**
     * Sends an error response for the given sequence number.
     * <p>
     * Note: This method only sends when in READY state. If in SENDING state,
     * the error is logged but not sent (the pending ACK takes priority, and
     * the connection will likely be closed anyway due to the error).
     */
    private void sendErrorResponse(HttpConnectionContext context, IlpV4ProcessorState state, long sequence, byte status, String errorMessage) {
        // Can only send when buffer is clear
        if (!state.isSendReady()) {
            LOG.error().$("Cannot send error response, buffer busy [fd=").$(context.getFd())
                    .$(", seq=").$(sequence)
                    .$(", status=").$(status).I$();
            return;
        }

        try {
            HttpRawSocket rawSocket = context.getRawResponseSocket();
            long bufferAddr = rawSocket.getBufferAddress();
            int bufferSize = rawSocket.getBufferSize();

            // Calculate payload size
            byte[] msgBytes = errorMessage != null ? errorMessage.getBytes(StandardCharsets.UTF_8) : new byte[0];
            int msgLen = Math.min(msgBytes.length, 1024);
            int payloadLen = 9 + 2 + msgLen; // status + seq + len + msg

            int frameSize = WebSocketFrameWriter.headerSize(payloadLen, false) + payloadLen;

            if (frameSize <= bufferSize) {
                int offset = WebSocketFrameWriter.writeBinaryFrameHeader(bufferAddr, payloadLen);

                // Write status
                Unsafe.getUnsafe().putByte(bufferAddr + offset, status);
                offset += 1;

                // Write sequence (little-endian)
                Unsafe.getUnsafe().putLong(bufferAddr + offset, sequence);
                offset += 8;

                // Write message length (little-endian)
                Unsafe.getUnsafe().putShort(bufferAddr + offset, (short) msgLen);
                offset += 2;

                // Write message
                for (int i = 0; i < msgLen; i++) {
                    Unsafe.getUnsafe().putByte(bufferAddr + offset + i, msgBytes[i]);
                }
                offset += msgLen;

                rawSocket.send(offset);
                LOG.debug().$("Sent error response [fd=").$(context.getFd())
                        .$(", seq=").$(sequence)
                        .$(", status=").$(status).I$();
            } else {
                LOG.error().$("Buffer too small for error response [fd=").$(context.getFd()).I$();
            }
        } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
            LOG.debug().$("Failed to send error response [fd=").$(context.getFd())
                    .$(", seq=").$(sequence).I$();
        }
    }

    private void handlePing(HttpConnectionContext context, IlpV4ProcessorState state, long payload, int length) {
        // Can only send pong when buffer is clear
        if (!state.isSendReady()) {
            LOG.debug().$("Skipping pong, buffer busy [fd=").$(context.getFd()).I$();
            return;
        }

        try {
            HttpRawSocket rawSocket = context.getRawResponseSocket();
            long bufferAddr = rawSocket.getBufferAddress();
            int bufferSize = rawSocket.getBufferSize();

            int frameSize = WebSocketFrameWriter.headerSize(length, false) + length;
            if (frameSize <= bufferSize) {
                int written = WebSocketFrameWriter.writePongFrame(bufferAddr, payload, length);
                rawSocket.send(written);
                LOG.debug().$("WebSocket pong sent [fd=").$(context.getFd()).I$();
            }
        } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
            LOG.debug().$("Failed to send pong [fd=").$(context.getFd()).I$();
        }
    }

    private void handleClose(HttpConnectionContext context, IlpV4ProcessorState state, long payload, int length) {
        int closeCode = -1;
        if (length >= 2) {
            int high = Unsafe.getUnsafe().getByte(payload) & 0xFF;
            int low = Unsafe.getUnsafe().getByte(payload + 1) & 0xFF;
            closeCode = (high << 8) | low;
        }
        LOG.info().$("WebSocket close [fd=").$(context.getFd()).$(", code=").$(closeCode).I$();

        // Send close response only if buffer is clear
        if (!state.isSendReady()) {
            LOG.debug().$("Skipping close response, buffer busy [fd=").$(context.getFd()).I$();
            return;
        }

        try {
            HttpRawSocket rawSocket = context.getRawResponseSocket();
            long bufferAddr = rawSocket.getBufferAddress();
            int bufferSize = rawSocket.getBufferSize();

            int written = WebSocketFrameWriter.writeCloseFrame(bufferAddr, closeCode, null);
            if (written <= bufferSize) {
                rawSocket.send(written);
            }
        } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
            // Ignore, we're closing anyway
        }
    }

    @Override
    public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        IlpV4ProcessorState state = LV.get(context);
        if (state == null) {
            throw ServerDisconnectException.INSTANCE;
        }

        if (state.isSending()) {
            // Try to flush the pending ACK in the buffer
            context.resumeResponseSend();

            // If we get here, the send succeeded
            state.onResumeSendComplete();
            LOG.debug().$("Resumed ACK sent successfully [fd=").$(context.getFd())
                    .$(", upTo=").$(state.getLastAckedSequence()).I$();

            // Check if more ACKs are pending (messages arrived while we were blocked)
            if (state.hasPendingAck()) {
                trySendAck(context, state);
            }
        }
        // If in READY state, nothing to do - no pending buffer data
    }

    @Override
    public void parkRequest(HttpConnectionContext context, boolean pausedQuery) {
        // WebSocket connections don't park like normal HTTP requests
    }

    @Override
    public void onConnectionClosed(HttpConnectionContext context) {
        LOG.info().$("WebSocket connection closed [fd=").$(context.getFd()).I$();
        IlpV4ProcessorState state = LV.get(context);
        if (state == null) {
            return;
        }
        // Try to flush any pending ACKs before closing
        try {
            if (state.isSending()) {
                // Try to flush pending buffer first
                context.resumeResponseSend();
                state.onResumeSendComplete();
            }
            // Now try to send any remaining ACKs
            flushPendingAck(context, state);
        } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
            // Connection is closing anyway, ignore
        }
        state.onDisconnected();
    }

    // ==================== STATIC RESPONSE WRITING METHODS ====================

    /**
     * Writes a WebSocket handshake response to the buffer.
     *
     * @param buffer     the buffer to write to
     * @param bufferSize the size of the buffer
     * @param key        the WebSocket key from the client
     * @return the number of bytes written, or -1 if buffer too small
     */
    public static int writeHandshakeResponse(long buffer, int bufferSize, Utf8Sequence key) {
        String acceptKey = WebSocketHandshake.computeAcceptKey(key);
        int requiredSize = WebSocketHandshake.responseSize(acceptKey);

        if (requiredSize > bufferSize) {
            return -1;
        }

        return WebSocketHandshake.writeResponse(buffer, acceptKey);
    }

    /**
     * Returns the size of the handshake response for the given key.
     *
     * @param key the WebSocket key from the client
     * @return the response size in bytes
     */
    public static int handshakeResponseSize(Utf8Sequence key) {
        String acceptKey = WebSocketHandshake.computeAcceptKey(key);
        return WebSocketHandshake.responseSize(acceptKey);
    }

    /**
     * Writes a 400 Bad Request response.
     *
     * @param buffer     the buffer to write to
     * @param bufferSize the size of the buffer
     * @param reason     the reason for the bad request
     * @return the number of bytes written, or -1 if buffer too small
     */
    public static int writeBadRequestResponse(long buffer, int bufferSize, String reason) {
        byte[] reasonBytes = reason.getBytes(StandardCharsets.UTF_8);
        String contentLength = String.valueOf(reasonBytes.length);
        byte[] contentLengthBytes = contentLength.getBytes(StandardCharsets.US_ASCII);

        int requiredSize = BAD_REQUEST_PREFIX.length + contentLengthBytes.length +
                HTTP_HEADER_END.length + reasonBytes.length;

        if (requiredSize > bufferSize) {
            return -1;
        }

        int offset = 0;

        // Write prefix
        for (byte b : BAD_REQUEST_PREFIX) {
            Unsafe.getUnsafe().putByte(buffer + offset++, b);
        }

        // Write content length
        for (byte b : contentLengthBytes) {
            Unsafe.getUnsafe().putByte(buffer + offset++, b);
        }

        // Write header end
        for (byte b : HTTP_HEADER_END) {
            Unsafe.getUnsafe().putByte(buffer + offset++, b);
        }

        // Write body
        for (byte b : reasonBytes) {
            Unsafe.getUnsafe().putByte(buffer + offset++, b);
        }

        return offset;
    }

    /**
     * Writes a 426 Upgrade Required response.
     *
     * @param buffer     the buffer to write to
     * @param bufferSize the size of the buffer
     * @return the number of bytes written, or -1 if buffer too small
     */
    public static int writeUpgradeRequiredResponse(long buffer, int bufferSize) {
        if (UPGRADE_REQUIRED_RESPONSE.length > bufferSize) {
            return -1;
        }

        for (int i = 0; i < UPGRADE_REQUIRED_RESPONSE.length; i++) {
            Unsafe.getUnsafe().putByte(buffer + i, UPGRADE_REQUIRED_RESPONSE[i]);
        }

        return UPGRADE_REQUIRED_RESPONSE.length;
    }
}
