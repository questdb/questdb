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
import io.questdb.cutlass.ilpv4.server.IlpV4ProcessorState;
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

    // Dependencies for ILP processing
    private final CairoEngine engine;
    private final HttpFullFatServerConfiguration httpConfiguration;
    private final int recvBufferSize;
    private final int maxResponseContentLength;

    // WebSocket frame parser
    private final WebSocketFrameParser frameParser = new WebSocketFrameParser();

    // State
    private boolean handshakeSent = false;
    private int recvBufferLen = 0;
    private IlpV4ProcessorState state;

    // Sequence number for response correlation
    private long messageSequence = 0;

    // Cumulative ACK tracking
    private static final int ACK_BATCH_SIZE = 8;  // Send ACK every N messages
    private long highestProcessedSequence = -1;   // Last successful commit
    private long lastAckedSequence = -1;          // Last ACK sent to client

    /**
     * Send state machine for ACK responses.
     * <p>
     * States:
     * <ul>
     *   <li>READY - Buffer is clear, can write new ACK frames</li>
     *   <li>SENDING - Buffer has ACK frame waiting to be sent (OS buffer was full)</li>
     * </ul>
     */
    private enum SendState {
        READY,    // Buffer clear, can write new data
        SENDING   // Buffer has pending ACK, waiting for OS buffer space
    }

    private SendState sendState = SendState.READY;
    private long sequenceInBuffer = -1;  // Sequence of ACK in buffer when SENDING

    /**
     * Resets the state machine for a new connection.
     */
    private void resetStateMachine() {
        handshakeSent = false;
        recvBufferLen = 0;
        messageSequence = 0;
        highestProcessedSequence = -1;
        lastAckedSequence = -1;
        sendState = SendState.READY;
        sequenceInBuffer = -1;
    }

    public IlpV4WebSocketUpgradeProcessor(CairoEngine engine, HttpFullFatServerConfiguration httpConfiguration) {
        this.engine = engine;
        this.httpConfiguration = httpConfiguration;
        this.recvBufferSize = httpConfiguration.getRecvBufferSize();
        this.maxResponseContentLength = httpConfiguration.getSendBufferSize();
        this.frameParser.setServerMode(true);  // Expect masked frames from clients
    }

    @Override
    public void onHeadersReady(HttpConnectionContext context) {
        // Initialize or get the ILP processor state for this connection
        state = LV.get(context);
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

        // Reset state machine for new connection
        resetStateMachine();

        // Get the WebSocket key from request headers
        Utf8Sequence wsKey = IlpV4WebSocketHttpProcessor.getWebSocketKey(context.getRequestHeader());

        if (wsKey == null) {
            // Should not happen - we already validated in isWebSocketUpgradeRequest
            LOG.error().$("WebSocket key missing after validation [fd=").$(context.getFd()).I$();
            return;
        }

        // Get raw socket to send the handshake response
        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufferAddr = rawSocket.getBufferAddress();
        int bufferSize = rawSocket.getBufferSize();

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
            handshakeSent = true;
            LOG.info().$("WebSocket handshake sent [fd=").$(context.getFd()).I$();

            // Switch to WebSocket protocol - this tells the framework to bypass HTTP parsing
            context.switchProtocol(this);
        }
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        // For WebSocket, after the handshake is sent, we just return normally.
        // The framework will call reset() and then loop back to handleClientRecv().
        // Since we called switchProtocol() in onHeadersReady, the framework will
        // delegate to resumeRecv instead of parsing more HTTP requests.
        if (handshakeSent) {
            LOG.debug().$("WebSocket handshake complete, ready for frames [fd=").$(context.getFd()).I$();
        }
    }

    @Override
    public void resumeRecv(HttpConnectionContext context) throws PeerIsSlowToWriteException, ServerDisconnectException {
        // Ensure state is available
        state = LV.get(context);
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
            LOG.debug().$("WebSocket recv [fd=").$(context.getFd()).$(", bytes=").$(read).$(", total=").$(recvBufferLen).I$();

            // Parse WebSocket frames
            processWebSocketFrames(context, recvBuffer, recvBufferLen);

        } catch (ServerDisconnectException | PeerIsSlowToWriteException e) {
            throw e;
        } catch (Throwable e) {
            LOG.error().$("WebSocket error [fd=").$(context.getFd()).$(", error=").$(e).I$();
            throw ServerDisconnectException.INSTANCE;
        }
    }

    private void processWebSocketFrames(HttpConnectionContext context, long buffer, int bufferLen)
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
                    recvBufferLen = remaining;
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
            handleWebSocketFrame(context, opcode, payloadPtr, payloadLen);

            pos += consumed;
        }

        // All data processed - flush any pending cumulative ACK
        flushPendingAck(context);
        recvBufferLen = 0;
    }

    private void handleWebSocketFrame(HttpConnectionContext context, int opcode, long payload, int length)
            throws ServerDisconnectException, PeerDisconnectedException, PeerIsSlowToReadException {
        switch (opcode) {
            case WebSocketOpcode.BINARY:
                handleBinaryMessage(context, payload, length);
                break;

            case WebSocketOpcode.TEXT:
                // ILP v4 is binary-only, but log text messages for debugging
                LOG.debug().$("WebSocket text message ignored [fd=").$(context.getFd()).$(", len=").$(length).I$();
                break;

            case WebSocketOpcode.PING:
                handlePing(context, payload, length);
                break;

            case WebSocketOpcode.PONG:
                // Just acknowledge pong
                LOG.debug().$("WebSocket pong [fd=").$(context.getFd()).I$();
                break;

            case WebSocketOpcode.CLOSE:
                handleClose(context, payload, length);
                throw ServerDisconnectException.INSTANCE;

            default:
                LOG.debug().$("WebSocket unknown opcode [fd=").$(context.getFd()).$(", opcode=").$(opcode).I$();
                break;
        }
    }

    private void handleBinaryMessage(HttpConnectionContext context, long payload, int length)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        long seq = messageSequence++;
        LOG.debug().$("WebSocket binary message [fd=").$(context.getFd())
                .$(", len=").$(length)
                .$(", seq=").$(seq).I$();

        if (state == null) {
            LOG.error().$("WebSocket binary message received but state is null [fd=").$(context.getFd()).I$();
            sendErrorResponse(context, seq, STATUS_INTERNAL_ERROR, "Internal error: state is null");
            return;
        }

        if (!state.isOk()) {
            LOG.debug().$("WebSocket ignoring message, state is in error [fd=").$(context.getFd()).I$();
            sendErrorResponse(context, seq, STATUS_INTERNAL_ERROR, "Previous message failed");
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

            // Reset state for next message (but preserve connectionSymbolDict for delta encoding)
            state.clear();
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

            // Reset state for next message (but preserve connectionSymbolDict for delta encoding)
            state.clear();
        }

        // Send response using cumulative ACK strategy
        if (responseStatus == STATUS_OK) {
            // Success - update tracking, send ACK if batch size reached
            highestProcessedSequence = seq;
            maybeSendCumulativeAck(context);
        } else {
            // Error - first ACK all successful messages (if in READY state), then send error
            if (sendState == SendState.READY && highestProcessedSequence > lastAckedSequence) {
                trySendAck(context, highestProcessedSequence);
            }
            sendErrorResponse(context, seq, responseStatus, errorMessage);
        }
    }

    /**
     * Sends a cumulative ACK if the batch size threshold is reached.
     * Only attempts to send when in READY state (buffer is clear).
     *
     * @throws PeerIsSlowToReadException if the client's receive buffer is full
     * @throws PeerDisconnectedException if the client disconnected
     */
    private void maybeSendCumulativeAck(HttpConnectionContext context)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        // Can only send new ACKs when in READY state
        if (sendState != SendState.READY) {
            return;
        }

        long gap = highestProcessedSequence - lastAckedSequence;
        if (gap >= ACK_BATCH_SIZE) {
            trySendAck(context, highestProcessedSequence);
        }
    }

    /**
     * Flushes any pending cumulative ACK.
     * Only attempts to send when in READY state (buffer is clear).
     *
     * @throws PeerIsSlowToReadException if the client's receive buffer is full
     * @throws PeerDisconnectedException if the client disconnected
     */
    private void flushPendingAck(HttpConnectionContext context)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        // Can only send new ACKs when in READY state
        if (sendState != SendState.READY) {
            return;
        }

        if (highestProcessedSequence > lastAckedSequence) {
            trySendAck(context, highestProcessedSequence);
        }
    }

    /**
     * Attempts to send a cumulative ACK for the given sequence number.
     * <p>
     * State transitions:
     * <ul>
     *   <li>READY + success → stays READY, updates lastAckedSequence</li>
     *   <li>READY + PeerIsSlowToReadException → transitions to SENDING, throws</li>
     * </ul>
     *
     * @param context  the HTTP connection context
     * @param sequence the sequence number to ACK (cumulative)
     * @throws PeerIsSlowToReadException if the client's receive buffer is full (transitions to SENDING)
     * @throws PeerDisconnectedException if the client disconnected
     */
    private void trySendAck(HttpConnectionContext context, long sequence)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        assert sendState == SendState.READY : "trySendAck called in wrong state: " + sendState;

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

        int headerLen = WebSocketFrameWriter.writeBinaryFrameHeader(bufferAddr, payloadLen);
        // Write status
        Unsafe.getUnsafe().putByte(bufferAddr + headerLen, STATUS_OK);
        // Write sequence (little-endian)
        Unsafe.getUnsafe().putLong(bufferAddr + headerLen + 1, sequence);

        try {
            rawSocket.send(headerLen + payloadLen);
            // Successfully sent - update tracking, stay in READY state
            lastAckedSequence = sequence;
            LOG.debug().$("Sent cumulative ACK [fd=").$(context.getFd()).$(", upTo=").$(sequence).I$();
        } catch (PeerIsSlowToReadException e) {
            // OS buffer full - transition to SENDING state
            sendState = SendState.SENDING;
            sequenceInBuffer = sequence;
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
    private void sendErrorResponse(HttpConnectionContext context, long sequence, byte status, String errorMessage) {
        // Can only send when buffer is clear
        if (sendState != SendState.READY) {
            LOG.error().$("Cannot send error response, buffer busy [fd=").$(context.getFd())
                    .$(", seq=").$(sequence)
                    .$(", status=").$(status)
                    .$(", state=").$(sendState).I$();
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

    private void handlePing(HttpConnectionContext context, long payload, int length) {
        // Can only send pong when buffer is clear
        if (sendState != SendState.READY) {
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

    private void handleClose(HttpConnectionContext context, long payload, int length) {
        int closeCode = -1;
        if (length >= 2) {
            int high = Unsafe.getUnsafe().getByte(payload) & 0xFF;
            int low = Unsafe.getUnsafe().getByte(payload + 1) & 0xFF;
            closeCode = (high << 8) | low;
        }
        LOG.info().$("WebSocket close [fd=").$(context.getFd()).$(", code=").$(closeCode).I$();

        // Send close response only if buffer is clear
        if (sendState != SendState.READY) {
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
        if (sendState == SendState.SENDING) {
            // Try to flush the pending ACK in the buffer
            context.resumeResponseSend();

            // If we get here, the send succeeded
            lastAckedSequence = sequenceInBuffer;
            sequenceInBuffer = -1;
            sendState = SendState.READY;
            LOG.debug().$("Resumed ACK sent successfully [fd=").$(context.getFd())
                    .$(", upTo=").$(lastAckedSequence).I$();

            // Check if more ACKs are pending (messages arrived while we were blocked)
            if (highestProcessedSequence > lastAckedSequence) {
                trySendAck(context, highestProcessedSequence);
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
        // Try to flush any pending ACKs before closing
        try {
            if (sendState == SendState.SENDING) {
                // Try to flush pending buffer first
                context.resumeResponseSend();
                lastAckedSequence = sequenceInBuffer;
                sequenceInBuffer = -1;
                sendState = SendState.READY;
            }
            // Now try to send any remaining ACKs
            flushPendingAck(context);
        } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
            // Connection is closing anyway, ignore
        }
        state = LV.get(context);
        if (state != null) {
            state.onDisconnected();
        }
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
