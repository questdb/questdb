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

package io.questdb.cutlass.qwp.server;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpException;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRawSocket;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.cutlass.qwp.websocket.WebSocketFrameParser;
import io.questdb.cutlass.qwp.websocket.WebSocketFrameWriter;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.network.Socket;
import io.questdb.std.CharSequenceLongHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

import java.nio.charset.StandardCharsets;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;

/**
 * HTTP request processor that handles WebSocket upgrade for QWP v1.
 * <p>
 * This processor:
 * 1. Validates the WebSocket handshake
 * 2. Sends the 101 Switching Protocols response
 * 3. Switches to WebSocket protocol for subsequent communication
 * 4. Parses WebSocket frames and processes QWP v1 messages
 * <p>
 * Per-connection state is stored in {@link QwpProcessorState} via {@link LocalValue},
 * so a single processor instance can safely be shared across connections on the same worker.
 */
public class QwpWebSocketUpgradeProcessor implements HttpRequestProcessor {
    // Cumulative ACK batch size
    private static final int ACK_BATCH_SIZE = 8;
    // HTTP response templates
    private static final byte[] BAD_REQUEST_PREFIX =
            "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: ".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HTTP_HEADER_END = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final Log LOG = LogFactory.getLog(QwpWebSocketUpgradeProcessor.class);
    private static final LocalValue<QwpProcessorState> LV = new LocalValue<>();
    private static final byte[] UPGRADE_REQUIRED_RESPONSE =
            ("""
                    HTTP/1.1 426 Upgrade Required\r
                    Upgrade: websocket\r
                    Connection: Upgrade\r
                    Sec-WebSocket-Version: 13\r
                    Content-Length: 0\r
                    \r
                    """).getBytes(StandardCharsets.US_ASCII);
    // Dependencies for ILP processing (safe as instance fields — config only)
    private final CairoEngine engine;
    private final int forceRecvFragmentationChunkSize;
    // WebSocket frame parser (scratchpad — fully reset within each processWebSocketFrames call)
    private final WebSocketFrameParser frameParser = new WebSocketFrameParser();
    private final HttpFullFatServerConfiguration httpConfiguration;
    private final int maxResponseContentLength;
    private final int recvBufferSize;

    public QwpWebSocketUpgradeProcessor(CairoEngine engine, HttpFullFatServerConfiguration httpConfiguration) {
        this.engine = engine;
        this.forceRecvFragmentationChunkSize = httpConfiguration.getHttpContextConfiguration()
                .getForceRecvFragmentationChunkSize();
        this.httpConfiguration = httpConfiguration;
        this.recvBufferSize = httpConfiguration.getRecvBufferSize();
        this.maxResponseContentLength = httpConfiguration.getSendBufferSize();
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

        int requiredSize = badRequestResponseSize(reasonBytes.length);

        if (requiredSize > bufferSize) {
            return -1;
        }

        int offset = 0;

        // Write prefix
        for (byte b : BAD_REQUEST_PREFIX) {
            Unsafe.putByte(buffer + offset++, b);
        }

        // Write content length
        for (byte b : contentLengthBytes) {
            Unsafe.putByte(buffer + offset++, b);
        }

        // Write header end
        for (byte b : HTTP_HEADER_END) {
            Unsafe.putByte(buffer + offset++, b);
        }

        // Write body
        for (byte b : reasonBytes) {
            Unsafe.putByte(buffer + offset++, b);
        }

        return offset;
    }

    /**
     * Writes a WebSocket handshake response to the buffer.
     *
     * @param buffer     the buffer to write to
     * @param bufferSize the size of the buffer
     * @param key        the WebSocket key from the client
     * @return the number of bytes written, or -1 if buffer too small
     */
    public static int writeHandshakeResponse(long buffer, int bufferSize, Utf8Sequence key, int qwpVersion) {
        String acceptKey = QwpWebSocketHttpProcessor.computeAcceptKey(key);
        int requiredSize = QwpWebSocketHttpProcessor.responseSize(acceptKey, qwpVersion);

        if (requiredSize > bufferSize) {
            return -1;
        }

        return QwpWebSocketHttpProcessor.writeResponse(buffer, acceptKey, qwpVersion);
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
            Unsafe.putByte(buffer + i, UPGRADE_REQUIRED_RESPONSE[i]);
        }

        return UPGRADE_REQUIRED_RESPONSE.length;
    }

    @Override
    public void onConnectionClosed(HttpConnectionContext context) {
        LOG.info().$("WebSocket connection closed [fd=").$(context.getFd()).I$();
        QwpProcessorState state = LV.get(context);
        if (state == null) {
            return;
        }
        // Best effort: flush any blocked outbound response first, then ACK
        // already-committed data before dropping the connection state.
        try {
            drainPendingResponse(context, state);
            flushPendingAck(context, state);
        } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
            // Connection is closing anyway, ignore
        } finally {
            try {
                state.onDisconnected();
            } finally {
                // Free native resources (bufferAddress, ddlMem, path, symbolCachePool).
                // set(null) calls Misc.freeIfCloseable(state) → state.close() and removes
                // the entry so that localValueMap.disconnect() won't call onDisconnected() again.
                LV.set(context, null);
            }
        }
    }

    @Override
    public void onHeadersReady(HttpConnectionContext context) throws PeerDisconnectedException {
        // Validate the WebSocket handshake (version, key, etc.) before allocating
        // any per-connection state. getProcessor() returns unconditionally (needed for
        // protocol-switched resume), so we validate here before sending the 101.
        // Rejecting early avoids allocating native buffers for malformed requests.
        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufferAddr = rawSocket.getBufferAddress();
        int bufferSize = rawSocket.getBufferSize();

        String validationError = QwpWebSocketHttpProcessor.validateHandshake(context.getRequestHeader());
        if (validationError != null) {
            LOG.error().$("WebSocket handshake validation failed [fd=").$(context.getFd())
                    .$(", error=").$(validationError).I$();
            final boolean versionError = QwpWebSocketHttpProcessor.isVersionValidationError(validationError);
            final int requiredSize = versionError ? UPGRADE_REQUIRED_RESPONSE.length : badRequestResponseSize(validationError);
            final CharSequence responseType = versionError ? "426 upgrade response" : "400 bad request response";
            if (requiredSize > bufferSize) {
                throw responseDoesNotFitSendBuffer(context.getFd(), responseType, bufferSize, requiredSize);
            }

            final int bytesWritten = versionError
                    ? writeUpgradeRequiredResponse(bufferAddr, bufferSize)
                    : writeBadRequestResponse(bufferAddr, bufferSize, validationError);
            if (bytesWritten <= 0) {
                throw responseDoesNotFitSendBuffer(context.getFd(), responseType, bufferSize, requiredSize);
            }
            try {
                rawSocket.send(bytesWritten);
            } catch (PeerIsSlowToReadException e) {
                // Send buffer full right after receiving headers with invalid handshake, that's weird error response cannot be delivered.
                // Throw HttpException so handleClientRecv disconnects the connection
                throw HttpException.instance("WebSocket handshake rejected: ").put(validationError);
            }
            // PeerDisconnectedException propagates to handleClientRecv → disconnectHttp()
            return;
        }

        HttpRequestHeader requestHeader = context.getRequestHeader();
        Utf8Sequence wsKey = QwpWebSocketHttpProcessor.getWebSocketKey(requestHeader);

        // Read QWP version negotiation headers
        int negotiatedVersion = negotiateQwpVersion(requestHeader, context.getFd());
        String acceptKey = QwpWebSocketHttpProcessor.computeAcceptKey(wsKey);
        int requiredHandshakeSize = QwpWebSocketHttpProcessor.responseSize(acceptKey, negotiatedVersion);
        if (requiredHandshakeSize > bufferSize) {
            throw responseDoesNotFitSendBuffer(context.getFd(), "101 handshake response", bufferSize, requiredHandshakeSize);
        }

        // Initialize or get the ILP processor state for this connection only after
        // confirming the 101 response fits in the raw HTTP send buffer.
        QwpProcessorState state = LV.get(context);
        if (state == null) {
            state = new QwpProcessorState(
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
        state.setNegotiatedVersion((byte) negotiatedVersion);
        // Opt-in flag for STATUS_DURABLE_ACK frames. Silently ignored when the
        // engine has no durable-ack registry installed (OSS build or primary
        // replication disabled), so opted-in clients on such servers simply
        // never see durable acks.
        Utf8Sequence durableAckHeader = requestHeader.getHeader(
                QwpWebSocketHttpProcessor.HEADER_X_QWP_REQUEST_DURABLE_ACK);
        boolean durableAckRequested = durableAckHeader != null
                && Utf8s.equalsIgnoreCaseAscii(durableAckHeader, QwpWebSocketHttpProcessor.HEADER_VALUE_DURABLE_ACK_ENABLED);
        state.setDurableAckEnabled(durableAckRequested && engine.getDurableAckRegistry().isEnabled());

        // Write the 101 Switching Protocols response (reuse the pre-computed accept key)
        int bytesWritten = QwpWebSocketHttpProcessor.writeResponse(bufferAddr, acceptKey, negotiatedVersion);
        if (bytesWritten <= 0) {
            throw responseDoesNotFitSendBuffer(context.getFd(), "101 handshake response", bufferSize, requiredHandshakeSize);
        }
        try {
            rawSocket.send(bytesWritten);
        } catch (PeerIsSlowToReadException e) {
            // Handshake blocked — shouldn't happen on a fresh connection.
            // Throw HttpException so handleClientRecv disconnects the connection
            // rather than leaving the buffer in an inconsistent state.
            throw HttpException.instance("WebSocket 101 handshake blocked");
        }
        // PeerDisconnectedException propagates to handleClientRecv → disconnectHttp()
        state.setWsHandshakeSent(true);
        LOG.info().$("WebSocket handshake sent [fd=").$(context.getFd()).I$();

        // Switch to WebSocket protocol - this tells the framework to bypass HTTP parsing
        context.switchProtocol();
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) {
        // For WebSocket, after the handshake is sent, we just return normally.
        // The framework will call reset() and then loop back to handleClientRecv().
        // Since we called switchProtocol() in onHeadersReady, the framework will
        // delegate to resumeRecv instead of parsing more HTTP requests.
        QwpProcessorState state = LV.get(context);
        if (state != null && state.isWsHandshakeSent()) {
            LOG.debug().$("WebSocket handshake complete, ready for frames [fd=").$(context.getFd()).I$();
        }
    }

    @Override
    public void parkRequest(HttpConnectionContext context, boolean pausedQuery) {
        // WebSocket connections don't park like normal HTTP requests
    }

    /**
     * Receives and processes WebSocket frames until the socket would block.
     */
    @Override
    public void resumeRecv(HttpConnectionContext context) throws PeerIsSlowToWriteException, ServerDisconnectException, PeerIsSlowToReadException {
        // Ensure state is available
        QwpProcessorState state = LV.get(context);
        if (state == null) {
            LOG.error().$("WebSocket resumeRecv but no state available [fd=").$(context.getFd()).I$();
            throw ServerDisconnectException.INSTANCE;
        }

        // This is called when there's data to read on a protocol-switched connection
        Socket socket = context.getSocket();
        long recvBuffer = context.getRecvBuffer();
        int recvBufferSize = context.getRecvBufferSize();

        try {
            int recvBufferLen = state.getRecvBufferLen();
            if (recvBufferLen >= recvBufferSize) {
                // Buffer is full, but the parser still needs more data — the frame
                // payload exceeds recv buffer capacity. Disconnect to avoid spinning.
                LOG.error().$("WebSocket frame too large for recv buffer [fd=").$(context.getFd())
                        .$(", bufferSize=").$(recvBufferSize).I$();
                throw ServerDisconnectException.INSTANCE;
            }

            int remaining = recvBufferSize - recvBufferLen;
            int read = socket.recv(recvBuffer + recvBufferLen, Math.min(forceRecvFragmentationChunkSize, remaining));
            if (read < 0) {
                // Connection closed
                LOG.info().$("WebSocket peer disconnected [fd=").$(context.getFd()).I$();
                throw ServerDisconnectException.INSTANCE;
            }

            if (read == 0) {
                // No data available from kernel right now, hand back to dispatcher.
                throw PeerIsSlowToWriteException.INSTANCE;
            }

            recvBufferLen += read;
            LOG.debug()
                    .$("WebSocket recv [fd=").$(context.getFd())
                    .$(", bytes=").$(read)
                    .$(", total=").$(recvBufferLen)
                    .I$();

            processWebSocketFrames(context, state, recvBuffer, recvBufferLen);

            if (read == forceRecvFragmentationChunkSize) {
                // Read was capped by the fragmentation chunk size — more data
                // may be available. Schedule for re-read via the dispatcher,
                // matching HttpConnectionContext.consumeChunked() behavior.
                throw PeerIsSlowToWriteException.INSTANCE;
            }

        } catch (ServerDisconnectException | PeerIsSlowToWriteException | PeerIsSlowToReadException e) {
            throw e;
        } catch (Throwable e) {
            LOG.error().$("WebSocket error [fd=").$(context.getFd()).$(", error=").$(e).I$();
            throw ServerDisconnectException.INSTANCE;
        }
    }

    @Override
    public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        QwpProcessorState state = LV.get(context);
        if (state == null) {
            throw ServerDisconnectException.INSTANCE;
        }

        switch (state.getSendState()) {
            case QwpProcessorState.SEND_STATE_READY -> {
            }
            case QwpProcessorState.SEND_STATE_RESUME_ACK -> {
                context.resumeResponseSend();
                state.onResumeAckComplete();
                LOG.debug().$("Resumed ACK sent successfully [fd=").$(context.getFd())
                        .$(", upTo=").$(state.getLastAckedSequence()).I$();
                if (state.hasPendingAck()) {
                    trySendAck(context, state);
                }
                if (state.isDurableAckEnabled()) {
                    trySendDurableAck(context, state);
                }
            }
            case QwpProcessorState.SEND_STATE_RESUME_DURABLE_ACK -> {
                context.resumeResponseSend();
                state.onResumeDurableAckComplete();
                LOG.debug().$("Resumed durable ACK sent successfully [fd=").$(context.getFd()).I$();
                trySendDurableAck(context, state);
            }
            case QwpProcessorState.SEND_STATE_RESUME_ERROR -> {
                context.resumeResponseSend();
                LOG.debug().$("Resumed error response sent successfully [fd=").$(context.getFd()).I$();
                state.onResumeErrorComplete();
            }
            case QwpProcessorState.SEND_STATE_RESUME_ACK_THEN_ERROR -> {
                context.resumeResponseSend();
                state.onResumeAckComplete();
                LOG.debug().$("Resumed ACK sent successfully [fd=").$(context.getFd())
                        .$(", upTo=").$(state.getLastAckedSequence()).I$();
                sendDeferredErrorResponse(context, state);
            }
            case QwpProcessorState.SEND_STATE_RESUME_DURABLE_ACK_THEN_ERROR -> {
                context.resumeResponseSend();
                state.onResumeDurableAckComplete();
                LOG.debug().$("Resumed durable ACK sent successfully [fd=").$(context.getFd()).I$();
                sendDeferredErrorResponse(context, state);
            }
            default -> {
                LOG.critical().$("Invalid WebSocket send state [fd=").$(context.getFd())
                        .$(", state=").$(state.getSendState()).I$();
                throw ServerDisconnectException.INSTANCE;
            }
        }
    }

    private static int badRequestResponseSize(String reason) {
        return badRequestResponseSize(reason.getBytes(StandardCharsets.UTF_8).length);
    }

    private static int badRequestResponseSize(int reasonByteCount) {
        return BAD_REQUEST_PREFIX.length
                + Integer.toString(reasonByteCount).length()
                + HTTP_HEADER_END.length
                + reasonByteCount;
    }

    private static HttpException responseDoesNotFitSendBuffer(long fd, CharSequence responseType, int bufferSize, int requiredSize) {
        LOG.error().$("WebSocket ").$(responseType).$(" does not fit send buffer [fd=").$(fd)
                .$(", required=").$(requiredSize)
                .$(", available=").$(bufferSize).I$();
        return HttpException.instance("WebSocket ").put(responseType)
                .put(" does not fit send buffer [required=").put(requiredSize)
                .put(", available=").put(bufferSize)
                .put(']');
    }

    private void drainPendingResponse(HttpConnectionContext context, QwpProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        switch (state.getSendState()) {
            case QwpProcessorState.SEND_STATE_READY -> {
            }
            case QwpProcessorState.SEND_STATE_RESUME_ACK -> {
                context.resumeResponseSend();
                state.onResumeAckComplete();
            }
            case QwpProcessorState.SEND_STATE_RESUME_DURABLE_ACK -> {
                context.resumeResponseSend();
                state.onResumeDurableAckComplete();
            }
            case QwpProcessorState.SEND_STATE_RESUME_ERROR -> {
                context.resumeResponseSend();
                state.onResumeErrorComplete();
            }
            case QwpProcessorState.SEND_STATE_RESUME_ACK_THEN_ERROR -> {
                context.resumeResponseSend();
                state.onResumeAckComplete();
                sendDeferredErrorResponse(context, state);
            }
            case QwpProcessorState.SEND_STATE_RESUME_DURABLE_ACK_THEN_ERROR -> {
                context.resumeResponseSend();
                state.onResumeDurableAckComplete();
                sendDeferredErrorResponse(context, state);
            }
            default -> {
                LOG.critical().$("Invalid WebSocket send state during close [fd=").$(context.getFd())
                        .$(", state=").$(state.getSendState()).I$();
                throw PeerDisconnectedException.INSTANCE;
            }
        }
    }

    private void flushPendingAck(HttpConnectionContext context, QwpProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (state.hasPendingAck()) {
            trySendAck(context, state);
        }
        if (state.isDurableAckEnabled() && state.isSendReady()) {
            trySendDurableAck(context, state);
        }
    }

    private void handleBinaryMessage(HttpConnectionContext context, QwpProcessorState state, long payload, int length)
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

            // Process the QWP v1 message
            state.processMessage();

            if (state.isOk()) {
                state.commit();
            }
            // commit() swallows exceptions internally
            if (state.isOk()) {
                LOG.debug().$("WebSocket message committed [fd=").$(context.getFd())
                        .$(", seq=").$(seq).I$();
            } else {
                errorMessage = state.getErrorText();
                LOG.error().$("WebSocket message processing failed [fd=").$(context.getFd())
                        .$(", error=").$safe(errorMessage).I$();
                responseStatus = switch (state.getStatus()) {
                    case PARSE_ERROR -> STATUS_PARSE_ERROR;
                    case SCHEMA_MISMATCH -> STATUS_SCHEMA_MISMATCH;
                    case SECURITY_ERROR -> STATUS_SECURITY_ERROR;
                    case INTERNAL_ERROR -> STATUS_INTERNAL_ERROR;
                    default -> STATUS_WRITE_ERROR;
                };
            }
        } catch (Throwable e) {
            LOG.error().$("WebSocket ILP processing error [fd=").$(context.getFd())
                    .$(", seq=").$(seq)
                    .$(", error=").$(e).I$();

            responseStatus = STATUS_INTERNAL_ERROR;
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
                try {
                    trySendAck(context, state);
                } catch (PeerIsSlowToReadException e) {
                    state.onErrorBlocked(responseStatus, seq, errorMessage);
                    throw e;
                }
            }
            sendErrorResponse(context, state, seq, responseStatus, errorMessage);
        }
    }

    private void handleClose(HttpConnectionContext context, QwpProcessorState state, long payload, int length) {
        int closeCode = -1;
        if (length >= 2) {
            int high = Unsafe.getByte(payload) & 0xFF;
            int low = Unsafe.getByte(payload + 1) & 0xFF;
            closeCode = (high << 8) | low;
        }
        LOG.info().$("WebSocket close [fd=").$(context.getFd()).$(", code=").$(closeCode).I$();

        // Flush any pending ACKs for already-committed data before closing.
        // The client may have sent [BINARY₁, ..., BINARYₙ, CLOSE] in the same
        // TCP segment — those messages are committed but not yet ACKed. Without
        // this flush the client would never learn that its data was persisted.
        try {
            flushPendingAck(context, state);
        } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
            // Best effort — if the ACK can't be sent, proceed with close.
            // PeerIsSlowToReadException transitions into a resume state, so the
            // isSendReady() check below will skip the close response.
        }

        // Send close response only if buffer is clear
        if (!state.isSendReady()) {
            LOG.debug().$("Skipping close response, buffer busy [fd=").$(context.getFd()).I$();
            return;
        }

        try {
            HttpRawSocket rawSocket = context.getRawResponseSocket();
            long bufferAddr = rawSocket.getBufferAddress();
            int bufferSize = rawSocket.getBufferSize();

            // Normalize close code for the response per RFC 6455 Section 7.4:
            // - 1004 is reserved and has no defined meaning
            // - 1005, 1006, 1015 must not appear on the wire
            // - 2000-2999 are reserved for extensions (none negotiated)
            // - codes outside the valid 1000-4999 range (including -1 for
            //   no-payload frames) are replaced with 1000 (normal closure)
            int responseCode;
            if (closeCode == 1004 || (closeCode >= 2000 && closeCode <= 2999)) {
                responseCode = WebSocketCloseCode.PROTOCOL_ERROR;
            } else if (closeCode < 1000 || closeCode > 4999
                    || closeCode == 1005 || closeCode == 1006 || closeCode == 1015) {
                responseCode = WebSocketCloseCode.NORMAL_CLOSURE;
            } else {
                responseCode = closeCode;
            }

            int written = WebSocketFrameWriter.writeCloseFrame(bufferAddr, bufferSize, responseCode, null);
            if (written > 0) {
                rawSocket.send(written);
            }
        } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
            // Ignore, we're closing anyway
        }
    }

    private void handlePing(HttpConnectionContext context, QwpProcessorState state, long payload, int length) {
        // PING is a documented flush point for pending ACK/durable-ACK frames.
        // A client may send PING specifically to prod the server into emitting
        // acks for commits whose uploads have completed since the last message.
        try {
            flushPendingAck(context, state);
        } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
            // Best effort — if the ACK/durable-ACK can't be sent, proceed
            // without throwing so the caller doesn't abort ping handling.
            // PeerIsSlowToReadException transitions into a resume state,
            // so the isSendReady() check below skips the pong. The client
            // may retry ping or the ACK will flush on the next drain.
        }

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

    private void handleWebSocketFrame(HttpConnectionContext context, QwpProcessorState state, int opcode, boolean fin, long payload, int length)
            throws ServerDisconnectException, PeerDisconnectedException, PeerIsSlowToReadException {
        switch (opcode) {
            case WebSocketOpcode.BINARY -> {
                if (!fin) {
                    // A BINARY frame with FIN=0 is the start of a fragmented message.
                    // We don't support reassembly — reject immediately so the client
                    // (or intermediary proxy/load balancer) knows data was not ingested.
                    rejectFragmentedFrame(context, state, opcode);
                    return;
                }
                handleBinaryMessage(context, state, payload, length);
            }
            case WebSocketOpcode.CONTINUATION ->
                // Continuation frames are part of a fragmented message we never started
                // tracking. Reject so the sender knows data was not ingested.
                    rejectFragmentedFrame(context, state, opcode);
            case WebSocketOpcode.TEXT -> rejectTextFrame(context, state);
            case WebSocketOpcode.PING -> handlePing(context, state, payload, length);
            case WebSocketOpcode.PONG -> LOG.debug().$("WebSocket pong [fd=").$(context.getFd()).I$();
            case WebSocketOpcode.CLOSE -> {
                handleClose(context, state, payload, length);
                throw ServerDisconnectException.INSTANCE;
            }
            default -> LOG.debug().$("WebSocket unknown opcode [fd=").$(context.getFd()).$(", opcode=").$(opcode).I$();
        }
    }

    private int negotiateQwpVersion(HttpRequestHeader requestHeader, long fd) {
        int clientMaxVersion = QwpConstants.VERSION_1; // default if header absent
        Utf8Sequence maxVersionHeader = requestHeader.getHeader(QwpWebSocketHttpProcessor.HEADER_X_QWP_MAX_VERSION);
        if (maxVersionHeader != null) {
            int parsed = Numbers.parseNonNegativeIntQuiet(maxVersionHeader);
            if (parsed >= QwpConstants.VERSION_1) {
                clientMaxVersion = parsed;
            }
        }

        int negotiated = Math.min(clientMaxVersion, QwpConstants.MAX_SUPPORTED_INGEST_VERSION);

        Utf8Sequence clientId = requestHeader.getHeader(QwpWebSocketHttpProcessor.HEADER_X_QWP_CLIENT_ID);
        if (clientId != null) {
            LOG.info().$("QWP version negotiated [fd=").$(fd)
                    .$(", clientId=").$(clientId)
                    .$(", clientMax=").$(clientMaxVersion)
                    .$(", negotiated=").$(negotiated).I$();
        } else {
            LOG.info().$("QWP version negotiated [fd=").$(fd)
                    .$(", clientMax=").$(clientMaxVersion)
                    .$(", negotiated=").$(negotiated).I$();
        }

        return negotiated;
    }

    private void processWebSocketFrames(HttpConnectionContext context, QwpProcessorState state, long buffer, int bufferLen)
            throws ServerDisconnectException, PeerDisconnectedException, PeerIsSlowToReadException {
        long bufferEnd = buffer + bufferLen;
        long pos = buffer;
        FrameProcessResult result = FrameProcessResult.COMPLETE;

        try {
            while (pos < bufferEnd) {
                frameParser.reset();
                int consumed = frameParser.parse(pos, bufferEnd);

                if (frameParser.getState() == WebSocketFrameParser.STATE_ERROR) {
                    LOG.error().$("WebSocket frame error [fd=").$(context.getFd()).$(", code=").$(frameParser.getErrorCode()).I$();
                    throw ServerDisconnectException.INSTANCE;
                }

                if (frameParser.getState() == WebSocketFrameParser.STATE_NEED_PAYLOAD) {
                    long totalFrameSize = frameParser.getHeaderSize() + frameParser.getPayloadLength();
                    if (totalFrameSize > recvBufferSize) {
                        // Payload declared in the frame header exceeds recv buffer capacity.
                        // Reject immediately instead of wasting bandwidth filling the buffer.
                        LOG.error().$("WebSocket frame too large [fd=").$(context.getFd())
                                .$(", payloadLength=").$(frameParser.getPayloadLength())
                                .$(", bufferSize=").$(recvBufferSize).I$();
                        if (state.isSendReady()) {
                            try {
                                HttpRawSocket rawSocket = context.getRawResponseSocket();
                                long bufferAddr = rawSocket.getBufferAddress();
                                int bufferSize = rawSocket.getBufferSize();
                                int written = WebSocketFrameWriter.writeCloseFrame(
                                        bufferAddr, bufferSize,
                                        WebSocketCloseCode.MESSAGE_TOO_BIG,
                                        "frame payload exceeds maximum size"
                                );
                                if (written > 0) {
                                    rawSocket.send(written);
                                }
                            } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
                                // Best effort -- we're disconnecting anyway.
                            }
                        }
                        throw ServerDisconnectException.INSTANCE;
                    }
                    result = FrameProcessResult.NEED_MORE_DATA;
                    break;
                }

                if (consumed == 0 || frameParser.getState() == WebSocketFrameParser.STATE_NEED_MORE) {
                    result = FrameProcessResult.NEED_MORE_DATA;
                    break;
                }

                // Frame parsed successfully
                int opcode = frameParser.getOpcode();
                long payloadPtr = pos + frameParser.getHeaderSize();
                int payloadLen = (int) frameParser.getPayloadLength();

                // Unmask payload
                if (frameParser.isMasked()) {
                    frameParser.unmaskPayload(payloadPtr, payloadLen);
                }

                // Advance past this frame BEFORE processing. If handleWebSocketFrame
                // throws (e.g. ACK backpressure), the committed frame won't be replayed.
                pos += consumed;

                handleWebSocketFrame(context, state, opcode, frameParser.isFin(), payloadPtr, payloadLen);
            }

            if (result == FrameProcessResult.COMPLETE) {
                // All frames processed — flush any pending cumulative ACK
                flushPendingAck(context, state);
            }
        } finally {
            // Compact unprocessed bytes to buffer start and update state.
            // Handles both normal exit (remaining=0) and exception unwind
            // (e.g. PeerIsSlowToReadException from trySendAck after a committed frame).
            int remaining = (int) (bufferEnd - pos);
            if (remaining > 0 && pos > buffer) {
                Unsafe.copyMemory(pos, buffer, remaining);
            }
            state.setRecvBufferLen(remaining);
        }

    }

    private void rejectFragmentedFrame(HttpConnectionContext context, QwpProcessorState state, int opcode)
            throws ServerDisconnectException {
        LOG.error()
                .$("WebSocket fragmented frame rejected, QWP requires unfragmented messages [fd=").$(context.getFd())
                .$(", opcode=").$(WebSocketOpcode.name(opcode))
                .$("] a WebSocket intermediary (proxy, load balancer) may be fragmenting frames; ")
                .$("configure it to pass WebSocket frames through without fragmentation, ")
                .$("or connect the QWP client directly to QuestDB")
                .I$();

        // Best-effort CLOSE with protocol error — the client or intermediary
        // receives a clear reason instead of a silent connection drop.
        if (state.isSendReady()) {
            try {
                HttpRawSocket rawSocket = context.getRawResponseSocket();
                long bufferAddr = rawSocket.getBufferAddress();
                int bufferSize = rawSocket.getBufferSize();
                int written = WebSocketFrameWriter.writeCloseFrame(
                        bufferAddr, bufferSize,
                        WebSocketCloseCode.PROTOCOL_ERROR,
                        "fragmented WebSocket frames are not supported"
                );
                if (written > 0) {
                    rawSocket.send(written);
                }
            } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
                // Best effort — we're disconnecting anyway.
            }
        }
        throw ServerDisconnectException.INSTANCE;
    }

    private void rejectTextFrame(HttpConnectionContext context, QwpProcessorState state)
            throws ServerDisconnectException {
        LOG.error()
                .$("WebSocket text frame rejected, QWP accepts only binary frames [fd=").$(context.getFd())
                .I$();

        // Best-effort CLOSE with 1003 (Unsupported Data) per RFC 6455 Section 7.4.1.
        if (state.isSendReady()) {
            try {
                HttpRawSocket rawSocket = context.getRawResponseSocket();
                long bufferAddr = rawSocket.getBufferAddress();
                int bufferSize = rawSocket.getBufferSize();
                int written = WebSocketFrameWriter.writeCloseFrame(
                        bufferAddr, bufferSize,
                        WebSocketCloseCode.UNSUPPORTED_DATA,
                        "text frames are not supported, QWP requires binary frames"
                );
                if (written > 0) {
                    rawSocket.send(written);
                }
            } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
                // Best effort — we're disconnecting anyway.
            }
        }
        throw ServerDisconnectException.INSTANCE;
    }

    /**
     * Sends the deferred error response stored in the processor state.
     * <p>
     * Used after a blocked ACK resumes and the original failure response must
     * be delivered before any later ACK activity can overtake it.
     */
    private void sendDeferredErrorResponse(HttpConnectionContext context, QwpProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        sendErrorResponse(
                context,
                state,
                state.getDeferredErrorSequence(),
                state.getDeferredErrorStatus(),
                state.getDeferredErrorMessage()
        );
    }

    private void sendErrorResponse(
            HttpConnectionContext context,
            QwpProcessorState state,
            long sequence,
            byte status,
            CharSequence errorMessage
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (!state.isSendReady()) {
            state.onErrorBlocked(status, sequence, errorMessage);
            throw PeerIsSlowToReadException.INSTANCE;
        }

        try {
            HttpRawSocket rawSocket = context.getRawResponseSocket();
            long bufferAddr = rawSocket.getBufferAddress();
            int bufferSize = rawSocket.getBufferSize();

            // Calculate payload size (UTF-8 byte count, capped at 1024 bytes)
            int msgLen = errorMessage != null ? Utf8s.utf8Bytes(errorMessage, 1024) : 0;
            int payloadLen = 9 + 2 + msgLen; // status + seq + len + msg

            int frameSize = WebSocketFrameWriter.headerSize(payloadLen, false) + payloadLen;

            if (frameSize <= bufferSize) {
                int offset = WebSocketFrameWriter.writeBinaryFrameHeader(bufferAddr, payloadLen);

                // Write status
                Unsafe.putByte(bufferAddr + offset, status);
                offset += 1;

                // Write sequence (little-endian)
                Unsafe.putLong(bufferAddr + offset, sequence);
                offset += 8;

                // Write message length (little-endian)
                Unsafe.putShort(bufferAddr + offset, (short) msgLen);
                offset += 2;

                // Write message (UTF-16 to UTF-8 directly to native memory, no byte[] allocation)
                if (msgLen > 0) {
                    Utf8s.strCpyUtf8(errorMessage, bufferAddr + offset, msgLen);
                }
                offset += msgLen;

                rawSocket.send(offset);
                state.onErrorSent();
                LOG.debug().$("Sent error response [fd=").$(context.getFd())
                        .$(", seq=").$(sequence)
                        .$(", status=").$(status).I$();
            } else {
                LOG.critical().$("Buffer too small for error response [fd=").$(context.getFd())
                        .$(", required=").$(frameSize)
                        .$(", bufferSize=").$(bufferSize).I$();
                throw PeerDisconnectedException.INSTANCE;
            }
        } catch (PeerIsSlowToReadException e) {
            state.onErrorBlocked(status, sequence, errorMessage);
            LOG.debug().$("Failed to send error response [fd=").$(context.getFd())
                    .$(", seq=").$(sequence).I$();
            throw e;
        }
    }

    /**
     * Attempts to send a cumulative ACK for the highest processed sequence.
     * <p>
     * State transitions (managed by {@link QwpProcessorState}):
     * <ul>
     *   <li>READY + success → stays READY, updates lastAckedSequence</li>
     *   <li>READY + PeerIsSlowToReadException → transitions to SEND_STATE_RESUME_ACK, throws</li>
     * </ul>
     *
     * @param context the HTTP connection context
     * @param state   the per-connection processor state
     * @throws PeerIsSlowToReadException if the client's receive buffer is full (transitions to SEND_STATE_RESUME_ACK)
     * @throws PeerDisconnectedException if the client disconnected
     */
    private void trySendAck(HttpConnectionContext context, QwpProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        assert state.isSendReady() : "trySendAck called in wrong state";

        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufferAddr = rawSocket.getBufferAddress();
        int bufferSize = rawSocket.getBufferSize();

        int payloadLen = state.computeAckPayloadSize();
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
        long writeAddr = bufferAddr + headerLen;
        Unsafe.putByte(writeAddr, STATUS_OK);
        Unsafe.putLong(writeAddr + 1, sequence);
        QwpProcessorState.writeTableSeqTxnEntries(writeAddr + 9, state.getPendingAckSeqTxns());

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

    private void trySendDurableAck(HttpConnectionContext context, QwpProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        assert state.isSendReady() : "trySendDurableAck called in wrong state";

        CharSequenceLongHashMap progress = state.collectDurableProgress(engine.getDurableAckRegistry());
        if (progress.size() == 0) {
            return;
        }

        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufferAddr = rawSocket.getBufferAddress();
        int bufferSize = rawSocket.getBufferSize();

        int payloadLen = state.computeDurableAckPayloadSize();
        int frameSize = WebSocketFrameWriter.headerSize(payloadLen, false) + payloadLen;

        if (frameSize > bufferSize) {
            LOG.critical().$("Buffer too small for durable ACK response [fd=").$(context.getFd())
                    .$(", required=").$(frameSize)
                    .$(", bufferSize=").$(bufferSize).I$();
            throw PeerDisconnectedException.INSTANCE;
        }

        int headerLen = WebSocketFrameWriter.writeBinaryFrameHeader(bufferAddr, payloadLen);
        long writeAddr = bufferAddr + headerLen;
        Unsafe.putByte(writeAddr, STATUS_DURABLE_ACK);
        QwpProcessorState.writeTableSeqTxnEntries(writeAddr + 1, progress);

        try {
            rawSocket.send(headerLen + payloadLen);
            state.onDurableAckSent();
            LOG.debug().$("Sent durable ACK [fd=").$(context.getFd())
                    .$(", numOfTables=").$(progress.size()).I$();
        } catch (PeerIsSlowToReadException e) {
            state.onDurableAckBlocked();
            LOG.debug().$("Durable ACK blocked [fd=").$(context.getFd()).I$();
            throw e;
        }
    }

    private enum FrameProcessResult {
        COMPLETE,
        NEED_MORE_DATA
    }
}
