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
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.cutlass.qwp.websocket.WebSocketFrameParser;
import io.questdb.cutlass.qwp.websocket.WebSocketFrameWriter;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.Net;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.network.Socket;
import io.questdb.std.CharSequenceLongHashMap;
import io.questdb.std.Mutable;
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
 * Per-connection state is stored in {@link QwpIngressProcessorState} via {@link LocalValue},
 * so a single processor instance can safely be shared across connections on the same worker.
 */
public class QwpIngressUpgradeProcessor implements HttpRequestProcessor {
    // Cumulative ACK batch size
    private static final int ACK_BATCH_SIZE = 8;
    // HTTP response templates
    private static final byte[] BAD_REQUEST_PREFIX =
            "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: ".getBytes(StandardCharsets.US_ASCII);
    // HTTP_HEADER_END is declared out of alphabetical order on purpose: the
    // BAD_REQUEST_RESPONSE_* initializers below read it via
    // precomputeBadRequestResponse, and Java initializes static fields in
    // textual order. Moving HTTP_HEADER_END below the BAD_REQUEST_RESPONSE_*
    // block would leave it null at the time the precomputation runs.
    private static final byte[] HTTP_HEADER_END = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    // Precomputed full 400 Bad Request responses for each handshake validation
    // error. validateHandshake returns one of the ERROR_ singletons by reference,
    // and the reject path memcpys the matching response directly into the send
    // buffer. Replaces a per-reject triple-allocation (reason.getBytes +
    // Integer.toString + contentLength.getBytes) with a zero-GC lookup so
    // probe / attack traffic does not produce GC pressure on the connect path.
    private static final byte[] BAD_REQUEST_RESPONSE_CONNECTION_MUST_CONTAIN_UPGRADE =
            precomputeBadRequestResponse(QwpIngressHttpProcessor.ERROR_CONNECTION_MUST_CONTAIN_UPGRADE);
    private static final byte[] BAD_REQUEST_RESPONSE_INVALID_SEC_WEBSOCKET_KEY =
            precomputeBadRequestResponse(QwpIngressHttpProcessor.ERROR_INVALID_SEC_WEBSOCKET_KEY);
    private static final byte[] BAD_REQUEST_RESPONSE_INVALID_UPGRADE_HEADER_VALUE =
            precomputeBadRequestResponse(QwpIngressHttpProcessor.ERROR_INVALID_UPGRADE_HEADER_VALUE);
    private static final byte[] BAD_REQUEST_RESPONSE_MISSING_CONNECTION_HEADER =
            precomputeBadRequestResponse(QwpIngressHttpProcessor.ERROR_MISSING_CONNECTION_HEADER);
    private static final byte[] BAD_REQUEST_RESPONSE_MISSING_SEC_WEBSOCKET_KEY_HEADER =
            precomputeBadRequestResponse(QwpIngressHttpProcessor.ERROR_MISSING_SEC_WEBSOCKET_KEY_HEADER);
    private static final byte[] BAD_REQUEST_RESPONSE_MISSING_UPGRADE_HEADER =
            precomputeBadRequestResponse(QwpIngressHttpProcessor.ERROR_MISSING_UPGRADE_HEADER);
    private static final byte[] BAD_REQUEST_RESPONSE_ORIGIN_HEADER_NOT_ALLOWED =
            precomputeBadRequestResponse(QwpIngressHttpProcessor.ERROR_ORIGIN_HEADER_NOT_ALLOWED);
    private static final Log LOG = LogFactory.getLog(QwpIngressUpgradeProcessor.class);
    private static final LocalValue<QwpIngressProcessorState> LV = new LocalValue<>();
    // Worst-case WebSocket frame header size (2-byte base + 8-byte 64-bit
    // extended length + 4-byte mask for client->server frames). Subtracted
    // from the recv buffer when computing the effective batch cap so the
    // advertised value still leaves room for the frame header on the wire.
    private static final int MAX_WS_FRAME_HEADER_BYTES = 14;
    // Carries the byte count of a 4xx upgrade rejection staged in the raw
    // response buffer by onHeadersReady, to be flushed by onRequestComplete
    // (which is allowed to propagate PeerIsSlowToReadException to the
    // framework's park-on-write path). Sized for the rare case of a
    // malformed or role-misrouted upgrade -- successful upgrades use the
    // handshake flush flags on QwpProcessorState instead and never touch
    // this LocalValue.
    private static final LocalValue<RejectFlushTracker> REJECT_FLUSH = new LocalValue<>();
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
    // Precomputed X-QWP-Max-Batch-Size header bytes, cached because the
    // effective cap is derived from recvBufferSize (config-fixed for the
    // lifetime of this processor) and would otherwise allocate a String and
    // a byte[] on every handshake. Null when the cap collapses to zero,
    // which omits the header entirely.
    private final byte[] effectiveMaxBatchSizeBytes;
    private final CairoEngine engine;
    private final int forceRecvFragmentationChunkSize;
    // WebSocket frame parser (scratchpad — fully reset within each processWebSocketFrames call)
    private final WebSocketFrameParser frameParser = new WebSocketFrameParser();
    private final HttpFullFatServerConfiguration httpConfiguration;
    private final int maxResponseContentLength;
    private final int recvBufferSize;

    public QwpIngressUpgradeProcessor(CairoEngine engine, HttpFullFatServerConfiguration httpConfiguration) {
        this.engine = engine;
        this.forceRecvFragmentationChunkSize = httpConfiguration.getHttpContextConfiguration()
                .getForceRecvFragmentationChunkSize();
        this.httpConfiguration = httpConfiguration;
        this.recvBufferSize = httpConfiguration.getRecvBufferSize();
        // Advertise the effective batch cap, not the QWP protocol ceiling. The
        // HTTP recv buffer is the actual binding constraint on inbound
        // WebSocket frame size, and it is checked before the QWP parser ever
        // sees the payload -- a frame larger than recv-buffer minus the
        // worst-case WebSocket frame header gets closed with code 1009 long
        // before STATUS_PARSE_ERROR can fire.
        int effectiveMaxBatchSize = Math.min(
                Math.max(0, recvBufferSize - MAX_WS_FRAME_HEADER_BYTES),
                QwpConstants.DEFAULT_MAX_BATCH_SIZE);
        this.effectiveMaxBatchSizeBytes = effectiveMaxBatchSize > 0
                ? Integer.toString(effectiveMaxBatchSize).getBytes(StandardCharsets.US_ASCII)
                : null;
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
        // Fast path: validateHandshake returns one of the ERROR_ singletons, so
        // the connect path always hits this lookup and avoids any allocation.
        byte[] precomputed = precomputedBadRequestResponse(reason);
        if (precomputed != null) {
            if (precomputed.length > bufferSize) {
                return -1;
            }
            Unsafe.copyMemory(precomputed, Unsafe.BYTE_OFFSET, null, buffer, precomputed.length);
            return precomputed.length;
        }

        // Slow path: arbitrary reason text (tests, future callers). Builds the
        // response with the customary getBytes / Integer.toString allocations.
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
        byte[] acceptKey = QwpIngressHttpProcessor.computeAcceptKey(key);
        int requiredSize = QwpIngressHttpProcessor.responseSize(acceptKey, qwpVersion);

        if (requiredSize > bufferSize) {
            return -1;
        }

        return QwpIngressHttpProcessor.writeResponse(buffer, acceptKey, qwpVersion);
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
        QwpIngressProcessorState state = LV.get(context);
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
            // Leave the state instance in the LocalValueMap slot. onDisconnected
            // resets the per-connection scoreboard (recv buffer length, sequence
            // counters, ACK / durable maps, send state, symbol cache) so the
            // next connection that lands on this context starts clean; the
            // connection-scoped native scaffolding (bufferAddress and the
            // pre-allocated decoder / appender / tudCache sub-objects) is sized
            // to the HttpConnectionContext and gets reused without paying the
            // re-allocation cost on every reconnect. LocalValueMap.close()
            // invokes state.close() at HTTP context teardown.
            state.onDisconnected();
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

        String validationError = QwpIngressHttpProcessor.validateHandshake(context.getRequestHeader());
        if (validationError != null) {
            LOG.error().$("WebSocket handshake validation failed [fd=").$(context.getFd())
                    .$(", error=").$(validationError).I$();
            final boolean versionError = QwpIngressHttpProcessor.isVersionValidationError(validationError);
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
            // Defer rawSocket.send to onRequestComplete for the same reason
            // the 101 success path defers: onHeadersReady is forbidden from
            // throwing PeerIsSlowToReadException, so a small send-fragmentation
            // cap that splits the reject body across two sends would otherwise
            // discard the residual fragment and disconnect the client before
            // it could see the full 400 / 426 response.
            stageReject(context, bytesWritten);
            // PeerDisconnectedException propagates to handleClientRecv → disconnectHttp()
            return;
        }

        byte role = engine.getQwpServerInfoProvider().role();
        byte[] roleBytes = QwpEgressMsgKind.roleNameBytes(role);
        if (role == QwpEgressMsgKind.ROLE_REPLICA || role == QwpEgressMsgKind.ROLE_PRIMARY_CATCHUP) {
            int rejectSize = QwpIngressHttpProcessor.misdirectedRequestWithRoleSize(roleBytes);
            if (rejectSize > bufferSize) {
                throw responseDoesNotFitSendBuffer(context.getFd(), "421 ingress role-reject response", bufferSize, rejectSize);
            }
            int rejectBytes = QwpIngressHttpProcessor.writeMisdirectedRequestWithRole(bufferAddr, bufferSize, roleBytes);
            if (rejectBytes <= 0) {
                throw responseDoesNotFitSendBuffer(context.getFd(), "421 ingress role-reject response", bufferSize, rejectSize);
            }
            // Same deferral rationale as the 400 / 426 paths above: a small
            // send-fragmentation cap would otherwise drop the second-fragment
            // send of the 421 body and disconnect the client before it could
            // see the X-QuestDB-Role header that tells it where to retry.
            stageReject(context, rejectBytes);
            LOG.info().$("ingress upgrade rejected by role [fd=").$(context.getFd())
                    .$(", role=").$(QwpEgressMsgKind.roleName(role)).I$();
            return;
        }

        HttpRequestHeader requestHeader = context.getRequestHeader();
        Utf8Sequence wsKey = QwpIngressHttpProcessor.getWebSocketKey(requestHeader);

        // Read QWP version negotiation headers
        int negotiatedVersion = negotiateQwpVersion(requestHeader, context.getFd());

        byte[] acceptKey = QwpIngressHttpProcessor.computeAcceptKey(wsKey);

        // Resolve durable-ack opt-in before sizing the 101 response, since
        // the X-QWP-Durable-Ack confirmation header affects the response size.
        // The header is silently dropped when the engine has no durable-ack
        // registry installed (OSS build or primary replication disabled), so
        // opted-in clients on such servers receive a 101 without confirmation
        // and fail at the client side.
        Utf8Sequence durableAckHeader = requestHeader.getHeader(
                QwpIngressHttpProcessor.HEADER_X_QWP_REQUEST_DURABLE_ACK);
        boolean durableAckRequested = durableAckHeader != null
                && Utf8s.equalsIgnoreCaseAscii(durableAckHeader, QwpIngressHttpProcessor.HEADER_VALUE_DURABLE_ACK_ENABLED);
        boolean durableAckEnabled = durableAckRequested && engine.getDurableAckRegistry().isEnabled();

        int requiredHandshakeSize = QwpIngressHttpProcessor.responseSize(
                acceptKey, negotiatedVersion, null, durableAckEnabled, roleBytes,
                effectiveMaxBatchSizeBytes);
        if (requiredHandshakeSize > bufferSize) {
            throw responseDoesNotFitSendBuffer(context.getFd(), "101 handshake response", bufferSize, requiredHandshakeSize);
        }

        // Initialize or get the ILP processor state for this connection only after
        // confirming the 101 response fits in the raw HTTP send buffer.
        QwpIngressProcessorState state = LV.get(context);
        if (state == null) {
            state = new QwpIngressProcessorState(
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
        state.setDurableAckEnabled(durableAckEnabled);

        // Write the 101 Switching Protocols response (reuse the pre-computed accept key)
        int bytesWritten = QwpIngressHttpProcessor.writeResponse(
                bufferAddr, acceptKey, negotiatedVersion, null, durableAckEnabled, roleBytes,
                effectiveMaxBatchSizeBytes);
        if (bytesWritten <= 0) {
            throw responseDoesNotFitSendBuffer(context.getFd(), "101 handshake response", bufferSize, requiredHandshakeSize);
        }
        // The HttpRequestProcessor contract forbids PeerIsSlowToReadException
        // from onHeadersReady, so we defer the raw-socket send to
        // onRequestComplete where PISR propagates cleanly into the framework's
        // park-on-write path. State carries the byte count across the two
        // calls (the framework invokes them back-to-back in handleClientRecv).
        // Without this deferral a small send-fragmentation cap (e.g.
        // DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE=125 with a ~220-byte
        // response) would partial-send and silently drop the rest, leaving
        // the client waiting on a handshake that never completes.
        state.setPendingHandshakeBytes(bytesWritten);
        state.setHandshakeFlushPending(true);
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        RejectFlushTracker rejectTracker = REJECT_FLUSH.get(context);
        if (rejectTracker != null && rejectTracker.pendingBytes > 0) {
            // Flush the deferred 400 / 426 / 421 reject body. PISR propagates
            // into the framework's park-on-write path; resumeSend picks the
            // residual flush back up and disconnects after the last byte.
            // pendingBytes stays non-zero until the send returns normally so
            // resumeSend can recognise that it is still in the reject path.
            HttpRawSocket rawSocket = context.getRawResponseSocket();
            rawSocket.send(rejectTracker.pendingBytes);
            rejectTracker.pendingBytes = 0;
            // Send completed in a single call. Throw HttpException so
            // handleClientRecv tears the connection down after the reject body
            // has fully landed on the wire.
            throw HttpException.instance("WebSocket upgrade rejected");
        }
        QwpIngressProcessorState state = LV.get(context);
        if (state == null || !state.isHandshakeFlushPending()) {
            // Either we're already past the handshake (post-protocol-switch
            // onRequestComplete after a recv cycle) or onHeadersReady
            // short-circuited (validation error / role reject) without
            // setting the deferred flush.
            if (state != null && state.isWsHandshakeSent()) {
                LOG.debug().$("WebSocket handshake complete, ready for frames [fd=").$(context.getFd()).I$();
            }
            return;
        }
        HttpRawSocket rawSocket = context.getRawResponseSocket();
        // rawSocket.send may park us when send fragmentation forces a
        // multi-fragment write. PISR propagates to handleClientRecv which
        // parks the connection for write and schedules resumeSend; resumeSend
        // finalises the protocol switch after the rest of the handshake
        // bytes flush.
        rawSocket.send(state.getPendingHandshakeBytes());
        finalizeHandshake(context, state);
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
        QwpIngressProcessorState state = LV.get(context);
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
                // payload exceeds recv buffer capacity. Notify the client with
                // a protocol-level CLOSE so it can distinguish "your frame is
                // too big" from a generic network failure.
                LOG.error().$("WebSocket frame too large for recv buffer [fd=").$(context.getFd())
                        .$(", bufferSize=").$(recvBufferSize).I$();
                sendFatalClose(context, state,
                        WebSocketCloseCode.MESSAGE_TOO_BIG,
                        "frame payload exceeds receive buffer capacity");
                return; // unreachable — sendFatalClose throws.
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
        } catch (PeerDisconnectedException e) {
            LOG.info().$("WebSocket peer disconnected [fd=").$(context.getFd()).I$();
            throw ServerDisconnectException.INSTANCE;
        } catch (Throwable e) {
            LOG.error().$("WebSocket error [fd=").$(context.getFd()).$(", error=").$(e).I$();
            throw ServerDisconnectException.INSTANCE;
        }
    }

    @Override
    public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        RejectFlushTracker rejectTracker = REJECT_FLUSH.get(context);
        if (rejectTracker != null && rejectTracker.pendingBytes > 0) {
            // Residual bytes of a 4xx upgrade reject were parked mid-write.
            // Flush the rest (PISR re-parks until the kernel takes the last
            // byte) and then close the connection -- there is no protocol to
            // switch to on a reject.
            context.resumeResponseSend();
            rejectTracker.pendingBytes = 0;
            throw ServerDisconnectException.INSTANCE;
        }

        QwpIngressProcessorState state = LV.get(context);
        if (state == null) {
            throw ServerDisconnectException.INSTANCE;
        }

        // If the 101 handshake response was parked mid-write (small send
        // fragmentation cap), flush the residual bytes first and finalise
        // the protocol switch. The connection is still in HTTP mode at this
        // point; finalizeHandshake() flips to WebSocket so the next recv
        // parses frames rather than HTTP.
        if (state.isHandshakeFlushPending()) {
            context.resumeResponseSend();
            finalizeHandshake(context, state);
            return;
        }

        switch (state.getSendState()) {
            case QwpIngressProcessorState.SEND_STATE_READY -> {
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_ACK -> {
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
            case QwpIngressProcessorState.SEND_STATE_RESUME_DURABLE_ACK -> {
                context.resumeResponseSend();
                state.onResumeDurableAckComplete();
                LOG.debug().$("Resumed durable ACK sent successfully [fd=").$(context.getFd()).I$();
                trySendDurableAck(context, state);
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_ERROR -> {
                context.resumeResponseSend();
                LOG.debug().$("Resumed error response sent successfully [fd=").$(context.getFd()).I$();
                state.onResumeErrorComplete();
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_ACK_THEN_ERROR -> {
                context.resumeResponseSend();
                state.onResumeAckComplete();
                LOG.debug().$("Resumed ACK sent successfully [fd=").$(context.getFd())
                        .$(", upTo=").$(state.getLastAckedSequence()).I$();
                sendDeferredErrorResponse(context, state);
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_DURABLE_ACK_THEN_ERROR -> {
                context.resumeResponseSend();
                state.onResumeDurableAckComplete();
                LOG.debug().$("Resumed durable ACK sent successfully [fd=").$(context.getFd()).I$();
                sendDeferredErrorResponse(context, state);
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_ACK_THEN_CLOSE -> {
                context.resumeResponseSend();
                state.onResumeAckComplete();
                LOG.debug().$("Resumed ACK sent before fatal close [fd=").$(context.getFd())
                        .$(", upTo=").$(state.getLastAckedSequence()).I$();
                finishDeferredFatalClose(context, state);
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_DURABLE_ACK_THEN_CLOSE -> {
                context.resumeResponseSend();
                state.onResumeDurableAckComplete();
                LOG.debug().$("Resumed durable ACK sent before fatal close [fd=").$(context.getFd()).I$();
                finishDeferredFatalClose(context, state);
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_DRAIN_THEN_CLOSE -> {
                context.resumeResponseSend();
                state.onResumeDrainComplete();
                LOG.debug().$("Resumed parked response drained before fatal close [fd=").$(context.getFd()).I$();
                finishDeferredFatalClose(context, state);
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_CLOSE -> {
                context.resumeResponseSend();
                LOG.debug().$("Resumed CLOSE frame sent [fd=").$(context.getFd()).I$();
                gracefulCloseAndDisconnect(context);
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_PONG -> {
                context.resumeResponseSend();
                state.onResumePongComplete();
                LOG.debug().$("Resumed pong frame sent [fd=").$(context.getFd()).I$();
            }
            default -> {
                LOG.critical().$("Invalid WebSocket send state [fd=").$(context.getFd())
                        .$(", state=").$(state.getSendState()).I$();
                throw ServerDisconnectException.INSTANCE;
            }
        }
        drainBufferedFrames(context, state);
    }

    private static int badRequestResponseSize(String reason) {
        byte[] precomputed = precomputedBadRequestResponse(reason);
        if (precomputed != null) {
            return precomputed.length;
        }
        return badRequestResponseSize(reason.getBytes(StandardCharsets.UTF_8).length);
    }

    private static int badRequestResponseSize(int reasonByteCount) {
        return BAD_REQUEST_PREFIX.length
                + Integer.toString(reasonByteCount).length()
                + HTTP_HEADER_END.length
                + reasonByteCount;
    }

    private static void finalizeHandshake(HttpConnectionContext context, QwpIngressProcessorState state) {
        state.setWsHandshakeSent(true);
        state.setHandshakeFlushPending(false);
        state.setPendingHandshakeBytes(0);
        LOG.info().$("WebSocket handshake sent [fd=").$(context.getFd()).I$();
        // Switch to WebSocket protocol -- the framework now routes recvs to
        // resumeRecv (frame parser) instead of HTTP request parsing.
        context.switchProtocol();
    }

    private static byte[] precomputeBadRequestResponse(String reason) {
        byte[] reasonBytes = reason.getBytes(StandardCharsets.US_ASCII);
        byte[] contentLengthBytes = Integer.toString(reasonBytes.length).getBytes(StandardCharsets.US_ASCII);
        byte[] result = new byte[BAD_REQUEST_PREFIX.length + contentLengthBytes.length
                + HTTP_HEADER_END.length + reasonBytes.length];
        int offset = 0;
        System.arraycopy(BAD_REQUEST_PREFIX, 0, result, offset, BAD_REQUEST_PREFIX.length);
        offset += BAD_REQUEST_PREFIX.length;
        System.arraycopy(contentLengthBytes, 0, result, offset, contentLengthBytes.length);
        offset += contentLengthBytes.length;
        System.arraycopy(HTTP_HEADER_END, 0, result, offset, HTTP_HEADER_END.length);
        offset += HTTP_HEADER_END.length;
        System.arraycopy(reasonBytes, 0, result, offset, reasonBytes.length);
        return result;
    }

    // Reference-identity switch on the singleton ERROR_ String constants
    // returned by QwpWebSocketHttpProcessor.validateHandshake. Returns the
    // pre-built 400 response for known errors, null for arbitrary text. The
    // returned byte[] is shared and read-only -- copy bytes into the response
    // buffer, do not mutate.
    private static byte[] precomputedBadRequestResponse(String validationError) {
        if (validationError == null) {
            return null;
        }
        return switch (validationError) {
            case QwpIngressHttpProcessor.ERROR_CONNECTION_MUST_CONTAIN_UPGRADE ->
                    BAD_REQUEST_RESPONSE_CONNECTION_MUST_CONTAIN_UPGRADE;
            case QwpIngressHttpProcessor.ERROR_INVALID_SEC_WEBSOCKET_KEY ->
                    BAD_REQUEST_RESPONSE_INVALID_SEC_WEBSOCKET_KEY;
            case QwpIngressHttpProcessor.ERROR_INVALID_UPGRADE_HEADER_VALUE ->
                    BAD_REQUEST_RESPONSE_INVALID_UPGRADE_HEADER_VALUE;
            case QwpIngressHttpProcessor.ERROR_MISSING_CONNECTION_HEADER ->
                    BAD_REQUEST_RESPONSE_MISSING_CONNECTION_HEADER;
            case QwpIngressHttpProcessor.ERROR_MISSING_SEC_WEBSOCKET_KEY_HEADER ->
                    BAD_REQUEST_RESPONSE_MISSING_SEC_WEBSOCKET_KEY_HEADER;
            case QwpIngressHttpProcessor.ERROR_MISSING_UPGRADE_HEADER -> BAD_REQUEST_RESPONSE_MISSING_UPGRADE_HEADER;
            case QwpIngressHttpProcessor.ERROR_ORIGIN_HEADER_NOT_ALLOWED ->
                    BAD_REQUEST_RESPONSE_ORIGIN_HEADER_NOT_ALLOWED;
            default -> null;
        };
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

    private static void stageReject(HttpConnectionContext context, int bytesWritten) {
        RejectFlushTracker tracker = REJECT_FLUSH.get(context);
        if (tracker == null) {
            tracker = new RejectFlushTracker();
            REJECT_FLUSH.set(context, tracker);
        }
        tracker.pendingBytes = bytesWritten;
    }

    private void drainBufferedFrames(HttpConnectionContext context, QwpIngressProcessorState state)
            throws ServerDisconnectException, PeerDisconnectedException, PeerIsSlowToReadException {
        if (state.isSendReady() && state.getRecvBufferLen() > 0) {
            processWebSocketFrames(context, state, context.getRecvBuffer(), state.getRecvBufferLen());
        }
    }

    private void drainPendingResponse(HttpConnectionContext context, QwpIngressProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        switch (state.getSendState()) {
            case QwpIngressProcessorState.SEND_STATE_READY -> {
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_ACK -> {
                context.resumeResponseSend();
                state.onResumeAckComplete();
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_DURABLE_ACK -> {
                context.resumeResponseSend();
                state.onResumeDurableAckComplete();
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_ERROR -> {
                context.resumeResponseSend();
                state.onResumeErrorComplete();
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_ACK_THEN_ERROR -> {
                context.resumeResponseSend();
                state.onResumeAckComplete();
                sendDeferredErrorResponse(context, state);
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_DURABLE_ACK_THEN_ERROR -> {
                context.resumeResponseSend();
                state.onResumeDurableAckComplete();
                sendDeferredErrorResponse(context, state);
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_ACK_THEN_CLOSE,
                 QwpIngressProcessorState.SEND_STATE_RESUME_DURABLE_ACK_THEN_CLOSE,
                 QwpIngressProcessorState.SEND_STATE_RESUME_DRAIN_THEN_CLOSE,
                 QwpIngressProcessorState.SEND_STATE_RESUME_CLOSE -> // The peer is voluntarily closing, but we have a fatal CLOSE
                // queued. The pending response will be torn down anyway, so
                // there is no value in attempting to flush the deferred CLOSE
                // frame on top of an in-flight ACK. Let the caller proceed.
                    LOG.debug().$("Pending fatal close superseded by peer close [fd=").$(context.getFd())
                            .$(", state=").$(state.getSendState()).I$();
            case QwpIngressProcessorState.SEND_STATE_RESUME_PONG -> {
                context.resumeResponseSend();
                state.onResumePongComplete();
            }
            default -> {
                LOG.critical().$("Invalid WebSocket send state during close [fd=").$(context.getFd())
                        .$(", state=").$(state.getSendState()).I$();
                throw PeerDisconnectedException.INSTANCE;
            }
        }
    }

    /**
     * Resume-path completion of a deferred fatal CLOSE: flushes pending
     * cumulative/durable ack progress first, then emits the CLOSE frame — the
     * same ordering {@link #sendFatalClose} guarantees on the happy path.
     * <p>
     * This ordering carries the role-change close deferral's invariant
     * ({@link #roleChangeCloseWithUploadGrace}): the final durable ack must
     * precede the CLOSE frame, because a durable-ack store-and-forward client
     * advances its replay/trim watermark only on STATUS_DURABLE_ACK frames.
     * Emitting the CLOSE without it leaves the watermark stale, and on
     * reconnect the client replays batches this server (or the promoted
     * replica, via replication) already owns — duplicates on tables without
     * DEDUP UPSERT KEYS, precisely under send backpressure at demote time.
     * The pre-fix resume branches called {@link #sendDeferredFatalClose}
     * directly, so any CLOSE that was ever deferred behind a blocked send
     * skipped the final durable ack entirely.
     * <p>
     * If the flush blocks again, the CLOSE is re-parked behind the newly
     * blocked ack frame and the dispatcher resumes us; every resume drains
     * one parked frame, so the sequence terminates.
     */
    private void finishDeferredFatalClose(HttpConnectionContext context, QwpIngressProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        try {
            flushPendingAck(context, state);
        } catch (PeerIsSlowToReadException e) {
            state.reArmDeferredFatalClose();
            LOG.debug().$("Pre-close ack flush blocked, re-deferring fatal CLOSE [fd=").$(context.getFd()).I$();
            throw e;
        }
        sendDeferredFatalClose(context, state);
    }

    private void flushPendingAck(HttpConnectionContext context, QwpIngressProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (state.hasPendingAck()) {
            trySendAck(context, state);
        }
        if (state.isDurableAckEnabled() && state.isSendReady()) {
            trySendDurableAck(context, state);
        }
    }

    /**
     * Half-closes the write side of the socket so the kernel emits FIN instead
     * of an abortive RST, then signals the framework to tear the connection
     * down. shutdown(WR) is best-effort: even if it fails (e.g. the peer is
     * already gone) we still raise ServerDisconnectException so the framework
     * proceeds with cleanup.
     */
    private void gracefulCloseAndDisconnect(HttpConnectionContext context)
            throws ServerDisconnectException {
        try {
            Socket socket = context.getSocket();
            if (socket != null) {
                socket.shutdown(Net.SHUT_WR);
                context.drainRecvBuffer();
            }
        } catch (Throwable ignored) {
        }
        throw ServerDisconnectException.INSTANCE;
    }

    private void handleBinaryMessage(HttpConnectionContext context, QwpIngressProcessorState state, long payload, int length)
            throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        long seq = state.nextMessageSequence();
        LOG.debug().$("WebSocket binary message [fd=").$(context.getFd())
                .$(", len=").$(length)
                .$(", seq=").$(seq).I$();

        // INVARIANT B enforcement: while a role-change close deferral is armed,
        // the connection exists ONLY to deliver the final durable ack before
        // the CLOSE frame. Data frames arriving in this window must not touch
        // the engine: the demote can revert within the grace period (in-place
        // re-promote), and a frame that slipped past the live read-only gate
        // would commit and advance the cumulative-ack watermark PAST the
        // silently refused frame that armed the deferral -- the client would
        // trim that frame's store-and-forward slot and its rows would be lost.
        // Treat every data frame in this window exactly like the refused frame
        // that armed the deferral: consume its sequence (the client replays it
        // after the reconnect-eligible close), record it as unresolved for the
        // ack clamp, and re-poll the deferral for coverage/expiry.
        if (state.isRoleChangeCloseDeferred()) {
            LOG.debug().$("WebSocket data frame refused, role-change close deferral armed [fd=").$(context.getFd())
                    .$(", seq=").$(seq).I$();
            state.markSequenceUnresolved(seq);
            roleChangeCloseWithUploadGrace(context, state, state.getRoleChangeCloseReason());
            return;
        }

        // A prior error broke the ordered pipeline: committing a later pipelined
        // frame would advance committed data past the gap the acked watermark
        // stopped at. Consume the sequence without touching the engine; the
        // client replays it from its acked watermark on a fresh connection.
        if (state.hasUnresolvedSequence()) {
            LOG.debug().$("WebSocket frame refused, connection pipeline broken by a prior error [fd=").$(context.getFd())
                    .$(", seq=").$(seq).I$();
            state.markSequenceUnresolved(seq);
            return;
        }

        if (!state.isOk()) {
            LOG.debug().$("WebSocket ignoring message, state is in error [fd=").$(context.getFd()).I$();
            sendErrorResponse(context, state, seq, STATUS_INTERNAL_ERROR, "Previous message failed");
            return;
        }

        byte responseStatus = STATUS_OK;
        String errorMessage = null;
        boolean roleChangeClose = false;

        boolean deferCommit = false;
        boolean closesDeferredGroup = false;
        try {
            // Add the binary data to the state buffer
            state.addData(payload, payload + length);

            deferCommit = state.isDeferCommit();

            // Process the QWP v1 message
            state.processMessage();

            if (state.isOk() && !deferCommit) {
                // Capture BEFORE commit(): a successful commitAll() clears the
                // uncommitted-deferred-rows flag, and this frame's ack must then
                // flush eagerly -- the group's deferred frames were never
                // individually acked, so the client's store-and-forward slots
                // (and, in durable-ack mode, seqTxn tracking) all hinge on the
                // ack that covers this group-closing sequence.
                closesDeferredGroup = state.hasUncommittedDeferredRows();
                state.commit();
            }
            if (state.isOk() && deferCommit) {
                state.commitIfMaxUncommittedRowsReached();
                if (state.isOk()) {
                    // Rows are buffered in WAL writers but NOT committed (the
                    // force-commit above fires per-table at the
                    // max-uncommitted-rows cap and gives no full-coverage
                    // guarantee). Until the group-closing commit or a rollback,
                    // the cumulative-ack watermark must not move past this
                    // frame -- an OK ack would let the client trim rows the
                    // server can still roll back (#7144's replay contract).
                    state.markUncommittedDeferredRows();
                }
            }
            // Read AFTER the commit calls: processMessage's read-only gate AND the
            // commit path's authorization-refusal containment (rejectCairoError)
            // can both flag the role-change close; reading before commit() would
            // miss the latter and send a client-visible error status instead of
            // the graceful reconnect-eligible close.
            roleChangeClose = state.isRoleChangeClosePending();
            // commit() swallows exceptions internally
            if (state.isOk()) {
                if (deferCommit) {
                    LOG.debug().$("WebSocket deferred commit [fd=").$(context.getFd())
                            .$(", seq=").$(seq).I$();
                } else {
                    LOG.debug().$("WebSocket message committed [fd=").$(context.getFd())
                            .$(", seq=").$(seq).I$();
                }
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
            if (deferCommit && state.isOk()) {
                // Preserve WAL state for the next message in the deferred batch
                state.clearMessageState();
            } else {
                // Reset state for next message (but preserve connectionSymbolDict for delta encoding)
                state.clear();
            }
        }

        // INVARIANT B: an in-place PRIMARY->REPLICA demote is TRANSIENT. Close the
        // connection with a reconnect-eligible code instead of sending a
        // SECURITY_ERROR that a store-and-forward client treats as a terminal HALT.
        // For durable-ack connections the close is deferred (bounded) until the
        // durable-upload registry covers this connection's committed work, so the
        // final durable ack is delivered BEFORE the CLOSE frame and the client's
        // replay window is empty -- see roleChangeCloseWithUploadGrace.
        if (roleChangeClose) {
            // No error response goes out for this frame -- the refusal is
            // transient and the client replays from its acked watermark after
            // the reconnect-eligible close. Until that close, no cumulative
            // OK ack may cover this sequence.
            state.markSequenceUnresolved(seq);
            roleChangeCloseWithUploadGrace(context, state, errorMessage);
            return;
        }

        // Send response using cumulative ACK strategy
        if (responseStatus == STATUS_OK) {
            if (deferCommit) {
                // Deferred frame: rows appended but uncommitted. NO watermark
                // advance and NO ack -- a cumulative OK ack at this sequence
                // would let the store-and-forward client trim slots whose rows
                // the server rolls back on any error, demote, or disconnect.
                // Coverage for this frame arrives with the ack of the
                // group-closing commit frame (cumulative semantics), which also
                // carries the group's real per-table seqTxns for durable-ack
                // tracking. Until then the frame stays replayable client-side,
                // exactly as #7144's error-handling contract requires.
                LOG.debug().$("WebSocket deferred frame ack withheld until group commit [fd=").$(context.getFd())
                        .$(", seq=").$(seq).I$();
            } else {
                // Success - update tracking, send ACK if batch size reached.
                // A group-closing commit flushes eagerly (hasPendingAck) instead
                // of waiting for the batch threshold: the deferred frames it
                // covers were never individually acked, and the client's
                // transaction confirmation should not wait for unrelated
                // follow-up traffic.
                state.setHighestProcessedSequence(seq);
                if (closesDeferredGroup ? state.hasPendingAck() : state.shouldSendAck(ACK_BATCH_SIZE)) {
                    trySendAck(context, state);
                }
            }
        } else {
            // Before any flush that may defer, so a blocked error frame still
            // clamps the watermark and refuses the pipelined tail.
            state.markSequenceUnresolved(seq);
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

    private void handleClose(HttpConnectionContext context, QwpIngressProcessorState state, long payload, int length)
            throws PeerIsSlowToReadException {
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
                try {
                    rawSocket.send(written);
                } catch (PeerIsSlowToReadException e) {
                    // CLOSE frame was partially written under a small send
                    // fragmentation cap. The framework holds the residual
                    // bytes; resumeSend's SEND_STATE_RESUME_CLOSE branch
                    // finishes the flush and gracefulCloseAndDisconnect.
                    // Swallowing PISR here -- as the original code did --
                    // tears the connection down before the rest of the
                    // CLOSE frame leaves the box, so the client sees EOF
                    // mid-frame instead of the close code we promised.
                    state.onFatalCloseSendBlocked();
                    throw e;
                }
            }
        } catch (PeerDisconnectedException e) {
            // Peer is gone, nothing more to do.
        }
    }

    private void handlePing(HttpConnectionContext context, QwpIngressProcessorState state, long payload, int length)
            throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        // PING is a documented flush point for pending ACK/durable-ACK frames.
        // A client may send PING specifically to prod the server into emitting
        // acks for commits whose uploads have completed since the last message.
        // flushPendingAck either drains everything or transitions the send
        // state machine to RESUME_ACK and rethrows PISR; the latter must
        // propagate so the framework parks the connection for write. Without
        // that, the parked ACK bytes would sit unsent in the response sink
        // until the next unrelated write.
        flushPendingAck(context, state);

        // A deferred role-change close completes here: the client's durable-ack
        // keepalive PING is the recv-driven poll that observes upload completion
        // (durable acks are only ever flushed on inbound events). The flush above
        // already delivered any newly-covered durable ack; once coverage is full
        // (or the grace budget is exhausted) the close is routed through
        // roleChangeCloseWithUploadGrace -- the same exit the gate-refused
        // data-frame re-entry takes -- so close behaviour and diagnostics
        // cannot drift between the two polls (a grace-expired close observed
        // by PING used to proceed silently, skipping the un-acked-durable-work
        // alarm). While the deferral holds, fall through to the pong keepalive.
        if (state.isRoleChangeCloseDeferred() && isRoleChangeCloseCompletable(state)) {
            roleChangeCloseWithUploadGrace(context, state, state.getRoleChangeCloseReason());
            return;
        }

        // Can only send pong when the response sink is clear. If a prior ACK
        // is still draining we skip the pong rather than interleave bytes;
        // the client either retries the ping or relies on the next ACK send
        // cycle to flush.
        if (!state.isSendReady()) {
            LOG.debug().$("Skipping pong, buffer busy [fd=").$(context.getFd()).I$();
            return;
        }

        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufferAddr = rawSocket.getBufferAddress();
        int bufferSize = rawSocket.getBufferSize();

        int frameSize = WebSocketFrameWriter.headerSize(length, false) + length;
        if (frameSize > bufferSize) {
            // Pong larger than the response sink buffer: drop quietly, same
            // as the previous behaviour. PING payloads are capped at 125
            // bytes by the RFC, so a real client cannot trigger this.
            LOG.error().$("Pong frame exceeds response buffer [fd=").$(context.getFd())
                    .$(", frameSize=").$(frameSize)
                    .$(", bufferSize=").$(bufferSize).I$();
            return;
        }
        int written = WebSocketFrameWriter.writePongFrame(bufferAddr, payload, length);
        try {
            rawSocket.send(written);
            LOG.debug().$("WebSocket pong sent [fd=").$(context.getFd()).I$();
        } catch (PeerIsSlowToReadException e) {
            // The send-fragmentation path can park mid-write when the chunk
            // cap is smaller than the pong frame. Transition into
            // RESUME_PONG and let the exception propagate so the framework
            // schedules a write and resumeSend can drain the residual bytes
            // via context.resumeResponseSend(). Swallowing the exception
            // here would leak the parked tail and the client would never
            // see the pong.
            state.onPongBlocked();
            LOG.debug().$("Pong send blocked, deferring to resume [fd=").$(context.getFd()).I$();
            throw e;
        }
    }

    /**
     * Completion predicate for a deferred role-change close: the registry's
     * durable-upload watermark covers every committed seqTxn (replay window
     * empty), or the bounded grace budget is exhausted (availability over the
     * duplicate guard). Single source of truth shared by the deferral's two
     * re-entry polls -- gate-refused data frames and keepalive PINGs
     * ({@link #handlePing}) -- so the close path and its diagnostics cannot
     * drift between them.
     */
    private boolean isRoleChangeCloseCompletable(QwpIngressProcessorState state) {
        return state.isDurableWorkFullyUploaded(engine.getDurableAckRegistry())
                || state.isRoleChangeCloseGraceExpired();
    }

    /**
     * INVARIANT B role-change close with an exactly-once guard for durable-ack
     * connections.
     * <p>
     * The demote cascade flips the engine read-only FIRST and completes pending
     * WAL uploads AFTERWARDS, so at the instant the read-only gate rejects a
     * frame the durable-ack watermark can lag this connection's committed work
     * by the in-flight upload latency. Closing inside that lag loses the final
     * durable ack forever -- durable acks are recv-driven, so there is no
     * delivery opportunity after the CLOSE frame -- while the demote drain
     * still publishes those commits to the object store. A store-and-forward
     * client (whose replay watermark advances ONLY on durable acks) would then
     * replay a batch the promoted replica already converged to via replication,
     * landing it twice on tables without DEDUP UPSERT KEYS.
     * <p>
     * So: while committed work remains un-uploaded, DEFER the close (bounded by
     * {@link QwpIngressProcessorState#ROLE_CHANGE_CLOSE_UPLOAD_GRACE_MICROS})
     * and keep flushing ack progress. Re-entry points during the deferral are
     * further data frames (refused by the deferral gate at the top of
     * {@code handleBinaryMessage} BEFORE they can touch the engine -- the live
     * read-only gate alone is not sufficient, because an in-place re-promote
     * within the grace window would let a frame commit and advance the
     * cumulative ack past the silently refused frame that armed the deferral)
     * and the client's durable-ack keepalive PINGs ({@link #handlePing}). Once the registry
     * covers the connection's pending seqTxns, sendFatalClose flushes the final
     * durable ack and only then emits NORMAL_CLOSURE: the replay window is
     * empty and every in-flight batch lands exactly once. If uploads stall past
     * the grace budget the close proceeds anyway -- availability over the
     * duplicate guard, matching the pre-deferral behaviour.
     * <p>
     * Non-durable-ack connections close immediately: their cumulative OK ack is
     * flushed synchronously by sendFatalClose and carries no upload lag.
     */
    private void roleChangeCloseWithUploadGrace(
            HttpConnectionContext context,
            QwpIngressProcessorState state,
            CharSequence reason
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        if (state.isDurableAckEnabled() && !isRoleChangeCloseCompletable(state)) {
            boolean firstDeferral = !state.isRoleChangeCloseDeferred();
            state.deferRoleChangeClose(reason);
            if (firstDeferral) {
                LOG.info().$("deferring role-change close until committed work is durably uploaded [fd=")
                        .$(context.getFd()).I$();
            }
            // Push whatever cumulative/durable progress exists right now; the
            // final durable ack goes out with the close itself once coverage
            // is confirmed.
            flushPendingAck(context, state);
            return;
        }
        if (state.isRoleChangeCloseGraceExpired()
                && !state.isDurableWorkFullyUploaded(engine.getDurableAckRegistry())) {
            // Grace expired with genuinely un-acked durable work: the one
            // close the operator must see. A slow-but-clean close -- uploads
            // catching up after the deadline but before this re-entry --
            // leaves an empty replay window and must not raise this alarm.
            LOG.error().$("role-change close upload grace expired; closing with un-acked durable work, client replay may duplicate [fd=")
                    .$(context.getFd()).I$();
        }
        sendFatalClose(
                context,
                state,
                WebSocketCloseCode.NORMAL_CLOSURE,
                state.isRoleChangeCloseDeferred() ? state.getRoleChangeCloseReason() : reason
        );
    }

    private void handleWebSocketFrame(HttpConnectionContext context, QwpIngressProcessorState state, int opcode, boolean fin, long payload, int length)
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
        int clientMaxVersion = QwpConstants.VERSION; // default if header absent
        Utf8Sequence maxVersionHeader = requestHeader.getHeader(QwpIngressHttpProcessor.HEADER_X_QWP_MAX_VERSION);
        if (maxVersionHeader != null) {
            int parsed = Numbers.parseNonNegativeIntQuiet(maxVersionHeader);
            if (parsed >= QwpConstants.VERSION) {
                clientMaxVersion = parsed;
            }
        }

        int negotiated = Math.min(clientMaxVersion, QwpConstants.VERSION);

        Utf8Sequence clientId = requestHeader.getHeader(QwpIngressHttpProcessor.HEADER_X_QWP_CLIENT_ID);
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

    private void processWebSocketFrames(HttpConnectionContext context, QwpIngressProcessorState state, long buffer, int bufferLen)
            throws ServerDisconnectException, PeerDisconnectedException, PeerIsSlowToReadException {
        long bufferEnd = buffer + bufferLen;
        long pos = buffer;

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
                        sendFatalClose(context, state,
                                WebSocketCloseCode.MESSAGE_TOO_BIG,
                                "frame payload exceeds maximum size");
                        return; // unreachable — sendFatalClose throws.
                    }
                    break;
                }

                if (consumed == 0 || frameParser.getState() == WebSocketFrameParser.STATE_NEED_MORE) {
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

            // Flush any pending cumulative ACK whether or not the buffer ends
            // on a clean frame boundary. The previous `COMPLETE`-only check
            // starved senders of ACKs whenever a recv landed mid-frame —
            // including any time the OS recv chunk was smaller than a full
            // QWP batch — leaving the client's drainOnClose to time out.
            // flushPendingAck is a no-op when nothing is pending, so this
            // is safe in the empty-buffer / partial-frame-only cases too.
            flushPendingAck(context, state);
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

    private void rejectFragmentedFrame(HttpConnectionContext context, QwpIngressProcessorState state, int opcode)
            throws PeerIsSlowToReadException, ServerDisconnectException {
        LOG.error()
                .$("WebSocket fragmented frame rejected, QWP requires unfragmented messages [fd=").$(context.getFd())
                .$(", opcode=").$(WebSocketOpcode.name(opcode))
                .$("] a WebSocket intermediary (proxy, load balancer) may be fragmenting frames; ")
                .$("configure it to pass WebSocket frames through without fragmentation, ")
                .$("or connect the QWP client directly to QuestDB")
                .I$();

        sendFatalClose(context, state,
                WebSocketCloseCode.PROTOCOL_ERROR,
                "fragmented WebSocket frames are not supported");
    }

    private void rejectTextFrame(HttpConnectionContext context, QwpIngressProcessorState state)
            throws PeerIsSlowToReadException, ServerDisconnectException {
        LOG.error()
                .$("WebSocket text frame rejected, QWP accepts only binary frames [fd=").$(context.getFd())
                .I$();

        sendFatalClose(context, state,
                WebSocketCloseCode.UNSUPPORTED_DATA,
                "text frames are not supported, QWP requires binary frames");
    }

    /**
     * Sends the deferred error response stored in the processor state.
     * <p>
     * Used after a blocked ACK resumes and the original failure response must
     * be delivered before any later ACK activity can overtake it.
     */
    private void sendDeferredErrorResponse(HttpConnectionContext context, QwpIngressProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        sendErrorResponse(
                context,
                state,
                state.getDeferredErrorSequence(),
                state.getDeferredErrorStatus(),
                state.getDeferredErrorMessage()
        );
    }

    /**
     * Resume-path emission of a previously-deferred fatal CLOSE frame. Caller
     * has already drained the in-flight response that was blocking the send.
     * On success, half-closes the write side and raises ServerDisconnect. On
     * partial flush of the CLOSE frame itself, transitions to RESUME_CLOSE so
     * the next dispatcher tick finishes the flush.
     */
    private void sendDeferredFatalClose(HttpConnectionContext context, QwpIngressProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        assert state.isSendReady() : "sendDeferredFatalClose called in wrong state";

        int closeCode = state.getDeferredCloseCode();
        CharSequence reason = state.getDeferredCloseReason();
        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufferAddr = rawSocket.getBufferAddress();
        int bufferSize = rawSocket.getBufferSize();

        int written = WebSocketFrameWriter.writeCloseFrame(bufferAddr, bufferSize, closeCode, reason);
        if (written <= 0) {
            // CLOSE frame did not fit the send buffer — abandon the protocol close.
            throw ServerDisconnectException.INSTANCE;
        }

        try {
            rawSocket.send(written);
        } catch (PeerIsSlowToReadException e) {
            // Bytes are queued in the framework buffer; the resume path
            // will finish flushing and disconnect.
            state.onFatalCloseSendBlocked();
            LOG.debug().$("Fatal CLOSE send blocked, deferring to resume [fd=").$(context.getFd()).I$();
            throw e;
        }

        gracefulCloseAndDisconnect(context);
    }

    private void sendErrorResponse(
            HttpConnectionContext context,
            QwpIngressProcessorState state,
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
     * Emits a fatal WebSocket CLOSE frame with the given protocol-level close
     * code and disconnects. Routes through the send state machine so the CLOSE
     * lands even when an ACK/durable-ACK is mid-flight:
     * <ul>
     *   <li>State READY, send succeeds → half-close (FIN) + ServerDisconnect.</li>
     *   <li>State READY, send returns PeerIsSlow → bytes queued in framework
     *       buffer, transitions to RESUME_CLOSE, throws PeerIsSlow.</li>
     *   <li>State not READY → stores (code, reason), transitions to
     *       *_THEN_CLOSE, throws PeerIsSlow.</li>
     *   <li>Peer already gone → ServerDisconnect.</li>
     * </ul>
     */
    private void sendFatalClose(
            HttpConnectionContext context,
            QwpIngressProcessorState state,
            int closeCode,
            CharSequence reason
    ) throws PeerIsSlowToReadException, ServerDisconnectException {
        // Give the client one more chance to learn about already-committed
        // sequences before tearing the connection down. flushPendingAck is a
        // no-op when state is not READY (ACK already in flight), so it does
        // not interfere with the deferred path below.
        try {
            flushPendingAck(context, state);
        } catch (PeerDisconnectedException pde) {
            throw ServerDisconnectException.INSTANCE;
        } catch (PeerIsSlowToReadException slow) {
            // ACK just transitioned into RESUME_ACK during flush — defer the
            // CLOSE and surface the backpressure so the dispatcher resumes us.
            state.onFatalCloseBlocked(closeCode, reason);
            throw slow;
        }

        if (!state.isSendReady()) {
            // Some other in-flight response is still blocking. Queue the CLOSE
            // for the resume path.
            state.onFatalCloseBlocked(closeCode, reason);
            throw PeerIsSlowToReadException.INSTANCE;
        }

        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufferAddr = rawSocket.getBufferAddress();
        int bufferSize = rawSocket.getBufferSize();

        int written = WebSocketFrameWriter.writeCloseFrame(bufferAddr, bufferSize, closeCode, reason);
        if (written <= 0) {
            throw ServerDisconnectException.INSTANCE;
        }

        try {
            rawSocket.send(written);
        } catch (PeerDisconnectedException pde) {
            throw ServerDisconnectException.INSTANCE;
        } catch (PeerIsSlowToReadException slow) {
            state.onFatalCloseSendBlocked();
            LOG.debug().$("Fatal CLOSE send blocked, deferring to resume [fd=").$(context.getFd()).I$();
            throw slow;
        }

        gracefulCloseAndDisconnect(context);
    }

    /**
     * Attempts to send a cumulative ACK for the highest processed sequence.
     * <p>
     * State transitions (managed by {@link QwpIngressProcessorState}):
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
    private void trySendAck(HttpConnectionContext context, QwpIngressProcessorState state)
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
        QwpIngressProcessorState.writeTableSeqTxnEntries(writeAddr + 9, state.getPendingAckSeqTxns());

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

    private void trySendDurableAck(HttpConnectionContext context, QwpIngressProcessorState state)
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
        QwpIngressProcessorState.writeTableSeqTxnEntries(writeAddr + 1, progress);

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

    // Per-connection holder for the byte count of a 4xx upgrade rejection
    // deferred from onHeadersReady to onRequestComplete. Lazily allocated;
    // only connections that actually trigger a reject pay the single-object
    // cost.
    //
    // Implements Mutable so LocalValueMap.clear() (invoked by
    // HttpConnectionContext.reset() on every request boundary AND
    // HttpConnectionContext.clear() on pool-return via super.clear()) resets
    // pendingBytes to 0. Without this, a PeerDisconnectedException thrown by
    // the staged send in onRequestComplete (which skips the
    // pendingBytes = 0 reset) would leave a stale value on the context; the
    // next pool reuse of that context would land a legitimate upgrade on a
    // tracker whose pendingBytes > 0 still drives the reject branch and
    // throws HttpException instead of finalising the 101 handshake.
    private static final class RejectFlushTracker implements Mutable {
        int pendingBytes;

        @Override
        public void clear() {
            pendingBytes = 0;
        }
    }

}
