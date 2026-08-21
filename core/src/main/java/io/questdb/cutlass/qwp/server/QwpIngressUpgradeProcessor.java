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
import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpException;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRawSocket;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpMessageHeader;
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
    /**
     * Per-dispatch read budget for the close-echo discard loop
     * ({@link #discardInboundBytes}). On exhaustion the drain yields the worker
     * rather than spinning: leftover readiness re-fires the dispatcher, so the
     * drain resumes next dispatch with a fresh budget and the echo grace
     * deadline is re-polled at every re-entry.
     */
    public static final int CLOSE_ECHO_DISCARD_READ_BUDGET = 8;
    /**
     * Per-dispatch byte budget for the close-echo discard loop. The read count
     * alone scales with the configured recv buffer (8 reads x 2 MiB default =
     * 16 MiB per worker turn); capping each recv at
     * {@code min(recvBufferSize, remainingByteBudget)} makes the per-dispatch
     * cost configuration-independent.
     */
    public static final int CLOSE_ECHO_DISCARD_BYTE_BUDGET = 256 * 1024;
    /**
     * Maximum bytes one close-echo worker turn may read, and the sole cap on
     * that read. The echo is queued behind whatever the client pipelined ahead
     * of it, so this trades echo latency against worker fairness at the same
     * per-dispatch cost as the sibling drains. {@link #resumeRecv} explains why
     * the bound is enforced on the receive and not the parse loop.
     */
    public static final int CLOSE_ECHO_FRAME_BYTE_BUDGET = 256 * 1024;
    /**
     * Read budget for the best-effort inbound drain before a graceful teardown
     * ({@link #gracefulCloseAndDisconnect}). The drain lowers the chance of an
     * abortive close (RST destroys the peer's unread tail) but is best-effort:
     * a still-streaming peer must not pin the worker on a path whose purpose is
     * prompt teardown.
     */
    public static final int GRACEFUL_CLOSE_DRAIN_READ_BUDGET = 8;
    /**
     * Byte companion to {@link #GRACEFUL_CLOSE_DRAIN_READ_BUDGET}; each recv is
     * capped at {@code min(recvBufferSize, remainingBudget)} so the drain cost
     * does not scale with the configured recv buffer.
     */
    public static final int GRACEFUL_CLOSE_DRAIN_BYTE_BUDGET = 256 * 1024;
    // Cumulative ACK batch size
    private static final int ACK_BATCH_SIZE = 8;
    // Per-dispatch BYTE budget for the post-CLOSE drain in resumeRecv. The recv-
    // count guard also bounds syscalls, but its byte cost scales with the configured
    // HTTP recv buffer. Capping each recv to the remainder limits a worker turn to
    // 256 KiB regardless of configuration.
    private static final int CLOSE_DRAIN_BYTE_BUDGET = 256 * 1024;
    // Upper bound on socket.recv() calls per resumeRecv dispatch while draining a
    // post-CLOSE connection. The count guard complements CLOSE_DRAIN_BYTE_BUDGET
    // by bounding syscall work when a peer supplies tiny fragments. On hitting
    // either cap we yield via PeerIsSlowToWriteException so the worker can service
    // other connections before the dispatcher resumes the drain.
    private static final int CLOSE_DRAIN_MAX_RECV_PER_DISPATCH = 32;
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
    private final int effectiveMaxBatchSize;
    private final byte[] effectiveMaxBatchSizeBytes;
    private final CairoEngine engine;
    // Frames handleWebSocketFrame's discard gate dropped during the current
    // processWebSocketFrames call. Scratch, exactly like frameParser: the
    // parse loop resets it on entry and logs the total once on the way out,
    // instead of paying a LOG.debug() call per discarded frame -- one capped
    // echo-wait read admits up to CLOSE_ECHO_FRAME_BYTE_BUDGET / 6 = 43_690
    // of them, enough to swamp the log ring of an operator who enabled DEBUG
    // to watch a demote.
    private int closeEchoDiscardedFrames;
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
        this.effectiveMaxBatchSize = Math.min(
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
        boolean durableAckHeaderRequested = durableAckHeader != null
                && Utf8s.equalsIgnoreCaseAscii(durableAckHeader, QwpIngressHttpProcessor.HEADER_VALUE_DURABLE_ACK_ENABLED);
        boolean durableAckWebSocketProtocolRequested = QwpIngressHttpProcessor.containsWebSocketProtocol(
                requestHeader.getHeader(QwpIngressHttpProcessor.HEADER_SEC_WEBSOCKET_PROTOCOL),
                QwpIngressHttpProcessor.WEBSOCKET_PROTOCOL_QWP_DURABLE_ACK);
        boolean durableAckRequested = durableAckHeaderRequested || durableAckWebSocketProtocolRequested;
        boolean durableAckEnabled = durableAckRequested && engine.getDurableAckRegistry().isEnabled();
        boolean durableAckWebSocketProtocolEnabled = durableAckEnabled && durableAckWebSocketProtocolRequested;
        Utf8Sequence browserHandshake = requestHeader.getUrlParam(
                QwpIngressHttpProcessor.URL_PARAM_QWP_BROWSER_HANDSHAKE);
        boolean browserHandshakeRequested = effectiveMaxBatchSize > 0
                && browserHandshake != null
                && Utf8s.equalsAscii("v1", browserHandshake);
        byte[] sessionCookieValueBytes = QwpIngressHttpProcessor.getSessionCookieValueBytes(context);

        int requiredHandshakeSize = QwpIngressHttpProcessor.responseSize(
                acceptKey, negotiatedVersion, null, durableAckEnabled, roleBytes,
                effectiveMaxBatchSizeBytes, sessionCookieValueBytes, durableAckWebSocketProtocolEnabled);
        if (browserHandshakeRequested) {
            requiredHandshakeSize += WebSocketFrameWriter.headerSize(5, false) + 5;
        }
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
                effectiveMaxBatchSizeBytes, sessionCookieValueBytes, durableAckWebSocketProtocolEnabled);
        if (bytesWritten <= 0) {
            throw responseDoesNotFitSendBuffer(context.getFd(), "101 handshake response", bufferSize, requiredHandshakeSize);
        }
        if (browserHandshakeRequested) {
            bytesWritten += writeBrowserServerInfoFrame(
                    bufferAddr + bytesWritten,
                    effectiveMaxBatchSize
            );
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

    /**
     * Writes the browser-only ingress SERVER_INFO WebSocket frame.
     */
    public static int writeBrowserServerInfoFrame(long bufferAddress, int maxBatchSizeBytes) {
        int headerSize = WebSocketFrameWriter.writeBinaryFrameHeader(bufferAddress, 5);
        long payloadAddress = bufferAddress + headerSize;
        Unsafe.putByte(payloadAddress, QwpConstants.STATUS_SERVER_INFO);
        Unsafe.putInt(payloadAddress + 1, maxBatchSizeBytes);
        return headerSize + 5;
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

    @Override
    public boolean processServiceAccountCookie(HttpConnectionContext context, SecurityContext securityContext) {
        return context.getCookieHandler().processServiceAccountCookie(context, securityContext);
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

        // Post-CLOSE read-drain (see gracefulCloseAndDrain): the fatal CLOSE and
        // FIN are out; inbound bytes are frames the client pipelined before it
        // observed them. Consume and discard so the fd close cannot race those
        // in-flight bytes into an RST that destroys the final ACK/durable-ACK +
        // CLOSE still queued unread in the peer's receive buffer. Exit on the
        // peer's close (FIN/RST -- the peer provably consumed or abandoned the
        // goodbye) or on the bounded drain deadline; a fully silent peer is
        // reaped by the transport idle timeout.
        if (state.isCloseDraining()) {
            if (state.isCloseDrainExpired()) {
                LOG.info().$("close drain deadline expired, disconnecting [fd=").$(context.getFd()).I$();
                throw ServerDisconnectException.INSTANCE;
            }
            try {
                int bytesDrained = 0;
                int drained;
                int recvCount = 0;
                while (true) {
                    int cap = Math.min(recvBufferSize, CLOSE_DRAIN_BYTE_BUDGET - bytesDrained);
                    if (cap <= 0) {
                        throw PeerIsSlowToWriteException.INSTANCE;
                    }
                    drained = socket.recv(recvBuffer, cap);
                    if (drained <= 0) {
                        break;
                    }
                    bytesDrained += drained;
                    // A continuously readable peer never leaves this loop, so
                    // poll expiry after every positive recv rather than waiting
                    // for the next dispatcher entry.
                    if (state.isCloseDrainExpired()) {
                        LOG.info().$("close drain deadline expired, disconnecting [fd=").$(context.getFd()).I$();
                        throw ServerDisconnectException.INSTANCE;
                    }
                    if (++recvCount >= CLOSE_DRAIN_MAX_RECV_PER_DISPATCH) {
                        // Per-dispatch syscall quantum exhausted while the socket
                        // remains readable. PISW re-arms the fd for read and lets
                        // the worker service other connections first.
                        throw PeerIsSlowToWriteException.INSTANCE;
                    }
                }
                if (drained < 0) {
                    LOG.debug().$("peer closed during close drain [fd=").$(context.getFd()).I$();
                    throw ServerDisconnectException.INSTANCE;
                }
            } catch (ServerDisconnectException | PeerIsSlowToWriteException e) {
                throw e;
            } catch (Throwable e) {
                throw ServerDisconnectException.INSTANCE;
            }
            // Would-block: keep the drain parked; the caller re-registers for read.
            return;
        }

        try {
            if (state.hasPendingCloseEchoHalfClose() && state.isAwaitingCloseEcho()) {
                // The wait armed while the TLS socket still held ciphertext of
                // the CLOSE record, so FIN would have truncated it. The
                // dispatcher runs tlsIO on a socket it found writable before
                // publishing this operation, so retry now; halfCloseWriteSide
                // defers again if the tail is still there. Both conditions are
                // required so a stale flag cannot half-close a live connection;
                // the deferral flag is read first and is false for every ingest
                // connection, keeping the steady-state cost one field load.
                halfCloseWriteSide(context, state);
            }

            if (state.hasLostCloseEchoSync()) {
                // Frame sync died earlier in the wait (a too-big frame jammed
                // the recv machinery), so the CLOSE echo can never be parsed.
                // The connection now exists only to keep the socket drained --
                // the fd close must not RST the client's unread
                // [durable ack][CLOSE] tail -- and to poll the grace budget on
                // inbound activity. Consuming the bytes also stops the
                // edge-triggered oneshot re-arm from spinning on stale
                // readiness.
                checkCloseEchoWaitExpiry(context, state);
                discardInboundBytes(context, state);
                throw PeerIsSlowToWriteException.INSTANCE;
            }

            int recvBufferLen = state.getRecvBufferLen();
            if (recvBufferLen >= recvBufferSize) {
                if (state.isAwaitingCloseEcho()) {
                    // Recv buffer jammed mid-wait: the trailing frame can never
                    // complete, so frame sync is unrecoverable. Enter
                    // read-and-discard mode (see the gate above). This must not
                    // fall through to sendFatalClose: returning without
                    // consuming socket bytes leaves the edge-triggered oneshot
                    // re-arm re-firing immediately, busy-looping until expiry.
                    LOG.error().$("WebSocket recv buffer jammed during close echo wait, discarding inbound bytes [fd=")
                            .$(context.getFd()).I$();
                    state.onCloseEchoSyncLost();
                    state.setRecvBufferLen(0);
                    checkCloseEchoWaitExpiry(context, state);
                    discardInboundBytes(context, state);
                    throw PeerIsSlowToWriteException.INSTANCE;
                }
                // Buffer is full, but the parser still needs more data — the frame
                // payload exceeds recv buffer capacity. Notify the client with
                // a protocol-level CLOSE so it can distinguish "your frame is
                // too big" from a generic network failure.
                LOG.error().$("WebSocket frame too large for recv buffer [fd=").$(context.getFd())
                        .$(", bufferSize=").$(recvBufferSize).I$();
                sendFatalClose(context, state,
                        WebSocketCloseCode.MESSAGE_TOO_BIG,
                        "frame payload exceeds receive buffer capacity");
                return; // CLOSE sent (echo wait or drain armed) or parked for resume.
            }

            int remaining = recvBufferSize - recvBufferLen;
            int uncappedReadSize = Math.min(forceRecvFragmentationChunkSize, remaining);
            int readSize = uncappedReadSize;
            if (state.isAwaitingCloseEcho()) {
                // Bound the RECEIVE, not the parse loop that follows. The echo
                // is queued behind whatever the client pipelined ahead of it,
                // so this cap bounds how many dispatcher turns the wait spends
                // before it can see the echo, independent of the configured
                // recv buffer. Leftover readiness re-fires the dispatcher, so
                // the drain resumes next turn with a fresh budget.
                //
                // Capping the recv is also what keeps the wait re-entrant:
                // processWebSocketFrames consumes every complete frame the read
                // admitted, so the remainder is always ONE incomplete frame
                // that genuinely needs more bytes. Breaking the parse loop early
                // would instead park complete frames in user space, and both
                // exits of handleProtocolSwitchedRecv end in an edge-triggered
                // oneshot registration -- a peer that goes quiet after
                // pipelining would never have the echo parsed.
                readSize = Math.min(readSize, CLOSE_ECHO_FRAME_BYTE_BUDGET);
            }
            int read = socket.recv(recvBuffer + recvBufferLen, readSize);
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

            if (read == readSize && (readSize < uncappedReadSize || read == forceRecvFragmentationChunkSize)) {
                // The close-echo fairness cap or forced fragmentation cap may
                // have left bytes in the kernel. Re-arm READ so another worker
                // turn can continue after other connections receive service.
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

        boolean wasAwaitingCloseEcho = state.isAwaitingCloseEcho();
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
                // Return the send machine to READY before arming the echo
                // wait. Every other entry into the wait leaves READY behind;
                // the central causal-boundary check below then observes the
                // transition and discards bytes buffered before this CLOSE.
                state.onResumeCloseComplete();
                LOG.debug().$("Resumed CLOSE frame sent [fd=").$(context.getFd()).I$();
                finishServerFatalClose(context, state);
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_CLOSE_RESPONSE -> {
                context.resumeResponseSend();
                LOG.debug().$("Resumed close response sent [fd=").$(context.getFd()).I$();
                // Close response to a client-initiated CLOSE: the client's
                // CLOSE is already consumed, so the RFC 6455 handshake is
                // complete the moment the response tail lands. No echo can
                // ever arrive; tear down immediately (RFC 6455 s5.5.1: the
                // server closes the TCP connection first).
                gracefulCloseAndDisconnect(context);
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_ACK_THEN_CLOSE_RESPONSE -> {
                context.resumeResponseSend();
                state.onResumeAckComplete();
                LOG.debug().$("Resumed ACK sent before client close response [fd=").$(context.getFd())
                        .$(", upTo=").$(state.getLastAckedSequence()).I$();
                finishClientCloseResponse(context, state);
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_DURABLE_ACK_THEN_CLOSE_RESPONSE -> {
                context.resumeResponseSend();
                state.onResumeDurableAckComplete();
                LOG.debug().$("Resumed durable ACK sent before client close response [fd=").$(context.getFd()).I$();
                finishClientCloseResponse(context, state);
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
        if (!wasAwaitingCloseEcho && state.isAwaitingCloseEcho()) {
            // Any buffered bytes came from the recv that initiated the send
            // continuation and therefore predate the server CLOSE. They cannot
            // contain its echo. Drop them before drainBufferedFrames to avoid
            // unbounded parsing and a false handshake completion on a
            // pre-CLOSE client frame. This covers every continuation that can
            // synchronously send CLOSE, not only RESUME_CLOSE.
            state.setRecvBufferLen(0);
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

    /**
     * Enters the RFC 6455 close handshake wait after a role-change CLOSE has
     * been fully sent, when that CLOSE carried the exactly-once contract:
     * durable-ack mode, a role-change close initiated, and a final durable ack
     * (flushed immediately before the CLOSE) covering every committed seqTxn.
     * <p>
     * Closing the fd immediately races the client's receive path: in-flight
     * client frames the server never reads make the close abortive (RST), and
     * the RST discards the client's unread [durable ack][CLOSE] tail. Its
     * replay watermark then never advances and it replays its whole corpus to
     * the promoted replica -- duplicates on tables without DEDUP UPSERT KEYS.
     * Holding the fd open until the echo (or FIN) arrives proves the client
     * consumed the stream up to our CLOSE, final durable ack included
     * (RFC 6455 s5.5.1 echoes before dispatching the close to its handler).
     * <p>
     * Grace-expired closes (un-acked durable work; the duplicate alarm has
     * already fired) and non-durable-ack connections keep the immediate
     * teardown: there is no delivery guarantee left to protect.
     * <p>
     * Arming half-closes the write side like the sibling teardowns do, except
     * behind a pending TLS write ({@link #halfCloseWriteSide}). FIN matters
     * more here than elsewhere: the grace budget
     * ({@link QwpIngressProcessorState#CLOSE_ECHO_WAIT_GRACE_MICROS}) is polled
     * only on inbound recv re-entry, so a peer that reads our CLOSE and falls
     * silent generates no further event and would pin the fd, dispatcher slot,
     * buffers and checked-out WAL writers until the transport idle reaper fires
     * -- for every connection at once at a mass demote. The half-close turns
     * that silence into an event: the peer reads EOF, closes, and its FIN
     * completes the wait on the next dispatch.
     *
     * @return true when the echo wait was entered and the caller must NOT
     * disconnect; false when the caller should proceed with the immediate
     * teardown
     */
    private boolean beginCloseEchoWaitIfEligible(HttpConnectionContext context, QwpIngressProcessorState state) {
        // Decided from local pending state, NOT a fresh registry read.
        // sendFatalClose flushed the final durable ack (T1) immediately before
        // this call, so empty pending maps mean that ack leaves no replay
        // window. Re-querying the registry would instead see a watermark the
        // concurrent demote drain may have advanced in (T1, now]: on the
        // grace-expired path that late advance would arm the wait with pending
        // work still in the maps, stranding a durable-ack frame behind our
        // CLOSE (RFC 6455 permits nothing after it) and holding a 5s wait open
        // on the path that must tear down immediately.
        if (state.isDurableAckEnabled()
                && state.isRoleChangeCloseInitiated()
                && !state.hasPendingDurableWork()) {
            // Half-close behind the CLOSE frame (see javadoc). RFC 6455 s5.5.1
            // permits no frame after our CLOSE, so nothing is lost by giving up
            // the write side, and the FIN prompts a conformant peer to close
            // promptly. Only an RST discards a receive queue, so the peer still
            // parses the buffered CLOSE before it can observe EOF.
            halfCloseWriteSide(context, state);
            state.beginCloseEchoWait();
            LOG.info().$("role-change CLOSE sent, awaiting client close echo [fd=").$(context.getFd()).I$();
            return true;
        }
        return false;
    }

    /**
     * Poll point for the close-echo wait: tears the connection down when the
     * grace budget is exhausted (availability over the duplicate guard, the
     * same trade the upload-grace expiry makes). A conformant client echoes
     * within one round trip, so expiry means a wedged peer. The deadline is
     * polled only on inbound events, so it bounds a peer that keeps talking
     * without echoing. A peer that answers neither the CLOSE nor the FIN
     * generates no event to poll on and remains the transport reaper's to
     * collect -- the same policy the post-CLOSE drain and upload grace follow.
     */
    private void checkCloseEchoWaitExpiry(HttpConnectionContext context, QwpIngressProcessorState state)
            throws ServerDisconnectException {
        if (state.isCloseEchoWaitExpired()) {
            LOG.error().$("close echo wait expired; closing without delivery confirmation, client replay may duplicate [fd=")
                    .$(context.getFd()).I$();
            gracefulCloseAndDisconnect(context);
        }
    }

    /**
     * Read-and-discard loop for the sync-lost phase of the close-echo wait
     * ({@link QwpIngressProcessorState#hasLostCloseEchoSync}): consumes the
     * kernel-buffered bytes using the recv buffer as scratch and throws them
     * away -- they are mid-frame garbage that can never be parsed. Draining
     * keeps the eventual fd close from turning abortive (RST) and stops the
     * edge-triggered oneshot re-arm from spinning on stale readiness.
     * <p>
     * Bounded per dispatch ({@link #CLOSE_ECHO_DISCARD_READ_BUDGET}) and
     * re-polls the grace deadline after every read: a peer that keeps the
     * kernel buffer non-empty keeps {@code recv() > 0}, so an unbounded loop
     * with a loop-external poll would pin this worker AND hold the wait open
     * past its budget. On exhaustion the caller re-arms for read and progress
     * continues dispatch-by-dispatch.
     * <p>
     * A negative read is the peer's FIN or a transport error; during the wait
     * the FIN is delivery confirmation, so that teardown is the success path.
     */
    private void discardInboundBytes(HttpConnectionContext context, QwpIngressProcessorState state)
            throws ServerDisconnectException {
        Socket socket = context.getSocket();
        long recvBuffer = context.getRecvBuffer();
        int recvBufferSize = context.getRecvBufferSize();
        int read;
        int reads = 0;
        int bytesDrained = 0;
        while (true) {
            // Both guards are per-dispatch: the read-count budget bounds
            // syscalls, the byte budget bounds copy work independently of
            // the configured recv buffer size (a count-only budget permits
            // reads x bufferSize bytes -- 16 MiB per dispatch at the 2 MiB
            // default). Cap each recv at the remaining byte budget.
            int cap = Math.min(recvBufferSize, CLOSE_ECHO_DISCARD_BYTE_BUDGET - bytesDrained);
            if (cap <= 0) {
                LOG.debug().$("WebSocket close-echo discard byte budget exhausted, yielding worker [fd=")
                        .$(context.getFd()).I$();
                return;
            }
            read = socket.recv(recvBuffer, cap);
            if (read <= 0) {
                break;
            }
            bytesDrained += read;
            LOG.debug().$("WebSocket bytes discarded awaiting close echo [fd=").$(context.getFd())
                    .$(", bytes=").$(read).I$();
            // The poll must live INSIDE the loop: only here does a
            // continuously readable socket ever observe the deadline.
            checkCloseEchoWaitExpiry(context, state);
            if (++reads >= CLOSE_ECHO_DISCARD_READ_BUDGET) {
                LOG.debug().$("WebSocket close-echo discard budget exhausted, yielding worker [fd=")
                        .$(context.getFd()).I$();
                return;
            }
        }
        if (read < 0) {
            LOG.info().$("WebSocket peer disconnected during close echo wait [fd=").$(context.getFd()).I$();
            throw ServerDisconnectException.INSTANCE;
        }
    }

    private void drainBufferedFrames(HttpConnectionContext context, QwpIngressProcessorState state)
            throws ServerDisconnectException, PeerDisconnectedException, PeerIsSlowToReadException {
        // isCloseDraining: a resume-path fatal close (finishDeferredFatalClose)
        // leaves the send state READY after the CLOSE flush; buffered frames are
        // pipelined pre-CLOSE input and must be discarded by the read-drain, not
        // processed against the engine.
        if (!state.isCloseDraining() && state.isSendReady() && state.getRecvBufferLen() > 0) {
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
            case QwpIngressProcessorState.SEND_STATE_RESUME_ACK_THEN_CLOSE_RESPONSE -> {
                // Teardown while a client-close continuation is parked: drain
                // the parked ack (the client's LAST cumulative ack) so a peer
                // that is still reading gets its watermark; the close
                // response is pointless mid-teardown and is skipped.
                context.resumeResponseSend();
                state.onResumeAckComplete();
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_DURABLE_ACK_THEN_CLOSE_RESPONSE -> {
                context.resumeResponseSend();
                state.onResumeDurableAckComplete();
            }
            case QwpIngressProcessorState.SEND_STATE_RESUME_ACK_THEN_CLOSE,
                 QwpIngressProcessorState.SEND_STATE_RESUME_DURABLE_ACK_THEN_CLOSE,
                 QwpIngressProcessorState.SEND_STATE_RESUME_DRAIN_THEN_CLOSE,
                 QwpIngressProcessorState.SEND_STATE_RESUME_CLOSE,
                 QwpIngressProcessorState.SEND_STATE_RESUME_CLOSE_RESPONSE -> // The peer is voluntarily closing, but we have a fatal CLOSE
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

    /**
     * Resume-path completion of a client-initiated CLOSE whose pre-response ack
     * flush blocked: flushes remaining ack progress, emits the close response
     * with the code parked by
     * {@link QwpIngressProcessorState#onClientCloseBlockedBehindAck(int)}, then
     * disconnects -- the same ack-before-close ordering {@link #handleClose}
     * guarantees on the happy path. If the flush blocks again the continuation
     * is re-parked behind the newly blocked frame; every resume drains one
     * parked frame, so the sequence terminates.
     */
    private void finishClientCloseResponse(HttpConnectionContext context, QwpIngressProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        try {
            flushPendingAck(context, state);
        } catch (PeerIsSlowToReadException e) {
            state.reArmClientCloseResponse();
            LOG.debug().$("Pre-close-response ack flush blocked, re-parking close response [fd=")
                    .$(context.getFd()).I$();
            throw e;
        }
        sendCloseResponse(context, state, state.getPendingCloseResponseCode());
        // The client's CLOSE is already consumed and our response is fully
        // flushed: the RFC 6455 handshake is complete, no echo can ever
        // arrive. Tear down immediately (s5.5.1: the server closes the TCP
        // connection first).
        gracefulCloseAndDisconnect(context);
    }

    /**
     * Teardown after a server-initiated fatal CLOSE frame has been fully sent.
     * Either enters the role-change close-echo handshake, or hands the
     * connection to the post-CLOSE read-drain.
     * <p>
     * A role-change close that is NOT echo-eligible (no durable acks, or the
     * upload grace expired with work un-acked) takes the SAME drain as every
     * other fatal close; it must not short-circuit to
     * {@link #gracefulCloseAndDisconnect}, which is for peers that already sent
     * their own CLOSE. A demoted primary's producer is by definition still
     * streaming, so its in-flight frames sit unread and the fd close turns
     * abortive (RST), destroying the client's unread [ack][CLOSE] tail -- the
     * grace-expired close still carries a partial durable ack, and the
     * non-durable-ack close carries the client's only trim signal. The drain
     * costs no availability: it is dispatcher-parked, bounded by
     * {@link QwpIngressProcessorState#CLOSE_DRAIN_TIMEOUT_MICROS}, and ends as
     * soon as the client closes.
     */
    private void finishServerFatalClose(HttpConnectionContext context, QwpIngressProcessorState state)
            throws ServerDisconnectException {
        if (beginCloseEchoWaitIfEligible(context, state)) {
            return;
        }
        gracefulCloseAndDrain(context, state);
    }

    private void flushPendingAck(HttpConnectionContext context, QwpIngressProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        flushPendingAck(context, state, true, false);
    }

    private void flushPendingAck(
            HttpConnectionContext context,
            QwpIngressProcessorState state,
            boolean isDurableAckPollAllowed
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        flushPendingAck(context, state, isDurableAckPollAllowed, false);
    }

    private void flushPendingAck(
            HttpConnectionContext context,
            QwpIngressProcessorState state,
            boolean isDurableAckPollAllowed,
            boolean isDurableProgressCollected
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (state.isAwaitingCloseEcho() || state.isCloseDraining()) {
            // A server CLOSE is already on the wire and RFC 6455 forbids any
            // frame following it. The final ACKs went out before the CLOSE;
            // this gate keeps "no ack after CLOSE" structural for both the
            // role-change echo wait and the general fatal-close read-drain.
            // It blocks the trailing frame-loop flush and onConnectionClosed's
            // best-effort teardown flush. Direct trySendDurableAck calls in
            // resumeSend cannot run here: both modes start from READY and no
            // send state begins after either mode arms.
            return;
        }
        if (state.hasPendingAck()) {
            trySendAck(context, state);
        }
        if (state.isDurableAckEnabled() && state.isSendReady()) {
            if (isDurableProgressCollected) {
                trySendCollectedDurableAck(context, state);
            } else if (isDurableAckPollAllowed) {
                trySendDurableAck(context, state);
            }
        }
    }

    /**
     * Half-closes the write side so the kernel emits FIN instead of an abortive
     * RST, performs a bounded best-effort inbound drain
     * ({@link #GRACEFUL_CLOSE_DRAIN_READ_BUDGET},
     * {@link #GRACEFUL_CLOSE_DRAIN_BYTE_BUDGET}), then signals teardown. This
     * prompt path is reserved for connections that can have nothing further in
     * flight: a completed client-initiated close handshake, a close-echo wait
     * the peer's CLOSE ended, and an expired close-echo wait. A
     * server-initiated fatal close against a still-streaming peer must use
     * {@link #gracefulCloseAndDrain} -- see {@link #finishServerFatalClose}.
     * <p>
     * The shutdown is unconditional. On the echo-wait cases
     * {@link #beginCloseEchoWaitIfEligible} usually half-closed already, making
     * it a no-op syscall on a cold path; the unconditional call also covers the
     * client-initiated paths (write side still open) and an echo wait whose
     * half-close was deferred behind a pending TLS write. On that last case it
     * performs exactly the truncation {@link #halfCloseWriteSide} declines, but
     * every caller here tears the connection down on return, so the fd close
     * would discard that ciphertext regardless -- FIN is strictly better than
     * the RST those paths emitted before the half-close existed.
     */
    private void gracefulCloseAndDisconnect(HttpConnectionContext context)
            throws ServerDisconnectException {
        try {
            Socket socket = context.getSocket();
            if (socket != null) {
                socket.shutdown(Net.SHUT_WR);
                long recvBuffer = context.getRecvBuffer();
                int recvBufferSize = context.getRecvBufferSize();
                int drained = 0;
                for (int i = 0; i < GRACEFUL_CLOSE_DRAIN_READ_BUDGET; i++) {
                    int cap = Math.min(recvBufferSize, GRACEFUL_CLOSE_DRAIN_BYTE_BUDGET - drained);
                    if (cap <= 0) {
                        break;
                    }
                    int n = socket.recv(recvBuffer, cap);
                    if (n <= 0) {
                        break;
                    }
                    drained += n;
                }
            }
        } catch (Throwable ignored) {
        }
        throw ServerDisconnectException.INSTANCE;
    }

    /**
     * Orderly teardown of a server-initiated fatal close that does not enter
     * the role-change CLOSE-echo handshake. Half-closes the write side so the
     * kernel emits FIN behind the CLOSE frame and its preceding ACKs, then
     * parks the connection in a bounded read-drain
     * ({@link QwpIngressProcessorState#beginCloseDrain}). Subsequent inbound
     * events land in {@code resumeRecv}'s drain branch and get discarded.
     * <p>
     * Closing the fd while a streaming client still has frames in flight can
     * force an RST and destroy the CLOSE or preceding ACKs in the peer's unread
     * receive queue. The drain exits when the peer closes, when
     * {@link QwpIngressProcessorState#CLOSE_DRAIN_TIMEOUT_MICROS} expires, or
     * when the transport idle timeout reaps a silent peer. A failed half-close
     * means the peer is already gone and triggers immediate cleanup.
     */
    private void gracefulCloseAndDrain(HttpConnectionContext context, QwpIngressProcessorState state)
            throws ServerDisconnectException {
        Socket socket = context.getSocket();
        if (socket == null || socket.shutdown(Net.SHUT_WR) != 0) {
            throw ServerDisconnectException.INSTANCE;
        }
        state.beginCloseDrain();
        LOG.debug().$("fatal CLOSE sent, draining until peer close [fd=").$(context.getFd()).I$();
    }

    /**
     * Puts FIN behind the role-change CLOSE frame of the close-echo wait, or
     * defers it when the socket still owes the peer TLS ciphertext.
     * <p>
     * An encrypted {@link Socket} reports a complete {@code send} while the tail
     * of the record is still buffered; only a later {@link Socket#tlsIO(int)} on
     * a writable socket flushes it, which is why the dispatchers OR EPOLLOUT in
     * on {@link Socket#wantsTlsWrite()}. Half-closing WRITE under a pending tail
     * makes that flush fail with EPIPE and the peer reads a truncated TLS stream
     * instead of the CLOSE. So this defers instead and every recv-driven
     * re-entry of {@link #resumeRecv} retries. A plain socket never reports a
     * pending TLS write, so it always takes the immediate half-close.
     * <p>
     * Residual: a peer that stays silent forever with our ciphertext undrained
     * never gets FIN, because nothing re-enters the processor to retry -- the
     * same transport-reaper exposure as a peer that ignores the FIN outright.
     * <p>
     * The shutdown result is deliberately NOT checked: shutdown fails only on an
     * already-dead socket, which epoll reports as readable, so the next dispatch
     * reads the error and tears the connection down anyway.
     */
    private void halfCloseWriteSide(HttpConnectionContext context, QwpIngressProcessorState state) {
        Socket socket = context.getSocket();
        if (socket == null) {
            return;
        }
        if (socket.wantsTlsWrite()) {
            state.onCloseEchoHalfCloseDeferred();
            return;
        }
        socket.shutdown(Net.SHUT_WR);
        state.onCloseEchoHalfClosed();
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

        if (QwpMessageHeader.isDurableAckPoll(payload, length)) {
            if (!state.isDurableAckEnabled()) {
                state.markSequenceUnresolved(seq);
                sendErrorResponse(
                        context,
                        state,
                        seq,
                        STATUS_PARSE_ERROR,
                        "durable ACK poll was not negotiated"
                );
                return;
            }
            // A poll must never close an in-progress FLAG_DEFER_COMMIT group.
            // Withhold its cumulative OK ACK until a later real commit covers
            // both the deferred rows and this sequence. Durable progress for
            // earlier committed work can still be flushed immediately.
            if (!state.hasUncommittedDeferredRows()) {
                state.setHighestProcessedSequence(seq);
            }
            // The receive-loop tail performs the normal ACK/durable-ACK flush
            // once for this event, just as it does for a regular binary frame.
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
                    case DICTIONARY_GAP -> STATUS_DICTIONARY_GAP;
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
        // While the close-echo wait is armed, our role-change CLOSE is already
        // on the wire and any inbound CLOSE completes the handshake. The client
        // sends nothing after its own CLOSE, so in-order TCP delivery proves it
        // consumed everything we sent before it -- the final durable ack
        // included; its replay window is trimmed and the reconnect cannot
        // duplicate.
        //
        // The role-change CLOSE carries NORMAL_CLOSURE, so a genuine echo is
        // indistinguishable from a voluntary client CLOSE that crossed ours on
        // the wire. Both are treated as completion; the crossed case leaves
        // delivery unproven but needs the producer to close in the same instant
        // as the demote, and the distinction would cost a private-use close code
        // that deployed fleets classify as a poison strike (see the role-change
        // close section of design/qwp-nack-policy-v2.md).
        //
        // Either way skip the close response (a second CLOSE would violate the
        // protocol) and tear down through gracefulCloseAndDisconnect, whose
        // bounded drain keeps the fd close from emitting an RST that would
        // destroy our unread tail.
        if (state.isAwaitingCloseEcho()) {
            LOG.info().$("close echo received, role-change close handshake complete [fd=")
                    .$(context.getFd()).$(", code=").$(closeCode).I$();
            return;
        }
        LOG.info().$("WebSocket close [fd=").$(context.getFd()).$(", code=").$(closeCode).I$();

        // Normalize close code for the response per RFC 6455 Section 7.4:
        // - 1004 is reserved and has no defined meaning
        // - 1005, 1006, 1015 must not appear on the wire
        // - 2000-2999 are reserved for extensions (none negotiated)
        // - codes outside the valid 1000-4999 range (including -1 for
        //   no-payload frames) are replaced with 1000 (normal closure)
        // Computed BEFORE the ack flush so a blocked flush can park the code
        // with the ack-then-close-response continuation.
        int responseCode;
        if (closeCode == 1004 || (closeCode >= 2000 && closeCode <= 2999)) {
            responseCode = WebSocketCloseCode.PROTOCOL_ERROR;
        } else if (closeCode < 1000 || closeCode > 4999
                || closeCode == 1005 || closeCode == 1006 || closeCode == 1015) {
            responseCode = WebSocketCloseCode.NORMAL_CLOSURE;
        } else {
            responseCode = closeCode;
        }

        // Flush any pending ACKs for already-committed data before closing.
        // The client may have sent [BINARY₁, ..., BINARYₙ, CLOSE] in the same
        // TCP segment — those messages are committed but not yet ACKed. Without
        // this flush the client would never learn that its data was persisted.
        try {
            flushPendingAck(context, state);
        } catch (PeerDisconnectedException e) {
            // Peer is gone; the response send below observes the same and the
            // CLOSE dispatch tears the connection down.
        } catch (PeerIsSlowToReadException e) {
            // ACK backpressure during the client's CLOSE. The parked frame
            // carries the client's LAST cumulative/durable ack: swallowing here
            // let the CLOSE dispatch throw with the ack still parked, and
            // onConnectionClosed's single teardown flush gives up under the same
            // backpressure -- committed-but-unacknowledged work then replays
            // after reconnect. Park an ack-then-close-response continuation and
            // propagate the backpressure instead.
            state.onClientCloseBlockedBehindAck(responseCode);
            throw e;
        }

        try {
            sendCloseResponse(context, state, responseCode);
        } catch (PeerDisconnectedException e) {
            // Peer is gone, nothing more to do.
        }
    }

    /**
     * Writes and sends the close response to a client-initiated CLOSE.
     * {@code responseCode} must already be normalized per RFC 6455 s7.4.
     * On partial write the residual bytes are parked in
     * {@code SEND_STATE_RESUME_CLOSE_RESPONSE} (flush-then-disconnect, no
     * close-echo wait -- the client's CLOSE is already consumed) and the
     * backpressure propagates so the framework parks for write.
     */
    private void sendCloseResponse(HttpConnectionContext context, QwpIngressProcessorState state, int responseCode)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        // Send close response only if buffer is clear
        if (!state.isSendReady()) {
            LOG.debug().$("Skipping close response, buffer busy [fd=").$(context.getFd()).I$();
            return;
        }

        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufferAddr = rawSocket.getBufferAddress();
        int bufferSize = rawSocket.getBufferSize();

        int written = WebSocketFrameWriter.writeCloseFrame(bufferAddr, bufferSize, responseCode, null);
        if (written > 0) {
            try {
                rawSocket.send(written);
            } catch (PeerIsSlowToReadException e) {
                // CLOSE frame partially written under a small send
                // fragmentation cap; resumeSend's
                // SEND_STATE_RESUME_CLOSE_RESPONSE branch finishes the flush and
                // disconnects. NOT onFatalCloseSendBlocked: this is the response
                // to a client-initiated CLOSE, so routing it through RESUME_CLOSE
                // would arm a close-echo wait for an echo that can never arrive.
                // Swallowing PISR would tear down mid-frame, so the client sees
                // EOF instead of the close code we promised.
                state.onCloseResponseSendBlocked();
                throw e;
            }
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
        if (state.isRoleChangeCloseDeferred()) {
            boolean isGraceExpired = state.isRoleChangeCloseGraceExpired();
            // When the send side is READY, flushPendingAck above produced both
            // the progress frame and its full-coverage result in one registry
            // traversal. A pre-existing parked send prevents that flush; use
            // the coverage-only traversal in that case because an in-flight
            // durable ACK may still own durableProgressSnapshot.
            boolean isDurableProgressFlushed = state.isSendReady();
            boolean isDurableWorkFullyUploaded = isDurableProgressFlushed
                    ? state.isDurableProgressSnapshotFullyUploaded()
                    : state.isDurableWorkFullyUploaded(engine.getDurableAckRegistry());
            if (isDurableWorkFullyUploaded || isGraceExpired) {
                roleChangeCloseWithUploadGrace(
                        context,
                        state,
                        state.getRoleChangeCloseReason(),
                        isDurableWorkFullyUploaded,
                        isGraceExpired,
                        isDurableProgressFlushed,
                        false
                );
                return;
            }
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
     * durable ack, emits the role-change close code, and then -- rather than closing the fd
     * and racing the client's receive path -- holds the connection open in the
     * RFC 6455 close-handshake wait ({@link #beginCloseEchoWaitIfEligible})
     * until the client's CLOSE echo (or FIN) confirms the ack was consumed:
     * only then is the replay window provably empty and every in-flight batch
     * lands exactly once. If uploads stall past the grace budget the close
     * proceeds anyway -- availability over the duplicate guard, matching the
     * pre-deferral behaviour.
     * <p>
     * Non-durable-ack connections close immediately: their cumulative OK ack is
     * flushed synchronously by sendFatalClose and carries no upload lag.
     */
    private void roleChangeCloseWithUploadGrace(
            HttpConnectionContext context,
            QwpIngressProcessorState state,
            CharSequence reason
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        boolean isGraceExpired = state.isRoleChangeCloseGraceExpired();
        boolean isDurableProgressCollected = false;
        boolean isDurableProgressFlushed = false;
        boolean isDurableWorkFullyUploaded;
        if (!state.isDurableAckEnabled() || !state.hasPendingDurableWork()) {
            // Nothing committed awaits a durable ack (or durable acks are
            // disabled): trivially covered, no registry pass needed.
            isDurableWorkFullyUploaded = true;
        } else if (state.isSendReady()) {
            if (!isGraceExpired && state.isRoleChangeCloseDeferred()) {
                // Deferral re-entry with a clear send side may flush before the
                // decision because the close is already armed. One fused pass
                // both sends progress and returns poll-fresh coverage.
                flushPendingAck(context, state);
                isDurableProgressFlushed = true;
            } else {
                // First entry must mark/arm the close before any throwing send,
                // and an expired close must not acquire a one-poll delay if its
                // send blocks. Collect without sending; the overload below
                // consumes this snapshot only after recording the close state.
                state.collectDurableProgress(engine.getDurableAckRegistry());
                isDurableProgressCollected = true;
            }
            isDurableWorkFullyUploaded = state.isDurableProgressSnapshotFullyUploaded();
        } else {
            // The durable progress snapshot may belong to a parked durable ACK,
            // so preserve it for onDurableAckSent and use the coverage-only
            // traversal. The continuation performs a fresh fused pass after
            // the genuine blocked-send interval when pending work remains.
            isDurableWorkFullyUploaded = state.isDurableWorkFullyUploaded(engine.getDurableAckRegistry());
        }
        roleChangeCloseWithUploadGrace(
                context,
                state,
                reason,
                isDurableWorkFullyUploaded,
                isGraceExpired,
                isDurableProgressFlushed,
                isDurableProgressCollected
        );
    }

    private void roleChangeCloseWithUploadGrace(
            HttpConnectionContext context,
            QwpIngressProcessorState state,
            CharSequence reason,
            boolean isDurableWorkFullyUploaded,
            boolean isGraceExpired,
            boolean isDurableProgressFlushed,
            boolean isDurableProgressCollected
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        if (state.isAwaitingCloseEcho()) {
            // CLOSE already sent; the deferral has completed. Just poll the
            // echo grace budget.
            checkCloseEchoWaitExpiry(context, state);
            return;
        }
        if (state.isDurableAckEnabled() && !isDurableWorkFullyUploaded && !isGraceExpired) {
            boolean firstDeferral = !state.isRoleChangeCloseDeferred();
            state.deferRoleChangeClose(reason);
            if (firstDeferral) {
                LOG.info().$("deferring role-change close until committed work is durably uploaded [fd=")
                        .$(context.getFd()).I$();
                // Push whatever cumulative/durable progress exists right now;
                // the final durable ack goes out with the close itself once
                // coverage is confirmed. Reuse a snapshot collected before
                // the close state was armed, or skip the poll when this
                // dispatch already flushed the fused result.
                flushPendingAck(
                        context,
                        state,
                        !isDurableProgressFlushed && !isDurableProgressCollected,
                        isDurableProgressCollected
                );
            }
            return;
        }
        if (isGraceExpired && !isDurableWorkFullyUploaded) {
            // Grace expired with genuinely un-acked durable work according to
            // the fused completion snapshot: the one close the operator must
            // see. Do not repeat the registry traversal just for diagnostics.
            LOG.error().$("role-change close upload grace expired; closing with un-acked durable work, client replay may duplicate [fd=")
                    .$(context.getFd()).I$();
        }
        // Mark the role-change close HERE -- immediately before the CLOSE goes
        // out -- and not before the deferral return above. The mark is the
        // close-echo eligibility key (beginCloseEchoWaitIfEligible) and survives
        // every per-message clear, so setting it while the deferral is merely
        // armed would leave it true for the whole grace budget with no CLOSE on
        // the wire. The deferral gate only covers BINARY data frames, so a TEXT,
        // CONTINUATION, fragmented or oversized frame arriving in that window
        // routes straight to sendFatalClose and would inherit role-change
        // semantics it never earned -- arming the echo wait on a 1002/1003
        // close. Setting it here loses nothing: both consumers run only after a
        // CLOSE has been written, this is the sole emission site, and the mark
        // still precedes the throwing send, so it is in place for the
        // onFatalCloseBlocked unwind and the sendDeferredFatalClose resume.
        state.initiateRoleChangeClose();
        // NORMAL_CLOSURE, not a private-use code: deployed store-and-forward
        // fleets classify close codes behaviourally and treat anything outside
        // NORMAL_CLOSURE/GOING_AWAY as a poison strike, which escalates to a
        // PROTOCOL_VIOLATION terminal that quarantines the very slot this
        // handoff protects. A distinguishable code would only sharpen a log line
        // and needs a negotiated capability first -- see
        // design/qwp-nack-policy-v2.md.
        sendFatalClose(
                context,
                state,
                WebSocketCloseCode.NORMAL_CLOSURE,
                state.isRoleChangeCloseDeferred() ? state.getRoleChangeCloseReason() : reason,
                isDurableProgressFlushed,
                isDurableProgressCollected
        );
    }

    private void handleWebSocketFrame(HttpConnectionContext context, QwpIngressProcessorState state, int opcode, boolean fin, long payload, int length)
            throws ServerDisconnectException, PeerDisconnectedException, PeerIsSlowToReadException {
        // While the close-echo wait is armed, our CLOSE -- preceded by the final
        // durable ack -- is already on the wire (RFC 6455: no frame may follow
        // it). Discard every inbound frame except the CLOSE echo, whatever the
        // opcode: data frames must not touch the engine or the sequence counters
        // (the client replays above its acked watermark anyway), PINGs get no
        // pong, and protocol-violating frames must NOT route to sendFatalClose
        // -- that would put a second CLOSE on the wire. Reading and dropping
        // them keeps the socket drained so the fd close cannot turn abortive.
        //
        // The discard does not poll the grace budget: processWebSocketFrames is
        // its only caller and polls once on entry, so every discarded frame
        // already sits behind a poll made in the same call. A per-frame poll
        // would add an Os.currentTimeMicros() JNI transition per six-byte header
        // -- up to 43_690 per turn at the read cap -- for no accuracy, since the
        // parse loop is bounded by one capped recv and always returns to the
        // dispatcher, which polls again.
        if (state.isAwaitingCloseEcho() && opcode != WebSocketOpcode.CLOSE) {
            // Count, do not log: processWebSocketFrames logs the per-call
            // total. An increment also keeps the gate free of the LOG.debug()
            // call chain a flooding peer would otherwise drive 43_690 times a
            // turn.
            closeEchoDiscardedFrames++;
            return;
        }
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
                // Every sub-case handleClose returns from has a complete RFC 6455
                // handshake behind it, so nothing more is coming and s5.5.1 has
                // the server close first. Use the graceful helper rather than a
                // bare disconnect: a peer that left bytes on the wire behind its
                // CLOSE leaves them unread in our receive queue, and close(2) on
                // unread bytes emits RST -- discarding the close response and,
                // on a durable-ack connection, the final ACKs ahead of it, even
                // when the peer's kernel already holds them. The drain is a
                // bounded best effort; a peer still streaming past the budgets
                // outruns it and still gets an RST.
                gracefulCloseAndDisconnect(context);
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
        boolean hasPolledDurableProgress = false;
        boolean hasProcessedFrame = false;
        closeEchoDiscardedFrames = 0;

        try {
            // This entry poll is the wait's ONE deadline check per call and it
            // covers every frame the loop below discards (which is why that gate
            // does not poll per frame). It must sit here rather than only on
            // parsed-frame dispatch: a peer trickling a legal-size frame
            // byte-by-byte re-enters on every recv without completing a frame,
            // and both break-out paths below would bypass the deadline -- a
            // slowloris peer would keep the 5s wait alive indefinitely while the
            // active socket also dodges the idle reaper.
            if (state.isAwaitingCloseEcho()) {
                checkCloseEchoWaitExpiry(context, state);
            }
            while (pos < bufferEnd) {
                frameParser.reset();
                int consumed = frameParser.parse(pos, bufferEnd);

                if (frameParser.getState() == WebSocketFrameParser.STATE_ERROR) {
                    LOG.error().$("WebSocket frame error [fd=").$(context.getFd()).$(", code=").$(frameParser.getErrorCode()).I$();
                    if (state.isAwaitingCloseEcho()) {
                        // A malformed header destroys frame synchronization, so a
                        // later byte sequence cannot safely be recognized as the
                        // client's CLOSE echo. Drop all buffered garbage and let
                        // resumeRecv drain subsequent bytes until peer FIN or the
                        // bounded echo wait expires. Disconnecting immediately can
                        // close the fd with unread inbound bytes and reset the peer's
                        // unread final durable ACK/CLOSE tail.
                        state.onCloseEchoSyncLost();
                        pos = bufferEnd;
                        checkCloseEchoWaitExpiry(context, state);
                        return;
                    }
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
                        if (state.isAwaitingCloseEcho()) {
                            // The frame can never complete within the buffer, so
                            // frame sync is unrecoverable and the echo can never
                            // be parsed. Switch to read-and-discard and drop
                            // everything buffered: advancing pos to bufferEnd
                            // makes the finally store recvBufferLen = 0, so no
                            // mid-frame garbage survives to be misread as a fake
                            // CLOSE opcode. No second CLOSE may go out.
                            state.onCloseEchoSyncLost();
                            checkCloseEchoWaitExpiry(context, state);
                            pos = bufferEnd;
                            return;
                        }
                        sendFatalClose(context, state,
                                WebSocketCloseCode.MESSAGE_TOO_BIG,
                                "frame payload exceeds maximum size");
                        return; // CLOSE sent (echo wait or drain armed) or parked for resume.
                    }
                    break;
                }

                if (consumed == 0 || frameParser.getState() == WebSocketFrameParser.STATE_NEED_MORE) {
                    break;
                }

                // Frame parsed successfully
                boolean wasAwaitingCloseEcho = state.isAwaitingCloseEcho();
                int opcode = frameParser.getOpcode();
                long payloadPtr = pos + frameParser.getHeaderSize();
                int payloadLen = (int) frameParser.getPayloadLength();

                // Unmask payload -- except while awaiting the close echo:
                // every non-CLOSE inbound frame in that window is discarded
                // without its payload being read (handleWebSocketFrame's
                // discard gate). Skipping the O(payload) XOR pass denies a
                // wedged-but-chatty peer free CPU: payloads can approach the
                // configured receive-buffer size (2 MiB by default) and
                // repeat for the lifetime of the wait. CLOSE frames are the
                // exception: handleClose reads the close code for the operator
                // log line, and RFC 6455 caps control frame payloads at 125
                // bytes, so their unmask is O(1).
                if (frameParser.isMasked()
                        && (!state.isAwaitingCloseEcho() || opcode == WebSocketOpcode.CLOSE)) {
                    frameParser.unmaskPayload(payloadPtr, payloadLen);
                }

                // Advance past this frame BEFORE processing. If handleWebSocketFrame
                // throws (e.g. ACK backpressure), the committed frame won't be replayed.
                pos += consumed;

                handleWebSocketFrame(context, state, opcode, frameParser.isFin(), payloadPtr, payloadLen);
                hasProcessedFrame = true;
                if (opcode == WebSocketOpcode.PING) {
                    hasPolledDurableProgress = true;
                } else if (opcode == WebSocketOpcode.BINARY) {
                    // A normal BINARY frame may have committed new durable
                    // work after an earlier PING poll in the same recv. Only a
                    // role-deferral BINARY path polls internally.
                    hasPolledDurableProgress = state.isRoleChangeCloseDeferred();
                }
                if (!wasAwaitingCloseEcho && state.isAwaitingCloseEcho() && pos < bufferEnd) {
                    // The server sent its CLOSE while handling this frame. All
                    // bytes already returned by the preceding recv necessarily
                    // arrived before that CLOSE and therefore cannot contain its
                    // echo. Drop the pipelined suffix without parsing it; later
                    // receives use the bounded close-echo read cap above.
                    pos = bufferEnd;
                    return;
                }
                if (state.isCloseDraining()) {
                    // The write side is shut down. Every remaining buffered
                    // frame predates the server CLOSE, so discard the lot and
                    // let the post-CLOSE read-drain consume future input.
                    pos = bufferEnd;
                    return;
                }
            }

            // Keep cumulative ACK progress available even when this recv ends
            // mid-frame. Durable progress polling is different: it scans every
            // table with outstanding work, so a partial-frame-only re-entry
            // must not repeat that scan without a meaningful frame event.
            flushPendingAck(context, state, hasProcessedFrame && !hasPolledDurableProgress);
        } catch (ServerDisconnectException e) {
            // Every teardown this method can reach -- handleWebSocketFrame's
            // CLOSE arm and the entry expiry poll both go through
            // gracefulCloseAndDisconnect -- drains inbound bytes into THIS
            // buffer, using it as scratch from offset 0. Whatever pos still
            // points at has been overwritten, so drop it: the compaction below
            // would otherwise memmove up to a full recv buffer of drained
            // garbage and record it as parked frame bytes. The connection is
            // going away in either case (a ServerDisconnectException always
            // ends in dispatcher.disconnect, whose onDisconnected() zeroes
            // recvBufferLen), so this changes no observable behaviour today --
            // it removes a pointless copy on teardown and keeps a future
            // non-terminal caller from parsing drained garbage as frames.
            pos = bufferEnd;
            throw e;
        } finally {
            // Compact unprocessed bytes to buffer start and update state.
            // Handles both normal exit (remaining=0) and exception unwind
            // (e.g. PeerIsSlowToReadException from trySendAck after a committed frame).
            int remaining = (int) (bufferEnd - pos);
            if (remaining > 0 && pos > buffer) {
                Unsafe.copyMemory(pos, buffer, remaining);
            }
            state.setRecvBufferLen(remaining);
            if (closeEchoDiscardedFrames > 0) {
                // One record per call, not per frame: the discard gate can fire
                // 43_690 times in a single capped echo-wait read.
                LOG.debug().$("WebSocket frames discarded awaiting close echo [fd=").$(context.getFd())
                        .$(", frames=").$(closeEchoDiscardedFrames).I$();
            }
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

        finishServerFatalClose(context, state);
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
     * code, then enters the role-change echo wait, performs prompt role-change
     * teardown, or hands the connection to the bounded post-CLOSE read-drain.
     * Routes through the send state machine so the CLOSE lands even when an
     * ACK/durable-ACK is mid-flight:
     * <ul>
     *   <li>Awaiting close echo → a CLOSE is already on the wire and no frame
     *       may follow it (RFC 6455); polls the echo grace budget and returns
     *       without emitting a second CLOSE.</li>
     *   <li>State READY, send succeeds → enters the role-change close-echo
     *       wait when eligible, tears down a grace-expired role-change close
     *       promptly, or half-closes and drains any other fatal close.</li>
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
        sendFatalClose(context, state, closeCode, reason, false, false);
    }

    private void sendFatalClose(
            HttpConnectionContext context,
            QwpIngressProcessorState state,
            int closeCode,
            CharSequence reason,
            boolean isDurableProgressFlushed,
            boolean isDurableProgressCollected
    ) throws PeerIsSlowToReadException, ServerDisconnectException {
        if (state.isAwaitingCloseEcho()) {
            // Our role-change CLOSE is already on the wire and nothing may
            // follow it. The connection exists only to observe the client's
            // echo; poll the grace budget and stand down. This gate is the
            // structural guarantee that NO caller -- the per-opcode reject
            // paths, the too-large frame paths in the recv machinery -- can
            // emit a second CLOSE while the echo wait is in progress.
            checkCloseEchoWaitExpiry(context, state);
            return;
        }
        // Give the client one more chance to learn about already-committed
        // sequences before tearing the connection down. flushPendingAck is a
        // no-op when state is not READY (ACK already in flight), so it does
        // not interfere with the deferred path below.
        try {
            flushPendingAck(
                    context,
                    state,
                    !isDurableProgressFlushed && !isDurableProgressCollected,
                    isDurableProgressCollected
            );
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

        finishServerFatalClose(context, state);
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

    private void trySendCollectedDurableAck(HttpConnectionContext context, QwpIngressProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        assert state.isSendReady() : "trySendCollectedDurableAck called in wrong state";

        CharSequenceLongHashMap progress = state.getDurableProgressSnapshot();
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

    private void trySendDurableAck(HttpConnectionContext context, QwpIngressProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        assert state.isSendReady() : "trySendDurableAck called in wrong state";

        state.collectDurableProgress(engine.getDurableAckRegistry());
        trySendCollectedDurableAck(context, state);
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
