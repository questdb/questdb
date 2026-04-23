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

package io.questdb.cutlass.qwp.server.egress;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpException;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRawSocket;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.qwp.codec.QwpEgressColumnDef;
import io.questdb.cutlass.qwp.codec.QwpEgressFrameWriter;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.codec.QwpResultBatchBuffer;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.server.QwpWebSocketHttpProcessor;
import io.questdb.cutlass.qwp.server.QwpWebSocketUpgradeProcessor;
import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.cutlass.qwp.websocket.WebSocketFrameParser;
import io.questdb.cutlass.qwp.websocket.WebSocketFrameWriter;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.network.Socket;
import io.questdb.std.AssociativeCache;
import io.questdb.std.ConcurrentAssociativeCache;
import io.questdb.std.Misc;
import io.questdb.std.NoOpAssociativeCache;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.Zstd;
import io.questdb.std.str.Utf8Sequence;

/**
 * HTTP request processor for the QWP egress endpoint at {@code /read/v1}.
 * <p>
 * The processor owns three distinct responsibilities, each triggered by a
 * different callback from the HTTP framework:
 * <ol>
 *   <li><b>WebSocket handshake</b> ({@code onHeadersReady} -> {@code onRequestComplete}
 *       -> {@code resumeSend}). Validates the upgrade headers, writes the 101
 *       response, then defers {@code rawSocket.send} to
 *       {@code onRequestComplete} so a partial write (e.g. under
 *       {@code DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE}) can park the
 *       connection for write-ready; {@code resumeSend} finalises the protocol
 *       switch after the flush completes.</li>
 *   <li><b>Inbound frame dispatch</b> ({@code resumeRecv} -> {@code processWebSocketFrames}
 *       -> {@code dispatchEgressMessage}). Decodes WebSocket frames from the
 *       recv buffer and routes QWP messages: {@code QUERY_REQUEST} to
 *       {@link #handleQueryRequest}, {@code CANCEL} to {@link #handleCancel},
 *       {@code CREDIT} to {@link #handleCredit}, plus PING/PONG/CLOSE for
 *       WebSocket control frames.</li>
 *   <li><b>Query streaming</b> ({@link #streamResults}, re-entered from
 *       {@link #resumeSend} and {@link #handleCredit}). Iterates the cursor
 *       batch-by-batch, emits {@code RESULT_BATCH} frames, and yields
 *       cooperatively on cancellation, credit exhaustion, or peer back-pressure.
 *       Non-SELECT statements (DDL / INSERT / UPDATE / ALTER / DROP / etc.) run
 *       synchronously via {@link #executeNonSelect} and reply with
 *       {@code EXEC_DONE}.</li>
 * </ol>
 * <p>
 * Per-connection state lives on {@link QwpEgressProcessorState} held via
 * {@code LocalValue} on the connection context. It carries the in-flight
 * cursor, schema registry, bind-variable service, credit / cancel flags, the
 * per-batch {@link QwpResultBatchBuffer}, and the connection-scoped SYMBOL
 * dictionary shared across queries on the same connection.
 * <p>
 * Wire-level flags on every {@code RESULT_BATCH}: {@code FLAG_DELTA_SYMBOL_DICT}
 * (SYMBOL values ship once per connection, per-row payload is a varint id) and
 * {@code FLAG_GORILLA} (TIMESTAMP / TIMESTAMP_NANOS / DATE columns carry a 1-byte
 * encoding discriminator; ordered columns compress via delta-of-delta, jumpy
 * ones fall back to raw).
 */
public class QwpEgressUpgradeProcessor implements HttpRequestProcessor, QuietCloseable {

    /**
     * Phase 1 batch cap. Size-based cap is indirectly enforced by the rawSocket
     * send buffer capacity (rejections become QUERY_ERROR). Larger batches
     * amortise per-batch overhead (schema-reference emit, WS header, send
     * syscall, client queue hand-off) across more rows, which is the dominant
     * per-byte throughput lever once the per-row emit cost has been
     * columnarised. Client cap is 1_048_576 so there is ample headroom for
     * future raises if wider schemas benefit.
     * <p>
     * Exposed as public only so batch-boundary tests can pin their assertions
     * against the live value; bumping this constant won't silently turn tests
     * into no-ops.
     */
    public static final int MAX_ROWS_PER_BATCH = 16_384;
    private static final Log LOG = LogFactory.getLog(QwpEgressUpgradeProcessor.class);
    private static final LocalValue<QwpEgressProcessorState> LV = new LocalValue<>();
    private static final NoOpAssociativeCache<RecordCursorFactory> NO_OP_SELECT_CACHE = new NoOpAssociativeCache<>();
    private final CairoEngine engine;
    private final int forceRecvFragmentationChunkSize;
    private final WebSocketFrameParser frameParser = new WebSocketFrameParser();
    private final int recvBufferSize;
    /**
     * Server-wide cache of compiled {@link RecordCursorFactory} keyed by SQL text.
     * Shared across every connection handled by this processor, so repeat queries
     * with the same text skip the compile + plan + factory-build pipeline. Mirrors
     * the {@code HttpServer.selectCache} pattern (same cache shape, separate
     * instance -- engine-level unification is a follow-up exercise).
     */
    private final AssociativeCache<RecordCursorFactory> selectCache;
    private final int sharedWorkerCount;

    public QwpEgressUpgradeProcessor(
            CairoEngine engine,
            HttpFullFatServerConfiguration httpConfiguration,
            int sharedWorkerCount
    ) {
        this.engine = engine;
        this.forceRecvFragmentationChunkSize = httpConfiguration.getHttpContextConfiguration()
                .getForceRecvFragmentationChunkSize();
        this.recvBufferSize = httpConfiguration.getRecvBufferSize();
        this.sharedWorkerCount = sharedWorkerCount;
        this.selectCache = httpConfiguration.isQueryCacheEnabled()
                ? new ConcurrentAssociativeCache<>(httpConfiguration.getConcurrentCacheConfiguration())
                : NO_OP_SELECT_CACHE;
    }

    // Exposed for unit tests in a different package that verify the error ->
    // status mapping (QwpEgressCancelTest). No production callers outside
    // this class.
    public static byte mapErrorStatus(Throwable e) {
        // SqlException covers both syntax errors and semantic errors (e.g., table not found).
        // Its getMessage() already embeds the "[position] text" form.
        if (e instanceof SqlException) {
            return QwpConstants.STATUS_PARSE_ERROR;
        }
        // QwpParseException signals a client-side protocol error (truncated frame,
        // unknown bind type code, out-of-range scale/precision, etc). It originates
        // entirely from client input so it belongs in the same bucket as SqlException
        // rather than STATUS_INTERNAL_ERROR.
        if (e instanceof QwpParseException) {
            return QwpConstants.STATUS_PARSE_ERROR;
        }
        if (e instanceof CairoException ce) {
            if (ce.isAuthorizationError()) {
                return QwpConstants.STATUS_SECURITY_ERROR;
            }
            // Explicit cancellation (setCancellation=true) surfaces as STATUS_CANCELLED.
            if (ce.isCancellation()) {
                return QwpEgressMsgKind.STATUS_CANCELLED;
            }
            // Non-cancellation interruptions (query timeout, circuit breaker) and
            // out-of-memory both map to STATUS_LIMIT_EXCEEDED -- the client can
            // distinguish them via the message text.
            if (ce.isInterruption() || ce.isOutOfMemory()) {
                return QwpEgressMsgKind.STATUS_LIMIT_EXCEEDED;
            }
            return QwpConstants.STATUS_INTERNAL_ERROR;
        }
        return QwpConstants.STATUS_INTERNAL_ERROR;
    }

    @Override
    public void close() {
        Misc.free(selectCache);
    }

    @Override
    public void onConnectionClosed(HttpConnectionContext context) {
        LOG.info().$("Egress WebSocket connection closed [fd=").$(context.getFd()).I$();
        QwpEgressProcessorState state = LV.get(context);
        if (state == null) {
            return;
        }
        try {
            state.onDisconnected();
        } finally {
            LV.set(context, null);
        }
    }

    @Override
    public void onHeadersReady(HttpConnectionContext context) throws PeerDisconnectedException {
        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufferAddr = rawSocket.getBufferAddress();
        int bufferSize = rawSocket.getBufferSize();

        String validationError = QwpWebSocketHttpProcessor.validateHandshake(context.getRequestHeader());
        if (validationError != null) {
            LOG.error().$("Egress WebSocket handshake validation failed [fd=").$(context.getFd())
                    .$(", error=").$(validationError).I$();
            try {
                final boolean versionError = QwpWebSocketHttpProcessor.isVersionValidationError(validationError);
                final int written = versionError
                        ? QwpWebSocketUpgradeProcessor.writeUpgradeRequiredResponse(bufferAddr, bufferSize)
                        : QwpWebSocketUpgradeProcessor.writeBadRequestResponse(bufferAddr, bufferSize, validationError);
                if (written <= 0) {
                    throw HttpException.instance("egress handshake error response does not fit send buffer");
                }
                rawSocket.send(written);
            } catch (PeerIsSlowToReadException e) {
                throw HttpException.instance("Egress handshake rejected: ").put(validationError);
            }
            return;
        }

        HttpRequestHeader requestHeader = context.getRequestHeader();
        Utf8Sequence wsKey = QwpWebSocketHttpProcessor.getWebSocketKey(requestHeader);

        int negotiatedVersion = negotiateQwpVersion(requestHeader, context.getFd());
        // Pick the compression codec now so the response size reflects the
        // optional X-QWP-Content-Encoding header. The negotiator returns
        // RESULT_NONE when the header is absent or no supported codec is
        // listed, which leaves the wire raw and omits the response header.
        Utf8Sequence acceptEncoding = requestHeader.getHeader(
                QwpWebSocketHttpProcessor.HEADER_X_QWP_ACCEPT_ENCODING);
        long negotiatedCompression = QwpEgressCompressionNegotiator.negotiate(acceptEncoding);
        byte negotiatedCodec = QwpEgressCompressionNegotiator.codec(negotiatedCompression);
        byte negotiatedLevel = QwpEgressCompressionNegotiator.level(negotiatedCompression);
        String contentEncodingHeader = QwpEgressCompressionNegotiator.responseHeaderValue(
                negotiatedCodec, negotiatedLevel);

        String acceptKey = QwpWebSocketHttpProcessor.computeAcceptKey(wsKey);
        int requiredHandshakeSize = QwpWebSocketHttpProcessor.responseSize(
                acceptKey, negotiatedVersion, contentEncodingHeader);
        if (requiredHandshakeSize > bufferSize) {
            throw HttpException.instance("egress 101 handshake response does not fit send buffer [required=")
                    .put(requiredHandshakeSize).put(", available=").put(bufferSize).put(']');
        }

        QwpEgressProcessorState state = LV.get(context);
        if (state == null) {
            state = new QwpEgressProcessorState(engine.getConfiguration());
            LV.set(context, state);
        } else {
            state.clear();
        }
        state.of(context.getFd(), context.getSecurityContext());
        state.setNegotiatedVersion((byte) negotiatedVersion);
        state.setCompression(negotiatedCodec, negotiatedLevel);
        // Optional client preference for per-batch row cap. Absent or malformed
        // header falls back to the server's hard cap. Values outside [1, MAX]
        // are clamped rather than rejected so one buggy client doesn't break
        // the handshake -- the server-authoritative cap is always applied.
        Utf8Sequence maxBatchRowsHeader = requestHeader.getHeader(
                QwpWebSocketHttpProcessor.HEADER_X_QWP_MAX_BATCH_ROWS);
        int effectiveMaxBatchRows = MAX_ROWS_PER_BATCH;
        if (maxBatchRowsHeader != null) {
            int clientRequested = Numbers.parseNonNegativeIntQuiet(maxBatchRowsHeader);
            if (clientRequested > 0) {
                effectiveMaxBatchRows = Math.min(clientRequested, MAX_ROWS_PER_BATCH);
            }
        }
        state.setMaxBatchRows(effectiveMaxBatchRows);

        int bytesWritten = QwpWebSocketHttpProcessor.writeResponse(
                bufferAddr, acceptKey, negotiatedVersion, contentEncodingHeader);
        // The HttpRequestProcessor contract forbids PeerIsSlowToReadException
        // from onHeadersReady, so we defer the raw-socket send to
        // onRequestComplete where PISR propagates cleanly into the framework's
        // park-on-write path. State carries the byte count across the two
        // calls (the framework invokes them back-to-back in handleClientRecv).
        state.setPendingHandshakeBytes(bytesWritten);
        state.setHandshakeFlushPending(true);
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context)
            throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        QwpEgressProcessorState state = LV.get(context);
        if (state == null || !state.isHandshakeFlushPending()) {
            // Either we're already past the handshake (protocol-switched
            // connection's onRequestComplete after a recv cycle) or a previous
            // handshake failure already disconnected us.
            if (state != null && state.isWsHandshakeSent()) {
                LOG.debug().$("Egress WebSocket ready for frames [fd=").$(context.getFd()).I$();
            }
            return;
        }
        HttpRawSocket rawSocket = context.getRawResponseSocket();
        // rawSocket.send may park us (partial write against a small send
        // fragmentation cap, or kernel send buffer full). PISR propagates to
        // handleClientRecv which parks the connection for write and schedules
        // resumeSend -- resumeSend finalises the protocol switch after the
        // rest of the handshake bytes flush.
        rawSocket.send(state.getPendingHandshakeBytes());
        finalizeHandshake(context, state);
    }

    @Override
    public void resumeRecv(HttpConnectionContext context)
            throws PeerIsSlowToWriteException, ServerDisconnectException, PeerIsSlowToReadException {
        QwpEgressProcessorState state = LV.get(context);
        if (state == null) {
            LOG.error().$("Egress resumeRecv but no state available [fd=").$(context.getFd()).I$();
            throw ServerDisconnectException.INSTANCE;
        }

        Socket socket = context.getSocket();
        long recvBuffer = context.getRecvBuffer();
        int recvBufferSize = context.getRecvBufferSize();

        try {
            int recvBufferLen = state.getRecvBufferLen();
            if (recvBufferLen >= recvBufferSize) {
                LOG.error().$("Egress WebSocket frame too large for recv buffer [fd=").$(context.getFd())
                        .$(", bufferSize=").$(recvBufferSize).I$();
                throw ServerDisconnectException.INSTANCE;
            }

            int remaining = recvBufferSize - recvBufferLen;
            int read = socket.recv(recvBuffer + recvBufferLen, Math.min(forceRecvFragmentationChunkSize, remaining));
            if (read < 0) {
                LOG.info().$("Egress WebSocket peer disconnected [fd=").$(context.getFd()).I$();
                throw ServerDisconnectException.INSTANCE;
            }
            if (read == 0) {
                throw PeerIsSlowToWriteException.INSTANCE;
            }

            recvBufferLen += read;
            processWebSocketFrames(context, state, recvBuffer, recvBufferLen);

            if (read == forceRecvFragmentationChunkSize) {
                throw PeerIsSlowToWriteException.INSTANCE;
            }
        } catch (ServerDisconnectException | PeerIsSlowToWriteException | PeerIsSlowToReadException e) {
            throw e;
        } catch (Throwable e) {
            LOG.error().$("Egress WebSocket error [fd=").$(context.getFd()).$(", error=").$(e).I$();
            throw ServerDisconnectException.INSTANCE;
        }
    }

    /**
     * Continues a send that was parked on {@code PeerIsSlowToReadException}.
     * <p>
     * Always flushes any deferred bytes first -- even when {@code isStreamingActive()}
     * is false. The PARKED bytes may belong to a {@code QUERY_ERROR} frame, or the
     * {@code RESULT_END} frame from a completed query (streamResults releases the
     * cursor BEFORE sending RESULT_END, so by the time we re-enter here streaming
     * is already inactive). If we skipped the flush in that case, the client would
     * see a stalled socket and never receive the final payload.
     * <p>
     * After the flush succeeds, two follow-on states are possible:
     * <ul>
     *   <li>streaming inactive -- nothing to do, return (error/end already flushed).</li>
     *   <li>streaming active and still producing -- re-enter the loop.</li>
     * </ul>
     */
    @Override
    public void resumeSend(HttpConnectionContext context)
            throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        QwpEgressProcessorState state = LV.get(context);

        // 1. Flush any deferred bytes from the previous send, regardless of
        //    streaming state. Throws PeerIsSlowToReadException if still blocked
        //    -- we'll be parked again and re-entered via another resumeSend.
        LOG.debug().$("Egress resumeSend flushing deferred bytes [fd=").$(context.getFd())
                .$(", streaming=").$(state != null && state.isStreamingActive())
                .I$();
        context.resumeResponseSend();

        // 2. If the handshake response was parked mid-write, the flush above
        //    just completed it. Finalise the protocol switch now so subsequent
        //    recvs parse WebSocket frames rather than HTTP.
        if (state != null && state.isHandshakeFlushPending()) {
            finalizeHandshake(context, state);
            return;
        }

        if (state == null || !state.isStreamingActive()) {
            // Nothing to drive -- the deferred flush above was the last thing
            // this connection had queued: either a QUERY_ERROR after endStreaming,
            // or the RESULT_END frame (streamResults calls endStreaming before the
            // final send, so a parked RESULT_END leaves streaming inactive).
            return;
        }

        // 2. Otherwise, continue the streaming loop from the cursor's current position.
        try {
            streamResults(context, state);
        } catch (PeerDisconnectedException e) {
            throw e;
        } catch (PeerIsSlowToReadException e) {
            LOG.debug().$("Egress resumeSend re-parked [fd=").$(context.getFd())
                    .$(", requestId=").$(state.getStreamingRequestId())
                    .$(", batchSeq=").$(state.getStreamingBatchSeq())
                    .$(", rowsEmitted=").$(state.getStreamingRowsEmitted())
                    .I$();
            throw e;
        } catch (Throwable t) {
            LOG.error().$("Egress resume-send failed [fd=").$(context.getFd())
                    .$(", requestId=").$(state.getStreamingRequestId())
                    .$(", error=").$(t).I$();
            long failedRequestId = state.getStreamingRequestId();
            state.endStreaming();
            try {
                sendQueryError(context, state, failedRequestId, mapErrorStatus(t),
                        t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage());
            } catch (PeerDisconnectedException | PeerIsSlowToReadException sendFail) {
                throw sendFail;
            } catch (Throwable ignored) {
            }
        }
    }

    /**
     * Returns {@code true} when a compiled query should stream result rows back
     * to the client. {@code SELECT} and {@code EXPLAIN} always do; {@code
     * PSEUDO_SELECT} only when the compiler produced a cursor (it returns null
     * for synchronous variants like certain {@code COPY} forms).
     */
    private static boolean isStreamingType(short type, CompiledQuery cq) {
        if (type == CompiledQuery.SELECT || type == CompiledQuery.EXPLAIN) {
            return true;
        }
        return type == CompiledQuery.PSEUDO_SELECT && cq.getRecordCursorFactory() != null;
    }

    private static int parseClientMaxVersion(HttpRequestHeader requestHeader) {
        Utf8Sequence maxVersionHeader = requestHeader.getHeader(QwpWebSocketHttpProcessor.HEADER_X_QWP_MAX_VERSION);
        if (maxVersionHeader == null) {
            return QwpConstants.VERSION_1;
        }
        int parsed = Numbers.parseNonNegativeIntQuiet(maxVersionHeader);
        return parsed >= QwpConstants.VERSION_1 ? parsed : QwpConstants.VERSION_1;
    }

    /**
     * Patches the WebSocket frame header into the reserved 10-byte prefix and
     * memmoves the QWP payload left if the actual header is shorter. Flushes
     * via {@link HttpRawSocket#send(int)}.
     */
    private static void sendFrame(HttpRawSocket rawSocket, long bufAddr, long qwpStart, int qwpSize)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        int wsHeaderSize = WebSocketFrameWriter.headerSize(qwpSize, false);
        long frameStart = qwpStart - wsHeaderSize;
        if (frameStart != bufAddr) {
            // memmove QWP bytes so the frame abuts offset 0
            Unsafe.getUnsafe().copyMemory(qwpStart, bufAddr + wsHeaderSize, qwpSize);
        }
        WebSocketFrameWriter.writeBinaryFrameHeader(bufAddr, qwpSize);
        rawSocket.send(wsHeaderSize + qwpSize);
    }

    /**
     * Detaches the streaming factory from {@code state} and puts it into the
     * compile cache keyed by the query's SQL text. Idempotent: safe to call
     * even when the factory was already detached (no-op), or when the SQL
     * text is null (drops the factory via {@link Misc#free}). Called on the
     * successful-completion paths only -- error/cancel paths continue to free
     * the factory via the normal {@link QwpEgressProcessorState#endStreaming}
     * route so a cursor that threw never seeds the cache with a poisoned factory.
     */
    private void cacheStreamingFactoryIfAvailable(QwpEgressProcessorState state) {
        RecordCursorFactory factory = state.detachStreamingFactory();
        if (factory == null) {
            return;
        }
        CharSequence sqlText = state.getStreamingSqlText();
        if (sqlText == null) {
            // Factory was detached but we have no SQL key to cache against.
            // Shouldn't happen in the normal flow; belt-and-braces free.
            Misc.free(factory);
            return;
        }
        selectCache.put(sqlText, factory);
    }

    private void dispatchEgressMessage(
            HttpConnectionContext context,
            QwpEgressProcessorState state,
            long payload,
            int length
    ) throws ServerDisconnectException, PeerDisconnectedException, PeerIsSlowToReadException {
        if (length < 1) {
            LOG.error().$("Egress empty binary frame [fd=").$(context.getFd()).I$();
            throw ServerDisconnectException.INSTANCE;
        }
        byte msgKind = state.getDecoder().peekMsgKind(payload);
        switch (msgKind) {
            case QwpEgressMsgKind.QUERY_REQUEST -> handleQueryRequest(context, state, payload, length);
            case QwpEgressMsgKind.CANCEL -> handleCancel(context, state, payload, length);
            case QwpEgressMsgKind.CREDIT -> handleCredit(context, state, payload, length);
            default -> {
                LOG.error().$("Egress unknown msg_kind [fd=").$(context.getFd())
                        .$(", kind=0x").$(Integer.toHexString(msgKind & 0xFF)).I$();
                throw ServerDisconnectException.INSTANCE;
            }
        }
    }

    /**
     * Runs a non-SELECT {@link CompiledQuery} synchronously and replies with an
     * {@code EXEC_DONE}. The HTTP worker blocks until the operation future
     * completes -- same shape that {@code JsonQueryProcessor} uses, minus its
     * async-retry dance (egress doesn't have an HTTP-level retry hook so a
     * bounded await is pointless). Throws so the caller's catch maps it to a
     * {@code QUERY_ERROR}.
     */
    private void executeNonSelect(
            HttpConnectionContext context,
            QwpEgressProcessorState state,
            SqlExecutionContextImpl sqlCtx,
            CompiledQuery cq,
            long requestId
    ) throws Exception {
        final short type = cq.getType();
        long rowsAffected = 0;
        switch (type) {
            case CompiledQuery.INSERT:
            case CompiledQuery.INSERT_AS_SELECT: {
                try (InsertOperation op = cq.popInsertOperation()) {
                    try (OperationFuture fut = op.execute(sqlCtx)) {
                        fut.await();
                        rowsAffected = fut.getAffectedRowsCount();
                    }
                }
                break;
            }
            case CompiledQuery.UPDATE: {
                try (OperationFuture fut = cq.execute(sqlCtx, state.getEventSubSequence(), true)) {
                    fut.await();
                    rowsAffected = fut.getAffectedRowsCount();
                }
                break;
            }
            case CompiledQuery.ALTER: {
                try (OperationFuture fut = cq.execute(state.getEventSubSequence())) {
                    fut.await();
                }
                break;
            }
            case CompiledQuery.DROP:
            case CompiledQuery.CREATE_TABLE:
            case CompiledQuery.CREATE_TABLE_AS_SELECT:
            case CompiledQuery.CREATE_MAT_VIEW:
            case CompiledQuery.CREATE_VIEW: {
                try (
                        Operation op = cq.getOperation();
                        OperationFuture fut = op.execute(sqlCtx, state.getEventSubSequence())
                ) {
                    fut.await();
                }
                break;
            }
            case CompiledQuery.COPY_REMOTE: {
                // Ingress `/write/v4` is the supported channel for bulk load.
                cq.closeAllButSelect();
                sendQueryError(context, state, requestId, QwpConstants.STATUS_PARSE_ERROR,
                        "COPY ... FROM is not supported on egress");
                return;
            }
            default: {
                // Parse-time-executed statements (TRUNCATE, RENAME TABLE, SET,
                // VACUUM, CHECKPOINT, BEGIN / COMMIT / ROLLBACK, DEALLOCATE,
                // TABLE_RESUME / SUSPEND / SET_TYPE, CREATE/ALTER USER, etc.)
                // need no further execute -- the compiler already did the work.
                rowsAffected = cq.getAffectedRowsCount();
                break;
            }
        }
        sendExecDone(context, state, requestId, type, rowsAffected);
    }

    private void finalizeHandshake(HttpConnectionContext context, QwpEgressProcessorState state) {
        state.setWsHandshakeSent(true);
        state.setHandshakeFlushPending(false);
        state.setPendingHandshakeBytes(0);
        LOG.info().$("Egress WebSocket handshake sent [fd=").$(context.getFd())
                .$(", qwpVersion=").$(state.getNegotiatedVersion() & 0xFF).I$();
        context.switchProtocol();
    }

    // Egress message dispatch and query execution

    /**
     * CANCEL handler: decodes the target {@code requestId} and, if it matches
     * the currently streaming query, flags the state so {@code streamResults}
     * aborts between batches with a {@code QUERY_ERROR} (status
     * {@code STATUS_CANCELLED}). Cancels against non-matching or absent queries
     * are logged and dropped.
     * <p>
     * Known limitation (not yet fixed): the IO dispatcher registers each fd for
     * a single operation (read OR write). While a streaming query is parked on
     * write backpressure, inbound CANCEL frames queue in the kernel recv buffer
     * but this handler is only invoked once write completes. That makes
     * mid-stream CANCEL effectively ineffective over slow consumers today -- the
     * query finishes before CANCEL is seen. Fixing it requires registering for
     * both read and write during streaming, which is a dispatcher-level change.
     * The in-place plumbing (flag + streamResults check + STATUS_CANCELLED
     * mapping) is ready for that fix.
     */
    private void handleCancel(HttpConnectionContext context, QwpEgressProcessorState state, long payload, int length) {
        try {
            long targetRequestId = state.getDecoder().decodeCancel(payload, length);
            if (state.isStreamingActive() && state.getStreamingRequestId() == targetRequestId) {
                state.markStreamingCancelRequested();
                LOG.info().$("Egress CANCEL accepted [fd=").$(context.getFd())
                        .$(", requestId=").$(targetRequestId).I$();
            } else {
                LOG.debug().$("Egress CANCEL for unknown query [fd=").$(context.getFd())
                        .$(", targetRequestId=").$(targetRequestId)
                        .$(", currentRequestId=").$(state.isStreamingActive() ? state.getStreamingRequestId() : -1L)
                        .I$();
            }
        } catch (QwpParseException e) {
            LOG.error().$("Egress CANCEL malformed [fd=").$(context.getFd())
                    .$(", error=").$(e.getFlyweightMessage()).I$();
        }
    }

    private void handleClose(HttpConnectionContext context, long payload, int length) {
        int closeCode = -1;
        if (length >= 2) {
            int high = Unsafe.getUnsafe().getByte(payload) & 0xFF;
            int low = Unsafe.getUnsafe().getByte(payload + 1) & 0xFF;
            closeCode = (high << 8) | low;
        }
        LOG.info().$("Egress WebSocket close [fd=").$(context.getFd()).$(", code=").$(closeCode).I$();
        try {
            HttpRawSocket rawSocket = context.getRawResponseSocket();
            int written = WebSocketFrameWriter.writeCloseFrame(
                    rawSocket.getBufferAddress(),
                    rawSocket.getBufferSize(),
                    WebSocketCloseCode.NORMAL_CLOSURE,
                    null);
            if (written > 0) {
                rawSocket.send(written);
            }
        } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
            // Best effort
        }
    }

    /**
     * CREDIT handler: the client advertises {@code additional_bytes} of
     * send-ahead budget for {@code request_id}. If the target matches the
     * currently streaming query, we add it to {@code streamingCreditRemaining}
     * and, if the stream is credit-suspended, resume it by re-entering
     * {@code streamResults}. Re-entering inline (same thread, same processor)
     * is safe: the suspended state left nothing mid-batch -- it exited cleanly
     * at the top of the loop.
     */
    private void handleCredit(HttpConnectionContext context, QwpEgressProcessorState state, long payload, int length)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        try {
            long targetRequestId = Unsafe.getUnsafe().getLong(payload + 1);
            long additional = state.getDecoder().decodeCredit(payload, length);
            if (additional <= 0) {
                LOG.error().$("Egress CREDIT rejected [fd=").$(context.getFd())
                        .$(", requestId=").$(targetRequestId)
                        .$(", additional=").$(additional).I$();
                return;
            }
            if (!state.isStreamingActive() || state.getStreamingRequestId() != targetRequestId) {
                LOG.debug().$("Egress CREDIT for unknown query [fd=").$(context.getFd())
                        .$(", targetRequestId=").$(targetRequestId).I$();
                return;
            }
            state.addStreamingCredit(additional);
            if (!state.isStreamingCreditSuspended()) {
                // Stream isn't parked -- just banked the credit for future batches.
                return;
            }
            LOG.debug().$("Egress CREDIT resume [fd=").$(context.getFd())
                    .$(", requestId=").$(targetRequestId)
                    .$(", added=").$(additional)
                    .$(", remaining=").$(state.getStreamingCreditRemaining()).I$();
            state.clearStreamingCreditSuspended();
            try {
                streamResults(context, state);
            } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
                throw e;
            } catch (Throwable t) {
                LOG.error().$("Egress CREDIT resume failed [fd=").$(context.getFd())
                        .$(", requestId=").$(targetRequestId).$(", error=").$(t).I$();
                state.endStreaming();
                try {
                    sendQueryError(context, state, targetRequestId, mapErrorStatus(t),
                            t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage());
                } catch (Throwable ignored) {
                }
            }
        } catch (QwpParseException e) {
            LOG.error().$("Egress CREDIT malformed [fd=").$(context.getFd())
                    .$(", error=").$(e.getFlyweightMessage()).I$();
        }
    }

    private void handlePing(HttpConnectionContext context, long payload, int length) {
        try {
            HttpRawSocket rawSocket = context.getRawResponseSocket();
            int frameSize = WebSocketFrameWriter.headerSize(length, false) + length;
            if (frameSize <= rawSocket.getBufferSize()) {
                int written = WebSocketFrameWriter.writePongFrame(rawSocket.getBufferAddress(), payload, length);
                rawSocket.send(written);
            }
        } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
            LOG.debug().$("Egress failed to send pong [fd=").$(context.getFd()).I$();
        }
    }

    private void handleQueryRequest(
            HttpConnectionContext context,
            QwpEgressProcessorState state,
            long payload,
            int length
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        QwpEgressRequestDecoder decoder = state.getDecoder();
        long requestId = 0;
        boolean streamingHandedOff = false;
        RecordCursorFactory factory = null;
        RecordCursor cursor = null;
        PageFrameCursor pageFrameCursor = null;
        // Phase 1 supports a single in-flight query per connection. A second QUERY_REQUEST
        // arriving while the first is still streaming (e.g., the send side is parked on
        // PeerIsSlowToReadException) would overwrite streamingFactory/streamingCursor in
        // beginStreaming without freeing the previous ones. Reject early, before we touch
        // bind variables or the SQL compiler. The requestId lives at a fixed offset
        // (msg_kind + requestId), so we can peek it without invoking the full decoder.
        if (state.isStreamingActive()) {
            if (length >= 9) {
                requestId = Unsafe.getUnsafe().getLong(payload + 1);
            }
            sendQueryError(context, state, requestId, QwpConstants.STATUS_PARSE_ERROR,
                    "Phase 1 egress supports a single in-flight query per connection");
            return;
        }
        try {
            decoder.decodeQueryRequest(payload, length, state.getBindVariableService());
            requestId = decoder.requestId;
            LOG.info().$("Egress QUERY_REQUEST [fd=").$(context.getFd())
                    .$(", requestId=").$(requestId)
                    .$(", sqlLen=").$(decoder.sql.length()).I$();

            SqlExecutionContextImpl sqlCtx = context.getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
            sqlCtx.with(
                    context.getSecurityContext(),
                    state.getBindVariableService(),
                    null,
                    context.getFd(),
                    null
            );
            sqlCtx.initNow();

            // Retry-once loop: a factory returned by the compile cache may have a
            // stale TableReader reference if the table was dropped+recreated after
            // the factory was compiled (matching by SQL text alone; tableId and
            // metadataVersion don't survive). Detected by
            // {@link TableReferenceOutOfDateException} on cursor open. We drop the
            // stale factory and recompile. Two consecutive occurrences means the
            // table changed mid-recompile -- rare and probably indicates an abusive
            // DDL pattern; propagate as a normal error.
            int attempts = 0;
            while (true) {
                attempts++;
                try {
                    // Cache lookup only on first attempt. Retry always recompiles.
                    if (attempts == 1) {
                        factory = selectCache.poll(decoder.sql);
                    }
                    if (factory == null) {
                        try (SqlCompiler compiler = engine.getSqlCompiler()) {
                            CompiledQuery cq = compiler.compile(decoder.sql, sqlCtx);
                            short type = cq.getType();
                            // Non-SELECT (DDL / INSERT / UPDATE / parse-time-executed) -- route to the
                            // synchronous exec path which awaits the operation and replies with an
                            // EXEC_DONE carrying the op type + rows affected. Non-SELECTs are never
                            // cached: they mutate state and can't be reused as plans.
                            if (!isStreamingType(type, cq)) {
                                executeNonSelect(context, state, sqlCtx, cq, requestId);
                                return;
                            }
                            factory = cq.getRecordCursorFactory();
                        }
                    }
                    RecordMetadata metadata = factory.getMetadata();
                    int columnCount = metadata.getColumnCount();
                    ObjList<QwpEgressColumnDef> columnDefs = state.borrowColumnDefs(columnCount);
                    for (int i = 0; i < columnCount; i++) {
                        columnDefs.getQuick(i).of(metadata.getColumnName(i), metadata.getColumnType(i));
                    }
                    int schemaId = state.findOrAllocateSchemaId(columnDefs);
                    if (schemaId == QwpEgressProcessorState.SCHEMA_ID_EXHAUSTED) {
                        // The connection has registered DEFAULT_MAX_SCHEMAS_PER_CONNECTION distinct
                        // schemas already; this query's shape is new and would need a fresh id the
                        // client would reject. Surface a controlled error and free the factory so
                        // the connection stays usable for queries against schemas already cached.
                        Misc.free(factory);
                        factory = null;
                        sendQueryError(context, state, requestId, QwpEgressMsgKind.STATUS_LIMIT_EXCEEDED,
                                "connection schema cache exhausted ("
                                        + QwpConstants.DEFAULT_MAX_SCHEMAS_PER_CONNECTION
                                        + " distinct schemas); reconnect to reset");
                        return;
                    }
                    boolean schemaAlreadyKnown = state.wasLastSchemaIdReuse();
                    // Prefer the PageFrameCursor fast path when the factory supports it: it
                    // hands us flat column addresses per frame and lets the SYMBOL fast path
                    // resolve dict keys via PageFrameMemoryRecord.getInt. Factories that don't
                    // support it (filtered/joined/grouped queries) keep the existing
                    // RecordCursor path without change.
                    if (factory.supportsPageFrameCursor()) {
                        int order = factory.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_BACKWARD
                                ? PartitionFrameCursorFactory.ORDER_DESC
                                : PartitionFrameCursorFactory.ORDER_ASC;
                        pageFrameCursor = factory.getPageFrameCursor(sqlCtx, order);
                    }
                    if (pageFrameCursor != null) {
                        // Streaming mode asks the cursor to release page cache pages
                        // after reading, so a 10M-row scan doesn't evict the server's
                        // working set. Same hint used by the parquet exporter.
                        pageFrameCursor.setStreamingMode(true);
                        state.beginStreamingPageFrame(requestId, factory, pageFrameCursor,
                                columnCount, schemaId, schemaAlreadyKnown, decoder.initialCredit, decoder.sql);
                    } else {
                        cursor = factory.getCursor(sqlCtx);
                        state.beginStreaming(requestId, factory, cursor,
                                columnCount, schemaId, schemaAlreadyKnown, decoder.initialCredit, decoder.sql);
                    }
                    streamingHandedOff = true;     // ownership of factory + cursor passed to state
                    break; // setup completed; fall through to streamResults outside the loop
                } catch (TableReferenceOutOfDateException e) {
                    // Free any partially-acquired resources from this attempt. After
                    // beginStreaming{,PageFrame} they'd be owned by state, but the
                    // exception fires BEFORE that (on getCursor / getPageFrameCursor),
                    // so we still own them here.
                    pageFrameCursor = Misc.free(pageFrameCursor);
                    cursor = Misc.free(cursor);
                    factory = Misc.free(factory);
                    if (attempts >= 2) {
                        // Fresh compile also raced with a DDL -- unusual, propagate.
                        throw e;
                    }
                    LOG.info().$("Egress cached factory stale, recompiling [fd=").$(context.getFd())
                            .$(", requestId=").$(requestId)
                            .$(", error=").$safe(e.getFlyweightMessage()).I$();
                }
            }
            // Streaming may complete here (cursor short and fast), or throw PeerIsSlowToReadException
            // (we'll be re-entered via resumeSend) or another exception (handled below).
            streamResults(context, state);
        } catch (PeerDisconnectedException e) {
            // PDX can only arrive from streamResults (the one network call in this
            // try block). By then streamingHandedOff is true, so state owns the
            // cursor/factory and the only cleanup we owe is endStreaming.
            if (state.isStreamingActive()) {
                state.endStreaming();
            }
            throw e;
        } catch (PeerIsSlowToReadException e) {
            // Streaming parked. State retains the cursor for resumeSend to continue.
            LOG.debug().$("Egress streaming parked (slow peer) [fd=").$(context.getFd())
                    .$(", requestId=").$(requestId)
                    .$(", batchSeq=").$(state.getStreamingBatchSeq())
                    .$(", rowsEmitted=").$(state.getStreamingRowsEmitted())
                    .I$();
            throw e;
        } catch (Throwable e) {
            LOG.error().$("Egress query failed [fd=").$(context.getFd())
                    .$(", requestId=").$(requestId)
                    .$(", error=").$(e).I$();
            if (state.isStreamingActive()) {
                state.endStreaming();
            } else if (!streamingHandedOff) {
                // Free anything we allocated before handing ownership to the state. Without this,
                // an exception between factory.getCursor() and beginStreaming() (e.g., OOM, table
                // metadata error, borrowColumnDefs growth failure) leaks the factory and cursor.
                Misc.free(cursor);
                Misc.free(pageFrameCursor);
                Misc.free(factory);
            }
            byte status = mapErrorStatus(e);
            try {
                sendQueryError(context, state, requestId, status,
                        e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
            } catch (PeerDisconnectedException | PeerIsSlowToReadException sendFail) {
                throw sendFail;
            } catch (Throwable ignored) {
                // Best-effort error report; drop.
            }
        }
    }

    private void handleWebSocketFrame(
            HttpConnectionContext context,
            QwpEgressProcessorState state,
            int opcode,
            boolean fin,
            long payload,
            int length
    ) throws ServerDisconnectException, PeerDisconnectedException, PeerIsSlowToReadException {
        switch (opcode) {
            case WebSocketOpcode.BINARY -> {
                if (!fin) {
                    LOG.error().$("Egress fragmented BINARY frame rejected [fd=").$(context.getFd()).I$();
                    throw ServerDisconnectException.INSTANCE;
                }
                dispatchEgressMessage(context, state, payload, length);
            }
            case WebSocketOpcode.CONTINUATION -> {
                LOG.error().$("Egress unexpected CONTINUATION frame [fd=").$(context.getFd()).I$();
                throw ServerDisconnectException.INSTANCE;
            }
            case WebSocketOpcode.TEXT -> {
                LOG.error().$("Egress TEXT frame rejected (binary only) [fd=").$(context.getFd()).I$();
                throw ServerDisconnectException.INSTANCE;
            }
            case WebSocketOpcode.PING -> handlePing(context, payload, length);
            case WebSocketOpcode.PONG -> LOG.debug().$("Egress pong [fd=").$(context.getFd()).I$();
            case WebSocketOpcode.CLOSE -> {
                handleClose(context, payload, length);
                throw ServerDisconnectException.INSTANCE;
            }
            default -> LOG.debug().$("Egress unknown opcode [fd=").$(context.getFd()).$(", opcode=").$(opcode).I$();
        }
    }

    private int negotiateQwpVersion(HttpRequestHeader requestHeader, long fd) {
        int clientMaxVersion = parseClientMaxVersion(requestHeader);
        int negotiated = Math.min(clientMaxVersion, QwpConstants.MAX_SUPPORTED_VERSION);
        Utf8Sequence clientId = requestHeader.getHeader(QwpWebSocketHttpProcessor.HEADER_X_QWP_CLIENT_ID);
        if (clientId != null) {
            LOG.info().$("Egress QWP version negotiated [fd=").$(fd)
                    .$(", clientId=").$(clientId)
                    .$(", clientMax=").$(clientMaxVersion)
                    .$(", negotiated=").$(negotiated).I$();
        } else {
            LOG.info().$("Egress QWP version negotiated [fd=").$(fd)
                    .$(", clientMax=").$(clientMaxVersion)
                    .$(", negotiated=").$(negotiated).I$();
        }
        return negotiated;
    }

    private void processWebSocketFrames(HttpConnectionContext context, QwpEgressProcessorState state, long buffer, int bufferLen)
            throws ServerDisconnectException, PeerDisconnectedException, PeerIsSlowToReadException {
        long bufferEnd = buffer + bufferLen;
        long pos = buffer;
        try {
            while (pos < bufferEnd) {
                frameParser.reset();
                int consumed = frameParser.parse(pos, bufferEnd);

                if (frameParser.getState() == WebSocketFrameParser.STATE_ERROR) {
                    LOG.error().$("Egress WebSocket frame error [fd=").$(context.getFd())
                            .$(", code=").$(frameParser.getErrorCode()).I$();
                    throw ServerDisconnectException.INSTANCE;
                }
                if (frameParser.getState() == WebSocketFrameParser.STATE_NEED_PAYLOAD) {
                    long totalFrameSize = frameParser.getHeaderSize() + frameParser.getPayloadLength();
                    if (totalFrameSize > recvBufferSize) {
                        LOG.error().$("Egress WebSocket frame too large [fd=").$(context.getFd())
                                .$(", payloadLength=").$(frameParser.getPayloadLength())
                                .$(", bufferSize=").$(recvBufferSize).I$();
                        throw ServerDisconnectException.INSTANCE;
                    }
                    break;
                }
                if (consumed == 0 || frameParser.getState() == WebSocketFrameParser.STATE_NEED_MORE) {
                    break;
                }

                int opcode = frameParser.getOpcode();
                long payloadPtr = pos + frameParser.getHeaderSize();
                int payloadLen = (int) frameParser.getPayloadLength();
                if (frameParser.isMasked()) {
                    frameParser.unmaskPayload(payloadPtr, payloadLen);
                }
                pos += consumed;
                handleWebSocketFrame(context, state, opcode, frameParser.isFin(), payloadPtr, payloadLen);
            }
        } finally {
            int remaining = (int) (bufferEnd - pos);
            if (remaining > 0 && pos > buffer) {
                Unsafe.getUnsafe().copyMemory(pos, buffer, remaining);
            }
            state.setRecvBufferLen(remaining);
        }
    }

    // Outbound frame serialisation

    /**
     * Ack for a non-SELECT query that completed successfully. Body is small and
     * always fits in the send buffer's header reservation plus a handful of bytes,
     * so this is a one-shot send -- no chunking.
     */
    private void sendExecDone(
            HttpConnectionContext context,
            QwpEgressProcessorState state,
            long requestId,
            short opType,
            long rowsAffected
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufAddr = rawSocket.getBufferAddress();
        long qwpStart = bufAddr + QwpEgressFrameWriter.WS_HEADER_RESERVATION;
        long bodyStart = QwpEgressFrameWriter.writeMessageHeader(
                qwpStart, state.getNegotiatedVersion(), (byte) 0, 0, 0 /* payload len patched */);
        long bodyEnd = QwpEgressFrameWriter.writeExecDone(bodyStart, requestId, opType, rowsAffected);
        int qwpSize = (int) (bodyEnd - qwpStart);
        int qwpPayloadLen = qwpSize - QwpConstants.HEADER_SIZE;
        QwpEgressFrameWriter.patchPayloadLength(qwpStart, qwpPayloadLen);
        sendFrame(rawSocket, bufAddr, qwpStart, qwpSize);
    }

    private void sendQueryError(
            HttpConnectionContext context,
            QwpEgressProcessorState state,
            long requestId,
            byte status,
            CharSequence msg
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufAddr = rawSocket.getBufferAddress();
        int bufSize = rawSocket.getBufferSize();
        long qwpStart = bufAddr + QwpEgressFrameWriter.WS_HEADER_RESERVATION;
        long bodyStart = QwpEgressFrameWriter.writeMessageHeader(
                qwpStart, state.getNegotiatedVersion(), (byte) 0, 0, 0);
        // Cap UTF-8 encoding so it can't overflow either the wire u16 length OR the send buffer.
        // Reserve a few bytes for the header + WS framing already accounted for.
        int msgCap = Math.min(0xFFFF, bufSize - QwpEgressFrameWriter.WS_HEADER_RESERVATION
                - QwpConstants.HEADER_SIZE - 12 /* prelude bytes */);
        long bodyEnd = QwpEgressFrameWriter.writeQueryError(bodyStart, requestId, status, msg, msgCap);
        int qwpSize = (int) (bodyEnd - qwpStart);
        int qwpPayloadLen = qwpSize - QwpConstants.HEADER_SIZE;
        QwpEgressFrameWriter.patchPayloadLength(qwpStart, qwpPayloadLen);
        sendFrame(rawSocket, bufAddr, qwpStart, qwpSize);
    }

    /**
     * Writes one RESULT_BATCH frame into the rawSocket buffer and sends it.
     * <p>
     * Layout inside rawSocket buffer:
     * <pre>
     *   [0 .. 10)                 WS header reservation
     *   [10 .. 10 + qwpSize)      QWP message header + prelude + table block
     * </pre>
     * After computing {@code qwpSize}, the method picks the real WS header size
     * and may memmove the QWP bytes left so the wire frame abuts offset 0
     * (which is what {@link HttpRawSocket#send(int)} flushes).
     *
     * @return the WS payload size (QWP bytes) actually written -- used by the
     * credit bookkeeping to debit the stream's remaining budget.
     */
    private int sendResultBatch(
            HttpConnectionContext context,
            QwpEgressProcessorState state,
            long requestId,
            long batchSeq,
            QwpResultBatchBuffer batchBuffer,
            long schemaId,
            boolean writeFullSchema
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        // Asserts the caller bumped streamingBatchSeq (via state.onStreamingBatchSent)
        // BEFORE reaching the socket. See QwpEgressProcessorState.consumeBatchSeqCommit.
        state.consumeBatchSeqCommit();
        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufAddr = rawSocket.getBufferAddress();
        int bufSize = rawSocket.getBufferSize();
        if (bufSize < QwpEgressFrameWriter.WS_HEADER_RESERVATION + QwpConstants.HEADER_SIZE + 32) {
            throw HttpException.instance("egress send buffer too small");
        }
        long qwpStart = bufAddr + QwpEgressFrameWriter.WS_HEADER_RESERVATION;
        // FLAG_DELTA_SYMBOL_DICT and FLAG_GORILLA are always set on RESULT_BATCH
        // frames. The delta section sits AFTER the prelude (msg_kind + request_id
        // + batch_seq) so the I/O thread's dispatch (which peeks msg_kind at
        // HEADER_SIZE) keeps working unchanged. SYMBOL columns inside the table
        // block are stripped of their per-column dict; indices reference the
        // connection dict. TIMESTAMP / TIMESTAMP_NANOS / DATE columns carry a
        // 1-byte encoding discriminator that the decoder consumes.
        long bodyStart = QwpEgressFrameWriter.writeMessageHeader(
                qwpStart, state.getNegotiatedVersion(),
                (byte) (QwpConstants.FLAG_DELTA_SYMBOL_DICT | QwpConstants.FLAG_GORILLA),
                1, 0 /* payload len patched */);
        long preludeEnd = QwpEgressFrameWriter.writeResultBatchPrelude(bodyStart, requestId, batchSeq);
        long bufLimit = bufAddr + bufSize;
        int deltaSize = batchBuffer.emitDeltaSection(preludeEnd, bufLimit);
        if (deltaSize < 0) {
            throw HttpException.instance("egress: batch too large for send buffer");
        }
        int tableBlockSize = batchBuffer.emitTableBlock(preludeEnd + deltaSize, bufLimit, schemaId, writeFullSchema);
        if (tableBlockSize < 0) {
            throw HttpException.instance("egress: batch too large for send buffer");
        }
        long qwpEnd = preludeEnd + deltaSize + tableBlockSize;

        // Optional zstd compression of the post-prelude body. The prelude stays
        // raw so the client I/O thread can peek msg_kind + requestId + batchSeq
        // for routing without paying the decompress cost. FLAG_ZSTD is only set
        // when compression actually shrinks the body; tiny batches that expand
        // under zstd's header overhead ship raw to stay within the send buffer
        // and avoid waste.
        if (state.getCompressionCodec() == QwpConstants.COMPRESSION_ZSTD) {
            int bodyLen = (int) (qwpEnd - preludeEnd);
            if (bodyLen > 0) {
                // zstd's compressed-size bound is srcLen + (srcLen >> 8) + 64;
                // pad to 128 for safety across libzstd versions.
                int scratchCap = bodyLen + (bodyLen >> 8) + 128;
                long scratch = state.zstdCompressScratch(scratchCap);
                long compLen = Zstd.compress(state.zstdCCtx(), preludeEnd, bodyLen, scratch, scratchCap);
                if (compLen > 0 && compLen < bodyLen) {
                    Vect.memcpy(preludeEnd, scratch, compLen);
                    qwpEnd = preludeEnd + compLen;
                    // Patch FLAG_ZSTD into the header's flags byte. writeMessageHeader
                    // already wrote FLAG_DELTA_SYMBOL_DICT | FLAG_GORILLA; we OR in
                    // the zstd bit without re-serialising the whole header.
                    long flagsAddr = qwpStart + QwpConstants.HEADER_OFFSET_FLAGS;
                    byte flags = Unsafe.getUnsafe().getByte(flagsAddr);
                    Unsafe.getUnsafe().putByte(flagsAddr, (byte) (flags | QwpConstants.FLAG_ZSTD));
                } else if (compLen < 0) {
                    LOG.error().$("zstd compress error [fd=").$(context.getFd())
                            .$(", code=").$(compLen).I$();
                    // Fall through and ship the batch raw; the flag stays off.
                }
                // When compLen >= bodyLen the batch is shipped raw. No memcpy
                // needed because preludeEnd..qwpEnd still holds the original
                // uncompressed bytes (compress wrote only into the scratch).
            }
        }

        int qwpSize = (int) (qwpEnd - qwpStart);
        int qwpPayloadLen = qwpSize - QwpConstants.HEADER_SIZE;
        QwpEgressFrameWriter.patchPayloadLength(qwpStart, qwpPayloadLen);

        sendFrame(rawSocket, bufAddr, qwpStart, qwpSize);
        return qwpSize;
    }

    /**
     * Composes RESULT_BATCH immediately followed by RESULT_END into the rawSocket
     * buffer and ships both frames in a single {@link HttpRawSocket#send(int)}
     * call. Used on the cursor-exhausted branch of {@link #streamResults} so a
     * short query ends in one syscall / one TCP segment rather than two.
     * <p>
     * Falls back to two sends when the batch is too large to fit both frames
     * in the send buffer. The worst-case RESULT_END footprint is small
     * (~41 bytes incl. reservation) so the fallback only triggers for batches
     * that already fill the buffer to within tens of bytes of capacity.
     * <p>
     * Returns the RESULT_BATCH payload size (not including RESULT_END), for the
     * caller's credit-bookkeeping debit.
     */
    private int sendResultBatchAndEnd(
            HttpConnectionContext context,
            QwpEgressProcessorState state,
            long requestId,
            long batchSeq,
            QwpResultBatchBuffer batchBuffer,
            long schemaId,
            boolean writeFullSchema,
            long totalRows
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        state.consumeBatchSeqCommit();
        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufAddr = rawSocket.getBufferAddress();
        int bufSize = rawSocket.getBufferSize();
        if (bufSize < QwpEgressFrameWriter.WS_HEADER_RESERVATION + QwpConstants.HEADER_SIZE + 32) {
            throw HttpException.instance("egress send buffer too small");
        }
        // Build RESULT_BATCH into [WS_HEADER_RESERVATION .. qwp1End), identical
        // shape to sendResultBatch. Deliberately duplicated because the shared
        // structure would need a multi-parameter callback to vary the final
        // framing step, which is harder to follow than the copy.
        long qwp1Start = bufAddr + QwpEgressFrameWriter.WS_HEADER_RESERVATION;
        long bodyStart = QwpEgressFrameWriter.writeMessageHeader(
                qwp1Start, state.getNegotiatedVersion(),
                (byte) (QwpConstants.FLAG_DELTA_SYMBOL_DICT | QwpConstants.FLAG_GORILLA),
                1, 0 /* payload len patched */);
        long preludeEnd = QwpEgressFrameWriter.writeResultBatchPrelude(bodyStart, requestId, batchSeq);
        long bufLimit = bufAddr + bufSize;
        int deltaSize = batchBuffer.emitDeltaSection(preludeEnd, bufLimit);
        if (deltaSize < 0) {
            throw HttpException.instance("egress: batch too large for send buffer");
        }
        int tableBlockSize = batchBuffer.emitTableBlock(preludeEnd + deltaSize, bufLimit, schemaId, writeFullSchema);
        if (tableBlockSize < 0) {
            throw HttpException.instance("egress: batch too large for send buffer");
        }
        long qwp1End = preludeEnd + deltaSize + tableBlockSize;

        if (state.getCompressionCodec() == QwpConstants.COMPRESSION_ZSTD) {
            int bodyLen = (int) (qwp1End - preludeEnd);
            if (bodyLen > 0) {
                int scratchCap = bodyLen + (bodyLen >> 8) + 128;
                long scratch = state.zstdCompressScratch(scratchCap);
                long compLen = Zstd.compress(state.zstdCCtx(), preludeEnd, bodyLen, scratch, scratchCap);
                if (compLen > 0 && compLen < bodyLen) {
                    Vect.memcpy(preludeEnd, scratch, compLen);
                    qwp1End = preludeEnd + compLen;
                    long flagsAddr = qwp1Start + QwpConstants.HEADER_OFFSET_FLAGS;
                    byte flags = Unsafe.getUnsafe().getByte(flagsAddr);
                    Unsafe.getUnsafe().putByte(flagsAddr, (byte) (flags | QwpConstants.FLAG_ZSTD));
                } else if (compLen < 0) {
                    LOG.error().$("zstd compress error [fd=").$(context.getFd())
                            .$(", code=").$(compLen).I$();
                }
            }
        }

        int qwp1Size = (int) (qwp1End - qwp1Start);
        QwpEgressFrameWriter.patchPayloadLength(qwp1Start, qwp1Size - QwpConstants.HEADER_SIZE);
        int ws1HeaderSize = WebSocketFrameWriter.headerSize(qwp1Size, false);
        int frame1Size = ws1HeaderSize + qwp1Size;

        // Worst-case RESULT_END footprint: WS reservation (10) + QWP header (12)
        // + msg_kind (1) + requestId (8) + finalSeq varint (<= 10) + totalRows
        // varint (<= 10) = 51 bytes. Checking upfront avoids a half-built second
        // frame that we'd have to rewind.
        final int resultEndWorstCase = QwpEgressFrameWriter.WS_HEADER_RESERVATION
                + QwpConstants.HEADER_SIZE + 1 + 8 + 10 + 10;
        if (frame1Size + resultEndWorstCase > bufSize) {
            // Batch fills the buffer: cannot coalesce. Fall back to two sends,
            // matching the pre-coalesce shape.
            if (qwp1Start - ws1HeaderSize != bufAddr) {
                Unsafe.getUnsafe().copyMemory(qwp1Start, bufAddr + ws1HeaderSize, qwp1Size);
            }
            WebSocketFrameWriter.writeBinaryFrameHeader(bufAddr, qwp1Size);
            rawSocket.send(frame1Size);
            state.endStreaming();
            sendResultEnd(context, state, requestId, batchSeq, totalRows);
            return qwp1Size;
        }

        // Shift RESULT_BATCH so it abuts offset 0 and write its WS header.
        if (qwp1Start - ws1HeaderSize != bufAddr) {
            Unsafe.getUnsafe().copyMemory(qwp1Start, bufAddr + ws1HeaderSize, qwp1Size);
        }
        WebSocketFrameWriter.writeBinaryFrameHeader(bufAddr, qwp1Size);

        // Compose RESULT_END right after frame1. Reserve WS_HEADER_RESERVATION
        // bytes of slack for its WS header, write the QWP payload, then shift
        // left to abut frame1's tail once we know the real WS header size.
        long qwp2Start = bufAddr + frame1Size + QwpEgressFrameWriter.WS_HEADER_RESERVATION;
        long body2Start = QwpEgressFrameWriter.writeMessageHeader(
                qwp2Start, state.getNegotiatedVersion(), (byte) 0, 0, 0 /* payload len patched */);
        long body2End = QwpEgressFrameWriter.writeResultEnd(body2Start, requestId, batchSeq, totalRows);
        int qwp2Size = (int) (body2End - qwp2Start);
        QwpEgressFrameWriter.patchPayloadLength(qwp2Start, qwp2Size - QwpConstants.HEADER_SIZE);
        int ws2HeaderSize = WebSocketFrameWriter.headerSize(qwp2Size, false);
        long frame2Start = bufAddr + frame1Size;
        if (qwp2Start != frame2Start + ws2HeaderSize) {
            Unsafe.getUnsafe().copyMemory(qwp2Start, frame2Start + ws2HeaderSize, qwp2Size);
        }
        WebSocketFrameWriter.writeBinaryFrameHeader(frame2Start, qwp2Size);

        // Release cursor/factory BEFORE the kernel gets the bytes. Otherwise the
        // client can observe RESULT_END and issue a DROP TABLE while we still
        // hold the TableReader. See the matching notes on the two-send paths.
        state.endStreaming();
        rawSocket.send(frame1Size + ws2HeaderSize + qwp2Size);
        return qwp1Size;
    }

    private void sendResultEnd(
            HttpConnectionContext context,
            QwpEgressProcessorState state,
            long requestId,
            long finalSeq,
            long totalRows
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufAddr = rawSocket.getBufferAddress();
        long qwpStart = bufAddr + QwpEgressFrameWriter.WS_HEADER_RESERVATION;
        long bodyStart = QwpEgressFrameWriter.writeMessageHeader(
                qwpStart, state.getNegotiatedVersion(), (byte) 0, 0, 0 /* payload len patched */);
        long bodyEnd = QwpEgressFrameWriter.writeResultEnd(bodyStart, requestId, finalSeq, totalRows);
        int qwpSize = (int) (bodyEnd - qwpStart);
        int qwpPayloadLen = qwpSize - QwpConstants.HEADER_SIZE;
        QwpEgressFrameWriter.patchPayloadLength(qwpStart, qwpPayloadLen);
        sendFrame(rawSocket, bufAddr, qwpStart, qwpSize);
    }

    /**
     * Re-entrant streaming loop. State (cursor, factory, columnDefs, batchSeq, schema-sent flag)
     * lives on {@link QwpEgressProcessorState} so that a parked send can be resumed in
     * {@link #resumeSend} without losing the iteration position.
     */
    private void streamResults(HttpConnectionContext context, QwpEgressProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        QwpResultBatchBuffer batchBuffer = state.getBatchBuffer();
        ObjList<QwpEgressColumnDef> columnDefs = state.borrowColumnDefs(state.getStreamingColumnCount());
        long requestId = state.getStreamingRequestId();
        int schemaId = state.getStreamingSchemaId();
        // Page-frame path is used when the factory supports it (typical full scans);
        // everything else comes through the RecordCursor path. Both feed the same
        // batchBuffer; the only difference is how we walk rows.
        final boolean pageFrame = state.isStreamingPageFrame();
        final RecordCursor cursor = pageFrame ? null : state.getStreamingCursor();

        while (true) {
            // CANCEL arriving while the query is streaming sets a flag on state.
            // We observe it between batches (not mid-batch -- Phase 1 doesn't plumb
            // a circuit breaker into the SQL layer) and abort with STATUS_CANCELLED.
            if (state.isStreamingCancelRequested()) {
                LOG.info().$("Egress streaming cancelled by client [fd=").$(context.getFd())
                        .$(", requestId=").$(requestId)
                        .$(", batchSeq=").$(state.getStreamingBatchSeq())
                        .$(", rowsEmitted=").$(state.getStreamingRowsEmitted())
                        .I$();
                state.endStreaming();
                sendQueryError(context, state, requestId, QwpEgressMsgKind.STATUS_CANCELLED, "cancelled by client");
                return;
            }
            // Credit-limited streams park when the client-advertised budget hits
            // zero. The next CREDIT frame replenishes via handleCredit and
            // re-enters streamResults to continue.
            if (state.isStreamingCreditLimited() && state.getStreamingCreditRemaining() <= 0) {
                LOG.debug().$("Egress streaming credit-suspended [fd=").$(context.getFd())
                        .$(", requestId=").$(requestId)
                        .$(", batchSeq=").$(state.getStreamingBatchSeq())
                        .I$();
                state.markStreamingCreditSuspended();
                return;
            }
            // Passing the symbol-table source lets the batch buffer pick up native
            // SymbolTables for SYMBOL columns, taking the getInt-based fast path.
            batchBuffer.beginBatch(columnDefs, state.getStreamingSymbolTableSource(), state.getConnSymbolDict());
            // Effective cap = server MAX clamped against any client preference
            // set during the handshake. Read once per batch so a later config
            // change (e.g. a hypothetical PER-QUERY knob) would not partially
            // apply within the inner loop.
            final int batchCap = state.getMaxBatchRows();
            int rowsThisBatch = 0;
            if (pageFrame) {
                // Columnar fast path: walk frames and hand each slice to the batch
                // buffer as a (frame, lo, hi) triple. For NATIVE-format frames the
                // buffer copies column-at-a-time via direct page addresses; for
                // Parquet frames and column types without a columnar fast path it
                // falls back to per-row through the bound page-frame record.
                PageFrame frame;
                while (rowsThisBatch < batchCap
                        && (frame = state.advanceToPageFrame()) != null) {
                    long lo = state.getStreamingPageFrameRow();
                    long rowsAvailable = state.getStreamingPageFrameRowHi() - lo;
                    int rowsBudget = batchCap - rowsThisBatch;
                    long hi = lo + Math.min(rowsBudget, rowsAvailable);
                    batchBuffer.appendPageFrame(frame, state.getStreamingPageFrameMemoryRecord(), lo, hi);
                    int rowsFromSlice = (int) (hi - lo);
                    state.consumePageFrameRows(rowsFromSlice);
                    rowsThisBatch += rowsFromSlice;
                }
            } else {
                while (rowsThisBatch < batchCap && cursor.hasNext()) {
                    batchBuffer.appendRow(cursor.getRecord());
                    rowsThisBatch++;
                }
            }
            boolean cursorExhausted = rowsThisBatch < batchCap;
            boolean writeFullSchema = !state.isStreamingFullSchemaSent();
            // Empty trailing batch (cursor exhausted between full-batch boundary and this iteration)
            // and we've already sent at least one batch with the schema -- skip straight to RESULT_END.
            if (rowsThisBatch == 0 && state.isStreamingFullSchemaSent()) {
                long finalSeq = state.getStreamingBatchSeq() == 0 ? 0 : state.getStreamingBatchSeq() - 1;
                long totalRows = state.getStreamingRowsEmitted();
                // Detach factory + SQL text BEFORE endStreaming so endStreaming
                // doesn't free the factory -- we put it back into the compile
                // cache for reuse instead. Cache-before-send so PeerIsSlowToReadException
                // (framework parks and resumes) doesn't strand the factory outside
                // the cache, and connection drops still drain via selectCache.close().
                cacheStreamingFactoryIfAvailable(state);
                state.endStreaming();
                sendResultEnd(context, state, requestId, finalSeq, totalRows);
                return;
            }
            // Advance the streaming sequence BEFORE the network send. HttpRawSocket.send commits
            // bytes to the response sink (buffer.onWrite) before flushSingle() -- which is what
            // throws PeerIsSlowToReadException. The parked bytes are delivered to the client by
            // resumeResponseSend, so from the client's perspective the batch IS sent. If we
            // advanced the sequence after the throw, resume would re-emit the next batch with
            // the same seq number, producing two batches labelled seq=N with different rows.
            long currentSeq = state.getStreamingBatchSeq();
            state.onStreamingBatchSent(rowsThisBatch);
            if (cursorExhausted) {
                // Last batch on this cursor. Compose RESULT_BATCH + RESULT_END into
                // the send buffer and hand both frames to the kernel in a single
                // rawSocket.send() call. Small queries pay one syscall and one TCP
                // segment instead of two, which is the bulk of the latency floor
                // on localhost round-trips.
                long totalRows = state.getStreamingRowsEmitted();
                // Cache the factory for the next query with this SQL text. Must
                // happen before sendResultBatchAndEnd because that method calls
                // state.endStreaming() internally; after it runs, state no longer
                // has the factory reference.
                cacheStreamingFactoryIfAvailable(state);
                int qwpBytes = sendResultBatchAndEnd(context, state, requestId, currentSeq,
                        batchBuffer, schemaId, writeFullSchema, totalRows);
                // Credit bookkeeping: debit only the RESULT_BATCH payload. RESULT_END
                // is a control frame and not subject to flow control.
                state.consumeStreamingCredit(qwpBytes);
                return;
            }
            int qwpBytes = sendResultBatch(context, state, requestId, currentSeq, batchBuffer, schemaId, writeFullSchema);
            // Credit bookkeeping: debit by the bytes we just committed to the wire.
            // No-op when the stream isn't credit-limited (initialCredit==0).
            state.consumeStreamingCredit(qwpBytes);
        }
    }
}
