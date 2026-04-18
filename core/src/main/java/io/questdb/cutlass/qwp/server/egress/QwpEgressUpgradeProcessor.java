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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
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
import io.questdb.cutlass.qwp.server.QwpWebSocketHttpProcessor;
import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.cutlass.qwp.websocket.WebSocketFrameParser;
import io.questdb.cutlass.qwp.websocket.WebSocketFrameWriter;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.network.Socket;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;

import java.nio.charset.StandardCharsets;

/**
 * HTTP request processor that handles WebSocket upgrade for QWP egress at /read/v1.
 * <p>
 * Phase 1 skeleton: completes the WebSocket handshake (reusing the validation and
 * response helpers from {@link QwpWebSocketHttpProcessor}), switches the connection
 * to WebSocket mode, and runs a frame-receive loop that handles control frames
 * (PING/PONG/CLOSE) and discards binary frames. Query execution wiring lands in a
 * follow-up commit.
 */
public class QwpEgressUpgradeProcessor implements HttpRequestProcessor {

    private static final Log LOG = LogFactory.getLog(QwpEgressUpgradeProcessor.class);
    private static final LocalValue<QwpEgressProcessorState> LV = new LocalValue<>();
    private final CairoEngine engine;
    private final int forceRecvFragmentationChunkSize;
    private final WebSocketFrameParser frameParser = new WebSocketFrameParser();
    private final HttpFullFatServerConfiguration httpConfiguration;
    private final int recvBufferSize;
    private final int sharedWorkerCount;

    public QwpEgressUpgradeProcessor(
            CairoEngine engine,
            HttpFullFatServerConfiguration httpConfiguration,
            int sharedWorkerCount
    ) {
        this.engine = engine;
        this.forceRecvFragmentationChunkSize = httpConfiguration.getHttpContextConfiguration()
                .getForceRecvFragmentationChunkSize();
        this.httpConfiguration = httpConfiguration;
        this.recvBufferSize = httpConfiguration.getRecvBufferSize();
        this.sharedWorkerCount = sharedWorkerCount;
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
                        ? io.questdb.cutlass.qwp.server.QwpWebSocketUpgradeProcessor
                            .writeUpgradeRequiredResponse(bufferAddr, bufferSize)
                        : io.questdb.cutlass.qwp.server.QwpWebSocketUpgradeProcessor
                            .writeBadRequestResponse(bufferAddr, bufferSize, validationError);
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
        String acceptKey = QwpWebSocketHttpProcessor.computeAcceptKey(wsKey);
        int requiredHandshakeSize = QwpWebSocketHttpProcessor.responseSize(acceptKey, negotiatedVersion);
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

        int bytesWritten = QwpWebSocketHttpProcessor.writeResponse(bufferAddr, acceptKey, negotiatedVersion);
        try {
            rawSocket.send(bytesWritten);
        } catch (PeerIsSlowToReadException e) {
            throw HttpException.instance("Egress 101 handshake blocked");
        }
        state.setWsHandshakeSent(true);
        LOG.info().$("Egress WebSocket handshake sent [fd=").$(context.getFd())
                .$(", qwpVersion=").$(negotiatedVersion).I$();

        context.switchProtocol();
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) {
        QwpEgressProcessorState state = LV.get(context);
        if (state != null && state.isWsHandshakeSent()) {
            LOG.debug().$("Egress WebSocket ready for frames [fd=").$(context.getFd()).I$();
        }
    }

    @Override
    public void parkRequest(HttpConnectionContext context, boolean pausedQuery) {
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

    @Override
    public void resumeSend(HttpConnectionContext context) {
        // Phase 1 skeleton: no outbound state to resume yet.
    }

    private static int parseClientMaxVersion(HttpRequestHeader requestHeader) {
        Utf8Sequence maxVersionHeader = requestHeader.getHeader(QwpWebSocketHttpProcessor.HEADER_X_QWP_MAX_VERSION);
        if (maxVersionHeader == null) {
            return QwpConstants.VERSION_1;
        }
        int parsed = Numbers.parseNonNegativeIntQuiet(maxVersionHeader);
        return parsed >= QwpConstants.VERSION_1 ? parsed : QwpConstants.VERSION_1;
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
            case WebSocketOpcode.PONG ->
                    LOG.debug().$("Egress pong [fd=").$(context.getFd()).I$();
            case WebSocketOpcode.CLOSE -> {
                handleClose(context, payload, length);
                throw ServerDisconnectException.INSTANCE;
            }
            default ->
                    LOG.debug().$("Egress unknown opcode [fd=").$(context.getFd()).$(", opcode=").$(opcode).I$();
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

    // =============================================================================
    // Egress message dispatch and query execution
    // =============================================================================

    /**
     * Phase 1 batch caps. Size-based cap is indirectly enforced by the rawSocket
     * send buffer capacity (rejections become QUERY_ERROR).
     */
    private static final int MAX_ROWS_PER_BATCH = 4096;

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
            case QwpEgressMsgKind.QUERY_REQUEST ->
                    handleQueryRequest(context, state, payload, length);
            case QwpEgressMsgKind.CANCEL, QwpEgressMsgKind.CREDIT -> {
                // Phase 1: parse and log, do not act.
                LOG.debug().$("Egress control frame (Phase 1 ignored) [fd=").$(context.getFd())
                        .$(", kind=0x").$(Integer.toHexString(msgKind & 0xFF)).I$();
            }
            default -> {
                LOG.error().$("Egress unknown msg_kind [fd=").$(context.getFd())
                        .$(", kind=0x").$(Integer.toHexString(msgKind & 0xFF)).I$();
                throw ServerDisconnectException.INSTANCE;
            }
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

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery cq = compiler.compile(decoder.sql, sqlCtx);
                if (cq.getType() != CompiledQuery.SELECT && cq.getType() != CompiledQuery.PSEUDO_SELECT) {
                    sendQueryError(context, requestId, QwpConstants.STATUS_PARSE_ERROR,
                            "Phase 1 egress only supports SELECT queries");
                    return;
                }
                RecordCursorFactory factory = cq.getRecordCursorFactory();
                try (RecordCursor cursor = factory.getCursor(sqlCtx)) {
                    streamResults(context, state, requestId, factory.getMetadata(), cursor);
                } finally {
                    Misc.free(factory);
                }
            }
        } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
            throw e;
        } catch (Throwable e) {
            LOG.error().$("Egress query failed [fd=").$(context.getFd())
                    .$(", requestId=").$(requestId)
                    .$(", error=").$(e).I$();
            byte status = mapErrorStatus(e);
            try {
                sendQueryError(context, requestId, status,
                        e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
            } catch (PeerDisconnectedException | PeerIsSlowToReadException sendFail) {
                throw sendFail;
            } catch (Throwable ignored) {
                // Best-effort error report; drop.
            }
        }
    }

    private static byte mapErrorStatus(Throwable e) {
        // SqlException covers both syntax errors and semantic errors (e.g., table not found).
        // Its getMessage() already embeds the "[position] text" form.
        if (e instanceof io.questdb.griffin.SqlException) {
            return QwpConstants.STATUS_PARSE_ERROR;
        }
        if (e instanceof io.questdb.cairo.CairoException) {
            io.questdb.cairo.CairoException ce = (io.questdb.cairo.CairoException) e;
            if (ce.isAuthorizationError()) {
                return QwpConstants.STATUS_SECURITY_ERROR;
            }
            return QwpConstants.STATUS_INTERNAL_ERROR;
        }
        return QwpConstants.STATUS_INTERNAL_ERROR;
    }

    private void streamResults(
            HttpConnectionContext context,
            QwpEgressProcessorState state,
            long requestId,
            RecordMetadata metadata,
            RecordCursor cursor
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        int columnCount = metadata.getColumnCount();
        ObjList<QwpEgressColumnDef> columnDefs = state.borrowColumnDefs(columnCount);
        for (int i = 0; i < columnCount; i++) {
            columnDefs.getQuick(i).of(metadata.getColumnName(i), metadata.getColumnType(i));
        }

        int schemaId = state.allocateSchemaId();
        long batchSeq = 0;
        boolean writeFullSchema = true;
        QwpResultBatchBuffer batchBuffer = state.getBatchBuffer();

        while (true) {
            batchBuffer.beginBatch(columnDefs);
            int rowsThisBatch = 0;
            while (rowsThisBatch < MAX_ROWS_PER_BATCH && cursor.hasNext()) {
                Record rec = cursor.getRecord();
                batchBuffer.appendRow(rec);
                rowsThisBatch++;
            }
            if (rowsThisBatch == 0 && !writeFullSchema) {
                // No rows and schema already sent: cursor exhausted.
                break;
            }
            sendResultBatch(context, requestId, batchSeq, batchBuffer, schemaId, writeFullSchema);
            writeFullSchema = false;
            batchSeq++;
            if (rowsThisBatch < MAX_ROWS_PER_BATCH) {
                // Cursor exhausted within this batch.
                break;
            }
        }
        sendResultEnd(context, requestId, batchSeq == 0 ? 0 : batchSeq - 1);
    }

    // -----------------------------------------------------------------------------
    // Outbound frame serialisation
    // -----------------------------------------------------------------------------

    /**
     * Writes one RESULT_BATCH frame into the rawSocket buffer and sends it.
     * <p>
     * Layout inside rawSocket buffer:
     * <pre>
     *   [0 .. 10)                 WS header reservation
     *   [10 .. 10 + qwpSize)      QWP message header + prelude + table block
     * </pre>
     * After computing {@code qwpSize}, we pick the real WS header size and may
     * memmove the QWP bytes left so the wire frame abuts offset 0 (which is
     * what {@link HttpRawSocket#send(int)} flushes).
     */
    private void sendResultBatch(
            HttpConnectionContext context,
            long requestId,
            long batchSeq,
            QwpResultBatchBuffer batchBuffer,
            long schemaId,
            boolean writeFullSchema
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufAddr = rawSocket.getBufferAddress();
        int bufSize = rawSocket.getBufferSize();
        if (bufSize < QwpEgressFrameWriter.WS_HEADER_RESERVATION + QwpConstants.HEADER_SIZE + 32) {
            throw HttpException.instance("egress send buffer too small");
        }
        long qwpStart = bufAddr + QwpEgressFrameWriter.WS_HEADER_RESERVATION;
        long bodyStart = QwpEgressFrameWriter.writeMessageHeader(
                qwpStart, QwpConstants.VERSION_1, (byte) 0, 1, 0 /* payload len patched */);
        long preludeEnd = QwpEgressFrameWriter.writeResultBatchPrelude(bodyStart, requestId, batchSeq);
        long bufLimit = bufAddr + bufSize;
        int tableBlockSize = batchBuffer.emitTableBlock(preludeEnd, bufLimit, schemaId, writeFullSchema);
        if (tableBlockSize < 0) {
            throw HttpException.instance("egress: batch too large for send buffer");
        }
        long qwpEnd = preludeEnd + tableBlockSize;
        int qwpSize = (int) (qwpEnd - qwpStart);
        int qwpPayloadLen = qwpSize - QwpConstants.HEADER_SIZE;
        QwpEgressFrameWriter.patchPayloadLength(qwpStart, qwpPayloadLen);

        sendFrame(rawSocket, bufAddr, qwpStart, qwpSize);
    }

    private void sendResultEnd(HttpConnectionContext context, long requestId, long finalSeq)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufAddr = rawSocket.getBufferAddress();
        long qwpStart = bufAddr + QwpEgressFrameWriter.WS_HEADER_RESERVATION;
        long bodyStart = QwpEgressFrameWriter.writeMessageHeader(
                qwpStart, QwpConstants.VERSION_1, (byte) 0, 0, 0 /* payload len patched */);
        long bodyEnd = QwpEgressFrameWriter.writeResultEnd(bodyStart, requestId, finalSeq, 0);
        int qwpSize = (int) (bodyEnd - qwpStart);
        int qwpPayloadLen = qwpSize - QwpConstants.HEADER_SIZE;
        QwpEgressFrameWriter.patchPayloadLength(qwpStart, qwpPayloadLen);
        sendFrame(rawSocket, bufAddr, qwpStart, qwpSize);
    }

    private void sendQueryError(HttpConnectionContext context, long requestId, byte status, CharSequence msg)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpRawSocket rawSocket = context.getRawResponseSocket();
        long bufAddr = rawSocket.getBufferAddress();
        long qwpStart = bufAddr + QwpEgressFrameWriter.WS_HEADER_RESERVATION;
        long bodyStart = QwpEgressFrameWriter.writeMessageHeader(
                qwpStart, QwpConstants.VERSION_1, (byte) 0, 0, 0);
        byte[] msgBytes = (msg == null ? "" : msg.toString()).getBytes(StandardCharsets.UTF_8);
        long bodyEnd = QwpEgressFrameWriter.writeQueryError(bodyStart, requestId, status, msgBytes);
        int qwpSize = (int) (bodyEnd - qwpStart);
        int qwpPayloadLen = qwpSize - QwpConstants.HEADER_SIZE;
        QwpEgressFrameWriter.patchPayloadLength(qwpStart, qwpPayloadLen);
        sendFrame(rawSocket, bufAddr, qwpStart, qwpSize);
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
}
