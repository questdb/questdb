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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitFailedException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.ConnectionAware;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.line.tcp.ConnectionSymbolCache;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.QwpWalAppender;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpSchemaRegistry;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;

/**
 * State management for QWP v1 processing.
 */
public class QwpProcessorState implements QuietCloseable, ConnectionAware {
    static final int SEND_STATE_READY = 0;
    static final int SEND_STATE_RESUME_ACK = 1;
    static final int SEND_STATE_RESUME_ACK_THEN_ERROR = 3;
    static final int SEND_STATE_RESUME_ERROR = 2;
    private static final Log LOG = LogFactory.getLog(QwpProcessorState.class);
    private final LineHttpProcessorConfiguration configuration;
    // Per-connection accumulated symbol dictionary for delta encoding
    private final ObjList<String> connectionSymbolDict = new ObjList<>();
    private final StringSink deferredErrorMessage = new StringSink();
    private final StringSink error = new StringSink();
    private final long maxBufferSize;
    private final int maxResponseErrorMessageLength;
    private final StringSink rejectMsg = new StringSink();
    // Per-connection symbol ID cache: clientSymbolId → tableSymbolId
    private final ConnectionSymbolCache symbolCache = new ConnectionSymbolCache();
    // Buffer to accumulate incoming data
    private long bufferAddress;
    private int bufferPosition;
    private int bufferSize;
    private Status currentStatus = Status.OK;
    private long deferredErrorSequence = -1;
    private byte deferredErrorStatus;
    private long fd = -1;
    // WebSocket connection state — persists across ILP messages, reset by onDisconnected()
    private long highestProcessedSequence = -1;
    private long lastAckedSequence = -1;
    private long messageSequence;
    private byte negotiatedVersion = QwpConstants.VERSION_1;
    private int recvBufferLen;
    private long resumeAckSequence = -1;
    private SecurityContext securityContext;
    // The response sink owns the serialized bytes; sendState tracks what
    // resumeResponseSend() will flush next and what must follow it.
    private int sendState = SEND_STATE_READY;
    private QwpStreamingDecoder streamingDecoder;
    private QwpTudCache tudCache;
    private QwpWalAppender walAppender;
    private boolean wsHandshakeSent;

    public QwpProcessorState(
            int initBufferSize,
            int maxResponseContentLength,
            CairoEngine engine,
            LineHttpProcessorConfiguration configuration
    ) {
        assert initBufferSize > 0;
        this.configuration = configuration;
        this.maxBufferSize = Math.min(configuration.getMaxRecvBufferSize(), Integer.MAX_VALUE);
        this.maxResponseErrorMessageLength = Math.max(0, (int) ((maxResponseContentLength - 100) / 1.5));
        try {
            this.streamingDecoder = new QwpStreamingDecoder(
                    new QwpSchemaRegistry(configuration.getQwpMaxSchemasPerConnection()),
                    configuration.getQwpMaxRowsPerTable()
            );
            this.walAppender = new QwpWalAppender(
                    configuration.autoCreateNewColumns(),
                    engine.getConfiguration().getMaxFileNameLength(),
                    engine.getConfiguration().getMaxSqlRecompileAttempts()
            );
            this.walAppender.setSymbolCache(symbolCache);

            final DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(configuration);
            this.tudCache = new QwpTudCache(
                    engine,
                    configuration.autoCreateNewColumns(),
                    configuration.autoCreateNewTables(),
                    defaultColumnTypes,
                    configuration.getDefaultPartitionBy()
            );

            this.bufferSize = initBufferSize;
            this.bufferAddress = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_HTTP_CONN);
            this.bufferPosition = 0;
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    public void addData(long lo, long hi) {
        int len = (int) (hi - lo);
        if (len <= 0) {
            return;
        }
        long required = (long) bufferPosition + len;
        if (required > maxBufferSize) {
            rejectMsg.clear();
            rejectMsg.put("message size exceeds maximum buffer size of ").put(maxBufferSize).put(" bytes");
            reject(Status.PARSE_ERROR, rejectMsg, fd);
            return;
        }
        ensureCapacity((int) required);
        Unsafe.getUnsafe().copyMemory(lo, bufferAddress + bufferPosition, len);
        bufferPosition += len;
    }

    public void clear() {
        tudCache.clear();
        error.clear();
        currentStatus = Status.OK;
        bufferPosition = 0;
        streamingDecoder.reset();
    }

    @Override
    public void close() {
        tudCache = Misc.free(tudCache);
        streamingDecoder = Misc.free(streamingDecoder);
        walAppender = Misc.free(walAppender);
        if (bufferAddress != 0) {
            Unsafe.free(bufferAddress, bufferSize, MemoryTag.NATIVE_HTTP_CONN);
            bufferAddress = 0;
        }
    }

    public void commit() {
        try {
            tudCache.commitAll();
        } catch (Throwable th) {
            tudCache.setDistressed();
            LOG.error().$('[').$(fd).$("] commit error: ").$(th).$();
            rejectCommitError(th);
        }
    }

    public CharSequence getDeferredErrorMessage() {
        return deferredErrorMessage;
    }

    public long getDeferredErrorSequence() {
        return deferredErrorSequence;
    }

    public byte getDeferredErrorStatus() {
        return deferredErrorStatus;
    }

    public String getErrorText() {
        return error.toString();
    }

    public long getHighestProcessedSequence() {
        return highestProcessedSequence;
    }

    public long getLastAckedSequence() {
        return lastAckedSequence;
    }

    public int getRecvBufferLen() {
        return recvBufferLen;
    }

    public int getSendState() {
        return sendState;
    }

    public Status getStatus() {
        return currentStatus;
    }

    /**
     * Returns true if there are successfully processed messages that haven't been
     * ACKed yet and the send buffer is clear (READY state).
     */
    public boolean hasPendingAck() {
        return sendState == SEND_STATE_READY && highestProcessedSequence > lastAckedSequence;
    }

    public boolean isOk() {
        return currentStatus == Status.OK;
    }

    public boolean isSendReady() {
        return sendState == SEND_STATE_READY;
    }

    public boolean isSending() {
        return sendState != SEND_STATE_READY;
    }

    public boolean isWsHandshakeSent() {
        return wsHandshakeSent;
    }

    public long nextMessageSequence() {
        return messageSequence++;
    }

    public void of(long fd, SecurityContext securityContext) {
        this.fd = fd;
        this.securityContext = securityContext;
    }

    /**
     * Records that an ACK send was blocked by a full OS buffer.
     * Transitions from READY to RESUME_ACK state.
     */
    public void onAckBlocked(long sequence) {
        sendState = SEND_STATE_RESUME_ACK;
        resumeAckSequence = sequence;
    }

    /**
     * Records a successful ACK send. Stays in READY state.
     */
    public void onAckSent(long sequence) {
        lastAckedSequence = sequence;
    }

    @Override
    public void onDisconnected() {
        clear();
        streamingDecoder.resetConnectionState();
        tudCache.reset();
        connectionSymbolDict.clear();  // Reset delta symbol dictionary on disconnect

        // Reset WebSocket connection state
        highestProcessedSequence = -1;
        lastAckedSequence = -1;
        messageSequence = 0;
        recvBufferLen = 0;
        resumeAckSequence = -1;
        sendState = SEND_STATE_READY;
        clearDeferredError();
        wsHandshakeSent = false;

        // Log cache stats before clearing (only if there were any lookups)
        long hits = symbolCache.getCacheHits();
        long misses = symbolCache.getCacheMisses();
        if (hits + misses > 0) {
            LOG.info()
                    .$("symbol cache stats [fd=").$(fd)
                    .$(", hits=").$(hits)
                    .$(", misses=").$(misses)
                    .$(", hitRate=").$(symbolCache.getHitRatePercent())
                    .$("%]").$();
        }
        symbolCache.clear();
    }

    public void onErrorBlocked(byte status, long sequence, CharSequence errorMessage) {
        deferredErrorStatus = status;
        deferredErrorSequence = sequence;
        deferredErrorMessage.clear();
        if (errorMessage != null) {
            deferredErrorMessage.put(errorMessage);
        }

        if (sendState == SEND_STATE_RESUME_ACK || sendState == SEND_STATE_RESUME_ACK_THEN_ERROR) {
            sendState = SEND_STATE_RESUME_ACK_THEN_ERROR;
        } else {
            sendState = SEND_STATE_RESUME_ERROR;
        }
    }

    public void onErrorSent() {
        clearDeferredError();
        sendState = SEND_STATE_READY;
    }

    /**
     * Completes a resumed ACK send that was previously blocked.
     */
    public void onResumeAckComplete() {
        lastAckedSequence = resumeAckSequence;
        resumeAckSequence = -1;
        sendState = SEND_STATE_READY;
    }

    public void onResumeErrorComplete() {
        clearDeferredError();
        sendState = SEND_STATE_READY;
    }

    public void processMessage() {
        if (bufferPosition == 0 || !isOk()) {
            return;
        }

        try {
            // Verify the message version matches what was negotiated during the upgrade
            if (bufferPosition >= QwpConstants.HEADER_SIZE) {
                byte messageVersion = Unsafe.getUnsafe().getByte(bufferAddress + QwpConstants.HEADER_OFFSET_VERSION);
                if (messageVersion != negotiatedVersion) {
                    rejectMsg.clear();
                    rejectMsg.put("message version ").put(messageVersion)
                            .put(" does not match negotiated version ").put(negotiatedVersion);
                    reject(Status.PARSE_ERROR, rejectMsg, fd);
                    return;
                }
            }

            // Decode using streaming decoder with delta symbol dictionary support
            QwpMessageCursor messageCursor = streamingDecoder.decode(
                    bufferAddress, bufferPosition, connectionSymbolDict);

            // Process each table block using streaming cursors
            while (messageCursor.hasNextTable()) {
                QwpTableBlockCursor tableBlock = messageCursor.nextTable();

                WalTableUpdateDetails tud = tudCache.getTableUpdateDetails(
                        securityContext,
                        tableBlock.getTableNameUtf8(),
                        tableBlock.getSchema(),
                        tableBlock,
                        configuration.getQwpMaxTablesPerConnection()
                );
                if (tud == null) {
                    rejectMsg.clear();
                    rejectMsg.put("failed to create table update details for: ").put(tableBlock.getTableName());
                    reject(Status.INTERNAL_ERROR, rejectMsg, fd);
                    return;
                }

                walAppender.appendToWalStreaming(securityContext, tableBlock, tud);
            }

        } catch (QwpParseException e) {
            LOG.error().$('[').$(fd).$("] QWP v1 parse error: ").$(e.getFlyweightMessage()).$();
            reject(
                    e.getErrorCode() == QwpParseException.ErrorCode.SCHEMA_MISMATCH
                            ? Status.SCHEMA_MISMATCH
                            : Status.PARSE_ERROR,
                    e.getFlyweightMessage(),
                    fd
            );
        } catch (CommitFailedException e) {
            LOG.error().$('[').$(fd).$("] commit failed: ").$(e.getMessage()).$();
            tudCache.setDistressed();
            rejectCommitError(e.getReason());
        } catch (CairoException e) {
            LOG.error().$('[').$(fd).$("] cairo error: ").$(e.getFlyweightMessage()).$();
            reject(cairoExceptionStatus(e), e.getFlyweightMessage(), fd);
        } catch (Throwable e) {
            LOG.critical().$('[').$(fd).$("] unexpected error: ").$(e).$();
            tudCache.setDistressed();
            rejectMsg.clear();
            rejectMsg.put("unexpected error: ").put(e.getMessage());
            reject(Status.INTERNAL_ERROR, rejectMsg, fd);
        }
    }

    public void reject(Status status, CharSequence errorText, long fd) {
        currentStatus = status;
        error.clear();
        if (errorText != null) {
            error.put(errorText, 0, Math.min(errorText.length(), maxResponseErrorMessageLength));
        } else {
            error.put("(no error message)");
        }
        this.fd = fd;
        LOG.error().$('[').$(fd).$("] rejected [status=").$(status).$(", error=").$safe(errorText).$(']').$();
    }

    public void setHighestProcessedSequence(long highestProcessedSequence) {
        this.highestProcessedSequence = highestProcessedSequence;
    }

    public void setNegotiatedVersion(byte negotiatedVersion) {
        this.negotiatedVersion = negotiatedVersion;
    }

    public void setRecvBufferLen(int recvBufferLen) {
        this.recvBufferLen = recvBufferLen;
    }

    public void setWsHandshakeSent(boolean wsHandshakeSent) {
        this.wsHandshakeSent = wsHandshakeSent;
    }

    /**
     * Returns true if the ACK batch threshold has been reached and the send
     * buffer is clear, meaning a cumulative ACK should be sent now.
     */
    public boolean shouldSendAck(int batchSize) {
        return sendState == SEND_STATE_READY
                && highestProcessedSequence - lastAckedSequence >= batchSize;
    }

    private static Status cairoExceptionStatus(CairoException e) {
        if (e.isAuthorizationError()) {
            return Status.SECURITY_ERROR;
        }
        return e.isCritical() ? Status.INTERNAL_ERROR : Status.NOT_ACCEPTING_WRITES;
    }

    private void clearDeferredError() {
        deferredErrorMessage.clear();
        deferredErrorSequence = -1;
        deferredErrorStatus = 0;
    }

    private void ensureCapacity(int required) {
        if (required > bufferSize) {
            int cappedDoubling = (int) Math.min(bufferSize * 2L, Integer.MAX_VALUE);
            int newSize = Math.max(required, cappedDoubling);
            bufferAddress = Unsafe.realloc(bufferAddress, bufferSize, newSize, MemoryTag.NATIVE_HTTP_CONN);
            bufferSize = newSize;
        }
    }

    private void rejectCommitError(Throwable th) {
        if (th instanceof CairoException e) {
            reject(cairoExceptionStatus(e), e.getFlyweightMessage(), fd);
            return;
        }

        rejectMsg.clear();
        rejectMsg.put("commit error: ");
        if (th != null && th.getMessage() != null) {
            rejectMsg.put(th.getMessage(), 0, Math.min(th.getMessage().length(), maxResponseErrorMessageLength));
        }
        reject(Status.INTERNAL_ERROR, rejectMsg, fd);
    }

    public enum Status {
        OK,
        PARSE_ERROR,
        SCHEMA_MISMATCH,
        SECURITY_ERROR,
        INTERNAL_ERROR,
        NOT_ACCEPTING_WRITES
    }
}
