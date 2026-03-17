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
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpSchemaCache;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;

import java.util.concurrent.atomic.AtomicLong;

/**
 * State management for QWP v1 processing.
 */
public class QwpProcessorState implements QuietCloseable, ConnectionAware {
    private static final AtomicLong ERROR_COUNT = new AtomicLong();
    private static final Log LOG = LogFactory.getLog(QwpProcessorState.class);
    // Per-connection accumulated symbol dictionary for delta encoding
    private final ObjList<String> connectionSymbolDict = new ObjList<>();
    private final StringSink error = new StringSink();
    private final long maxBufferSize;
    private final int maxResponseErrorMessageLength;
    private final QwpStreamingDecoder streamingDecoder;
    // Per-connection symbol ID cache: clientSymbolId → tableSymbolId
    private final ConnectionSymbolCache symbolCache = new ConnectionSymbolCache();
    private final QwpTudCache tudCache;
    private final QwpWalAppender walAppender;
    // Buffer to accumulate incoming data
    private long bufferAddress;
    private int bufferPosition;
    private int bufferSize;
    private Status currentStatus = Status.OK;
    private long fd = -1;

    // WebSocket connection state — persists across ILP messages, reset by onDisconnected()
    private long highestProcessedSequence = -1;
    private long lastAckedSequence = -1;
    private long messageSequence;
    private int recvBufferLen;
    private SecurityContext securityContext;
    private SendState sendState = SendState.READY;
    private long sequenceInBuffer = -1;
    private boolean wsHandshakeSent;

    public QwpProcessorState(
            int initBufferSize,
            int maxResponseContentLength,
            CairoEngine engine,
            LineHttpProcessorConfiguration configuration
    ) {
        assert initBufferSize > 0;
        this.maxBufferSize = Math.min(configuration.getMaxRecvBufferSize(), Integer.MAX_VALUE);
        this.maxResponseErrorMessageLength = Math.max(0, (int) ((maxResponseContentLength - 100) / 1.5));
        try {
            this.streamingDecoder = new QwpStreamingDecoder(new QwpSchemaCache());
            this.walAppender = new QwpWalAppender(
                    configuration.autoCreateNewColumns(),
                    engine.getConfiguration().getMaxFileNameLength()
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
            reject(Status.PARSE_ERROR, "message size exceeds maximum buffer size of " + maxBufferSize + " bytes", fd);
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
        Misc.free(tudCache);
        Misc.free(streamingDecoder);
        Misc.free(walAppender);
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
            currentStatus = Status.INTERNAL_ERROR;
            ERROR_COUNT.incrementAndGet();
            String msg = th.getMessage();
            error.put("commit error: ");
            if (msg != null) {
                error.put(msg, 0, Math.min(msg.length(), maxResponseErrorMessageLength));
            }
            LOG.error().$('[').$(fd).$("] commit error: ").$(th).$();
        }
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

    public Status getStatus() {
        return currentStatus;
    }

    /**
     * Returns true if there are successfully processed messages that haven't been
     * ACKed yet and the send buffer is clear (READY state).
     */
    public boolean hasPendingAck() {
        return sendState == SendState.READY && highestProcessedSequence > lastAckedSequence;
    }

    public boolean isOk() {
        return currentStatus == Status.OK;
    }

    public boolean isSendReady() {
        return sendState == SendState.READY;
    }

    public boolean isSending() {
        return sendState == SendState.SENDING;
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
     * Transitions from READY to SENDING state.
     */
    public void onAckBlocked(long sequence) {
        sendState = SendState.SENDING;
        sequenceInBuffer = sequence;
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
        tudCache.reset();
        connectionSymbolDict.clear();  // Reset delta symbol dictionary on disconnect

        // Reset WebSocket connection state
        highestProcessedSequence = -1;
        lastAckedSequence = -1;
        messageSequence = 0;
        recvBufferLen = 0;
        sequenceInBuffer = -1;
        sendState = SendState.READY;
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

    /**
     * Completes a resumed send that was previously blocked.
     * Transitions from SENDING back to READY state.
     */
    public void onResumeSendComplete() {
        lastAckedSequence = sequenceInBuffer;
        sequenceInBuffer = -1;
        sendState = SendState.READY;
    }

    public void processMessage() {
        if (bufferPosition == 0 || !isOk()) {
            return;
        }

        try {
            // Decode using streaming decoder with delta symbol dictionary support
            QwpMessageCursor messageCursor = streamingDecoder.decode(
                    bufferAddress, bufferPosition, connectionSymbolDict);

            // Process each table block using streaming cursors
            while (messageCursor.hasNextTable()) {
                QwpTableBlockCursor tableBlock = messageCursor.nextTable();

                WalTableUpdateDetails tud = tudCache.getTableUpdateDetails(securityContext, tableBlock.getTableNameUtf8(), tableBlock.getSchema(), tableBlock);
                if (tud == null) {
                    reject(Status.INTERNAL_ERROR, "failed to create table update details for: " + tableBlock.getTableName(), fd);
                    return;
                }

                walAppender.appendToWalStreaming(securityContext, tableBlock, tud);
            }

        } catch (QwpParseException e) {
            LOG.error().$('[').$(fd).$("] QWP v1 parse error: ").$(e.getMessage()).$();
            reject(Status.PARSE_ERROR, e.getMessage(), fd);
        } catch (CommitFailedException e) {
            LOG.error().$('[').$(fd).$("] commit failed: ").$(e.getMessage()).$();
            tudCache.setDistressed();
            reject(Status.INTERNAL_ERROR, "commit failed: " + e.getMessage(), fd);
        } catch (CairoException e) {
            LOG.error().$('[').$(fd).$("] cairo error: ").$(e.getFlyweightMessage()).$();
            if (e.isAuthorizationError()) {
                reject(Status.SECURITY_ERROR, e.getFlyweightMessage().toString(), fd);
            } else if (e.isCritical()) {
                tudCache.setDistressed();
                reject(Status.INTERNAL_ERROR, e.getFlyweightMessage().toString(), fd);
            } else {
                reject(Status.NOT_ACCEPTING_WRITES, e.getFlyweightMessage().toString(), fd);
            }
        } catch (Throwable e) {
            LOG.critical().$('[').$(fd).$("] unexpected error: ").$(e).$();
            tudCache.setDistressed();
            reject(Status.INTERNAL_ERROR, "unexpected error: " + e.getMessage(), fd);
        }
    }

    public void reject(Status status, String errorText, long fd) {
        currentStatus = status;
        if (errorText != null) {
            error.put(errorText, 0, Math.min(errorText.length(), maxResponseErrorMessageLength));
        } else {
            error.put("(no error message)");
        }
        ERROR_COUNT.incrementAndGet();
        this.fd = fd;
        LOG.error().$('[').$(fd).$("] rejected [status=").$(status).$(", error=").$(errorText).$(']').$();
    }

    public void setHighestProcessedSequence(long highestProcessedSequence) {
        this.highestProcessedSequence = highestProcessedSequence;
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
        return sendState == SendState.READY
                && highestProcessedSequence - lastAckedSequence >= batchSize;
    }

    private void ensureCapacity(int required) {
        if (required > bufferSize) {
            int newSize = Math.max(bufferSize * 2, required);
            bufferAddress = Unsafe.realloc(bufferAddress, bufferSize, newSize, MemoryTag.NATIVE_HTTP_CONN);
            bufferSize = newSize;
        }
    }

    enum SendState {
        READY,
        SENDING
    }

    public enum Status {
        OK,
        PARSE_ERROR,
        SECURITY_ERROR,
        INTERNAL_ERROR,
        NOT_ACCEPTING_WRITES
    }
}
