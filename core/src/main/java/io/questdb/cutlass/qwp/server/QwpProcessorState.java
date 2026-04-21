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
import io.questdb.cairo.wal.DurableAckRegistry;
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
import io.questdb.std.Chars;
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
    static final int SEND_STATE_RESUME_DURABLE_ACK = 4;
    static final int SEND_STATE_RESUME_DURABLE_ACK_THEN_ERROR = 5;
    static final int SEND_STATE_RESUME_ERROR = 2;
    private static final Log LOG = LogFactory.getLog(QwpProcessorState.class);
    private final QwpTudCache.CommittedTxnConsumer committedTxnConsumer = this::recordCommittedSegment;
    private final LineHttpProcessorConfiguration configuration;
    private final ObjList<String> connectionSymbolDict = new ObjList<>();
    private final StringSink deferredErrorMessage = new StringSink();
    private final ObjList<DurableSegmentEntry> durableSegmentPool = new ObjList<>();
    private final ObjList<DurableSegmentEntry> durableSegments = new ObjList<>();
    private final StringSink error = new StringSink();
    private final long maxBufferSize;
    private final int maxResponseErrorMessageLength;
    private final StringSink rejectMsg = new StringSink();
    private final ConnectionSymbolCache symbolCache = new ConnectionSymbolCache();
    private long bufferAddress;
    private int bufferPosition;
    private int bufferSize;
    private long currentCommitClientSeq = -1;
    private Status currentStatus = Status.OK;
    private long deferredErrorSequence = -1;
    private byte deferredErrorStatus;
    private boolean durableAckEnabled;
    private long fd = -1;
    private long highestDurableSequence = -1;
    private long highestProcessedSequence = -1;
    private long lastAckedSequence = -1;
    private long lastDurablyAckedSequence = -1;
    private long messageSequence;
    private byte negotiatedVersion = QwpConstants.VERSION_1;
    private int recvBufferLen;
    private long resumeAckSequence = -1;
    private long resumeDurableAckSequence = -1;
    private SecurityContext securityContext;
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

    /**
     * Walks the pending-durable queue against the current upload watermarks and
     * advances {@link #highestDurableSequence} over any client sequences whose
     * tables are now fully uploaded. Callers should follow a successful advance
     * with a durable-ack send attempt.
     */
    public void advanceDurableWatermark(DurableAckRegistry registry) {
        if (!durableAckEnabled) {
            return;
        }
        long minUnuploadedFirstClientSeq = Long.MAX_VALUE;
        int n = durableSegments.size();
        int removed = 0;
        for (int i = 0; i < n; i++) {
            DurableSegmentEntry e = durableSegments.getQuick(i);
            if (registry.getDurablyUploadedSeqTxn(e.tableDirName) >= e.seqTxn) {
                durableSegmentPool.add(e);
                durableSegments.setQuick(i, null);
                removed++;
            } else {
                if (e.firstClientSeq < minUnuploadedFirstClientSeq) {
                    minUnuploadedFirstClientSeq = e.firstClientSeq;
                }
            }
        }
        if (removed > 0) {
            int dst = 0;
            for (int i = 0; i < n; i++) {
                DurableSegmentEntry e = durableSegments.getQuick(i);
                if (e != null) {
                    durableSegments.setQuick(dst++, e);
                }
            }
            durableSegments.setPos(dst);
        }
        long candidate;
        if (durableSegments.size() == 0) {
            candidate = highestProcessedSequence;
        } else {
            candidate = minUnuploadedFirstClientSeq - 1;
        }
        if (candidate > highestDurableSequence) {
            highestDurableSequence = candidate;
        }
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

    public void commit(long clientSeq) {
        if (durableAckEnabled) {
            currentCommitClientSeq = clientSeq;
        }
        try {
            tudCache.commitAll(durableAckEnabled ? committedTxnConsumer : null);
        } catch (Throwable th) {
            tudCache.setDistressed();
            LOG.error().$('[').$(fd).$("] commit error: ").$(th).$();
            rejectCommitError(th);
        } finally {
            currentCommitClientSeq = -1;
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

    public long getHighestDurableSequence() {
        return highestDurableSequence;
    }

    public long getHighestProcessedSequence() {
        return highestProcessedSequence;
    }

    public long getLastAckedSequence() {
        return lastAckedSequence;
    }

    public long getLastDurablyAckedSequence() {
        return lastDurablyAckedSequence;
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

    /**
     * Returns true when the durable-upload watermark has advanced past the last
     * durable ACK sent to the client. Only meaningful when
     * {@link #durableAckEnabled} is true; always false otherwise.
     */
    public boolean hasPendingDurableAck() {
        return durableAckEnabled
                && sendState == SEND_STATE_READY
                && highestDurableSequence > lastDurablyAckedSequence;
    }

    public boolean isDurableAckEnabled() {
        return durableAckEnabled;
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

    /**
     * Records that a durable-ack send was blocked by a full OS buffer.
     * Transitions from READY to RESUME_DURABLE_ACK.
     */
    public void onDurableAckBlocked(long sequence) {
        sendState = SEND_STATE_RESUME_DURABLE_ACK;
        resumeDurableAckSequence = sequence;
    }

    /**
     * Records a successful durable-ack send. Stays in READY state.
     */
    public void onDurableAckSent(long sequence) {
        lastDurablyAckedSequence = sequence;
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

        // Drop any durable-ack state; the connection is going away, so even if
        // uploads complete later, there is nobody left to notify.
        durableAckEnabled = false;
        highestDurableSequence = -1;
        lastDurablyAckedSequence = -1;
        resumeDurableAckSequence = -1;
        currentCommitClientSeq = -1;
        durableSegments.clear();
        durableSegmentPool.clear();

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
        } else if (sendState == SEND_STATE_RESUME_DURABLE_ACK || sendState == SEND_STATE_RESUME_DURABLE_ACK_THEN_ERROR) {
            // A durable-ack send was partially written before the error arose.
            // Preserve the in-flight frame; the resume handler will finish it and
            // then send the deferred error.
            sendState = SEND_STATE_RESUME_DURABLE_ACK_THEN_ERROR;
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

    /**
     * Completes a resumed durable-ack send that was previously blocked.
     */
    public void onResumeDurableAckComplete() {
        lastDurablyAckedSequence = resumeDurableAckSequence;
        resumeDurableAckSequence = -1;
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

    public void setDurableAckEnabled(boolean durableAckEnabled) {
        this.durableAckEnabled = durableAckEnabled;
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

    private DurableSegmentEntry findOrCreateSegmentEntry(CharSequence tableDirName, int walId, int segmentId) {
        for (int i = 0, n = durableSegments.size(); i < n; i++) {
            DurableSegmentEntry e = durableSegments.getQuick(i);
            if (e.walId == walId && e.segmentId == segmentId && Chars.equals(e.tableDirName, tableDirName)) {
                return e;
            }
        }
        int poolSize = durableSegmentPool.size();
        DurableSegmentEntry e;
        if (poolSize > 0) {
            e = durableSegmentPool.getQuick(poolSize - 1);
            durableSegmentPool.setPos(poolSize - 1);
        } else {
            e = new DurableSegmentEntry();
        }
        e.tableDirName = tableDirName;
        e.walId = walId;
        e.segmentId = segmentId;
        e.firstClientSeq = currentCommitClientSeq;
        durableSegments.add(e);
        return e;
    }

    private void recordCommittedSegment(CharSequence tableDirName, int walId, int segmentId, long seqTxn) {
        if (currentCommitClientSeq < 0 || seqTxn < 0) {
            return;
        }
        DurableSegmentEntry e = findOrCreateSegmentEntry(tableDirName, walId, segmentId);
        e.seqTxn = seqTxn;
        e.lastClientSeq = currentCommitClientSeq;
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

    /**
     * Tracks a single WAL segment that this connection has written to.
     * Each entry maps a (table, walId, segmentId) to the range of clientSeqs
     * that wrote into it and the latest seqTxn produced.
     */
    private static final class DurableSegmentEntry {
        long firstClientSeq;
        long lastClientSeq;
        int segmentId;
        long seqTxn;
        CharSequence tableDirName;
        int walId;
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
