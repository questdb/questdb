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
import io.questdb.std.CharSequenceLongHashMap;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;

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
    private final QwpTudCache.CommittedTxnConsumer committedTxnConsumer = this::recordCommittedTable;
    private final LineHttpProcessorConfiguration configuration;
    // Delta symbol dictionary for this connection
    private final ObjList<String> connectionSymbolDict = new ObjList<>();
    private final StringSink deferredErrorMessage = new StringSink();
    private final CharSequenceLongHashMap durableProgressSnapshot = new CharSequenceLongHashMap();
    private final StringSink error = new StringSink();
    private final CharSequenceLongHashMap lastDurableSeqTxns = new CharSequenceLongHashMap();
    private final long maxBufferSize;
    private final int maxResponseErrorMessageLength;
    private final CharSequenceLongHashMap pendingAckSeqTxns = new CharSequenceLongHashMap();
    private final CharSequenceObjHashMap<String> pendingDurableDirNames = new CharSequenceObjHashMap<>();
    private final CharSequenceLongHashMap pendingDurableSeqTxns = new CharSequenceLongHashMap();
    private final StringSink rejectMsg = new StringSink();
    private final CharSequenceLongHashMap resumeAckSeqTxns = new CharSequenceLongHashMap();
    private final ConnectionSymbolCache symbolCache = new ConnectionSymbolCache();
    private final CharSequenceObjHashMap<String> tableDirNames = new CharSequenceObjHashMap<>();
    private long bufferAddress;
    private int bufferPosition;
    private int bufferSize;
    private Status currentStatus = Status.OK;
    private long deferredErrorSequence = -1;
    private byte deferredErrorStatus;
    private boolean durableAckEnabled;
    private long fd = -1;
    private long highestProcessedSequence = -1;
    private long lastAckedSequence = -1;
    private long messageSequence;
    private byte negotiatedVersion = QwpConstants.VERSION_1;
    private int recvBufferLen;
    private long resumeAckSequence = -1;
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
        Unsafe.copyMemory(lo, bufferAddress + bufferPosition, len);
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

    /**
     * Collects per-table durable progress from the registry. Returns the
     * snapshot map (owned by this instance) containing only tables whose
     * durable seqTxn has advanced since the last durable ack was sent.
     * The caller must consume the map before the next call.
     * <p>
     * Only iterates tables with outstanding durable work, not every table
     * the connection has ever written to.
     */
    public CharSequenceLongHashMap collectDurableProgress(DurableAckRegistry registry) {
        durableProgressSnapshot.clear();
        if (!durableAckEnabled) {
            return durableProgressSnapshot;
        }
        ObjList<CharSequence> tableNames = pendingDurableDirNames.keys();
        for (int i = 0, n = tableNames.size(); i < n; i++) {
            CharSequence tableName = tableNames.getQuick(i);
            String dirName = pendingDurableDirNames.get(tableName);
            long uploadedSeqTxn = registry.getDurablyUploadedSeqTxn(dirName);
            if (uploadedSeqTxn >= 0) {
                long lastSent = lastDurableSeqTxns.get(tableName);
                if (uploadedSeqTxn > lastSent) {
                    durableProgressSnapshot.put(tableName, uploadedSeqTxn);
                }
            }
        }
        return durableProgressSnapshot;
    }

    public void commit() {
        try {
            tudCache.commitAll(committedTxnConsumer);
        } catch (Throwable th) {
            tudCache.setDistressed();
            LOG.error().$('[').$(fd).$("] commit error: ").$(th).$();
            rejectCommitError(th);
        }
    }

    public int computeAckPayloadSize() {
        int size = 9 + 2;
        ObjList<CharSequence> keys = pendingAckSeqTxns.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            size += 2 + Utf8s.utf8Bytes(keys.getQuick(i)) + 8;
        }
        return size;
    }

    public int computeDurableAckPayloadSize() {
        int size = 1 + 2;
        ObjList<CharSequence> keys = durableProgressSnapshot.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            size += 2 + Utf8s.utf8Bytes(keys.getQuick(i)) + 8;
        }
        return size;
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

    public CharSequenceLongHashMap getPendingAckSeqTxns() {
        return pendingAckSeqTxns;
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
     * Transitions from READY to RESUME_ACK state. Snapshots the
     * pending per-table seqTxns (already written to the OS buffer)
     * and clears the map so new commits accumulate fresh.
     */
    public void onAckBlocked(long sequence) {
        sendState = SEND_STATE_RESUME_ACK;
        resumeAckSequence = sequence;
        resumeAckSeqTxns.clear();
        resumeAckSeqTxns.putAll(pendingAckSeqTxns);
        pendingAckSeqTxns.clear();
    }

    /**
     * Records a successful ACK send. Stays in READY state.
     */
    public void onAckSent(long sequence) {
        lastAckedSequence = sequence;
        pendingAckSeqTxns.clear();
    }

    /**
     * Records that a durable-ack send was blocked by a full OS buffer.
     * Transitions from READY to RESUME_DURABLE_ACK. The durableProgressSnapshot
     * is retained so onDurableAckSent() can update lastDurableSeqTxns on resume.
     */
    public void onDurableAckBlocked() {
        sendState = SEND_STATE_RESUME_DURABLE_ACK;
    }

    /**
     * Records a successful durable-ack send. Updates lastDurableSeqTxns
     * from the current durableProgressSnapshot so that the next
     * collectDurableProgress() only reports further advances. Removes
     * tables from the pending set when the durable watermark has caught
     * up to or exceeded the committed seqTxn.
     */
    public void onDurableAckSent() {
        ObjList<CharSequence> keys = durableProgressSnapshot.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            long durableSeqTxn = durableProgressSnapshot.get(tableName);
            if (durableSeqTxn >= pendingDurableSeqTxns.get(tableName)) {
                // Watermark caught up — prune all per-table tracking so
                // these maps don't grow one entry per unique table name
                // for the connection's lifetime. A later commit to the
                // same table re-populates via recordCommittedTable; the
                // drop-recreate check there treats an absent entry the
                // same as a first-sight and still works correctly.
                int dirIdx = pendingDurableDirNames.keyIndex(tableName);
                if (dirIdx < 0) {
                    pendingDurableDirNames.removeAt(dirIdx);
                }
                int seqIdx = pendingDurableSeqTxns.keyIndex(tableName);
                if (seqIdx < 0) {
                    pendingDurableSeqTxns.removeAt(seqIdx);
                }
                int tdnIdx = tableDirNames.keyIndex(tableName);
                if (tdnIdx < 0) {
                    tableDirNames.removeAt(tdnIdx);
                }
                int ldsIdx = lastDurableSeqTxns.keyIndex(tableName);
                if (ldsIdx < 0) {
                    lastDurableSeqTxns.removeAt(ldsIdx);
                }
            } else {
                // Pending still ahead of durable watermark — remember
                // what we reported so the next collectDurableProgress
                // only reports further advances.
                lastDurableSeqTxns.put(tableName, durableSeqTxn);
            }
        }
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
        pendingAckSeqTxns.clear();
        pendingDurableDirNames.clear();
        pendingDurableSeqTxns.clear();
        resumeAckSeqTxns.clear();
        lastDurableSeqTxns.clear();
        durableProgressSnapshot.clear();
        tableDirNames.clear();

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
        resumeAckSeqTxns.clear();
        sendState = SEND_STATE_READY;
    }

    /**
     * Completes a resumed durable-ack send that was previously blocked.
     */
    public void onResumeDurableAckComplete() {
        onDurableAckSent();
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
                byte messageVersion = Unsafe.getByte(bufferAddress + QwpConstants.HEADER_OFFSET_VERSION);
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

    /**
     * Writes per-table seqTxn entries from the given map to native memory.
     * Format: tableCount(2) + [nameLen(2) + nameUtf8(N) + seqTxn(8)] * count
     *
     * @return number of bytes written
     */
    public static int writeTableSeqTxnEntries(long addr, CharSequenceLongHashMap entries) {
        int offset = 0;
        ObjList<CharSequence> keys = entries.keys();
        int count = keys.size();
        Unsafe.getUnsafe().putShort(addr + offset, (short) count);
        offset += 2;
        for (int i = 0; i < count; i++) {
            CharSequence tableName = keys.getQuick(i);
            int utf8Len = Utf8s.utf8Bytes(tableName);
            Unsafe.getUnsafe().putShort(addr + offset, (short) utf8Len);
            offset += 2;
            Utf8s.strCpyUtf8(tableName, addr + offset, utf8Len);
            offset += utf8Len;
            Unsafe.getUnsafe().putLong(addr + offset, entries.get(tableName));
            offset += 8;
        }
        return offset;
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

    private void recordCommittedTable(String tableName, String tableDirName, long seqTxn) {
        if (seqTxn < 0) {
            return;
        }
        pendingAckSeqTxns.put(tableName, seqTxn);
        if (durableAckEnabled) {
            String oldDirName = tableDirNames.get(tableName);
            if (oldDirName != null && !oldDirName.equals(tableDirName)) {
                // Table was dropped and re-created with a new dir name.
                // Reset the durable watermark so the new incarnation's
                // uploads are properly reported.
                lastDurableSeqTxns.put(tableName, -1L);
            }
            tableDirNames.put(tableName, tableDirName);
            pendingDurableDirNames.put(tableName, tableDirName);
            pendingDurableSeqTxns.put(tableName, seqTxn);
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
