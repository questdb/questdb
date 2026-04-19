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

import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cutlass.http.ConnectionAware;
import io.questdb.cutlass.qwp.codec.QwpEgressColumnDef;
import io.questdb.cutlass.qwp.codec.QwpResultBatchBuffer;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.table.FwdTableReaderPageFrameCursor;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

/**
 * Per-connection state for QWP egress (query results) processing.
 * <p>
 * Holds the reusable scratch objects for the per-connection processing loop:
 * inbound decoder, outbound result-batch accumulator, schema registry, bind
 * variable service, and column-definition pool. Each is allocated once and
 * reused across queries on the same connection.
 */
public class QwpEgressProcessorState implements QuietCloseable, ConnectionAware {

    private final QwpResultBatchBuffer batchBuffer = new QwpResultBatchBuffer();
    private final BindVariableServiceImpl bindVariableService;
    private final ObjList<QwpEgressColumnDef> columnDefsPool = new ObjList<>();
    private final QwpEgressRequestDecoder decoder = new QwpEgressRequestDecoder();
    private long fd = -1;
    private byte negotiatedVersion = QwpConstants.VERSION_1;
    private int nextSchemaId;
    // Page-frame iteration scaffolding. Allocated lazily on first page-frame query and
    // reused across queries on the same connection; per-query binding happens in
    // beginStreamingPageFrame. None of these are freed on endStreaming -- only the
    // per-query streamingPageFrameCursor is.
    private PageFrameAddressCache pageFrameAddressCache;
    private PageFrameMemoryPool pageFrameMemoryPool;
    private PageFrameMemoryRecord pageFrameMemoryRecord;
    private int recvBufferLen;
    private SecurityContext securityContext;
    private boolean wsHandshakeSent;

    /**
     * Streaming-state for an in-flight query. Populated when the query starts; cleared
     * (and resources freed) on completion, error, or disconnect. Lets the upgrade
     * processor's {@code resumeSend} continue iteration from the cursor's current
     * position after a {@code PeerIsSlowToReadException} parks the connection.
     */
    private boolean streamingActive;
    private long streamingBatchSeq;
    private int streamingColumnCount;
    private RecordCursor streamingCursor;
    private RecordCursorFactory streamingFactory;
    private boolean streamingFullSchemaSent;
    // PageFrame streaming state. streamingPageFrameCursor is the per-query cursor
    // (freed on endStreaming). streamingPageFrameIndex counts frames consumed since
    // the query started - used to key PageFrameAddressCache entries. row/rowHi track
    // the iteration position inside the current frame; when row == rowHi we advance
    // to the next frame or finish.
    private PageFrameCursor streamingPageFrameCursor;
    private int streamingPageFrameIndex;
    private long streamingPageFrameRow;
    private long streamingPageFrameRowHi;
    private long streamingRequestId;
    /**
     * Set true the moment {@code sendResultEnd} is initiated (whether it succeeds or
     * throws {@code PeerIsSlowToReadException}). Lets {@code resumeSend} know not to
     * re-issue the {@code RESULT_END} after flushing the deferred bytes.
     */
    private boolean streamingResultEndInitiated;
    /**
     * Total rows emitted for the in-flight query across all batches. Reported to the
     * client in {@code RESULT_END.total_rows} so application code can verify the
     * server's view of the full result matches the row count it observed.
     */
    private long streamingRowsEmitted;
    private int streamingSchemaId;

    public QwpEgressProcessorState(io.questdb.cairo.CairoConfiguration cairoConfiguration) {
        this.bindVariableService = new BindVariableServiceImpl(cairoConfiguration);
    }

    /**
     * Returns the next unused schema id on this connection and advances the counter.
     */
    public int allocateSchemaId() {
        return nextSchemaId++;
    }

    /**
     * Returns (or extends+returns) a column-definition slot. The pool grows to at
     * least {@code requiredSize}; caller re-populates each slot via {@link QwpEgressColumnDef#of}.
     */
    public ObjList<QwpEgressColumnDef> borrowColumnDefs(int requiredSize) {
        int currentPos = columnDefsPool.size();
        if (requiredSize > currentPos) {
            columnDefsPool.setPos(requiredSize);
            for (int i = currentPos; i < requiredSize; i++) {
                if (columnDefsPool.getQuick(i) == null) {
                    columnDefsPool.setQuick(i, new QwpEgressColumnDef());
                }
            }
        } else {
            columnDefsPool.setPos(requiredSize);
        }
        return columnDefsPool;
    }

    public void beginStreaming(
            long requestId,
            RecordCursorFactory factory,
            RecordCursor cursor,
            int columnCount,
            int schemaId
    ) {
        // Defence in depth: if a caller forgets to check isStreamingActive(), free the
        // previous factory/cursor before overwriting. The primary gate lives in
        // QwpEgressUpgradeProcessor.handleQueryRequest, but this second line prevents
        // a native-resource leak if the gate is ever bypassed.
        if (streamingActive) {
            endStreaming();
        }
        this.streamingRequestId = requestId;
        this.streamingFactory = factory;
        this.streamingCursor = cursor;
        this.streamingColumnCount = columnCount;
        this.streamingSchemaId = schemaId;
        this.streamingBatchSeq = 0;
        this.streamingFullSchemaSent = false;
        this.streamingRowsEmitted = 0;
        this.streamingActive = true;
    }

    /**
     * Starts a streaming query whose row iteration will run through a
     * {@link PageFrameCursor}. Lazily allocates the per-connection page-frame
     * scaffolding ({@link PageFrameAddressCache}, {@link PageFrameMemoryPool},
     * {@link PageFrameMemoryRecord}) on first call and rebinds them to the new
     * query. The cursor is owned by the state from this point on and is freed by
     * {@link #endStreaming}.
     */
    public void beginStreamingPageFrame(
            long requestId,
            RecordCursorFactory factory,
            PageFrameCursor pageFrameCursor,
            int columnCount,
            int schemaId
    ) {
        if (streamingActive) {
            endStreaming();
        }
        if (pageFrameAddressCache == null) {
            pageFrameAddressCache = new PageFrameAddressCache();
            pageFrameMemoryPool = new PageFrameMemoryPool(1);
            pageFrameMemoryRecord = new PageFrameMemoryRecord();
        }
        pageFrameAddressCache.of(
                factory.getMetadata(),
                pageFrameCursor.getColumnMapping(),
                pageFrameCursor.isExternal()
        );
        pageFrameMemoryPool.of(pageFrameAddressCache);
        pageFrameMemoryRecord.of(pageFrameCursor);
        this.streamingRequestId = requestId;
        this.streamingFactory = factory;
        this.streamingPageFrameCursor = pageFrameCursor;
        this.streamingColumnCount = columnCount;
        this.streamingSchemaId = schemaId;
        this.streamingBatchSeq = 0;
        this.streamingFullSchemaSent = false;
        this.streamingRowsEmitted = 0;
        this.streamingPageFrameIndex = 0;
        this.streamingPageFrameRow = 0;
        this.streamingPageFrameRowHi = 0;
        this.streamingActive = true;
    }

    public void clear() {
        endStreaming();
        recvBufferLen = 0;
        wsHandshakeSent = false;
        fd = -1;
        securityContext = null;
        negotiatedVersion = QwpConstants.VERSION_1;
        nextSchemaId = 0;
        batchBuffer.reset();
        bindVariableService.clear();
    }

    /**
     * Releases the in-flight cursor + factory and marks streaming inactive.
     * Idempotent -- safe to call from completion, error, or disconnect paths.
     */
    public void endStreaming() {
        streamingActive = false;
        streamingCursor = Misc.free(streamingCursor);
        streamingPageFrameCursor = Misc.free(streamingPageFrameCursor);
        streamingFactory = Misc.free(streamingFactory);
        streamingColumnCount = 0;
        streamingSchemaId = 0;
        streamingBatchSeq = 0;
        streamingFullSchemaSent = false;
        streamingResultEndInitiated = false;
        streamingPageFrameIndex = 0;
        streamingPageFrameRow = 0;
        streamingPageFrameRowHi = 0;
    }

    /**
     * Advances the page-frame iteration by one row. When the current frame is
     * exhausted, pulls the next {@link PageFrame} from the cursor and rebinds
     * the memory record to it. Returns the {@link Record} pointing at the next
     * row, or {@code null} when no more rows are available.
     * <p>
     * Row indices passed to {@link PageFrameMemoryRecord#setRowIndex} are
     * frame-local (0..frameRowCount). {@link FwdTableReaderPageFrameCursor}
     * already offsets the page addresses by the frame's partition-lo so the
     * record's getLong/getVarchar read from position 0 inside the frame.
     * <p>
     * Only valid when streaming was started via {@link #beginStreamingPageFrame}.
     */
    public Record advancePageFrameRow() {
        if (streamingPageFrameRow >= streamingPageFrameRowHi) {
            PageFrame frame = streamingPageFrameCursor.next();
            if (frame == null) {
                return null;
            }
            pageFrameAddressCache.add(streamingPageFrameIndex, frame);
            pageFrameMemoryPool.navigateTo(streamingPageFrameIndex, pageFrameMemoryRecord);
            streamingPageFrameRow = 0;
            streamingPageFrameRowHi = frame.getPartitionHi() - frame.getPartitionLo();
            streamingPageFrameIndex++;
            if (streamingPageFrameRow >= streamingPageFrameRowHi) {
                // Empty frame - try again.
                return advancePageFrameRow();
            }
        }
        pageFrameMemoryRecord.setRowIndex(streamingPageFrameRow++);
        return pageFrameMemoryRecord;
    }

    public byte getNegotiatedVersion() {
        return negotiatedVersion;
    }

    public boolean isStreamingResultEndInitiated() {
        return streamingResultEndInitiated;
    }

    public void markStreamingResultEndInitiated() {
        streamingResultEndInitiated = true;
    }

    public long getStreamingBatchSeq() {
        return streamingBatchSeq;
    }

    public int getStreamingColumnCount() {
        return streamingColumnCount;
    }

    public RecordCursor getStreamingCursor() {
        return streamingCursor;
    }

    public PageFrameCursor getStreamingPageFrameCursor() {
        return streamingPageFrameCursor;
    }

    public long getStreamingRequestId() {
        return streamingRequestId;
    }

    /**
     * Returns the symbol-table source matching the active streaming mode so
     * {@link QwpResultBatchBuffer#beginBatch} can hook up the SYMBOL fast path
     * without the processor having to know which path is in use.
     */
    public SymbolTableSource getStreamingSymbolTableSource() {
        return streamingPageFrameCursor != null ? streamingPageFrameCursor : streamingCursor;
    }

    public long getStreamingRowsEmitted() {
        return streamingRowsEmitted;
    }

    public int getStreamingSchemaId() {
        return streamingSchemaId;
    }

    public boolean isStreamingActive() {
        return streamingActive;
    }

    public boolean isStreamingFullSchemaSent() {
        return streamingFullSchemaSent;
    }

    /**
     * True if the active streaming query is iterating via a {@link PageFrameCursor};
     * false if it's using a {@link RecordCursor}. Undefined when streaming is inactive.
     */
    public boolean isStreamingPageFrame() {
        return streamingPageFrameCursor != null;
    }

    public void onStreamingBatchSent(int rowsEmittedInBatch) {
        streamingBatchSeq++;
        streamingFullSchemaSent = true;
        streamingRowsEmitted += rowsEmittedInBatch;
    }

    @Override
    public void close() {
        clear();
        Misc.free(batchBuffer);
        pageFrameMemoryRecord = Misc.free(pageFrameMemoryRecord);
        pageFrameMemoryPool = Misc.free(pageFrameMemoryPool);
        pageFrameAddressCache = Misc.free(pageFrameAddressCache);
    }

    public QwpResultBatchBuffer getBatchBuffer() {
        return batchBuffer;
    }

    public BindVariableServiceImpl getBindVariableService() {
        return bindVariableService;
    }

    public QwpEgressRequestDecoder getDecoder() {
        return decoder;
    }

    public long getFd() {
        return fd;
    }

    public int getRecvBufferLen() {
        return recvBufferLen;
    }

    public SecurityContext getSecurityContext() {
        return securityContext;
    }

    public boolean isWsHandshakeSent() {
        return wsHandshakeSent;
    }

    public void of(long fd, SecurityContext securityContext) {
        this.fd = fd;
        this.securityContext = securityContext;
    }

    public void onDisconnected() {
        clear();
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
}
