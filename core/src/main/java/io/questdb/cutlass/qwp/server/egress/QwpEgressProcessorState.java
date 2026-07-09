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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cutlass.http.ConnectionAware;
import io.questdb.cutlass.http.HttpException;
import io.questdb.cutlass.qwp.codec.QwpEgressColumnDef;
import io.questdb.cutlass.qwp.codec.QwpEgressConnSymbolDict;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.codec.QwpResultBatchBuffer;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Zstd;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-connection state for QWP egress (query results) processing.
 * <p>
 * Holds the reusable scratch objects for the per-connection processing loop:
 * inbound decoder, outbound result-batch accumulator, schema registry, bind
 * variable service, and column-definition pool. Each is allocated once and
 * reused across queries on the same connection.
 */
public class QwpEgressProcessorState implements QuietCloseable, ConnectionAware {

    private static final Log LOG = LogFactory.getLog(QwpEgressProcessorState.class);
    // Test-only default overrides for the CACHE_RESET soft caps. Set these
    // before opening a connection so every new {@link QwpEgressProcessorState}
    // picks them up in its constructor; set back to {@code -1} at the end of
    // the test to restore production defaults for subsequent tests in the same
    // JVM. Production code never touches these.
    @TestOnly
    public static volatile int defaultMaxDictEntriesOverrideForTest = -1;
    @TestOnly
    public static volatile int defaultMaxDictHeapBytesOverrideForTest = -1;
    private final QwpResultBatchBuffer batchBuffer = new QwpResultBatchBuffer();
    private final BindVariableServiceImpl bindVariableService;
    private final ObjList<QwpEgressColumnDef> columnDefsPool = new ObjList<>();
    // Connection-scoped SYMBOL dictionary shared across all queries on this connection.
    // Holds the concatenated UTF-8 bytes of every unique symbol value and a parallel
    // end-offsets array; the entry index doubles as the wire-level conn-id. Per-column
    // native-key -> conn-id maps live on each {@code QwpColumnScratch} so the per-row
    // hot path does a single int probe (no Utf8 hashing / equality). Grows but never
    // shrinks; freed on connection close.
    private final QwpEgressConnSymbolDict connSymbolDict = new QwpEgressConnSymbolDict();
    private final QwpEgressRequestDecoder decoder = new QwpEgressRequestDecoder();
    // Reused consumer sequence handed to CompiledQuery.execute for async ALTER /
    // DDL operations. Subscribed to the engine's message bus on first use;
    // cleared between queries (the sequence object itself is reused).
    private final SCSequence eventSubSequence = new SCSequence();
    /**
     * Remaining send-ahead credit in bytes, under byte-based flow control.
     * <p>
     * {@code AtomicLong} rather than {@code volatile long}: {@link #addStreamingCredit}
     * performs a saturating add and {@link #consumeStreamingCredit} performs a
     * subtract, both read-modify-write sequences. Today's single-op-per-fd
     * dispatcher serialises the two callers (CREDIT handler on the read side,
     * streamResults on the write side), so a plain volatile would happen to be
     * safe; the Phase-2 plan to register READ + WRITE on the same fd during
     * streaming turns both sites into true data races. The atomic makes the
     * fix durable regardless of which dispatcher variant is active.
     */
    private final AtomicLong streamingCreditRemaining = new AtomicLong();
    // Compression negotiated at handshake time. codec == COMPRESSION_NONE (default)
    // sends RESULT_BATCH bytes raw; COMPRESSION_ZSTD compresses the region after
    // the prelude with the zstd level the client requested (clamped server-side).
    // The native CCtx handle is allocated lazily on the first batch we compress
    // and reused across every batch on the connection; freed in close().
    private byte compressionCodec = QwpConstants.COMPRESSION_NONE;
    private byte compressionLevel;
    private long fd = -1;
    /**
     * True between {@code onHeadersReady} (which writes the 101 response bytes
     * into the raw-socket buffer) and {@code resumeSend}'s flush + protocol
     * switch. While true, {@code onRequestComplete} / {@code resumeSend} are
     * responsible for committing the bytes and calling {@code switchProtocol}.
     * Handles the case where {@code rawSocket.send} parks mid-write on a
     * fragmented send buffer.
     */
    private boolean handshakeFlushPending;
    // Effective per-batch row cap for this connection: the minimum of the
    // server's hard cap ({@code QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH})
    // and any client-requested limit sent via {@code X-QWP-Max-Batch-Rows} at
    // handshake time. Defaults to the server's cap when no header is sent.
    // Set once per handshake from {@link #setMaxBatchRows}; read on every
    // iteration of the streamResults emit loop.
    private int maxBatchRows;
    // Soft-cap overrides for the CACHE_RESET mechanism. -1 means "use the
    // QwpConstants default"; any non-negative value wins. Exposed so tests can
    // trigger resets deterministically at small row / entry counts; production
    // callers never set these.
    private int maxDictEntriesOverride;
    private int maxDictHeapBytesOverride;
    private byte negotiatedVersion = QwpConstants.VERSION;
    // Page-frame iteration scaffolding. Allocated lazily on first page-frame query and
    // reused across queries on the same connection; per-query binding happens in
    // beginStreamingPageFrame. None of these are freed on endStreaming -- only the
    // per-query streamingPageFrameCursor is.
    private PageFrameAddressCache pageFrameAddressCache;
    private PageFrameMemoryPool pageFrameMemoryPool;
    private PageFrameMemoryRecord pageFrameMemoryRecord;
    /**
     * Cache-reset bitmask the upcoming query applies locally on the server
     * but has NOT yet emitted on the wire. Set by
     * {@code applyCacheResetForUpcomingQuery} in handleQueryRequest before
     * the cursor is opened; consumed by {@code emitPendingCacheReset} at
     * the top of streamResults, after {@code state.beginStreaming} has
     * flipped {@code streamingActive=true}.
     * <p>
     * Splitting "apply locally" from "emit on the wire" lets the wire-send
     * sit inside a streaming-active region. If the send parks on
     * {@code PeerIsSlowToReadException} under a small send fragmentation
     * cap, {@code resumeSend} can re-enter {@code streamResults} and the
     * query continues; emitting from {@code handleQueryRequest} (the
     * earlier shape) abandoned the query because {@code resumeSend} saw
     * {@code streamingActive=false} and returned.
     */
    private byte pendingCacheResetMask;
    /**
     * True between a CLOSE-frame send that parked on
     * {@code PeerIsSlowToReadException} (either the {@code handleClose} echo
     * or a {@code sendFatalClose} diagnostic) and the {@code resumeSend} that
     * finishes flushing the residual bytes. While true, {@code resumeSend}
     * runs {@code gracefulCloseAndDisconnect} after the deferred flush
     * completes, instead of attempting to continue streaming.
     * <p>
     * Without this flag, the catch-and-swallow pattern that used to live in
     * {@code handleClose} / {@code sendFatalClose} tore the connection down
     * before the rest of the CLOSE frame left the box, so the client saw EOF
     * mid-frame and reported "peer disconnect" instead of the close code we
     * promised.
     */
    private boolean pendingDisconnectAfterFlush;
    /**
     * Byte count of the WebSocket 101 handshake response written by
     * {@code onHeadersReady} but not yet committed. {@code onRequestComplete}
     * issues the {@code rawSocket.send(pendingHandshakeBytes)} -- PISR from
     * that path is legal (unlike in {@code onHeadersReady}) and lets the
     * framework park + resume the remainder properly.
     */
    private int pendingHandshakeBytes;
    private int recvBufferLen;
    private SecurityContext securityContext;
    /**
     * Streaming-state for an in-flight query. Populated when the query starts; cleared
     * (and resources freed) on completion, error, or disconnect. Lets the upgrade
     * processor's {@code resumeSend} continue iteration from the cursor's current
     * position after a {@code PeerIsSlowToReadException} parks the connection.
     * <p>
     * {@code volatile} because CANCEL / CREDIT handling on the read side and
     * streamResults on the write side observe this flag across workers. Today
     * per-fd events serialise through the IO dispatcher (one registered op at
     * a time), but the Phase-2 plan to register for both READ and WRITE during
     * streaming would expose a true data race on plain fields.
     */
    private volatile boolean streamingActive;
    private long streamingBatchSeq;
    // Set by {@link #onStreamingBatchSent} and consumed by
    // {@link #consumeBatchSeqCommit} at the top of
    // {@link QwpEgressUpgradeProcessor}'s {@code sendResultBatch}.
    // Encodes the invariant "seq was incremented before the send committed bytes
    // to the response sink". Violating it (e.g. by calling sendResultBatch without
    // first calling onStreamingBatchSent) produces duplicate seq=N batches if the
    // first send parks mid-flight -- silent data corruption from the client's view.
    private boolean streamingBatchSeqCommitted;
    /**
     * Set when a CANCEL frame arriving on this connection targets the in-flight
     * streaming query's {@code requestId}. {@code streamResults} reads the flag
     * between batches and aborts with {@code STATUS_CANCELLED} when true.
     * Cleared on {@code beginStreaming}/{@code beginStreamingPageFrame} so a
     * stale cancel can't kill the next query.
     * <p>
     * {@code volatile}: see the note on {@link #streamingActive}.
     */
    private volatile boolean streamingCancelRequested;
    private int streamingColumnCount;
    /**
     * Credit-flow state for the in-flight query.
     * <p>
     * {@code initialCredit} captures the value the client advertised in
     * QUERY_REQUEST. Zero means "unbounded" (the Phase-1 default -- no credit
     * checking at all). A positive value puts the stream under byte-based flow
     * control: the server tracks {@code creditRemaining} (in bytes of egress
     * payload), refuses to start a new batch when it hits zero, and parks the
     * stream until a CREDIT frame replenishes it.
     */
    private long streamingCreditInitial;
    /**
     * True when {@code streamResults} returned early because credit is exhausted.
     * The state is still "active"; an inbound CREDIT frame will replenish the
     * budget and re-enter {@code streamResults} to continue.
     * <p>
     * {@code volatile}: see the note on {@link #streamingActive}.
     */
    private volatile boolean streamingCreditSuspended;
    // PageFrame streaming state. streamingPageFrameCursor is the per-query cursor
    // (freed on endStreaming). streamingPageFrameIndex counts frames consumed since
    // the query started - used to key PageFrameAddressCache entries. row/rowHi track
    // the iteration position inside the current frame; when row == rowHi we advance
    // to the next frame or finish.
    // The currently-loaded page frame. Kept around across batch boundaries so
    // callers can emit column-at-a-time over the slice [streamingPageFrameRow,
    // streamingPageFrameRowHi). Cleared on {@link #endStreaming}.
    private PageFrame streamingCurrentPageFrame;
    private RecordCursor streamingCursor;
    private RecordCursorFactory streamingFactory;
    private PageFrameCursor streamingPageFrameCursor;
    private int streamingPageFrameIndex;
    private long streamingPageFrameRow;
    private long streamingPageFrameRowHi;
    /**
     * {@code volatile}: the CANCEL handler compares the current request id
     * against the incoming target; see the note on {@link #streamingActive}.
     */
    private volatile long streamingRequestId;
    /**
     * Total rows emitted for the in-flight query across all batches. Reported to the
     * client in {@code RESULT_END.total_rows} so application code can verify the
     * server's view of the full result matches the row count it observed.
     */
    private long streamingRowsEmitted;
    /**
     * Immutable copy of the SQL text that produced the current streaming factory.
     * Captured at {@code beginStreaming*} time and used on the success path to
     * put the factory back into the compile cache. Must be a stable String (not a
     * reference into a reusable decoder sink) because it lives across
     * {@code PeerIsSlowToReadException} parks where the decoder buffer may be
     * repurposed before the re-entered {@code streamResults} in
     * {@link QwpEgressUpgradeProcessor} reaches the cache-put site.
     */
    private String streamingSqlText;
    private boolean wsHandshakeSent;
    // Native ZSTD_CCtx handle (pointer from Zstd.createCCtx). 0 means not yet
    // allocated. Lives across queries on the same connection because the
    // compression codec + level are fixed at handshake time.
    private long zstdCCtx;
    // Growable native scratch buffer that holds compressed bytes between the
    // call to Zstd.compress and the memcpy back into the rawSocket buffer. The
    // uncompressed payload stays in the rawSocket buffer; only the compressed
    // output lands here. Sized on demand in zstdCompressScratch().
    private long zstdCompressScratchAddr;
    private int zstdCompressScratchCapacity;

    public QwpEgressProcessorState(io.questdb.cairo.CairoConfiguration cairoConfiguration) {
        this.bindVariableService = new BindVariableServiceImpl(cairoConfiguration);
        // Pick up any test-only default overrides active at construction time
        // so tests that need tiny soft caps don't have to reach into every
        // per-connection state instance.
        this.maxDictEntriesOverride = defaultMaxDictEntriesOverrideForTest;
        this.maxDictHeapBytesOverride = defaultMaxDictHeapBytesOverrideForTest;
    }

    /**
     * Adds the given number of bytes to the remaining credit. Called from the
     * CREDIT frame handler. Saturates at {@code Long.MAX_VALUE} to avoid
     * overflow on pathological clients. {@code bytes} must be non-negative;
     * negative inputs are rejected as a no-op rather than silently clamped to
     * {@code Long.MAX_VALUE} (which the bare {@code sum < prev} guard would
     * have done because the comparison can't distinguish signed overflow from
     * a negative addend).
     */
    public void addStreamingCredit(long bytes) {
        if (bytes <= 0L) {
            // Caller (handleCredit) already filters this from the wire path,
            // but the contract is enforced here so any future internal caller
            // can't silently grant infinite credit by passing a negative.
            return;
        }
        // CAS loop so the saturating add stays atomic against a concurrent
        // consumeStreamingCredit from the streamResults path. With bytes > 0,
        // sum < prev fires only on signed overflow.
        while (true) {
            long prev = streamingCreditRemaining.get();
            long sum = prev + bytes;
            long next = sum < prev ? Long.MAX_VALUE : sum;
            if (streamingCreditRemaining.compareAndSet(prev, next)) {
                return;
            }
        }
    }

    /**
     * Advances to a page frame that has rows available. Returns the current
     * frame if it still has unconsumed rows, or pulls the next frame from the
     * cursor and returns it. Returns {@code null} when the cursor is drained.
     * <p>
     * After a non-null return, callers may read {@link #getStreamingPageFrameRow}
     * / {@link #getStreamingPageFrameRowHi} for the unconsumed-row range and
     * call {@link #consumePageFrameRows} after emitting rows from that slice.
     * <p>
     * Used by the columnar emit path in
     * {@link io.questdb.cutlass.qwp.codec.QwpResultBatchBuffer#appendPageFrame}.
     */
    public PageFrame advanceToPageFrame() {
        // Skip zero-row frames (iteratively, to keep the stack bounded).
        while (streamingPageFrameRow >= streamingPageFrameRowHi) {
            PageFrame frame = streamingPageFrameCursor.next();
            if (frame == null) {
                streamingCurrentPageFrame = null;
                return null;
            }
            pageFrameAddressCache.add(streamingPageFrameIndex, frame);
            pageFrameMemoryPool.navigateTo(streamingPageFrameIndex, pageFrameMemoryRecord);
            streamingCurrentPageFrame = frame;
            streamingPageFrameRow = 0;
            streamingPageFrameRowHi = frame.getPartitionHi() - frame.getPartitionLo();
            streamingPageFrameIndex++;
        }
        return streamingCurrentPageFrame;
    }

    /**
     * Clears the connection-scoped caches indicated by {@code resetMask}
     * (currently only {@link QwpEgressMsgKind#RESET_MASK_DICT}). Caller must
     * have emitted a {@code CACHE_RESET} frame carrying the same mask before
     * calling this, so the client's view of the caches stays in lockstep with
     * the server's.
     * <p>
     * The dict bit also clears every per-column scratch's native-key to
     * conn-id cache via {@link QwpResultBatchBuffer#clearConnKeyMaps()} --
     * without that, a cached key would resolve to an id the reset dict has
     * already dropped, and the next batch's row payload would reference an id
     * the client was never taught.
     */
    public void applyCacheReset(byte resetMask) {
        if ((resetMask & QwpEgressMsgKind.RESET_MASK_DICT) != 0) {
            connSymbolDict.clear();
            batchBuffer.clearConnKeyMaps();
        }
    }

    public void beginStreaming(
            long requestId,
            RecordCursorFactory factory,
            RecordCursor cursor,
            int columnCount,
            long initialCredit,
            CharSequence sqlText
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
        this.streamingBatchSeq = 0;
        this.streamingBatchSeqCommitted = false;
        this.streamingCancelRequested = false;
        this.streamingCreditInitial = initialCredit;
        this.streamingCreditRemaining.set(initialCredit);
        this.streamingCreditSuspended = false;
        this.streamingRowsEmitted = 0;
        this.streamingSqlText = sqlText != null ? Chars.toString(sqlText) : null;
        this.streamingActive = true;
        // Native symbol keys are per-cursor: clear the per-column native-key -> conn-id
        // caches so a stale mapping from the previous query can't resurface as a
        // wrong conn-id being emitted on the wire.
        batchBuffer.resetForNewQuery();
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
            long initialCredit,
            CharSequence sqlText
    ) {
        if (streamingActive) {
            endStreaming();
        }
        if (pageFrameAddressCache == null) {
            pageFrameAddressCache = new PageFrameAddressCache();
            pageFrameMemoryPool = new PageFrameMemoryPool(0L);
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
        this.streamingBatchSeq = 0;
        this.streamingBatchSeqCommitted = false;
        this.streamingCancelRequested = false;
        this.streamingCreditInitial = initialCredit;
        this.streamingCreditRemaining.set(initialCredit);
        this.streamingCreditSuspended = false;
        this.streamingRowsEmitted = 0;
        this.streamingPageFrameIndex = 0;
        this.streamingPageFrameRow = 0;
        this.streamingPageFrameRowHi = 0;
        this.streamingSqlText = sqlText != null ? Chars.toString(sqlText) : null;
        this.streamingActive = true;
        batchBuffer.resetForNewQuery();
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

    public void clear() {
        endStreaming();
        recvBufferLen = 0;
        wsHandshakeSent = false;
        handshakeFlushPending = false;
        pendingCacheResetMask = 0;
        pendingDisconnectAfterFlush = false;
        pendingHandshakeBytes = 0;
        fd = -1;
        securityContext = null;
        negotiatedVersion = QwpConstants.VERSION;
        maxBatchRows = 0;  // reset to "unset"; will be set per-connection in onHeadersReady
        batchBuffer.reset();
        bindVariableService.clear();
        // Connection dropped/reset -- client cannot reuse the delta dict on a
        // new connection so we discard it too. On a clean close() this is idempotent.
        connSymbolDict.clear();
        // Compression is renegotiated on every handshake, so drop the CCtx
        // now even if the next connection happens to pick the same level.
        compressionCodec = QwpConstants.COMPRESSION_NONE;
        compressionLevel = 0;
        if (zstdCCtx != 0) {
            Zstd.freeCCtx(zstdCCtx);
            zstdCCtx = 0;
        }
    }

    public void clearStreamingCreditSuspended() {
        streamingCreditSuspended = false;
    }

    @Override
    public void close() {
        clear();
        Misc.free(batchBuffer);
        Misc.free(connSymbolDict);
        pageFrameMemoryRecord = Misc.free(pageFrameMemoryRecord);
        pageFrameMemoryPool = Misc.free(pageFrameMemoryPool);
        pageFrameAddressCache = Misc.free(pageFrameAddressCache);
        if (zstdCompressScratchAddr != 0) {
            Unsafe.free(zstdCompressScratchAddr, zstdCompressScratchCapacity, MemoryTag.NATIVE_DEFAULT);
            zstdCompressScratchAddr = 0;
            zstdCompressScratchCapacity = 0;
        }
    }

    /**
     * Returns which connection-scoped caches have outgrown their soft caps and
     * should be flushed via a {@code CACHE_RESET} frame. Currently only
     * {@link QwpEgressMsgKind#RESET_MASK_DICT}, set when the SYMBOL dict exceeds
     * {@link QwpConstants#DEFAULT_MAX_EGRESS_DICT_ENTRIES} or
     * {@link QwpConstants#DEFAULT_MAX_EGRESS_DICT_HEAP_BYTES}. Zero means no
     * cache is over cap.
     * <p>
     * Bounds apply per connection. The caller -- typically {@code
     * QwpEgressUpgradeProcessor} at a query boundary -- decides when it is
     * safe to emit the frame; this method has no side effects.
     */
    public byte computeCacheResetMask() {
        return computeCacheResetMask(false);
    }

    /**
     * As {@link #computeCacheResetMask()}, plus a forced SYMBOL dict reset when
     * {@code forceDictReset} is set and the dict is non-empty (an empty dict
     * needs no {@code CACHE_RESET} frame).
     */
    public byte computeCacheResetMask(boolean forceDictReset) {
        byte mask = 0;
        int dictEntriesCap = maxDictEntriesOverride >= 0
                ? maxDictEntriesOverride : QwpConstants.DEFAULT_MAX_EGRESS_DICT_ENTRIES;
        int dictHeapCap = maxDictHeapBytesOverride >= 0
                ? maxDictHeapBytesOverride : QwpConstants.DEFAULT_MAX_EGRESS_DICT_HEAP_BYTES;
        boolean capExceeded = connSymbolDict.size() >= dictEntriesCap || connSymbolDict.heapBytes() >= dictHeapCap;
        if (capExceeded || (forceDictReset && connSymbolDict.size() > 0)) {
            mask |= QwpEgressMsgKind.RESET_MASK_DICT;
        }
        return mask;
    }

    /**
     * Consumes the seq-commit flag set by {@link #onStreamingBatchSent}, asserting
     * the ordering invariant: a batch send never reaches the response sink without
     * having first bumped the seq + row counters. Call at the top of
     * {@code sendResultBatch}.
     */
    public void consumeBatchSeqCommit() {
        if (!streamingBatchSeqCommitted) {
            throw HttpException.instance("sendResultBatch reached without a preceding onStreamingBatchSent [seq=")
                    .put(streamingBatchSeq)
                    .put(']');
        }
        streamingBatchSeqCommitted = false;
    }

    /**
     * Called by the columnar emit path after it processes a slice of the
     * current page frame. Advances the in-frame row cursor; the next call to
     * {@link #advanceToPageFrame} returns the same frame if rows remain, or
     * pulls the next frame when the cursor is exhausted.
     */
    public void consumePageFrameRows(int rows) {
        streamingPageFrameRow += rows;
    }

    /**
     * Subtracts the bytes actually sent in the just-completed batch from
     * {@code creditRemaining}. No-op when the stream is not credit-limited.
     */
    public void consumeStreamingCredit(long bytes) {
        if (streamingCreditInitial > 0) {
            streamingCreditRemaining.addAndGet(-bytes);
        }
    }

    /**
     * Removes the current streaming factory reference from this state and
     * returns it. {@link #endStreaming} will no longer free it -- ownership
     * transfers to the caller, typically to put the factory back into the
     * compile cache for reuse.
     * <p>
     * Returns {@code null} if streaming isn't active or the factory has
     * already been detached; callers should null-check before caching.
     */
    public RecordCursorFactory detachStreamingFactory() {
        RecordCursorFactory detached = streamingFactory;
        streamingFactory = null;
        return detached;
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
        streamingBatchSeq = 0;
        streamingBatchSeqCommitted = false;
        streamingCancelRequested = false;
        streamingCreditInitial = 0;
        streamingCreditRemaining.set(0);
        streamingCreditSuspended = false;
        streamingPageFrameIndex = 0;
        streamingPageFrameRow = 0;
        streamingPageFrameRowHi = 0;
        streamingCurrentPageFrame = null;
        streamingSqlText = null;
    }

    public QwpResultBatchBuffer getBatchBuffer() {
        return batchBuffer;
    }

    public BindVariableServiceImpl getBindVariableService() {
        return bindVariableService;
    }

    public byte getCompressionCodec() {
        return compressionCodec;
    }

    public byte getCompressionLevel() {
        return compressionLevel;
    }

    /**
     * Returns the connection-scoped SYMBOL dictionary. {@link QwpResultBatchBuffer}
     * appends new entries to it directly from its SYMBOL appendRow branch and
     * emits the per-batch slice from {@code emitDeltaSection}.
     */
    public QwpEgressConnSymbolDict getConnSymbolDict() {
        return connSymbolDict;
    }

    public QwpEgressRequestDecoder getDecoder() {
        return decoder;
    }

    /**
     * Returns the reusable consumer sequence fed to {@code CompiledQuery.execute}
     * for async ALTER / DDL waits. One instance per connection is fine -- calls
     * to {@code fut.await()} subscribe it, release it on completion.
     */
    public SCSequence getEventSubSequence() {
        return eventSubSequence;
    }

    public long getFd() {
        return fd;
    }

    /**
     * Effective per-batch row cap for this connection, already clamped to the
     * server's hard maximum. Read by {@link QwpEgressUpgradeProcessor}'s
     * {@code streamResults} loop to size each batch.
     */
    public int getMaxBatchRows() {
        return maxBatchRows;
    }

    public byte getNegotiatedVersion() {
        return negotiatedVersion;
    }

    public byte getPendingCacheResetMask() {
        return pendingCacheResetMask;
    }

    public int getPendingHandshakeBytes() {
        return pendingHandshakeBytes;
    }

    public int getRecvBufferLen() {
        return recvBufferLen;
    }

    public SecurityContext getSecurityContext() {
        return securityContext;
    }

    public long getStreamingBatchSeq() {
        return streamingBatchSeq;
    }

    public int getStreamingColumnCount() {
        return streamingColumnCount;
    }

    public long getStreamingCreditRemaining() {
        return streamingCreditRemaining.get();
    }

    public RecordCursor getStreamingCursor() {
        return streamingCursor;
    }

    /**
     * {@link PageFrameMemoryRecord} bound to the current page frame. Useful
     * for the per-column fallback inside the columnar emit when a column type
     * doesn't have a bulk-append fast path.
     */
    public PageFrameMemoryRecord getStreamingPageFrameMemoryRecord() {
        return pageFrameMemoryRecord;
    }

    public long getStreamingPageFrameRow() {
        return streamingPageFrameRow;
    }

    public long getStreamingPageFrameRowHi() {
        return streamingPageFrameRowHi;
    }

    public long getStreamingRequestId() {
        return streamingRequestId;
    }

    public long getStreamingRowsEmitted() {
        return streamingRowsEmitted;
    }

    /**
     * Returns the stable SQL text string captured when the current streaming
     * query started, or {@code null} if streaming isn't active. Callers use
     * this as the cache key when putting the factory back for reuse.
     */
    public String getStreamingSqlText() {
        return streamingSqlText;
    }

    /**
     * Returns the symbol-table source matching the active streaming mode so
     * {@link QwpResultBatchBuffer#beginBatch} can hook up the SYMBOL fast path
     * without the processor having to know which path is in use.
     */
    public SymbolTableSource getStreamingSymbolTableSource() {
        return streamingPageFrameCursor != null ? streamingPageFrameCursor : streamingCursor;
    }

    public boolean isHandshakeFlushPending() {
        return handshakeFlushPending;
    }

    /**
     * True between a CLOSE-frame send that parked on
     * {@code PeerIsSlowToReadException} and the {@code resumeSend} that
     * finishes flushing the residual bytes. {@code resumeSend} consumes the
     * flag and triggers {@code gracefulCloseAndDisconnect} once the deferred
     * flush completes.
     */
    public boolean isPendingDisconnectAfterFlush() {
        return pendingDisconnectAfterFlush;
    }

    public boolean isStreamingActive() {
        return streamingActive;
    }

    /**
     * True if a CANCEL frame has been observed for this query since it started
     * streaming.
     */
    public boolean isStreamingCancelRequested() {
        return streamingCancelRequested;
    }

    /**
     * True when the client advertised a non-zero {@code initial_credit} for this
     * query. Credit-limited streams honour {@code creditRemaining} and park on
     * exhaustion; uncapped streams ignore credit entirely (Phase-1 default).
     */
    public boolean isStreamingCreditLimited() {
        return streamingCreditInitial > 0;
    }

    /**
     * True when {@code streamResults} exited early because {@code creditRemaining}
     * hit zero. An inbound CREDIT frame replenishes and un-suspends.
     */
    public boolean isStreamingCreditSuspended() {
        return streamingCreditSuspended;
    }

    /**
     * True if the active streaming query is iterating via a {@link PageFrameCursor};
     * false if it's using a {@link RecordCursor}. Undefined when streaming is inactive.
     */
    public boolean isStreamingPageFrame() {
        return streamingPageFrameCursor != null;
    }

    public boolean isWsHandshakeSent() {
        return wsHandshakeSent;
    }

    /**
     * Records that a CANCEL frame was received for the current streaming query.
     * Idempotent -- multiple CANCELs coalesce into a single abort path.
     */
    public void markStreamingCancelRequested() {
        streamingCancelRequested = true;
    }

    /**
     * Marks the streaming loop as credit-suspended. {@code streamResults} returns
     * early; the next CREDIT frame (or cancel) re-enters it.
     */
    public void markStreamingCreditSuspended() {
        streamingCreditSuspended = true;
    }

    /**
     * OR-merges {@code additionalBits} into the staged {@code CACHE_RESET}
     * mask. Used by the egress processor when a soft cap trips and a fresh
     * mask must accumulate on top of any bits already staged by an earlier
     * query whose {@code emitPendingCacheReset} never ran (non-SELECT path
     * via {@code executeNonSelect}, or a SELECT that failed before
     * {@code emitPendingCacheReset} ran).
     * Plain assignment would silently drop the prior bits and let the client
     * fall out of sync with the server's already-cleared caches.
     */
    public void mergePendingCacheResetMask(byte additionalBits) {
        pendingCacheResetMask |= additionalBits;
    }

    public void of(long fd, SecurityContext securityContext) {
        this.fd = fd;
        this.securityContext = securityContext;
    }

    public void onDisconnected() {
        clear();
    }

    /**
     * Marks the current batch's seq as incremented and its rows as counted.
     * Must be called exactly once per batch, BEFORE the network send commits
     * bytes to the response sink -- so that if the send parks mid-flight, the
     * resumed stream starts the NEXT batch with {@code streamingBatchSeq + 1}
     * rather than re-emitting seq = N with different rows.
     */
    public void onStreamingBatchSent(int rowsEmittedInBatch) {
        if (streamingBatchSeqCommitted) {
            throw HttpException.instance("onStreamingBatchSent called twice without an intervening sendResultBatch [seq=")
                    .put(streamingBatchSeq)
                    .put(']');
        }
        streamingBatchSeq++;
        streamingRowsEmitted += rowsEmittedInBatch;
        streamingBatchSeqCommitted = true;
    }

    /**
     * Test-only: override the egress CACHE_RESET soft caps so tests can trip
     * resets at low entry counts without stuffing the connection with millions
     * of rows. Any argument set to {@code -1} restores the corresponding
     * production default ({@link QwpConstants#DEFAULT_MAX_EGRESS_DICT_ENTRIES},
     * {@link QwpConstants#DEFAULT_MAX_EGRESS_DICT_HEAP_BYTES}).
     */
    @TestOnly
    public void setCacheResetCapsForTest(int maxDictEntries, int maxDictHeapBytes) {
        this.maxDictEntriesOverride = maxDictEntries;
        this.maxDictHeapBytesOverride = maxDictHeapBytes;
    }

    /**
     * Records the compression codec + level chosen at handshake time. Called once
     * per connection from {@code onHeadersReady}; the CCtx is allocated lazily on
     * the first batch that actually needs to compress.
     */
    public void setCompression(byte codec, byte level) {
        this.compressionCodec = codec;
        this.compressionLevel = level;
    }

    public void setHandshakeFlushPending(boolean pending) {
        this.handshakeFlushPending = pending;
    }

    public void setPendingCacheResetMask(byte mask) {
        this.pendingCacheResetMask = mask;
    }

    public void setPendingDisconnectAfterFlush(boolean pending) {
        this.pendingDisconnectAfterFlush = pending;
    }

    /**
     * Called from {@code onHeadersReady} with the client's parsed
     * {@code X-QWP-Max-Batch-Rows} preference, already clamped to the server's
     * hard cap. See {@link #getMaxBatchRows}.
     */
    public void setMaxBatchRows(int rows) {
        this.maxBatchRows = rows;
    }

    public void setNegotiatedVersion(byte negotiatedVersion) {
        this.negotiatedVersion = negotiatedVersion;
    }

    public void setPendingHandshakeBytes(int bytes) {
        this.pendingHandshakeBytes = bytes;
    }

    public void setRecvBufferLen(int recvBufferLen) {
        this.recvBufferLen = recvBufferLen;
    }

    public void setWsHandshakeSent(boolean wsHandshakeSent) {
        this.wsHandshakeSent = wsHandshakeSent;
    }

    /**
     * Returns the native {@code ZSTD_CCtx} pointer, allocating it on first use.
     * Caller must already know compression is active (codec != COMPRESSION_NONE)
     * -- no null check is performed. Returns 0 if native allocation fails, which
     * {@code sendResultBatch} treats as "fall back to raw this batch".
     * The 0 return is also logged once per occurrence so an operator notices
     * silent compression downgrades caused by allocator pressure.
     */
    public long zstdCCtx() {
        if (zstdCCtx == 0) {
            zstdCCtx = Zstd.createCCtx(compressionLevel);
            if (zstdCCtx == 0) {
                LOG.error().$("zstd createCCtx returned 0 [level=").$(compressionLevel)
                        .$("]; batches will ship raw until the next allocation attempt succeeds").$();
            }
        }
        return zstdCCtx;
    }

    /**
     * Grows (or allocates) the compression scratch buffer to at least
     * {@code minCapacity} bytes and returns its native address. The buffer is
     * owned by this state and freed in {@link #clear()} / {@link #close()}.
     * <p>
     * Growth is geometric (at least 2x previous) so a connection whose batch
     * size drifts up over time amortises the realloc cost; without this, a
     * monotonically growing batch size would re-malloc on every batch.
     * {@link Unsafe#realloc} is used in place of {@code free + malloc} so the
     * allocator can extend the existing mapping in place when possible.
     */
    public long zstdCompressScratch(int minCapacity) {
        if (zstdCompressScratchCapacity < minCapacity) {
            // Round up to next 4 KiB so small growths don't thrash when batch
            // sizes drift slightly between queries; combine with a 2x geometric
            // term so larger growths amortise.
            long doubled = (long) zstdCompressScratchCapacity * 2L;
            long target = Math.max(doubled, minCapacity);
            target = (target + 4095L) & ~4095L;
            // Stay within int range; minCapacity is int so a 2x overflow is
            // bounded by 2 * Integer.MAX_VALUE which still fits in long.
            int cap = (int) Math.min(target, Integer.MAX_VALUE);
            zstdCompressScratchAddr = Unsafe.realloc(
                    zstdCompressScratchAddr, zstdCompressScratchCapacity, cap, MemoryTag.NATIVE_DEFAULT);
            zstdCompressScratchCapacity = cap;
        }
        return zstdCompressScratchAddr;
    }
}
