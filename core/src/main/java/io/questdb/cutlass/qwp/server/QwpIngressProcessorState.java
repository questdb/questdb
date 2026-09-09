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
import org.jetbrains.annotations.TestOnly;

/**
 * State management for QWP v1 processing.
 */
public class QwpIngressProcessorState implements QuietCloseable, ConnectionAware {
    static final int SEND_STATE_READY = 0;
    // Bounded grace the server waits for the client's CLOSE echo after sending a
    // role-change CLOSE whose final durable ack must not be lost. Closing the fd
    // immediately races the client's receive path: an abortive close (RST,
    // triggered by unread in-flight client frames) discards the client's unread
    // [durable ack][CLOSE] tail, forcing a full-corpus replay. RFC 6455 s5.5.1
    // instead: hold the fd open and keep reading until the echo proves the
    // client consumed the stream up to our CLOSE.
    //
    // Like ROLE_CHANGE_CLOSE_UPLOAD_GRACE_MICROS below, this is NOT a wall-clock
    // teardown bound -- expiry is polled only on inbound recv re-entry, so it is
    // a stall guard against a wedged-but-chatty peer. beginCloseEchoWaitIfEligible
    // half-closes the write side when it arms the wait (deferred while the socket
    // still owes TLS ciphertext, see hasPendingCloseEchoHalfClose), so a peer
    // that reads the CLOSE and falls silent still gets an EOF to react to. A peer
    // that answers neither the CLOSE nor the FIN escapes this budget entirely and
    // is left to the transport reaper.
    public static final long CLOSE_ECHO_WAIT_GRACE_MICROS = 5_000_000;
    // Bounded grace a role-change close may be deferred while
    // committed-but-not-yet-durably-uploaded work drains. The demote cascade
    // flips the engine read-only FIRST and completes pending WAL uploads AFTERWARDS,
    // so at gate-reject time the durable-ack watermark can lag this connection's
    // committed work by the in-flight upload latency; closing inside that lag loses
    // the final durable ack forever and forces a duplicate-producing client replay.
    //
    // NOTE: not a wall-clock teardown bound. Completion -- coverage OR expiry --
    // is evaluated ONLY on inbound, recv-driven re-entry: a gate-refused data
    // frame (handleBinaryMessage) or the client's durable-ack keepalive PING
    // (handlePing). A conformant client completes in milliseconds; a fully silent
    // client lingers deferred until the transport idle reaper tears the
    // connection down via onConnectionClosed (best-effort final flush, no CLOSE
    // frame). 10s is a stall guard for the live-client path.
    public static final long ROLE_CHANGE_CLOSE_UPLOAD_GRACE_MICROS = 10_000_000;
    // Budget for the post-CLOSE read-drain (gracefulCloseAndDrain in
    // QwpIngressUpgradeProcessor): after a server-initiated fatal CLOSE + FIN the
    // connection lingers, discarding inbound bytes, until the peer closes or this
    // deadline expires. Closing the fd under in-flight client frames would RST the
    // connection and destroy the final ACK/durable-ACK + CLOSE still queued in the
    // peer's receive buffer (Windows purges the rx queue on RST) -- the client
    // would replay batches the server already owns. Like the upload grace above,
    // the deadline is measured in MicrosecondClock ticks but enforced only on
    // inbound events: a conformant client reads the CLOSE and closes within
    // milliseconds (FIN exit); a live writer that never reads is cut off here; a
    // fully silent peer is reaped by the transport idle timeout.
    public static final long CLOSE_DRAIN_TIMEOUT_MICROS = 5_000_000;
    static final int SEND_STATE_RESUME_ACK = 1;
    static final int SEND_STATE_RESUME_ACK_THEN_CLOSE = 7;
    static final int SEND_STATE_RESUME_ACK_THEN_ERROR = 3;
    static final int SEND_STATE_RESUME_CLOSE = 6;
    // Parked close RESPONSE to a client-initiated CLOSE. Distinct from
    // SEND_STATE_RESUME_CLOSE (a parked server-initiated fatal CLOSE): this
    // resume path must tear the connection down unconditionally -- the client's
    // CLOSE is already consumed, so the handshake completes the instant the tail
    // flushes and no close-echo wait may be armed.
    static final int SEND_STATE_RESUME_CLOSE_RESPONSE = 11;
    // Continuation for a client-initiated CLOSE that arrived while an
    // ack/durable-ack send was parked. The parked frame carries the client's LAST
    // cumulative/durable ack -- dropping it makes a store-and-forward client
    // replay committed work after reconnect. The resume path finishes the ack,
    // emits the close response with pendingCloseResponseCode, then disconnects.
    static final int SEND_STATE_RESUME_ACK_THEN_CLOSE_RESPONSE = 12;
    static final int SEND_STATE_RESUME_DURABLE_ACK_THEN_CLOSE_RESPONSE = 13;
    static final int SEND_STATE_RESUME_DRAIN_THEN_CLOSE = 10;
    static final int SEND_STATE_RESUME_DURABLE_ACK = 4;
    static final int SEND_STATE_RESUME_DURABLE_ACK_THEN_CLOSE = 8;
    static final int SEND_STATE_RESUME_DURABLE_ACK_THEN_ERROR = 5;
    static final int SEND_STATE_RESUME_ERROR = 2;
    static final int SEND_STATE_RESUME_PONG = 9;
    private static final Log LOG = LogFactory.getLog(QwpIngressProcessorState.class);
    private final QwpTudCache.CommittedTxnConsumer committedTxnConsumer = this::recordCommittedTable;
    private final LineHttpProcessorConfiguration configuration;
    // Delta symbol dictionary for this connection
    private final ObjList<String> connectionSymbolDict = new ObjList<>();
    private final StringSink deferredCloseReason = new StringSink();
    private final StringSink deferredErrorMessage = new StringSink();
    private final CharSequenceLongHashMap durableProgressSnapshot = new CharSequenceLongHashMap();
    // Per-table dir names for the current durable progress snapshot, keyed by
    // table name. Kept in lock-step with durableProgressSnapshot so the wire
    // durable-ack entries can carry the table dir name as an incarnation
    // discriminator (drop/recreate under a stable table name changes the dir
    // name; the client uses the change to reset its per-table watermark).
    private final CharSequenceObjHashMap<String> durableProgressDirNames = new CharSequenceObjHashMap<>();
    private final CairoEngine engine;
    private final StringSink error = new StringSink();
    private final CharSequenceLongHashMap lastDurableSeqTxns = new CharSequenceLongHashMap();
    private final long maxBufferSize;
    private final int maxResponseErrorMessageLength;
    private final CharSequenceLongHashMap pendingAckSeqTxns = new CharSequenceLongHashMap();
    private final CharSequenceObjHashMap<String> pendingDurableDirNames = new CharSequenceObjHashMap<>();
    private final CharSequenceLongHashMap pendingDurableSeqTxns = new CharSequenceLongHashMap();
    private final StringSink rejectMsg = new StringSink();
    private final StringSink roleChangeCloseReason = new StringSink();
    private final CharSequenceLongHashMap resumeAckSeqTxns = new CharSequenceLongHashMap();
    private final ConnectionSymbolCache symbolCache = new ConnectionSymbolCache();
    private final CharSequenceObjHashMap<String> tableDirNames = new CharSequenceObjHashMap<>();
    private long bufferAddress;
    private int bufferPosition;
    private int bufferSize;
    // Deadline (MicrosecondClock ticks) for the post-CLOSE read-drain, or -1 when
    // no drain is in progress. Armed by gracefulCloseAndDrain after the fatal
    // CLOSE frame and FIN went out; while set, inbound frames are discarded
    // without touching the engine and the fd stays open so the peer can finish
    // reading the goodbye. Survives per-message clear(); reset on onDisconnected.
    private long closeDrainDeadline = -1;
    private Status currentStatus = Status.OK;
    private int deferredCloseCode = -1;
    private long deferredErrorSequence = -1;
    private byte deferredErrorStatus;
    private boolean durableAckEnabled;
    private long fd = -1;
    // Whether onHeadersReady wrote the 101 bytes into the send buffer but
    // deferred the actual rawSocket.send to onRequestComplete. Set true in
    // onHeadersReady, cleared in finalizeHandshake() after the send (and any
    // resumeSend re-flush) completes. The contract that onHeadersReady cannot
    // throw PeerIsSlowToReadException means a small send-fragmentation cap
    // (DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE) could otherwise drop
    // the second-fragment send without ever surfacing to the park/resume path.
    private boolean handshakeFlushPending;
    private long highestProcessedSequence = -1;
    private boolean isDurableProgressSnapshotFullyUploaded;
    private long lastAckedSequence = -1;
    private long messageSequence;
    private byte negotiatedVersion = QwpConstants.VERSION;
    // Bytes that onHeadersReady staged in the raw response buffer waiting to
    // be flushed by onRequestComplete. Carried across the two calls so
    // resumeSend can finalise after a parked write.
    private int pendingHandshakeBytes;
    private int recvBufferLen;
    private long resumeAckSequence = -1;
    // Normalized close code to echo in the deferred close response once the
    // parked ack ahead of it drains (SEND_STATE_RESUME_ACK_THEN_CLOSE_RESPONSE
    // / SEND_STATE_RESUME_DURABLE_ACK_THEN_CLOSE_RESPONSE), or -1 when no
    // client-close continuation is armed. Reset on onDisconnected().
    private int pendingCloseResponseCode = -1;
    // Set when a batch is refused because the node just flipped to a read-only
    // replica (an in-place PRIMARY->REPLICA demote). A TRANSIENT failover, not a
    // permanent auth failure -- the upgrade processor closes the connection with a
    // reconnect-eligible code instead of sending a SECURITY_ERROR that a
    // store-and-forward client would treat as a terminal HALT.
    private boolean roleChangeClosePending;
    // Deadline (MicrosecondClock ticks) for the close-echo wait, or -1 when no
    // wait is in progress. While set, the connection exists only to observe the
    // client's CLOSE echo (or FIN): inbound data frames are discarded, PINGs are
    // not answered, and no further acks are flushed. Survives per-message clears;
    // reset only on onDisconnected().
    private long closeEchoDeadline = -1;
    // Set when frame sync is lost during the close-echo wait: an inbound frame's
    // declared payload exceeds the recv buffer, so the echo can never be
    // recognised. While set the recv path must not parse buffered bytes (a
    // re-parse could misread mid-frame garbage as a fake CLOSE opcode) and
    // read-and-discards instead. Reset only on onDisconnected().
    private boolean hasLostCloseEchoSync;
    // Set when the arming half-close could not go out because the TLS socket still
    // held ciphertext of the CLOSE record (FIN would truncate it). While set,
    // every recv-driven re-entry retries the half-close. Always false for a plain
    // socket. Reset only on onDisconnected().
    private boolean hasPendingCloseEchoHalfClose;
    // Deadline (MicrosecondClock ticks) for a deferred role-change close, or -1
    // when no deferral is in progress. Unlike roleChangeClosePending this survives
    // per-message clear()/clearMessageState(): the deferral spans multiple inbound
    // events (gate-rejected frames, keepalive PINGs) until the durable-upload
    // registry covers pendingDurableSeqTxns or the grace budget expires.
    private long roleChangeCloseDeferredDeadline = -1;
    // Set immediately before the role-change CLOSE frame is emitted, whether or
    // not the close deferred first: eligibility depends on WHAT the close
    // delivers (the final durable ack flushed right before the CLOSE), not on
    // whether uploads happened to lag at the first rejected frame. It must NOT be
    // set while a deferral is merely armed -- the mark survives per-message
    // clears and parked-close resumes, so an early set would keep it true for the
    // whole grace budget with no CLOSE on the wire, and any unrelated fatal close
    // in that window would inherit role-change semantics.
    private boolean roleChangeCloseInitiated;
    // Lowest sequence number consumed from the wire but neither committed nor
    // answered with an error response. Only the role-change close path can
    // leave a sequence in this limbo: it consumes the sequence, then either
    // closes the connection or defers the close -- in both cases without an
    // error response, because the refusal is TRANSIENT and the client is
    // expected to replay from its acked watermark after the reconnect-eligible
    // close. Cleared only on disconnect: once a sequence is unresolved, the
    // connection's sole legitimate exit is that close. The cumulative-ack
    // watermark must never reach this sequence -- a cumulative OK ack confirms
    // every sequence up to and including its value, so an ack at or past an
    // unresolved sequence would make a store-and-forward client trim rows the
    // server never wrote. Enforced in setHighestProcessedSequence.
    private long firstUnresolvedSequence = -1;
    private SecurityContext securityContext;
    // Lowest sequence of a FLAG_DEFER_COMMIT frame that appended NO rows and is
    // still uncovered; -1 when there is none. Survives per-message clear();
    // reset by a successful commitAll and by onDisconnected().
    private long firstRowlessDeferredSequence = -1;
    // True when the message currently being processed appended at least one row.
    // Reset at the top of every processMessage(), so it describes THIS frame and
    // not the connection. A frame that registers symbols and nothing else -- the
    // client's dictionary-chunk frames, which carry tableCount=0 and
    // FLAG_DEFER_COMMIT -- leaves it false.
    private boolean messageAppendedRows;
    // True while WAL rows appended by FLAG_DEFER_COMMIT frames remain
    // uncommitted. Updated from the force-commit traversal after every deferred
    // frame, cleared when commitAll() succeeds (the group-closing commit frame)
    // or when clear() rolls the rows back, and set
    // outright by markUncommittedDeferredRows where the fact is known without a
    // cache to ask. While true, the cumulative-ack watermark must not advance:
    // a cumulative OK ack confirms every sequence up to and including its
    // value, so covering an uncommitted deferred frame would let a
    // store-and-forward client trim slots whose rows the server can still roll
    // back -- silent data loss. This is the contract stated (but previously not
    // enforced) by the deferred-commit feature (#7144): "the client's
    // store-and-forward replays all unacknowledged messages on reconnect" --
    // deferred frames must therefore stay unacknowledged until their rows are
    // committed. Enforced in setHighestProcessedSequence as a last resort; the
    // upgrade processor's ack path must not attempt the advance to begin with.
    private boolean uncommittedDeferredRows;
    private int sendState = SEND_STATE_READY;
    private QwpStreamingDecoder streamingDecoder;
    private QwpTudCache tudCache;
    private QwpWalAppender walAppender;
    private boolean wsHandshakeSent;

    public QwpIngressProcessorState(
            int initBufferSize,
            int maxResponseContentLength,
            CairoEngine engine,
            LineHttpProcessorConfiguration configuration
    ) {
        assert initBufferSize > 0;
        this.engine = engine;
        this.configuration = configuration;
        this.maxBufferSize = Math.min(configuration.getMaxRecvBufferSize(), Integer.MAX_VALUE);
        this.maxResponseErrorMessageLength = Math.max(0, (int) ((maxResponseContentLength - 100) / 1.5));
        try {
            this.streamingDecoder = new QwpStreamingDecoder(
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
                    configuration.getDefaultPartitionBy(),
                    -1,
                    configuration.getQwpMaxUncommittedRows()
            );
            // Salvage commits in evictStaleTud bypass commitAll(consumer), so
            // the cache needs its own hook into recordCommittedTable to keep
            // durable-ack bookkeeping in sync with a salvaged txn.
            this.tudCache.setCommittedTxnConsumer(committedTxnConsumer);

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
        handshakeFlushPending = false;
        pendingHandshakeBytes = 0;
        roleChangeClosePending = false;
        // tudCache.clear() above rolled back any uncommitted deferred rows;
        // the deferred group is gone, its frames were never acked, and the
        // client replays them from its acked watermark.
        uncommittedDeferredRows = false;
    }

    /**
     * Resets per-message parsing buffers without rolling back WAL state.
     * Used between deferred-commit messages where the accumulated WAL
     * rows must survive until the final commit.
     */
    public void clearMessageState() {
        error.clear();
        currentStatus = Status.OK;
        bufferPosition = 0;
        streamingDecoder.reset();
        roleChangeClosePending = false;
    }

    @Override
    public void close() {
        final var tudCacheToFree = tudCache;
        tudCache = null;
        Throwable cleanupFailure = Misc.freeBestEffort(null, tudCacheToFree);
        final var streamingDecoderToFree = streamingDecoder;
        streamingDecoder = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, streamingDecoderToFree);
        final var walAppenderToFree = walAppender;
        walAppender = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, walAppenderToFree);
        // The native buffer free cannot throw and must run even when a free
        // above failed -- previously a tudCache close failure leaked it.
        if (bufferAddress != 0) {
            Unsafe.free(bufferAddress, bufferSize, MemoryTag.NATIVE_HTTP_CONN);
            bufferAddress = 0;
        }
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    /**
     * Collects per-table durable progress from the registry. Returns the
     * snapshot map (owned by this instance) containing only tables whose
     * durable seqTxn has advanced since the last durable ack was sent. The
     * same traversal also determines whether the registry covers all pending
     * work; callers can read that result via
     * {@link #isDurableProgressSnapshotFullyUploaded()} until the next call.
     * The caller must consume the map before the next call.
     * <p>
     * Only iterates tables with outstanding durable work, not every table
     * the connection has ever written to.
     */
    public CharSequenceLongHashMap collectDurableProgress(DurableAckRegistry registry) {
        durableProgressSnapshot.clear();
        isDurableProgressSnapshotFullyUploaded = true;
        if (!durableAckEnabled) {
            return durableProgressSnapshot;
        }
        ObjList<CharSequence> tableNames = pendingDurableSeqTxns.keys();
        for (int i = 0, n = tableNames.size(); i < n; i++) {
            CharSequence tableName = tableNames.getQuick(i);
            String dirName = pendingDurableDirNames.get(tableName);
            if (dirName == null) {
                isDurableProgressSnapshotFullyUploaded = false;
                continue;
            }
            long uploadedSeqTxn = registry.getDurablyUploadedSeqTxn(dirName);
            if (uploadedSeqTxn < pendingDurableSeqTxns.get(tableName)) {
                isDurableProgressSnapshotFullyUploaded = false;
            }
            if (uploadedSeqTxn >= 0) {
                long lastSent = lastDurableSeqTxns.get(tableName);
                if (uploadedSeqTxn > lastSent) {
                    durableProgressSnapshot.put(tableName, uploadedSeqTxn);
                    durableProgressDirNames.put(tableName, dirName);
                }
            }
        }
        return durableProgressSnapshot;
    }

    public void commit() {
        try {
            tudCache.commitAll(committedTxnConsumer);
            // commitAll covers every table this connection buffered rows for,
            // including rows accumulated by FLAG_DEFER_COMMIT frames -- the
            // deferred group (if any) is now durable in the WAL and the
            // cumulative-ack watermark may advance over it.
            uncommittedDeferredRows = false;
            // commitAll covered every frame of the group, rowless ones included.
            firstRowlessDeferredSequence = -1;
        } catch (Throwable th) {
            tudCache.setDistressed();
            LOG.error().$('[').$(fd).$("] commit error: ").$(th).$();
            rejectCommitError(th);
        }
    }

    public void commitIfMaxUncommittedRowsReached() {
        try {
            uncommittedDeferredRows = tudCache.commitIfMaxUncommittedRowsReached(committedTxnConsumer);
        } catch (Throwable th) {
            tudCache.setDistressed();
            LOG.error().$('[').$(fd).$("] deferred commit error: ").$(th).$();
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
            CharSequence tableName = keys.getQuick(i);
            CharSequence dirName = durableProgressDirNames.get(tableName);
            size += 2 + Utf8s.utf8Bytes(tableName) + 2 + Utf8s.utf8Bytes(dirName) + 8;
        }
        return size;
    }

    public int getDeferredCloseCode() {
        return deferredCloseCode;
    }

    public CharSequence getDeferredCloseReason() {
        return deferredCloseReason;
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

    public CharSequenceLongHashMap getDurableProgressSnapshot() {
        return durableProgressSnapshot;
    }

    public CharSequenceObjHashMap<String> getDurableProgressDirNames() {
        return durableProgressDirNames;
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

    public int getPendingHandshakeBytes() {
        return pendingHandshakeBytes;
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

    @TestOnly
    public QwpStreamingDecoder getStreamingDecoder() {
        return streamingDecoder;
    }

    /**
     * True when frame sync was lost during the close-echo wait and the recv
     * path is in read-and-discard mode -- see {@link #onCloseEchoSyncLost}.
     */
    public boolean hasLostCloseEchoSync() {
        return hasLostCloseEchoSync;
    }

    /**
     * Returns true if there are successfully processed messages that haven't been
     * ACKed yet and the send buffer is clear (READY state).
     */
    public boolean hasPendingAck() {
        return sendState == SEND_STATE_READY && highestProcessedSequence > lastAckedSequence;
    }

    /**
     * True while the close-echo wait still owes the peer its write-side
     * half-close -- see {@link #onCloseEchoHalfCloseDeferred}.
     */
    public boolean hasPendingCloseEchoHalfClose() {
        return hasPendingCloseEchoHalfClose;
    }

    /**
     * True when this connection has committed seqTxns whose durable-upload
     * coverage has not yet been fully acked. Unlike
     * {@link #isDurableWorkFullyUploaded} this reads only local state, so it is
     * immune to the registry advancing concurrently under a demote drain.
     */
    public boolean hasPendingDurableWork() {
        return pendingDurableSeqTxns.size() > 0;
    }

    /**
     * Whether the message just processed appended at least one row.
     * <p>
     * A deferred frame may only be ack-covered when THIS frame's own rows are
     * durable. "Nothing is uncommitted on the connection" is not the same claim:
     * it is vacuously true for a frame that appended nothing.
     *
     * @see #markRowlessDeferredSequence(long) for why a rowless deferred frame
     * needs more than withholding its own ack
     */
    @TestOnly
    public boolean hasAppendedRowsInMessage() {
        return messageAppendedRows;
    }

    /**
     * True while WAL rows appended by FLAG_DEFER_COMMIT frames remain
     * uncommitted. While true, no cumulative OK ack may cover the deferred
     * frames' sequences -- see {@link #setHighestProcessedSequence}.
     * <p>
     * {@code QwpIngressUpgradeProcessor.handleBinaryMessage} also reads it to
     * decide whether an outgoing ack closes a run of withheld deferred frames
     * and so must flush eagerly rather than wait for the batch threshold.
     */
    public boolean hasUncommittedDeferredRows() {
        return uncommittedDeferredRows;
    }

    public boolean isDeferCommit() {
        if (bufferPosition >= QwpConstants.HEADER_SIZE) {
            byte flags = Unsafe.getByte(bufferAddress + QwpConstants.HEADER_OFFSET_FLAGS);
            return (flags & QwpConstants.FLAG_DEFER_COMMIT) != 0;
        }
        return false;
    }

    public boolean isDurableAckEnabled() {
        return durableAckEnabled;
    }

    public boolean isHandshakeFlushPending() {
        return handshakeFlushPending;
    }

    public boolean isOk() {
        return currentStatus == Status.OK;
    }

    /**
     * True while the server has sent its role-change CLOSE frame and is holding
     * the connection open awaiting the client's CLOSE echo (RFC 6455 close
     * handshake) to confirm delivery of the final durable ack.
     */
    public boolean isAwaitingCloseEcho() {
        return closeEchoDeadline != -1;
    }

    /**
     * True when the close-echo wait has exhausted its grace budget and the
     * connection must be torn down without echo confirmation (availability over
     * the duplicate guard). Always false when no echo wait is in progress.
     */
    public boolean isCloseEchoWaitExpired() {
        return closeEchoDeadline != -1
                && configuration.getMicrosecondClock().getTicks() >= closeEchoDeadline;
    }

    public boolean isRoleChangeClosePending() {
        return roleChangeClosePending;
    }

    /**
     * Starts the bounded wait for the client's CLOSE echo after the role-change
     * CLOSE frame (preceded by the final durable ack) has been fully sent.
     * No-op when already waiting, so a follow-on poll cannot extend the deadline.
     */
    public void beginCloseEchoWait() {
        if (closeEchoDeadline == -1) {
            closeEchoDeadline = configuration.getMicrosecondClock().getTicks()
                    + CLOSE_ECHO_WAIT_GRACE_MICROS;
        }
    }

    /**
     * Starts (or keeps, if already started) the bounded deferral of a role-change
     * close, stashing {@code reason} for the eventual CLOSE frame. No-op when a
     * deferral is already in progress so the deadline is never extended by
     * follow-on gate-rejected frames.
     */
    public void deferRoleChangeClose(CharSequence reason) {
        if (roleChangeCloseDeferredDeadline == -1) {
            roleChangeCloseDeferredDeadline = configuration.getMicrosecondClock().getTicks()
                    + ROLE_CHANGE_CLOSE_UPLOAD_GRACE_MICROS;
            roleChangeCloseReason.clear();
            if (reason != null) {
                roleChangeCloseReason.put(reason);
            }
        }
    }

    public CharSequence getRoleChangeCloseReason() {
        return roleChangeCloseReason;
    }

    /**
     * True while a role-change close is deferred awaiting durable-upload coverage
     * of this connection's committed work.
     */
    public boolean isRoleChangeCloseDeferred() {
        return roleChangeCloseDeferredDeadline != -1;
    }

    /**
     * True once this connection has emitted (or is emitting) its role-change
     * CLOSE, whether that close deferred awaiting upload coverage or completed
     * on the first attempt. This -- not {@link #isRoleChangeCloseDeferred()} --
     * is the close-echo eligibility predicate: the CLOSE frame carries the final
     * durable ack in both cases. False for the whole deferral window.
     */
    public boolean isRoleChangeCloseInitiated() {
        return roleChangeCloseInitiated;
    }

    /**
     * Marks this connection as closing due to a role change (in-place
     * PRIMARY-&gt;REPLICA demote). Called immediately before the role-change
     * CLOSE is handed to {@code sendFatalClose} -- never while the close is only
     * deferred. Idempotent; reset only on {@code onDisconnected()}.
     */
    public void initiateRoleChangeClose() {
        roleChangeCloseInitiated = true;
    }

    /**
     * True when a deferred role-change close has exhausted its grace budget and
     * must proceed even with un-acked durable work (availability over the
     * duplicate guard). Always false when no deferral is in progress.
     */
    public boolean isRoleChangeCloseGraceExpired() {
        return roleChangeCloseDeferredDeadline != -1
                && configuration.getMicrosecondClock().getTicks() >= roleChangeCloseDeferredDeadline;
    }

    /**
     * Arms the post-CLOSE read-drain: the fatal CLOSE frame and FIN are on the
     * wire, and from here until the peer closes (or the
     * {@link #CLOSE_DRAIN_TIMEOUT_MICROS} budget runs out) the connection only
     * consumes and discards inbound bytes. Idempotent so a late re-entry cannot
     * extend the deadline.
     */
    public void beginCloseDrain() {
        if (closeDrainDeadline == -1) {
            closeDrainDeadline = configuration.getMicrosecondClock().getTicks()
                    + CLOSE_DRAIN_TIMEOUT_MICROS;
        }
    }

    /**
     * True while the connection lingers in the post-CLOSE read-drain awaiting
     * the peer's close (or the drain deadline).
     */
    public boolean isCloseDraining() {
        return closeDrainDeadline != -1;
    }

    /**
     * True when the post-CLOSE read-drain has exhausted its budget and the
     * connection must disconnect even though the peer has not closed. Always
     * false when no drain is in progress.
     */
    public boolean isCloseDrainExpired() {
        return closeDrainDeadline != -1
                && configuration.getMicrosecondClock().getTicks() >= closeDrainDeadline;
    }

    /**
     * Returns the full-coverage result produced by the most recent
     * {@link #collectDurableProgress(DurableAckRegistry)} traversal.
     */
    public boolean isDurableProgressSnapshotFullyUploaded() {
        return isDurableProgressSnapshotFullyUploaded;
    }

    /**
     * True when every seqTxn this connection has committed but not yet durably
     * acked is covered by the registry's durable-upload watermark -- i.e. a
     * durable ack flushed right now would advance the client's replay watermark
     * past ALL of this connection's committed work, leaving no replay window.
     * Trivially true when nothing is pending (or durable ack is disabled:
     * {@code pendingDurableSeqTxns} is only populated when enabled).
     * <p>
     * This coverage-only traversal exists for the send-blocked path, where
     * {@code durableProgressSnapshot} may belong to an in-flight durable ACK
     * and must remain unchanged until {@link #onDurableAckSent()} consumes it.
     */
    public boolean isDurableWorkFullyUploaded(DurableAckRegistry registry) {
        ObjList<CharSequence> tableNames = pendingDurableSeqTxns.keys();
        for (int i = 0, n = tableNames.size(); i < n; i++) {
            CharSequence tableName = tableNames.getQuick(i);
            String dirName = pendingDurableDirNames.get(tableName);
            if (dirName == null || registry.getDurablyUploadedSeqTxn(dirName) < pendingDurableSeqTxns.get(tableName)) {
                return false;
            }
        }
        return true;
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

    /**
     * Records a sequence number that was consumed from the wire but will get
     * neither a commit nor an error response -- the role-change close paths.
     * Keeps the minimum across calls; {@link #setHighestProcessedSequence}
     * refuses to advance the cumulative-ack watermark to or past it.
     */
    public void markSequenceUnresolved(long seq) {
        if (firstUnresolvedSequence == -1 || seq < firstUnresolvedSequence) {
            firstUnresolvedSequence = seq;
        }
    }

    // Survives per-message clear(); reset only on onDisconnected().
    public boolean hasUnresolvedSequence() {
        return firstUnresolvedSequence != -1;
    }

    /**
     * Records that a FLAG_DEFER_COMMIT frame which appended NO rows was withheld
     * at {@code seq}, so no later ack may cover it until the group commits.
     * Keeps the minimum across calls.
     * <p>
     * Withholding such a frame's own ack is not enough, because an OK ack is
     * CUMULATIVE and {@link #setHighestProcessedSequence} assigns rather than
     * increments: a later row-bearing deferred frame whose own rows have become
     * durable would otherwise jump the watermark straight over it.
     * <p>
     * The row-bearing case needs no such marker, but not because those frames
     * are self-contained -- in delta-dictionary mode they carry a delta section
     * that later frames reference by id. It is safe because the client can
     * re-derive that: it keeps a lifetime mirror of every delta it has sent and
     * replays the whole dictionary as a catch-up before any post-reconnect
     * traffic, so trimming such a frame orphans nothing.
     * <p>
     * A rowless deferred frame is the case with no such re-derivation. The
     * full-dictionary chunks of the client's dictionary-chunk publisher carry symbol
     * state that the data frames after them reference, and that mode
     * deliberately sends no catch-up. Its own contract says so -- the chunks
     * "cannot be trimmed away ahead of the data frames that depend on them".
     */
    public void markRowlessDeferredSequence(long seq) {
        if (firstRowlessDeferredSequence == -1 || seq < firstRowlessDeferredSequence) {
            firstRowlessDeferredSequence = seq;
        }
    }

    /**
     * Records a FLAG_DEFER_COMMIT frame at {@code seq} whose ack is being
     * withheld. A frame that appended rows needs nothing beyond the withholding
     * itself -- once its rows commit, a later cumulative ack may cover it. A
     * rowless one must additionally block coverage; see
     * {@link #markRowlessDeferredSequence(long)}.
     */
    public void withholdDeferredFrame(long seq) {
        if (!messageAppendedRows) {
            markRowlessDeferredSequence(seq);
        }
    }

    /**
     * Arms the deferred-rows clamp outright, without asking the TUD cache.
     * <p>
     * No production caller remains: the deferred-frame path derives the clamp
     * through {@link #commitIfMaxUncommittedRowsReached()}. Kept so tests can put
     * the clamp into a known state without standing up a cache that holds
     * uncommitted rows. Cleared by {@link #commit()} (successful commitAll) or
     * {@link #clear()} (rollback).
     */
    @TestOnly
    public void markUncommittedDeferredRows() {
        uncommittedDeferredRows = true;
    }

    /**
     * Recomputes the deferred-rows clamp from what this connection actually
     * holds uncommitted, and returns the result.
     * <p>
     * The per-table force-commit at the max-uncommitted-rows cap fires for
     * whichever tables crossed the cap, so after a deferred frame the
     * connection may hold everything, something, or nothing uncommitted. The
     * clamp exists to keep the cumulative-ack watermark off sequences whose
     * rows the server can still roll back (#7144); once a force-commit has left
     * nothing uncommitted, the deferred frames are as durable as a committed
     * one and the watermark may cover them. Acking there is what keeps a
     * mid-group reconnect from replaying -- and so duplicating -- rows the
     * server has already written.
     * <p>
     * The clamp releases only when NO live table holds uncommitted rows; any
     * table still holding rows keeps it set, so the "ack implies committed"
     * guarantee is unchanged.
     *
     * @return {@code true} if rows remain uncommitted (the watermark stays clamped)
     */
    public boolean refreshUncommittedDeferredRows() {
        uncommittedDeferredRows = tudCache.hasUncommittedRows();
        return uncommittedDeferredRows;
    }

    /**
     * Answers whether a cumulative OK ack may cover the FLAG_DEFER_COMMIT frame
     * just processed. The preceding force-commit traversal updates the clamp,
     * so this decision does not rescan the table cache. This is the whole ack
     * decision for the deferred path; {@code QwpIngressUpgradeProcessor.handleBinaryMessage}
     * only wires it up.
     * <p>
     * Both terms are load-bearing, and each guards a different failure:
     * <ul>
     * <li>Nothing uncommitted -- otherwise an ack would let a store-and-forward
     * client trim rows the server can still roll back (#7144).</li>
     * <li>This frame appended rows -- "nothing uncommitted" is vacuously true
     * for a frame that appended none, and the client emits exactly such frames:
     * its dictionary chunks carry {@code tableCount=0} with FLAG_DEFER_COMMIT,
     * and the data frames after them reference the ids those chunks register.
     * Acking a chunk would let the client trim it away from the frames that
     * depend on it, and a reconnect would then replay symbol ids nothing
     * registered.</li>
     * </ul>
     * The force-commit traversal runs even for a frame that appended nothing,
     * so the clamp cannot stay stale from an earlier frame.
     *
     * @return {@code true} if the ack may cover this frame
     */
    public boolean refreshDeferredAckCoverage() {
        return !uncommittedDeferredRows && messageAppendedRows;
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
     * Records that the arming half-close of the close-echo wait was deferred
     * because the socket still held pending TLS write data. Every recv-driven
     * re-entry retries the half-close until the ciphertext drains, so the peer
     * still gets FIN behind an intact CLOSE record.
     */
    public void onCloseEchoHalfCloseDeferred() {
        hasPendingCloseEchoHalfClose = true;
    }

    /**
     * Records that the close-echo wait's write-side half-close went out, so no
     * re-entry retries it.
     */
    public void onCloseEchoHalfClosed() {
        hasPendingCloseEchoHalfClose = false;
    }

    /**
     * Records that WebSocket frame sync died during the close-echo wait: a
     * too-big inbound frame jammed the recv machinery, so the CLOSE echo can
     * never be parsed. The recv path switches to read-and-discard until the
     * echo grace expires or the peer's FIN arrives.
     */
    public void onCloseEchoSyncLost() {
        hasLostCloseEchoSync = true;
    }

    /**
     * Records that the close response to a client-initiated CLOSE was
     * partially flushed to the OS buffer. The framework's send buffer holds
     * the rest; the resume path finishes the flush and disconnects
     * unconditionally -- the client's CLOSE is already consumed, so the close
     * handshake is complete once the tail lands and no echo wait applies.
     */
    public void onCloseResponseSendBlocked() {
        sendState = SEND_STATE_RESUME_CLOSE_RESPONSE;
    }

    /**
     * Records that a client-initiated CLOSE arrived while its pre-response ack
     * flush left a send parked. Stores the normalized close code for the resume
     * path and chains the send state so the ack frame -- the client's LAST
     * cumulative/durable ack -- is not lost to the teardown. The caller must
     * propagate the write backpressure.
     */
    public void onClientCloseBlockedBehindAck(int responseCode) {
        assert sendState == SEND_STATE_RESUME_ACK || sendState == SEND_STATE_RESUME_DURABLE_ACK
                : "onClientCloseBlockedBehindAck called in wrong state: " + sendState;
        pendingCloseResponseCode = responseCode;
        if (sendState == SEND_STATE_RESUME_ACK) {
            sendState = SEND_STATE_RESUME_ACK_THEN_CLOSE_RESPONSE;
        } else if (sendState == SEND_STATE_RESUME_DURABLE_ACK) {
            sendState = SEND_STATE_RESUME_DURABLE_ACK_THEN_CLOSE_RESPONSE;
        }
    }

    public int getPendingCloseResponseCode() {
        return pendingCloseResponseCode;
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
                int dpdIdx = durableProgressDirNames.keyIndex(tableName);
                if (dpdIdx < 0) {
                    durableProgressDirNames.removeAt(dpdIdx);
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
        streamingDecoder.reset();
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
        clearDeferredClose();
        closeDrainDeadline = -1;
        closeEchoDeadline = -1;
        hasLostCloseEchoSync = false;
        hasPendingCloseEchoHalfClose = false;
        pendingCloseResponseCode = -1;
        roleChangeCloseDeferredDeadline = -1;
        roleChangeCloseInitiated = false;
        roleChangeCloseReason.clear();
        firstUnresolvedSequence = -1;
        firstRowlessDeferredSequence = -1;
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
        durableProgressDirNames.clear();
        isDurableProgressSnapshotFullyUploaded = false;
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
     * Records that the CLOSE frame itself was partially flushed to the OS buffer.
     * The framework's send buffer holds the rest; the resume path will finish
     * flushing and then disconnect. Deferred close-code/reason are no longer
     * needed because the bytes are already written.
     */
    public void onFatalCloseSendBlocked() {
        clearDeferredClose();
        sendState = SEND_STATE_RESUME_CLOSE;
    }

    /**
     * Records that a fatal CLOSE could not be written immediately because an
     * ACK/durable-ACK/error response is in flight. Stores the close code and
     * reason so the resume path can emit the CLOSE frame after the in-flight
     * response drains.
     */
    public void onFatalCloseBlocked(int closeCode, CharSequence reason) {
        deferredCloseCode = closeCode;
        deferredCloseReason.clear();
        if (reason != null) {
            deferredCloseReason.put(reason);
        }

        if (sendState == SEND_STATE_RESUME_ACK || sendState == SEND_STATE_RESUME_ACK_THEN_ERROR
                || sendState == SEND_STATE_RESUME_ACK_THEN_CLOSE) {
            sendState = SEND_STATE_RESUME_ACK_THEN_CLOSE;
        } else if (sendState == SEND_STATE_RESUME_DURABLE_ACK
                || sendState == SEND_STATE_RESUME_DURABLE_ACK_THEN_ERROR
                || sendState == SEND_STATE_RESUME_DURABLE_ACK_THEN_CLOSE) {
            sendState = SEND_STATE_RESUME_DURABLE_ACK_THEN_CLOSE;
        } else if (sendState == SEND_STATE_RESUME_CLOSE || sendState == SEND_STATE_RESUME_CLOSE_RESPONSE) {
            // The parked bytes ARE a CLOSE frame -- a previous fatal close or
            // the close response to a client-initiated CLOSE was partially
            // flushed. Either way a CLOSE is already on the wire and nothing
            // may follow it (RFC 6455); the resume path finishes that flush
            // and disconnects, so the just-stored code/reason are redundant.
            clearDeferredClose();
        } else if (sendState == SEND_STATE_RESUME_ACK_THEN_CLOSE_RESPONSE
                || sendState == SEND_STATE_RESUME_DURABLE_ACK_THEN_CLOSE_RESPONSE) {
            // A close response to a client-initiated CLOSE is already queued
            // behind the parked ack. That response IS a CLOSE frame and the
            // continuation disconnects right after it, so the fatal close is
            // redundant: keep the continuation (it still delivers the final
            // ack first) and drop the just-stored code/reason.
            clearDeferredClose();
        } else {
            // RESUME_PONG / RESUME_ERROR (or a re-entered DRAIN_THEN_CLOSE):
            // some non-ack response is parked. Drain it on resume, then flush
            // final ack/durable-ack progress and emit the stored CLOSE frame.
            // The previous collapse to RESUME_CLOSE was wrong on two counts:
            // RESUME_CLOSE assumes the parked bytes are the CLOSE frame
            // itself, so the deferred CLOSE was never written (the client saw
            // a bare FIN with no close code), and the final durable ack was
            // dropped with it, leaving a durable-ack client's replay
            // watermark stale (reconnect-replay duplicates).
            clearDeferredError();
            sendState = SEND_STATE_RESUME_DRAIN_THEN_CLOSE;
        }
    }

    /**
     * Records that a PONG send was blocked by a full OS buffer.
     * Transitions from READY to RESUME_PONG; the framework parks the
     * connection for write so resumeSend can drain the parked bytes.
     */
    public void onPongBlocked() {
        sendState = SEND_STATE_RESUME_PONG;
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
     * Completes a resumed fatal CLOSE send that was previously blocked. Returns
     * to READY so the recv-side drains keep working while the connection is held
     * open awaiting the client's CLOSE echo.
     */
    public void onResumeCloseComplete() {
        sendState = SEND_STATE_READY;
    }

    /**
     * Completes the drain of a parked non-ack response (pong or error
     * response) that a deferred fatal CLOSE was queued behind. Returns to
     * READY so the caller can flush final ack/durable-ack progress and emit
     * the deferred CLOSE frame.
     */
    public void onResumeDrainComplete() {
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

    /**
     * Completes a resumed PONG send that was previously blocked. No further
     * follow-up work is required -- the pong frame is the entire response,
     * so the connection just returns to READY for the next recv cycle.
     */
    public void onResumePongComplete() {
        sendState = SEND_STATE_READY;
    }

    public void processMessage() {
        if (bufferPosition == 0 || !isOk()) {
            return;
        }

        messageAppendedRows = false;
        try {
            // Re-check the LIVE write-refusal state on every batch. A QWP ingress connection
            // caches its SecurityContext (and a per-table WAL writer) at handshake time, so a
            // connection opened while this node was PRIMARY would otherwise keep writing after an
            // in-place PRIMARY-to-REPLICA switch flips the node read-only -- the cached state
            // bypasses per-write authorization entirely. Consulting engine.isReadOnlyMode() here
            // closes that boundary race: the instant the node starts demoting (the dynamic
            // read-only-replica flag flips FIRST in the switch cascade), the whole batch is
            // refused. HOW it is refused depends on WHY the node is read-only -- see
            // isStaticReadOnlyInstance() for the contract.
            if (engine.isReadOnlyMode()) {
                if (isStaticReadOnlyInstance()) {
                    // Statically read-only (readonly=true in the server config): a permanent
                    // property of this process, not a role transition. The role-change close
                    // is WRONG here because its 421 backstop does not exist -- the upgrade
                    // gate rejects only ROLE_REPLICA / ROLE_PRIMARY_CATCHUP, and a statically
                    // read-only node keeps reporting its upgrade-eligible role (STANDALONE on
                    // OSS) forever. A store-and-forward client treats the resulting
                    // NORMAL_CLOSURE close as orderly (no NACK, no poison strike, no typed
                    // terminal) and would reconnect-replay in a silent infinite loop, its
                    // producer never learning of the misconfiguration. Answer with the typed
                    // SECURITY_ERROR NACK instead: the client latches it as terminal and
                    // surfaces it loudly.
                    reject(Status.SECURITY_ERROR, CairoException.READ_ONLY_ACCESS_MESSAGE, fd);
                    return;
                }
                // INVARIANT B: role-derived read-only means an in-place PRIMARY-to-REPLICA
                // demote is underway -- a TRANSIENT failover (the node can be promoted
                // back / a sibling primary may exist), NOT a permanent auth failure. Do
                // not reject the batch as an authorization error (which maps to
                // SECURITY_ERROR and a store-and-forward client treats as a terminal
                // HALT). Flag the connection for a reconnect-eligible close instead: the
                // client reconnects, hits the 421 role reject on the now-replica
                // endpoint, and retries from SF until a primary is reachable.
                roleChangeClosePending = true;
                reject(Status.NOT_ACCEPTING_WRITES, CairoException.READ_ONLY_ACCESS_MESSAGE, fd);
                return;
            }

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

            // A redefined client symbol dictionary (orphan-adoption replays a
            // different sender's dict-from-0 into this connection, remapping
            // existing client symbol IDs to new strings) leaves the
            // clientSymbolId -> tableSymbolId cache pointing at the prior
            // sender's keys. Drop it so symbols re-resolve against the
            // refreshed dictionary; the watermark check alone cannot catch a
            // remap that adds no new table symbols.
            if (messageCursor.isSymbolDictRedefined()) {
                symbolCache.clear();
            }

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

                // Read BEFORE the append: the cursor is consumed by it, and a
                // metadata-change retry resets row iteration.
                final boolean blockHasRows = tableBlock.getRowCount() > 0;
                walAppender.appendToWalStreaming(securityContext, tableBlock, tud);
                messageAppendedRows |= blockHasRows;
            }

        } catch (QwpParseException e) {
            if (e.getErrorCode() != QwpParseException.ErrorCode.DELTA_DICT_GAP) {
                streamingDecoder.releaseCachedResources();
            }
            LOG.error().$('[').$(fd).$("] QWP v1 parse error: ").$(e.getFlyweightMessage()).$();
            reject(statusForParseError(e.getErrorCode()), e.getFlyweightMessage(), fd);
        } catch (CommitFailedException e) {
            LOG.error().$('[').$(fd).$("] commit failed: ").$(e.getMessage()).$();
            tudCache.setDistressed();
            rejectCommitError(e.getReason());
        } catch (CairoException e) {
            LOG.error().$('[').$(fd).$("] cairo error: ").$(e.getFlyweightMessage()).$();
            rejectCairoError(e);
        } catch (Throwable e) {
            LOG.critical().$('[').$(fd).$("] unexpected error: ").$(e).$();
            tudCache.setDistressed();
            rejectMsg.clear();
            rejectMsg.put("unexpected error: ").put(e.getMessage());
            reject(Status.INTERNAL_ERROR, rejectMsg, fd);
        }
    }

    /**
     * Re-parks a deferred fatal CLOSE behind an ack/durable-ack send that
     * blocked during the resume path's pre-close flush
     * ({@code QwpIngressUpgradeProcessor#finishDeferredFatalClose}). The
     * deferred close code/reason are already stored from the original
     * {@link #onFatalCloseBlocked} call, so this performs only the state
     * transition. Routing the stored reason back through onFatalCloseBlocked
     * would clear the reason sink and then append it to itself, losing the
     * reason.
     */
    public void reArmDeferredFatalClose() {
        assert sendState == SEND_STATE_RESUME_ACK || sendState == SEND_STATE_RESUME_DURABLE_ACK
                : "reArmDeferredFatalClose called in wrong state: " + sendState;
        if (sendState == SEND_STATE_RESUME_ACK) {
            sendState = SEND_STATE_RESUME_ACK_THEN_CLOSE;
        } else if (sendState == SEND_STATE_RESUME_DURABLE_ACK) {
            sendState = SEND_STATE_RESUME_DURABLE_ACK_THEN_CLOSE;
        }
    }

    /**
     * Re-parks the client-close response continuation behind an ack send that
     * blocked during the resume path's pre-response flush. The response code is
     * already stored by {@link #onClientCloseBlockedBehindAck(int)}, so this is
     * the state transition only -- the mirror of
     * {@link #reArmDeferredFatalClose()}.
     */
    public void reArmClientCloseResponse() {
        assert sendState == SEND_STATE_RESUME_ACK || sendState == SEND_STATE_RESUME_DURABLE_ACK
                : "reArmClientCloseResponse called in wrong state: " + sendState;
        if (sendState == SEND_STATE_RESUME_ACK) {
            sendState = SEND_STATE_RESUME_ACK_THEN_CLOSE_RESPONSE;
        } else if (sendState == SEND_STATE_RESUME_DURABLE_ACK) {
            sendState = SEND_STATE_RESUME_DURABLE_ACK_THEN_CLOSE_RESPONSE;
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

    public void setHandshakeFlushPending(boolean pending) {
        this.handshakeFlushPending = pending;
    }

    public void setHighestProcessedSequence(long highestProcessedSequence) {
        // LAST-RESORT CONTAINMENT for the cumulative-ack leapfrog: a sequence
        // flagged by markSequenceUnresolved was refused without an error
        // response, so no cumulative OK ack may ever cover it -- the client
        // would trim its store-and-forward slot for rows the server never
        // wrote. The deferral gate in the upgrade processor's
        // handleBinaryMessage must prevent any commit after such a refusal;
        // if the watermark still tries to advance past the unresolved
        // sequence, that gate has regressed. Clamp and scream: the client
        // stalls on acks and replays after the close -- an availability hit,
        // never data loss.
        if (firstUnresolvedSequence != -1 && highestProcessedSequence >= firstUnresolvedSequence) {
            LOG.critical().$('[').$(fd)
                    .$("] cumulative-ack watermark tried to leapfrog an unresolved sequence; clamping [requested=")
                    .$(highestProcessedSequence)
                    .$(", firstUnresolved=").$(firstUnresolvedSequence)
                    .$(']').$();
            this.highestProcessedSequence = Math.max(this.highestProcessedSequence, firstUnresolvedSequence - 1);
            return;
        }
        // LAST-RESORT CONTAINMENT for the deferred-commit ack hole (#7144): while
        // FLAG_DEFER_COMMIT rows sit uncommitted in WAL writers, a cumulative OK
        // ack covering their frames would let the client trim slots the server
        // can still roll back. The upgrade processor's ack path advances over a
        // deferred frame only once the force-commit traversal reports that
        // nothing is uncommitted; if the watermark still tries to move while
        // deferred rows ARE uncommitted, that path has regressed.
        // Clamp and scream: the client stalls on acks and replays after the
        // close -- an availability hit, never data loss.
        if (uncommittedDeferredRows && highestProcessedSequence > this.highestProcessedSequence) {
            LOG.critical().$('[').$(fd)
                    .$("] cumulative-ack watermark tried to advance over uncommitted deferred rows; clamping [requested=")
                    .$(highestProcessedSequence)
                    .$(", current=").$(this.highestProcessedSequence)
                    .$(']').$();
            return;
        }
        // A rowless deferred frame carries symbol-dictionary state that later
        // frames reference, and a cumulative ack past it lets the client trim it
        // away from them. Withholding its own ack cannot express that, because
        // the watermark is assigned, not incremented.
        //
        // Stop just short of it rather than refusing outright. Everything BEFORE
        // a rowless frame precedes it on the wire, so nothing there can depend on
        // it, and reaching this line means the uncommitted-rows clamp above
        // already found nothing uncommitted -- so those frames' rows are durable
        // and covering them is exactly what the ack promises. Refusing the whole
        // advance would leave them in the client's replay queue to be re-sent and
        // duplicated, which is the very thing this change exists to stop.
        if (firstRowlessDeferredSequence != -1 && highestProcessedSequence >= firstRowlessDeferredSequence) {
            this.highestProcessedSequence =
                    Math.max(this.highestProcessedSequence, firstRowlessDeferredSequence - 1);
            return;
        }
        this.highestProcessedSequence = highestProcessedSequence;
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
     * Returns true if the ACK batch threshold has been reached and the send
     * buffer is clear, meaning a cumulative ACK should be sent now.
     */
    public boolean shouldSendAck(int batchSize) {
        return sendState == SEND_STATE_READY
                && highestProcessedSequence - lastAckedSequence >= batchSize;
    }

    /**
     * Maps a parse failure to the wire status a client classifies on. Extracted so the
     * routing is unit-testable without driving a whole connection.
     */
    public static Status statusForParseError(QwpParseException.ErrorCode errorCode) {
        return switch (errorCode) {
            case SCHEMA_MISMATCH -> Status.SCHEMA_MISMATCH;
            case DELTA_DICT_GAP -> Status.DICTIONARY_GAP;
            default -> Status.PARSE_ERROR;
        };
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
        Unsafe.putShort(addr + offset, (short) count);
        offset += 2;
        for (int i = 0; i < count; i++) {
            CharSequence tableName = keys.getQuick(i);
            int utf8Len = Utf8s.utf8Bytes(tableName);
            Unsafe.putShort(addr + offset, (short) utf8Len);
            offset += 2;
            Utf8s.strCpyUtf8(tableName, addr + offset, utf8Len);
            offset += utf8Len;
            Unsafe.putLong(addr + offset, entries.get(tableName));
            offset += 8;
        }
        return offset;
    }

    /**
     * Writes per-table seqTxn entries for a durable ACK frame, carrying the
     * table dir name as an incarnation discriminator.
     * Format: tableCount(2) + [nameLen(2) + nameUtf8(N) + dirLen(2) + dirUtf8(M) + seqTxn(8)] * count
     *
     * @return number of bytes written
     */
    public static int writeDurableAckEntries(long addr, CharSequenceLongHashMap seqTxns, CharSequenceObjHashMap<String> dirNames) {
        int offset = 0;
        ObjList<CharSequence> keys = seqTxns.keys();
        int count = keys.size();
        Unsafe.putShort(addr + offset, (short) count);
        offset += 2;
        for (int i = 0; i < count; i++) {
            CharSequence tableName = keys.getQuick(i);
            int nameUtf8Len = Utf8s.utf8Bytes(tableName);
            Unsafe.putShort(addr + offset, (short) nameUtf8Len);
            offset += 2;
            Utf8s.strCpyUtf8(tableName, addr + offset, nameUtf8Len);
            offset += nameUtf8Len;
            CharSequence dirName = dirNames.get(tableName);
            int dirUtf8Len = Utf8s.utf8Bytes(dirName);
            Unsafe.putShort(addr + offset, (short) dirUtf8Len);
            offset += 2;
            Utf8s.strCpyUtf8(dirName, addr + offset, dirUtf8Len);
            offset += dirUtf8Len;
            Unsafe.putLong(addr + offset, seqTxns.get(tableName));
            offset += 8;
        }
        return offset;
    }

    private static Status cairoExceptionStatus(CairoException e) {
        if (e.isAuthorizationError()) {
            return Status.SECURITY_ERROR;
        }
        if (e.isSchemaMismatch()) {
            return Status.SCHEMA_MISMATCH;
        }
        if (e.isMalformedUtf8()) {
            return Status.PARSE_ERROR;
        }
        return e.isCritical() ? Status.INTERNAL_ERROR : Status.NOT_ACCEPTING_WRITES;
    }

    private void clearDeferredClose() {
        deferredCloseCode = -1;
        deferredCloseReason.clear();
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

    /**
     * Whether this node is read-only STATICALLY -- {@code readonly=true}
     * ({@code READ_ONLY_INSTANCE}) in the server configuration -- as opposed to
     * DYNAMICALLY through the role an enterprise engine reports. The distinction
     * decides the refusal shape for write batches on a read-only node:
     * <ul>
     * <li>Role-derived read-only ({@code engine.isReadOnlyMode()} true while this
     * flag is false; only reachable where an enterprise engine widens
     * {@code isReadOnlyMode()} with the replica leg) is TRANSIENT. Refusals take
     * the reconnect-eligible role-change close, and the reconnecting client is
     * handed over to the 421 role reject of the now-replica endpoint.</li>
     * <li>Static read-only is PERMANENT. The node's role never changes on account
     * of it, so the 421 backstop never materialises, and a role-change close
     * would send a store-and-forward client into a silent infinite
     * reconnect-replay loop. Refusals must stay the typed, terminal
     * {@link Status#SECURITY_ERROR} NACK.</li>
     * </ul>
     * When BOTH legs hold (an enterprise replica that is also statically
     * read-only) the static answer wins: the node refuses writes regardless of
     * any future promote, so the terminal NACK is the truthful signal. (The
     * combination is unreachable mid-connection anyway -- a statically read-only
     * node refuses the very first batch, before any role flip can land.)
     * <p>
     * The flag is immutable for the process lifetime (a {@code final} field of
     * the server configuration; the dynamic replica leg is a SEPARATE
     * configuration method), so consulting it after a live
     * {@code engine.isReadOnlyMode()} read opens no TOCTOU window: the shape
     * decision cannot flip between the two reads.
     */
    private boolean isStaticReadOnlyInstance() {
        return engine.getConfiguration().isReadOnlyInstance();
    }

    /**
     * Rejects a batch that failed with a {@link CairoException}, with Invariant B
     * containment for the in-place demote race.
     * <p>
     * The {@code engine.isReadOnlyMode()} gate at the top of
     * {@link #processMessage()} re-checks the live role per batch, but the flag
     * can flip BETWEEN that check and the WAL writer acquisition (or the commit)
     * within the same batch. The deeper engine gates then refuse with
     * {@code CairoException.readOnlyAccess()}, which
     * {@link #cairoExceptionStatus} would map to {@link Status#SECURITY_ERROR}
     * -- a status a store-and-forward client latches as a terminal HALT and
     * surfaces to its producer.
     * <p>
     * Classification keys on {@link CairoException#isReadOnlyAccessRefusal()}
     * -- the marker every read-only gate stamps at the THROW site -- never on
     * live engine state at catch time. A live {@code engine.isReadOnlyMode()}
     * re-read here races with a demote REVERT: when the demote fails between
     * the throw and the catch (real path:
     * {@code EntCairoEngine.drainWriterPool(restorePrimaryOnTimeout=true)}
     * restores PRIMARY on drain-budget expiry, and nothing fences the
     * throw-to-catch propagation -- the commit path releases the role-switch
     * READ lock as the exception unwinds), the re-read would see writable and
     * leak the transient refusal as a terminal SECURITY_ERROR for a
     * milliseconds-long condition on a node that ended up writable PRIMARY.
     * The marker records the refusal's CAUSE, which no later state flip can
     * rewrite.
     * <p>
     * Shape decision for a marked refusal: role-derived read-only takes the
     * reconnect-eligible close (the client reconnects, hits the 421 role reject
     * on the now-replica endpoint, and retries from SF until a primary is
     * reachable); a STATICALLY read-only node
     * ({@link #isStaticReadOnlyInstance()}, an immutable config flag -- no
     * TOCTOU) keeps the terminal SECURITY_ERROR, because the role-change close
     * has no 421 backstop there and would loop the client forever. A genuine
     * ACL denial (unmarked authorization error) maps to SECURITY_ERROR
     * regardless of the node's role at catch time: ACL state is
     * role-independent, so the terminal NACK is truthful even when the denial
     * lands mid-demote.
     */
    private void rejectCairoError(CairoException e) {
        if (e.isReadOnlyAccessRefusal() && !isStaticReadOnlyInstance()) {
            roleChangeClosePending = true;
            reject(Status.NOT_ACCEPTING_WRITES, e.getFlyweightMessage(), fd);
            return;
        }
        reject(cairoExceptionStatus(e), e.getFlyweightMessage(), fd);
    }

    private void rejectCommitError(Throwable th) {
        if (th instanceof CairoException e) {
            rejectCairoError(e);
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
        NOT_ACCEPTING_WRITES,
        DICTIONARY_GAP
    }
}
