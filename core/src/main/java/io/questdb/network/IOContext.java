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

package io.questdb.network;

import io.questdb.log.Log;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicLong;

public abstract class IOContext<T extends IOContext<T>> implements Mutable, QuietCloseable {
    /**
     * Gated behind this compile-time constant so HotSpot eliminates the
     * AtomicLong allocations and the per-event increments when disabled.
     * Flip to {@code true} only when actively chasing dispatcher anomalies;
     * leaving it on regresses every protocol's dispatcher path with
     * four atomic increments per IOEvent.
     */
    private static final boolean DIAGNOSTICS_ENABLED = false;
    private static final long DEFERRED_MASK = 0xFFFFFFFFL;
    /**
     * "At most one worker handling this context at a time" gate, packed
     * into a single AtomicLong with the deferred-op bitmask so the
     * loser's "fail to acquire AND stash my op" pair is a single CAS.
     * <p>
     * Layout:
     * <ul>
     *   <li>bit 32: in-flight flag</li>
     *   <li>bits 0..31: deferred-op bitmask, OR-combined from
     *       {@link IOOperation#READ}, {@link IOOperation#WRITE},
     *       {@link IOOperation#HEARTBEAT}</li>
     * </ul>
     * <p>
     * The previous design used two separate fields (an
     * {@code AtomicBoolean inFlight} and an {@code AtomicInteger deferredOp})
     * with a "last write wins" deferred slot. That admitted two losses:
     * (a) a second deferral overwrote the first when the two operations
     * differed (e.g. WRITE published by an extension publisher and READ
     * published by an epoll readability event), and (b) a deferral whose
     * {@code set} was scheduled after the holder's release-then-consume
     * landed in an orphaned slot that no future holder would observe
     * unless an unrelated event happened to arrive. Both are closed
     * here: the loser's CAS encodes its op as part of the same atomic
     * update against the live state, so an op cannot be observed-as-failed
     * unless the holder's subsequent release-and-consume sees it.
     */
    private static final long IN_FLIGHT_BIT = 1L << 32;
    protected final Socket socket;
    protected long heartbeatId = -1;
    /**
     * Diagnostic counters tracking IOEvent flow through the dispatcher
     * pipeline. Used during development to localise wake-loss races: which
     * stage drops the event when {@code wakesArmed} (publisher-side count)
     * exceeds {@code drainEntries} (worker-side count). See
     * {@link #DIAGNOSTICS_ENABLED} for the gating rationale.
     */
    private final AtomicLong epollFireCount = DIAGNOSTICS_ENABLED ? new AtomicLong() : null;
    private final AtomicLong inFlightAndDeferred = new AtomicLong();
    private final AtomicLong interestQueueConsumeCount = DIAGNOSTICS_ENABLED ? new AtomicLong() : null;
    private final AtomicLong interestQueuePublishCount = DIAGNOSTICS_ENABLED ? new AtomicLong() : null;
    private final AtomicLong ioEventPublishCount = DIAGNOSTICS_ENABLED ? new AtomicLong() : null;
    private int disconnectReason;
    private volatile boolean initialized = false;

    // IMPORTANT: Keep subclass constructors lightweight!
    // Under high load, new context objects are created for each accepted connection.
    // Since connection acceptance runs on a single thread, slow constructors can
    // significantly degrade performance and throttle incoming connections.
    // To avoid this, defer context initialization to the doInit() method.
    protected IOContext(@NotNull SocketFactory socketFactory, NetworkFacade nf, Log log) {
        this.socket = socketFactory.newInstance(nf, log);
    }

    @Override
    public void clear() {
        _clear();
    }

    @Override
    public void close() {
        _clear();
    }

    public long getAndResetHeartbeatId() {
        long id = heartbeatId;
        heartbeatId = -1;
        return id;
    }

    public int getDisconnectReason() {
        return disconnectReason;
    }

    public long getEpollFireCount() {
        return DIAGNOSTICS_ENABLED ? epollFireCount.get() : 0L;
    }

    public long getFd() {
        return socket != null ? socket.getFd() : -1;
    }

    public long getInterestQueueConsumeCount() {
        return DIAGNOSTICS_ENABLED ? interestQueueConsumeCount.get() : 0L;
    }

    public long getInterestQueuePublishCount() {
        return DIAGNOSTICS_ENABLED ? interestQueuePublishCount.get() : 0L;
    }

    public long getIoEventPublishCount() {
        return DIAGNOSTICS_ENABLED ? ioEventPublishCount.get() : 0L;
    }

    public Socket getSocket() {
        return socket;
    }

    public void incrementEpollFireCount() {
        if (DIAGNOSTICS_ENABLED) {
            epollFireCount.incrementAndGet();
        }
    }

    public void incrementInterestQueueConsumeCount() {
        if (DIAGNOSTICS_ENABLED) {
            interestQueueConsumeCount.incrementAndGet();
        }
    }

    public void incrementInterestQueuePublishCount() {
        if (DIAGNOSTICS_ENABLED) {
            interestQueuePublishCount.incrementAndGet();
        }
    }

    public void incrementIoEventPublishCount() {
        if (DIAGNOSTICS_ENABLED) {
            ioEventPublishCount.incrementAndGet();
        }
    }

    public final void init() throws TlsSessionInitFailedException {
        if (!initialized) {
            doInit();
            initialized = true;
        }
    }

    public boolean invalid() {
        return socket.isClosed();
    }

    /**
     * Atomically clears the in-flight bit AND the deferred-op bitmask in
     * one CAS, returning the deferred mask the holder is responsible for
     * republishing. Called from the in-flight finally block in
     * {@link AbstractIODispatcher#processIOQueue}.
     * <p>
     * The atomicity is the point: a loser whose CAS landed BEFORE this
     * one is observed (its bit is in the returned mask). A loser whose
     * CAS lands AFTER this one finds the in-flight bit cleared and so
     * takes the acquire branch instead of the defer branch, becoming
     * the next holder and processing its own op directly. Either way,
     * no op is dropped.
     *
     * @return bitmask of deferred ops to republish (0 = nothing deferred)
     */
    public int releaseAndConsume() {
        long s;
        do {
            s = inFlightAndDeferred.get();
        } while (!inFlightAndDeferred.compareAndSet(s, 0L));
        return (int) (s & DEFERRED_MASK);
    }

    /**
     * Single-CAS combined acquire-or-defer. Either acquires the in-flight
     * gate (returns {@code true}, caller is now the holder and must
     * release via {@link #releaseAndConsume}) or atomically ORs
     * {@code op} into the deferred bitmask (returns {@code false}; the
     * caller's op is now safely visible to whichever holder picks it up
     * via release-and-consume).
     * <p>
     * Replaces the previous two-call {@code tryAcquireInFlight} +
     * {@code setDeferredOp} pair, which admitted a window where the
     * holder's release-then-consume could land between a loser's failed
     * acquire-CAS and the loser's slot write, orphaning the op.
     *
     * @param op a single {@link IOOperation} bit (READ, WRITE, or HEARTBEAT)
     * @return {@code true} if the caller acquired the gate; {@code false}
     *         if the op was deferred to the current holder
     */
    public boolean tryAcquireOrDefer(int op) {
        assert op == IOOperation.READ || op == IOOperation.WRITE || op == IOOperation.HEARTBEAT
                : "op must be a single IOOperation bit, got " + op;
        long s;
        do {
            s = inFlightAndDeferred.get();
            if ((s & IN_FLIGHT_BIT) == 0) {
                if (inFlightAndDeferred.compareAndSet(s, IN_FLIGHT_BIT)) {
                    // We acquired into a guaranteed-empty deferred slot
                    // because the only way IN_FLIGHT_BIT was clear was a
                    // prior holder's releaseAndConsume zeroing the long.
                    return true;
                }
            } else {
                if (inFlightAndDeferred.compareAndSet(s, s | (op & DEFERRED_MASK))) {
                    return false;
                }
            }
        } while (true);
    }

    @SuppressWarnings("unchecked")
    public final T of(long fd) {
        socket.of(fd);
        return (T) this;
    }

    public ServerDisconnectException registerDispatcherDisconnect(int reason) {
        disconnectReason = reason;
        return ServerDisconnectException.INSTANCE;
    }

    public HeartBeatException registerDispatcherHeartBeat() {
        return HeartBeatException.INSTANCE;
    }

    public PeerIsSlowToWriteException registerDispatcherRead() {
        return PeerIsSlowToWriteException.INSTANCE;
    }

    public PeerIsSlowToReadException registerDispatcherWrite() {
        return PeerIsSlowToReadException.INSTANCE;
    }

    public void setHeartbeatId(long heartbeatId) {
        this.heartbeatId = heartbeatId;
    }

    private void _clear() {
        heartbeatId = -1;
        socket.close();
        disconnectReason = -1;
        initialized = false;
        // Reset the in-flight gate and any stashed deferred ops so a
        // pool-recycled context starts in a clean state. Normal
        // operation always release()s in the dispatcher's finally
        // block, so this is belt-and-braces for shutdown / abrupt
        // teardown paths.
        inFlightAndDeferred.set(0L);
    }

    /**
     * Initializes the state of the context object.
     * <p>
     * Override this method to perform any setup required.
     * Note that this method is called once per connection, but not on the thread
     * that accepts connections. Instead, it is invoked when the first I/O event
     * is dispatched for this context.
     * <p>
     * Avoid placing initialization logic in the context's constructor, as it may
     * negatively impact the database's ability to accept new connections under high load.
     *
     * @throws io.questdb.cairo.CairoException if initialization fails
     */
    protected void doInit() throws TlsSessionInitFailedException {
    }
}
