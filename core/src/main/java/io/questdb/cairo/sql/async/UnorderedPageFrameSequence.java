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

package io.questdb.cairo.sql.async;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerWrapper;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.continuation.CancellationBinding;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dispatches page frames to a shared queue without ordered collection.
 * Queue entries are tickets: a worker releases the slot, then claims the sequence's next frame.
 * Completion is tracked via an {@link SOUnboundedCountDownLatch}.
 * Designed for factories that don't need ordered results (GROUP BY, top-K).
 */
public class UnorderedPageFrameSequence<T extends StatefulAtom> extends AbstractPageFrameSequence implements Closeable {
    private static final AtomicLong ID_SEQ = new AtomicLong();
    private static final Log LOG = LogFactory.getLog(UnorderedPageFrameSequence.class);
    private static final long MAX_TAIL_SPIN_NANOS = 16_000L;
    private static final int TAIL_SPIN_OUTSTANDING_FRAMES = 2;
    // Frame claims shared by the owner and the workers: the high 32 bits hold the claim limit, the
    // low 32 bits the next frame to hand out. Queue tickets name the sequence, not a frame, so the
    // owner can reduce its own frames without consuming other queries' tickets from the shared queue.
    private final AtomicLong claimState = new AtomicLong();
    private final MillisecondClock clock;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final AsyncQueryErrorState errorState = new AsyncQueryErrorState("unexpected reduce error");
    private final LongList frameRowCounts = new LongList();
    private final MessageBus messageBus;
    private final MPSequence reducePubSeq;
    private final RingQueue<UnorderedPageFrameReduceTask> reduceQueue;
    private final UnorderedPageFrameReducer reducer;
    private final long tailSpinTimeoutNanos;
    // At most one ticket per worker: more could never be held at once, and each extra one is a queue
    // slot that crowds out other queries once this sequence runs out of unclaimed frames.
    private final int ticketLimit;
    // Tickets out for the current dispatch, queued or held by a worker: the high 32 bits hold the
    // dispatch generation (the low 32 bits of the sequence id), the low 32 bits the count. The
    // generation keeps a ticket from an earlier dispatch from touching the count.
    private final AtomicLong ticketState = new AtomicLong();
    private final WorkStealingStrategy workStealingStrategy;
    private T atom;
    private PageFrameAddressCache frameAddressCache;
    private int frameCount;
    private PageFrameCursor frameCursor;
    private boolean hasTailSpun;
    private long id;
    private boolean isClosing;
    private boolean isReadyToDispatch;
    private boolean isUninterruptible;
    private PageFrameMemoryRecord localRecord;
    // Per-query native memory tracker captured from the owning SqlExecutionContext
    // at workload start. Null when no per-query limit is configured. Workers read
    // this off the task via task.getFrameSequence().getMemoryTracker() to charge
    // their allocations to the active workload.
    private MemoryTracker memoryTracker;
    private SqlExecutionContext sqlExecutionContext;
    private long startTime;
    private SqlExecutionCircuitBreakerWrapper workStealCircuitBreaker;

    public UnorderedPageFrameSequence(
            CairoEngine engine,
            CairoConfiguration configuration,
            MessageBus messageBus,
            T atom,
            UnorderedPageFrameReducer reducer,
            int sharedQueryWorkerCount
    ) {
        try {
            this.atom = atom;
            this.frameAddressCache = new PageFrameAddressCache();
            this.messageBus = messageBus;
            this.reducer = reducer;
            this.clock = configuration.getMillisecondClock();
            this.tailSpinTimeoutNanos = Math.max(
                    0,
                    Math.min(configuration.getSqlParallelWorkStealingSpinTimeout(), MAX_TAIL_SPIN_NANOS)
            );
            this.ticketLimit = Math.max(1, sharedQueryWorkerCount);
            this.workStealingStrategy = configuration.getFactoryProvider()
                    .getWorkStealingStrategy(configuration, sharedQueryWorkerCount, atom);
            this.workStealCircuitBreaker = new SqlExecutionCircuitBreakerWrapper(engine, configuration.getCircuitBreakerConfiguration());
            this.reduceQueue = messageBus.getUnorderedPageFrameReduceQueue();
            this.reducePubSeq = messageBus.getUnorderedPageFrameReducePubSeq();
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    public void await() {
        // Stop handing out frames, then wait for the claimed ones: their reducers read this sequence.
        awaitClaimedFrames(closeClaims(), messageBus.getPageFrameReduceDispatcher(), true);
    }

    /**
     * Builds the typed exception to throw from {@link #dispatchAndAwait()} based on
     * the kind captured by {@link #setError(Throwable)}. Mirrors
     * {@link PageFrameReduceTask#buildError()} for the filter/top-K paths.
     */
    public RuntimeException buildError() {
        return errorState.buildException();
    }

    @Override
    public void close() {
        Throwable cleanupFailure = null;
        isClosing = true;
        try {
            reset();
        } catch (Throwable th) {
            cleanupFailure = th;
        }
        final PageFrameMemoryRecord localRecordToFree = localRecord;
        localRecord = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, localRecordToFree);
        final SqlExecutionCircuitBreakerWrapper circuitBreakerToFree = workStealCircuitBreaker;
        workStealCircuitBreaker = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, circuitBreakerToFree);
        final T atomToFree = atom;
        atom = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, atomToFree);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    /**
     * Dispatches all frames and waits for completion. The owner reduces only its own frames: it
     * never consumes other queries' tickets from the shared queue, because their frames may be
     * slow (cold data, a large scan) and running them here would make this query as slow as theirs.
     *
     * @throws CairoException if a worker encountered an error
     */
    public void dispatchAndAwait() {
        hasTailSpun = false;
        if (frameCount == 0) {
            return;
        }

        // Initialize the circuit breaker for local reduces.
        workStealCircuitBreaker.init(sqlExecutionContext.getCircuitBreaker());
        claimState.set((long) frameCount << 32);

        // Phase 1: reduce own frames until none is left unclaimed, keeping at least one ticket out.
        // The owner publishes a single ticket; workers fan it out while they are free to help, see
        // claimFrame(long), so a busy pool sees few tickets and an idle one quickly takes them all.
        final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
        ticketState.set((long) (int) id << 32);
        do {
            if ((int) ticketState.get() == 0 && hasUnclaimedFrames()) {
                publishFirstTicket(dispatcher);
            }
        } while (isActive() && reduceOwnFrame());

        // Phase 2: stop handing out frames and wait for the ones workers claimed.
        awaitClaimedFrames(closeClaims(), dispatcher, false);

        // Phase 3: Check for errors.
        if (errorState.hasError()) {
            throw buildError();
        }

        if (!isActive() && getCancelReason() != SqlExecutionCircuitBreaker.STATE_OK) {
            throw buildInterruptionException();
        }
    }

    public T getAtom() {
        return atom;
    }

    @Override
    public SqlExecutionCircuitBreaker getCircuitBreaker() {
        return sqlExecutionContext.getCircuitBreaker();
    }

    public SOUnboundedCountDownLatch getDoneLatch() {
        return doneLatch;
    }

    public int getFrameCount() {
        return frameCount;
    }

    public long getFrameRowCount(int frameIndex) {
        return frameRowCounts.getQuick(frameIndex);
    }

    public long getId() {
        return id;
    }

    public MemoryTracker getMemoryTracker() {
        return memoryTracker;
    }

    public PageFrameAddressCache getPageFrameAddressCache() {
        return frameAddressCache;
    }

    public UnorderedPageFrameReducer getReducer() {
        return reducer;
    }

    public long getStartTime() {
        return startTime;
    }

    public SymbolTableSource getSymbolTableSource() {
        return frameCursor;
    }

    /**
     * Returns the post-aggregation strategy; callers rebind it to their own counter with {@code of()}.
     */
    public WorkStealingStrategy getWorkStealingStrategy() {
        return workStealingStrategy;
    }

    @Override
    public boolean isUninterruptible() {
        return isUninterruptible;
    }

    public UnorderedPageFrameSequence<T> of(
            RecordCursorFactory base,
            SqlExecutionContext executionContext,
            int order
    ) throws SqlException {
        sqlExecutionContext = executionContext;
        memoryTracker = executionContext.getMemoryTracker();
        startTime = clock.getTicks();
        isUninterruptible = executionContext.isUninterruptible();

        if (localRecord == null) {
            localRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
        }

        try {
            assert frameCursor == null;
            frameCursor = base.getPageFrameCursor(executionContext, order);
            frameAddressCache.of(base.getMetadata(), frameCursor.getColumnMapping(), frameCursor.isExternal());

            id = ID_SEQ.incrementAndGet();
            resetCancellation();
            doneLatch.reset();
            claimState.set(0);
            errorState.clear();

            atom.init(frameCursor, executionContext);
        } catch (TableReferenceOutOfDateException e) {
            frameCursor = Misc.freeIfCloseable(frameCursor);
            throw e;
        } catch (Throwable th) {
            LOG.error().$("could not initialize unordered page frame sequence [error=").$(th).I$();
            frameCursor = Misc.free(frameCursor);
            throw th;
        }
        return this;
    }

    public void prepareForDispatch() {
        if (!isReadyToDispatch) {
            buildAddressCache();
            isReadyToDispatch = true;
        }
    }

    public void reset() {
        // Close the claims before tearing down the frame state, so that a leftover ticket
        // can't start a frame. reset() must be called only once the claimed frames are done.
        final int claimedCount = closeClaims();
        assert doneLatch.done(claimedCount);

        frameCount = 0;
        isReadyToDispatch = false;
        // Drop the borrowed tracker reference; the provider owns the native block.
        memoryTracker = null;
        frameRowCounts.clear();
        // Drop the retained Throwable so a pooled sequence does not pin it while idle.
        errorState.clear();

        Throwable cleanupFailure = null;
        try {
            if (atom != null) {
                atom.clear();
            }
        } catch (Throwable th) {
            cleanupFailure = th;
        }
        // Unfreeze the covered posting readers frozen in buildAddressCache() BEFORE the
        // address cache (which holds them) and the frame cursor (which owns them) are
        // torn down. reset() runs after the sequence has been awaited, so every worker
        // cursor has finished and the unfreeze is race-free. A reader left frozen would
        // make its reloadConditionally() a permanent no-op and break the next query
        // against the same partition.
        final PageFrameAddressCache frameAddressCacheToFree = frameAddressCache;
        if (isClosing) {
            frameAddressCache = null;
        }
        if (frameAddressCacheToFree != null) {
            try {
                frameAddressCacheToFree.unfreezeCoveredReaders();
            } catch (Throwable th) {
                if (cleanupFailure == null) {
                    cleanupFailure = th;
                } else if (cleanupFailure != th) {
                    cleanupFailure.addSuppressed(th);
                }
            }
        }
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, frameAddressCacheToFree);
        final PageFrameCursor frameCursorToFree = frameCursor;
        frameCursor = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, frameCursorToFree);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    /**
     * Stores the first error from a worker thread. Thread-safe (synchronized).
     */
    public void setError(Throwable th) {
        if (errorState.setError(th)) {
            cancelOnReducerError(th);
        }
    }

    /**
     * Hands out the next unclaimed frame of this sequence, or -1 when none is left. Both the owner
     * and the workers holding this sequence's tickets call it, so each frame is reduced exactly once.
     * The caller must count down the done latch once it has reduced the claimed frame.
     */
    int claimFrame() {
        while (true) {
            final long state = claimState.get();
            final int next = (int) state;
            if (next >= (int) (state >>> 32)) {
                return -1;
            }
            if (claimState.compareAndSet(state, state + 1)) {
                return next;
            }
        }
    }

    boolean hasUnclaimedFrames() {
        final long state = claimState.get();
        return (int) state < (int) (state >>> 32);
    }

    boolean isDoneAfterTailSpin() {
        // Called while the owner waits for claimed frames, so the claims are closed already
        // and the claim limit equals the number of claimed frames.
        final int claimedCount = (int) (claimState.get() >>> 32);
        if (hasTailSpun
                || tailSpinTimeoutNanos == 0
                || !isActive()
                || isUninterruptible
                || claimedCount < 1
                || !doneLatch.done(Math.max(0, claimedCount - TAIL_SPIN_OUTSTANDING_FRAMES))) {
            return false;
        }
        hasTailSpun = true;
        final long startNanos = System.nanoTime();
        do {
            if (doneLatch.done(claimedCount)) {
                return true;
            }
            Thread.onSpinWait();
        } while (isActive() && System.nanoTime() - startNanos < tailSpinTimeoutNanos);
        return doneLatch.done(claimedCount);
    }

    /**
     * A worker claims a frame with a ticket it took from the queue, or -1 when none is left. On a
     * claim, it fans out: it publishes up to two more tickets while fewer than one ticket per worker
     * is out, so the helpers double while workers are free to take tickets, and a busy pool, which
     * takes few, sees few. A ticket that claims nothing is retired.
     */
    int claimFrame(long ticketId) {
        if (ticketId != id) {
            // From an earlier dispatch: its count was reset with the generation.
            return -1;
        }
        final int frameIndex = claimFrame();
        if (frameIndex < 0) {
            retireTicket(ticketId);
        } else if (isActive()) {
            for (int i = 0; i < 2 && addTicket(ticketId); i++) {
                if (!tryPublishTicket()) {
                    retireTicket(ticketId);
                    break;
                }
            }
        }
        return frameIndex;
    }

    /**
     * A worker calls this after reducing a frame. While frames remain unclaimed, it hands the ticket
     * back to the queue tail, so the query keeps its helper but takes turns with other queries;
     * otherwise it retires the ticket. Returns false when the queue is full: the worker then keeps
     * the ticket and claims the next frame itself, so a full queue never takes a helper away.
     */
    boolean handBackTicket(long ticketId) {
        if (!isActive() || !hasUnclaimedFrames()) {
            retireTicket(ticketId);
            return true;
        }
        return tryPublishTicket();
    }

    void retireTicket(long ticketId) {
        while (true) {
            final long state = ticketState.get();
            if ((int) (state >>> 32) != (int) ticketId || (int) state == 0) {
                return;
            }
            if (ticketState.compareAndSet(state, state - 1)) {
                return;
            }
        }
    }

    // Counts a new ticket of the current dispatch, unless one ticket per worker is already out.
    private boolean addTicket(long ticketId) {
        while (true) {
            final long state = ticketState.get();
            if ((int) (state >>> 32) != (int) ticketId || (int) state >= ticketLimit) {
                return false;
            }
            if (ticketState.compareAndSet(state, state + 1)) {
                return true;
            }
        }
    }

    private void awaitClaimedFrames(int claimedCount, PageFrameReduceDispatcher dispatcher, boolean isDraining) {
        final boolean canPark = dispatcher != null && isFiberSuspendable();
        while (true) {
            final long observedProgress = canPark ? getProgressVersion() : 0;
            final long observedGlobalProgress = canPark ? dispatcher.getProgressVersion() : 0;
            if (doneLatch.done(claimedCount)) {
                break;
            }
            if (canPark) {
                awaitProgress(dispatcher, observedProgress, observedGlobalProgress, isDraining || !isActive());
            } else {
                // awaitProgress() watches for cancellation while parked; a thread that cannot park
                // checks the breaker itself, or a cancellation during the last worker frames is lost.
                if (!isDraining && !isUninterruptible && isActive()) {
                    hasCircuitBreakerInterruptionBeenSuperseded(getCircuitBreaker(), true);
                }
                Os.pause();
            }
        }
    }

    private void buildAddressCache() {
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            frameRowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
            frameAddressCache.add(frameCount++, frame);
        }

        // Mirror PageFrameSequence.buildAddressCache(): covered frames decode their
        // columns on the async workers by iterating detached cursors over the shared
        // per-partition posting readers, which is only race-free if those readers are
        // positioned at the query txn, cache-warm, and FROZEN before any worker decodes.
        // The eager production iteration above already positioned + warmed each reader,
        // so freeze them now, before dispatch. unfreezeCoveredReaders() in reset()
        // reverses it once the sequence has been awaited.
        frameAddressCache.freezeCoveredReaders();
    }

    /**
     * Stops handing out frames and returns how many were claimed. Shrinking the limit down to the
     * next frame keeps the call idempotent: the limit then equals the claimed count.
     */
    private int closeClaims() {
        while (true) {
            final long state = claimState.get();
            final int next = (int) state;
            final long closedState = ((long) next << 32) | next;
            if (state == closedState || claimState.compareAndSet(state, closedState)) {
                return next;
            }
        }
    }

    private boolean hasCircuitBreakerInterruptionBeenSuperseded(
            SqlExecutionCircuitBreaker circuitBreaker,
            boolean isTimeThrottled
    ) {
        try {
            if (isTimeThrottled) {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            } else {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            }
            return false;
        } catch (CairoException e) {
            if (isInterruptionSuperseded(e)) {
                return true;
            }
            throw e;
        }
    }

    /**
     * Publishes a ticket for the workers to fan out. Holds the publication permit only while
     * publishing: quiesce waits for it. A full queue publishes nothing; the owner tries again
     * before its next frame.
     */
    private void publishFirstTicket(@Nullable PageFrameReduceDispatcher dispatcher) {
        if (dispatcher != null && !dispatcher.tryAcquirePublication()) {
            // The dispatcher is quiescing. An owner on a fiber of the quiescing runtime reduces its
            // frames itself, unless the quiesce drain cancels it first; any other owner cancels.
            if (!dispatcher.isCurrentFiberOwned()) {
                cancel(SqlExecutionCircuitBreaker.STATE_CANCELLED);
            }
            return;
        }
        try {
            if (addTicket(id) && !tryPublishTicket()) {
                retireTicket(id);
            }
        } finally {
            if (dispatcher != null) {
                dispatcher.releasePublication();
            }
        }
    }

    private void reduceLocally(int frameIndex) {
        final boolean isFiberSuspendable = isFiberSuspendable();
        final SuspensionScope.CarrierScope suspensionScope = isFiberSuspendable
                ? null
                : SuspensionScope.scope();
        final SuspensionScope.Mode previousMode = isFiberSuspendable
                ? null
                : SuspensionScope.enterBlocking(suspensionScope);
        final FiberCancellationSignal previousCancellationSignal = isFiberSuspendable
                ? SuspensionScope.getCancellationSignal()
                : null;
        final long previousCancellationSignalGeneration = isFiberSuspendable
                ? SuspensionScope.getCancellationSignalGeneration()
                : CancellationBinding.NO_GENERATION;
        final FiberCancellationSignal previousSupplementalCancellationSignal = isFiberSuspendable
                ? SuspensionScope.getSupplementalCancellationSignal()
                : null;
        final long previousSupplementalCancellationSignalGeneration = isFiberSuspendable
                ? SuspensionScope.getSupplementalCancellationSignalGeneration()
                : CancellationBinding.NO_GENERATION;
        if (isFiberSuspendable) {
            enterReducerCancellationScope();
        }
        try {
            if (isActive()) {
                localRecord.of(getSymbolTableSource());
                reducer.reduce(-1, localRecord, frameIndex, workStealCircuitBreaker, this, this);
            }
        } catch (Throwable th) {
            if (isReducerFailureReportable(th)) {
                LOG.error()
                        .$("local reduce error [error=").$(th)
                        .$(", id=").$(id)
                        .$(", frameIndex=").$(frameIndex)
                        .$(", frameCount=").$(frameCount)
                        .I$();
                // Record the error so dispatchAndAwait applies the same normalization as queued reducers.
                setError(th);
            }
        } finally {
            if (isFiberSuspendable) {
                SuspensionScope.restoreCancellationSignal(
                        previousCancellationSignal,
                        previousCancellationSignalGeneration
                );
                SuspensionScope.enterSupplementalCancellationSignal(
                        previousSupplementalCancellationSignal,
                        previousSupplementalCancellationSignalGeneration
                );
            } else {
                SuspensionScope.restoreMode(suspensionScope, previousMode);
            }
        }
    }

    private boolean reduceOwnFrame() {
        // Check for cancellation before every frame: the owner may reduce all frames itself.
        if (!isUninterruptible
                && hasCircuitBreakerInterruptionBeenSuperseded(sqlExecutionContext.getCircuitBreaker(), true)) {
            return false;
        }
        workStealingStrategy.onBeforeOwnerReduce();
        final int frameIndex = claimFrame();
        if (frameIndex < 0) {
            return false;
        }
        try {
            reduceLocally(frameIndex);
        } finally {
            doneLatch.countDown();
        }
        return true;
    }

    private boolean tryPublishTicket() {
        while (true) {
            final long cursor = reducePubSeq.next();
            if (cursor > -1) {
                reduceQueue.get(cursor).of(this);
                reducePubSeq.done(cursor);
                return true;
            }
            if (cursor == -1) {
                return false;
            }
            Os.pause();
        }
    }
}
