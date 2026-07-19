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
import io.questdb.cairo.ImplicitCastException;
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
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.StringSink;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dispatches page frames to a shared queue without ordered collection.
 * Workers release queue slots immediately after reading frame index and sequence reference.
 * Completion is tracked via an {@link SOUnboundedCountDownLatch}.
 * Designed for factories that don't need ordered results (GROUP BY, top-K).
 */
public class UnorderedPageFrameSequence<T extends StatefulAtom> implements Closeable {
    private static final AtomicLong ID_SEQ = new AtomicLong();
    private static final Log LOG = LogFactory.getLog(UnorderedPageFrameSequence.class);
    private T atom;
    private final AtomicInteger cancelReason = new AtomicInteger(SqlExecutionCircuitBreaker.STATE_OK);
    private final MillisecondClock clock;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final StringSink errorMsg = new StringSink();
    private PageFrameAddressCache frameAddressCache;
    private final LongList frameRowCounts = new LongList();
    private final AtomicBoolean isValid = new AtomicBoolean(true);
    private final MPSequence reducePubSeq;
    private final RingQueue<UnorderedPageFrameReduceTask> reduceQueue;
    private final AtomicInteger reduceStartedCounter = new AtomicInteger(0);
    private final MCSequence reduceSubSeq;
    private final UnorderedPageFrameReducer reducer;
    private final WorkStealingStrategy workStealingStrategy;
    private int errno = CairoException.NON_CRITICAL;
    private byte errorKind = AsyncQueryErrorKind.KIND_NONE;
    private int errorMessagePosition;
    private int frameCount;
    private PageFrameCursor frameCursor;
    private long id;
    private boolean isCancelled;
    private boolean isClosing;
    private boolean isInterrupted;
    private boolean isOutOfMemory;
    private boolean isReadyToDispatch;
    private boolean isUninterruptible;
    private PageFrameMemoryRecord localRecord;
    // Per-query native memory tracker captured from the owning SqlExecutionContext
    // at workload start. Null when no per-query limit is configured. Workers read
    // this off the task via task.getFrameSequence().getMemoryTracker() to charge
    // their allocations to the active workload.
    private MemoryTracker memoryTracker;
    private int queuedCount;
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
            this.reducer = reducer;
            this.clock = configuration.getMillisecondClock();
            this.workStealingStrategy = WorkStealingStrategyFactory.getInstance(configuration, sharedQueryWorkerCount);
            this.workStealCircuitBreaker = new SqlExecutionCircuitBreakerWrapper(engine, configuration.getCircuitBreakerConfiguration());
            this.reduceQueue = messageBus.getUnorderedPageFrameReduceQueue();
            this.reducePubSeq = messageBus.getUnorderedPageFrameReducePubSeq();
            this.reduceSubSeq = messageBus.getUnorderedPageFrameReduceSubSeq();
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    public void await() {
        // Nothing to do if no frames were queued.
        if (queuedCount == 0) {
            return;
        }
        // Wait for all queued frames to complete.
        while (!doneLatch.done(queuedCount)) {
            if (stealWork()) {
                workStealCircuitBreaker.init(sqlExecutionContext.getCircuitBreaker());
            }
            Os.pause();
        }
    }

    /**
     * Builds the typed exception to throw from {@link #dispatchAndAwait()} based on
     * the kind captured by {@link #setError(Throwable)}. Mirrors
     * {@link PageFrameReduceTask#buildError()} for the filter/top-K paths.
     */
    public RuntimeException buildError() {
        return switch (errorKind) {
            case AsyncQueryErrorKind.KIND_IMPLICIT_CAST ->
                    ImplicitCastException.instance().position(errorMessagePosition).put(errorMsg);
            case AsyncQueryErrorKind.KIND_NUMERIC ->
                    NumericException.instance().position(errorMessagePosition).put(errorMsg);
            // critical(errno) preserves the worker's errno and, with it, isCritical();
            // errno == NON_CRITICAL reduces to the previous nonCritical() behaviour.
            default -> CairoException.critical(errno)
                    .position(errorMessagePosition)
                    .put(errorMsg)
                    .setCancellation(isCancelled)
                    .setInterruption(isInterrupted)
                    .setOutOfMemory(isOutOfMemory);
        };
    }

    public void cancel(int reason) {
        isValid.compareAndSet(true, false);
        cancelReason.set(reason);
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
     * Dispatches all frames to the queue and waits for completion.
     * The owner thread work-steals while waiting.
     *
     * @throws CairoException if a worker encountered an error
     */
    public void dispatchAndAwait() {
        if (frameCount == 0) {
            return;
        }

        // Initialize the circuit breaker for work stealing and local reduces.
        workStealCircuitBreaker.init(sqlExecutionContext.getCircuitBreaker());

        int queued = 0;
        int localCount = 0;

        // Phase 1: Dispatch all frames.
        // The try/finally ensures queuedCount is set even if reduceLocally() throws,
        // so that await() in close() properly drains in-flight tasks.
        try {
            for (int i = 0; i < frameCount; i++) {
                while (true) {
                    long cursor = reducePubSeq.next();
                    if (cursor > -1) {
                        reduceQueue.get(cursor).of(this, i);
                        reducePubSeq.done(cursor);
                        queued++;
                        break;
                    } else if (cursor == -1) {
                        // Queue full.
                        if (workStealingStrategy.shouldSteal(localCount)) {
                            if (stealWork()) {
                                workStealCircuitBreaker.init(sqlExecutionContext.getCircuitBreaker());
                            }
                            continue;
                        }
                        // Reduce locally as fallback.
                        reduceLocally(i);
                        localCount++;
                        break;
                    } else {
                        Os.pause();
                    }
                }
            }
        } finally {
            this.queuedCount = queued;
        }

        // Phase 2: Wait for all queued frames to complete.
        final SqlExecutionCircuitBreaker circuitBreaker = sqlExecutionContext.getCircuitBreaker();
        while (!doneLatch.done(queued)) {
            if (!isActive()) {
                break;
            }
            if (!isUninterruptible) {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            }
            if (stealWork()) {
                workStealCircuitBreaker.init(circuitBreaker);
            }
            Os.pause();
        }

        // If we exited early due to cancellation, still wait for in-flight tasks
        // to complete to avoid data races with setError().
        while (!doneLatch.done(queued)) {
            if (stealWork()) {
                workStealCircuitBreaker.init(circuitBreaker);
            }
            Os.pause();
        }

        // Phase 3: Check for errors.
        if (hasError()) {
            if (isOutOfMemory) {
                throw CairoException.nonCritical().setOutOfMemory(true).put(errorMsg);
            }
            if (isCancelled) {
                throw CairoException.queryCancelled();
            }
            throw buildError();
        }

        if (!isActive()) {
            if (cancelReason.get() == SqlExecutionCircuitBreaker.STATE_CANCELLED) {
                throw CairoException.queryCancelled();
            } else {
                throw CairoException.queryTimedOut();
            }
        }
    }

    public T getAtom() {
        return atom;
    }

    public int getCancelReason() {
        return cancelReason.get();
    }

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

    public AtomicInteger getReduceStartedCounter() {
        return reduceStartedCounter;
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

    public WorkStealingStrategy getWorkStealingStrategy() {
        return workStealingStrategy;
    }

    public boolean isActive() {
        return isValid.get();
    }

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
            isValid.set(true);
            cancelReason.set(SqlExecutionCircuitBreaker.STATE_OK);
            doneLatch.reset();
            reduceStartedCounter.set(0);
            workStealingStrategy.of(reduceStartedCounter);
            errorMsg.clear();
            errorMessagePosition = 0;
            errno = CairoException.NON_CRITICAL;
            errorKind = AsyncQueryErrorKind.KIND_NONE;
            isCancelled = false;
            isInterrupted = false;
            isOutOfMemory = false;

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
        // reset() must be called only if there are no tasks in progress for this page frame sequence
        assert queuedCount == 0 || doneLatch.done(queuedCount);

        frameCount = 0;
        queuedCount = 0;
        isReadyToDispatch = false;
        // Drop the borrowed tracker reference; the provider owns the native block.
        memoryTracker = null;
        frameRowCounts.clear();

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
    public synchronized void setError(Throwable th) {
        // First error wins.
        if (!errorMsg.isEmpty()) {
            return;
        }
        errorKind = AsyncQueryErrorKind.of(th);
        if (th instanceof CairoException e) {
            errorMsg.put(e.getFlyweightMessage());
            errorMessagePosition = e.getPosition();
            errno = e.getErrno();
            isCancelled = e.isCancellation();
            isInterrupted = e.isInterruption();
            isOutOfMemory = e.isOutOfMemory();
            cancel(e.getInterruptionReason());
        } else if (th instanceof FlyweightMessageContainer fmc) {
            // ImplicitCastException / NumericException: a legitimate user-facing error.
            // Preserve the message and position verbatim so the collector can re-throw
            // the same class via buildError().
            errorMsg.put(fmc.getFlyweightMessage());
            errorMessagePosition = fmc.getPosition();
            cancel(SqlExecutionCircuitBreaker.STATE_OK);
        } else {
            errorMsg.put("unexpected reduce error");
            if (th.getMessage() != null) {
                errorMsg.put(": ").put(th.getMessage());
            }
            cancel(SqlExecutionCircuitBreaker.STATE_OK);
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

    private boolean hasError() {
        return !errorMsg.isEmpty();
    }

    private void reduceLocally(int frameIndex) {
        try {
            if (isActive()) {
                localRecord.of(getSymbolTableSource());
                reduceStartedCounter.incrementAndGet();
                reducer.reduce(-1, localRecord, frameIndex, workStealCircuitBreaker, this, this);
            }
        } catch (Throwable th) {
            LOG.error()
                    .$("local reduce error [error=").$(th)
                    .$(", id=").$(id)
                    .$(", frameIndex=").$(frameIndex)
                    .$(", frameCount=").$(frameCount)
                    .I$();
            int interruptReason = SqlExecutionCircuitBreaker.STATE_OK;
            if (th instanceof CairoException e) {
                interruptReason = e.getInterruptionReason();
            }
            // Record the error on the sequence and let dispatchAndAwait surface it
            // via buildError(). Re-throwing here would propagate through the owner
            // thread and bypass the kind-aware rebuild, losing the original class.
            setError(th);
            cancel(interruptReason);
        }
    }

    private boolean stealWork() {
        // N.B. consumeQueue may process a task from any UnorderedPageFrameSequence,
        // not just this one, which will re-initialize localRecord and the circuit
        // breaker wrapper for the foreign sequence. Callers must not assume their
        // state is preserved across this call and must re-init the wrapper when
        // this method returns true (a task was consumed).
        return !UnorderedPageFrameReduceJob.consumeQueue(
                reduceQueue,
                reduceSubSeq,
                localRecord,
                workStealCircuitBreaker,
                this
        );
    }
}
