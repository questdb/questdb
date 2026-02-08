/*******************************************************************************
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
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
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
    private final T atom;
    private final AtomicInteger cancelReason = new AtomicInteger(SqlExecutionCircuitBreaker.STATE_OK);
    private final MillisecondClock clock;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final StringSink errorMsg = new StringSink();
    private final PageFrameAddressCache frameAddressCache;
    private final LongList frameRowCounts = new LongList();
    private final MPSequence reducePubSeq;
    private final RingQueue<UnorderedPageFrameReduceTask> reduceQueue;
    private final AtomicInteger reduceStartedCounter = new AtomicInteger(0);
    private final MCSequence reduceSubSeq;
    private final UnorderedPageFrameReducer reducer;
    private final AtomicBoolean isValid = new AtomicBoolean(true);
    private final WorkStealingStrategy workStealingStrategy;
    private int errorMessagePosition;
    private int frameCount;
    private PageFrameCursor frameCursor;
    private long id;
    private boolean isCancelled;
    private boolean isOutOfMemory;
    private PageFrameMemoryRecord localRecord;
    private volatile int queuedCount;
    private boolean isReadyToDispatch;
    private SqlExecutionContext sqlExecutionContext;
    private long startTime;
    private boolean isUninterruptible;
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
            close();
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
            stealWork();
            Os.pause();
        }
    }

    public void cancel(int reason) {
        isValid.compareAndSet(true, false);
        cancelReason.set(reason);
    }

    @Override
    public void close() {
        reset();
        localRecord = Misc.free(localRecord);
        workStealCircuitBreaker = Misc.free(workStealCircuitBreaker);
        Misc.free(atom);
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
                        stealWork();
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
        this.queuedCount = queued;

        // Phase 2: Wait for all queued frames to complete.
        while (!doneLatch.done(queued)) {
            if (!isActive()) {
                break;
            }
            if (!isUninterruptible) {
                workStealCircuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
            }
            stealWork();
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
            CairoException ex = CairoException.nonCritical().put(errorMsg);
            ex.position(errorMessagePosition);
            throw ex;
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

    public boolean hasError() {
        return !errorMsg.isEmpty();
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
        startTime = clock.getTicks();
        isUninterruptible = executionContext.isUninterruptible();

        if (localRecord == null) {
            localRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
        }

        try {
            assert frameCursor == null;
            frameCursor = base.getPageFrameCursor(executionContext, order);
            frameAddressCache.of(base.getMetadata(), frameCursor.getColumnIndexes(), frameCursor.isExternal());

            id = ID_SEQ.incrementAndGet();
            isValid.set(true);
            cancelReason.set(SqlExecutionCircuitBreaker.STATE_OK);
            doneLatch.reset();
            reduceStartedCounter.set(0);
            workStealingStrategy.of(reduceStartedCounter);
            errorMsg.clear();
            errorMessagePosition = 0;
            isCancelled = false;
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
        frameCount = 0;
        queuedCount = 0;
        isReadyToDispatch = false;
        frameRowCounts.clear();
        atom.clear();
        Misc.free(frameAddressCache);
        frameCursor = Misc.free(frameCursor);
    }

    /**
     * Stores the first error from a worker thread. Thread-safe (synchronized).
     */
    public synchronized void setError(Throwable th) {
        // First error wins.
        if (!errorMsg.isEmpty()) {
            return;
        }
        if (th instanceof CairoException e) {
            errorMsg.put(e.getFlyweightMessage());
            errorMessagePosition = e.getPosition();
            isCancelled = e.isCancellation();
            isOutOfMemory = e.isOutOfMemory();
            cancel(e.getInterruptionReason());
        } else {
            errorMsg.put("unexpected filter error");
            if (th.getMessage() != null) {
                errorMsg.put(": ").put(th.getMessage());
            }
            cancel(SqlExecutionCircuitBreaker.STATE_OK);
        }
    }

    public void toTop() {
        if (frameCount > 0) {
            long newId = ID_SEQ.incrementAndGet();
            LOG.debug()
                    .$("toTop [id=").$(id)
                    .$(", newId=").$(newId)
                    .I$();

            id = newId;
            doneLatch.reset();
            reduceStartedCounter.set(0);
            workStealingStrategy.of(reduceStartedCounter);
            isValid.set(true);
            cancelReason.set(SqlExecutionCircuitBreaker.STATE_OK);
            errorMsg.clear();
            errorMessagePosition = 0;
            isCancelled = false;
            isOutOfMemory = false;
        }
    }

    private void buildAddressCache() {
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            frameRowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
            frameAddressCache.add(frameCount++, frame);
        }
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
            cancel(interruptReason);
            throw th;
        }
    }

    private void stealWork() {
        UnorderedPageFrameReduceJob.consumeQueue(
                reduceQueue,
                reduceSubSeq,
                localRecord,
                workStealCircuitBreaker,
                this
        );
    }
}
