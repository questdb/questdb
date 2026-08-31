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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerWrapper;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MCSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

final class PageFrameFiberTask extends FiberTask implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(PageFrameFiberTask.class);
    private final SqlExecutionCircuitBreakerWrapper circuitBreaker;
    private final PageFrameReduceDispatcher dispatcher;
    private long orderedCursor = -1;
    private PageFrameSequence<?> orderedFrameSequence;
    private RingQueue<PageFrameReduceTask> orderedQueue;
    private PageFrameReduceTask orderedReduceTask;
    private MCSequence orderedSubSeq;
    private final FiberTaskPool<PageFrameFiberTask> pool;
    private final PageFrameMemoryRecord record;
    private final TimerShards timerShards;
    private int unorderedFrameIndex = -1;
    private UnorderedPageFrameSequence<?> unorderedFrameSequence;
    private RingQueue<UnorderedPageFrameReduceTask> unorderedQueue;
    private MCSequence unorderedSubSeq;
    private int workerId = -1;

    PageFrameFiberTask(
            CairoEngine engine,
            FiberTaskPool<PageFrameFiberTask> pool,
            PageFrameReduceDispatcher dispatcher
    ) {
        this.circuitBreaker = new SqlExecutionCircuitBreakerWrapper(
                engine,
                engine.getConfiguration().getCircuitBreakerConfiguration()
        );
        this.dispatcher = dispatcher;
        this.pool = pool;
        this.record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
        this.timerShards = engine.getTimerShards();
    }

    void abortBeforeLaunch() {
        Throwable failure = null;
        try {
            cancelFrameSequence(SqlExecutionCircuitBreaker.STATE_CANCELLED);
        } catch (Throwable th) {
            failure = th;
        }
        try {
            completeOwnership();
        } catch (Throwable th) {
            failure = addCleanupFailure(failure, th);
        }
        try {
            recycle();
        } catch (Throwable th) {
            failure = addCleanupFailure(failure, th);
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void close() {
        Misc.free(circuitBreaker);
        Misc.free(record);
    }

    @Override
    public @Nullable FiberCancellationSignal getCancellationSignal() {
        if (orderedFrameSequence != null) {
            return orderedFrameSequence.getCancellationSignal();
        }
        return unorderedFrameSequence != null ? unorderedFrameSequence.getCancellationSignal() : null;
    }

    boolean isBound() {
        return orderedFrameSequence != null || unorderedFrameSequence != null;
    }

    void ofOrdered(
            int workerId,
            RingQueue<PageFrameReduceTask> queue,
            MCSequence subSeq,
            long cursor,
            PageFrameReduceTask reduceTask,
            PageFrameSequence<?> frameSequence
    ) {
        this.workerId = workerId;
        this.orderedQueue = queue;
        this.orderedSubSeq = subSeq;
        this.orderedCursor = cursor;
        this.orderedReduceTask = reduceTask;
        this.orderedFrameSequence = frameSequence;
    }

    void ofUnordered(
            int workerId,
            RingQueue<UnorderedPageFrameReduceTask> queue,
            MCSequence subSeq,
            int frameIndex,
            UnorderedPageFrameSequence<?> frameSequence
    ) {
        this.workerId = workerId;
        this.unorderedQueue = queue;
        this.unorderedSubSeq = subSeq;
        this.unorderedFrameIndex = frameIndex;
        this.unorderedFrameSequence = frameSequence;
    }

    @Override
    protected void onAbandoned() {
        cancelFrameSequence(SqlExecutionCircuitBreaker.STATE_CANCELLED);
    }

    @Override
    protected void onDone() {
        try {
            completeOwnership();
        } finally {
            recycle();
        }
    }

    @Override
    protected void onError(Throwable th) {
        if (orderedFrameSequence != null) {
            LOG.error()
                    .$("reduce error [error=").$(th)
                    .$(", id=").$(orderedFrameSequence.getId())
                    .$(", taskType=").$(orderedReduceTask.getTaskType())
                    .$(", frameIndex=").$(orderedReduceTask.getFrameIndex())
                    .$(", frameCount=").$(orderedFrameSequence.getFrameCount())
                    .I$();
            orderedReduceTask.setErrorMsg(th);
            orderedFrameSequence.cancelOnReducerError(th);
        } else if (unorderedFrameSequence != null) {
            LOG.error()
                    .$("reduce error [error=").$(th)
                    .$(", id=").$(unorderedFrameSequence.getId())
                    .$(", frameIndex=").$(unorderedFrameIndex)
                    .$(", frameCount=").$(unorderedFrameSequence.getFrameCount())
                    .I$();
            unorderedFrameSequence.setError(th);
        }
    }

    @Override
    protected boolean runStep() {
        SuspensionScope.enterTimerShards(timerShards);
        if (orderedFrameSequence != null) {
            final RingQueue<PageFrameReduceTask> queue = orderedQueue;
            final MCSequence subSeq = orderedSubSeq;
            // row counts must be read before done() releases the queue slot for reuse
            long batchRows = orderedReduceTask.getFrameRowCount();
            reduceOrderedFrame(subSeq, orderedCursor, orderedReduceTask, orderedFrameSequence);
            final int batchLimit = dispatcher.getBatchLimit();
            // Stop claiming once the accumulated work reaches one configured-max-frame's row
            // count. The last claimed frame may take the total above that threshold.
            final long batchRowBudget = dispatcher.getBatchRowBudget();
            for (int i = 1; i < batchLimit && batchRows < batchRowBudget; i++) {
                final long cursor;
                while (true) {
                    final long next = subSeq.next();
                    if (next != -2) {
                        cursor = next;
                        break;
                    }
                    Os.pause();
                }
                if (cursor < 0) {
                    break;
                }
                final PageFrameReduceTask reduceTask = queue.get(cursor);
                final PageFrameSequence<?> frameSequence = reduceTask.getFrameSequence();
                orderedCursor = cursor;
                orderedReduceTask = reduceTask;
                orderedFrameSequence = frameSequence;
                batchRows += reduceTask.getFrameRowCount();
                reduceOrderedFrame(subSeq, cursor, reduceTask, frameSequence);
            }
        } else if (unorderedFrameSequence != null) {
            final RingQueue<UnorderedPageFrameReduceTask> queue = unorderedQueue;
            final MCSequence subSeq = unorderedSubSeq;
            long batchRows = unorderedFrameSequence.getFrameRowCount(unorderedFrameIndex);
            reduceUnorderedFrame(unorderedFrameIndex, unorderedFrameSequence);
            final int batchLimit = dispatcher.getBatchLimit();
            final long batchRowBudget = dispatcher.getBatchRowBudget();
            for (int i = 1; i < batchLimit && batchRows < batchRowBudget; i++) {
                final long cursor;
                while (true) {
                    final long next = subSeq.next();
                    if (next != -2) {
                        cursor = next;
                        break;
                    }
                    Os.pause();
                }
                if (cursor < 0) {
                    break;
                }
                final UnorderedPageFrameReduceTask reduceTask = queue.get(cursor);
                final UnorderedPageFrameSequence<?> frameSequence = reduceTask.getFrameSequence();
                final int frameIndex = reduceTask.getFrameIndex();
                final long frameSequenceId = reduceTask.getFrameSequenceId();
                reduceTask.clear();
                subSeq.done(cursor);
                dispatcher.signalProgress(frameSequence);
                if (frameSequenceId != frameSequence.getId()) {
                    LOG.error()
                            .$("skipping stale task [expected=").$(frameSequence.getId())
                            .$(", got=").$(frameSequenceId)
                            .I$();
                    continue;
                }
                unorderedFrameIndex = frameIndex;
                unorderedFrameSequence = frameSequence;
                batchRows += frameSequence.getFrameRowCount(frameIndex);
                reduceUnorderedFrame(frameIndex, frameSequence);
            }
        }
        return true;
    }

    private static Throwable addCleanupFailure(@Nullable Throwable primary, Throwable failure) {
        if (primary == null) {
            return failure;
        }
        if (primary != failure) {
            primary.addSuppressed(failure);
        }
        return primary;
    }

    private void cancelFrameSequence(int reason) {
        if (orderedFrameSequence != null) {
            if (orderedFrameSequence.isActive()) {
                orderedFrameSequence.cancel(reason);
            }
        } else if (unorderedFrameSequence != null) {
            if (unorderedFrameSequence.isActive()) {
                unorderedFrameSequence.cancel(reason);
            }
        }
    }

    private void clearBinding() {
        orderedCursor = -1;
        orderedFrameSequence = null;
        orderedQueue = null;
        orderedReduceTask = null;
        orderedSubSeq = null;
        unorderedFrameIndex = -1;
        unorderedFrameSequence = null;
        unorderedQueue = null;
        unorderedSubSeq = null;
        workerId = -1;
    }

    private void completeOwnership() {
        if (orderedFrameSequence != null) {
            try {
                orderedSubSeq.done(orderedCursor);
            } finally {
                orderedFrameSequence.getReduceFinishedCounter().incrementAndGet();
                dispatcher.signalProgress(orderedFrameSequence);
            }
        } else if (unorderedFrameSequence != null) {
            try {
                unorderedFrameSequence.getDoneLatch().countDown();
            } finally {
                unorderedFrameSequence.signalProgress();
            }
        }
    }

    // The frame stays bound while the reducer runs, so a park freezes owning exactly this cursor;
    // the cleared binding is what tells completeOwnership() the cursor is already done.
    private void reduceOrderedFrame(
            MCSequence subSeq,
            long cursor,
            PageFrameReduceTask reduceTask,
            PageFrameSequence<?> frameSequence
    ) {
        this.orderedCursor = cursor;
        this.orderedReduceTask = reduceTask;
        this.orderedFrameSequence = frameSequence;
        // frames of one batch can belong to different queries; the carrier scope's signal must
        // track the frame, not the mount
        frameSequence.enterReducerCancellationScope();
        try {
            if (frameSequence.isActive()) {
                circuitBreaker.init(frameSequence.getCircuitBreaker());
                PageFrameReduceJob.reduce(workerId, record, circuitBreaker, reduceTask, frameSequence, null);
            }
        } catch (Throwable th) {
            if (frameSequence.isReducerFailureReportable(th)) {
                LOG.error()
                        .$("reduce error [error=").$(th)
                        .$(", id=").$(frameSequence.getId())
                        .$(", taskType=").$(reduceTask.getTaskType())
                        .$(", frameIndex=").$(reduceTask.getFrameIndex())
                        .$(", frameCount=").$(frameSequence.getFrameCount())
                        .I$();
                reduceTask.setErrorMsg(th);
                frameSequence.cancelOnReducerError(th);
            }
            if (th instanceof Error error) {
                throw error;
            }
        } finally {
            this.orderedCursor = -1;
            this.orderedReduceTask = null;
            this.orderedFrameSequence = null;
            try {
                subSeq.done(cursor);
            } finally {
                frameSequence.getReduceFinishedCounter().incrementAndGet();
                dispatcher.signalProgress(frameSequence);
            }
        }
    }

    private void reduceUnorderedFrame(int frameIndex, UnorderedPageFrameSequence<?> frameSequence) {
        this.unorderedFrameIndex = frameIndex;
        this.unorderedFrameSequence = frameSequence;
        frameSequence.enterReducerCancellationScope();
        try {
            if (frameSequence.isActive()) {
                circuitBreaker.init(frameSequence.getCircuitBreaker());
                UnorderedPageFrameReduceJob.reduce(workerId, record, circuitBreaker, frameIndex, frameSequence, null);
            }
        } catch (Throwable th) {
            if (frameSequence.isReducerFailureReportable(th)) {
                LOG.error()
                        .$("reduce error [error=").$(th)
                        .$(", id=").$(frameSequence.getId())
                        .$(", frameIndex=").$(frameIndex)
                        .$(", frameCount=").$(frameSequence.getFrameCount())
                        .I$();
                frameSequence.setError(th);
            }
            if (th instanceof Error error) {
                throw error;
            }
        } finally {
            this.unorderedFrameIndex = -1;
            this.unorderedFrameSequence = null;
            try {
                frameSequence.getDoneLatch().countDown();
            } finally {
                dispatcher.signalProgress(frameSequence);
            }
        }
    }

    private void recycle() {
        clearBinding();
        circuitBreaker.clear();
        try {
            record.of(null);
        } finally {
            try {
                tryReopen();
            } finally {
                pool.release(this);
            }
        }
    }
}
