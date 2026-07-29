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
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

final class PageFrameFiberTask extends FiberTask implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(PageFrameFiberTask.class);
    private final SqlExecutionCircuitBreakerWrapper circuitBreaker;
    private long orderedCursor = -1;
    private PageFrameSequence<?> orderedFrameSequence;
    private PageFrameReduceTask orderedReduceTask;
    private MCSequence orderedSubSeq;
    private final PageFrameFiberTaskPool pool;
    private final PageFrameMemoryRecord record;
    private int unorderedFrameIndex = -1;
    private UnorderedPageFrameSequence<?> unorderedFrameSequence;
    private int workerId = -1;

    PageFrameFiberTask(CairoEngine engine, PageFrameFiberTaskPool pool) {
        this.circuitBreaker = new SqlExecutionCircuitBreakerWrapper(
                engine,
                engine.getConfiguration().getCircuitBreakerConfiguration()
        );
        this.pool = pool;
        this.record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
    }

    void abortBeforeLaunch() {
        cancelFrameSequence(SqlExecutionCircuitBreaker.STATE_CANCELLED);
        completeOwnership();
        recycle(false);
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
            MCSequence subSeq,
            long cursor,
            PageFrameReduceTask reduceTask,
            PageFrameSequence<?> frameSequence
    ) {
        this.workerId = workerId;
        this.orderedSubSeq = subSeq;
        this.orderedCursor = cursor;
        this.orderedReduceTask = reduceTask;
        this.orderedFrameSequence = frameSequence;
    }

    void ofUnordered(
            int workerId,
            int frameIndex,
            UnorderedPageFrameSequence<?> frameSequence
    ) {
        this.workerId = workerId;
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
            recycle(true);
        }
    }

    @Override
    protected void onError(Throwable th) {
        final int interruptReason = th instanceof CairoException e
                ? e.getInterruptionReason()
                : SqlExecutionCircuitBreaker.STATE_OK;
        if (orderedFrameSequence != null) {
            LOG.error()
                    .$("reduce error [error=").$(th)
                    .$(", id=").$(orderedFrameSequence.getId())
                    .$(", taskType=").$(orderedReduceTask.getTaskType())
                    .$(", frameIndex=").$(orderedReduceTask.getFrameIndex())
                    .$(", frameCount=").$(orderedFrameSequence.getFrameCount())
                    .I$();
            orderedReduceTask.setErrorMsg(th);
        } else if (unorderedFrameSequence != null) {
            LOG.error()
                    .$("reduce error [error=").$(th)
                    .$(", id=").$(unorderedFrameSequence.getId())
                    .$(", frameIndex=").$(unorderedFrameIndex)
                    .$(", frameCount=").$(unorderedFrameSequence.getFrameCount())
                    .I$();
            unorderedFrameSequence.setError(th);
        }
        cancelFrameSequence(interruptReason);
    }

    @Override
    protected boolean runStep() {
        if (orderedFrameSequence != null) {
            if (orderedFrameSequence.isActive()) {
                circuitBreaker.init(orderedFrameSequence.getCircuitBreaker());
                PageFrameReduceJob.reduce(
                        workerId,
                        record,
                        circuitBreaker,
                        orderedReduceTask,
                        orderedFrameSequence,
                        null
                );
            }
        } else if (unorderedFrameSequence != null && unorderedFrameSequence.isActive()) {
            circuitBreaker.init(unorderedFrameSequence.getCircuitBreaker());
            UnorderedPageFrameReduceJob.reduce(
                    workerId,
                    record,
                    circuitBreaker,
                    unorderedFrameIndex,
                    unorderedFrameSequence,
                    null
            );
        }
        return true;
    }

    private void cancelFrameSequence(int reason) {
        if (orderedFrameSequence != null) {
            orderedFrameSequence.cancel(reason);
        } else if (unorderedFrameSequence != null) {
            unorderedFrameSequence.cancel(reason);
        }
    }

    private void clearBinding() {
        orderedCursor = -1;
        orderedFrameSequence = null;
        orderedReduceTask = null;
        orderedSubSeq = null;
        unorderedFrameIndex = -1;
        unorderedFrameSequence = null;
        workerId = -1;
    }

    private void completeOwnership() {
        if (orderedFrameSequence != null) {
            try {
                orderedSubSeq.done(orderedCursor);
            } finally {
                orderedFrameSequence.getReduceFinishedCounter().incrementAndGet();
            }
        } else if (unorderedFrameSequence != null) {
            unorderedFrameSequence.getDoneLatch().countDown();
        }
    }

    private void recycle(boolean isTerminal) {
        clearBinding();
        if (isTerminal) {
            reopen();
        }
        pool.release(this);
    }
}
