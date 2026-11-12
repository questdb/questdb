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

package io.questdb.griffin.engine.groupby.vect;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.AsyncQueryErrorState;
import io.questdb.cairo.sql.async.AsyncQueryProgressState;
import io.questdb.cairo.sql.async.QueryParallelFiberDispatcher;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.mp.Sequence;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.Rosti;
import io.questdb.std.RostiAllocFacade;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

public class VectorAggregateEntry implements Mutable {
    /**
     * The number of NULL keys in the buffer that a keyed aggregation passes to
     * {@link VectorAggregateFunction#aggregateNullKeys}: it aggregates a frame whose key column holds no
     * data in chunks of at most this many rows.
     */
    public static final int NULL_KEY_CHUNK_SIZE = 1024;
    private AsyncQueryErrorState aggregateError;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private CountDownLatchSPI doneLatch;
    private int frameIndex;
    private ObjList<PageFrameMemoryPool> frameMemoryPools;
    private long frameRowCount;
    private VectorAggregateFunction func;
    private int keyColIndex;
    private AtomicInteger oomCounter;
    private long pNullKeys;
    private long[] pRosti;
    private PerWorkerLocks perWorkerLocks;
    private AsyncQueryProgressState progressState;
    private RostiAllocFacade raf;
    private AtomicInteger startedCounter;
    private int valueColIndex;
    private int valueSize;

    /**
     * Aggregates one frame for one function.
     *
     * @param valueSize the bytes of a value of the function's column, 0 for a function without one
     * @param pNullKeys {@link #NULL_KEY_CHUNK_SIZE} INT keys, each the NULL key, for a keyed aggregation;
     *                  see {@link VectorAggregateFunction#aggregateNullKeys}
     */
    public static void aggregateUnsafe(
            int workerId,
            @Nullable AtomicInteger oomCounter,
            int frameIndex,
            long frameRowCount,
            int keyColIndex,
            int valueColIndex,
            int valueSize,
            long pNullKeys,
            long @Nullable [] pRosti,
            @NotNull ObjList<PageFrameMemoryPool> frameMemoryPools,
            @Nullable RostiAllocFacade raf,
            @NotNull VectorAggregateFunction func,
            @NotNull PerWorkerLocks perWorkerLocks,
            @NotNull ExecutionCircuitBreaker circuitBreaker
    ) {
        final int slot = perWorkerLocks.acquireSlot(workerId, circuitBreaker);
        try {
            final PageFrameMemoryPool frameMemoryPool = frameMemoryPools.getQuick(slot);
            final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex);
            // for functions like `count()`, that do not have arguments we are required to provide
            // count of rows in table in a form of "pageSize >> shr". Since `vaf` doesn't provide column
            // this code used column 0. Assumption here that column 0 is fixed size.
            // This assumption only holds because our aggressive algorithm for "top down columns", e.g.
            // the algorithm that forces page frame to provide only columns required by the select. At the time
            // of writing this code there is no way to return variable length column out of non-keyed aggregation
            // query. This might change if we introduce something like `first(string)`. When this happens we will
            // need to rethink our way of computing size for the count. This would be either type checking column
            // 0 and working out size differently or finding any fixed-size column and using that.
            final long valueAddress = valueColIndex > -1 ? frameMemory.getPageAddress(valueColIndex) : 0;

            // In a keyed aggregation, a zero keyAddress means that the key column holds no data in the
            // frame, a column top or a Parquet row group whose key column is all NULL: every row of
            // the frame has the NULL key.
            final long keyAddress = keyColIndex > -1 ? frameMemory.getPageAddress(keyColIndex) : 0;
            if (pRosti != null) {
                final long oldSize = Rosti.getAllocMemory(pRosti[slot]);
                final boolean isAggregated = keyAddress != 0
                        ? func.aggregate(pRosti[slot], keyAddress, valueAddress, frameRowCount)
                        : aggregateNullKeys(func, pRosti[slot], pNullKeys, valueAddress, valueSize, frameRowCount);
                if (!isAggregated) {
                    if (oomCounter != null) {
                        oomCounter.incrementAndGet();
                    }
                }
                if (raf != null) {
                    raf.updateMemoryUsage(pRosti[slot], oldSize);
                }
            } else {
                func.aggregate(valueAddress, frameRowCount, slot);
            }
        } finally {
            perWorkerLocks.releaseSlot(slot);
        }
    }

    @Override
    public void clear() {
        this.aggregateError = null;
        this.frameMemoryPools = null;
        this.func = null;
        this.pRosti = null;
        this.startedCounter = null;
        this.doneLatch = null;
        this.oomCounter = null;
        this.raf = null;
        this.perWorkerLocks = null;
        this.circuitBreaker = null;
        this.progressState = null;
        this.frameRowCount = 0;
        this.keyColIndex = -1;
        this.valueColIndex = -1;
        this.valueSize = 0;
        this.pNullKeys = 0;
    }

    public void abort(boolean started) {
        if (!started) {
            startedCounter.incrementAndGet();
        }
        try {
            circuitBreaker.cancel();
        } finally {
            doneLatch.countDown();
        }
    }

    public SqlExecutionCircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    public long getFrameRowCount() {
        return frameRowCount;
    }

    public AsyncQueryProgressState getProgressState() {
        return progressState;
    }

    public void run(int workerId, Sequence seq, long cursor) {
        seq.done(cursor);
        runDetached(workerId);
    }

    public void run(int workerId, Sequence seq, long cursor, @NotNull QueryParallelFiberDispatcher dispatcher) {
        final AsyncQueryProgressState ownerProgress = getProgressState();
        Throwable failure = null;
        try {
            seq.done(cursor);
        } catch (Throwable th) {
            failure = th;
        }
        try {
            dispatcher.signalQueueProgress();
        } catch (Throwable th) {
            failure = Misc.foldCleanupFailure(failure, th);
        }
        try {
            runDetached(workerId);
        } catch (Throwable th) {
            failure = Misc.foldCleanupFailure(failure, th);
        }
        try {
            dispatcher.signalOwnerProgress(ownerProgress);
        } catch (Throwable th) {
            failure = Misc.foldCleanupFailure(failure, th);
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    public void runDetached(int workerId) {
        AsyncQueryErrorState aggregateError = this.aggregateError;
        AtomicInteger oomCounter = this.oomCounter;
        int frameIndex = this.frameIndex;
        long frameRowCount = this.frameRowCount;
        int keyColIndex = this.keyColIndex;
        int valueColIndex = this.valueColIndex;
        int valueSize = this.valueSize;
        long pNullKeys = this.pNullKeys;
        long[] pRosti = this.pRosti;
        ObjList<PageFrameMemoryPool> frameMemoryPools = this.frameMemoryPools;
        RostiAllocFacade raf = this.raf;
        VectorAggregateFunction func = this.func;
        SqlExecutionCircuitBreaker circuitBreaker = this.circuitBreaker;
        AtomicInteger startedCounter = this.startedCounter;
        CountDownLatchSPI doneLatch = this.doneLatch;
        PerWorkerLocks perWorkerLocks = this.perWorkerLocks;

        aggregate(
                workerId,
                oomCounter,
                aggregateError,
                frameIndex,
                frameRowCount,
                keyColIndex,
                valueColIndex,
                valueSize,
                pNullKeys,
                pRosti,
                frameMemoryPools,
                raf,
                func,
                perWorkerLocks,
                circuitBreaker,
                startedCounter,
                doneLatch
        );
    }

    private static void aggregate(
            int workerId,
            AtomicInteger oomCounter,
            @NotNull AsyncQueryErrorState aggregateError,
            int frameIndex,
            long frameRowCount,
            int keyColIndex,
            int valueColIndex,
            int valueSize,
            long pNullKeys,
            long[] pRosti,
            ObjList<PageFrameMemoryPool> frameMemoryPools,
            RostiAllocFacade raf,
            VectorAggregateFunction func,
            PerWorkerLocks perWorkerLocks,
            SqlExecutionCircuitBreaker circuitBreaker,
            AtomicInteger startedCounter,
            CountDownLatchSPI doneLatch
    ) {
        startedCounter.incrementAndGet();

        try {
            // The initial poll can throw cancellation after yielding. Complete the entry on
            // that path too, but keep the normal countdown outside this error-handling scope.
            if (!circuitBreaker.checkIfTrippedOrYield() && (oomCounter == null || oomCounter.get() <= 0)) {
                aggregateUnsafe(
                        workerId,
                        oomCounter,
                        frameIndex,
                        frameRowCount,
                        keyColIndex,
                        valueColIndex,
                        valueSize,
                        pNullKeys,
                        pRosti,
                        frameMemoryPools,
                        raf,
                        func,
                        perWorkerLocks,
                        circuitBreaker
                );
            }
        } catch (Throwable th) {
            Throwable failure = th;
            try {
                aggregateError.setError(th);
            } catch (Throwable cleanupFailure) {
                failure = Misc.foldCleanupFailure(failure, cleanupFailure);
            }
            try {
                circuitBreaker.cancel();
            } catch (Throwable cleanupFailure) {
                failure = Misc.foldCleanupFailure(failure, cleanupFailure);
            }
            try {
                doneLatch.detachResourceMemoryAndCountDown();
            } catch (Throwable cleanupFailure) {
                failure = Misc.foldCleanupFailure(failure, cleanupFailure);
            }
            CairoException.rethrowCleanupFailure(failure);
            return;
        }
        doneLatch.detachResourceMemoryAndCountDown();
    }

    // Every row of the frame has the NULL key: the function aggregates them under the NULL key in
    // the map, as it does rows whose key is NULL, a chunk of NULL keys at a time.
    private static boolean aggregateNullKeys(
            VectorAggregateFunction func,
            long pRosti,
            long pNullKeys,
            long valueAddress,
            int valueSize,
            long frameRowCount
    ) {
        for (long lo = 0; lo < frameRowCount; lo += NULL_KEY_CHUNK_SIZE) {
            final long count = Math.min(NULL_KEY_CHUNK_SIZE, frameRowCount - lo);
            // A zero value address stays zero: the value column holds no data in the frame either.
            final long chunkValueAddress = valueAddress != 0 ? valueAddress + lo * valueSize : 0;
            if (!func.aggregateNullKeys(pRosti, pNullKeys, chunkValueAddress, count)) {
                return false;
            }
        }
        return true;
    }

    void of(
            int frameIndex,
            long frameRowCount,
            int keyColIndex,
            int valueColIndex,
            int valueSize,
            long pNullKeys,
            @NotNull VectorAggregateFunction vaf,
            long @Nullable [] pRosti,
            @NotNull ObjList<PageFrameMemoryPool> frameMemoryPools,
            @NotNull AtomicInteger startedCounter,
            @NotNull CountDownLatchSPI doneLatch,
            // OOM is not possible when aggregation is not keyed
            @Nullable AtomicInteger oomCounter,
            @NotNull AsyncQueryErrorState aggregateError,
            @Nullable RostiAllocFacade raf,
            @NotNull PerWorkerLocks perWorkerLocks,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @NotNull AsyncQueryProgressState progressState
    ) {
        this.frameIndex = frameIndex;
        this.frameRowCount = frameRowCount;
        this.keyColIndex = keyColIndex;
        this.valueColIndex = valueColIndex;
        this.valueSize = valueSize;
        this.pNullKeys = pNullKeys;
        this.pRosti = pRosti;
        this.frameMemoryPools = frameMemoryPools;
        this.func = vaf;
        this.startedCounter = startedCounter;
        this.doneLatch = doneLatch;
        this.oomCounter = oomCounter;
        this.aggregateError = aggregateError;
        this.raf = raf;
        this.perWorkerLocks = perWorkerLocks;
        this.circuitBreaker = circuitBreaker;
        this.progressState = progressState;
    }
}
