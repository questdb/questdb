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

import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.mp.Sequence;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.Rosti;
import io.questdb.std.RostiAllocFacade;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class VectorAggregateEntry implements Mutable {
    private ExecutionCircuitBreaker circuitBreaker;
    private CountDownLatchSPI doneLatch;
    private int frameIndex;
    private ObjList<PageFrameMemoryPool> frameMemoryPools;
    private long frameRowCount;
    private VectorAggregateFunction func;
    private AtomicBoolean hasNullKeyRows;
    private int keyColIndex;
    private AtomicInteger oomCounter;
    private long[] pRosti;
    private PerWorkerLocks perWorkerLocks;
    private RostiAllocFacade raf;
    private AtomicInteger startedCounter;
    private int valueColIndex;

    public static void aggregateUnsafe(
            int workerId,
            @Nullable AtomicInteger oomCounter,
            @NotNull AtomicBoolean hasNullKeyRows,
            int frameIndex,
            long frameRowCount,
            int keyColIndex,
            int valueColIndex,
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

            // Zero keyAddress means non-keyed aggregation or column top.
            final long keyAddress = keyColIndex > -1 ? frameMemory.getPageAddress(keyColIndex) : 0;
            if (pRosti != null && keyAddress != 0) {
                final long oldSize = Rosti.getAllocMemory(pRosti[slot]);
                if (!func.aggregate(pRosti[slot], keyAddress, valueAddress, frameRowCount)) {
                    if (oomCounter != null) {
                        oomCounter.incrementAndGet();
                    }
                }
                if (raf != null) {
                    raf.updateMemoryUsage(pRosti[slot], oldSize);
                }
            } else {
                if (pRosti != null && frameRowCount > 0) {
                    // Every row in this frame has a null key. wrapUp() only creates that group when the
                    // value it folds in is non-null, so record row presence here; the coordinator
                    // materializes the group once, in the rosti that survives the merge.
                    // The factory clears the flag only between drains, so within a drain it travels
                    // false -> true once. The guard read drops the repeat stores, which leaves a single
                    // cache line invalidation instead of one per (frame, vaf) that lands here. A worker
                    // reading a stale false pays one redundant store and nothing else.
                    if (!hasNullKeyRows.get()) {
                        hasNullKeyRows.set(true);
                    }
                }
                func.aggregate(valueAddress, frameRowCount, slot);
            }
        } finally {
            perWorkerLocks.releaseSlot(slot);
        }
    }

    @Override
    public void clear() {
        this.frameMemoryPools = null;
        this.func = null;
        this.hasNullKeyRows = null;
        this.pRosti = null;
        this.startedCounter = null;
        this.doneLatch = null;
        this.oomCounter = null;
        this.raf = null;
        this.perWorkerLocks = null;
        this.circuitBreaker = null;
        this.frameRowCount = 0;
        this.keyColIndex = -1;
        this.valueColIndex = -1;
    }

    public void run(int workerId, Sequence seq, long cursor) {
        AtomicInteger oomCounter = this.oomCounter;
        AtomicBoolean hasNullKeyRows = this.hasNullKeyRows;
        int frameIndex = this.frameIndex;
        long frameRowCount = this.frameRowCount;
        int keyColIndex = this.keyColIndex;
        int valueColIndex = this.valueColIndex;
        long[] pRosti = this.pRosti;
        ObjList<PageFrameMemoryPool> frameMemoryPools = this.frameMemoryPools;
        RostiAllocFacade raf = this.raf;
        VectorAggregateFunction func = this.func;
        ExecutionCircuitBreaker circuitBreaker = this.circuitBreaker;
        AtomicInteger startedCounter = this.startedCounter;
        CountDownLatchSPI doneLatch = this.doneLatch;
        PerWorkerLocks perWorkerLocks = this.perWorkerLocks;

        seq.done(cursor);
        aggregate(
                workerId,
                oomCounter,
                hasNullKeyRows,
                frameIndex,
                frameRowCount,
                keyColIndex,
                valueColIndex,
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
            AtomicBoolean hasNullKeyRows,
            int frameIndex,
            long frameRowCount,
            int keyColIndex,
            int valueColIndex,
            long[] pRosti,
            ObjList<PageFrameMemoryPool> frameMemoryPools,
            RostiAllocFacade raf,
            VectorAggregateFunction func,
            PerWorkerLocks perWorkerLocks,
            ExecutionCircuitBreaker circuitBreaker,
            AtomicInteger startedCounter,
            CountDownLatchSPI doneLatch
    ) {
        // GroupByVectorAggregateJob.doRun() swallows any Throwable that escapes run(), so a throw
        // before the countdown leaves the coordinator spinning forever in runWhatsLeft(), which has
        // no circuit-breaker exit. The try therefore covers the counter bump and the breaker probe
        // as well as the aggregation itself, and the finally is the single countdown site.
        try {
            startedCounter.incrementAndGet();

            if (circuitBreaker.checkIfTripped() || (oomCounter != null && oomCounter.get() > 0)) {
                return;
            }

            aggregateUnsafe(
                    workerId,
                    oomCounter,
                    hasNullKeyRows,
                    frameIndex,
                    frameRowCount,
                    keyColIndex,
                    valueColIndex,
                    pRosti,
                    frameMemoryPools,
                    raf,
                    func,
                    perWorkerLocks,
                    circuitBreaker
            );
        } finally {
            doneLatch.countDown();
        }
    }

    void of(
            int frameIndex,
            long frameRowCount,
            int keyColIndex,
            int valueColIndex,
            @NotNull VectorAggregateFunction vaf,
            long @Nullable [] pRosti,
            @NotNull ObjList<PageFrameMemoryPool> frameMemoryPools,
            @NotNull AtomicInteger startedCounter,
            @NotNull CountDownLatchSPI doneLatch,
            // OOM is not possible when aggregation is not keyed
            @Nullable AtomicInteger oomCounter,
            @NotNull AtomicBoolean hasNullKeyRows,
            @Nullable RostiAllocFacade raf,
            @NotNull PerWorkerLocks perWorkerLocks,
            @NotNull ExecutionCircuitBreaker circuitBreaker
    ) {
        this.frameIndex = frameIndex;
        this.frameRowCount = frameRowCount;
        this.keyColIndex = keyColIndex;
        this.valueColIndex = valueColIndex;
        this.pRosti = pRosti;
        this.frameMemoryPools = frameMemoryPools;
        this.func = vaf;
        this.startedCounter = startedCounter;
        this.doneLatch = doneLatch;
        this.oomCounter = oomCounter;
        this.hasNullKeyRows = hasNullKeyRows;
        this.raf = raf;
        this.perWorkerLocks = perWorkerLocks;
        this.circuitBreaker = circuitBreaker;
    }
}
