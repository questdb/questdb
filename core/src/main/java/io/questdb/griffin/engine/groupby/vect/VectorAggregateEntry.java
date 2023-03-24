/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.mp.Sequence;
import io.questdb.std.Mutable;
import io.questdb.std.Rosti;
import io.questdb.std.RostiAllocFacade;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

public class VectorAggregateEntry implements Mutable {
    private ExecutionCircuitBreaker circuitBreaker;
    private int columnSizeShr;
    private CountDownLatchSPI doneLatch;
    private VectorAggregateFunction func;
    private long keyAddress;
    private AtomicInteger oomCounter;
    private long[] pRosti;
    private PerWorkerLocks perWorkerLocks;
    private RostiAllocFacade raf;
    private long valueAddress;
    private long valueCount;

    @Override
    public void clear() {
        this.valueAddress = 0;
        this.valueCount = 0;
        this.func = null;
    }

    public void run(int workerId, Sequence seq, long cursor) {
        long keyAddress = this.keyAddress;
        long valueAddress = this.valueAddress;
        long valueCount = this.valueCount;
        int columnSizeShr = this.columnSizeShr;
        AtomicInteger oomCounter = this.oomCounter;
        long[] pRosti = this.pRosti;
        RostiAllocFacade raf = this.raf;
        VectorAggregateFunction func = this.func;
        ExecutionCircuitBreaker circuitBreaker = this.circuitBreaker;
        CountDownLatchSPI doneLatch = this.doneLatch;
        PerWorkerLocks perWorkerLocks = this.perWorkerLocks;

        seq.done(cursor);
        run(workerId, keyAddress, valueAddress, valueCount, columnSizeShr, oomCounter, pRosti, raf, func, perWorkerLocks, circuitBreaker, doneLatch);
    }

    private static void run(
            int workerId,
            long keyAddress,
            long valueAddress,
            long valueCount,
            int columnSizeShr,
            AtomicInteger oomCounter,
            long[] pRosti,
            RostiAllocFacade raf,
            VectorAggregateFunction func,
            PerWorkerLocks perWorkerLocks,
            ExecutionCircuitBreaker circuitBreaker,
            CountDownLatchSPI doneLatch
    ) {
        if (circuitBreaker.checkIfTripped() || (oomCounter != null && oomCounter.get() > 0)) {
            doneLatch.countDown();
            return;
        }

        final int slot = perWorkerLocks.acquireSlot(workerId, circuitBreaker);
        try {
            if (pRosti != null) {
                long oldSize = Rosti.getAllocMemory(pRosti[slot]);
                if (!func.aggregate(pRosti[slot], keyAddress, valueAddress, valueCount, columnSizeShr, slot)) {
                    if (oomCounter != null) {
                        oomCounter.incrementAndGet();
                    }
                }
                raf.updateMemoryUsage(pRosti[slot], oldSize);
            } else {
                func.aggregate(valueAddress, valueCount, columnSizeShr, slot);
            }
        } finally {
            perWorkerLocks.releaseSlot(slot);
            doneLatch.countDown();
        }
    }

    void of(
            VectorAggregateFunction vaf,
            long[] pRosti,
            long keyPageAddress,
            long valuePageAddress,
            long valuePageCount,
            int columnSizeShr,
            CountDownLatchSPI doneLatch,
            // oom is not possible when aggregation is not keyed
            @Nullable AtomicInteger oomCounter,
            RostiAllocFacade raf,
            PerWorkerLocks perWorkerLocks,
            ExecutionCircuitBreaker circuitBreaker
    ) {
        this.pRosti = pRosti;
        this.keyAddress = keyPageAddress;
        this.valueAddress = valuePageAddress;
        this.valueCount = valuePageCount;
        this.func = vaf;
        this.columnSizeShr = columnSizeShr;
        this.doneLatch = doneLatch;
        this.oomCounter = oomCounter;
        this.raf = raf;
        this.perWorkerLocks = perWorkerLocks;
        this.circuitBreaker = circuitBreaker;
    }
}
