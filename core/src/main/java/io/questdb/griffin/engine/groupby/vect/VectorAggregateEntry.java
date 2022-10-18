/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.std.*;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

public class VectorAggregateEntry extends AbstractLockable implements Mutable {
    private long[] pRosti;
    private long keyAddress;
    private long valueAddress;
    private long valueCount;
    private int columnSizeShr;
    private VectorAggregateFunction func;
    private CountDownLatchSPI doneLatch;
    private AtomicInteger oomCounter;
    private RostiAllocFacade raf;
    private ExecutionCircuitBreaker circuitBreaker;

    @Override
    public void clear() {
        this.valueAddress = 0;
        this.valueCount = 0;
        func = null;
    }

    public boolean run(int workerId) {
        if (tryLock()) {
            if (!circuitBreaker.checkIfTripped() &&
                    (oomCounter == null || oomCounter.get() == 0)) {
                if (pRosti != null) {
                    long oldSize = Rosti.getAllocMemory(pRosti[workerId]);
                    if (!func.aggregate(pRosti[workerId], keyAddress, valueAddress, valueCount, columnSizeShr, workerId)) {
                        oomCounter.incrementAndGet();
                    }
                    raf.updateMemoryUsage(pRosti[workerId], oldSize);
                } else {
                    func.aggregate(valueAddress, valueCount, columnSizeShr, workerId);
                }
            }
            doneLatch.countDown();
            return true;
        }
        return false;
    }

    void of(
            int sequence,
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
            ExecutionCircuitBreaker circuitBreaker
    ) {
        of(sequence);
        this.pRosti = pRosti;
        this.keyAddress = keyPageAddress;
        this.valueAddress = valuePageAddress;
        this.valueCount = valuePageCount;
        this.func = vaf;
        this.columnSizeShr = columnSizeShr;
        this.doneLatch = doneLatch;
        this.oomCounter = oomCounter;
        this.raf = raf;
        this.circuitBreaker = circuitBreaker;
    }
}
