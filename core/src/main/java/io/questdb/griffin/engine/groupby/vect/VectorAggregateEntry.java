/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.mp.CountDownLatchSPI;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;

public class VectorAggregateEntry implements Mutable {

    private static final long TARGET_SEQUENCE_OFFSET;

    static {
        TARGET_SEQUENCE_OFFSET = Unsafe.getFieldOffset(VectorAggregateEntry.class, "targetSequence");
    }

    private long[] pRosti;
    private long keyAddress;
    private long valueAddress;
    private long valueCount;
    private VectorAggregateFunction func;
    private int srcSequence;
    // to "lock" the entry thread must successfully CAS targetSequence form "srcSequence" value
    // to "srcSequence+1". Executing thread must not be changing value of "srcSequence"
    private int targetSequence;
    private CountDownLatchSPI doneLatch;

    void of(
            int sequence,
            VectorAggregateFunction vaf,
            long[] pRosti,
            long keyPageAddress,
            long valuePageAddress,
            long valuePageCount,
            CountDownLatchSPI doneLatch
    ) {
        this.pRosti = pRosti;
        this.keyAddress = keyPageAddress;
        this.valueAddress = valuePageAddress;
        this.valueCount = valuePageCount;
        this.func = vaf;
        this.srcSequence = sequence;
        this.targetSequence = sequence;
        this.doneLatch = doneLatch;
    }

    public boolean tryLock() {
        return Unsafe.cas(this, TARGET_SEQUENCE_OFFSET, srcSequence, srcSequence + 1);
    }

    public boolean run(int workerId) {
        if (tryLock()) {
            if (pRosti != null) {
                func.aggregate(pRosti[workerId], keyAddress, valueAddress, valueCount, workerId);
            } else {
                func.aggregate(valueAddress, valueCount, workerId);
            }
            doneLatch.countDown();
            return true;
        }
        return false;
    }

    @Override
    public void clear() {
        this.valueAddress = 0;
        this.valueCount = 0;
        func = null;
    }
}
