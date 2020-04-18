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

    private long address;
    private long count;
    private VectorAggregateFunction func;
    private int srcSequence;
    // to "lock" the entry thread must successfully CAS targetSequence form "srcSequence" value
    // to "srcSequence+1". Executing thread must not be changing value of "srcSequence"
    private int targetSequence;
    private CountDownLatchSPI doneLatch;

    void of(int counter, VectorAggregateFunction vaf, long pageAddress, long pageValueCount, CountDownLatchSPI doneLatch) {
        this.address = pageAddress;
        this.count = pageValueCount;
        this.func = vaf;
        this.srcSequence = counter;
        this.targetSequence = counter;
        this.doneLatch = doneLatch;
    }

    public boolean tryLock() {
        return Unsafe.cas(this, TARGET_SEQUENCE_OFFSET, srcSequence, srcSequence + 1);
    }

    public boolean run(int workerId) {
        if (tryLock()) {
            func.aggregate(address, count, workerId);
            doneLatch.countDown();
            return true;
        }
        return false;
    }

    @Override
    public void clear() {
        this.address = 0;
        this.count = 0;
        func = null;
    }
}
