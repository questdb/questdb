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

package io.questdb.mp;

import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;

import java.util.Arrays;

//abstract multi producer or consumer sequence 
abstract class AbstractMSequence extends AbstractSSequence {
    private final int[] flags;
    private final int mask;
    private final int shift;

    AbstractMSequence(int cycle, WaitStrategy waitStrategy) {
        super(waitStrategy);
        this.flags = new int[cycle];
        Arrays.fill(flags, -1);
        this.mask = cycle - 1;
        this.shift = Numbers.msb(cycle);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public long availableIndex(final long lo) {
        long l = lo;
        for (long hi = this.value + 1; l < hi && available0(l); l++) ;
        return l - 1;
    }

    @Override
    public long current() {
        return value;
    }

    @Override
    public void done(long cursor) {
        Unsafe.getUnsafe().putOrderedInt(flags, ((cursor & mask) << Unsafe.INT_SCALE) + Unsafe.INT_OFFSET, (int) (cursor >>> shift));
        barrier.getWaitStrategy().signal();
    }

    @Override
    public void setCurrent(long value) {
        this.value = value;
    }

    private boolean available0(long lo) {
        return Unsafe.getUnsafe().getIntVolatile(flags, (((lo & mask)) << Unsafe.INT_SCALE) + Unsafe.INT_OFFSET) == (int) (lo >>> shift);
    }
}
