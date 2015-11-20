/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.concurrent;

import com.nfsdb.misc.Numbers;
import com.nfsdb.misc.Unsafe;

import java.util.Arrays;

public abstract class AbstractMSequence extends AbstractSSequence {
    final PLong index = new PLong();
    final PLong cache = new PLong();
    private final int flags[];
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
    public long availableIndex(long lo) {
        for (long hi = this.index.fencedGet() + 1; lo < hi && available0(lo); lo++) ;
        return lo - 1;
    }

    @Override
    public void done(long cursor) {
        Unsafe.getUnsafe().putOrderedInt(flags, ((int) (cursor & mask)) * Unsafe.INT_SCALE + Unsafe.INT_OFFSET, (int) (cursor >>> shift));
        barrier.signal();
    }

    @Override
    public void reset() {
        index.fencedSet(-1);
        Arrays.fill(flags, -1);
        cache.fencedSet(-1);
    }

    private boolean available0(long lo) {
        return Unsafe.getUnsafe().getIntVolatile(flags, ((int) (lo & mask)) * Unsafe.INT_SCALE + Unsafe.INT_OFFSET) == (int) (lo >>> shift);
    }
}
