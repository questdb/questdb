/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.mp;

import com.questdb.std.Numbers;
import com.questdb.std.Unsafe;

import java.util.Arrays;

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
        Unsafe.getUnsafe().putOrderedInt(flags, (((int) (cursor & mask)) << Unsafe.INT_SCALE) + Unsafe.INT_OFFSET, (int) (cursor >>> shift));
        barrier.getWaitStrategy().signal();
    }

    private boolean available0(long lo) {
        return Unsafe.getUnsafe().getIntVolatile(flags, (((int) (lo & mask)) << Unsafe.INT_SCALE) + Unsafe.INT_OFFSET) == (int) (lo >>> shift);
    }
}
