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

import com.nfsdb.misc.Unsafe;

public class FanOut implements Barrier {
    private static final long BARRIERS;
    private final Sequence[] barriers;

    public FanOut(Sequence... barriers) {
        this.barriers = barriers;
    }

    public void add(Sequence barrier) {
        Sequence[] _new;
        do {
            Sequence[] barriers = this.barriers;
            if (indexOf(barriers, barrier) > -1) {
                return;
            }

            int len = barriers.length;
            _new = new Sequence[len + 1];
            _new[0] = barrier;
            System.arraycopy(barriers, 0, _new, 1, len);

        } while (!Unsafe.getUnsafe().compareAndSwapObject(this, BARRIERS, barriers, _new));
    }

    @Override
    public long availableIndex(long lo) {
        Barrier[] barriers = this.barriers;
        for (int i = 0, n = barriers.length; i < n; i++) {
            long cursor = Unsafe.arrayGet(barriers, i).availableIndex(lo);
            lo = lo < cursor ? lo : cursor;
        }
        return lo;
    }

    @Override
    public void signal() {
        Sequence[] barriers = this.barriers;
        for (int i = 0, n = barriers.length; i < n; i++) {
            Unsafe.arrayGet(barriers, i).signal();
        }
    }

    public void followedBy(Barrier barrier) {
        for (int i = 0, n = barriers.length; i < n; i++) {
            Unsafe.arrayGet(barriers, i).followedBy(barrier);
        }
    }

    public void remove(Sequence barrier) {
        Sequence[] _new;
        do {
            Sequence[] barriers = this.barriers;
            int index;
            if ((index = indexOf(barriers, barrier)) == -1) {
                return;
            }

            int len = barriers.length;
            _new = new Sequence[len - 1];
            System.arraycopy(barriers, 0, _new, 0, index);
            System.arraycopy(barriers, index + 1, _new, index, len - index - 1);
        } while (!Unsafe.getUnsafe().compareAndSwapObject(this, BARRIERS, barriers, _new));
    }

    private static int indexOf(Sequence[] barriers, Sequence barrier) {
        for (int i = 0, n = barriers.length; i < n; i++) {
            if (barrier == Unsafe.arrayGet(barriers, i)) {
                return i;
            }
        }

        return -1;
    }

    static {
        try {
            BARRIERS = Unsafe.getUnsafe().objectFieldOffset(FanOut.class.getDeclaredField("barriers"));
        } catch (NoSuchFieldException e) {
            throw new Error("Internal error", e);
        }
    }
}
