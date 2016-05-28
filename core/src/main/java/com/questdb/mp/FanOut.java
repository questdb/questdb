/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

import com.questdb.ex.FatalError;
import com.questdb.misc.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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

    @SuppressFBWarnings("CVAA_CONTRAVARIANT_ELEMENT_ASSIGNMENT")
    // this is firebug bug, the code does not write to array elements
    // it has to take a copy of this.barriers as this reference can change while
    // loop is in flight
    @Override
    public long availableIndex(final long lo) {
        final Barrier[] barriers = this.barriers;
        long l = lo;
        for (int i = 0, n = barriers.length; i < n; i++) {
            long cursor = Unsafe.arrayGet(barriers, i).availableIndex(l);
            l = l < cursor ? l : cursor;
        }
        return l;
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
            throw new FatalError("Internal error", e);
        }
    }
}
