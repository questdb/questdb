/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.mp;

import com.nfsdb.ex.FatalError;
import com.nfsdb.misc.Unsafe;
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
