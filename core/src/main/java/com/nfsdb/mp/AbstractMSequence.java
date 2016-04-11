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

import com.nfsdb.misc.Numbers;
import com.nfsdb.misc.Unsafe;

import java.util.Arrays;

abstract class AbstractMSequence extends AbstractSSequence {
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
    public long availableIndex(final long lo) {
        long l = lo;
        for (long hi = this.index.fencedGet() + 1; l < hi && available0(l); l++) ;
        return l - 1;
    }

    @Override
    public long current() {
        return index.fencedGet();
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
