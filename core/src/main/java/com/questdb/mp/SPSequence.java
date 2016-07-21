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

public class SPSequence extends AbstractSSequence {
    private final int cycle;
    protected int p0, p1, p2, p3, p4;
    protected int p5, p6, p7, p8, p9;
    private volatile long index = -1;
    private long cache = -1;

    private SPSequence(int cycle, WaitStrategy waitStrategy) {
        super(waitStrategy);
        this.cycle = cycle;
    }

    public SPSequence(int cycle) {
        this(cycle, null);
    }

    @Override
    public long availableIndex(long lo) {
        return index;
    }

    @Override
    public long current() {
        return index;
    }

    @Override
    public void done(long cursor) {
        index = cursor;
        barrier.signal();
    }

    @Override
    public long next() {
        long next = index + 1;
        long lo = next - cycle;
        return lo > cache && lo > (cache = barrier.availableIndex(lo)) ? -1 : next;
    }

    @Override
    public void reset() {

    }
}
