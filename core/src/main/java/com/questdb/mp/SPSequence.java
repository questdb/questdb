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

public class SPSequence extends AbstractSSequence {
    private final int cycle;

    private SPSequence(int cycle, WaitStrategy waitStrategy) {
        super(waitStrategy);
        this.cycle = cycle;
    }

    public SPSequence(int cycle) {
        this(cycle, NullWaitStrategy.INSTANCE);
    }

    public long available() {
        return cache + cycle + 1;
    }

    @Override
    public long availableIndex(long lo) {
        return value;
    }

    @Override
    public long current() {
        return value;
    }

    @Override
    public void done(long cursor) {
        value = cursor;
        barrier.getWaitStrategy().signal();
    }

    @Override
    public long next() {
        long next = getValue() + 1;
        long lo = next - cycle;
        return lo > cache && lo > (cache = barrier.availableIndex(lo)) ? -1 : next;
    }
}
