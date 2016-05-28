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

public class MPSequence extends AbstractMSequence {
    private final int cycle;

    public MPSequence(int cycle) {
        this(cycle, null);
    }

    private MPSequence(int cycle, WaitStrategy waitStrategy) {
        super(cycle, waitStrategy);
        this.cycle = cycle;
    }

    @Override
    public long next() {
        long current = index.fencedGet();
        long next = current + 1;
        long lo = next - cycle;
        long cached = cache.fencedGet();

        if (lo > cached) {
            long avail = barrier.availableIndex(lo);

            if (avail > cached) {
                cache.fencedSet(avail);
                if (lo > avail) {
                    return -1;
                }
            } else {
                return -1;
            }
        }

        return index.cas(current, next) ? next : -2;
    }
}
