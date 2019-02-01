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

public class MPSequence extends AbstractMSequence {
    private final int cycle;

    public MPSequence(int cycle) {
        this(cycle, NullWaitStrategy.INSTANCE);
    }

    private MPSequence(int cycle, WaitStrategy waitStrategy) {
        super(cycle, waitStrategy);
        this.cycle = cycle;
    }

    @Override
    public long next() {
        // reading cache before value is essential because algo relies on barrier inserted by value read.
        long cached = cache;
        long current = value;
        long next = current + 1;
        long lo = next - cycle;

        if (lo > cached) {
            long avail = barrier.availableIndex(lo);

            if (avail > cached) {
                setCacheFenced(avail);
                if (lo > avail) {
                    return -1;
                }
            } else {
                return -1;
            }
        }

        return casValue(current, next) ? next : -2;
    }
}
