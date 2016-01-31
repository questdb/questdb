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

package com.nfsdb.mp;

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
