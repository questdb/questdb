/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.mp;

/**
 * M - multi thread
 * P - producer
 */
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

        if (lo <= cached) {
            return casValue(current, next) ? next : -2;
        }

        final long avail = barrier.availableIndex(lo);

        if (avail > cached) {
            setCacheFenced(avail);
            if (lo <= avail) {
                return casValue(current, next) ? next : -2;
            }
        }
        return -1;
    }
}
