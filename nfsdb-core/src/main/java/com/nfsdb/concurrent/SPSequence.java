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

public class SPSequence extends AbstractSSequence {
    private final int cycle;
    private volatile long index = -1;
    private volatile long cache = -1;

    public SPSequence(int cycle, WaitStrategy waitStrategy) {
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
