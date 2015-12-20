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

public class SCSequence extends AbstractSSequence {

    private volatile long index = -1;
    private volatile long cache = -1;

    public SCSequence(WaitStrategy waitStrategy) {
        super(waitStrategy);
    }

    public SCSequence(long index, WaitStrategy waitStrategy) {
        super(waitStrategy);
        this.index = index;
    }

    public SCSequence() {
    }

    public SCSequence(long index) {
        this.index = index;
    }

    @Override
    public long availableIndex(long lo) {
        return this.index;
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
        return next > cache && next > (cache = barrier.availableIndex(next)) ? -1 : next;
    }

    @Override
    public void reset() {
        index = -1;
        cache = -1;
    }
}
