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

//Single Producer Sequence
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

    @Override
    public void setCurrent(long value) {
        this.value = value;
    }
}
