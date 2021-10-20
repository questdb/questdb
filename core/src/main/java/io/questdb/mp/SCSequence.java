/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

public class SCSequence extends AbstractSSequence {

    public SCSequence(WaitStrategy waitStrategy) {
        super(waitStrategy);
    }

    public SCSequence(long index, WaitStrategy waitStrategy) {
        super(waitStrategy);
        this.value = index;
    }

    public SCSequence() {
    }

    SCSequence(long index) {
        this.value = index;
    }

    public long available() {
        return cache + 1;
    }

    @Override
    public long availableIndex(long lo) {
        return this.value;
    }

    @Override
    public long current() {
        return value;
    }

    @Override
    public void done(long cursor) {
        this.value = cursor;
        barrier.getWaitStrategy().signal();
    }

    @Override
    public long next() {
        long next = getValue();
        if (next < cache) {
            return next + 1;
        }

        return next0(next + 1);
    }

    private long next0(long next) {
        cache = barrier.availableIndex(next);
        return next > cache ? -1 : next;
    }

    public <T> boolean consumeAll(RingQueue<T> queue, QueueConsumer<T> consumer) {
        long cursor = next();
        if (cursor < 0) {
            return false;
        }

        do {
            if (cursor > -1) {
                final long available = available();
                while (cursor < available) {
                    consumer.consume(queue.get(cursor++));
                }
                done(available - 1);
            }
        } while ((cursor = next()) != -1);

        return true;
    }
}
