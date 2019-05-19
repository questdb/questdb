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
