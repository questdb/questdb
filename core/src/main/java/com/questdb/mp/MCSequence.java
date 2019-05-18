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

public class MCSequence extends AbstractMSequence {

    public MCSequence(int cycle) {
        this(cycle, null);
    }

    public MCSequence(int cycle, WaitStrategy waitStrategy) {
        super(cycle, waitStrategy);
    }

    public <T> void consumeAll(RingQueue<T> queue, QueueConsumer<T> consumer) {
        long cursor;
        do {
            cursor = next();
            if (cursor > -1) {
                consumer.consume(queue.get(cursor));
                done(cursor);
            }
        } while (cursor != -1);

    }

    @Override
    public long next() {
        long cached = cache;
        // this is a volatile read, we should have correct order for "cache" too
        long current = value;
        long next = current + 1;

        if (next > cached) {
            long avail = barrier.availableIndex(next);
            if (avail > cached) {
                setCacheFenced(avail);
                if (next > avail) {
                    return -1;
                }
            } else {
                return -1;
            }
        }
        return casValue(current, next) ? next : -2;
    }
}
