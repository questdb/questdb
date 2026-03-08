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

import io.questdb.std.Os;

/**
 * M - multi thread
 * C - consumer
 */
public class MCSequence extends AbstractMSequence {

    public MCSequence(int cycle) {
        this(cycle, null);
    }

    public MCSequence(int cycle, WaitStrategy waitStrategy) {
        super(cycle, waitStrategy);
    }

    public void clear() {
        while (true) {
            long n = next();
            if (n == -1) {
                break;
            }
            if (n != -2) {
                done(n);
            }
        }
    }

    public <T> void consumeAll(RingQueue<T> queue, QueueConsumer<T> consumer) {
        long cursor;
        do {
            cursor = next();
            if (cursor > -1) {
                consumer.consume(queue.get(cursor));
                done(cursor);
            } else if (cursor == -2) {
                Os.pause();
            }
        } while (cursor != -1);
    }

    @Override
    public long next() {
        long cached = cache;
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
