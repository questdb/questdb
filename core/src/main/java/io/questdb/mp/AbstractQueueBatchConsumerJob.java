/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.ValueHolderList;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public abstract class AbstractQueueBatchConsumerJob<T extends ValueHolder<T>> implements Job {
    private static final int BATCH_LIMIT = 1024;
    private static final int INITIAL_CAPACITY = 128;
    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<AbstractQueueBatchConsumerJob> LOCK = AtomicIntegerFieldUpdater.newUpdater(
            AbstractQueueBatchConsumerJob.class, "lock");

    private final ValueHolderList<T> buffer;
    private final ConcurrentQueue<T> queue;
    private volatile int lock;

    public AbstractQueueBatchConsumerJob(ConcurrentQueue<T> queue) {
        this.queue = queue;
        this.buffer = new ValueHolderList<>(queue.itemFactory(), INITIAL_CAPACITY);
    }

    @Override
    public boolean run(int workerId, @NotNull RunStatus runStatus) {
        if (!canRun() || !LOCK.compareAndSet(this, 0, 1)) {
            return false;
        }
        try {
            buffer.clear();
            for (int i = 0; i < BATCH_LIMIT && queue.tryDequeue(buffer.peekNextHolder()); i++) {
                buffer.commitNextHolder();
            }
            if (buffer.size() > 0) {
                doRun(workerId, buffer, runStatus);
            }
            return false;
        } finally {
            LOCK.lazySet(this, 0);
        }
    }

    protected boolean canRun() {
        return true;
    }

    protected abstract boolean doRun(int workerId, ValueHolderList<T> buffer, RunStatus runStatus);
}
