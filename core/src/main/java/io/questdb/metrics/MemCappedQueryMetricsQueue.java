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

package io.questdb.metrics;

import io.questdb.mp.ConcurrentQueue;
import io.questdb.std.ObjectFactory;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicLong;

public class MemCappedQueryMetricsQueue {
    public static final ObjectFactory<QueryMetrics> ITEM_FACTORY = QueryMetrics::new;
    private static final long QUERY_MEM_USAGE_LIMIT = 64 << 20;
    private final AtomicLong droppedItemCount = new AtomicLong();
    private final AtomicLong queryTextMemUsage = new AtomicLong();
    private final @NotNull ConcurrentQueue<QueryMetrics> queue;

    public MemCappedQueryMetricsQueue() {
        this.queue = new ConcurrentQueue<>(ITEM_FACTORY);
    }

    public void offer(@NotNull QueryMetrics item) {
        int size = QueryMetrics.OBJECT_SIZE + item.queryText.length();
        long memUsage = queryTextMemUsage.addAndGet(size);
        if (memUsage > QUERY_MEM_USAGE_LIMIT) {
            droppedItemCount.addAndGet(1);
            queryTextMemUsage.addAndGet(-size);
            return;
        }
        queue.enqueue(item);
    }

    public boolean tryDequeue(@NotNull QueryMetrics dest) {
        boolean didDequeue = queue.tryDequeue(dest);
        if (didDequeue) {
            queryTextMemUsage.addAndGet(-dest.queryText.length());
        }
        return didDequeue;
    }
}
