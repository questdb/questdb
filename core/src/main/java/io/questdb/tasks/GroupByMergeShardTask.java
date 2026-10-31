/*+*****************************************************************************
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

package io.questdb.tasks;

import io.questdb.griffin.engine.groupby.PostAggregationCircuitBreaker;
import io.questdb.griffin.engine.groupby.GroupByShardMerger;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberDispatchContext;
import io.questdb.std.Mutable;

import java.util.concurrent.atomic.AtomicInteger;

public class GroupByMergeShardTask implements Mutable {
    private GroupByShardMerger merger;
    private PostAggregationCircuitBreaker circuitBreaker;
    private FiberDispatchContext dispatchContext;
    private CountDownLatchSPI doneLatch;
    private int shardIndex = -1;
    private AtomicInteger startedCounter;

    @Override
    public void clear() {
        shardIndex = -1;
        merger = null;
        circuitBreaker = null;
        dispatchContext = null;
        doneLatch = null;
        startedCounter = null;
    }

    public GroupByShardMerger getMerger() {
        return merger;
    }

    public PostAggregationCircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    public CountDownLatchSPI getDoneLatch() {
        return doneLatch;
    }

    public FiberDispatchContext getDispatchContext() {
        return dispatchContext;
    }

    public int getShardIndex() {
        return shardIndex;
    }

    public AtomicInteger getStartedCounter() {
        return startedCounter;
    }

    public void of(
            PostAggregationCircuitBreaker circuitBreaker,
            AtomicInteger startedCounter,
            CountDownLatchSPI doneLatch,
            GroupByShardMerger merger,
            int shardIndex
    ) {
        this.circuitBreaker = circuitBreaker;
        this.dispatchContext = Fiber.captureParallelDispatchContext();
        this.startedCounter = startedCounter;
        this.doneLatch = doneLatch;
        this.merger = merger;
        this.shardIndex = shardIndex;
    }
}
