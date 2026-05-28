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

import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.griffin.engine.table.GroupByShardingContext;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Mutable;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

public class GroupByMergeShardTask implements Mutable {
    private GroupByShardingContext shardingCtx;
    private AtomicBooleanCircuitBreaker circuitBreaker;
    private CountDownLatchSPI doneLatch;
    // Per-query memory tracker captured at dispatch time. Null when no per-query
    // limit applies. Workers read it via getMemoryTracker() to charge their
    // allocations to the active workload.
    private MemoryTracker memoryTracker;
    private int shardIndex = -1;
    private AtomicInteger startedCounter;

    @Override
    public void clear() {
        shardIndex = -1;
        shardingCtx = null;
        circuitBreaker = null;
        doneLatch = null;
        memoryTracker = null;
        startedCounter = null;
    }

    public GroupByShardingContext getShardingContext() {
        return shardingCtx;
    }

    public AtomicBooleanCircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    public CountDownLatchSPI getDoneLatch() {
        return doneLatch;
    }

    public MemoryTracker getMemoryTracker() {
        return memoryTracker;
    }

    public int getShardIndex() {
        return shardIndex;
    }

    public AtomicInteger getStartedCounter() {
        return startedCounter;
    }

    public void of(
            AtomicBooleanCircuitBreaker circuitBreaker,
            @Nullable MemoryTracker memoryTracker,
            AtomicInteger startedCounter,
            CountDownLatchSPI doneLatch,
            GroupByShardingContext shardingCtx,
            int shardIndex
    ) {
        this.circuitBreaker = circuitBreaker;
        this.memoryTracker = memoryTracker;
        this.startedCounter = startedCounter;
        this.doneLatch = doneLatch;
        this.shardingCtx = shardingCtx;
        this.shardIndex = shardIndex;
    }
}
