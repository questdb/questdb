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

package io.questdb.cairo.sql.async;

import io.questdb.std.Os;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Forces busy spinning in situations when a sufficient number of tasks is picked up
 * by other threads. The goal of this strategy is to decrease query latency
 * on large multicore machines.
 */
public class AdaptiveWorkStealingStrategy implements WorkStealingStrategy {
    private final long napThreshold;
    private final int noStealingThreshold;
    private AtomicInteger startedCounter;
    private long ticker;

    public AdaptiveWorkStealingStrategy(int noStealingThreshold, long napThreshold) {
        this.noStealingThreshold = noStealingThreshold;
        this.napThreshold = napThreshold;
    }

    @Override
    public WorkStealingStrategy of(AtomicInteger startedCounter) {
        this.startedCounter = startedCounter;
        ticker = 0;
        return this;
    }

    @Override
    public void reset() {
        ticker = 0;
    }

    @Override
    public boolean shouldStealWork(int finishedCount) {
        // Give shared workers a chance to pick up the tasks.
        for (int i = 0, n = 2 * noStealingThreshold; i < n; i++) {
            if (startedCounter.get() - finishedCount >= noStealingThreshold) {
                // A number of tasks are being processed,
                // so let's spin while those workers are doing their job.
                if (++ticker < napThreshold) {
                    Os.pause();
                } else {
                    Os.sleep(1);
                }
                return false;
            }
            Os.pause();
        }
        return true;
    }
}
